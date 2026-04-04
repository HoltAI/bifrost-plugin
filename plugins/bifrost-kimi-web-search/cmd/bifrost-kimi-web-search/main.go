package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"golang.org/x/net/html"
)

type PluginConfig struct {
	Enabled          bool     `json:"enabled"`
	Debug            bool     `json:"debug"`
	Paths            []string `json:"paths"`
	MatchModels      []string `json:"match_models"`
	MatchSubstrings  []string `json:"match_substrings"`
	MatchVirtualKeys []string `json:"match_virtual_keys"`
	SearchToolNames  []string `json:"search_tool_names"`
	MaxResults       int      `json:"max_results"`
	TimeoutMS        int      `json:"timeout_ms"`
	InjectLabel      string   `json:"inject_label"`

	normalizedPaths map[string]struct{} `json:"-"`
}

type searchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet,omitempty"`
}

type anthropicSearchToolUse struct {
	ID    string
	Query string
}

type ddgInstantAnswer struct {
	Heading       string         `json:"Heading"`
	AbstractURL   string         `json:"AbstractURL"`
	AbstractText  string         `json:"AbstractText"`
	RelatedTopics []instantTopic `json:"RelatedTopics"`
}

type instantTopic struct {
	FirstURL string         `json:"FirstURL"`
	Text     string         `json:"Text"`
	Topics   []instantTopic `json:"Topics"`
}

var pluginConfig = PluginConfig{
	Enabled:         true,
	Paths:           []string{"/anthropic/v1/messages", "/v1/chat/completions"},
	MatchModels:     []string{"Kimi-K2.5", "claude-sonnet-4-6"},
	MatchSubstrings: []string{"__azure-kimi"},
	SearchToolNames: []string{"web_search", "search_web"},
	MaxResults:      5,
	TimeoutMS:       20000,
	InjectLabel:     "[Server Web Search Results]",
	normalizedPaths: nil,
}

var searchHTTPClient = &http.Client{Timeout: 20 * time.Second}

func Init(config any) error {
	if config != nil {
		raw, err := json.Marshal(config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
		var parsed PluginConfig
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return fmt.Errorf("unmarshal config: %w", err)
		}
		mergeConfig(&pluginConfig, parsed)
	}
	if len(pluginConfig.Paths) == 0 {
		pluginConfig.Paths = []string{"/anthropic/v1/messages", "/v1/chat/completions"}
	}
	if len(pluginConfig.SearchToolNames) == 0 {
		pluginConfig.SearchToolNames = []string{"web_search", "search_web"}
	}
	if pluginConfig.MaxResults <= 0 {
		pluginConfig.MaxResults = 5
	}
	if pluginConfig.TimeoutMS <= 0 {
		pluginConfig.TimeoutMS = 20000
	}
	if pluginConfig.InjectLabel == "" {
		pluginConfig.InjectLabel = "[Server Web Search Results]"
	}
	pluginConfig.normalizedPaths = make(map[string]struct{}, len(pluginConfig.Paths))
	for _, path := range pluginConfig.Paths {
		pluginConfig.normalizedPaths[normalizePath(path)] = struct{}{}
	}
	searchHTTPClient = &http.Client{Timeout: time.Duration(pluginConfig.TimeoutMS) * time.Millisecond}
	log.Printf("[%s] initialized paths=%v match_models=%v match_substrings=%v match_virtual_keys=%v search_tool_names=%v max_results=%d timeout_ms=%d",
		GetName(),
		pluginConfig.Paths,
		pluginConfig.MatchModels,
		pluginConfig.MatchSubstrings,
		maskSensitiveList(pluginConfig.MatchVirtualKeys),
		pluginConfig.SearchToolNames,
		pluginConfig.MaxResults,
		pluginConfig.TimeoutMS,
	)
	return nil
}

func GetName() string {
	return "bifrost-kimi-web-search"
}

func Cleanup() error {
	return nil
}

func HTTPTransportPreHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) (*schemas.HTTPResponse, error) {
	if !pluginConfig.Enabled || req == nil || !strings.EqualFold(req.Method, http.MethodPost) {
		return nil, nil
	}
	if !pathEnabled(req.Path) || len(req.Body) == 0 {
		return nil, nil
	}

	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, nil
	}
	model := asString(body["model"])
	if !shouldHandleRequest(ctx, req, model) {
		return nil, nil
	}

	if hasToolResults(req.Path, body) {
		if normalizePath(req.Path) != "/anthropic/v1/messages" {
			debugf("skipping search tool result rewrite for model=%q path=%s: unsupported path", model, req.Path)
			return nil, nil
		}
		if !rewriteAnthropicSearchToolResults(body) {
			debugf(
				"skipping search tool result rewrite for model=%q path=%s: no matching search tool results; summary=%s",
				model,
				req.Path,
				summarizeAnthropicToolResultState(body),
			)
			return nil, nil
		}
		removeSearchTools(req.Path, body)
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		req.Body = encoded
		debugf("rewrote client search tool_result into user context model=%q path=%s", model, req.Path)
		return nil, nil
	}

	toolNames := extractSearchToolNames(req.Path, body)
	if len(toolNames) == 0 {
		debugf("skipping search tool protocol for model=%q path=%s: no matching search tools", model, req.Path)
		return nil, nil
	}

	if normalizePath(req.Path) != "/anthropic/v1/messages" {
		debugf("skipping search tool short-circuit for model=%q path=%s: unsupported path", model, req.Path)
		return nil, nil
	}
	query := latestUserText(req.Path, body)
	if query == "" {
		debugf("skipping search tool short-circuit for model=%q path=%s: empty user query", model, req.Path)
		return nil, nil
	}
	toolName := firstSearchToolName(req.Path, body)
	if toolName == "" {
		debugf("skipping search tool short-circuit for model=%q path=%s: could not resolve tool name", model, req.Path)
		return nil, nil
	}
	debugf("returning synthetic tool_use for model=%q path=%s tool=%q query=%q", model, req.Path, toolName, trimForLog(query))
	return anthropicToolUseResponse(req, model, toolName, query)
}

func HTTPTransportPostHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, resp *schemas.HTTPResponse) error {
	return nil
}

func HTTPTransportStreamChunkHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, chunk *schemas.BifrostStreamChunk) (*schemas.BifrostStreamChunk, error) {
	return chunk, nil
}

func mergeConfig(dst *PluginConfig, src PluginConfig) {
	dst.Enabled = src.Enabled
	dst.Debug = src.Debug
	if len(src.Paths) > 0 {
		dst.Paths = append([]string(nil), src.Paths...)
	}
	if len(src.MatchModels) > 0 {
		dst.MatchModels = append([]string(nil), src.MatchModels...)
	}
	if len(src.MatchSubstrings) > 0 {
		dst.MatchSubstrings = append([]string(nil), src.MatchSubstrings...)
	}
	if len(src.MatchVirtualKeys) > 0 {
		dst.MatchVirtualKeys = append([]string(nil), src.MatchVirtualKeys...)
	}
	if len(src.SearchToolNames) > 0 {
		dst.SearchToolNames = append([]string(nil), src.SearchToolNames...)
	}
	if src.MaxResults > 0 {
		dst.MaxResults = src.MaxResults
	}
	if src.TimeoutMS > 0 {
		dst.TimeoutMS = src.TimeoutMS
	}
	if src.InjectLabel != "" {
		dst.InjectLabel = src.InjectLabel
	}
}

func pathEnabled(path string) bool {
	if len(pluginConfig.normalizedPaths) == 0 {
		return true
	}
	_, ok := pluginConfig.normalizedPaths[normalizePath(path)]
	return ok
}

func shouldHandleRequest(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, model string) bool {
	return shouldHandleModel(model) && shouldHandleVirtualKey(ctx, req)
}

func shouldHandleModel(model string) bool {
	variants := modelMatchVariants(model)
	if len(variants) == 0 {
		return false
	}
	for _, candidate := range pluginConfig.MatchModels {
		candidate = strings.TrimSpace(candidate)
		for _, variant := range variants {
			if strings.EqualFold(candidate, variant) {
				return true
			}
		}
	}
	for _, candidate := range pluginConfig.MatchSubstrings {
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if candidate == "" {
			continue
		}
		for _, variant := range variants {
			if strings.Contains(strings.ToLower(variant), candidate) {
				return true
			}
		}
	}
	return false
}

func shouldHandleVirtualKey(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) bool {
	if len(pluginConfig.MatchVirtualKeys) == 0 {
		return true
	}
	virtualKey := firstNonEmpty(
		contextStringValue(ctx, schemas.BifrostContextKeyVirtualKey),
		headerVirtualKey(req),
	)
	if virtualKey == "" {
		return false
	}
	for _, candidate := range pluginConfig.MatchVirtualKeys {
		if strings.EqualFold(strings.TrimSpace(candidate), virtualKey) {
			return true
		}
	}
	return false
}

func extractSearchToolNames(path string, body map[string]any) []string {
	names := make([]string, 0)
	seen := make(map[string]struct{})
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			name := normalizeToolName(asString(tool["name"]))
			if name == "" || !isSearchToolName(name) {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	case "/v1/chat/completions":
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			function := asMap(tool["function"])
			name := normalizeToolName(asString(function["name"]))
			if name == "" || !isSearchToolName(name) {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	return names
}

func firstSearchToolName(path string, body map[string]any) string {
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		if toolChoice := asMap(body["tool_choice"]); strings.EqualFold(asString(toolChoice["type"]), "tool") {
			name := strings.TrimSpace(asString(toolChoice["name"]))
			if isSearchToolName(name) {
				return name
			}
		}
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			name := strings.TrimSpace(asString(tool["name"]))
			if isSearchToolName(name) {
				return name
			}
		}
	case "/v1/chat/completions":
		if toolChoice := asMap(body["tool_choice"]); strings.EqualFold(asString(toolChoice["type"]), "function") {
			function := asMap(toolChoice["function"])
			name := strings.TrimSpace(asString(function["name"]))
			if isSearchToolName(name) {
				return name
			}
		}
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			function := asMap(tool["function"])
			name := strings.TrimSpace(asString(function["name"]))
			if isSearchToolName(name) {
				return name
			}
		}
	}
	return ""
}

func isSearchToolName(name string) bool {
	name = normalizeToolName(name)
	if name == "" {
		return false
	}
	for _, candidate := range pluginConfig.SearchToolNames {
		if normalizeToolName(candidate) == name {
			return true
		}
	}
	return false
}

func normalizeToolName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func hasToolResults(path string, body map[string]any) bool {
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		for _, rawMessage := range asSlice(body["messages"]) {
			message := asMap(rawMessage)
			for _, rawBlock := range asSlice(message["content"]) {
				block := asMap(rawBlock)
				if strings.EqualFold(asString(block["type"]), "tool_result") {
					return true
				}
			}
		}
	case "/v1/chat/completions":
		for _, rawMessage := range asSlice(body["messages"]) {
			message := asMap(rawMessage)
			if strings.EqualFold(asString(message["role"]), "tool") {
				return true
			}
		}
	}
	return false
}

func latestUserText(path string, body map[string]any) string {
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		for i := len(asSlice(body["messages"])) - 1; i >= 0; i-- {
			message := asMap(asSlice(body["messages"])[i])
			if !strings.EqualFold(asString(message["role"]), "user") {
				continue
			}
			return extractAnthropicMessageText(message["content"])
		}
	case "/v1/chat/completions":
		for i := len(asSlice(body["messages"])) - 1; i >= 0; i-- {
			message := asMap(asSlice(body["messages"])[i])
			if !strings.EqualFold(asString(message["role"]), "user") {
				continue
			}
			return extractOpenAIMessageText(message["content"])
		}
	}
	return ""
}

func extractAnthropicMessageText(content any) string {
	switch value := content.(type) {
	case string:
		return strings.TrimSpace(value)
	case []any:
		parts := make([]string, 0)
		for _, rawPart := range value {
			part := asMap(rawPart)
			if strings.EqualFold(asString(part["type"]), "text") {
				text := strings.TrimSpace(asString(part["text"]))
				if text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n\n"))
	default:
		return ""
	}
}

func extractOpenAIMessageText(content any) string {
	switch value := content.(type) {
	case string:
		return strings.TrimSpace(value)
	case []any:
		parts := make([]string, 0)
		for _, rawPart := range value {
			part := asMap(rawPart)
			partType := strings.ToLower(strings.TrimSpace(asString(part["type"])))
			switch partType {
			case "text", "input_text":
				text := strings.TrimSpace(firstNonEmpty(asString(part["text"]), asString(part["input_text"])))
				if text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n\n"))
	default:
		return ""
	}
}

func rewriteAnthropicSearchToolResults(body map[string]any) bool {
	messages := asSlice(body["messages"])
	if len(messages) == 0 {
		return false
	}

	searchToolUses := collectAnthropicSearchToolUses(messages)
	toolResultCount := countAnthropicToolResults(messages)
	fallbackQuery := extractFallbackSearchQuery(messages)
	if len(searchToolUses) == 0 && toolResultCount == 0 {
		return false
	}

	rewrittenMessages := make([]any, 0, len(messages))
	rewroteAny := false
	searchCache := make(map[string]string)
	pendingSearchToolUses := make([]anthropicSearchToolUse, 0)
	for _, rawMessage := range messages {
		message := asMap(rawMessage)
		if len(message) == 0 {
			rewrittenMessages = append(rewrittenMessages, rawMessage)
			continue
		}

		contentBlocks := asSlice(message["content"])
		if len(contentBlocks) == 0 {
			rewrittenMessages = append(rewrittenMessages, rawMessage)
			continue
		}

		switch strings.ToLower(strings.TrimSpace(asString(message["role"]))) {
		case "assistant":
			pendingSearchToolUses = append(pendingSearchToolUses, extractAnthropicSearchToolUsesFromContent(contentBlocks)...)
			filteredBlocks := make([]any, 0, len(contentBlocks))
			removedSearchToolUse := false
			for _, rawBlock := range contentBlocks {
				block := asMap(rawBlock)
				if strings.EqualFold(asString(block["type"]), "tool_use") {
					if _, ok := searchToolUses[asString(block["id"])]; ok {
						removedSearchToolUse = true
						rewroteAny = true
						continue
					}
				}
				filteredBlocks = append(filteredBlocks, rawBlock)
			}
			if removedSearchToolUse && len(filteredBlocks) == 0 {
				continue
			}
			message["content"] = filteredBlocks
			rewrittenMessages = append(rewrittenMessages, message)
		case "user":
			onlyToolResults := anthropicContentHasOnlyToolResults(contentBlocks)
			filteredBlocks := make([]any, 0, len(contentBlocks))
			for _, rawBlock := range contentBlocks {
				block := asMap(rawBlock)
				if strings.EqualFold(asString(block["type"]), "tool_result") {
					rendered := ""
					if toolUse, ok := searchToolUses[asString(block["tool_use_id"])]; ok {
						pendingSearchToolUses = consumeAnthropicSearchToolUse(pendingSearchToolUses, asString(block["tool_use_id"]))
						rendered = renderAnthropicToolResultAsText(block["content"], toolUse.Query, searchCache)
					} else if onlyToolResults {
						if toolUse, ok := peekAnthropicSearchToolUse(pendingSearchToolUses); ok {
							pendingSearchToolUses = consumeAnthropicSearchToolUse(pendingSearchToolUses, toolUse.ID)
							rendered = renderAnthropicToolResultAsText(block["content"], toolUse.Query, searchCache)
						}
					} else if shouldTreatAsFallbackSearchToolResult(block, toolResultCount, fallbackQuery) {
						rendered = renderAnthropicToolResultAsText(block["content"], fallbackQuery, searchCache)
					}
					if rendered != "" {
						filteredBlocks = append(filteredBlocks, map[string]any{
							"type": "text",
							"text": rendered,
						})
						rewroteAny = true
						continue
					}
				}
				filteredBlocks = append(filteredBlocks, rawBlock)
			}
			message["content"] = filteredBlocks
			rewrittenMessages = append(rewrittenMessages, message)
		default:
			rewrittenMessages = append(rewrittenMessages, rawMessage)
		}
	}

	if !rewroteAny {
		return false
	}
	body["messages"] = rewrittenMessages
	return true
}

func collectAnthropicSearchToolUses(messages []any) map[string]anthropicSearchToolUse {
	toolUses := make(map[string]anthropicSearchToolUse)
	for _, rawMessage := range messages {
		message := asMap(rawMessage)
		if !strings.EqualFold(asString(message["role"]), "assistant") {
			continue
		}
		for _, toolUse := range extractAnthropicSearchToolUsesFromContent(message["content"]) {
			if toolUse.ID == "" {
				continue
			}
			toolUses[toolUse.ID] = toolUse
		}
	}
	return toolUses
}

func extractAnthropicSearchToolUsesFromContent(content any) []anthropicSearchToolUse {
	toolUses := make([]anthropicSearchToolUse, 0)
	for _, rawBlock := range asSlice(content) {
		block := asMap(rawBlock)
		if !strings.EqualFold(asString(block["type"]), "tool_use") {
			continue
		}
		name := strings.TrimSpace(asString(block["name"]))
		if !isSearchToolName(name) {
			continue
		}
		toolUses = append(toolUses, anthropicSearchToolUse{
			ID:    strings.TrimSpace(asString(block["id"])),
			Query: extractSearchToolQuery(block["input"]),
		})
	}
	return toolUses
}

func extractSearchToolQuery(input any) string {
	payload := asMap(input)
	if len(payload) == 0 {
		return ""
	}
	return normalizeFallbackSearchQuery(strings.TrimSpace(firstNonEmpty(
		asString(payload["query"]),
		asString(payload["q"]),
		asString(payload["search_query"]),
	)))
}

func countAnthropicToolResults(messages []any) int {
	count := 0
	for _, rawMessage := range messages {
		message := asMap(rawMessage)
		if !strings.EqualFold(asString(message["role"]), "user") {
			continue
		}
		for _, rawBlock := range asSlice(message["content"]) {
			block := asMap(rawBlock)
			if strings.EqualFold(asString(block["type"]), "tool_result") {
				count++
			}
		}
	}
	return count
}

func extractFallbackSearchQuery(messages []any) string {
	for i := len(messages) - 1; i >= 0; i-- {
		message := asMap(messages[i])
		if len(message) == 0 {
			continue
		}

		if query := extractExplicitSearchQueryFromAnthropicContent(message["content"]); query != "" {
			return query
		}
		if query := extractSearchToolQueryFromAnthropicContent(message["content"]); query != "" {
			return query
		}

		if !strings.EqualFold(asString(message["role"]), "user") {
			continue
		}
		if anthropicContentHasOnlyToolResults(message["content"]) {
			continue
		}
		query := normalizeFallbackSearchQuery(extractAnthropicMessageText(message["content"]))
		if query != "" {
			return query
		}
	}
	return ""
}

func extractExplicitSearchQueryFromAnthropicContent(content any) string {
	text := extractAnthropicMessageText(content)
	if text == "" {
		return ""
	}
	return extractSearchInvocationQuery(text)
}

func extractSearchToolQueryFromAnthropicContent(content any) string {
	toolUses := extractAnthropicSearchToolUsesFromContent(content)
	for i := len(toolUses) - 1; i >= 0; i-- {
		if query := strings.TrimSpace(toolUses[i].Query); query != "" {
			return query
		}
	}
	return ""
}

func anthropicContentHasOnlyToolResults(content any) bool {
	contentBlocks := asSlice(content)
	if len(contentBlocks) == 0 {
		return false
	}
	for _, rawBlock := range contentBlocks {
		block := asMap(rawBlock)
		if !strings.EqualFold(asString(block["type"]), "tool_result") {
			return false
		}
	}
	return true
}

func normalizeFallbackSearchQuery(query string) string {
	query = strings.TrimSpace(query)
	lower := strings.ToLower(query)
	prefixes := []string{
		"perform a web search for the query:",
		"perform web search for the query:",
		"search the web for:",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			trimmed := strings.TrimSpace(query[len(prefix):])
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return query
}

func extractSearchInvocationQuery(text string) string {
	text = strings.ReplaceAll(text, "“", "\"")
	text = strings.ReplaceAll(text, "”", "\"")
	text = strings.ReplaceAll(text, "‘", "'")
	text = strings.ReplaceAll(text, "’", "'")
	lower := strings.ToLower(text)

	markers := []string{
		"web search(",
		"websearch(",
		"web_search(",
		"search_web(",
	}
	for _, marker := range markers {
		if idx := strings.LastIndex(lower, marker); idx >= 0 {
			if query := parseDelimitedQuery(text[idx+len(marker):]); query != "" {
				return normalizeFallbackSearchQuery(query)
			}
		}
	}

	prefixes := []string{
		"perform a web search for the query:",
		"perform web search for the query:",
		"search the web for:",
	}
	for _, prefix := range prefixes {
		if idx := strings.LastIndex(lower, prefix); idx >= 0 {
			query := strings.TrimSpace(text[idx+len(prefix):])
			if query != "" {
				return normalizeFallbackSearchQuery(query)
			}
		}
	}

	return ""
}

func parseDelimitedQuery(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}

	if quote := text[0]; quote == '"' || quote == '\'' {
		for i := 1; i < len(text); i++ {
			if text[i] == quote && text[i-1] != '\\' {
				return strings.TrimSpace(text[1:i])
			}
		}
	}

	if idx := strings.IndexByte(text, ')'); idx >= 0 {
		return strings.TrimSpace(text[:idx])
	}

	return strings.TrimSpace(text)
}

func renderAnthropicToolResultAsText(content any, fallbackQuery string, searchCache map[string]string) string {
	rendered := strings.TrimSpace(renderToolResultContent(content))
	rendered = sanitizeAnthropicToolResultText(rendered)

	if shouldFallbackSearch(rendered, fallbackQuery) {
		if cached := strings.TrimSpace(searchCache[fallbackQuery]); cached != "" {
			debugf("reused cached fallback search results for query=%q", trimForLog(fallbackQuery))
			return cached
		}
		results, err := doSearch(fallbackQuery, pluginConfig.MaxResults)
		if err != nil {
			debugf("fallback search failed for query=%q: %v", trimForLog(fallbackQuery), err)
		} else if len(results) > 0 {
			rendered = renderSearchResults(fallbackQuery, results)
			searchCache[fallbackQuery] = rendered
			debugf("fallback search produced %d results for query=%q", len(results), trimForLog(fallbackQuery))
			return rendered
		}
	}

	if rendered == "" {
		return ""
	}
	if strings.HasPrefix(rendered, pluginConfig.InjectLabel+"\n") || rendered == pluginConfig.InjectLabel {
		return rendered
	}
	return pluginConfig.InjectLabel + "\n" + rendered
}

func shouldFallbackSearch(rendered, fallbackQuery string) bool {
	if strings.TrimSpace(fallbackQuery) == "" {
		return false
	}
	if strings.TrimSpace(rendered) == "" {
		return true
	}
	normalized := strings.ToLower(strings.TrimSpace(rendered))
	switch {
	case strings.Contains(normalized, "did 0 searches"),
		strings.Contains(normalized, "did 0 search"),
		strings.Contains(normalized, "0 searches in"),
		strings.Contains(normalized, "0 search in"),
		strings.Contains(normalized, "no search results"),
		strings.Contains(normalized, "no search result"),
		strings.Contains(normalized, "no results found"),
		strings.Contains(normalized, "\"results\":[]"),
		strings.Contains(normalized, "\"data\":[]"),
		strings.Contains(normalized, "\"items\":[]"):
		return true
	default:
		return false
	}
}

func shouldTreatAsFallbackSearchToolResult(block map[string]any, totalToolResults int, fallbackQuery string) bool {
	if strings.TrimSpace(fallbackQuery) == "" {
		return false
	}
	rendered := sanitizeAnthropicToolResultText(renderToolResultContent(block["content"]))
	if shouldFallbackSearch(rendered, fallbackQuery) {
		return true
	}
	return totalToolResults == 1
}

func peekAnthropicSearchToolUse(toolUses []anthropicSearchToolUse) (anthropicSearchToolUse, bool) {
	for _, toolUse := range toolUses {
		if strings.TrimSpace(toolUse.Query) != "" {
			return toolUse, true
		}
	}
	return anthropicSearchToolUse{}, false
}

func consumeAnthropicSearchToolUse(toolUses []anthropicSearchToolUse, id string) []anthropicSearchToolUse {
	if len(toolUses) == 0 {
		return toolUses
	}
	if strings.TrimSpace(id) != "" {
		for i, toolUse := range toolUses {
			if strings.EqualFold(strings.TrimSpace(toolUse.ID), strings.TrimSpace(id)) {
				return append(toolUses[:i], toolUses[i+1:]...)
			}
		}
	}
	return toolUses[1:]
}

func summarizeAnthropicToolResultState(body map[string]any) string {
	messages := asSlice(body["messages"])
	if len(messages) == 0 {
		return "messages=0"
	}

	summaries := make([]string, 0, 4)
	for i := len(messages) - 1; i >= 0 && len(summaries) < 4; i-- {
		message := asMap(messages[i])
		if len(message) == 0 {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(asString(message["role"])))
		blockTypes := make([]string, 0, len(asSlice(message["content"])))
		for _, rawBlock := range asSlice(message["content"]) {
			block := asMap(rawBlock)
			blockType := strings.ToLower(strings.TrimSpace(asString(block["type"])))
			if blockType != "" {
				blockTypes = append(blockTypes, blockType)
			}
		}

		query := firstNonEmpty(
			extractExplicitSearchQueryFromAnthropicContent(message["content"]),
			extractSearchToolQueryFromAnthropicContent(message["content"]),
		)
		if query != "" {
			summaries = append(summaries, fmt.Sprintf("%s[%s] query=%q", role, strings.Join(blockTypes, ","), trimForLog(query)))
			continue
		}
		text := extractAnthropicMessageText(message["content"])
		if text != "" {
			summaries = append(summaries, fmt.Sprintf("%s[%s] text=%q", role, strings.Join(blockTypes, ","), trimForLog(text)))
			continue
		}
		summaries = append(summaries, fmt.Sprintf("%s[%s]", role, strings.Join(blockTypes, ",")))
	}

	if len(summaries) == 0 {
		return "messages-present"
	}
	return strings.Join(summaries, " | ")
}

func sanitizeAnthropicToolResultText(rendered string) string {
	rendered = strings.TrimSpace(rendered)
	if rendered == "" {
		return ""
	}

	lines := strings.Split(rendered, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if len(filtered) == 0 || filtered[len(filtered)-1] == "" {
				continue
			}
			filtered = append(filtered, "")
			continue
		}
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "did ") && strings.Contains(lower, " search") && strings.Contains(lower, " in ") {
			continue
		}
		filtered = append(filtered, line)
	}

	return strings.TrimSpace(strings.Join(filtered, "\n"))
}

func injectSearchResults(path string, body map[string]any, query string, results []searchResult) bool {
	searchText := renderSearchResults(query, results)
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		messages := asSlice(body["messages"])
		for i := len(messages) - 1; i >= 0; i-- {
			message := asMap(messages[i])
			if !strings.EqualFold(asString(message["role"]), "user") {
				continue
			}
			message["content"] = injectAnthropicContent(message["content"], searchText)
			return true
		}
	case "/v1/chat/completions":
		messages := asSlice(body["messages"])
		for i := len(messages) - 1; i >= 0; i-- {
			message := asMap(messages[i])
			if !strings.EqualFold(asString(message["role"]), "user") {
				continue
			}
			message["content"] = injectOpenAIContent(message["content"], searchText)
			return true
		}
	}
	return false
}

func injectAnthropicContent(content any, searchText string) any {
	switch value := content.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return searchText
		}
		return value + "\n\n" + searchText
	case []any:
		cloned := append([]any(nil), value...)
		for i, rawPart := range cloned {
			part := asMap(rawPart)
			if strings.EqualFold(asString(part["type"]), "text") {
				part["text"] = strings.TrimSpace(asString(part["text"]) + "\n\n" + searchText)
				cloned[i] = part
				return cloned
			}
		}
		return append(cloned, map[string]any{"type": "text", "text": searchText})
	default:
		return []any{map[string]any{"type": "text", "text": searchText}}
	}
}

func injectOpenAIContent(content any, searchText string) any {
	switch value := content.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return searchText
		}
		return value + "\n\n" + searchText
	case []any:
		cloned := append([]any(nil), value...)
		for i, rawPart := range cloned {
			part := asMap(rawPart)
			partType := strings.ToLower(strings.TrimSpace(asString(part["type"])))
			if partType == "text" || partType == "input_text" {
				current := firstNonEmpty(asString(part["text"]), asString(part["input_text"]))
				if partType == "input_text" {
					part["input_text"] = strings.TrimSpace(current + "\n\n" + searchText)
				} else {
					part["text"] = strings.TrimSpace(current + "\n\n" + searchText)
				}
				cloned[i] = part
				return cloned
			}
		}
		return append(cloned, map[string]any{"type": "text", "text": searchText})
	default:
		return []any{map[string]any{"type": "text", "text": searchText}}
	}
}

func removeSearchTools(path string, body map[string]any) {
	switch normalizePath(path) {
	case "/anthropic/v1/messages":
		filtered := make([]any, 0)
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			if isSearchToolName(asString(tool["name"])) {
				continue
			}
			filtered = append(filtered, rawTool)
		}
		updateToolsAndChoice(body, filtered, anthropicToolChoiceShouldClear(body["tool_choice"]))
	case "/v1/chat/completions":
		filtered := make([]any, 0)
		for _, rawTool := range asSlice(body["tools"]) {
			tool := asMap(rawTool)
			function := asMap(tool["function"])
			if isSearchToolName(asString(function["name"])) {
				continue
			}
			filtered = append(filtered, rawTool)
		}
		updateToolsAndChoice(body, filtered, openAIToolChoiceShouldClear(body["tool_choice"]))
	}
}

func updateToolsAndChoice(body map[string]any, filtered []any, clearChoice bool) {
	if len(filtered) == 0 {
		delete(body, "tools")
	} else {
		body["tools"] = filtered
	}
	if clearChoice || len(filtered) == 0 {
		delete(body, "tool_choice")
	}
}

func anthropicToolChoiceShouldClear(toolChoice any) bool {
	switch value := toolChoice.(type) {
	case map[string]any:
		if strings.EqualFold(asString(value["type"]), "tool") && isSearchToolName(asString(value["name"])) {
			return true
		}
	case string:
		return isSearchToolName(value)
	}
	return false
}

func openAIToolChoiceShouldClear(toolChoice any) bool {
	switch value := toolChoice.(type) {
	case map[string]any:
		if strings.EqualFold(asString(value["type"]), "function") {
			function := asMap(value["function"])
			return isSearchToolName(asString(function["name"]))
		}
	case string:
		return isSearchToolName(value)
	}
	return false
}

func renderSearchResults(query string, results []searchResult) string {
	lines := []string{
		pluginConfig.InjectLabel,
		"Query: " + query,
	}
	if len(results) == 0 {
		lines = append(lines, "No search results were found.")
		return strings.Join(lines, "\n")
	}
	for index, item := range results {
		lines = append(lines, fmt.Sprintf("%d. %s", index+1, item.Title))
		lines = append(lines, "   URL: "+item.URL)
		if item.Snippet != "" {
			lines = append(lines, "   Snippet: "+item.Snippet)
		}
	}
	lines = append(lines, "Use these live search results when answering current or time-sensitive questions.")
	return strings.Join(lines, "\n")
}

func renderToolResultContent(content any) string {
	switch value := content.(type) {
	case string:
		return strings.TrimSpace(value)
	case []any:
		parts := make([]string, 0, len(value))
		for _, rawPart := range value {
			part := asMap(rawPart)
			if len(part) == 0 {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(asString(part["type"]))) {
			case "text", "input_text":
				text := strings.TrimSpace(firstNonEmpty(asString(part["text"]), asString(part["input_text"])))
				if text != "" {
					parts = append(parts, text)
				}
			case "json":
				if payload, ok := part["json"]; ok {
					if marshaled, err := json.Marshal(payload); err == nil {
						parts = append(parts, string(marshaled))
					}
				}
			default:
				if text := strings.TrimSpace(asString(part["text"])); text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n\n"))
	default:
		if marshaled, err := json.Marshal(value); err == nil {
			return strings.TrimSpace(string(marshaled))
		}
		return ""
	}
}

func anthropicToolUseResponse(req *schemas.HTTPRequest, model, toolName, query string) (*schemas.HTTPResponse, error) {
	messageID := "msg_" + compactUUID(22)
	toolUseID := "toolu_" + compactUUID(24)
	payload := map[string]any{
		"id":    messageID,
		"type":  "message",
		"role":  "assistant",
		"model": model,
		"content": []any{
			map[string]any{
				"type":  "tool_use",
				"id":    toolUseID,
				"name":  toolName,
				"input": map[string]any{"query": query},
			},
		},
		"stop_reason":   "tool_use",
		"stop_sequence": nil,
		"usage": map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		},
	}

	headers := map[string]string{
		"cache-control": "no-cache",
	}
	if anthropicVersion := req.CaseInsensitiveHeaderLookup("anthropic-version"); anthropicVersion != "" {
		headers["anthropic-version"] = anthropicVersion
	}

	if asBool(payloadValue(req, "stream")) {
		body, err := anthropicToolUseStreamBody(payload)
		if err != nil {
			return nil, err
		}
		headers["content-type"] = "text/event-stream"
		headers["connection"] = "keep-alive"
		return &schemas.HTTPResponse{
			StatusCode: http.StatusOK,
			Headers:    headers,
			Body:       body,
		}, nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	headers["content-type"] = "application/json"
	return &schemas.HTTPResponse{
		StatusCode: http.StatusOK,
		Headers:    headers,
		Body:       body,
	}, nil
}

func payloadValue(req *schemas.HTTPRequest, key string) any {
	if req == nil || len(req.Body) == 0 {
		return nil
	}
	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil
	}
	return body[key]
}

func asBool(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func anthropicToolUseStreamBody(payload map[string]any) ([]byte, error) {
	var buffer bytes.Buffer
	messageID := asString(payload["id"])
	model := asString(payload["model"])
	contentBlocks := asSlice(payload["content"])
	toolBlock := map[string]any{}
	if len(contentBlocks) > 0 {
		toolBlock = asMap(contentBlocks[0])
	}

	if err := emitAnthropicSSE(&buffer, "message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            messageID,
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens":  0,
				"output_tokens": 0,
			},
		},
	}); err != nil {
		return nil, err
	}
	if err := emitAnthropicSSE(&buffer, "content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": 0,
		"content_block": map[string]any{
			"type":  "tool_use",
			"id":    asString(toolBlock["id"]),
			"name":  asString(toolBlock["name"]),
			"input": map[string]any{},
		},
	}); err != nil {
		return nil, err
	}
	inputJSON, err := json.Marshal(asMap(toolBlock["input"]))
	if err != nil {
		return nil, err
	}
	if err := emitAnthropicSSE(&buffer, "content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": 0,
		"delta": map[string]any{
			"type":         "input_json_delta",
			"partial_json": string(inputJSON),
		},
	}); err != nil {
		return nil, err
	}
	if err := emitAnthropicSSE(&buffer, "content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": 0,
	}); err != nil {
		return nil, err
	}
	if err := emitAnthropicSSE(&buffer, "message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason": "tool_use",
		},
		"usage": map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		},
	}); err != nil {
		return nil, err
	}
	if err := emitAnthropicSSE(&buffer, "message_stop", map[string]any{
		"type": "message_stop",
	}); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func emitAnthropicSSE(writer io.Writer, eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", eventName, data)
	return err
}

func doSearch(query string, maxResults int) ([]searchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}
	if maxResults <= 0 {
		maxResults = 5
	}
	if maxResults > 10 {
		maxResults = 10
	}
	results, err := duckDuckGoHTMLSearch(query, maxResults)
	if err == nil && len(results) > 0 {
		return results, nil
	}
	fallback, fallbackErr := duckDuckGoInstantAnswer(query, maxResults)
	if fallbackErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, fallbackErr
	}
	return fallback, nil
}

func duckDuckGoHTMLSearch(query string, maxResults int) ([]searchResult, error) {
	form := url.Values{}
	form.Set("q", query)
	form.Set("kl", "us-en")
	req, err := http.NewRequest(http.MethodPost, "https://html.duckduckgo.com/html/", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("user-agent", "Mozilla/5.0 bifrost-kimi-web-search/1.0")
	resp, err := searchHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("search endpoint returned status %d", resp.StatusCode)
	}
	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, err
	}
	results := extractDDGResults(doc, maxResults)
	if len(results) == 0 {
		return nil, fmt.Errorf("no html search results")
	}
	return results, nil
}

func extractDDGResults(root *html.Node, maxResults int) []searchResult {
	results := make([]searchResult, 0, maxResults)
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node == nil || len(results) >= maxResults {
			return
		}
		if node.Type == html.ElementNode && node.Data == "div" && hasClass(node, "result") {
			titleNode := findFirstNode(node, func(candidate *html.Node) bool {
				return candidate.Type == html.ElementNode && candidate.Data == "a" && hasClass(candidate, "result__a")
			})
			if titleNode != nil {
				title := cleanText(nodeText(titleNode))
				targetURL := decodeResultURL(attrValue(titleNode, "href"))
				snippetNode := findFirstNode(node, func(candidate *html.Node) bool {
					return candidate.Type == html.ElementNode && hasClass(candidate, "result__snippet")
				})
				snippet := ""
				if snippetNode != nil {
					snippet = cleanText(nodeText(snippetNode))
				}
				if title != "" && targetURL != "" {
					results = append(results, searchResult{
						Title:   title,
						URL:     targetURL,
						Snippet: snippet,
					})
				}
			}
		}
		for child := node.FirstChild; child != nil && len(results) < maxResults; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return results
}

func duckDuckGoInstantAnswer(query string, maxResults int) ([]searchResult, error) {
	u, err := url.Parse("https://api.duckduckgo.com/")
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set("q", query)
	params.Set("format", "json")
	params.Set("no_html", "1")
	params.Set("skip_disambig", "1")
	u.RawQuery = params.Encode()
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("user-agent", "Mozilla/5.0 bifrost-kimi-web-search/1.0")
	resp, err := searchHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("instant answer endpoint returned status %d", resp.StatusCode)
	}
	var payload ddgInstantAnswer
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	results := make([]searchResult, 0, maxResults)
	if payload.AbstractURL != "" && (payload.Heading != "" || payload.AbstractText != "") {
		results = append(results, searchResult{
			Title:   cleanText(firstNonEmpty(payload.Heading, payload.AbstractURL)),
			URL:     payload.AbstractURL,
			Snippet: cleanText(payload.AbstractText),
		})
	}
	flattened := flattenInstantTopics(payload.RelatedTopics)
	for _, item := range flattened {
		if len(results) >= maxResults {
			break
		}
		if item.FirstURL == "" {
			continue
		}
		results = append(results, searchResult{
			Title:   cleanText(firstNonEmpty(item.Text, item.FirstURL)),
			URL:     item.FirstURL,
			Snippet: cleanText(item.Text),
		})
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no instant answer results")
	}
	return results, nil
}

func flattenInstantTopics(items []instantTopic) []instantTopic {
	flattened := make([]instantTopic, 0)
	var walk func([]instantTopic)
	walk = func(nodes []instantTopic) {
		for _, item := range nodes {
			if item.FirstURL != "" || item.Text != "" {
				flattened = append(flattened, item)
			}
			if len(item.Topics) > 0 {
				walk(item.Topics)
			}
		}
	}
	walk(items)
	return flattened
}

func decodeResultURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if strings.HasSuffix(strings.ToLower(parsed.Hostname()), "duckduckgo.com") && strings.HasPrefix(parsed.Path, "/l/") {
		if target := parsed.Query().Get("uddg"); target != "" {
			decoded, err := url.QueryUnescape(target)
			if err == nil && decoded != "" {
				return decoded
			}
			return target
		}
	}
	return raw
}

func findFirstNode(node *html.Node, predicate func(*html.Node) bool) *html.Node {
	if node == nil {
		return nil
	}
	if predicate(node) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findFirstNode(child, predicate); found != nil {
			return found
		}
	}
	return nil
}

func hasClass(node *html.Node, class string) bool {
	values := strings.Fields(attrValue(node, "class"))
	for _, value := range values {
		if value == class {
			return true
		}
	}
	return false
}

func attrValue(node *html.Node, key string) string {
	if node == nil {
		return ""
	}
	for _, attr := range node.Attr {
		if strings.EqualFold(attr.Key, key) {
			return attr.Val
		}
	}
	return ""
}

func nodeText(node *html.Node) string {
	if node == nil {
		return ""
	}
	var builder strings.Builder
	var walk func(*html.Node)
	walk = func(current *html.Node) {
		if current == nil {
			return
		}
		if current.Type == html.TextNode {
			builder.WriteString(current.Data)
			builder.WriteByte(' ')
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return builder.String()
}

func cleanText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func modelMatchVariants(model string) []string {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil
	}
	variants := []string{model}
	if slash := strings.Index(model, "/"); slash > 0 && slash < len(model)-1 {
		trimmed := strings.TrimSpace(model[slash+1:])
		if trimmed != "" && !strings.EqualFold(trimmed, model) {
			variants = append(variants, trimmed)
		}
	}
	return variants
}

func contextStringValue(ctx *schemas.BifrostContext, key any) string {
	if ctx == nil {
		return ""
	}
	return strings.TrimSpace(asString(ctx.Value(key)))
}

func headerVirtualKey(req *schemas.HTTPRequest) string {
	if req == nil {
		return ""
	}
	if vk := strings.TrimSpace(req.CaseInsensitiveHeaderLookup("x-bf-vk")); vk != "" {
		return vk
	}
	if apiKey := strings.TrimSpace(req.CaseInsensitiveHeaderLookup("x-api-key")); apiKey != "" {
		return apiKey
	}
	authorization := strings.TrimSpace(req.CaseInsensitiveHeaderLookup("authorization"))
	if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return strings.TrimSpace(authorization[7:])
	}
	return ""
}

func asMap(value any) map[string]any {
	if value == nil {
		return nil
	}
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return nil
}

func asSlice(value any) []any {
	if value == nil {
		return nil
	}
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func compactUUID(length int) string {
	value := strings.ReplaceAll(uuid.NewString(), "-", "")
	if length <= 0 || length >= len(value) {
		return value
	}
	return value[:length]
}

func trimForLog(text string) string {
	text = cleanText(text)
	if len(text) <= 160 {
		return text
	}
	return text[:160] + "..."
}

func maskSensitiveList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	masked := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if len(value) <= 10 {
			masked = append(masked, "****")
			continue
		}
		masked = append(masked, value[:6]+"..."+value[len(value)-4:])
	}
	return masked
}

func debugf(format string, args ...any) {
	if !pluginConfig.Debug {
		return
	}
	log.Printf("[%s] %s", GetName(), fmt.Sprintf(format, args...))
}

// Keep the linked package graph close to the other plugins. This avoids fragile
// Go plugin loader differences between tiny and large plugin binaries.
var (
	_ = bytes.NewBuffer
	_ = context.Background
	_ = io.Copy
	_ = reflect.TypeOf
	_ = time.Second
	_ = unsafe.Sizeof(0)
	_ = uuid.NewString
)
