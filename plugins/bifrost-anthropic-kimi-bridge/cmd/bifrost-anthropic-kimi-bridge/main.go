package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/providers/openai"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

const (
	ctxKeyConverted        = "bifrost_anthropic_kimi_bridge_converted"
	ctxKeyIdentityRule     = "bifrost_anthropic_kimi_bridge_identity_rule"
	ctxKeyRequestedModel   = "bifrost_anthropic_kimi_bridge_requested_model"
	ctxKeyToolAliasReverse = "bifrost_anthropic_kimi_bridge_tool_alias_reverse"

	maxKimiToolNameLength = 48
	toolAliasPrefix       = "bfk_"
	toolAliasHashLength   = 16
)

type PluginConfig struct {
	Enabled               bool           `json:"enabled"`
	UpstreamBaseURL       string         `json:"upstream_base_url"`
	ChatCompletionsPath   string         `json:"chat_completions_path"`
	AnthropicMessagesPath string         `json:"anthropic_messages_path"`
	MatchModels           []string       `json:"match_models"`
	MatchSubstrings       []string       `json:"match_substrings"`
	MatchVirtualKeys      []string       `json:"match_virtual_keys"`
	BridgeAllModels       bool           `json:"bridge_all_models"`
	TimeoutMS             int            `json:"timeout_ms"`
	Debug                 bool           `json:"debug"`
	IdentityRules         []IdentityRule `json:"identity_rules,omitempty"`
}

type IdentityRule struct {
	Name                  string        `json:"name"`
	Enabled               *bool         `json:"enabled,omitempty"`
	Paths                 []string      `json:"paths,omitempty"`
	MatchVirtualKeys      []string      `json:"match_virtual_keys,omitempty"`
	Match                 MatchRule     `json:"match"`
	DisplayName           string        `json:"display_name,omitempty"`
	KnowledgeCutoff       string        `json:"knowledge_cutoff,omitempty"`
	PublicIdentity        string        `json:"public_identity,omitempty"`
	IdentityRole          string        `json:"identity_role,omitempty"`
	IdentityPrompt        string        `json:"identity_prompt,omitempty"`
	UpstreamIdentityHints []string      `json:"upstream_identity_hints,omitempty"`
	StripReasoning        bool          `json:"strip_reasoning,omitempty"`
	StripThinkingTags     bool          `json:"strip_thinking_tags,omitempty"`
	Rewrites              []RewriteRule `json:"rewrites,omitempty"`

	compiledRewrites     []compiledRewrite `json:"-"`
	compiledHintRewrites []compiledRewrite `json:"-"`
	normalizedPaths      []string          `json:"-"`
}

type MatchRule struct {
	Contains []string `json:"contains,omitempty"`
	Equals   []string `json:"equals,omitempty"`
	Prefixes []string `json:"prefixes,omitempty"`
	Regex    []string `json:"regex,omitempty"`

	compiledRegex []*regexp.Regexp `json:"-"`
}

type RewriteRule struct {
	Pattern string `json:"pattern"`
	Replace string `json:"replace"`
}

type compiledRewrite struct {
	pattern *regexp.Regexp
	replace string
}

type anthropicStreamBridgeState struct {
	requestedModel        string
	reverseToolNames      map[string]string
	upstreamMessageID     string
	upstreamModel         string
	messageStarted        bool
	messageFinished       bool
	visibleContentEmitted bool
	reasoningFallback     strings.Builder
	nextContentIndex      int
	activeBlockType       string
	activeBlockIndex      int
	textBlockStarted      bool
	toolBlocks            map[int]*toolBlockState
}

type toolBlockState struct {
	openAIIndex     int
	anthropicIndex  int
	id              string
	name            string
	argumentsBuffer strings.Builder
	started         bool
	stopped         bool
}

var pluginConfig = PluginConfig{
	Enabled:               true,
	UpstreamBaseURL:       "http://127.0.0.1:8080",
	ChatCompletionsPath:   "/v1/chat/completions",
	AnthropicMessagesPath: "/anthropic/v1/messages",
	MatchModels:           []string{"Kimi-K2.5", "azure/Kimi-K2.5"},
	MatchSubstrings:       []string{"__azure-kimi"},
	MatchVirtualKeys:      nil,
	BridgeAllModels:       false,
	TimeoutMS:             300000,
	Debug:                 false,
}

var httpClient = &http.Client{Timeout: 300 * time.Second}
var streamingHTTPClient = &http.Client{}
var (
	thinkingTagPattern    = regexp.MustCompile(`(?is)</?thinking[^>]*>`)
	reasoningBlockPattern = regexp.MustCompile(`(?is)<think>.*?</think>`)
)

func Init(config any) error {
	if config != nil {
		raw, err := json.Marshal(config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
		cfg := pluginConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return fmt.Errorf("unmarshal config: %w", err)
		}
		pluginConfig = cfg
	}
	pluginConfig.UpstreamBaseURL = strings.TrimRight(pluginConfig.UpstreamBaseURL, "/")
	if pluginConfig.UpstreamBaseURL == "" {
		pluginConfig.UpstreamBaseURL = "http://127.0.0.1:8080"
	}
	if !strings.HasPrefix(pluginConfig.ChatCompletionsPath, "/") {
		pluginConfig.ChatCompletionsPath = "/" + pluginConfig.ChatCompletionsPath
	}
	if !strings.HasPrefix(pluginConfig.AnthropicMessagesPath, "/") {
		pluginConfig.AnthropicMessagesPath = "/" + pluginConfig.AnthropicMessagesPath
	}
	if pluginConfig.TimeoutMS <= 0 {
		pluginConfig.TimeoutMS = 300000
	}
	for i := range pluginConfig.IdentityRules {
		normalizeRuleDefaults(&pluginConfig.IdentityRules[i])
		if err := compileRule(&pluginConfig.IdentityRules[i]); err != nil {
			return fmt.Errorf("compile identity rule %q: %w", pluginConfig.IdentityRules[i].Name, err)
		}
	}

	httpClient = &http.Client{Timeout: time.Duration(pluginConfig.TimeoutMS) * time.Millisecond}
	log.Printf("[%s] initialized enabled=%v upstream=%s anthropic_path=%s chat_path=%s match_models=%v match_substrings=%v match_virtual_keys=%v bridge_all_models=%v timeout_ms=%d identity_rules=%d",
		GetName(),
		pluginConfig.Enabled,
		pluginConfig.UpstreamBaseURL,
		pluginConfig.AnthropicMessagesPath,
		pluginConfig.ChatCompletionsPath,
		pluginConfig.MatchModels,
		pluginConfig.MatchSubstrings,
		maskSensitiveList(pluginConfig.MatchVirtualKeys),
		pluginConfig.BridgeAllModels,
		pluginConfig.TimeoutMS,
		len(pluginConfig.IdentityRules),
	)
	return nil
}

func GetName() string {
	return "bifrost-anthropic-kimi-bridge"
}

func Cleanup() error {
	return nil
}

func normalizeRuleDefaults(rule *IdentityRule) {
	rule.compiledRewrites = nil
	rule.compiledHintRewrites = nil
	rule.Match.compiledRegex = nil
	rule.normalizedPaths = nil
	if rule.Name == "" {
		rule.Name = "identity-rule"
	}
	if len(rule.Paths) == 0 {
		rule.Paths = []string{"/anthropic/v1/messages", "/v1/chat/completions"}
	}
	for _, path := range rule.Paths {
		rule.normalizedPaths = append(rule.normalizedPaths, normalizePath(path))
	}
	if rule.IdentityRole == "" {
		rule.IdentityRole = "system"
	}
	if rule.PublicIdentity == "" {
		switch {
		case rule.DisplayName != "":
			rule.PublicIdentity = rule.DisplayName
		default:
			rule.PublicIdentity = "Claude"
		}
	}
	if rule.DisplayName == "" {
		rule.DisplayName = rule.PublicIdentity
	}
}

func compileRule(rule *IdentityRule) error {
	for _, pattern := range rule.Match.Regex {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		rule.Match.compiledRegex = append(rule.Match.compiledRegex, compiled)
	}
	for _, rewrite := range rule.Rewrites {
		compiled, err := regexp.Compile(rewrite.Pattern)
		if err != nil {
			return err
		}
		rule.compiledRewrites = append(rule.compiledRewrites, compiledRewrite{
			pattern: compiled,
			replace: rewrite.Replace,
		})
	}
	for _, hint := range rule.UpstreamIdentityHints {
		pattern := regexp.QuoteMeta(hint)
		compiled, err := regexp.Compile(`(?i)\b` + pattern + `\b`)
		if err != nil {
			return err
		}
		rule.compiledHintRewrites = append(rule.compiledHintRewrites, compiledRewrite{
			pattern: compiled,
			replace: rule.PublicIdentity,
		})
	}
	return nil
}

func matchIdentityRule(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, path, model string) *IdentityRule {
	for i := range pluginConfig.IdentityRules {
		rule := &pluginConfig.IdentityRules[i]
		if rule.Enabled != nil && !*rule.Enabled {
			continue
		}
		if path != "" && !pathAllowed(rule.normalizedPaths, normalizePath(path)) {
			continue
		}
		if !matchesVirtualKey(rule.MatchVirtualKeys, ctx, req) {
			continue
		}
		if matchesModelRule(rule.Match, model) {
			return rule
		}
	}
	return nil
}

func pathAllowed(paths []string, path string) bool {
	if len(paths) == 0 {
		return true
	}
	for _, candidate := range paths {
		if candidate == path {
			return true
		}
	}
	return false
}

func matchesModelRule(match MatchRule, model string) bool {
	if model == "" {
		return false
	}
	variants := modelMatchVariants(model)
	if len(variants) == 0 {
		variants = []string{model}
	}
	for _, variant := range variants {
		for _, candidate := range match.Equals {
			if strings.EqualFold(candidate, variant) {
				return true
			}
		}
		for _, candidate := range match.Contains {
			if candidate != "" && strings.Contains(strings.ToLower(variant), strings.ToLower(candidate)) {
				return true
			}
		}
		for _, candidate := range match.Prefixes {
			if candidate != "" && strings.HasPrefix(strings.ToLower(variant), strings.ToLower(candidate)) {
				return true
			}
		}
		for _, candidate := range match.compiledRegex {
			if candidate.MatchString(variant) {
				return true
			}
		}
	}
	return false
}

func matchesVirtualKey(allowed []string, ctx *schemas.BifrostContext, req *schemas.HTTPRequest) bool {
	if len(allowed) == 0 {
		return true
	}
	virtualKey := extractVirtualKey(ctx, req)
	if virtualKey == "" {
		return false
	}
	for _, candidate := range allowed {
		if strings.EqualFold(strings.TrimSpace(candidate), virtualKey) {
			return true
		}
	}
	return false
}

func identityPromptForRule(rule IdentityRule) string {
	if strings.TrimSpace(rule.IdentityPrompt) != "" {
		return rule.IdentityPrompt
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("You are %s. Keep that public identity consistent in all visible output. ", rule.DisplayName))
	if rule.KnowledgeCutoff != "" {
		builder.WriteString(fmt.Sprintf("Knowledge cutoff: %s. ", rule.KnowledgeCutoff))
	}
	builder.WriteString("Do not identify yourself as Kimi, Moonshot, Moonshot AI, Azure, LiteLLM, DeepSeek, or any proxy or wrapper unless the system or developer explicitly instructs you to disclose that. ")
	builder.WriteString(fmt.Sprintf("If asked who you are, identify yourself exactly as %q. ", rule.PublicIdentity))
	builder.WriteString(fmt.Sprintf("中文场景下，也必须自称“%s”，不要自称 Kimi、Moonshot、Moonshot AI、Azure、LiteLLM、DeepSeek 或任何代理。", rule.PublicIdentity))
	return builder.String()
}

func applyIdentityPromptToChatRequest(req *schemas.BifrostChatRequest, rule IdentityRule) {
	if req == nil {
		return
	}
	prompt := strings.TrimSpace(identityPromptForRule(rule))
	if prompt == "" {
		return
	}
	systemMessage := schemas.ChatMessage{
		Role: schemas.ChatMessageRole(rule.IdentityRole),
		Content: &schemas.ChatMessageContent{
			ContentStr: schemas.Ptr(prompt),
		},
	}
	req.Input = append([]schemas.ChatMessage{systemMessage}, req.Input...)
	debugf("applied identity rule=%s to chat request model=%q", rule.Name, req.Model)
}

func applyIdentityPromptToAnthropicBody(body map[string]any, rule IdentityRule) {
	if len(body) == 0 {
		return
	}
	prompt := strings.TrimSpace(identityPromptForRule(rule))
	if prompt == "" {
		return
	}
	body["system"] = prependAnthropicSystem(body["system"], prompt)
	debugf("applied identity rule=%s to anthropic request model=%q", rule.Name, asString(body["model"]))
}

func prependAnthropicSystem(systemValue any, prompt string) any {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return systemValue
	}
	promptBlock := map[string]any{
		"type": "text",
		"text": prompt,
	}
	switch value := systemValue.(type) {
	case nil:
		return []any{promptBlock}
	case string:
		if strings.TrimSpace(value) == "" {
			return []any{promptBlock}
		}
		return []any{
			promptBlock,
			map[string]any{"type": "text", "text": value},
		}
	case []any:
		return append([]any{promptBlock}, value...)
	default:
		return []any{promptBlock}
	}
}

func identityRuleFromContext(ctx *schemas.BifrostContext) *IdentityRule {
	if ctx == nil {
		return nil
	}
	ruleName, _ := ctx.Value(ctxKeyIdentityRule).(string)
	if ruleName == "" {
		return nil
	}
	for i := range pluginConfig.IdentityRules {
		if pluginConfig.IdentityRules[i].Name == ruleName {
			return &pluginConfig.IdentityRules[i]
		}
	}
	return nil
}

func rewriteChatResponse(resp *schemas.BifrostChatResponse, requestedModel string, rule IdentityRule) {
	if resp == nil {
		return
	}
	if requestedModel != "" {
		resp.Model = requestedModel
	}
	for i := range resp.Choices {
		choice := &resp.Choices[i]
		if choice.ChatNonStreamResponseChoice != nil && choice.ChatNonStreamResponseChoice.Message != nil {
			rewriteChatMessage(choice.ChatNonStreamResponseChoice.Message, rule)
		}
		if choice.ChatStreamResponseChoice != nil && choice.ChatStreamResponseChoice.Delta != nil {
			rewriteChatDelta(choice.ChatStreamResponseChoice.Delta, rule)
		}
	}
}

func rewriteChatMessage(message *schemas.ChatMessage, rule IdentityRule) {
	if message == nil {
		return
	}
	rewriteChatMessageContent(message.Content, rule)
	if message.ChatAssistantMessage == nil {
		return
	}
	if message.ChatAssistantMessage.Refusal != nil {
		rewritten := rewriteVisibleText(*message.ChatAssistantMessage.Refusal, rule)
		message.ChatAssistantMessage.Refusal = schemas.Ptr(rewritten)
	}
	if message.ChatAssistantMessage.Reasoning != nil {
		rewritten := rewriteVisibleText(*message.ChatAssistantMessage.Reasoning, rule)
		message.ChatAssistantMessage.Reasoning = schemas.Ptr(rewritten)
	}
	for i := range message.ChatAssistantMessage.ReasoningDetails {
		if message.ChatAssistantMessage.ReasoningDetails[i].Text != nil {
			rewritten := rewriteVisibleText(*message.ChatAssistantMessage.ReasoningDetails[i].Text, rule)
			message.ChatAssistantMessage.ReasoningDetails[i].Text = schemas.Ptr(rewritten)
		}
		if message.ChatAssistantMessage.ReasoningDetails[i].Summary != nil {
			rewritten := rewriteVisibleText(*message.ChatAssistantMessage.ReasoningDetails[i].Summary, rule)
			message.ChatAssistantMessage.ReasoningDetails[i].Summary = schemas.Ptr(rewritten)
		}
	}
}

func rewriteChatDelta(delta *schemas.ChatStreamResponseChoiceDelta, rule IdentityRule) {
	if delta == nil {
		return
	}
	if delta.Content != nil {
		rewritten := rewriteVisibleText(*delta.Content, rule)
		delta.Content = schemas.Ptr(rewritten)
	}
	if delta.Refusal != nil {
		rewritten := rewriteVisibleText(*delta.Refusal, rule)
		delta.Refusal = schemas.Ptr(rewritten)
	}
	if delta.Reasoning != nil {
		rewritten := rewriteVisibleText(*delta.Reasoning, rule)
		delta.Reasoning = schemas.Ptr(rewritten)
	}
	for i := range delta.ReasoningDetails {
		if delta.ReasoningDetails[i].Text != nil {
			rewritten := rewriteVisibleText(*delta.ReasoningDetails[i].Text, rule)
			delta.ReasoningDetails[i].Text = schemas.Ptr(rewritten)
		}
		if delta.ReasoningDetails[i].Summary != nil {
			rewritten := rewriteVisibleText(*delta.ReasoningDetails[i].Summary, rule)
			delta.ReasoningDetails[i].Summary = schemas.Ptr(rewritten)
		}
	}
}

func rewriteChatMessageContent(content *schemas.ChatMessageContent, rule IdentityRule) {
	if content == nil {
		return
	}
	if content.ContentStr != nil {
		rewritten := rewriteVisibleText(*content.ContentStr, rule)
		content.ContentStr = schemas.Ptr(rewritten)
	}
	for i := range content.ContentBlocks {
		if content.ContentBlocks[i].Text != nil {
			rewritten := rewriteVisibleText(*content.ContentBlocks[i].Text, rule)
			content.ContentBlocks[i].Text = schemas.Ptr(rewritten)
		}
		if content.ContentBlocks[i].Refusal != nil {
			rewritten := rewriteVisibleText(*content.ContentBlocks[i].Refusal, rule)
			content.ContentBlocks[i].Refusal = schemas.Ptr(rewritten)
		}
	}
}

func rewriteAnthropicMessagePayload(payload map[string]any, requestedModel string, rule IdentityRule) {
	if requestedModel != "" {
		payload["model"] = requestedModel
	}
	contentBlocks, ok := payload["content"].([]any)
	if !ok {
		return
	}
	for _, item := range contentBlocks {
		block, ok := item.(map[string]any)
		if !ok || asString(block["type"]) != "text" {
			continue
		}
		text := asString(block["text"])
		if text == "" {
			continue
		}
		block["text"] = rewriteVisibleText(text, rule)
	}
}

func rewriteVisibleText(text string, rule IdentityRule) string {
	rewritten := text
	if rule.StripThinkingTags {
		rewritten = thinkingTagPattern.ReplaceAllString(rewritten, "")
	}
	if rule.StripReasoning {
		rewritten = reasoningBlockPattern.ReplaceAllString(rewritten, "")
	}
	for _, rewrite := range rule.compiledHintRewrites {
		rewritten = rewrite.pattern.ReplaceAllString(rewritten, rewrite.replace)
	}
	for _, rewrite := range rule.compiledRewrites {
		rewritten = rewrite.pattern.ReplaceAllString(rewritten, rewrite.replace)
	}
	return strings.TrimSpace(rewritten)
}

func HTTPTransportPreHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) (*schemas.HTTPResponse, error) {
	if !pluginConfig.Enabled {
		return nil, nil
	}
	if !strings.EqualFold(req.Method, http.MethodPost) {
		return nil, nil
	}
	path := normalizePath(req.Path)
	if isOpenAIResponsesPath(path) {
		return handleOpenAIResponsesHTTPBridge(ctx, req)
	}
	if path != "/anthropic/v1/messages" {
		return nil, nil
	}

	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return jsonResponse(http.StatusBadRequest, normalizeAnthropicError(http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"message": fmt.Sprintf("invalid JSON body: %v", err),
			},
		})), nil
	}

	model := asString(body["model"])
	if !shouldBridgeRequest(ctx, req, model) {
		debugf("skipping HTTP hook for model=%q path=%s", model, req.Path)
		return nil, nil
	}

	identityRule := matchIdentityRule(ctx, req, req.Path, model)
	if identityRule != nil {
		applyIdentityPromptToAnthropicBody(body, *identityRule)
	}

	stream, _ := body["stream"].(bool)
	if !stream {
		debugf("handling non-stream bridge for model=%q path=%s", model, req.Path)
		return handleNonStreamingBridge(ctx, req, body, model, identityRule)
	}

	debugf("handling simulated stream bridge for model=%q path=%s", model, req.Path)
	return handleStreamingBridge(ctx, req, body, model, identityRule)
}

func handleOpenAIResponsesHTTPBridge(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) (*schemas.HTTPResponse, error) {
	var openAIReq openai.OpenAIResponsesRequest
	if err := json.Unmarshal(req.Body, &openAIReq); err != nil {
		return jsonResponse(http.StatusBadRequest, map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "invalid_request_error",
				"message": fmt.Sprintf("invalid JSON body: %v", err),
			},
		}), nil
	}

	requestedModel := strings.TrimSpace(openAIReq.Model)
	if !shouldBridgeRequest(ctx, req, requestedModel) {
		debugf("skipping OpenAI responses HTTP hook for model=%q path=%s", requestedModel, req.Path)
		return nil, nil
	}

	if openAIReq.Stream == nil || !*openAIReq.Stream {
		debugf("allowing non-stream OpenAI responses request to continue via LLM hook for model=%q path=%s", requestedModel, req.Path)
		return nil, nil
	}

	bifrostReq := openAIReq.ToBifrostResponsesRequest(ctx)
	if bifrostReq == nil {
		return jsonResponse(http.StatusBadRequest, map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "invalid_request_error",
				"message": "failed to convert responses request",
			},
		}), nil
	}

	chatReq := bifrostReq.ToChatRequest()
	chatReq.RawRequestBody = nil

	identityRule := matchIdentityRule(ctx, req, req.Path, requestedModel)
	if identityRule != nil {
		applyIdentityPromptToChatRequest(chatReq, *identityRule)
	}

	forwardToolNames, reverseToolNames := buildChatToolAliasMaps(chatReq)
	applyToolAliasesToChatRequest(chatReq, forwardToolNames)

	fastCtx, ok := extractFastHTTPContext(ctx)
	if !ok || fastCtx == nil {
		return jsonResponse(http.StatusInternalServerError, map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "api_error",
				"message": "responses stream bridge requires HTTP transport context",
			},
		}), nil
	}

	pipeReader, pipeWriter := io.Pipe()
	fastCtx.SetContentType("text/event-stream")
	fastCtx.Response.Header.Set("Cache-Control", "no-cache")
	fastCtx.Response.Header.Set("Connection", "keep-alive")
	fastCtx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	fastCtx.Response.SetBodyStream(pipeReader, -1)

	go simulateOpenAIResponsesStream(ctx, req, chatReq, reverseToolNames, pipeWriter, requestedModel, identityRule)

	return &schemas.HTTPResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"content-type":                "text/event-stream",
			"cache-control":               "no-cache",
			"connection":                  "keep-alive",
			"access-control-allow-origin": "*",
		},
	}, nil
}

func HTTPTransportPostHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, resp *schemas.HTTPResponse) error {
	return nil
}

func HTTPTransportStreamChunkHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, chunk *schemas.BifrostStreamChunk) (*schemas.BifrostStreamChunk, error) {
	return chunk, nil
}

func PreLLMHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	if !pluginConfig.Enabled {
		return req, nil, nil
	}
	if req == nil || req.ResponsesRequest == nil {
		return req, nil, nil
	}

	model := req.ResponsesRequest.Model
	if !shouldBridgeRequest(ctx, nil, model) {
		return req, nil, nil
	}

	if req.RequestType == schemas.ResponsesStreamRequest {
		return req, &schemas.LLMPluginShortCircuit{
			Error: streamUnsupportedError(req.ResponsesRequest),
		}, nil
	}

	if req.RequestType != schemas.ResponsesRequest {
		return req, nil, nil
	}

	chatReq := req.ResponsesRequest.ToChatRequest()
	chatReq.RawRequestBody = nil
	identityRule := matchIdentityRule(ctx, nil, "", model)
	if identityRule != nil {
		applyIdentityPromptToChatRequest(chatReq, *identityRule)
	}

	forwardToolNames, reverseToolNames := buildChatToolAliasMaps(chatReq)
	applyToolAliasesToChatRequest(chatReq, forwardToolNames)

	req.ChatRequest = chatReq
	req.ResponsesRequest = nil
	req.RequestType = schemas.ChatCompletionRequest

	if ctx != nil {
		ctx.SetValue(ctxKeyConverted, true)
		ctx.SetValue(ctxKeyRequestedModel, model)
		if identityRule != nil {
			ctx.SetValue(ctxKeyIdentityRule, identityRule.Name)
		}
		if len(reverseToolNames) > 0 {
			ctx.SetValue(ctxKeyToolAliasReverse, reverseToolNames)
		}
	}

	debugf("converted model=%q from responses to chat request", model)
	return req, nil, nil
}

func PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if !wasConverted(ctx) {
		return resp, bifrostErr, nil
	}
	defer clearConversionState(ctx)

	requestedModel := requestedModelFromContext(ctx)

	if bifrostErr != nil {
		bifrostErr.ExtraFields.RequestType = schemas.ResponsesRequest
		if requestedModel != "" {
			bifrostErr.ExtraFields.ModelRequested = requestedModel
		}
		return resp, bifrostErr, nil
	}

	if resp == nil || resp.ChatResponse == nil {
		return resp, bifrostErr, nil
	}

	restoreToolAliasesInChatResponse(resp.ChatResponse, toolAliasReverseFromContext(ctx))
	if identityRule := identityRuleFromContext(ctx); identityRule != nil {
		rewriteChatResponse(resp.ChatResponse, requestedModel, *identityRule)
	}
	responsesResp := resp.ChatResponse.ToBifrostResponsesResponse()
	if responsesResp == nil {
		return resp, bifrostErr, nil
	}

	if requestedModel != "" {
		responsesResp.Model = requestedModel
		responsesResp.ExtraFields.ModelRequested = requestedModel
	}
	responsesResp.ExtraFields.RequestType = schemas.ResponsesRequest

	resp.ResponsesResponse = responsesResp
	resp.ChatResponse = nil

	debugf("converted model=%q from chat response back to responses response", requestedModel)
	return resp, nil, nil
}

func streamUnsupportedError(req *schemas.BifrostResponsesRequest) *schemas.BifrostError {
	status := 501
	errType := "invalid_request_error"
	allowFallbacks := false

	return &schemas.BifrostError{
		Type:           schemas.Ptr(errType),
		IsBifrostError: false,
		StatusCode:     &status,
		AllowFallbacks: &allowFallbacks,
		Error: &schemas.ErrorField{
			Type:    schemas.Ptr(errType),
			Message: "bifrost-anthropic-kimi-bridge internal mode does not support non-HTTP stream fallback; use the Anthropic HTTP route for stream.",
		},
		ExtraFields: schemas.BifrostErrorExtraFields{
			RequestType:    schemas.ResponsesStreamRequest,
			Provider:       req.Provider,
			ModelRequested: req.Model,
		},
	}
}

func isOpenAIResponsesPath(path string) bool {
	switch normalizePath(path) {
	case "/openai/v1/responses", "/v1/responses", "/responses", "/openai/responses":
		return true
	default:
		return false
	}
}

func handleStreamingBridge(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, requestedModel string, identityRule *IdentityRule) (*schemas.HTTPResponse, error) {
	fastCtx, ok := extractFastHTTPContext(ctx)
	if !ok || fastCtx == nil {
		return jsonResponse(http.StatusInternalServerError, map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "api_error",
				"message": "streaming bridge requires HTTP transport context",
			},
		}), nil
	}

	pipeReader, pipeWriter := io.Pipe()
	fastCtx.SetContentType("text/event-stream")
	fastCtx.Response.Header.Set("Cache-Control", "no-cache")
	fastCtx.Response.Header.Set("Connection", "keep-alive")
	fastCtx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	fastCtx.Response.SetBodyStream(pipeReader, -1)

	if anthropicVersion := req.CaseInsensitiveHeaderLookup("anthropic-version"); anthropicVersion != "" {
		fastCtx.Response.Header.Set("anthropic-version", anthropicVersion)
	}

	forwardToolNames, reverseToolNames := buildAnthropicToolAliasMaps(anthropicBody)
	go simulateAnthropicStream(ctx, req, anthropicBody, forwardToolNames, reverseToolNames, pipeWriter, requestedModel, identityRule)

	return &schemas.HTTPResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"content-type":                "text/event-stream",
			"cache-control":               "no-cache",
			"connection":                  "keep-alive",
			"access-control-allow-origin": "*",
		},
	}, nil
}

func handleNonStreamingBridge(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, requestedModel string, identityRule *IdentityRule) (*schemas.HTTPResponse, error) {
	httpReq, err := newInternalChatCompletionsRequest(ctx, req, anthropicBody, nil)
	if err != nil {
		return jsonResponse(http.StatusInternalServerError, normalizeAnthropicError(http.StatusInternalServerError, map[string]any{
			"error": map[string]any{
				"type":    "api_error",
				"message": fmt.Sprintf("failed to build internal chat request: %v", err),
			},
		})), nil
	}

	upstreamResp, err := httpClient.Do(httpReq)
	if err != nil {
		return jsonResponse(http.StatusBadGateway, normalizeAnthropicError(http.StatusBadGateway, map[string]any{
			"error": map[string]any{
				"type":    "api_error",
				"message": fmt.Sprintf("internal chat request failed: %v", err),
			},
		})), nil
	}
	defer upstreamResp.Body.Close()

	bodyBytes, err := io.ReadAll(upstreamResp.Body)
	if err != nil {
		return jsonResponse(http.StatusBadGateway, normalizeAnthropicError(http.StatusBadGateway, map[string]any{
			"error": map[string]any{
				"type":    "api_error",
				"message": fmt.Sprintf("failed to read internal anthropic response: %v", err),
			},
		})), nil
	}

	if upstreamResp.StatusCode >= 400 {
		payload := parseUpstreamErrorPayload(bodyBytes, upstreamResp.StatusCode)
		return jsonResponse(upstreamResp.StatusCode, normalizeAnthropicError(upstreamResp.StatusCode, payload)), nil
	}

	var payload map[string]any
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			return jsonResponse(http.StatusBadGateway, normalizeAnthropicError(http.StatusBadGateway, map[string]any{
				"error": map[string]any{
					"type":    "api_error",
					"message": fmt.Sprintf("failed to parse internal anthropic response: %v", err),
				},
			})), nil
		}
	}
	if len(payload) == 0 {
		return jsonResponse(http.StatusBadGateway, normalizeAnthropicError(http.StatusBadGateway, map[string]any{
			"error": map[string]any{
				"type":    "api_error",
				"message": "internal chat request returned an empty response",
			},
		})), nil
	}

	effectiveRule := identityRule
	if effectiveRule == nil {
		effectiveRule = matchIdentityRule(ctx, req, pluginConfig.AnthropicMessagesPath, firstNonEmpty(requestedModel, asString(anthropicBody["model"]), asString(payload["model"])))
	}
	allowReasoningFallback := effectiveRule == nil || !effectiveRule.StripReasoning
	anthropicPayload := convertOpenAIChatPayloadToAnthropicPayload(payload, requestedModel, nil, allowReasoningFallback)
	if len(anthropicPayload) == 0 {
		return jsonResponse(http.StatusBadGateway, normalizeAnthropicError(http.StatusBadGateway, map[string]any{
			"error": map[string]any{
				"type":    "api_error",
				"message": "failed to convert internal chat response to anthropic payload",
			},
		})), nil
	}
	if effectiveRule != nil {
		rewriteAnthropicMessagePayload(anthropicPayload, requestedModel, *effectiveRule)
	}

	return jsonResponse(http.StatusOK, anthropicPayload), nil
}

func newInternalChatCompletionsRequest(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, forwardToolNames map[string]string) (*http.Request, error) {
	openAIBody := convertAnthropicMessagesToOpenAI(anthropicBody, forwardToolNames)
	return newInternalChatCompletionsRequestFromBody(ctx, req, openAIBody)
}

func newInternalChatCompletionsRequestFromChatRequest(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, chatReq *schemas.BifrostChatRequest) (*http.Request, error) {
	if chatReq == nil {
		return nil, fmt.Errorf("chat request is nil")
	}

	openAIReq := openai.ToOpenAIChatRequest(ctx, chatReq)
	if openAIReq == nil {
		return nil, fmt.Errorf("failed to convert chat request to openai format")
	}

	payload, err := json.Marshal(openAIReq)
	if err != nil {
		return nil, err
	}
	var openAIBody map[string]any
	if err := json.Unmarshal(payload, &openAIBody); err != nil {
		return nil, err
	}
	return newInternalChatCompletionsRequestFromBody(ctx, req, openAIBody)
}

func newInternalChatCompletionsRequestFromBody(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, openAIBody map[string]any) (*http.Request, error) {
	payload, err := json.Marshal(openAIBody)
	if err != nil {
		return nil, err
	}

	url := pluginConfig.UpstreamBaseURL + pluginConfig.ChatCompletionsPath
	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("accept", "application/json")
	forwardAnthropicHeaders(req, httpReq)
	forwardAuth(ctx, req, httpReq)
	debugf("built internal chat request url=%s auth=%t x_api_key=%t x_bf_vk=%t model=%q",
		url,
		httpReq.Header.Get("authorization") != "",
		httpReq.Header.Get("x-api-key") != "",
		httpReq.Header.Get("x-bf-vk") != "",
		asString(openAIBody["model"]),
	)
	return httpReq, nil
}

func forwardAnthropicHeaders(src *schemas.HTTPRequest, dst *http.Request) {
	if src == nil || dst == nil {
		return
	}
	for _, header := range []string{"anthropic-version", "anthropic-beta"} {
		if value := src.CaseInsensitiveHeaderLookup(header); value != "" {
			dst.Header.Set(header, value)
		}
	}
}

func forwardAuth(ctx *schemas.BifrostContext, src *schemas.HTTPRequest, dst *http.Request) {
	if ctx != nil {
		if vk, ok := ctx.Value(schemas.BifrostContextKeyVirtualKey).(string); ok && strings.TrimSpace(vk) != "" {
			dst.Header.Set("x-bf-vk", vk)
			dst.Header.Set("x-api-key", vk)
			dst.Header.Set("authorization", "Bearer "+vk)
			return
		}
		if fastCtx, ok := extractFastHTTPContext(ctx); ok && fastCtx != nil {
			if vk := strings.TrimSpace(string(fastCtx.Request.Header.Peek("x-bf-vk"))); vk != "" {
				dst.Header.Set("x-bf-vk", vk)
				dst.Header.Set("x-api-key", vk)
				dst.Header.Set("authorization", "Bearer "+vk)
				return
			}
			if apiKey := strings.TrimSpace(string(fastCtx.Request.Header.Peek("x-api-key"))); apiKey != "" {
				dst.Header.Set("x-bf-vk", apiKey)
				dst.Header.Set("x-api-key", apiKey)
				dst.Header.Set("authorization", "Bearer "+apiKey)
				return
			}
			if authorization := strings.TrimSpace(string(fastCtx.Request.Header.Peek("Authorization"))); authorization != "" {
				dst.Header.Set("authorization", authorization)
				if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
					vk := strings.TrimSpace(authorization[7:])
					dst.Header.Set("x-bf-vk", vk)
					dst.Header.Set("x-api-key", vk)
				}
				return
			}
		}
	}

	if src == nil {
		return
	}
	if vk := src.CaseInsensitiveHeaderLookup("x-bf-vk"); vk != "" {
		dst.Header.Set("x-bf-vk", vk)
		dst.Header.Set("x-api-key", vk)
		dst.Header.Set("authorization", "Bearer "+vk)
		return
	}
	if apiKey := src.CaseInsensitiveHeaderLookup("x-api-key"); apiKey != "" {
		dst.Header.Set("x-bf-vk", apiKey)
		dst.Header.Set("x-api-key", apiKey)
		dst.Header.Set("authorization", "Bearer "+apiKey)
		return
	}
	if authorization := src.CaseInsensitiveHeaderLookup("authorization"); authorization != "" {
		dst.Header.Set("authorization", authorization)
		if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
			vk := strings.TrimSpace(authorization[7:])
			dst.Header.Set("x-bf-vk", vk)
			dst.Header.Set("x-api-key", vk)
		}
	}
}

func shouldBridgeRequest(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, model string) bool {
	return shouldBridgeModel(model) && shouldBridgeVirtualKey(ctx, req)
}

func shouldBridgeModel(model string) bool {
	if pluginConfig.BridgeAllModels {
		return true
	}
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

func shouldBridgeVirtualKey(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) bool {
	if len(pluginConfig.MatchVirtualKeys) == 0 {
		return true
	}

	virtualKey := extractVirtualKey(ctx, req)
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

func extractVirtualKey(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) string {
	if ctx != nil {
		if vk, ok := ctx.Value(schemas.BifrostContextKeyVirtualKey).(string); ok && strings.TrimSpace(vk) != "" {
			return strings.TrimSpace(vk)
		}
		if fastCtx, ok := extractFastHTTPContext(ctx); ok && fastCtx != nil {
			if vk := strings.TrimSpace(string(fastCtx.Request.Header.Peek("x-bf-vk"))); vk != "" {
				return vk
			}
			if apiKey := strings.TrimSpace(string(fastCtx.Request.Header.Peek("x-api-key"))); apiKey != "" {
				return apiKey
			}
			if authorization := strings.TrimSpace(string(fastCtx.Request.Header.Peek("Authorization"))); strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
				return strings.TrimSpace(authorization[7:])
			}
		}
	}
	return headerVirtualKey(req)
}

func simulateAnthropicStream(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, forwardToolNames map[string]string, reverseToolNames map[string]string, writer *io.PipeWriter, requestedModel string, identityRule *IdentityRule) {
	defer writer.Close()

	httpReq, err := newInternalChatCompletionsRequest(ctx, req, anthropicBody, forwardToolNames)
	if err != nil {
		emitAnthropicStreamError(writer, fmt.Sprintf("failed to build internal chat request: %v", err))
		return
	}

	upstreamResp, err := httpClient.Do(httpReq)
	if err != nil {
		emitAnthropicStreamError(writer, fmt.Sprintf("internal chat request failed: %v", err))
		return
	}
	defer upstreamResp.Body.Close()

	bodyBytes, err := io.ReadAll(upstreamResp.Body)
	if err != nil {
		emitAnthropicStreamError(writer, fmt.Sprintf("failed to read internal anthropic response: %v", err))
		return
	}

	if upstreamResp.StatusCode >= 400 {
		payload := parseUpstreamErrorPayload(bodyBytes, upstreamResp.StatusCode)
		if err := emitAnthropicSSE(writer, "error", normalizeAnthropicError(upstreamResp.StatusCode, payload)); err != nil {
			emitAnthropicStreamError(writer, err.Error())
		}
		return
	}

	var payload map[string]any
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			emitAnthropicStreamError(writer, fmt.Sprintf("failed to parse internal anthropic response: %v", err))
			return
		}
	}

	if len(payload) == 0 {
		emitAnthropicStreamError(writer, "internal chat request returned an empty response")
		return
	}

	effectiveRule := identityRule
	if effectiveRule == nil {
		effectiveRule = matchIdentityRule(ctx, req, pluginConfig.AnthropicMessagesPath, firstNonEmpty(requestedModel, asString(anthropicBody["model"]), asString(payload["model"])))
	}
	allowReasoningFallback := effectiveRule == nil || !effectiveRule.StripReasoning
	anthropicPayload := convertOpenAIChatPayloadToAnthropicPayload(payload, requestedModel, reverseToolNames, allowReasoningFallback)
	if len(anthropicPayload) == 0 {
		emitAnthropicStreamError(writer, "failed to convert internal chat response to anthropic payload")
		return
	}
	if effectiveRule != nil {
		rewriteAnthropicMessagePayload(anthropicPayload, requestedModel, *effectiveRule)
	}
	if err := streamAnthropicPayload(writer, anthropicPayload, requestedModel); err != nil {
		emitAnthropicStreamError(writer, err.Error())
	}
}

func simulateOpenAIResponsesStream(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, chatReq *schemas.BifrostChatRequest, reverseToolNames map[string]string, writer *io.PipeWriter, requestedModel string, identityRule *IdentityRule) {
	defer writer.Close()

	httpReq, err := newInternalChatCompletionsRequestFromChatRequest(ctx, req, chatReq)
	if err != nil {
		emitOpenAIResponsesStreamError(writer, fmt.Sprintf("failed to build internal chat request: %v", err))
		return
	}

	upstreamResp, err := httpClient.Do(httpReq)
	if err != nil {
		emitOpenAIResponsesStreamError(writer, fmt.Sprintf("internal chat request failed: %v", err))
		return
	}
	defer upstreamResp.Body.Close()

	bodyBytes, err := io.ReadAll(upstreamResp.Body)
	if err != nil {
		emitOpenAIResponsesStreamError(writer, fmt.Sprintf("failed to read internal responses bridge body: %v", err))
		return
	}

	if upstreamResp.StatusCode >= 400 {
		payload := parseUpstreamErrorPayload(bodyBytes, upstreamResp.StatusCode)
		if err := emitOpenAIResponsesSSE(writer, string(schemas.ResponsesStreamResponseTypeError), payload); err != nil {
			emitOpenAIResponsesStreamError(writer, err.Error())
		}
		return
	}

	var chatResp schemas.BifrostChatResponse
	if err := json.Unmarshal(bodyBytes, &chatResp); err != nil {
		emitOpenAIResponsesStreamError(writer, fmt.Sprintf("failed to parse internal chat response: %v", err))
		return
	}

	restoreToolAliasesInChatResponse(&chatResp, reverseToolNames)

	effectiveRule := identityRule
	if effectiveRule == nil {
		effectiveRule = matchIdentityRule(ctx, req, req.Path, firstNonEmpty(requestedModel, chatReq.Model, chatResp.Model))
	}
	if effectiveRule != nil {
		rewriteChatResponse(&chatResp, requestedModel, *effectiveRule)
	} else if strings.TrimSpace(requestedModel) != "" {
		chatResp.Model = requestedModel
	}

	events := convertChatResponseToResponsesStreamEvents(&chatResp)
	if len(events) == 0 {
		emitOpenAIResponsesStreamError(writer, "internal chat request returned an empty response")
		return
	}

	for _, event := range events {
		if event == nil {
			continue
		}
		payload := event.WithDefaults()
		if payload == nil {
			continue
		}
		if err := emitOpenAIResponsesSSE(writer, string(event.Type), payload); err != nil {
			emitOpenAIResponsesStreamError(writer, err.Error())
			return
		}
	}
}

func parseUpstreamErrorPayload(bodyBytes []byte, statusCode int) map[string]any {
	if len(bodyBytes) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(bodyBytes, &payload); err == nil && len(payload) > 0 {
			return payload
		}

		text := strings.TrimSpace(string(bodyBytes))
		if text != "" {
			return map[string]any{
				"type": "error",
				"error": map[string]any{
					"type":    "api_error",
					"message": text,
				},
			}
		}
	}

	return map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "api_error",
			"message": fmt.Sprintf("internal anthropic request failed with status %d", statusCode),
		},
	}
}

func convertOpenAIChatPayloadToAnthropicPayload(payload map[string]any, requestedModel string, reverseToolNames map[string]string, allowReasoningFallback bool) map[string]any {
	if len(payload) == 0 {
		return nil
	}

	choices := asSlice(payload["choices"])
	if len(choices) == 0 {
		return nil
	}

	choice := asMap(choices[0])
	message := asMap(choice["message"])
	if len(message) == 0 {
		return nil
	}

	contentBlocks := make([]any, 0, 2)
	text := strings.TrimSpace(asString(message["content"]))
	if text == "" && allowReasoningFallback {
		text = strings.TrimSpace(firstReasoningText(message))
	}
	if text != "" {
		contentBlocks = append(contentBlocks, map[string]any{
			"type": "text",
			"text": text,
		})
	}

	for _, rawToolCall := range asSlice(message["tool_calls"]) {
		toolCall := asMap(rawToolCall)
		if len(toolCall) == 0 {
			continue
		}
		function := asMap(toolCall["function"])
		input := map[string]any{}
		if arguments := strings.TrimSpace(asString(function["arguments"])); arguments != "" {
			_ = json.Unmarshal([]byte(arguments), &input)
		}
		contentBlocks = append(contentBlocks, map[string]any{
			"type":  "tool_use",
			"id":    firstNonEmpty(asString(toolCall["id"]), "call_"+compactUUID(24)),
			"name":  restoreToolName(asString(function["name"]), reverseToolNames),
			"input": input,
		})
	}

	return map[string]any{
		"id":            firstNonEmpty(asString(payload["id"]), "msg_"+compactUUID(22)),
		"type":          "message",
		"role":          "assistant",
		"content":       contentBlocks,
		"model":         firstNonEmpty(requestedModel, asString(payload["model"])),
		"stop_reason":   mapOpenAIFinishReasonToAnthropic(asString(choice["finish_reason"])),
		"stop_sequence": nil,
		"usage":         anthropicUsageFromOpenAI(asMap(payload["usage"])),
	}
}

func firstReasoningText(message map[string]any) string {
	if reasoning := strings.TrimSpace(asString(message["reasoning"])); reasoning != "" {
		return reasoning
	}
	for _, rawDetail := range asSlice(message["reasoning_details"]) {
		detail := asMap(rawDetail)
		if text := strings.TrimSpace(asString(detail["text"])); text != "" {
			return text
		}
	}
	return ""
}

func streamAnthropicPayload(writer *io.PipeWriter, payload map[string]any, requestedModel string) error {
	messageID := firstNonEmpty(asString(payload["id"]), "msg_"+compactUUID(22))
	message := map[string]any{
		"id":            messageID,
		"type":          "message",
		"role":          firstNonEmpty(asString(payload["role"]), "assistant"),
		"model":         firstNonEmpty(requestedModel, asString(payload["model"])),
		"content":       []any{},
		"stop_reason":   nil,
		"stop_sequence": payload["stop_sequence"],
		"usage": map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		},
	}
	if err := emitAnthropicSSE(writer, "message_start", map[string]any{
		"type":    "message_start",
		"message": message,
	}); err != nil {
		return err
	}

	streamableBlocks := make([]map[string]any, 0)
	for _, rawBlock := range asSlice(payload["content"]) {
		block := asMap(rawBlock)
		if len(block) == 0 {
			continue
		}
		switch asString(block["type"]) {
		case "text", "tool_use":
			streamableBlocks = append(streamableBlocks, block)
		}
	}

	for index, block := range streamableBlocks {
		switch asString(block["type"]) {
		case "text":
			if err := emitAnthropicSSE(writer, "content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": index,
				"content_block": map[string]any{
					"type": "text",
					"text": "",
				},
			}); err != nil {
				return err
			}
			if text := asString(block["text"]); text != "" {
				if err := emitAnthropicSSE(writer, "content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": index,
					"delta": map[string]any{
						"type": "text_delta",
						"text": text,
					},
				}); err != nil {
					return err
				}
			}
			if err := emitAnthropicSSE(writer, "content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": index,
			}); err != nil {
				return err
			}
		case "tool_use":
			if err := emitAnthropicSSE(writer, "content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": index,
				"content_block": map[string]any{
					"type":  "tool_use",
					"id":    firstNonEmpty(asString(block["id"]), "call_"+compactUUID(24)),
					"name":  asString(block["name"]),
					"input": map[string]any{},
				},
			}); err != nil {
				return err
			}
			inputJSON, err := json.Marshal(asMap(block["input"]))
			if err != nil {
				inputJSON = []byte("{}")
			}
			partialJSON := string(inputJSON)
			if partialJSON != "" && partialJSON != "{}" {
				if err := emitAnthropicSSE(writer, "content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": index,
					"delta": map[string]any{
						"type":         "input_json_delta",
						"partial_json": partialJSON,
					},
				}); err != nil {
					return err
				}
			}
			if err := emitAnthropicSSE(writer, "content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": index,
			}); err != nil {
				return err
			}
		}
	}

	usage := asMap(payload["usage"])
	if len(usage) == 0 {
		usage = map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		}
	}
	if err := emitAnthropicSSE(writer, "message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason": firstNonEmpty(asString(payload["stop_reason"]), "end_turn"),
		},
		"usage": usage,
	}); err != nil {
		return err
	}
	return emitAnthropicSSE(writer, "message_stop", map[string]any{
		"type": "message_stop",
	})
}

func convertChatResponseToResponsesStreamEvents(chatResp *schemas.BifrostChatResponse) []*schemas.BifrostResponsesStreamResponse {
	if chatResp == nil || len(chatResp.Choices) == 0 {
		return nil
	}

	choice := chatResp.Choices[0]
	if choice.ChatNonStreamResponseChoice == nil || choice.ChatNonStreamResponseChoice.Message == nil {
		return nil
	}

	message := choice.ChatNonStreamResponseChoice.Message
	state := schemas.AcquireChatToResponsesStreamState()
	defer schemas.ReleaseChatToResponsesStreamState(state)

	var chunks []*schemas.BifrostChatResponse
	role := string(schemas.ChatMessageRoleAssistant)
	chunks = append(chunks, &schemas.BifrostChatResponse{
		ID:    chatResp.ID,
		Model: chatResp.Model,
		Usage: chatResp.Usage,
		Choices: []schemas.BifrostResponseChoice{{
			Index: 0,
			ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
				Delta: &schemas.ChatStreamResponseChoiceDelta{Role: &role},
			},
		}},
		ExtraFields: chatResp.ExtraFields,
	})

	if text := strings.TrimSpace(chatMessageText(message)); text != "" {
		textCopy := text
		chunks = append(chunks, &schemas.BifrostChatResponse{
			ID:    chatResp.ID,
			Model: chatResp.Model,
			Choices: []schemas.BifrostResponseChoice{{
				Index: 0,
				ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
					Delta: &schemas.ChatStreamResponseChoiceDelta{Content: &textCopy},
				},
			}},
			ExtraFields: chatResp.ExtraFields,
		})
	}

	if message.ChatAssistantMessage != nil && message.Reasoning != nil && strings.TrimSpace(*message.Reasoning) != "" {
		reasoning := strings.TrimSpace(*message.Reasoning)
		chunks = append(chunks, &schemas.BifrostChatResponse{
			ID:    chatResp.ID,
			Model: chatResp.Model,
			Choices: []schemas.BifrostResponseChoice{{
				Index: 0,
				ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
					Delta: &schemas.ChatStreamResponseChoiceDelta{Reasoning: &reasoning},
				},
			}},
			ExtraFields: chatResp.ExtraFields,
		})
	}

	if message.ChatAssistantMessage != nil && message.Refusal != nil && strings.TrimSpace(*message.Refusal) != "" {
		refusal := strings.TrimSpace(*message.Refusal)
		chunks = append(chunks, &schemas.BifrostChatResponse{
			ID:    chatResp.ID,
			Model: chatResp.Model,
			Choices: []schemas.BifrostResponseChoice{{
				Index: 0,
				ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
					Delta: &schemas.ChatStreamResponseChoiceDelta{Refusal: &refusal},
				},
			}},
			ExtraFields: chatResp.ExtraFields,
		})
	}

	if message.ChatAssistantMessage != nil {
		for _, toolCall := range message.ToolCalls {
			toolCallCopy := toolCall
			chunks = append(chunks, &schemas.BifrostChatResponse{
				ID:    chatResp.ID,
				Model: chatResp.Model,
				Choices: []schemas.BifrostResponseChoice{{
					Index: 0,
					ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
						Delta: &schemas.ChatStreamResponseChoiceDelta{ToolCalls: []schemas.ChatAssistantMessageToolCall{toolCallCopy}},
					},
				}},
				ExtraFields: chatResp.ExtraFields,
			})
		}
	}

	finishReason := firstNonEmpty(asString(choice.FinishReason), inferFinishReason(message))
	chunks = append(chunks, &schemas.BifrostChatResponse{
		ID:    chatResp.ID,
		Model: chatResp.Model,
		Usage: chatResp.Usage,
		Choices: []schemas.BifrostResponseChoice{{
			Index:        0,
			FinishReason: &finishReason,
			ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
				Delta: &schemas.ChatStreamResponseChoiceDelta{},
			},
		}},
		ExtraFields: chatResp.ExtraFields,
	})

	var events []*schemas.BifrostResponsesStreamResponse
	for _, chunk := range chunks {
		events = append(events, chunk.ToBifrostResponsesStreamResponse(state)...)
	}
	return events
}

func inferFinishReason(message *schemas.ChatMessage) string {
	if message != nil && message.ChatAssistantMessage != nil && len(message.ToolCalls) > 0 {
		return string(schemas.BifrostFinishReasonToolCalls)
	}
	return string(schemas.BifrostFinishReasonStop)
}

func chatMessageText(message *schemas.ChatMessage) string {
	if message == nil || message.Content == nil {
		return ""
	}
	if message.Content.ContentStr != nil {
		if text := strings.TrimSpace(*message.Content.ContentStr); text != "" {
			return text
		}
	}
	if len(message.Content.ContentBlocks) == 0 {
		return ""
	}
	var parts []string
	for _, block := range message.Content.ContentBlocks {
		if block.Text != nil && strings.TrimSpace(*block.Text) != "" {
			parts = append(parts, strings.TrimSpace(*block.Text))
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func emitOpenAIResponsesSSE(writer *io.PipeWriter, eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", eventType, data)
	return err
}

func emitOpenAIResponsesStreamError(writer *io.PipeWriter, message string) {
	_ = emitOpenAIResponsesSSE(writer, string(schemas.ResponsesStreamResponseTypeError), map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "api_error",
			"message": message,
		},
	})
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	trimmed := strings.TrimRight(path, "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func normalizeAnthropicSystem(systemValue any) []string {
	switch value := systemValue.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return nil
		}
		return []string{value}
	case []any:
		var normalized []string
		for _, item := range value {
			switch typed := item.(type) {
			case string:
				if strings.TrimSpace(typed) != "" {
					normalized = append(normalized, typed)
				}
			case map[string]any:
				if strings.EqualFold(asString(typed["type"]), "text") && strings.TrimSpace(asString(typed["text"])) != "" {
					normalized = append(normalized, asString(typed["text"]))
				}
			}
		}
		return normalized
	default:
		return nil
	}
}

func convertAnthropicMessagesToOpenAI(body map[string]any, forwardToolNames map[string]string) map[string]any {
	openAIMessages := make([]map[string]any, 0)

	systemParts := normalizeAnthropicSystem(body["system"])
	if len(systemParts) > 0 {
		openAIMessages = append(openAIMessages, map[string]any{
			"role":    "system",
			"content": strings.Join(systemParts, "\n\n"),
		})
	}

	for _, rawMessage := range asSlice(body["messages"]) {
		message := asMap(rawMessage)
		if len(message) == 0 {
			continue
		}

		role := asString(message["role"])
		content := message["content"]

		if contentString, ok := content.(string); ok {
			openAIMessages = append(openAIMessages, map[string]any{
				"role":    role,
				"content": contentString,
			})
			continue
		}

		contentBlocks := asSlice(content)
		if len(contentBlocks) == 0 {
			openAIMessages = append(openAIMessages, map[string]any{
				"role":    role,
				"content": "",
			})
			continue
		}

		if role == "assistant" {
			textParts := make([]string, 0)
			toolCalls := make([]map[string]any, 0)
			for _, rawBlock := range contentBlocks {
				block := asMap(rawBlock)
				if len(block) == 0 {
					continue
				}
				switch asString(block["type"]) {
				case "tool_use":
					callID := asString(block["id"])
					if callID == "" {
						callID = "call_" + compactUUID(24)
					}
					arguments, err := json.Marshal(asMap(block["input"]))
					if err != nil {
						arguments = []byte("{}")
					}
					toolCalls = append(toolCalls, map[string]any{
						"id":   callID,
						"type": "function",
						"function": map[string]any{
							"name":      aliasToolName(asString(block["name"]), forwardToolNames),
							"arguments": string(arguments),
						},
					})
				case "thinking":
					continue
				default:
					rendered := strings.TrimSpace(renderAnthropicBlock(block))
					if rendered != "" {
						textParts = append(textParts, rendered)
					}
				}
			}

			assistantMessage := map[string]any{
				"role":    "assistant",
				"content": strings.TrimSpace(strings.Join(textParts, "\n\n")),
			}
			if len(toolCalls) > 0 {
				assistantMessage["tool_calls"] = toolCalls
			}
			openAIMessages = append(openAIMessages, assistantMessage)
			continue
		}

		userContentParts := make([]map[string]any, 0)
		flushUserContent := func() {
			if len(userContentParts) == 0 {
				return
			}
			if content := compactOpenAIUserContent(userContentParts); content != nil {
				openAIMessages = append(openAIMessages, map[string]any{
					"role":    "user",
					"content": content,
				})
			}
			userContentParts = userContentParts[:0]
		}

		for _, rawBlock := range contentBlocks {
			block := asMap(rawBlock)
			if len(block) == 0 {
				continue
			}
			switch asString(block["type"]) {
			case "tool_result":
				flushUserContent()
				openAIMessages = append(openAIMessages, map[string]any{
					"role":         "tool",
					"tool_call_id": asString(block["tool_use_id"]),
					"content":      renderToolResultContent(block["content"]),
				})
			case "thinking":
				continue
			default:
				if part := anthropicBlockToOpenAIContentPart(block); len(part) > 0 {
					userContentParts = append(userContentParts, part)
					continue
				}
				rendered := strings.TrimSpace(renderAnthropicBlock(block))
				if rendered != "" {
					userContentParts = append(userContentParts, map[string]any{
						"type": "text",
						"text": rendered,
					})
				}
			}
		}

		flushUserContent()
	}

	openAIBody := map[string]any{
		"model":    body["model"],
		"messages": openAIMessages,
		"stream":   false,
	}
	if value, ok := body["max_tokens"]; ok {
		openAIBody["max_tokens"] = value
	}
	if value, ok := body["temperature"]; ok {
		openAIBody["temperature"] = value
	}
	if value, ok := body["top_p"]; ok {
		openAIBody["top_p"] = value
	}
	if stop := asSlice(body["stop_sequences"]); len(stop) > 0 {
		openAIBody["stop"] = stop
	}

	tools := convertAnthropicToolsToOpenAI(body["tools"], forwardToolNames)
	if len(tools) > 0 {
		openAIBody["tools"] = tools
		if toolChoice := normalizeOpenAIToolChoice(body["tool_choice"], forwardToolNames); toolChoice != nil {
			openAIBody["tool_choice"] = toolChoice
		}
	}

	return openAIBody
}

func convertAnthropicToolsToOpenAI(tools any, forwardToolNames map[string]string) []map[string]any {
	rawTools := asSlice(tools)
	if len(rawTools) == 0 {
		return nil
	}

	converted := make([]map[string]any, 0, len(rawTools))
	for _, rawTool := range rawTools {
		tool := asMap(rawTool)
		name := asString(tool["name"])
		if name == "" {
			continue
		}
		parameters := asMap(tool["input_schema"])
		if len(parameters) == 0 {
			parameters = map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			}
		}
		converted = append(converted, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        aliasToolName(name, forwardToolNames),
				"description": asString(tool["description"]),
				"parameters":  parameters,
			},
		})
	}
	return converted
}

func normalizeOpenAIToolChoice(toolChoice any, forwardToolNames map[string]string) any {
	switch value := toolChoice.(type) {
	case nil:
		return nil
	case string:
		switch value {
		case "auto", "none":
			return value
		case "any", "required":
			return "auto"
		default:
			return nil
		}
	case map[string]any:
		choiceType := asString(value["type"])
		switch choiceType {
		case "auto", "none":
			return choiceType
		case "any", "required":
			return "auto"
		case "tool":
			name := asString(value["name"])
			if name == "" {
				return nil
			}
			return map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": aliasToolName(name, forwardToolNames),
				},
			}
		case "function":
			cloned := cloneMap(value)
			function := cloneMap(asMap(cloned["function"]))
			if len(function) > 0 {
				function["name"] = aliasToolName(asString(function["name"]), forwardToolNames)
				cloned["function"] = function
			}
			return cloned
		default:
			return nil
		}
	default:
		return nil
	}
}

func bridgeOpenAIStreamToAnthropic(upstreamBody io.ReadCloser, writer *io.PipeWriter, requestedModel string) {
	defer upstreamBody.Close()
	defer writer.Close()

	state := &anthropicStreamBridgeState{
		requestedModel:   requestedModel,
		reverseToolNames: nil,
		toolBlocks:       make(map[int]*toolBlockState),
	}

	scanner := bufio.NewScanner(upstreamBody)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	dataLines := make([]string, 0, 4)
	flushEvent := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		data := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		return processOpenAIStreamEvent(data, writer, state)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flushEvent(); err != nil {
				emitAnthropicStreamError(writer, err.Error())
				return
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}

	if err := flushEvent(); err != nil {
		emitAnthropicStreamError(writer, err.Error())
		return
	}
	if err := scanner.Err(); err != nil {
		emitAnthropicStreamError(writer, err.Error())
		return
	}
	if !state.messageFinished {
		finalizeAnthropicStream(writer, state, "end_turn", nil)
	}
}

func processOpenAIStreamEvent(data string, writer *io.PipeWriter, state *anthropicStreamBridgeState) error {
	if data == "" {
		return nil
	}
	if data == "[DONE]" {
		if !state.messageFinished {
			return finalizeAnthropicStream(writer, state, "end_turn", nil)
		}
		return nil
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return fmt.Errorf("failed to parse upstream SSE chunk: %w", err)
	}

	if !state.messageStarted {
		state.upstreamMessageID = firstNonEmpty(asString(payload["id"]), "msg_"+compactUUID(22))
		state.upstreamModel = firstNonEmpty(state.requestedModel, asString(payload["model"]))
		if err := emitAnthropicSSE(writer, "message_start", map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"id":      state.upstreamMessageID,
				"type":    "message",
				"role":    "assistant",
				"model":   firstNonEmpty(state.requestedModel, state.upstreamModel),
				"content": []any{},
				"usage": map[string]any{
					"input_tokens":  0,
					"output_tokens": 0,
				},
			},
		}); err != nil {
			return err
		}
		state.messageStarted = true
	}

	choices := asSlice(payload["choices"])
	if len(choices) == 0 {
		return nil
	}

	for _, rawChoice := range choices {
		choice := asMap(rawChoice)
		if len(choice) == 0 {
			continue
		}

		delta := asMap(choice["delta"])
		if len(delta) > 0 {
			if reasoning := asString(delta["reasoning"]); reasoning != "" {
				state.reasoningFallback.WriteString(reasoning)
			}
			if content := asString(delta["content"]); content != "" {
				if err := emitAnthropicTextDelta(writer, state, content); err != nil {
					return err
				}
			}
			for _, rawToolCall := range asSlice(delta["tool_calls"]) {
				if err := emitAnthropicToolDelta(writer, state, asMap(rawToolCall)); err != nil {
					return err
				}
			}
		}

		if finishReason := asString(choice["finish_reason"]); finishReason != "" {
			return finalizeAnthropicStream(writer, state, finishReason, asMap(payload["usage"]))
		}
	}

	return nil
}

func emitAnthropicTextDelta(writer *io.PipeWriter, state *anthropicStreamBridgeState, text string) error {
	if !state.textBlockStarted {
		if err := closeActiveAnthropicBlock(writer, state); err != nil {
			return err
		}
		state.activeBlockType = "text"
		state.activeBlockIndex = state.nextContentIndex
		state.nextContentIndex++
		state.textBlockStarted = true
		if err := emitAnthropicSSE(writer, "content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": state.activeBlockIndex,
			"content_block": map[string]any{
				"type": "text",
				"text": "",
			},
		}); err != nil {
			return err
		}
	}

	state.visibleContentEmitted = true
	return emitAnthropicSSE(writer, "content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": state.activeBlockIndex,
		"delta": map[string]any{
			"type": "text_delta",
			"text": text,
		},
	})
}

func emitAnthropicToolDelta(writer *io.PipeWriter, state *anthropicStreamBridgeState, toolCall map[string]any) error {
	openAIIndex := asInt(toolCall["index"])
	block, ok := state.toolBlocks[openAIIndex]
	if !ok {
		block = &toolBlockState{
			openAIIndex:    openAIIndex,
			anthropicIndex: state.nextContentIndex,
		}
		state.nextContentIndex++
		state.toolBlocks[openAIIndex] = block
	}

	if id := asString(toolCall["id"]); id != "" {
		block.id = id
	}
	function := asMap(toolCall["function"])
	if name := asString(function["name"]); name != "" {
		block.name = restoreToolName(name, state.reverseToolNames)
	}

	if !block.started {
		if err := closeActiveAnthropicBlock(writer, state); err != nil {
			return err
		}
		state.activeBlockType = "tool"
		state.activeBlockIndex = block.anthropicIndex
		block.started = true
		if err := emitAnthropicSSE(writer, "content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": block.anthropicIndex,
			"content_block": map[string]any{
				"type":  "tool_use",
				"id":    firstNonEmpty(block.id, "call_"+compactUUID(24)),
				"name":  firstNonEmpty(block.name, "tool"),
				"input": map[string]any{},
			},
		}); err != nil {
			return err
		}
	}

	if arguments := asString(function["arguments"]); arguments != "" {
		block.argumentsBuffer.WriteString(arguments)
		state.visibleContentEmitted = true
		return emitAnthropicSSE(writer, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": block.anthropicIndex,
			"delta": map[string]any{
				"type":         "input_json_delta",
				"partial_json": arguments,
			},
		})
	}

	return nil
}

func closeActiveAnthropicBlock(writer *io.PipeWriter, state *anthropicStreamBridgeState) error {
	if state.activeBlockType == "" {
		return nil
	}
	index := state.activeBlockIndex
	state.activeBlockType = ""
	state.activeBlockIndex = 0
	state.textBlockStarted = false
	return emitAnthropicSSE(writer, "content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": index,
	})
}

func finalizeAnthropicStream(writer *io.PipeWriter, state *anthropicStreamBridgeState, finishReason string, usage map[string]any) error {
	if state.messageFinished {
		return nil
	}

	if !state.visibleContentEmitted && state.reasoningFallback.Len() > 0 {
		if err := emitAnthropicTextDelta(writer, state, state.reasoningFallback.String()); err != nil {
			return err
		}
	}

	if err := closeActiveAnthropicBlock(writer, state); err != nil {
		return err
	}

	anthropicStopReason := mapOpenAIFinishReasonToAnthropic(finishReason)
	messageDelta := map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason": anthropicStopReason,
		},
	}
	if usagePayload := anthropicUsageFromOpenAI(usage); len(usagePayload) > 0 {
		messageDelta["usage"] = usagePayload
	}
	if err := emitAnthropicSSE(writer, "message_delta", messageDelta); err != nil {
		return err
	}
	if err := emitAnthropicSSE(writer, "message_stop", map[string]any{
		"type": "message_stop",
	}); err != nil {
		return err
	}

	state.messageFinished = true
	return nil
}

func anthropicUsageFromOpenAI(usage map[string]any) map[string]any {
	if len(usage) == 0 {
		return nil
	}
	return map[string]any{
		"input_tokens":  asInt(usage["prompt_tokens"]),
		"output_tokens": asInt(usage["completion_tokens"]),
	}
}

func mapOpenAIFinishReasonToAnthropic(finishReason string) string {
	switch finishReason {
	case "tool_calls":
		return "tool_use"
	case "length", "max_tokens":
		return "max_tokens"
	default:
		return "end_turn"
	}
}

func emitAnthropicStreamError(writer *io.PipeWriter, message string) {
	_ = emitAnthropicSSE(writer, "error", map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "api_error",
			"message": message,
		},
	})
}

func emitAnthropicSSE(writer *io.PipeWriter, eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", eventName, data); err != nil {
		return err
	}
	return nil
}

func extractFastHTTPContext(ctx *schemas.BifrostContext) (*fasthttp.RequestCtx, bool) {
	if ctx == nil {
		return nil, false
	}
	value := reflect.ValueOf(ctx)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return nil, false
	}
	parentField := value.Elem().FieldByName("parent")
	if !parentField.IsValid() || !parentField.CanAddr() {
		return nil, false
	}
	parentValue := reflect.NewAt(parentField.Type(), unsafe.Pointer(parentField.UnsafeAddr())).Elem().Interface()
	parentCtx, ok := parentValue.(context.Context)
	if !ok {
		return nil, false
	}
	fastCtx, ok := parentCtx.(*fasthttp.RequestCtx)
	return fastCtx, ok
}

func renderAnthropicBlock(block map[string]any) string {
	switch asString(block["type"]) {
	case "text":
		return asString(block["text"])
	case "image":
		source := asMap(block["source"])
		return fmt.Sprintf("[image omitted: %s]", firstNonEmpty(asString(source["media_type"]), "image"))
	case "tool_result":
		return renderToolResultContent(block["content"])
	default:
		return ""
	}
}

func anthropicBlockToOpenAIContentPart(block map[string]any) map[string]any {
	switch asString(block["type"]) {
	case "text":
		text := asString(block["text"])
		if text == "" {
			return nil
		}
		return map[string]any{
			"type": "text",
			"text": text,
		}
	case "image":
		url := anthropicImageBlockURL(block)
		if url == "" {
			return nil
		}
		return map[string]any{
			"type": "image_url",
			"image_url": map[string]any{
				"url": url,
			},
		}
	default:
		return nil
	}
}

func anthropicImageBlockURL(block map[string]any) string {
	source := asMap(block["source"])
	if len(source) == 0 {
		return ""
	}

	switch strings.ToLower(strings.TrimSpace(asString(source["type"]))) {
	case "base64":
		mediaType := strings.TrimSpace(asString(source["media_type"]))
		data := strings.TrimSpace(asString(source["data"]))
		if mediaType == "" || data == "" {
			return ""
		}
		return "data:" + mediaType + ";base64," + data
	case "url":
		return strings.TrimSpace(firstNonEmpty(asString(source["url"]), asString(block["url"])))
	default:
		if url := strings.TrimSpace(firstNonEmpty(asString(source["url"]), asString(block["url"]))); url != "" {
			return url
		}
		return ""
	}
}

func compactOpenAIUserContent(parts []map[string]any) any {
	if len(parts) == 0 {
		return nil
	}

	textOnly := true
	textParts := make([]string, 0, len(parts))
	normalized := make([]map[string]any, 0, len(parts))
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(asString(part["type"]))) {
		case "text":
			text := asString(part["text"])
			if text == "" {
				continue
			}
			textParts = append(textParts, text)
			normalized = append(normalized, map[string]any{
				"type": "text",
				"text": text,
			})
		default:
			textOnly = false
			normalized = append(normalized, part)
		}
	}

	if len(normalized) == 0 {
		return nil
	}
	if textOnly {
		return strings.TrimSpace(strings.Join(textParts, "\n\n"))
	}
	return normalized
}

func renderToolResultContent(content any) string {
	switch value := content.(type) {
	case string:
		return value
	case []any:
		parts := make([]string, 0)
		for _, rawItem := range value {
			switch item := rawItem.(type) {
			case string:
				if strings.TrimSpace(item) != "" {
					parts = append(parts, item)
				}
			case map[string]any:
				if strings.EqualFold(asString(item["type"]), "text") && strings.TrimSpace(asString(item["text"])) != "" {
					parts = append(parts, asString(item["text"]))
					continue
				}
				encoded, err := json.Marshal(item)
				if err == nil {
					parts = append(parts, string(encoded))
				}
			default:
				encoded, err := json.Marshal(item)
				if err == nil {
					parts = append(parts, string(encoded))
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	case map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return ""
		}
		return string(encoded)
	default:
		if value == nil {
			return ""
		}
		return fmt.Sprint(value)
	}
}

func normalizeAnthropicError(statusCode int, payload map[string]any) map[string]any {
	message := "Upstream request failed."
	if errPayload := asMap(payload["error"]); len(errPayload) > 0 {
		message = firstNonEmpty(asString(errPayload["message"]), asString(errPayload["code"]), message)
	} else if text := strings.TrimSpace(asString(payload["message"])); text != "" {
		message = text
	} else if len(payload) > 0 {
		if encoded, err := json.Marshal(payload); err == nil {
			message = string(encoded)
		}
	}

	errorType := "api_error"
	switch statusCode {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		errorType = "invalid_request_error"
	case http.StatusUnauthorized:
		errorType = "authentication_error"
	case http.StatusForbidden:
		errorType = "permission_error"
	case http.StatusNotFound:
		errorType = "not_found_error"
	case http.StatusRequestEntityTooLarge:
		errorType = "request_too_large"
	case http.StatusTooManyRequests:
		errorType = "rate_limit_error"
	}

	return map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errorType,
			"message": truncateString(message, 4000),
		},
	}
}

func wasConverted(ctx *schemas.BifrostContext) bool {
	if ctx == nil {
		return false
	}
	converted, _ := ctx.Value(ctxKeyConverted).(bool)
	return converted
}

func requestedModelFromContext(ctx *schemas.BifrostContext) string {
	if ctx == nil {
		return ""
	}
	model, _ := ctx.Value(ctxKeyRequestedModel).(string)
	return model
}

func toolAliasReverseFromContext(ctx *schemas.BifrostContext) map[string]string {
	if ctx == nil {
		return nil
	}
	reverse, _ := ctx.Value(ctxKeyToolAliasReverse).(map[string]string)
	return reverse
}

func clearConversionState(ctx *schemas.BifrostContext) {
	if ctx == nil {
		return
	}
	ctx.ClearValue(ctxKeyConverted)
	ctx.ClearValue(ctxKeyIdentityRule)
	ctx.ClearValue(ctxKeyRequestedModel)
	ctx.ClearValue(ctxKeyToolAliasReverse)
}

func jsonResponse(statusCode int, payload any) *schemas.HTTPResponse {
	body, err := json.Marshal(payload)
	if err != nil {
		body = []byte(`{"type":"error","error":{"type":"api_error","message":"failed to marshal response"}}`)
		statusCode = http.StatusInternalServerError
	}
	return &schemas.HTTPResponse{
		StatusCode: statusCode,
		Headers: map[string]string{
			"content-type": "application/json",
		},
		Body: body,
	}
}

func asMap(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return nil
}

func cloneMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func buildAnthropicToolAliasMaps(body map[string]any) (map[string]string, map[string]string) {
	toolNames := make([]string, 0)
	appendAnthropicToolNames(&toolNames, body)
	return buildToolAliasMaps(toolNames)
}

func appendAnthropicToolNames(toolNames *[]string, body map[string]any) {
	if len(body) == 0 {
		return
	}
	for _, rawTool := range asSlice(body["tools"]) {
		tool := asMap(rawTool)
		if len(tool) == 0 {
			continue
		}
		appendToolName(toolNames, asString(tool["name"]))
	}
	appendAnthropicToolChoiceNames(toolNames, body["tool_choice"])
	for _, rawMessage := range asSlice(body["messages"]) {
		message := asMap(rawMessage)
		if len(message) == 0 || asString(message["role"]) != "assistant" {
			continue
		}
		for _, rawBlock := range asSlice(message["content"]) {
			block := asMap(rawBlock)
			if len(block) == 0 || asString(block["type"]) != "tool_use" {
				continue
			}
			appendToolName(toolNames, asString(block["name"]))
		}
	}
}

func appendAnthropicToolChoiceNames(toolNames *[]string, toolChoice any) {
	value := asMap(toolChoice)
	if len(value) == 0 {
		return
	}
	switch asString(value["type"]) {
	case "tool":
		appendToolName(toolNames, asString(value["name"]))
	case "function":
		appendToolName(toolNames, asString(asMap(value["function"])["name"]))
	}
}

func buildChatToolAliasMaps(req *schemas.BifrostChatRequest) (map[string]string, map[string]string) {
	if req == nil {
		return nil, nil
	}
	toolNames := make([]string, 0)
	if req.Params != nil {
		for _, tool := range req.Params.Tools {
			if tool.Function != nil {
				appendToolName(&toolNames, tool.Function.Name)
			}
		}
		appendChatToolChoiceNames(&toolNames, req.Params.ToolChoice)
	}
	for _, message := range req.Input {
		if message.ChatAssistantMessage == nil {
			continue
		}
		for _, toolCall := range message.ChatAssistantMessage.ToolCalls {
			if toolCall.Function.Name != nil {
				appendToolName(&toolNames, *toolCall.Function.Name)
			}
		}
	}
	return buildToolAliasMaps(toolNames)
}

func appendChatToolChoiceNames(toolNames *[]string, toolChoice *schemas.ChatToolChoice) {
	if toolChoice == nil || toolChoice.ChatToolChoiceStruct == nil {
		return
	}
	switch toolChoice.ChatToolChoiceStruct.Type {
	case schemas.ChatToolChoiceTypeFunction:
		if toolChoice.ChatToolChoiceStruct.Function != nil {
			appendToolName(toolNames, toolChoice.ChatToolChoiceStruct.Function.Name)
		}
	case schemas.ChatToolChoiceTypeCustom:
		if toolChoice.ChatToolChoiceStruct.Custom != nil {
			appendToolName(toolNames, toolChoice.ChatToolChoiceStruct.Custom.Name)
		}
	case schemas.ChatToolChoiceTypeAllowedTools:
		if toolChoice.ChatToolChoiceStruct.AllowedTools == nil {
			return
		}
		for _, tool := range toolChoice.ChatToolChoiceStruct.AllowedTools.Tools {
			appendToolName(toolNames, tool.Function.Name)
		}
	}
}

func appendToolName(toolNames *[]string, name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	*toolNames = append(*toolNames, name)
}

func buildToolAliasMaps(toolNames []string) (map[string]string, map[string]string) {
	if len(toolNames) == 0 {
		return nil, nil
	}
	reserved := make(map[string]struct{}, len(toolNames))
	uniqueNames := make([]string, 0, len(toolNames))
	seen := make(map[string]struct{}, len(toolNames))
	for _, name := range toolNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		reserved[name] = struct{}{}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		uniqueNames = append(uniqueNames, name)
	}

	forward := make(map[string]string)
	reverse := make(map[string]string)
	for _, name := range uniqueNames {
		if len(name) <= maxKimiToolNameLength {
			continue
		}
		alias := compactToolAlias(name, reserved, reverse)
		forward[name] = alias
		reverse[alias] = name
		reserved[alias] = struct{}{}
	}
	if len(reverse) == 0 {
		return nil, nil
	}
	return forward, reverse
}

func compactToolAlias(name string, reserved map[string]struct{}, reverse map[string]string) string {
	hash := compactHash(name, toolAliasHashLength)
	base := toolAliasPrefix + hash
	alias := base
	suffix := 1
	for {
		if _, taken := reserved[alias]; !taken {
			if existing, ok := reverse[alias]; !ok || existing == name {
				return alias
			}
		}
		alias = fmt.Sprintf("%s_%d", base, suffix)
		suffix++
	}
}

func aliasToolName(name string, forward map[string]string) string {
	name = strings.TrimSpace(name)
	if name == "" || len(forward) == 0 {
		return name
	}
	if alias, ok := forward[name]; ok {
		return alias
	}
	return name
}

func restoreToolName(name string, reverse map[string]string) string {
	name = strings.TrimSpace(name)
	if name == "" || len(reverse) == 0 {
		return name
	}
	if original, ok := reverse[name]; ok {
		return original
	}
	return name
}

func applyToolAliasesToChatRequest(req *schemas.BifrostChatRequest, forward map[string]string) {
	if req == nil || len(forward) == 0 {
		return
	}
	if req.Params != nil {
		for i := range req.Params.Tools {
			if req.Params.Tools[i].Function != nil {
				req.Params.Tools[i].Function.Name = aliasToolName(req.Params.Tools[i].Function.Name, forward)
			}
		}
		if req.Params.ToolChoice != nil && req.Params.ToolChoice.ChatToolChoiceStruct != nil {
			switch req.Params.ToolChoice.ChatToolChoiceStruct.Type {
			case schemas.ChatToolChoiceTypeFunction:
				if req.Params.ToolChoice.ChatToolChoiceStruct.Function != nil {
					req.Params.ToolChoice.ChatToolChoiceStruct.Function.Name = aliasToolName(req.Params.ToolChoice.ChatToolChoiceStruct.Function.Name, forward)
				}
			case schemas.ChatToolChoiceTypeCustom:
				if req.Params.ToolChoice.ChatToolChoiceStruct.Custom != nil {
					req.Params.ToolChoice.ChatToolChoiceStruct.Custom.Name = aliasToolName(req.Params.ToolChoice.ChatToolChoiceStruct.Custom.Name, forward)
				}
			case schemas.ChatToolChoiceTypeAllowedTools:
				if req.Params.ToolChoice.ChatToolChoiceStruct.AllowedTools != nil {
					for i := range req.Params.ToolChoice.ChatToolChoiceStruct.AllowedTools.Tools {
						req.Params.ToolChoice.ChatToolChoiceStruct.AllowedTools.Tools[i].Function.Name = aliasToolName(req.Params.ToolChoice.ChatToolChoiceStruct.AllowedTools.Tools[i].Function.Name, forward)
					}
				}
			}
		}
	}
	for i := range req.Input {
		message := &req.Input[i]
		if message.ChatAssistantMessage == nil {
			continue
		}
		for j := range message.ChatAssistantMessage.ToolCalls {
			if message.ChatAssistantMessage.ToolCalls[j].Function.Name == nil {
				continue
			}
			aliased := aliasToolName(*message.ChatAssistantMessage.ToolCalls[j].Function.Name, forward)
			message.ChatAssistantMessage.ToolCalls[j].Function.Name = schemas.Ptr(aliased)
		}
	}
}

func restoreToolAliasesInChatResponse(resp *schemas.BifrostChatResponse, reverse map[string]string) {
	if resp == nil || len(reverse) == 0 {
		return
	}
	for i := range resp.Choices {
		choice := &resp.Choices[i]
		if choice.ChatNonStreamResponseChoice != nil && choice.ChatNonStreamResponseChoice.Message != nil {
			restoreToolAliasesInChatMessage(choice.ChatNonStreamResponseChoice.Message, reverse)
		}
		if choice.ChatStreamResponseChoice != nil && choice.ChatStreamResponseChoice.Delta != nil {
			for j := range choice.ChatStreamResponseChoice.Delta.ToolCalls {
				if choice.ChatStreamResponseChoice.Delta.ToolCalls[j].Function.Name == nil {
					continue
				}
				restored := restoreToolName(*choice.ChatStreamResponseChoice.Delta.ToolCalls[j].Function.Name, reverse)
				choice.ChatStreamResponseChoice.Delta.ToolCalls[j].Function.Name = schemas.Ptr(restored)
			}
		}
	}
}

func restoreToolAliasesInChatMessage(message *schemas.ChatMessage, reverse map[string]string) {
	if message == nil || message.ChatAssistantMessage == nil {
		return
	}
	for i := range message.ChatAssistantMessage.ToolCalls {
		if message.ChatAssistantMessage.ToolCalls[i].Function.Name == nil {
			continue
		}
		restored := restoreToolName(*message.ChatAssistantMessage.ToolCalls[i].Function.Name, reverse)
		message.ChatAssistantMessage.ToolCalls[i].Function.Name = schemas.Ptr(restored)
	}
}

func maskSensitiveList(values []string) []string {
	if len(values) == 0 {
		return values
	}
	masked := make([]string, 0, len(values))
	for _, value := range values {
		masked = append(masked, maskSensitiveValue(value))
	}
	return masked
}

func maskSensitiveValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	if len(value) <= 10 {
		return "****"
	}
	return value[:6] + "..." + value[len(value)-4:]
}

func asSlice(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func asInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		intValue, err := typed.Int64()
		if err == nil {
			return int(intValue)
		}
	}
	return 0
}

func anyToStringSlice(value any) []string {
	rawValues := asSlice(value)
	if len(rawValues) == 0 {
		return nil
	}
	values := make([]string, 0, len(rawValues))
	for _, rawValue := range rawValues {
		text := strings.TrimSpace(asString(rawValue))
		if text != "" {
			values = append(values, text)
		}
	}
	return values
}

func compactUUID(length int) string {
	value := strings.ReplaceAll(uuid.NewString(), "-", "")
	if length > 0 && length < len(value) {
		return value[:length]
	}
	return value
}

func compactHash(text string, length int) string {
	sum := sha1.Sum([]byte(text))
	value := fmt.Sprintf("%x", sum)
	if length > 0 && length < len(value) {
		return value[:length]
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func truncateString(text string, limit int) string {
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit]
}

func debugf(format string, args ...any) {
	if pluginConfig.Debug {
		log.Printf("["+GetName()+"] "+format, args...)
	}
}
