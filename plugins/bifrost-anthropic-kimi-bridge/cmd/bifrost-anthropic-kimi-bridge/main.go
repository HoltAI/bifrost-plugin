package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

const (
	ctxKeyConverted      = "bifrost_anthropic_kimi_bridge_converted"
	ctxKeyRequestedModel = "bifrost_anthropic_kimi_bridge_requested_model"
)

type PluginConfig struct {
	UpstreamBaseURL       string   `json:"upstream_base_url"`
	ChatCompletionsPath   string   `json:"chat_completions_path"`
	AnthropicMessagesPath string   `json:"anthropic_messages_path"`
	MatchModels           []string `json:"match_models"`
	MatchSubstrings       []string `json:"match_substrings"`
	MatchVirtualKeys      []string `json:"match_virtual_keys"`
	BridgeAllModels       bool     `json:"bridge_all_models"`
	TimeoutMS             int      `json:"timeout_ms"`
	Debug                 bool     `json:"debug"`
}

type anthropicStreamBridgeState struct {
	requestedModel        string
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

func Init(config any) error {
	cfgMap, ok := config.(map[string]any)
	if ok {
		if value := asString(cfgMap["upstream_base_url"]); value != "" {
			pluginConfig.UpstreamBaseURL = strings.TrimRight(value, "/")
		}
		if value := asString(cfgMap["chat_completions_path"]); value != "" {
			if !strings.HasPrefix(value, "/") {
				value = "/" + value
			}
			pluginConfig.ChatCompletionsPath = value
		}
		if value := asString(cfgMap["anthropic_messages_path"]); value != "" {
			if !strings.HasPrefix(value, "/") {
				value = "/" + value
			}
			pluginConfig.AnthropicMessagesPath = value
		}
		if values := anyToStringSlice(cfgMap["match_models"]); len(values) > 0 {
			pluginConfig.MatchModels = values
		}
		if values := anyToStringSlice(cfgMap["match_substrings"]); len(values) > 0 {
			pluginConfig.MatchSubstrings = values
		}
		if values := anyToStringSlice(cfgMap["match_virtual_keys"]); len(values) > 0 {
			pluginConfig.MatchVirtualKeys = values
		}
		if value, ok := cfgMap["bridge_all_models"].(bool); ok {
			pluginConfig.BridgeAllModels = value
		}
		if value := asInt(cfgMap["timeout_ms"]); value > 0 {
			pluginConfig.TimeoutMS = value
		}
		if value, ok := cfgMap["debug"].(bool); ok {
			pluginConfig.Debug = value
		}
	}

	httpClient = &http.Client{Timeout: time.Duration(pluginConfig.TimeoutMS) * time.Millisecond}
	log.Printf("[%s] initialized upstream=%s anthropic_path=%s chat_path=%s match_models=%v match_substrings=%v match_virtual_keys=%v bridge_all_models=%v timeout_ms=%d",
		GetName(),
		pluginConfig.UpstreamBaseURL,
		pluginConfig.AnthropicMessagesPath,
		pluginConfig.ChatCompletionsPath,
		pluginConfig.MatchModels,
		pluginConfig.MatchSubstrings,
		maskSensitiveList(pluginConfig.MatchVirtualKeys),
		pluginConfig.BridgeAllModels,
		pluginConfig.TimeoutMS,
	)
	return nil
}

func GetName() string {
	return "bifrost-anthropic-kimi-bridge"
}

func Cleanup() error {
	return nil
}

func HTTPTransportPreHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest) (*schemas.HTTPResponse, error) {
	if !strings.EqualFold(req.Method, http.MethodPost) {
		return nil, nil
	}
	if normalizePath(req.Path) != "/anthropic/v1/messages" {
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

	stream, _ := body["stream"].(bool)
	if !stream {
		return nil, nil
	}

	debugf("handling simulated stream bridge for model=%q path=%s", model, req.Path)
	return handleStreamingBridge(ctx, req, body, model)
}

func HTTPTransportPostHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, resp *schemas.HTTPResponse) error {
	return nil
}

func HTTPTransportStreamChunkHook(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, chunk *schemas.BifrostStreamChunk) (*schemas.BifrostStreamChunk, error) {
	return chunk, nil
}

func PreLLMHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
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

	req.ChatRequest = chatReq
	req.ResponsesRequest = nil
	req.RequestType = schemas.ChatCompletionRequest

	if ctx != nil {
		ctx.SetValue(ctxKeyConverted, true)
		ctx.SetValue(ctxKeyRequestedModel, model)
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

func handleStreamingBridge(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, requestedModel string) (*schemas.HTTPResponse, error) {
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

	go simulateAnthropicStream(ctx, req, anthropicBody, pipeWriter, requestedModel)

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

func newInternalChatCompletionsRequest(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any) (*http.Request, error) {
	openAIBody := convertAnthropicMessagesToOpenAI(anthropicBody)

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

func simulateAnthropicStream(ctx *schemas.BifrostContext, req *schemas.HTTPRequest, anthropicBody map[string]any, writer *io.PipeWriter, requestedModel string) {
	defer writer.Close()

	httpReq, err := newInternalChatCompletionsRequest(ctx, req, anthropicBody)
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

	anthropicPayload := convertOpenAIChatPayloadToAnthropicPayload(payload, requestedModel)
	if len(anthropicPayload) == 0 {
		emitAnthropicStreamError(writer, "failed to convert internal chat response to anthropic payload")
		return
	}
	if err := streamAnthropicPayload(writer, anthropicPayload); err != nil {
		emitAnthropicStreamError(writer, err.Error())
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

func convertOpenAIChatPayloadToAnthropicPayload(payload map[string]any, requestedModel string) map[string]any {
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
	if text == "" {
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
			"name":  asString(function["name"]),
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

func streamAnthropicPayload(writer *io.PipeWriter, payload map[string]any) error {
	messageID := firstNonEmpty(asString(payload["id"]), "msg_"+compactUUID(22))
	message := map[string]any{
		"id":            messageID,
		"type":          "message",
		"role":          firstNonEmpty(asString(payload["role"]), "assistant"),
		"model":         asString(payload["model"]),
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

func convertAnthropicMessagesToOpenAI(body map[string]any) map[string]any {
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
							"name":      asString(block["name"]),
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

	tools := convertAnthropicToolsToOpenAI(body["tools"])
	if len(tools) > 0 {
		openAIBody["tools"] = tools
		if toolChoice := normalizeOpenAIToolChoice(body["tool_choice"]); toolChoice != nil {
			openAIBody["tool_choice"] = toolChoice
		}
	}

	return openAIBody
}

func convertAnthropicToolsToOpenAI(tools any) []map[string]any {
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
				"name":        name,
				"description": asString(tool["description"]),
				"parameters":  parameters,
			},
		})
	}
	return converted
}

func normalizeOpenAIToolChoice(toolChoice any) any {
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
					"name": name,
				},
			}
		case "function":
			return value
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
		requestedModel: requestedModel,
		toolBlocks:     make(map[int]*toolBlockState),
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
		block.name = name
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

func clearConversionState(ctx *schemas.BifrostContext) {
	if ctx == nil {
		return
	}
	ctx.ClearValue(ctxKeyConverted)
	ctx.ClearValue(ctxKeyRequestedModel)
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
