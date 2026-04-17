package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func TestHTTPTransportPreHook_NonStreamUsesBridgeConversion(t *testing.T) {
	originalConfig := pluginConfig
	originalClient := httpClient
	defer func() {
		pluginConfig = originalConfig
		httpClient = originalClient
	}()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/v1/chat/completions"; got != want {
			t.Fatalf("unexpected upstream path: got %q want %q", got, want)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-vk" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl_test",
			"model": "Kimi-K2.5",
			"choices": []any{
				map[string]any{
					"index":         0,
					"finish_reason": "stop",
					"message": map[string]any{
						"role":      "assistant",
						"content":   "I am Kimi.",
						"reasoning": "hidden reasoning",
					},
				},
			},
			"usage": map[string]any{
				"prompt_tokens":     12,
				"completion_tokens": 4,
				"total_tokens":      16,
			},
		})
	}))
	defer upstream.Close()

	httpClient = upstream.Client()
	pluginConfig.Enabled = true
	pluginConfig.UpstreamBaseURL = upstream.URL
	pluginConfig.ChatCompletionsPath = "/v1/chat/completions"
	pluginConfig.AnthropicMessagesPath = "/anthropic/v1/messages"
	pluginConfig.BridgeAllModels = true

	body := []byte(`{"model":"claude-sonnet-4-6","max_tokens":256,"messages":[{"role":"user","content":"who are you"}]}`)
	req := &schemas.HTTPRequest{
		Method: "POST",
		Path:   "/anthropic/v1/messages",
		Headers: map[string]string{
			"authorization":     "Bearer test-vk",
			"anthropic-version": "2023-06-01",
		},
		Body: body,
	}
	ctx := schemas.NewBifrostContext(context.Background(), time.Time{})

	resp, err := HTTPTransportPreHook(ctx, req)
	if err != nil {
		t.Fatalf("HTTPTransportPreHook returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-stream bridge to short-circuit with a response")
	}
	if got, want := resp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("unexpected status code: got %d want %d", got, want)
	}

	var payload map[string]any
	if err := json.Unmarshal(resp.Body, &payload); err != nil {
		t.Fatalf("failed to unmarshal response body: %v", err)
	}

	if got, want := payload["type"], "message"; got != want {
		t.Fatalf("unexpected response type: got %v want %v", got, want)
	}
	if got, want := payload["model"], "claude-sonnet-4-6"; got != want {
		t.Fatalf("unexpected response model: got %v want %v", got, want)
	}

	content, ok := payload["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("unexpected content blocks: %#v", payload["content"])
	}
	block, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected content block type: %#v", content[0])
	}
	if got, want := block["type"], "text"; got != want {
		t.Fatalf("unexpected block type: got %v want %v", got, want)
	}
	if got, want := block["text"], "I am Kimi."; got != want {
		t.Fatalf("unexpected block text: got %v want %v", got, want)
	}
}

func TestConvertChatResponseToResponsesStreamEvents_TextResponse(t *testing.T) {
	text := "I am Claude Opus 4.7."
	stop := string(schemas.BifrostFinishReasonStop)

	events := convertChatResponseToResponsesStreamEvents(&schemas.BifrostChatResponse{
		ID:    "chatcmpl_text",
		Model: "claude-opus-4-7",
		Usage: &schemas.BifrostLLMUsage{
			PromptTokens:     12,
			CompletionTokens: 4,
			TotalTokens:      16,
		},
		Choices: []schemas.BifrostResponseChoice{{
			Index:        0,
			FinishReason: &stop,
			ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
				Message: &schemas.ChatMessage{
					Role: schemas.ChatMessageRoleAssistant,
					Content: &schemas.ChatMessageContent{
						ContentStr: &text,
					},
				},
			},
		}},
	})

	if len(events) == 0 {
		t.Fatal("expected responses stream events")
	}

	var sawCreated, sawDelta, sawDone, sawCompleted bool
	for _, event := range events {
		if event == nil {
			continue
		}
		switch event.Type {
		case schemas.ResponsesStreamResponseTypeCreated:
			sawCreated = true
		case schemas.ResponsesStreamResponseTypeOutputTextDelta:
			sawDelta = true
		case schemas.ResponsesStreamResponseTypeOutputTextDone:
			sawDone = true
			if event.Text == nil || *event.Text != text {
				t.Fatalf("unexpected output_text.done text: %#v", event.Text)
			}
		case schemas.ResponsesStreamResponseTypeCompleted:
			sawCompleted = true
			if event.Response == nil {
				t.Fatal("expected completed event response payload")
			}
			if got, want := event.Response.Model, "claude-opus-4-7"; got != want {
				t.Fatalf("unexpected completed model: got %q want %q", got, want)
			}
			if event.Response.Usage == nil || event.Response.Usage.TotalTokens != 16 {
				t.Fatalf("unexpected usage on completed response: %#v", event.Response.Usage)
			}
			if len(event.Response.Output) != 1 {
				t.Fatalf("unexpected completed output count: %d", len(event.Response.Output))
			}
			if event.Response.Output[0].Content == nil || len(event.Response.Output[0].Content.ContentBlocks) != 1 {
				t.Fatalf("unexpected completed output content: %#v", event.Response.Output[0].Content)
			}
			block := event.Response.Output[0].Content.ContentBlocks[0]
			if block.Text == nil || *block.Text != text {
				t.Fatalf("unexpected completed output text: %#v", block.Text)
			}
		}
	}

	if !sawCreated || !sawDelta || !sawDone || !sawCompleted {
		t.Fatalf("missing expected events: created=%v delta=%v done=%v completed=%v", sawCreated, sawDelta, sawDone, sawCompleted)
	}
}

func TestConvertChatResponseToResponsesStreamEvents_ToolCallResponse(t *testing.T) {
	stop := string(schemas.BifrostFinishReasonToolCalls)
	toolName := "web_search"
	toolID := "call_123"
	args := `{"q":"today news"}`

	events := convertChatResponseToResponsesStreamEvents(&schemas.BifrostChatResponse{
		ID:    "chatcmpl_tool",
		Model: "claude-opus-4-7",
		Choices: []schemas.BifrostResponseChoice{{
			Index:        0,
			FinishReason: &stop,
			ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
				Message: &schemas.ChatMessage{
					Role: schemas.ChatMessageRoleAssistant,
					ChatAssistantMessage: &schemas.ChatAssistantMessage{
						ToolCalls: []schemas.ChatAssistantMessageToolCall{{
							Index: 0,
							ID:    &toolID,
							Function: schemas.ChatAssistantMessageToolCallFunction{
								Name:      &toolName,
								Arguments: args,
							},
						}},
					},
				},
			},
		}},
	})

	if len(events) == 0 {
		t.Fatal("expected responses stream events")
	}

	var sawArgsDelta, sawArgsDone, sawCompleted bool
	for _, event := range events {
		if event == nil {
			continue
		}
		switch event.Type {
		case schemas.ResponsesStreamResponseTypeFunctionCallArgumentsDelta:
			sawArgsDelta = true
			if event.Delta == nil || *event.Delta != args {
				t.Fatalf("unexpected arguments delta: %#v", event.Delta)
			}
		case schemas.ResponsesStreamResponseTypeFunctionCallArgumentsDone:
			sawArgsDone = true
			if event.Arguments == nil || *event.Arguments != args {
				t.Fatalf("unexpected arguments done: %#v", event.Arguments)
			}
		case schemas.ResponsesStreamResponseTypeCompleted:
			sawCompleted = true
			if event.Response == nil || len(event.Response.Output) != 1 {
				t.Fatalf("unexpected completed output: %#v", event.Response)
			}
			item := event.Response.Output[0]
			if item.ResponsesToolMessage == nil || item.ResponsesToolMessage.Name == nil || *item.ResponsesToolMessage.Name != toolName {
				t.Fatalf("unexpected completed tool call name: %#v", item.ResponsesToolMessage)
			}
			if item.ResponsesToolMessage.Arguments == nil || *item.ResponsesToolMessage.Arguments != args {
				t.Fatalf("unexpected completed tool arguments: %#v", item.ResponsesToolMessage.Arguments)
			}
		}
	}

	if !sawArgsDelta || !sawArgsDone || !sawCompleted {
		t.Fatalf("missing expected tool events: delta=%v done=%v completed=%v", sawArgsDelta, sawArgsDone, sawCompleted)
	}
}
