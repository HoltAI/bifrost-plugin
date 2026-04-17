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
