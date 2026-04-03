.PHONY: build build-linux-amd64 deploy-remote clean

PLUGIN_NAME := bifrost-anthropic-kimi-bridge
PLUGIN_PACKAGE := ./cmd/$(PLUGIN_NAME)
BUILD_DIR := build
OUTPUT := $(BUILD_DIR)/$(PLUGIN_NAME).so
DOCKER_BUILD_IMAGE := golang:1.26.1-bookworm
GO_BUILD_FLAGS := -trimpath

build:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 go build $(GO_BUILD_FLAGS) -buildmode=plugin -o $(OUTPUT) $(PLUGIN_PACKAGE)

build-linux-amd64:
	@mkdir -p $(BUILD_DIR)
	docker run --rm \
		--platform linux/amd64 \
		-e GOPROXY="$${GOPROXY}" \
		-e GOSUMDB="$${GOSUMDB}" \
		-e http_proxy="$${http_proxy}" \
		-e https_proxy="$${https_proxy}" \
		-e all_proxy="$${all_proxy}" \
		-v "$$(pwd):/work" \
		-w /work \
			$(DOCKER_BUILD_IMAGE) \
			bash -lc 'GOAMD64=v1 CGO_ENABLED=1 GOOS=linux GOARCH=amd64 /usr/local/go/bin/go build $(GO_BUILD_FLAGS) -buildmode=plugin -o $(OUTPUT) $(PLUGIN_PACKAGE) && chown $$(stat -c "%u:%g" /work) $(OUTPUT)'

deploy-remote:
	./deploy/remote.sh

clean:
	rm -rf $(BUILD_DIR)
