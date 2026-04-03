.PHONY: build build-plugins-linux-amd64 deploy-bridge-remote deploy-search-remote test clean

build: build-plugins-linux-amd64

build-plugins-linux-amd64:
	$(MAKE) -C ./plugins/bifrost-anthropic-kimi-bridge build-linux-amd64
	$(MAKE) -C ./plugins/bifrost-kimi-web-search build-linux-amd64
	$(MAKE) -C ./plugins/bifrost-model-identity-injector build-linux-amd64

deploy-bridge-remote:
	$(MAKE) -C ./plugins/bifrost-anthropic-kimi-bridge deploy-remote

deploy-search-remote:
	$(MAKE) -C ./plugins/bifrost-kimi-web-search deploy-remote

test:
	go test ./...

clean:
	$(MAKE) -C ./plugins/bifrost-anthropic-kimi-bridge clean
	$(MAKE) -C ./plugins/bifrost-kimi-web-search clean
	$(MAKE) -C ./plugins/bifrost-model-identity-injector clean
