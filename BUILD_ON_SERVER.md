# Server Build Guide

This repository is the source of truth for custom Bifrost plugins. The production server must build plugins against the live Bifrost source snapshot, not from a local laptop binary upload.

## Canonical paths on the server

- Plugin source repo: `/home/azureuser/bifrost-plugin`
- Live deployment root: `/home/azureuser/bifrost-dynamic`
- Live Bifrost source snapshot: `/home/azureuser/bifrost-dynamic/src`
- Persistent build workspace: `/home/azureuser/bifrost-plugin/.build/live-bifrost-1.4.19`
- Built plugin output: `/home/azureuser/bifrost-plugin/.build/live-bifrost-1.4.19/out/<plugin>.so`

## Why this flow exists

Go plugins are ABI-sensitive. If a plugin is built against a different effective dependency graph than the running Bifrost binary, Bifrost may fail to load it with errors like:

- `plugin was built with a different version of package internal/goarch`
- `plugin was built with a different version of package github.com/maximhq/bifrost/core/schemas`

The safe rule is:

1. Build on the server.
2. Build against `/home/azureuser/bifrost-dynamic/src`.
3. Use the exact same Go and CGO settings as the live runtime.

## Build a plugin

Run from the server:

```bash
cd /home/azureuser/bifrost-plugin
./tools/build-live-plugin.sh bifrost-anthropic-kimi-bridge
```

Other plugins:

```bash
cd /home/azureuser/bifrost-plugin
./tools/build-live-plugin.sh bifrost-kimi-web-search
./tools/build-live-plugin.sh bifrost-model-identity-injector
```

## Deploy a plugin

```bash
cd /home/azureuser/bifrost-plugin
./tools/deploy-live-plugin.sh bifrost-anthropic-kimi-bridge
```

The deploy script will:

1. Back up the current `.so` in `/home/azureuser/bifrost-dynamic/plugins`
2. Copy the freshly built `.so`
3. Restart `bifrost-dynamic`
4. Print plugin log lines and `/health`

## Refresh the build workspace

If the live Bifrost source changes, rebuild the workspace from the current runtime snapshot:

```bash
cd /home/azureuser/bifrost-plugin
REFRESH_WORKSPACE=1 ./tools/build-live-plugin.sh bifrost-anthropic-kimi-bridge
```

Use this after:

- upgrading Bifrost
- changing `/home/azureuser/bifrost-dynamic/src`
- seeing any plugin ABI mismatch again

## Do not do this

- Do not build the plugin on a local Mac and upload the `.so`
- Do not reuse old `.so` backups after the live runtime changes
- Do not compile against a random standalone Go module without the live workspace

## Current layout policy

Keep only these long-lived server areas:

- `/home/azureuser/bifrost-dynamic`: live deployment
- `/home/azureuser/bifrost-plugin`: plugin source + helper scripts + build workspace
- `/home/azureuser/litellm`: unrelated LiteLLM deployment, do not touch unless needed

Temporary test containers and ad hoc workspaces should be removed after use.
