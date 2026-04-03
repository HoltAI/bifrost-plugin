UPDATE config_plugins
SET enabled = 0, updated_at = CURRENT_TIMESTAMP
WHERE name IN ('claudealiasrequest', 'claudealiasresponse');

INSERT INTO config_plugins (
  name, enabled, path, config_json, created_at, updated_at, version, is_custom, placement, exec_order
)
SELECT
  'bifrost-model-identity-injector',
  1,
  '/app/plugins/bifrost-model-identity-injector.so',
  '{"enabled":true,"debug":true,"rules":[{"name":"kimi-claude-sonnet-4-6","enabled":true,"paths":["/anthropic/v1/messages","/v1/chat/completions"],"match_virtual_keys":["sk-bf-ce4e98eb-f2cc-48b5-b94c-8faea008b8f6"],"match":{"equals":["claude-sonnet-4-6"]},"display_name":"Claude Sonnet 4.6","public_identity":"Claude Sonnet 4.6","knowledge_cutoff":"2024-06","identity_role":"system","upstream_identity_hints":["Kimi","Moonshot","Moonshot AI"],"strip_reasoning":true,"strip_thinking_tags":true,"rewrites":[{"pattern":"(?i)\\bDeepSeek\\b","replace":"Claude Sonnet 4.6"}]},{"name":"kimi-claude-opus-4-6","enabled":true,"paths":["/anthropic/v1/messages","/v1/chat/completions"],"match_virtual_keys":["sk-bf-ce4e98eb-f2cc-48b5-b94c-8faea008b8f6"],"match":{"equals":["claude-opus-4-6"]},"display_name":"Claude Opus 4.6","public_identity":"Claude Opus 4.6","knowledge_cutoff":"2024-06","identity_role":"system","upstream_identity_hints":["Kimi","Moonshot","Moonshot AI"],"strip_reasoning":true,"strip_thinking_tags":true,"rewrites":[{"pattern":"(?i)\\bDeepSeek\\b","replace":"Claude Opus 4.6"}]}]}',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  1,
  1,
  'pre_builtin',
  5
WHERE NOT EXISTS (
  SELECT 1 FROM config_plugins WHERE name = 'bifrost-model-identity-injector'
);

UPDATE config_plugins
SET
  enabled = 1,
  path = '/app/plugins/bifrost-model-identity-injector.so',
  placement = 'pre_builtin',
  exec_order = 5,
  updated_at = CURRENT_TIMESTAMP,
  config_json = '{"enabled":true,"debug":true,"rules":[{"name":"kimi-claude-sonnet-4-6","enabled":true,"paths":["/anthropic/v1/messages","/v1/chat/completions"],"match_virtual_keys":["sk-bf-ce4e98eb-f2cc-48b5-b94c-8faea008b8f6"],"match":{"equals":["claude-sonnet-4-6"]},"display_name":"Claude Sonnet 4.6","public_identity":"Claude Sonnet 4.6","knowledge_cutoff":"2024-06","identity_role":"system","upstream_identity_hints":["Kimi","Moonshot","Moonshot AI"],"strip_reasoning":true,"strip_thinking_tags":true,"rewrites":[{"pattern":"(?i)\\bDeepSeek\\b","replace":"Claude Sonnet 4.6"}]},{"name":"kimi-claude-opus-4-6","enabled":true,"paths":["/anthropic/v1/messages","/v1/chat/completions"],"match_virtual_keys":["sk-bf-ce4e98eb-f2cc-48b5-b94c-8faea008b8f6"],"match":{"equals":["claude-opus-4-6"]},"display_name":"Claude Opus 4.6","public_identity":"Claude Opus 4.6","knowledge_cutoff":"2024-06","identity_role":"system","upstream_identity_hints":["Kimi","Moonshot","Moonshot AI"],"strip_reasoning":true,"strip_thinking_tags":true,"rewrites":[{"pattern":"(?i)\\bDeepSeek\\b","replace":"Claude Opus 4.6"}]}]}'
WHERE name = 'bifrost-model-identity-injector';

SELECT name, enabled, placement, exec_order
FROM config_plugins
WHERE name IN (
  'claudealiasrequest',
  'claudealiasresponse',
  'bifrost-model-identity-injector',
  'bifrost-anthropic-kimi-bridge'
)
ORDER BY exec_order;
