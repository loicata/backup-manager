#!/bin/bash
# Hook: block destructive commands (rm -rf, rmdir, del) without user approval.
# Installed in .claude/settings.local.json as a PreToolUse hook for Bash.

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if [ -z "$COMMAND" ]; then
  echo '{"decision": "approve"}'
  exit 0
fi

# Check for destructive patterns
if echo "$COMMAND" | grep -qiE '(rm\s+-rf|rm\s+-r\s|rmdir|del\s+/|Remove-Item|rd\s+/s)'; then
  echo '{"decision": "block", "message": "BLOCKED: Destructive command detected. Ask the user before deleting files or directories."}'
  exit 0
fi

echo '{"decision": "approve"}'
