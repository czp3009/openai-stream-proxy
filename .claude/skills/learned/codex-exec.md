---
name: codex-exec
description: "Non-interactive Codex CLI invocation for agent/script integration via codex exec --json"
user-invocable: false
origin: auto-extracted
---

# Codex Exec for Programmatic Use

Calling Codex CLI from other programs or AI agents.

## Usage

```bash
codex exec --ephemeral --json "your prompt here"
```

Output is JSONL — one JSON object per line:

```jsonl
{"type":"thread.started","thread_id":"..."}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"response text here"}}
{"type":"turn.completed","usage":{"input_tokens":10624,"output_tokens":129}}
```

## Extracting the Response

The final assistant message is in the `item.completed` event where `item.type == "agent_message"`.

**Bash:**

```bash
codex exec --ephemeral --json "prompt" |
  jq -r 'select(.type == "item.completed" and .item.type == "agent_message") | .item.text'
```

**PowerShell:**

```powershell
codex exec --ephemeral --json "prompt" |
  ConvertFrom-Json |
  Where-Object { $_.type -eq "item.completed" -and $_.item.type -eq "agent_message" } |
  ForEach-Object { $_.item.text }
```

## Key Flags

- `--ephemeral` — no session files persisted to disk
- `--json` — structured JSONL output
- `-o, --output-last-message <FILE>` — write only last message to file
- `-c, --config <key=value>` — override config.toml values
- `-m, --model <MODEL>` — override model
- `-s, --sandbox <MODE>` — sandbox policy: read-only, workspace-write, danger-full-access
- `--full-auto` — low-friction sandboxed automatic execution
