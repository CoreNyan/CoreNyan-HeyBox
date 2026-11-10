# CoreNyan HeyBox adapter

Gateway owns tool dispatch, review tickets, scheduling and workflow state. Brain owns draft cognition, persona, memories and skills. This Python process owns the existing platform operations and durable platform write journal; it does not call an LLM.

Copy `config.example.json` to private `config.json`, configure Redis privately and opt in with `enabled: true`. The default is disabled, including direct tool execution and comment patrol; a disabled startup creates no account or Redis connection. Gateway's HeyBox module must also be enabled. Set the same namespace on both sides (default `corenyan`). Namespace changes require restarting this adapter and the corresponding Gateway.

Never retry unknown platform writes. Keep `bridge_state.sqlite3` and the session outside Git. The bridge logs to `logs/heybox.log`, rotating at midnight with 14 retained daily files. Standard output retains `[HeyBox]` and level fields.

Offline verification: `uv run --extra dev --extra mcp python scripts/verify.py`. Tests use synthetic local state and do not access accounts or publish. The CLI/SDK/API remain independent entrypoints with their existing explicit user authorization; the CoreNyan `enabled` switch governs the Redis integration.
