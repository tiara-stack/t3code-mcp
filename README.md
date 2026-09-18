# Effect MCP Server

A standalone TypeScript starter for building an [MCP](https://modelcontextprotocol.io/)
server with Effect 4.

The server uses Effect's native MCP support over stdio and includes one small
`echo` tool as a starting point. Add tools in [`src/tools.ts`](src/tools.ts)
and compose their layers in [`src/server.ts`](src/server.ts).

## Development

```bash
pnpm install
pnpm check
pnpm test
pnpm build
```

Run the server from source while developing:

```bash
pnpm dev
```

After building, run the packaged server with `pnpm start` or the local binary:

```bash
pnpm start
pnpm exec effect-mcp-server
```

An MCP client can launch the development server with a configuration like:

```json
{
  "mcpServers": {
    "effect-mcp-server": {
      "command": "pnpm",
      "args": ["--dir", "/path/to/effect-mcp-server", "dev"]
    }
  }
}
```

Fallow is pinned to the current project-local release and can be run with:

```bash
pnpm fallow
```

The audit compares the working tree with `main`; use
`pnpm exec fallow audit --base <ref>` when working from another base branch.
