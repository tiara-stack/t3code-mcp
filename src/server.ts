import { NodeStdio } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import { McpProtocol, McpServer } from "effect/unstable/ai";
import { ServerToolkit, serverToolkitLayer } from "./tools";

export const serverLayer = McpServer.layerStdio({
  name: "effect-mcp-server",
  version: "0.1.0",
  description: "An Effect-based MCP server starter.",
  protocols: [
    McpProtocol.v2025_11_25,
    McpProtocol.v2025_06_18,
    McpProtocol.v2025_03_26,
    McpProtocol.v2024_11_05,
  ],
}).pipe(
  Layer.provide(McpServer.toolkit(ServerToolkit)),
  Layer.provide(serverToolkitLayer),
  Layer.provide(NodeStdio.layer),
);
