import { NodeStdio } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import { McpProtocol, McpServer } from "effect/unstable/ai";
import { LocalStore } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import { mcpServerToolkitLayer, serverToolkitLayer } from "./tools";

const mcpLayer = McpServer.layerStdio({
  name: "t3code-mcp",
  version: "0.1.0",
  description: "An Effect-based MCP server starter.",
  protocols: [
    McpProtocol.v2025_11_25,
    McpProtocol.v2025_06_18,
    McpProtocol.v2025_03_26,
    McpProtocol.v2024_11_05,
  ],
});

export const serverLayer = mcpServerToolkitLayer.pipe(
  Layer.provideMerge(mcpLayer),
  Layer.provide(serverToolkitLayer),
  Layer.provide(InstanceConnections.layer),
  Layer.provide(LocalStore.layerFromEnvironment),
  Layer.provide(NodeStdio.layer),
);
