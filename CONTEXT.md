# Effect MCP Server

This context defines the vocabulary for the standalone server that exposes
Effect-backed capabilities through the Model Context Protocol.

## Language

**MCP server**:
A process that exposes capabilities to an MCP client through the Model Context
Protocol.
_Avoid_: MCP app, agent server, API server

**MCP client**:
The external host that connects to the MCP server and requests its capabilities.
_Avoid_: consumer, caller

**Tool**:
A named capability that an MCP client can invoke with structured arguments and
receive as a structured result.
_Avoid_: function, endpoint, command

**Toolkit**:
A cohesive set of tools that forms the server's callable capability surface.
_Avoid_: tool registry, plugin

**Transport**:
The connection mode that carries MCP messages between the client and server.
_Avoid_: protocol, channel

**Protocol version**:
The MCP message and capability contract negotiated between a client and server.
_Avoid_: API version, transport version
