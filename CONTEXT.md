# t3code-mcp

This context defines the vocabulary for an MCP server through which a
controlling agent manages work across T3Code instances.

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

**T3Code instance**:
A running T3Code server that holds the threads and worktrees available to the
controlling agent. One MCP server can connect to multiple T3Code instances.
_Avoid_: worker, MCP server

**Instance registration**:
A persistent connection record through which the MCP server identifies and
accesses one T3Code instance. Removing a registration does not remove the
instance or its work.
_Avoid_: instance, environment

**Resource reference**:
An instance-qualified designation of a project, thread, turn, or worktree
held by T3Code. A reference does not require a separate resource record in
the MCP server.
_Avoid_: managed resource, registration

**Controlling agent**:
The agent that directs work in T3Code, decides whether that work is complete,
and requests cleanup when its resources are no longer needed.
_Avoid_: MCP server, worker

**Work completion**:
The controlling agent's decision that no further work is required for the
task. A thread ending its current response does not by itself establish work
completion.
_Avoid_: settled, stopped, response finished

**Execution interruption**:
A request to stop the current execution in a thread. Depending on the
provider, interruption may also close the provider session; it does not
establish work completion.
_Avoid_: task completion, thread settlement

**Thread settlement**:
T3Code's attention state for parking a thread as done, whether explicitly
requested or applied by its own settlement policy. It is distinct from a
turn ending and does not establish the controlling agent's work completion.
_Avoid_: turn completion, execution idle

**Turn reference**:
The instance registration, thread, and native orchestration turn identity
that identify a particular observed execution. A submitted prompt does not
necessarily create a new turn or identify one in its acknowledgement.
_Avoid_: submission receipt, provider turn ID

**Provider session shutdown**:
A request to close a thread's provider session. It is distinct from
execution interruption and does not shut down the T3Code instance.
_Avoid_: instance shutdown, task completion

**Guarded cleanup**:
Removal of explicitly named resources subject to checks for active work
and other threads sharing a worktree. Thread removal retains its worktree
unless the controlling agent separately requests explicit discard.
_Avoid_: unconditional deletion, automatic cleanup

**Explicit discard**:
A separate request by the controlling agent to remove a specified worktree
and its contents without requiring their preservation. Active-work and
shared-reference guards still apply, and the branch is retained.
_Avoid_: ordinary cleanup, implicit force
