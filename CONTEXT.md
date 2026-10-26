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

**Provider instance**:
A configured provider inside one T3Code instance, selected with a model to
execute work. It is distinct from an instance registration or provider session.
_Avoid_: registration, T3Code instance, delegation authority

**Resource reference**:
An instance-qualified designation of a project, thread, run, or worktree
held by T3Code. A reference does not require a separate resource record in
the MCP server.
_Avoid_: managed resource, registration

**Run**:
T3Code's application execution for a counted conversation turn, which can span
multiple attempts. A run ending does not establish that related delegated work
has ended or that the controlling agent's work is complete.
_Avoid_: provider turn, mutation receipt, task completion

**Run reference**:
The instance registration, thread, and native run identity that identify one
T3Code run. An accepted message need not yet identify its resulting run.
_Avoid_: mutation receipt, provider turn ID, historical turn reference

**Run attempt**:
One attempt to execute a run, with its own root execution node. An explicit
restart can replace the attempt without changing the run identity.
_Avoid_: run, provider turn, restart continuation

**Execution node**:
A unit in T3Code's execution graph, with its own identity, lifecycle and known
run or parent relationships. A node's state does not establish the state of
every related node.
_Avoid_: thread, run attempt, delegated task

**Provider turn**:
One provider-native execution within a run attempt. An attempt can contain
multiple provider turns, whose identities are distinct from the run identity.
_Avoid_: run, application turn, mutation receipt

**Restart continuation**:
A new run that continues a previous run after server recovery, linked to the
previous run by T3Code. It differs from restarting an attempt of the same run.
_Avoid_: run attempt, uninterrupted steering

**Delegated task**:
Child work owned by a parent execution in T3Code, with its native task and
execution relationships. An ordinary top-level thread is not a delegated task.
_Avoid_: instance registration, ordinary thread, externally owned child

**Provider session**:
A provider runtime context that can have attachments to multiple threads.
Detaching one thread does not establish that the shared session or process ended.
_Avoid_: T3Code instance, run, provider instance

**Runtime request**:
A native approval or input request with its own lifecycle, offered response
schema and current response capability. Its run correlation may be unknown.
_Avoid_: client request ID, mutation receipt, invented approval ID

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
run ending and does not establish the controlling agent's work completion.
_Avoid_: turn completion, execution idle

**Execution inactivity**:
The absence of queued, executing or waiting work and unresolved requests in
the relevant thread and delegated-work scope. Missing required evidence leaves
inactivity unknown rather than established.
_Avoid_: parent run ended, settled, provider detached, work complete

**Historical turn reference**:
The instance registration, thread, and native orchestration turn identity
retained from the earlier T3Code contract. It is historical evidence, not a
V2 run reference.
_Avoid_: run reference, provider turn ID

**Mutation admission**:
The MCP server's acceptance of responsibility for a requested change,
independent of the originating MCP call. Admission does not establish that
T3Code accepted or completed the change.
_Avoid_: command acceptance, execution start, completion

**Mutation receipt**:
The MCP server's record of a requested change and its known outcome. It can
identify accepted intent while the resulting execution remains uncorrelated
or unresolved.
_Avoid_: run reference, execution receipt, work completion

**Captured view**:
A retained, immutable observation used for bounded pagination. Exhausting its
pages does not establish that the upstream source history was complete.
_Avoid_: live state, complete execution log, native history cursor

**Observation cursor**:
A reference to an observed state and its continuity baseline for a later
change wait. It is distinct from a captured-view continuation or native timeline
position.
_Avoid_: page cursor, native offset, cleanup authorization

**Projected provider detachment**:
T3Code's recorded removal of a thread's provider-session binding. It does not
establish that the provider runtime applied the detachment.
_Avoid_: runtime detachment, session shutdown, execution inactivity

**Runtime provider detachment**:
Removal of the thread's attachment in the provider runtime. It is distinct
from projected detachment and does not require a shared provider process to exit.
_Avoid_: projected detachment, provider-process shutdown, work completion

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
