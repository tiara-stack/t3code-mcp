import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import * as Schema from "effect/Schema";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export type JsonRpcResponse = {
  readonly jsonrpc: "2.0";
  readonly id: number | string;
  readonly result?: {
    readonly protocolVersion?: string;
    readonly tools?: ReadonlyArray<ToolDescription>;
    readonly isError?: boolean;
    readonly structuredContent?: Record<string, unknown>;
    readonly content?: ReadonlyArray<{ readonly type: string; readonly text?: string }>;
  };
  readonly error?: { readonly code: number; readonly message: string };
};

export type ToolDescription = {
  readonly name: string;
  readonly inputSchema?: Record<string, unknown>;
  readonly outputSchema?: Record<string, unknown>;
};

const extraJsonProperties = [Schema.Record(Schema.String, Schema.Unknown)] as const;

const ToolDescriptionSchema = Schema.StructWithRest(
  Schema.Struct({
    name: Schema.String,
    inputSchema: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    outputSchema: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  }),
  extraJsonProperties,
);

const JsonRpcContentSchema = Schema.StructWithRest(
  Schema.Struct({
    type: Schema.String,
    text: Schema.optionalKey(Schema.String),
  }),
  extraJsonProperties,
);

const JsonRpcResultSchema = Schema.StructWithRest(
  Schema.Struct({
    protocolVersion: Schema.optionalKey(Schema.String),
    tools: Schema.optionalKey(Schema.Array(ToolDescriptionSchema)),
    isError: Schema.optionalKey(Schema.Boolean),
    structuredContent: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    content: Schema.optionalKey(Schema.Array(JsonRpcContentSchema)),
  }),
  extraJsonProperties,
);

const JsonRpcErrorSchema = Schema.StructWithRest(
  Schema.Struct({
    code: Schema.Number,
    message: Schema.String,
  }),
  extraJsonProperties,
);

const JsonRpcFrameSchema = Schema.StructWithRest(
  Schema.Struct({
    jsonrpc: Schema.Literal("2.0"),
    id: Schema.optionalKey(Schema.Union([Schema.Number, Schema.String])),
    method: Schema.optionalKey(Schema.NonEmptyString),
    params: Schema.optionalKey(Schema.Unknown),
    result: Schema.optionalKey(JsonRpcResultSchema),
    error: Schema.optionalKey(JsonRpcErrorSchema),
  }),
  extraJsonProperties,
);

export class StdioMcpProcess {
  readonly child: ChildProcessWithoutNullStreams;
  private readonly pending = new Map<
    number,
    {
      readonly resolve: (message: JsonRpcResponse) => void;
      readonly reject: (error: Error) => void;
      readonly clearTimeout: () => void;
    }
  >();
  private nextId = 0;
  private buffer = "";
  private stderrTail = "";
  private failure: Error | undefined;

  private constructor(child: ChildProcessWithoutNullStreams) {
    this.child = child;
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => this.read(chunk));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      this.stderrTail = (this.stderrTail + chunk).slice(-4_096);
    });
    child.stdin.on("error", (error) => this.fail(error));
    child.on("error", (error) => this.fail(error));
    child.on("exit", (code, signal) => {
      const stderr = this.stderrTail.trim();
      const detail = stderr.length === 0 ? "" : `\nChild stderr tail:\n${stderr}`;
      this.fail(new Error(`MCP process exited (${code ?? signal})${detail}`));
    });
  }

  static start(input: {
    readonly executable: string;
    readonly databasePath: string;
    readonly dataHome: string;
    readonly protocolVersion: string;
  }): Promise<{ readonly process: StdioMcpProcess; readonly selectedProtocolVersion: string }> {
    const childEnvironment: NodeJS.ProcessEnv = {};
    for (const name of [
      "PATH",
      "HOME",
      "USERPROFILE",
      "TMPDIR",
      "TMP",
      "TEMP",
      "LANG",
      "TZ",
      "SYSTEMROOT",
    ]) {
      const value = process.env[name];
      if (value !== undefined) childEnvironment[name] = value;
    }
    childEnvironment["T3CODE_MCP_DATABASE_PATH"] = input.databasePath;
    childEnvironment["XDG_DATA_HOME"] = input.dataHome;

    const child = spawn(process.execPath, [input.executable], {
      cwd: repositoryRoot,
      env: childEnvironment,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const mcp = new StdioMcpProcess(child);

    return mcp
      .request("initialize", {
        protocolVersion: input.protocolVersion,
        capabilities: {},
        clientInfo: { name: "t3code-mcp-acceptance", version: "1.0.0" },
      })
      .then(async (response) => {
        if (response.error !== undefined) {
          throw new Error(`MCP initialize failed: ${response.error.message}`);
        }
        const selectedProtocolVersion = response.result?.protocolVersion;
        if (selectedProtocolVersion === undefined) {
          throw new Error("MCP initialize did not return a protocol version");
        }
        mcp.notify("notifications/initialized", {});
        return { process: mcp, selectedProtocolVersion };
      })
      .catch(async (error: unknown) => {
        try {
          await mcp.stop("SIGTERM");
        } catch (cleanupError) {
          throw new AggregateError(
            [error, cleanupError],
            "MCP startup failed and the child process could not be stopped cleanly",
          );
        }
        throw error;
      });
  }

  get isAlive(): boolean {
    return this.child.exitCode === null && this.child.signalCode === null;
  }

  request(
    method: string,
    params: Record<string, unknown>,
    timeoutMs = 30_000,
  ): Promise<JsonRpcResponse> {
    return this.beginRequest(method, params, timeoutMs).promise;
  }

  beginRequest(method: string, params: Record<string, unknown>, timeoutMs = 30_000) {
    if (this.failure !== undefined) throw this.failure;
    if (!this.isAlive) throw new Error("MCP process is not running");
    const id = ++this.nextId;
    const message = { jsonrpc: "2.0", id, method, params };

    const promise = new Promise<JsonRpcResponse>((resolve, reject) => {
      const timeout = setTimeout(() => {
        const pending = this.pending.get(id);
        if (pending === undefined) return;
        this.pending.delete(id);
        try {
          this.notify("notifications/cancelled", {
            requestId: id,
            reason: `request timed out after ${timeoutMs} ms`,
          });
        } catch {
          // Reject the local wait even when the child process cannot receive cancellation.
        }
        pending.reject(new Error(`MCP request ${method} timed out after ${timeoutMs} ms`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (response) => {
          clearTimeout(timeout);
          resolve(response);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
        clearTimeout: () => clearTimeout(timeout),
      });
      this.child.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
        if (error === null || error === undefined) return;
        this.pending.delete(id);
        clearTimeout(timeout);
        reject(error);
      });
    });
    return { id, promise };
  }

  notify(method: string, params: Record<string, unknown>): void {
    if (this.failure !== undefined) throw this.failure;
    if (!this.isAlive) throw new Error("MCP process is not running");
    this.child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
  }

  async callTool(name: string, args: Record<string, unknown>, timeoutMs = 60_000) {
    return this.request("tools/call", { name, arguments: args }, timeoutMs);
  }

  beginToolCall(name: string, args: Record<string, unknown>, timeoutMs = 60_000) {
    return this.beginRequest("tools/call", { name, arguments: args }, timeoutMs);
  }

  cancelRequest(id: number, reason: string): boolean {
    if (this.failure !== undefined) throw this.failure;
    if (!this.isAlive) throw new Error("MCP process is not running");
    const pending = this.pending.get(id);
    if (pending === undefined) return false;
    this.notify("notifications/cancelled", { requestId: id, reason });
    if (this.pending.get(id) === pending) {
      this.pending.delete(id);
      pending.clearTimeout();
      pending.reject(new Error(`MCP request ${id} was cancelled: ${reason}`));
      return true;
    }
    return false;
  }

  async crash(): Promise<void> {
    if (!this.isAlive) return;
    if (!(await this.signalAndWait("SIGKILL", 5_000))) {
      throw new Error("MCP process did not exit after SIGKILL");
    }
  }

  async stop(signal: NodeJS.Signals = "SIGTERM"): Promise<void> {
    if (!this.isAlive) return;
    const completed = await this.signalAndWait(signal, 10_000);
    if (completed) return;

    if (!(await this.signalAndWait("SIGKILL", 5_000))) {
      throw new Error("MCP process did not exit after SIGKILL");
    }
    throw new Error("MCP process did not stop after SIGTERM; SIGKILL was required");
  }

  private signalAndWait(signal: NodeJS.Signals, timeoutMs: number): Promise<boolean> {
    if (!this.isAlive) return Promise.resolve(true);
    return new Promise((resolve) => {
      let settled = false;
      const finish = (exited: boolean) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        this.child.off("exit", onExit);
        resolve(exited);
      };
      const onExit = () => finish(true);
      const timeout = setTimeout(() => finish(false), timeoutMs);
      this.child.once("exit", onExit);
      const sent = this.child.kill(signal);
      if (!sent) finish(!this.isAlive);
    });
  }

  private read(chunk: string): void {
    this.buffer += chunk;
    const lines = this.buffer.split("\n");
    this.buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (line.trim().length === 0) continue;
      let response: JsonRpcResponse;
      try {
        const frame = Schema.decodeUnknownSync(JsonRpcFrameSchema)(JSON.parse(line));
        if (frame.method !== undefined) {
          if (frame.result !== undefined || frame.error !== undefined) {
            throw new Error("method messages cannot contain response fields");
          }
          continue;
        }
        if (frame.id === undefined) throw new Error("JSON-RPC responses require an id");
        if ((frame.result === undefined) === (frame.error === undefined)) {
          throw new Error("JSON-RPC responses require exactly one result or error");
        }
        if (frame.result !== undefined) {
          response = { jsonrpc: frame.jsonrpc, id: frame.id, result: frame.result };
        } else if (frame.error !== undefined) {
          response = { jsonrpc: frame.jsonrpc, id: frame.id, error: frame.error };
        } else {
          throw new Error("JSON-RPC response has no result or error");
        }
      } catch (error) {
        this.fail(
          new Error("MCP process wrote an invalid JSON-RPC response to stdout", { cause: error }),
        );
        return;
      }
      if (typeof response.id !== "number") continue;
      const waiter = this.pending.get(response.id);
      if (waiter === undefined) continue;
      this.pending.delete(response.id);
      waiter.resolve(response);
    }
  }

  private fail(error: Error): void {
    if (this.failure !== undefined) return;
    this.failure = error;
    for (const waiter of this.pending.values()) {
      waiter.clearTimeout();
      waiter.reject(error);
    }
    this.pending.clear();
  }
}
