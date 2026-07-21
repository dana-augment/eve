import { beforeEach, describe, expect, it, vi } from "vitest";

import type { SessionAuthContext } from "#channel/types.js";
import { WorkflowAgentInvocationExecution } from "#internal/invocation/workflow-execution.js";
import type { HandleMessageStreamEvent } from "#protocol/message.js";
import type { Agent } from "#public/definitions/channel.js";

const runsGet = vi.fn();
const cancel = vi.fn();
const returnValue = vi.fn();
const getReadable = vi.fn();

vi.mock("#internal/workflow/runtime.js", () => ({
  getWorld: async () => ({ runs: { get: runsGet } }),
  getRun: () => ({
    cancel,
    get returnValue() {
      return returnValue();
    },
    getReadable,
  }),
}));

const auth: SessionAuthContext = {
  attributes: {},
  authenticator: "test",
  principalId: "alice",
  principalType: "user",
};

const agent: Agent = {
  cancelTurn: vi.fn(),
  deliver: vi.fn(),
  getEventStream: vi.fn(),
  run: vi.fn(),
};

describe("WorkflowAgentInvocationExecution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getReadable.mockReturnValue(eventStream([]));
  });

  it("seeds invocation metadata when starting a task run", async () => {
    vi.mocked(agent.run).mockResolvedValue({
      continuationToken: "mcp:invocation:token",
      events: new ReadableStream(),
      sessionId: "wrun_invocation",
    });
    const invocation = await execution().create({
      auth,
      message: "work",
    });

    expect(agent.run).toHaveBeenCalledWith(
      expect.objectContaining({
        externalInvocation: expect.objectContaining({ continuationToken: expect.any(String) }),
        mode: "task",
      }),
    );
    expect(invocation).toMatchObject({ invocationId: "wrun_invocation", status: "working" });
  });

  it("hides a run from a different owner", async () => {
    runsGet.mockResolvedValue(run({ status: "running" }));
    await expect(
      execution().read({
        auth: { ...auth, principalId: "other" },
        invocationId: "wrun_invocation",
      }),
    ).resolves.toBeUndefined();
  });

  it("uses persisted initiator auth rather than mutable auth attributes", async () => {
    runsGet.mockResolvedValue(
      run({
        initiatorAuth: {
          ...auth,
          attributes: { role: "member" },
          issuer: "idp",
          subject: "original-subject",
        },
        status: "running",
      }),
    );

    await expect(
      execution().read({
        auth: {
          ...auth,
          attributes: { role: "admin" },
          authenticator: "custom",
          issuer: "idp",
          subject: "new-subject",
        },
        invocationId: "wrun_invocation",
      }),
    ).resolves.toMatchObject({ status: "working" });
  });

  it("rejects an invocation whose persisted initiator auth is malformed", async () => {
    const malformed = run({ status: "running" });
    (malformed.input[0]!.serializedContext as Record<string, unknown>)["eve.initiatorAuth"] = {
      principalId: "alice",
    };
    runsGet.mockResolvedValue(malformed);

    await expect(
      execution().read({ auth, invocationId: "wrun_invocation" }),
    ).resolves.toBeUndefined();
  });

  it("replays the existing event stream to reconstruct pending input", async () => {
    runsGet.mockResolvedValue(run({ status: "running" }));
    getReadable.mockReturnValue(
      eventStream([
        { type: "turn.started", data: { turnId: "turn_1" } } as HandleMessageStreamEvent,
        {
          type: "input.requested",
          data: {
            sequence: 0,
            stepIndex: 0,
            turnId: "turn_1",
            requests: [
              {
                action: {
                  callId: "call_1",
                  input: {},
                  kind: "tool-call",
                  toolName: "ask_question",
                },
                options: [{ id: "yes", label: "Yes" }],
                prompt: "Proceed?",
                requestId: "question",
              },
            ],
          },
        } as HandleMessageStreamEvent,
      ]),
    );

    await expect(
      execution().read({ auth, invocationId: "wrun_invocation" }),
    ).resolves.toMatchObject({
      inputRequests: { question: { prompt: "Proceed?" } },
      revision: 2,
      status: "input_required",
    });
  });

  it("uses workflow return value as terminal result", async () => {
    runsGet.mockResolvedValue(run({ status: "completed" }));
    getReadable.mockReturnValue(eventStream([{ type: "session.completed" }]));
    returnValue.mockResolvedValue({ output: { answer: 42 } });

    await expect(
      execution().read({ auth, invocationId: "wrun_invocation" }),
    ).resolves.toMatchObject({ result: { answer: 42 }, revision: 1, status: "completed" });
  });

  it("subscribes before reading when waiting for a revision", async () => {
    runsGet.mockResolvedValue(run({ status: "running" }));
    getReadable
      .mockReturnValueOnce(eventStream([{ type: "turn.started", data: { turnId: "turn_1" } }]))
      .mockReturnValueOnce(eventStream([]))
      .mockReturnValueOnce(eventStream([{ type: "turn.started", data: { turnId: "turn_1" } }]));

    await expect(
      execution().waitForRevision({
        afterRevision: 0,
        auth,
        invocationId: "wrun_invocation",
        waitMs: 100,
      }),
    ).resolves.toMatchObject({ revision: 1 });

    expect(getReadable.mock.invocationCallOrder[0]).toBeLessThan(
      getReadable.mock.invocationCallOrder[1]!,
    );
  });

  it("terminally cancels the workflow run", async () => {
    runsGet
      .mockResolvedValueOnce(run({ status: "running" }))
      .mockResolvedValueOnce(run({ status: "cancelled" }));
    cancel.mockResolvedValue(undefined);

    await expect(
      execution().cancel({ auth, invocationId: "wrun_invocation" }),
    ).resolves.toMatchObject({ status: "cancelled" });
    expect(cancel).toHaveBeenCalledWith({ cancelReason: "Agent invocation cancelled." });
  });
});

function execution(): WorkflowAgentInvocationExecution {
  return new WorkflowAgentInvocationExecution(agent, "mcp");
}

function run(input: { status: string; initiatorAuth?: SessionAuthContext }) {
  return {
    attributes: {
      "$eve.invocation": "agent",
      "$eve.invocation_token": "invocation:token",
    },
    createdAt: new Date("2026-07-20T00:00:00.000Z"),
    input: [{ serializedContext: { "eve.initiatorAuth": input.initiatorAuth ?? auth } }],
    runId: "wrun_invocation",
    status: input.status,
  };
}

function eventStream(events: readonly unknown[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const chunks = events.map((event) => encoder.encode(`${JSON.stringify(event)}\n`));
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  return Object.assign(stream, { getTailIndex: async () => events.length - 1 });
}
