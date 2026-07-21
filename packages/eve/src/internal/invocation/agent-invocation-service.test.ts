import { describe, expect, it } from "vitest";

import {
  AgentInvocationNotFoundError,
  AgentInvocationService,
  type AgentInvocation,
  type AgentInvocationExecution,
  type AgentInvocationMutationResult,
} from "#internal/invocation/agent-invocation-service.js";
import type { SessionAuthContext } from "#channel/types.js";
import type { InputResponse } from "#runtime/input/types.js";

const alice = auth("alice");
const bob = auth("bob");

class MemoryExecution implements AgentInvocationExecution {
  readonly records = new Map<string, AgentInvocation & { owner: SessionAuthContext }>();
  creates = 0;
  waitResolvers = new Map<string, Array<(invocation: AgentInvocation) => void>>();

  async create(input: Parameters<AgentInvocationExecution["create"]>[0]): Promise<AgentInvocation> {
    this.creates++;
    const invocation = {
      invocationId: `inv_${this.creates}`,
      revision: 0,
      status: "working" as const,
      createdAt: "2026-07-20T00:00:00.000Z",
      pollAfterMs: 1_000,
      owner: input.auth,
    };
    this.records.set(invocation.invocationId, invocation);
    return invocation;
  }

  async read(input: {
    invocationId: string;
    auth: SessionAuthContext;
  }): Promise<AgentInvocation | undefined> {
    const record = this.records.get(input.invocationId);
    if (!record || record.owner.principalId !== input.auth.principalId) {
      return undefined;
    }
    // Return without the owner field
    const { owner: _owner, ...invocation } = record;
    return invocation;
  }

  async waitForRevision(input: {
    invocationId: string;
    auth: SessionAuthContext;
    afterRevision: number;
    waitMs: number;
  }): Promise<AgentInvocation | undefined> {
    const current = this.records.get(input.invocationId);
    if (!current || current.owner.principalId !== input.auth.principalId) return undefined;

    if (current.revision > input.afterRevision) {
      return current;
    }

    // Set up a resolver for this wait
    let resolvers = this.waitResolvers.get(input.invocationId);
    if (!resolvers) {
      resolvers = [];
      this.waitResolvers.set(input.invocationId, resolvers);
    }

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        // Remove this resolver on timeout
        const resolvers = this.waitResolvers.get(input.invocationId);
        if (resolvers) {
          const index = resolvers.indexOf(resolve);
          if (index >= 0) resolvers.splice(index, 1);
        }
        resolve(this.records.get(input.invocationId) ?? undefined);
      }, input.waitMs);

      resolvers!.push((invocation: AgentInvocation) => {
        clearTimeout(timeout);
        resolve(invocation);
      });
    });
  }

  async update(input: {
    invocationId: string;
    auth: SessionAuthContext;
    responses: readonly InputResponse[];
  }): Promise<AgentInvocationMutationResult> {
    const current = this.records.get(input.invocationId);
    if (!current || current.owner.principalId !== input.auth.principalId) {
      return { type: "not_found" };
    }

    if (current.status !== "input_required") {
      return {
        type: "conflict",
        message: `Invocation is ${current.status}, not waiting for input`,
      };
    }

    // Simulate successful update
    const updated = {
      ...current,
      revision: current.revision + 1,
      status: "working" as const,
      inputRequests: undefined,
    };
    this.records.set(input.invocationId, updated);
    this.#notifyWaiters(input.invocationId, updated);
    return { type: "success", invocation: updated };
  }

  async cancel(input: {
    invocationId: string;
    auth: SessionAuthContext;
  }): Promise<AgentInvocation | undefined> {
    const current = this.records.get(input.invocationId);
    if (!current || current.owner.principalId !== input.auth.principalId) return undefined;

    const cancelled = {
      ...current,
      status: "cancelled" as const,
      pollAfterMs: undefined,
    };
    this.records.set(input.invocationId, cancelled);
    this.#notifyWaiters(input.invocationId, cancelled);
    return cancelled;
  }

  // Test helpers
  setInvocationState(invocationId: string, state: Partial<AgentInvocation>) {
    const current = this.records.get(invocationId);
    if (current) {
      const updated = { ...current, ...state };
      this.records.set(invocationId, updated);
      this.#notifyWaiters(invocationId, updated);
    }
  }

  #notifyWaiters(invocationId: string, invocation: AgentInvocation) {
    const resolvers = this.waitResolvers.get(invocationId) ?? [];
    resolvers.forEach((resolve) => resolve(invocation));
    this.waitResolvers.delete(invocationId);
  }
}

describe("AgentInvocationService", () => {
  it("creates new invocations without idempotency", async () => {
    const execution = new MemoryExecution();
    const service = new AgentInvocationService(execution);
    const first = await service.create({
      auth: alice,
      message: "work",
    });
    const second = await service.create({
      auth: alice,
      message: "work",
    });
    expect(second.invocationId).not.toBe(first.invocationId);
    expect(execution.creates).toBe(2);
  });

  it("hides invocations from other principals", async () => {
    const execution = new MemoryExecution();
    const service = new AgentInvocationService(execution);
    const invocation = await service.create({ auth: alice, message: "work" });
    await expect(
      service.read({ auth: bob, invocationId: invocation.invocationId }),
    ).rejects.toBeInstanceOf(AgentInvocationNotFoundError);
  });

  it("handles input requests, updates, and cancellation", async () => {
    const execution = new MemoryExecution();
    const service = new AgentInvocationService(execution);
    const invocation = await service.create({ auth: alice, message: "work" });

    // Simulate input required state
    execution.setInvocationState(invocation.invocationId, {
      status: "input_required",
      revision: 1,
      inputRequests: {
        question: {
          requestId: "question",
          prompt: "Proceed?",
          options: [{ id: "yes", label: "Yes" }],
          action: { kind: "tool-call", toolName: "ask_question", callId: "call1", input: {} },
        },
      },
    });

    expect(
      await service.read({ auth: alice, invocationId: invocation.invocationId }),
    ).toMatchObject({
      status: "input_required",
      inputRequests: { question: { prompt: "Proceed?" } },
    });

    await service.update({
      auth: alice,
      invocationId: invocation.invocationId,
      responses: [{ optionId: "yes", requestId: "question" }],
    });

    await service.cancel({ auth: alice, invocationId: invocation.invocationId });
    expect(
      await service.read({ auth: alice, invocationId: invocation.invocationId }),
    ).toMatchObject({ status: "cancelled" });
  });

  it("waits for revision changes", async () => {
    const execution = new MemoryExecution();
    const service = new AgentInvocationService(execution, { maxWaitMs: 2_000 });
    const invocation = await service.create({ auth: alice, message: "work" });

    const read = service.read({
      auth: alice,
      invocationId: invocation.invocationId,
      afterRevision: 0,
      waitMs: 100,
    });

    // Simulate state change
    setTimeout(() => {
      execution.setInvocationState(invocation.invocationId, {
        status: "completed",
        revision: 1,
        pollAfterMs: undefined,
      });
    }, 50);

    await expect(read).resolves.toMatchObject({ revision: 1, status: "completed" });
  });
});

function auth(principalId: string): SessionAuthContext {
  return { attributes: {}, authenticator: "test", principalId, principalType: "user" };
}
