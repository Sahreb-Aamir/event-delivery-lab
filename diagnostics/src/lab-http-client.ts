import { z } from "zod";

export const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
export const errorCode = z.enum(["TIMEOUT", "CANCELLED", "UNAVAILABLE", "INVALID_RESPONSE", "RESPONSE_TOO_LARGE", "BUSY"]);
export const MAX_RESPONSE_BYTES = 262_144;
export const QUERY_TIMEOUT_MS = 3000;
type ReadResult = { ok: true; body: unknown } | { ok: false; error: z.infer<typeof errorCode>; status?: number };

export function parsePort(value = "8080"): number {
  if (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 65535) throw new Error("LAB_PORT must be 1–65535");
  return Number(value);
}

/** Shared admission and byte/deadline bounds for the two fixed, read-only lab endpoints. */
export class LabHttpClient {
  private active = 0;
  constructor(private readonly port: number) { parsePort(String(port)); }

  event(eventId: string, signal?: AbortSignal): Promise<ReadResult> {
    return this.read(`/api/events/${uuid.parse(eventId).toLowerCase()}`, signal);
  }

  deadLetters(eventId: string | undefined, limit: number, signal?: AbortSignal): Promise<ReadResult> {
    z.number().int().min(1).max(20).parse(limit);
    const query = new URLSearchParams({ limit: String(limit) });
    if (eventId !== undefined) query.set("eventId", uuid.parse(eventId).toLowerCase());
    return this.read(`/api/dead-letters?${query}`, signal);
  }

  private async read(path: string, callerSignal?: AbortSignal): Promise<ReadResult> {
    if (this.active >= 4) return { ok: false, error: "BUSY" };
    this.active++;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), QUERY_TIMEOUT_MS);
    const signal = callerSignal ? AbortSignal.any([controller.signal, callerSignal]) : controller.signal;
    try {
      const response = await fetch(`http://127.0.0.1:${this.port}${path}`,
        { method: "GET", redirect: "error", signal, headers: { Accept: "application/json" } });
      if (!response.ok) return { ok: false, error: response.status === 429 ? "BUSY" : "UNAVAILABLE", status: response.status };
      if (response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !== "application/json" || !response.body) {
        return { ok: false, error: "INVALID_RESPONSE" };
      }
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > MAX_RESPONSE_BYTES) return { ok: false, error: "RESPONSE_TOO_LARGE" };
          chunks.push(value);
        }
      } finally { reader.releaseLock(); }
      return { ok: true, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown };
    } catch (failure) {
      if (callerSignal?.aborted) return { ok: false, error: "CANCELLED" };
      if (controller.signal.aborted) return { ok: false, error: "TIMEOUT" };
      return { ok: false, error: failure instanceof SyntaxError ? "INVALID_RESPONSE" : "UNAVAILABLE" };
    } finally {
      clearTimeout(timer);
      controller.abort();
      this.active--;
    }
  }
}
