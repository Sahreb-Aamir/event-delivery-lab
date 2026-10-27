import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { deadLetterOutput } from "./dead-letter-client.js";
import { parsePort, uuid } from "./lab-http-client.js";

const client = new Client({ name: "lab-dead-letter-cli", version: "0.1.0" });
try {
  const eventId = process.argv[2] === undefined || process.argv[2] === "all" ? undefined : uuid.parse(process.argv[2]);
  const limit = z.number().int().min(1).max(20).parse(process.argv[3] === undefined ? 10 : Number(process.argv[3]));
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [fileURLToPath(new URL("./index.js", import.meta.url))],
    env: { LAB_PORT: String(parsePort(process.env.LAB_PORT)) }, stderr: "inherit" });
  await client.connect(transport);
  const result = await client.callTool({ name: "inspect_dead_letters", arguments: { ...(eventId ? { eventId } : {}), limit } });
  console.log(JSON.stringify(deadLetterOutput.parse(result.structuredContent), null, 2));
  if (result.isError) process.exitCode = 1;
} catch {
  console.error("Inspection failed. Usage: npm run inspect-dead-letters -- [event-uuid|all] [limit-1-to-20]");
  process.exitCode = 1;
} finally { await client.close(); }
