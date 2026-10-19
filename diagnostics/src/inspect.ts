import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { fileURLToPath } from "node:url";
import { output, parsePort, uuid } from "./event-client.js";

// A model-free MCP client for reproducing a tool call from a terminal.
const client = new Client({ name: "lab-diagnostics-cli", version: "0.1.0" });
try {
  const eventId = uuid.parse(process.argv[2]);
  const attemptLimit = process.argv[3] === undefined ? 10 : Number(process.argv[3]);
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [fileURLToPath(new URL("./index.js", import.meta.url))],
    env: { LAB_PORT: String(parsePort(process.env.LAB_PORT)) }, stderr: "inherit" });
  await client.connect(transport);
  const result = await client.callTool({ name: "inspect_event", arguments: { eventId, attemptLimit } });
  console.log(JSON.stringify(output.parse(result.structuredContent), null, 2));
  if (result.isError) process.exitCode = 1;
} catch {
  console.error("Inspection failed. Usage: npm run inspect -- <event-uuid> [attempt-limit-1-to-20]");
  process.exitCode = 1;
} finally { await client.close(); }
