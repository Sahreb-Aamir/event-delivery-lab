import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { parsePort } from "./event-client.js";
import { createServer } from "./server.js";

try {
  const server = createServer(parsePort(process.env.LAB_PORT));
  await server.connect(new StdioServerTransport());
} catch {
  // stdout belongs exclusively to the MCP protocol; never log API data or exception details.
  console.error("Diagnostics could not start. Check LAB_PORT and the MCP client configuration.");
  process.exitCode = 1;
}
