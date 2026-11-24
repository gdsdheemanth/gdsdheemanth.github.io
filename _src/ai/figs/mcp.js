/* mcp: Model Context Protocol and agent interoperability, original drawings */
Object.assign(FIGS, {

mcp_arch:{ t:"Hosts, clients and servers", cap:"The host is the AI application the user works in. It creates one MCP client per server it uses, and each client talks to exactly one server. Local servers run as subprocesses over stdio; remote servers are reached over Streamable HTTP. The host alone decides what reaches the model and what needs the user's approval.", svg:S("mcp1", 300, "A host containing the model loop, a permission layer and three MCP clients; each client connects to one server, two local over stdio and one remote over HTTP",
  B(20, 20, 330, 260, "box", [""]) + T(40, 44, "Host: IDE, chat app or your agent", "b") +
  B(40, 60, 140, 60, "ka", ["Model loop", "tools in prompt"]) +
  B(200, 60, 130, 60, "st", ["Permissions", "consent, audit"]) +
  B(40, 140, 290, 36, "sp", ["MCP client 1"], false) +
  B(40, 186, 290, 36, "sp", ["MCP client 2"], false) +
  B(40, 232, 290, 36, "sp", ["MCP client 3"], false) +
  B(470, 130, 270, 56, "fill", ["Warehouse server", "local subprocess"]) +
  B(470, 40, 270, 56, "fill", ["Git server", "local subprocess"]) +
  B(470, 220, 270, 56, "pr", ["Trino server", "remote, OAuth"]) +
  A("mcp1", "M330 158 L464 72") + T(400, 104, "stdio", "sm", "middle") +
  A("mcp1", "M330 204 L464 160") + T(400, 176, "stdio", "sm", "middle") +
  A("mcp1", "M330 250 L464 248", "acc") + T(400, 242, "Streamable HTTP", "sm", "middle") +
  T(605, 18, "one client per server, JSON-RPC 2.0", "sm", "middle")) },

mcp_mrtr:{ t:"Asking the user mid-call: multi round-trip requests", cap:"Since the 2026-07-28 revision a server never sends a request to the client. When it needs input it answers the tool call with an input_required result and an opaque, integrity-protected requestState. The client asks the user, then retries the same call with a new id, the answers and the echoed state. Any server replica can finish the call.", svg:S("mcp2", 320, "Sequence: client calls a tool, server returns input_required with an elicitation request, the client asks the user, then retries with input responses and the server completes",
  B(20, 20, 140, 40, "ka", ["User"], true) + B(310, 20, 140, 40, "sp", ["MCP client"], true) + B(600, 20, 140, 40, "fill", ["MCP server"], true) +
  A("mcp2", "M90 60 L90 306", "dash") + A("mcp2", "M380 60 L380 306", "dash") + A("mcp2", "M670 60 L670 306", "dash") +
  A("mcp2", "M380 90 L664 90") + T(525, 84, "1. tools/call id 1", "sm", "middle") +
  A("mcp2", "M670 130 L386 130", "kac") + T(525, 124, "2. input_required + requestState", "sm", "middle") +
  A("mcp2", "M380 170 L96 170") + T(235, 164, "3. show form", "sm", "middle") +
  A("mcp2", "M90 205 L374 205", "kac") + T(235, 199, "4. accept, values", "sm", "middle") +
  A("mcp2", "M380 245 L664 245") + T(525, 239, "5. tools/call id 2 + answers + state", "sm", "middle") +
  A("mcp2", "M670 285 L386 285", "acc") + T(525, 279, "6. complete result", "sm", "middle")) },

mcp_auth:{ t:"Authorization for a remote MCP server", cap:"The MCP server is an OAuth resource server. A first request without a token gets a 401 that points to its protected resource metadata, which names the authorization server. The client runs an authorization code flow with PKCE and a resource parameter, so the token is bound to this server. The server checks the audience and never forwards the token downstream; it uses its own credentials there.", svg:S("mcp3", 300, "Client gets 401 from MCP server, reads resource metadata, gets a token from the authorization server with PKCE and resource, then calls the server, which uses its own database role for PostgreSQL",
  B(20, 110, 150, 70, "sp", ["MCP client", "in the host"]) +
  B(300, 20, 170, 60, "st", ["Authorization", "server (IdP)"]) +
  B(300, 210, 170, 70, "fill", ["MCP server", "resource server,", "checks audience"]) +
  B(590, 210, 150, 70, "pr", ["PostgreSQL", "own role,", "secret in Vault"]) +
  A("mcp3", "M170 160 L294 230") + T(196, 218, "1. no token", "sm") +
  A("mcp3", "M294 250 L176 176", "kac") + T(180, 252, "2. 401 + metadata URL", "sm") +
  A("mcp3", "M170 130 L294 60") + T(150, 76, "3. code + PKCE + resource", "sm") +
  A("mcp3", "M300 76 L176 140", "kac") + T(255, 112, "4. token", "sm", "middle") +
  A("mcp3", "M170 170 L294 270", "acc") + T(252, 296, "5. Bearer token, aud = this server", "sm", "middle") +
  A("mcp3", "M470 245 L584 245") + T(527, 238, "own creds", "sm", "middle") +
  T(620, 150, "never pass the", "sm", "middle") + T(620, 166, "user's token through", "sm", "middle")) },

mcp_a2a:{ t:"MCP and A2A side by side", cap:"MCP connects one agent to tools and data: the server is a capability the agent controls. A2A connects agents to other agents: the remote agent is an opaque peer with its own model, tools and policies, reached through its Agent Card. Most real systems use both.", svg:S("mcp4", 290, "An orchestrator agent uses MCP to reach a database server and a files server, and A2A to reach a remote billing agent, which itself uses MCP for its own tools",
  B(40, 110, 200, 70, "ka", ["Data platform agent", "your orchestrator"]) +
  B(40, 230, 200, 46, "fill", ["Warehouse MCP server"], false) +
  B(40, 10, 200, 46, "fill", ["Lineage MCP server"], false) +
  A("mcp4", "M140 180 L140 224") + T(150, 206, "MCP: tools/call", "sm") +
  A("mcp4", "M140 110 L140 62") + T(150, 90, "MCP: resources/read", "sm") +
  B(500, 110, 220, 70, "st", ["Billing agent", "other team, other stack"]) +
  A("mcp4", "M240 145 L494 145", "acc") + T(367, 136, "A2A: SendMessage", "sm", "middle") + T(367, 166, "task, artifacts", "sm", "middle") +
  B(500, 10, 220, 46, "sp", ["Agent Card", "/.well-known/agent-card.json"]) +
  A("mcp4", "M610 110 L610 62", "dash") + T(620, 90, "discovered first", "sm") +
  B(500, 230, 220, 46, "fill", ["its own MCP servers"], false) +
  A("mcp4", "M610 180 L610 224") + T(620, 206, "hidden from caller", "sm")) }

});
