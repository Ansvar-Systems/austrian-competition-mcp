# Privacy and Confidentiality

## Data Flows

### MCP (Model Context Protocol) Architecture

This Tool uses the **Model Context Protocol (MCP)** to communicate with AI clients:

```
User Query -> MCP Client (Claude Desktop/API) -> Anthropic Cloud -> MCP Server (This Tool) -> Database
```

**What This Means:**

1. **Queries Transit Anthropic Infrastructure**: Your queries are sent to Anthropic's servers for LLM processing
2. **Query Logging**: Anthropic may log queries subject to their [Privacy Policy](https://www.anthropic.com/legal/privacy)
3. **Tool Responses**: Database responses return through the same path
4. **No Direct Control**: You do not control Anthropic's data processing, retention, or security practices

### What Gets Transmitted

When you use this Tool through Claude Desktop or API:

- **Query Text**: The full text of your search queries
- **Tool Parameters**: Regulation identifiers, provision references, filters
- **Tool Responses**: Regulatory text, decision summaries, requirement details
- **Metadata**: Timestamps, user agent, API keys (handled by Anthropic)

**What Does NOT Get Transmitted:**
- Direct database access (Tool runs locally, queries Anthropic only for LLM processing)
- Files on your computer
- Your full conversation history (unless using Claude.ai web interface)

---

## GDPR Considerations

Under **GDPR Article 28**, when you use a service that processes personal data:

- You are the **Data Controller**
- Anthropic is a **Data Processor**
- A **Data Processing Agreement (DPA)** may be required
- You must ensure adequate technical and organizational measures

**ACTION REQUIRED**: Review Anthropic's current policy and negotiate appropriate terms if needed for professional use.

---

## This Tool's Data Practices

**Local Data (Not Transmitted):**
- **Database**: Stored locally, never transmitted
- **Query History**: NOT logged by this Tool
- **User Data**: No personal data collected or stored

**Transmitted Data (via Anthropic):**
- Query text and tool parameters (logged per Anthropic policy)
- Tool responses (logged per Anthropic policy)

---

## Safe Use Guidelines

### Low Risk: General Research

Safe to use through Claude API for non-client-specific queries about regulatory requirements, framework comparisons, and general compliance research.

### Higher Risk: Client-Specific Queries

For queries that contain or could reveal client-specific information, confidential business details, or sensitive compliance matters:

- Remove ALL identifying details before querying
- Use general terms, not case-specific facts
- Consider on-premise deployment for sensitive matters

### On-Premise Deployment

For **confidential matters**, deploy this Tool with a **self-hosted LLM** to eliminate external data transmission:

```
User Query -> Local MCP Client -> Local LLM (no external API) -> MCP Server (This Tool) -> Local Database
```

This keeps all data local with no external transmission.

---

## Security Best Practices

1. **API Key Protection**: Store Anthropic API keys in a secure vault, never in code
2. **Encrypted Storage**: Database file on encrypted disk
3. **Access Control**: Limit database file permissions to your user account only
4. **Network Security**: Use VPN or private network if accessing remote MCP server
5. **Audit Trail**: Log when and how the Tool is used for client matters

---

## Questions

For questions about privacy and data handling:

1. **Anthropic Privacy**: Contact privacy@anthropic.com
2. **Tool-Specific**: Open issue on [GitHub](https://github.com/Ansvar-Systems/austrian-competition-mcp/issues)

---

**Last Updated**: 2026-04-03
