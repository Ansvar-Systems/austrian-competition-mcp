#!/usr/bin/env node

/**
 * Austrian Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying BWB (Bundeswettbewerbsbehörde — Austrian
 * Federal Competition Authority) decisions, merger control cases, and sector
 * enforcement activity under Austrian competition law (KartG — Kartellgesetz).
 *
 * Tool prefix: at_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { searchDecisions, getDecision, searchMergers, getMerger, listSectors } from "./db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(readFileSync(join(__dirname, "..", "package.json"), "utf8")) as { version: string };
  pkgVersion = pkg.version;
} catch { /* fallback */ }

const SERVER_NAME = "austrian-competition-mcp";

const TOOLS = [
  {
    name: "at_comp_search_decisions",
    description: "Full-text search across BWB enforcement decisions (abuse of dominance, cartel, sector inquiries) and Kartellgericht (Cartel Court) decisions. Returns matching decisions with case number, parties, outcome, fine amount, and KartG articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query in German (e.g., 'Marktmissbrauch', 'Kartellabsprache', 'Preisabsprachen')" },
        type: { type: "string", enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"], description: "Filter by decision type. Optional." },
        sector: { type: "string", description: "Filter by sector ID (e.g., 'energie', 'lebensmittelhandel', 'telekommunikation'). Optional." },
        outcome: { type: "string", enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"], description: "Filter by outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "at_comp_get_decision",
    description: "Get a specific BWB or Kartellgericht decision by case number (e.g., 'BWB/Z-1234', 'KOG/24G1/23').",
    inputSchema: { type: "object" as const, properties: { case_number: { type: "string", description: "BWB or Kartellgericht case number" } }, required: ["case_number"] },
  },
  {
    name: "at_comp_search_mergers",
    description: "Search BWB merger control decisions (Fusionskontrolle). Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query in German (e.g., 'Lebensmittelhandel', 'Telekommunikation', 'Energieversorgung')" },
        sector: { type: "string", description: "Filter by sector ID. Optional." },
        outcome: { type: "string", enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"], description: "Filter by merger outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "at_comp_get_merger",
    description: "Get a specific merger control decision by case number (e.g., 'BWB/Z-1200', 'BWB/Z-1305').",
    inputSchema: { type: "object" as const, properties: { case_number: { type: "string", description: "BWB merger case number" } }, required: ["case_number"] },
  },
  {
    name: "at_comp_list_sectors",
    description: "List all sectors with BWB enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "at_comp_about",
    description: "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
];

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetDecisionArgs = z.object({ case_number: z.string().min(1) });
const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetMergerArgs = z.object({ case_number: z.string().min(1) });

function textContent(data: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(data, null, 2) }] };
}
function errorContent(message: string) {
  return { content: [{ type: "text" as const, text: message }], isError: true as const };
}

const server = new Server({ name: SERVER_NAME, version: pkgVersion }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;
  try {
    switch (name) {
      case "at_comp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({ query: parsed.query, type: parsed.type, sector: parsed.sector, outcome: parsed.outcome, limit: parsed.limit });
        return textContent({ results, count: results.length });
      }
      case "at_comp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.case_number);
        if (!decision) return errorContent(`Decision not found: ${parsed.case_number}`);
        return textContent(decision);
      }
      case "at_comp_search_mergers": {
        const parsed = SearchMergersArgs.parse(args);
        const results = searchMergers({ query: parsed.query, sector: parsed.sector, outcome: parsed.outcome, limit: parsed.limit });
        return textContent({ results, count: results.length });
      }
      case "at_comp_get_merger": {
        const parsed = GetMergerArgs.parse(args);
        const merger = getMerger(parsed.case_number);
        if (!merger) return errorContent(`Merger case not found: ${parsed.case_number}`);
        return textContent(merger);
      }
      case "at_comp_list_sectors": {
        const sectors = listSectors();
        return textContent({ sectors, count: sectors.length });
      }
      case "at_comp_about":
        return textContent({
          name: SERVER_NAME, version: pkgVersion,
          description: "BWB (Bundeswettbewerbsbehörde — Austrian Federal Competition Authority) MCP server. Provides access to Austrian competition law enforcement decisions, Kartellgericht (Cartel Court) decisions, merger control cases, and sector enforcement data under the KartG (Kartellgesetz).",
          data_source: "BWB (https://www.bwb.gv.at/)",
          coverage: { decisions: "Abuse of dominance (Marktmissbrauch), cartel enforcement, sector inquiries", mergers: "Merger control decisions (Fusionskontrolle) — Phase I and Phase II", sectors: "Energie, Lebensmittelhandel, Telekommunikation, Medien, Banken, Versicherungen, Gesundheit, Verkehr" },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    return errorContent(`Error executing ${name}: ${err instanceof Error ? err.message : String(err)}`);
  }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
