/**
 * Ingestion crawler for the BWB (Bundeswettbewerbsbehörde) MCP server.
 *
 * Scrapes competition decisions, merger control filings, and sector inquiry
 * data from bwb.gv.at and populates the SQLite database.
 *
 * Data sources:
 *   1. Decisions (Kartelle & Marktmachtmissbrauch / Entscheidungen)
 *      — paginated list at /kartelle_marktmachtmissbrauch/entscheidungen
 *      — individual detail pages at /kartelle_marktmachtmissbrauch/entscheidungen/detail/<slug>
 *   2. Merger control filings (Zusammenschlüsse)
 *      — year-based listings at /zusammenschluesse/<year>
 *      — individual merger pages at /zusammenschluesse/<year>/<id>
 *   3. Sector inquiries (Branchenuntersuchungen)
 *      — listing at /branchenuntersuchungen
 *      — news pages with sector inquiry results
 *   4. News archive (Pressemitteilungen)
 *      — /news/news-<year> pages for supplementary decision announcements
 *
 * The BWB website is a TYPO3 CMS site. It uses tx_news_pi1 pagination for
 * the decisions list and year-based URL segments for merger filings.
 *
 * Usage:
 *   npx tsx scripts/ingest-bwb.ts                # full crawl
 *   npx tsx scripts/ingest-bwb.ts --resume        # resume from last checkpoint
 *   npx tsx scripts/ingest-bwb.ts --dry-run       # log what would be inserted
 *   npx tsx scripts/ingest-bwb.ts --force          # drop and recreate DB first
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["BWB_DB_PATH"] ?? "data/bwb.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.bwb.gv.at";

const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarBWBCrawler/1.0 (+https://github.com/Ansvar-Systems/austrian-competition-mcp)";

/** Decision list: TYPO3 paginated listing. */
const DECISIONS_LIST_URL =
  `${BASE_URL}/kartelle_marktmachtmissbrauch/entscheidungen`;

/** Merger filings are grouped by year. Crawl from 2002 (BWB founded) to now. */
const MERGER_START_YEAR = 2002;
const MERGER_END_YEAR = new Date().getFullYear();

/** Sector inquiries main page. */
const SECTOR_INQUIRIES_URL = `${BASE_URL}/branchenuntersuchungen`;

/** News pages for supplementary decisions. */
const NEWS_START_YEAR = 2017;
const NEWS_END_YEAR = new Date().getFullYear();

/** Maximum decision list pages to crawl (TYPO3 paginated, ~10 items per page). */
const MAX_DECISION_PAGES = 30;

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const force = args.includes("--force");
const dryRun = args.includes("--dry-run");
const resume = args.includes("--resume");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "de-AT,de;q=0.9,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// URL discovery — decisions (TYPO3 paginated list)
// ---------------------------------------------------------------------------

/**
 * Discover decision detail page URLs from the paginated decisions listing.
 *
 * The BWB decisions list is a TYPO3 news extension page. Pagination uses
 * query parameters: tx_news_pi1[@widget_0][currentPage]=N
 *
 * Each item links to a detail page at:
 *   /kartelle_marktmachtmissbrauch/entscheidungen/detail/<slug>
 */
async function discoverDecisionUrls(): Promise<string[]> {
  const urls: string[] = [];
  console.log("\n--- Discovering decision URLs ---");

  for (let page = 1; page <= MAX_DECISION_PAGES; page++) {
    const listUrl =
      page === 1
        ? DECISIONS_LIST_URL
        : `${DECISIONS_LIST_URL}?tx_news_pi1%5B%40widget_0%5D%5BcurrentPage%5D=${page}`;

    console.log(`  Fetching decisions page ${page}/${MAX_DECISION_PAGES}...`);
    const html = await rateLimitedFetch(listUrl);
    if (!html) {
      console.warn(`  [WARN] Could not fetch decisions page ${page}`);
      break;
    }

    const $ = cheerio.load(html);

    // Extract links to detail pages. TYPO3 news extension uses links
    // containing "/detail/" in the href.
    const pageLinks: string[] = [];
    $('a[href*="/entscheidungen/detail/"]').each((_i, el) => {
      const href = $(el).attr("href");
      if (href) {
        const absoluteUrl = href.startsWith("http")
          ? href
          : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
        pageLinks.push(absoluteUrl.split("?")[0]!.split("#")[0]!);
      }
    });

    if (pageLinks.length === 0) {
      console.log(`  No more decision links found on page ${page}. Stopping.`);
      break;
    }

    // Deduplicate within this page
    const unique = [...new Set(pageLinks)];
    urls.push(...unique);
    console.log(`    Found ${unique.length} decision links (total: ${urls.length})`);
  }

  const deduped = [...new Set(urls)];
  console.log(`  Discovered ${deduped.length} unique decision URLs`);
  return deduped;
}

// ---------------------------------------------------------------------------
// URL discovery — merger filings (year-based listings)
// ---------------------------------------------------------------------------

/**
 * Discover merger filing URLs from the year-based merger control pages.
 *
 * The BWB lists mergers at /zusammenschluesse/<year> (German) and
 * /en/merger-control/<year> (English). Some years use /en/merger_control/<year>.
 *
 * Individual mergers link to /zusammenschluesse/<year>/<id> or
 * /en/merger-control/<year>/<id>.
 */
async function discoverMergerUrls(): Promise<string[]> {
  const urls: string[] = [];
  console.log("\n--- Discovering merger URLs ---");

  for (let year = MERGER_END_YEAR; year >= MERGER_START_YEAR; year--) {
    // Try multiple URL patterns that the BWB site uses
    const yearUrls = [
      `${BASE_URL}/zusammenschluesse/${year}`,
      `${BASE_URL}/en/merger-control/${year}`,
      `${BASE_URL}/en/merger_control/${year}`,
    ];

    let found = false;
    for (const yearUrl of yearUrls) {
      console.log(`  Fetching mergers for ${year}...`);
      const html = await rateLimitedFetch(yearUrl);
      if (!html) continue;

      const $ = cheerio.load(html);
      const pageLinks: string[] = [];

      // Look for individual merger links. These are typically table rows or
      // list items linking to a detail page with a numeric ID.
      $("a[href]").each((_i, el) => {
        const href = $(el).attr("href") ?? "";
        // Match merger detail links: /zusammenschluesse/YYYY/NNN or /merger-control/YYYY/NNN
        if (
          /\/(zusammenschluesse|merger-control|merger_control)\/\d{4}\/\d+/.test(href) ||
          /\/merger\/\d+/.test(href)
        ) {
          const absoluteUrl = href.startsWith("http")
            ? href
            : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
          pageLinks.push(absoluteUrl.split("?")[0]!.split("#")[0]!);
        }
      });

      // Also look for table-based merger listings (BWB uses tables for merger lists)
      $("table tbody tr").each((_i, el) => {
        const link = $(el).find("a[href]").first();
        const href = link.attr("href") ?? "";
        if (href && (href.includes("zusammenschl") || href.includes("merger"))) {
          const absoluteUrl = href.startsWith("http")
            ? href
            : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
          pageLinks.push(absoluteUrl.split("?")[0]!.split("#")[0]!);
        }
      });

      if (pageLinks.length > 0) {
        const unique = [...new Set(pageLinks)];
        urls.push(...unique);
        console.log(`    Found ${unique.length} merger links for ${year} (total: ${urls.length})`);
        found = true;
        break; // Found valid year page, skip alternative URL patterns
      }
    }

    if (!found) {
      // Try extracting merger info directly from the year listing page text
      // (some years only have a table on the listing page, no detail links)
      console.log(`    No individual merger links for ${year}`);
    }
  }

  const deduped = [...new Set(urls)];
  console.log(`  Discovered ${deduped.length} unique merger URLs`);
  return deduped;
}

// ---------------------------------------------------------------------------
// URL discovery — news archive for supplementary decisions
// ---------------------------------------------------------------------------

/**
 * Discover decision-relevant news URLs from the BWB news archive.
 *
 * BWB publishes press releases about decisions, fines, and sector inquiries
 * in /news/news-<year>/ pages. We filter for competition-relevant news.
 */
async function discoverNewsUrls(): Promise<string[]> {
  const urls: string[] = [];
  console.log("\n--- Discovering news URLs ---");

  /** Keywords that indicate a news item is about a competition decision. */
  const RELEVANT_SLUGS = [
    "kartell",
    "geldbu",
    "geldbusse",
    "missbrauch",
    "marktmacht",
    "zusammenschluss",
    "fusionskontrolle",
    "branchenuntersuchung",
    "sektoruntersuchung",
    "kronzeug",
    "verpflichtungszusag",
    "wettbewerbsbeschr",
    "preisabsprach",
    "baukartell",
    "vertikalvereinbar",
  ];

  for (let year = NEWS_END_YEAR; year >= NEWS_START_YEAR; year--) {
    const newsUrl = `${BASE_URL}/news/news-${year}`;
    console.log(`  Fetching news for ${year}...`);
    const html = await rateLimitedFetch(newsUrl);
    if (!html) {
      // Try alternative URL patterns
      const altUrl = `${BASE_URL}/en/news/news-${year}`;
      const altHtml = await rateLimitedFetch(altUrl);
      if (!altHtml) {
        console.log(`    No news page found for ${year}`);
        continue;
      }
    }

    const pageHtml = html ?? "";
    const $ = cheerio.load(pageHtml);
    const pageLinks: string[] = [];

    $('a[href*="/news/"]').each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      // Only collect detail page links (contain /detail/)
      if (!href.includes("/detail/")) return;

      const slug = href.split("/detail/").pop() ?? "";
      const isRelevant = RELEVANT_SLUGS.some((kw) =>
        slug.toLowerCase().includes(kw),
      );
      if (!isRelevant) return;

      const absoluteUrl = href.startsWith("http")
        ? href
        : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
      pageLinks.push(absoluteUrl.split("?")[0]!.split("#")[0]!);
    });

    if (pageLinks.length > 0) {
      const unique = [...new Set(pageLinks)];
      urls.push(...unique);
      console.log(`    Found ${unique.length} relevant news links for ${year} (total: ${urls.length})`);
    } else {
      console.log(`    No relevant news links for ${year}`);
    }
  }

  const deduped = [...new Set(urls)];
  console.log(`  Discovered ${deduped.length} unique news URLs`);
  return deduped;
}

// ---------------------------------------------------------------------------
// Page parsing — extract structured data from decision detail pages
// ---------------------------------------------------------------------------

/** Parse a German date string to ISO format (YYYY-MM-DD). */
function parseGermanDate(raw: string): string | null {
  if (!raw) return null;

  // Pattern: dd.mm.yyyy
  const dotMatch = raw.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (dotMatch) {
    const [, day, month, year] = dotMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Pattern: dd. Monat yyyy (German month names)
  const germanMonths: Record<string, string> = {
    jänner: "01",
    januar: "01",
    februar: "02",
    feber: "02",
    märz: "03",
    april: "04",
    mai: "05",
    juni: "06",
    juli: "07",
    august: "08",
    september: "09",
    oktober: "10",
    november: "11",
    dezember: "12",
  };

  const nameMatch = raw
    .toLowerCase()
    .match(
      /(\d{1,2})\.\s*(jänner|januar|februar|feber|märz|april|mai|juni|juli|august|september|oktober|november|dezember)\s+(\d{4})/,
    );
  if (nameMatch) {
    const day = nameMatch[1]!.padStart(2, "0");
    const month = germanMonths[nameMatch[2]!] ?? "01";
    const year = nameMatch[3]!;
    return `${year}-${month}-${day}`;
  }

  // Pattern: yyyy-mm-dd (already ISO)
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return isoMatch[0]!;

  return null;
}

/**
 * Extract metadata fields from a BWB page.
 *
 * BWB TYPO3 pages use various patterns for metadata:
 *   - Inline date text (e.g., "Datum: 15.06.2023")
 *   - Definition lists in the sidebar
 *   - Table-based metadata blocks
 *   - Meta tags and structured data
 */
function extractMetadata(
  $: cheerio.CheerioAPI,
): Record<string, string> {
  const meta: Record<string, string> = {};

  // Pattern 1: TYPO3 news metadata fields
  $(".news-list-date, .article-date, time").each((_i, el) => {
    const dateText =
      $(el).attr("datetime") ?? $(el).text().trim();
    if (dateText) meta["datum"] = dateText;
  });

  // Pattern 2: Definition list (dl/dt/dd)
  $("dl dt").each((_i, el) => {
    const label = $(el).text().trim().replace(/:$/, "").toLowerCase();
    const dd = $(el).next("dd");
    if (dd.length > 0) {
      meta[label] = dd.text().trim();
    }
  });

  // Pattern 3: Labelled spans or divs (TYPO3 content elements)
  $(".ce-bodytext strong, .frame-default strong, p strong, dt").each(
    (_i, el) => {
      const label = $(el).text().trim().replace(/:$/, "").toLowerCase();
      const sibling = $(el).parent().text().trim();
      const value = sibling.replace($(el).text().trim(), "").replace(/^:\s*/, "").trim();
      if (label && value && value.length < 500) {
        meta[label] = value;
      }
    },
  );

  // Pattern 4: Table rows with label/value pairs
  $("table tr").each((_i, el) => {
    const cells = $(el).find("td, th");
    if (cells.length >= 2) {
      const label = $(cells[0]).text().trim().replace(/:$/, "").toLowerCase();
      const value = $(cells[1]).text().trim();
      if (label && value) {
        meta[label] = value;
      }
    }
  });

  // Pattern 5: og:title and description meta tags
  const ogTitle = $('meta[property="og:title"]').attr("content");
  if (ogTitle) meta["og_title"] = ogTitle.trim();
  const ogDesc = $('meta[property="og:description"]').attr("content");
  if (ogDesc) meta["og_description"] = ogDesc.trim();

  return meta;
}

/**
 * Extract a case number from BWB page content.
 *
 * BWB case numbers follow patterns:
 *   - BWB/Z-NNNN  (investigations / Ermittlungsverfahren)
 *   - BWB/K-NNN   (Kartellgericht applications)
 *   - BWB/FD-NNN  (foreign direct investment)
 *   - KOG/NNGN/NN (Kartellobergericht / Supreme Cartel Court)
 *   - Z-NNNN      (older format without BWB prefix)
 */
function extractCaseNumber(
  title: string,
  bodyText: string,
  url: string,
): string {
  const fullText = `${title} ${bodyText}`;

  // BWB/Z-NNNN or BWB/K-NNN or BWB/FD-NNN
  const bwbMatch = fullText.match(/BWB\/[A-Z]{1,3}-\d{2,5}/);
  if (bwbMatch) return bwbMatch[0]!;

  // KOG reference (Kartellobergericht)
  const kogMatch = fullText.match(/KOG\/\d+[A-Z]\d+\/\d{2,4}/);
  if (kogMatch) return kogMatch[0]!;

  // Kartellgericht reference (e.g., 27 Kt 200/03)
  const ktMatch = fullText.match(/\d+\s*Kt\s*\d+\/\d{2,4}/);
  if (ktMatch) return ktMatch[0]!.replace(/\s+/g, " ");

  // Z-NNNN (older format)
  const zMatch = fullText.match(/Z-\d{3,5}/);
  if (zMatch) return `BWB/${zMatch[0]}`;

  // Fallback: generate from URL slug
  const slug = url.split("/detail/").pop() ?? url.split("/").pop() ?? "unknown";
  const shortSlug = slug.slice(0, 80).replace(/-+$/, "");
  return `BWB-WEB/${shortSlug}`;
}

/** Extract fine amounts from German text. */
function extractFineAmount(text: string): number | null {
  // Pattern: EUR X.XXX.XXX or EUR X,XX Mio or EUR X Millionen
  const patterns = [
    // "EUR 8,2 Millionen" or "EUR 8,2 Mio."
    /EUR\s+([\d.,]+)\s*(?:Mio|Million)/i,
    // "Geldbuße von EUR 15.000.000" or "iHv EUR 9.800.000"
    /(?:Geldb[uü][sß]e|iHv|Höhe\s+von)\s+(?:EUR|€)\s*([\d.]+(?:,\d{1,2})?)/i,
    // "EUR 210.000"
    /EUR\s+([\d.]+(?:,\d{1,2})?)\s/,
    // "€ 3.000.000"
    /€\s*([\d.]+(?:,\d{1,2})?)/,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      let numStr = match[1]!;
      // Check if the match involves Mio/Millionen multiplier
      const isMillion = /Mio|Million/i.test(match[0]!);

      // German number format: dots as thousands separators, comma as decimal
      numStr = numStr.replace(/\./g, "").replace(",", ".");
      const value = parseFloat(numStr);
      if (!isNaN(value)) {
        return isMillion ? value * 1_000_000 : value;
      }
    }
  }

  return null;
}

/** Extract legal article references from Austrian/EU competition law. */
function extractLegalArticles(text: string): string | null {
  const articles: string[] = [];

  // Austrian Cartel Act (KartG)
  const kartgMatches = text.matchAll(/§\s*\d+(?:\s*(?:Abs\.?\s*\d+)?(?:\s*Z\s*\d+)?)?\s*KartG/g);
  for (const m of kartgMatches) articles.push(m[0]!.replace(/\s+/g, " "));

  // Austrian Competition Act (WettbG)
  const wettbgMatches = text.matchAll(
    /§\s*\d+(?:\s*(?:Abs\.?\s*\d+)?(?:\s*Z\s*\d+)?)?\s*Wettb(?:G|ewerbsgesetz)/g,
  );
  for (const m of wettbgMatches) articles.push(m[0]!.replace(/\s+/g, " "));

  // UWG (Unlauterer Wettbewerb / Unfair Competition Act)
  const uwgMatches = text.matchAll(/§\s*\d+\s*UWG/g);
  for (const m of uwgMatches) articles.push(m[0]!.replace(/\s+/g, " "));

  // EU Treaty articles (TFUE / AEUV)
  const euMatches = text.matchAll(/Art\.?\s*\d+\s*(?:TFUE|AEUV|TFEU|EGV)/g);
  for (const m of euMatches) articles.push(m[0]!.replace(/\s+/g, " "));

  // EU regulations
  const regMatches = text.matchAll(/(?:VO|Verordnung)\s*\((?:EG|EU)\)\s*(?:Nr\.?\s*)?\d+\/\d+/g);
  for (const m of regMatches) articles.push(m[0]!.replace(/\s+/g, " "));

  const unique = [...new Set(articles)];
  return unique.length > 0 ? unique.join(", ") : null;
}

// ---------------------------------------------------------------------------
// Classification — decision type, outcome, sector
// ---------------------------------------------------------------------------

/** BWB decision types mapped to DB enum values. */
function classifyDecisionType(
  title: string,
  bodyText: string,
  meta: Record<string, string>,
): string | null {
  const text = `${title} ${bodyText} ${meta["og_description"] ?? ""}`.toLowerCase();

  if (
    text.includes("kartell") ||
    text.includes("preisabsprach") ||
    text.includes("angebotsabstimmung") ||
    text.includes("marktaufteilung") ||
    text.includes("submissionsabsprach") ||
    text.includes("bieterabsprach") ||
    text.includes("baukartell") ||
    text.includes("§ 1 kartg")
  ) {
    return "cartel";
  }

  if (
    text.includes("missbrauch") ||
    text.includes("marktbeherrsch") ||
    text.includes("marktmacht") ||
    text.includes("margin squeeze") ||
    text.includes("kosten-preis-schere") ||
    text.includes("§ 5 kartg") ||
    text.includes("art. 102")
  ) {
    return "abuse_of_dominance";
  }

  if (
    text.includes("sektoruntersuchung") ||
    text.includes("branchenuntersuchung") ||
    text.includes("marktuntersuchung") ||
    text.includes("§ 2 abs. 1 z 1 wettbg")
  ) {
    return "sector_inquiry";
  }

  if (
    text.includes("verpflichtungszusag") ||
    text.includes("zusagen") ||
    text.includes("commitment")
  ) {
    return "commitment_decision";
  }

  if (
    text.includes("vertikalvereinbar") ||
    text.includes("vertikalvertr") ||
    text.includes("preisbindung") ||
    text.includes("alleinvertrieb") ||
    text.includes("exklusivvertrieb")
  ) {
    return "vertical_restraint";
  }

  return null;
}

/** Classify outcome from decision text. */
function classifyOutcome(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText}`.toLowerCase();

  if (
    text.includes("geldbuße") ||
    text.includes("geldbusse") ||
    text.includes("bußgeld") ||
    text.includes("bussgeld") ||
    (text.includes("eur") && text.includes("verhängt"))
  ) {
    return "fine";
  }

  if (
    text.includes("verpflichtungszusag") ||
    text.includes("mit auflagen") ||
    text.includes("mit bedingungen") ||
    text.includes("unter auflagen")
  ) {
    return "cleared_with_conditions";
  }

  if (text.includes("untersagt") || text.includes("verboten")) {
    return "blocked";
  }

  if (
    text.includes("eingestellt") ||
    text.includes("verfahren beendet") ||
    text.includes("keine bedenken")
  ) {
    return "cleared";
  }

  if (text.includes("fase 2") || text.includes("phase 2") || text.includes("phase ii")) {
    return "cleared_phase2";
  }

  if (
    text.includes("fase 1") ||
    text.includes("phase 1") ||
    text.includes("phase i") ||
    text.includes("genehmigt") ||
    text.includes("freigegeben") ||
    text.includes("nicht untersagt")
  ) {
    return "cleared_phase1";
  }

  if (text.includes("kronzeug") || text.includes("immunität") || text.includes("bonusantrag")) {
    return "leniency";
  }

  return null;
}

/**
 * Map content to sector IDs.
 *
 * Sector IDs match the sectors table in the schema (from seed-sample.ts)
 * and extend to additional sectors the BWB covers.
 */
const SECTOR_MAPPING: Array<{ id: string; patterns: string[] }> = [
  {
    id: "lebensmittelhandel",
    patterns: [
      "lebensmittel",
      "supermarkt",
      "rewe",
      "billa",
      "spar",
      "hofer",
      "lidl",
      "merkur",
      "mpreis",
    ],
  },
  {
    id: "energie",
    patterns: [
      "energie",
      "strom",
      "gas",
      "elektrizität",
      "verbund",
      "kelag",
      "wien energie",
      "evn",
      "e-control",
      "treibstoff",
      "tankstelle",
      "mineralöl",
    ],
  },
  {
    id: "telekommunikation",
    patterns: [
      "telekommunikation",
      "telekom",
      "breitband",
      "mobilfunk",
      "a1",
      "magenta",
      "drei",
      "hutchison",
    ],
  },
  {
    id: "banken",
    patterns: [
      "bank",
      "finanz",
      "kredit",
      "kontoführung",
      "erste",
      "raiffeisen",
      "bawag",
      "volksbank",
      "sparkasse",
      "zahlungsverkehr",
    ],
  },
  {
    id: "medien",
    patterns: [
      "medien",
      "verlag",
      "rundfunk",
      "orf",
      "presse",
      "streaming",
      "prosiebensat",
      "puls 4",
      "zeitung",
    ],
  },
  {
    id: "bau",
    patterns: [
      "bau",
      "bauunterneh",
      "baugesellschaft",
      "bauwirtschaft",
      "hochbau",
      "tiefbau",
      "fassadenbau",
      "trockenbau",
      "asphalt",
      "straßenbau",
    ],
  },
  {
    id: "transport",
    patterns: [
      "transport",
      "spedition",
      "güterbeförderung",
      "logistik",
      "lkw",
      "fracht",
      "eisenbahn",
      "schiene",
    ],
  },
  {
    id: "gesundheit",
    patterns: [
      "gesundheit",
      "pharma",
      "arzneimittel",
      "medizinprodukt",
      "krankenhaus",
      "apotheke",
      "medikament",
    ],
  },
  {
    id: "kfz",
    patterns: [
      "kfz",
      "automobil",
      "fahrzeug",
      "autohandel",
      "werkstatt",
      "ersatzteil",
      "peugeot",
    ],
  },
  {
    id: "versicherung",
    patterns: [
      "versicherung",
      "rückversicherung",
      "lebensversicherung",
      "sachversicherung",
    ],
  },
  {
    id: "abfallwirtschaft",
    patterns: [
      "abfall",
      "entsorgung",
      "recycling",
      "müll",
      "abfallwirtschaft",
    ],
  },
  {
    id: "handel",
    patterns: [
      "handel",
      "einzelhandel",
      "großhandel",
      "e-commerce",
      "online-handel",
    ],
  },
  {
    id: "landwirtschaft",
    patterns: [
      "landwirtschaft",
      "agrar",
      "milch",
      "molkerei",
      "fleisch",
      "getreide",
      "zucker",
    ],
  },
  {
    id: "elektromobilitaet",
    patterns: [
      "ladeinfrastruktur",
      "e-ladeinfrastruktur",
      "elektromobilität",
      "ladesäule",
    ],
  },
  {
    id: "bestattung",
    patterns: ["bestatt", "beerdigung", "friedhof", "krematorium"],
  },
  {
    id: "schweißtechnik",
    patterns: ["schweißtechnik", "fronius", "schweißgeräte"],
  },
  {
    id: "gastronomie",
    patterns: ["gastronomie", "restaurant", "bäckerei", "anker snack"],
  },
  {
    id: "brauerei",
    patterns: ["brauerei", "brau union", "bier", "getränke"],
  },
];

function classifySector(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText.slice(0, 3000)}`.toLowerCase();
  for (const { id, patterns } of SECTOR_MAPPING) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }
  return null;
}

/** Sector display names and English translations. */
const SECTOR_NAMES: Record<
  string,
  { name: string; name_en: string; description: string }
> = {
  lebensmittelhandel: {
    name: "Lebensmittelhandel",
    name_en: "Food Retail",
    description:
      "Lebensmitteleinzelhandel und -großhandel. Rewe (Billa/Merkur), Spar, Hofer/Lidl, MPreis. Schwerpunkt: Lieferantenbeziehungen und Marktkonzentration.",
  },
  energie: {
    name: "Energie",
    name_en: "Energy",
    description:
      "Strom-, Gas- und Treibstoffmärkte. Verbund, Wien Energie, EVN, Kelag. Reguliert durch E-Control.",
  },
  telekommunikation: {
    name: "Telekommunikation",
    name_en: "Telecommunications",
    description:
      "Mobil-, Festnetz- und Breitbanddienste. A1, Magenta, Drei. Reguliert durch RTR.",
  },
  banken: {
    name: "Banken und Finanzdienstleistungen",
    name_en: "Banking and Financial Services",
    description:
      "Bankensektor: Erste Group, Raiffeisen, UniCredit/Bank Austria, BAWAG.",
  },
  medien: {
    name: "Medien und Verlage",
    name_en: "Media and Publishing",
    description:
      "Presse, Rundfunk, Online-Medien. ORF, ProSiebenSat.1 PULS 4, Mediengruppe Österreich.",
  },
  bau: {
    name: "Bauwirtschaft",
    name_en: "Construction",
    description:
      "Hoch- und Tiefbau, Fassadenbau, Trockenbau, Straßenbau. Großes BWB-Baukartell-Verfahren.",
  },
  transport: {
    name: "Transport und Logistik",
    name_en: "Transport and Logistics",
    description:
      "Güterbeförderung, Spedition, Schienengüterverkehr.",
  },
  gesundheit: {
    name: "Gesundheit und Pharma",
    name_en: "Healthcare and Pharmaceuticals",
    description:
      "Arzneimittelversorgung, Medizinprodukte, Krankenhäuser.",
  },
  kfz: {
    name: "Kraftfahrzeuge",
    name_en: "Automotive",
    description:
      "Automobilhandel, Werkstätten, Ersatzteile, Kfz-Zulieferer.",
  },
  versicherung: {
    name: "Versicherungen",
    name_en: "Insurance",
    description:
      "Lebens-, Sach- und Rückversicherung.",
  },
  abfallwirtschaft: {
    name: "Abfallwirtschaft",
    name_en: "Waste Management",
    description:
      "Entsorgung, Recycling, Abfallsammlung.",
  },
  handel: {
    name: "Handel",
    name_en: "Retail and Wholesale",
    description:
      "Einzel- und Großhandel, E-Commerce.",
  },
  landwirtschaft: {
    name: "Landwirtschaft",
    name_en: "Agriculture",
    description:
      "Agrar- und Lebensmittelproduktion, Milchwirtschaft.",
  },
  elektromobilitaet: {
    name: "Elektromobilität",
    name_en: "Electric Mobility",
    description:
      "Ladeinfrastruktur für Elektrofahrzeuge.",
  },
  bestattung: {
    name: "Bestattungswesen",
    name_en: "Funeral Services",
    description:
      "Bestattungsunternehmen und Friedhofsverwaltung.",
  },
  schweißtechnik: {
    name: "Schweißtechnik",
    name_en: "Welding Technology",
    description:
      "Schweißgeräte und -zubehör.",
  },
  gastronomie: {
    name: "Gastronomie",
    name_en: "Food Service",
    description:
      "Gastronomie, Bäckereien, Systemgastronomie.",
  },
  brauerei: {
    name: "Brauereien und Getränke",
    name_en: "Brewing and Beverages",
    description:
      "Brauereien, Getränkegroßhandel, Getränkeherstellung.",
  },
};

// ---------------------------------------------------------------------------
// Page parsing — build decision and merger records
// ---------------------------------------------------------------------------

/** Extract acquiring party and target from German merger text. */
function extractMergerParties(
  title: string,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  // Pattern: "X / Y" in title (common BWB naming)
  const slashMatch = title.match(/^(.+?)\s*\/\s*(.+?)$/);
  if (slashMatch) {
    return {
      acquiring: slashMatch[1]!.trim().slice(0, 300),
      target: slashMatch[2]!.trim().slice(0, 300),
    };
  }

  // Pattern: "X übernimmt Y" or "X erwirbt Y"
  const acquireMatch = title.match(
    /^(.+?)\s+(?:übernimmt|erwirbt|kauft|beteiligt sich an)\s+(.+?)$/i,
  );
  if (acquireMatch) {
    return {
      acquiring: acquireMatch[1]!.trim().slice(0, 300),
      target: acquireMatch[2]!.trim().slice(0, 300),
    };
  }

  // Pattern in body: "X erwirbt" / "X übernimmt" / "Erwerb durch X"
  const bodyAcquire = bodyText.match(
    /(?:erwerb|übernahme|beteiligung)\s+(?:durch|von|der)\s+(.{3,100}?)\s+(?:an|der|des|am)\s+(.{3,100}?)[\.,;]/i,
  );
  if (bodyAcquire) {
    return {
      acquiring: bodyAcquire[1]!.trim().slice(0, 300),
      target: bodyAcquire[2]!.trim().slice(0, 300),
    };
  }

  return { acquiring: null, target: null };
}

/**
 * Parse a single BWB decision detail page into a decision or merger record.
 */
function parseDecisionPage(
  html: string,
  url: string,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // Extract title
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    $("title").text().trim().replace(/\s*:\s*BWB.*$/, "") ||
    "";

  if (!title || title.length < 5) {
    return { decision: null, merger: null };
  }

  // Extract metadata
  const meta = extractMetadata($);

  // Extract body text from various TYPO3 content selectors
  const bodySelectors = [
    ".news-text-wrap",
    ".ce-bodytext",
    ".frame-default .ce-bodytext",
    "article .text",
    ".news-single-item .article-content",
    ".tx-news .article",
    "main .content-main",
    "article",
  ];

  let bodyText = "";
  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0) {
      // Remove navigation, header, footer elements within
      el.find("nav, .nav, footer, .breadcrumb, .social-sharing").remove();
      bodyText = el.text().trim();
      if (bodyText.length > 100) break;
    }
  }

  // Fallback: grab paragraph text from main content
  if (!bodyText || bodyText.length < 100) {
    const paragraphs: string[] = [];
    $("main p, article p, .content p").each((_i, el) => {
      const text = $(el).text().trim();
      if (text.length > 20) paragraphs.push(text);
    });
    bodyText = paragraphs.join("\n\n");
  }

  // Final fallback
  if (!bodyText || bodyText.length < 50) {
    $("nav, footer, header, .menu, .breadcrumb, script, style, .social-sharing").remove();
    bodyText = $("main, article, .content").text().trim();
  }

  if (!bodyText || bodyText.length < 30) {
    return { decision: null, merger: null };
  }

  // Clean up whitespace
  bodyText = bodyText.replace(/\s+/g, " ").replace(/\n{3,}/g, "\n\n").trim();

  // Extract structured fields
  const caseNumber = extractCaseNumber(title, bodyText, url);
  const rawDate =
    meta["datum"] ??
    meta["date"] ??
    meta["veröffentlicht"] ??
    meta["entscheidungsdatum"] ??
    "";
  const date = parseGermanDate(rawDate);

  const type = classifyDecisionType(title, bodyText, meta);
  const outcome = classifyOutcome(title, bodyText);
  const sector = classifySector(title, bodyText);
  const fineAmount = extractFineAmount(bodyText);
  const articles = extractLegalArticles(bodyText);

  // Build summary: first 500 chars of body text
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();

  // Determine if this is a merger filing
  const isMerger =
    url.includes("zusammenschl") ||
    url.includes("merger") ||
    url.includes("fusionskontrolle") ||
    type === null &&
      (title.toLowerCase().includes("zusammenschluss") ||
        title.toLowerCase().includes("übernahme") ||
        title.toLowerCase().includes("erwerb") ||
        title.includes(" / ") && bodyText.toLowerCase().includes("angemeldet"));

  if (isMerger) {
    const { acquiring, target } = extractMergerParties(title, bodyText);

    // Try to extract turnover from text
    let turnover: number | null = null;
    const turnoverMatch = bodyText.match(
      /(?:Umsatz|Gesamtumsatz|Umsatzerlöse)\s+(?:von\s+)?(?:EUR|€)\s*([\d.,]+)\s*(?:Mio|Million|Mrd|Milliard)?/i,
    );
    if (turnoverMatch) {
      let numStr = turnoverMatch[1]!.replace(/\./g, "").replace(",", ".");
      let multiplier = 1;
      if (/Mio|Million/i.test(turnoverMatch[0]!)) multiplier = 1_000_000;
      if (/Mrd|Milliard/i.test(turnoverMatch[0]!)) multiplier = 1_000_000_000;
      const val = parseFloat(numStr);
      if (!isNaN(val)) turnover = val * multiplier;
    }

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date,
        sector,
        acquiring_party: acquiring,
        target,
        summary,
        full_text: bodyText,
        outcome: outcome ?? "pending",
        turnover,
      },
    };
  }

  // Treat as a decision
  return {
    decision: {
      case_number: caseNumber,
      title,
      date,
      type: type ?? "decision",
      sector,
      parties: meta["parteien"] ?? meta["beteiligte"] ?? null,
      summary,
      full_text: bodyText,
      outcome: outcome ?? (fineAmount ? "fine" : "pending"),
      fine_amount: fineAmount,
      gwb_articles: articles,
      status: outcome === "pending" || !outcome ? "pending" : "final",
    },
    merger: null,
  };
}

/**
 * Parse a merger listing page (year-based table) to extract merger records
 * directly from the table when individual detail pages are not available.
 *
 * BWB merger listings often contain a table with columns:
 *   Anmeldedatum | Zusammenschlusswerber | Zielunternehmen | Branche | Ergebnis
 */
function parseMergerListingPage(
  html: string,
  year: number,
): ParsedMerger[] {
  const $ = cheerio.load(html);
  const mergers: ParsedMerger[] = [];

  // Look for table rows with merger data
  $("table tbody tr, table tr").each((rowIdx, el) => {
    const cells = $(el).find("td");
    if (cells.length < 3) return; // Need at least date, party, target

    const cellTexts = cells
      .map((_i, c) => $(c).text().trim())
      .get();

    // Skip header rows
    if (
      cellTexts.some(
        (t) =>
          t.toLowerCase().includes("anmeldedatum") ||
          t.toLowerCase().includes("zusammenschlusswerber"),
      )
    ) {
      return;
    }

    // Try to parse based on common column orders
    const dateStr = cellTexts[0] ?? "";
    const date = parseGermanDate(dateStr);
    const acquiring = cellTexts[1]?.trim() ?? null;
    const target = cellTexts[2]?.trim() ?? null;
    const branche = cellTexts.length > 3 ? cellTexts[3]?.trim() : null;

    if (!acquiring || acquiring.length < 3) return;

    const title = target
      ? `${acquiring} / ${target}`
      : acquiring;

    const caseNumber = `BWB-ZSS/${year}-${String(rowIdx + 1).padStart(3, "0")}`;
    const sector = classifySector(title, branche ?? "");
    const fullText = cellTexts.join(" | ");

    mergers.push({
      case_number: caseNumber,
      title,
      date,
      sector,
      acquiring_party: acquiring,
      target,
      summary: fullText.slice(0, 500),
      full_text: fullText,
      outcome: "pending",
      turnover: null,
    });
  });

  return mergers;
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return { upsertDecision, upsertMerger, upsertSector };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== BWB Competition Decisions Crawler ===");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${dryRun}`);
  console.log(`  Resume:     ${resume}`);
  console.log(`  Force:      ${force}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // -----------------------------------------------------------------------
  // Step 1: Discover all URLs
  // -----------------------------------------------------------------------

  const decisionUrls = await discoverDecisionUrls();
  const mergerUrls = await discoverMergerUrls();
  const newsUrls = await discoverNewsUrls();

  // Combine decision and news URLs (both produce decision records)
  const allDecisionUrls = [...new Set([...decisionUrls, ...newsUrls])];

  // Filter already-processed URLs (for --resume)
  const decisionUrlsToProcess = resume
    ? allDecisionUrls.filter((u) => !processedSet.has(u))
    : allDecisionUrls;
  const mergerUrlsToProcess = resume
    ? mergerUrls.filter((u) => !processedSet.has(u))
    : mergerUrls;

  const totalUrls = decisionUrlsToProcess.length + mergerUrlsToProcess.length;

  console.log(`\n--- Processing summary ---`);
  console.log(`  Decision/news URLs: ${decisionUrlsToProcess.length}`);
  console.log(`  Merger URLs:        ${mergerUrlsToProcess.length}`);
  console.log(`  Total:              ${totalUrls}`);

  if (resume) {
    const skipped =
      allDecisionUrls.length +
      mergerUrls.length -
      totalUrls;
    if (skipped > 0) {
      console.log(`  Skipping ${skipped} already-processed URLs`);
    }
  }

  if (totalUrls === 0) {
    console.log("\nNothing to process. Exiting.");
    return;
  }

  // -----------------------------------------------------------------------
  // Step 2: Initialize database (unless dry run)
  // -----------------------------------------------------------------------

  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
    console.log(`\nDatabase ready at ${DB_PATH}`);
  }

  const sectorAcc: SectorAccumulator = {};
  let decisionsIngested = state.decisionsIngested;
  let mergersIngested = state.mergersIngested;
  let errors = 0;
  let processed = 0;

  // -----------------------------------------------------------------------
  // Step 3: Process decision detail pages
  // -----------------------------------------------------------------------

  console.log(`\n--- Processing ${decisionUrlsToProcess.length} decision/news pages ---`);

  for (const url of decisionUrlsToProcess) {
    processed++;
    const progress = `[${processed}/${totalUrls}]`;

    try {
      console.log(`${progress} Fetching: ${url}`);
      const html = await rateLimitedFetch(url);
      if (!html) {
        console.warn(`${progress} SKIP (fetch failed): ${url}`);
        state.errors.push(`fetch-failed: ${url}`);
        errors++;
        continue;
      }

      const { decision, merger } = parseDecisionPage(html, url);

      if (decision) {
        if (dryRun) {
          console.log(
            `${progress} [DRY RUN] Would insert decision: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
          );
        } else {
          stmts!.upsertDecision.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.gwb_articles,
            decision.status,
          );
          decisionsIngested++;
          console.log(
            `${progress} Decision: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
          );
        }

        // Accumulate sector counts
        if (decision.sector) {
          const s = sectorAcc[decision.sector] ??= {
            name: SECTOR_NAMES[decision.sector]?.name ?? decision.sector,
            name_en: SECTOR_NAMES[decision.sector]?.name_en ?? null,
            description: SECTOR_NAMES[decision.sector]?.description ?? null,
            decisionCount: 0,
            mergerCount: 0,
          };
          s.decisionCount++;
        }
      } else if (merger) {
        if (dryRun) {
          console.log(
            `${progress} [DRY RUN] Would insert merger: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
          );
        } else {
          stmts!.upsertMerger.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          mergersIngested++;
          console.log(
            `${progress} Merger: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
          );
        }

        if (merger.sector) {
          const s = sectorAcc[merger.sector] ??= {
            name: SECTOR_NAMES[merger.sector]?.name ?? merger.sector,
            name_en: SECTOR_NAMES[merger.sector]?.name_en ?? null,
            description: SECTOR_NAMES[merger.sector]?.description ?? null,
            decisionCount: 0,
            mergerCount: 0,
          };
          s.mergerCount++;
        }
      } else {
        console.log(`${progress} SKIP (no structured data extracted): ${url}`);
      }

      // Track processed URL
      state.processedUrls.push(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`${progress} ERROR: ${url}: ${message}`);
      state.errors.push(`error: ${url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 URLs)
    if (!dryRun && processed % 25 === 0) {
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;
      saveState(state);
    }
  }

  // -----------------------------------------------------------------------
  // Step 4: Process merger detail pages
  // -----------------------------------------------------------------------

  console.log(`\n--- Processing ${mergerUrlsToProcess.length} merger pages ---`);

  for (const url of mergerUrlsToProcess) {
    processed++;
    const progress = `[${processed}/${totalUrls}]`;

    try {
      console.log(`${progress} Fetching: ${url}`);
      const html = await rateLimitedFetch(url);
      if (!html) {
        console.warn(`${progress} SKIP (fetch failed): ${url}`);
        state.errors.push(`fetch-failed: ${url}`);
        errors++;
        continue;
      }

      // Check if this is a detail page or a listing page
      const isDetailPage =
        /\/\d{4}\/\d+$/.test(url) || url.includes("/merger/");

      if (isDetailPage) {
        const { merger } = parseDecisionPage(html, url);
        if (merger) {
          if (dryRun) {
            console.log(
              `${progress} [DRY RUN] Would insert merger: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
            );
          } else {
            stmts!.upsertMerger.run(
              merger.case_number,
              merger.title,
              merger.date,
              merger.sector,
              merger.acquiring_party,
              merger.target,
              merger.summary,
              merger.full_text,
              merger.outcome,
              merger.turnover,
            );
            mergersIngested++;
            console.log(
              `${progress} Merger: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
            );
          }

          if (merger.sector) {
            const s = sectorAcc[merger.sector] ??= {
              name: SECTOR_NAMES[merger.sector]?.name ?? merger.sector,
              name_en: SECTOR_NAMES[merger.sector]?.name_en ?? null,
              description: SECTOR_NAMES[merger.sector]?.description ?? null,
              decisionCount: 0,
              mergerCount: 0,
            };
            s.mergerCount++;
          }
        } else {
          console.log(`${progress} SKIP (no merger data extracted): ${url}`);
        }
      } else {
        // This is a year listing page — parse the table
        const yearMatch = url.match(/\/(\d{4})/);
        const year = yearMatch ? parseInt(yearMatch[1]!, 10) : MERGER_END_YEAR;
        const tableMergers = parseMergerListingPage(html, year);

        for (const merger of tableMergers) {
          if (dryRun) {
            console.log(
              `${progress} [DRY RUN] Would insert merger from table: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
            );
          } else {
            stmts!.upsertMerger.run(
              merger.case_number,
              merger.title,
              merger.date,
              merger.sector,
              merger.acquiring_party,
              merger.target,
              merger.summary,
              merger.full_text,
              merger.outcome,
              merger.turnover,
            );
            mergersIngested++;
          }

          if (merger.sector) {
            const s = sectorAcc[merger.sector] ??= {
              name: SECTOR_NAMES[merger.sector]?.name ?? merger.sector,
              name_en: SECTOR_NAMES[merger.sector]?.name_en ?? null,
              description: SECTOR_NAMES[merger.sector]?.description ?? null,
              decisionCount: 0,
              mergerCount: 0,
            };
            s.mergerCount++;
          }
        }

        if (tableMergers.length > 0) {
          console.log(
            `${progress} Extracted ${tableMergers.length} mergers from table (${year})`,
          );
        }
      }

      state.processedUrls.push(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`${progress} ERROR: ${url}: ${message}`);
      state.errors.push(`error: ${url}: ${message}`);
      errors++;
    }

    // Save state periodically
    if (!dryRun && processed % 25 === 0) {
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;
      saveState(state);
    }
  }

  // -----------------------------------------------------------------------
  // Step 5: Upsert sector records
  // -----------------------------------------------------------------------

  if (!dryRun && db && stmts) {
    console.log("\n--- Upserting sector records ---");

    // Merge with any existing sector counts in the DB
    const existingSectors = db
      .prepare("SELECT id, decision_count, merger_count FROM sectors")
      .all() as Array<{ id: string; decision_count: number; merger_count: number }>;

    for (const es of existingSectors) {
      if (sectorAcc[es.id]) {
        sectorAcc[es.id]!.decisionCount += es.decision_count;
        sectorAcc[es.id]!.mergerCount += es.merger_count;
      }
    }

    for (const [id, data] of Object.entries(sectorAcc)) {
      stmts.upsertSector.run(
        id,
        data.name,
        data.name_en,
        data.description,
        data.decisionCount,
        data.mergerCount,
      );
      console.log(
        `  Sector: ${id} (${data.decisionCount} decisions, ${data.mergerCount} mergers)`,
      );
    }
  }

  // -----------------------------------------------------------------------
  // Step 6: Final state save and summary
  // -----------------------------------------------------------------------

  state.decisionsIngested = decisionsIngested;
  state.mergersIngested = mergersIngested;

  if (!dryRun) {
    saveState(state);
  }

  if (db) {
    const dc = (
      db.prepare("SELECT COUNT(*) as n FROM decisions").get() as { n: number }
    ).n;
    const mc = (
      db.prepare("SELECT COUNT(*) as n FROM mergers").get() as { n: number }
    ).n;
    const sc = (
      db.prepare("SELECT COUNT(*) as n FROM sectors").get() as { n: number }
    ).n;

    console.log("\n=== Ingestion complete ===");
    console.log(`  Decisions in DB: ${dc}`);
    console.log(`  Mergers in DB:   ${mc}`);
    console.log(`  Sectors in DB:   ${sc}`);
    console.log(`  New decisions:   ${decisionsIngested}`);
    console.log(`  New mergers:     ${mergersIngested}`);
    console.log(`  Errors:          ${errors}`);
    console.log(`  State saved to:  ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry run complete ===");
    console.log(`  Decision URLs found: ${decisionUrlsToProcess.length}`);
    console.log(`  Merger URLs found:   ${mergerUrlsToProcess.length}`);
    console.log(`  Errors:              ${errors}`);
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
