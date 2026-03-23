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
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/** Kartellgericht (Cartel Court) decisions — paginated TYPO3 news listing. */
const DECISIONS_KG_URL =
  `${BASE_URL}/kartelle-marktmachtmissbrauch/entscheidungen/entscheidungen-des-kartellgerichts`;
// Note: Kartellobergericht (Supreme Cartel Court) decisions are at
// /kartelle-marktmachtmissbrauch/entscheidungen/entscheidungen-des-kartellobergerichts
// but use a flat table (no detail pages). Could be added as a table extraction step.

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
      const headers: Record<string, string> = {
        "User-Agent": USER_AGENT,
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-AT,de;q=0.9,en;q=0.5",
      };
      if (sucuriCookie) {
        headers["Cookie"] = sucuriCookie;
      }
      const response = await fetch(url, {
        headers,
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
// Sucuri WAF challenge solver
// ---------------------------------------------------------------------------

/**
 * The BWB site is behind Sucuri/Cloudproxy WAF. Initial requests receive a
 * 307 with a JS challenge that computes a cookie value. We must:
 *   1. Fetch the challenge page (307 with inline JS)
 *   2. Decode the Base64 payload to extract the cookie name and value
 *   3. Store the cookie for all subsequent requests
 *
 * The challenge JS sets document.cookie = '<name>=<value>' then reloads.
 * We evaluate the value-building JS in a sandboxed context (no DOM needed).
 */

let sucuriCookie: string | null = null;

async function solveSucuriChallenge(): Promise<void> {
  console.log("  Solving Sucuri WAF challenge...");
  const response = await fetch(BASE_URL, {
    headers: {
      "User-Agent": USER_AGENT,
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "de-AT,de;q=0.9,en;q=0.5",
    },
    redirect: "manual",
  });

  const html = await response.text();

  // Extract the Base64 payload from the Sucuri challenge script
  const b64Match = html.match(/S='([A-Za-z0-9+/=]+)'/);
  if (!b64Match) {
    console.warn("  [WARN] No Sucuri challenge found (site may not require it).");
    return;
  }

  // Decode the Base64 payload
  const decoded = Buffer.from(b64Match[1]!, "base64").toString("utf-8");

  // The decoded JS has two parts:
  //   1. Variable assignment building the cookie value, e.g.  m='e' + '2' + ...
  //   2. document.cookie = '<name>=' + <var> + ';path=/;max-age=86400'
  // We need to extract the cookie name and compute the value.

  // Extract cookie name from the document.cookie assignment
  const cookieNameChars = decoded.match(
    /document\.cookie='([^']+)'|document\.cookie=([^;]+;)/,
  );
  // The name is built char-by-char with + concatenation. Easier to regex the
  // reconstructed string after the assignment.
  const nameMatch = decoded.match(
    /document\.cookie=((?:'[^']*'\+)*'[^']*')\s*\+\s*"="\s*\+/,
  );
  let cookieName: string;
  if (nameMatch) {
    // Evaluate the concatenation of single-quoted strings
    cookieName = nameMatch[1]!.split("+").map((s) => s.trim().replace(/^'|'$/g, "")).join("");
  } else {
    // Fallback: try to find sucuri_cloudproxy_uuid pattern directly
    const directName = decoded.match(/sucuri_cloudproxy_uuid_[a-f0-9]+/);
    if (directName) {
      cookieName = directName[0]!;
    } else {
      console.warn("  [WARN] Could not extract Sucuri cookie name.");
      return;
    }
  }

  // Extract the variable assignment (first statement) and evaluate it to get
  // the cookie value. The JS looks like:  f='e' + "2" + String.fromCharCode(98) + ...
  const varName = decoded.charAt(0);
  const assignEnd = decoded.indexOf(";document.cookie");
  if (assignEnd < 0) {
    console.warn("  [WARN] Could not parse Sucuri challenge script.");
    return;
  }
  const assignment = decoded.slice(0, assignEnd + 1); // include trailing ;

  // Evaluate in a minimal sandbox (only String.fromCharCode is needed)
  let cookieValue: string;
  try {
    const fn = new Function(`${assignment} return ${varName};`);
    cookieValue = fn() as string;
  } catch (err) {
    console.warn(
      `  [WARN] Failed to evaluate Sucuri challenge: ${err instanceof Error ? err.message : String(err)}`,
    );
    return;
  }

  sucuriCookie = `${cookieName}=${cookieValue}`;
  console.log(`  Sucuri challenge solved. Cookie: ${cookieName}=<${cookieValue.length} chars>`);
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
 * BWB has two decision sub-pages:
 *   - Kartellgericht: /kartelle-marktmachtmissbrauch/entscheidungen/entscheidungen-des-kartellgerichts
 *   - Kartellobergericht: /kartelle-marktmachtmissbrauch/entscheidungen/entscheidungen-des-kartellobergerichts
 *
 * Kartellgericht uses TYPO3 news pagination:
 *   tx_news_pi1[controller]=News&tx_news_pi1[currentPage]=N&cHash=XXX
 * We discover pagination links from the page itself (cHash is required).
 *
 * Detail links point to:
 *   /kartelle_marktmachtmissbrauch/entscheidungen/detail/<slug>
 *
 * Kartellobergericht has a table listing (no detail links) — we extract
 * data directly from the table in a later step (see parseKogTable).
 */
async function discoverDecisionUrls(): Promise<string[]> {
  const urls: string[] = [];
  console.log("\n--- Discovering decision URLs (Kartellgericht) ---");

  // -- Page 1 --
  console.log(`  Fetching Kartellgericht decisions page 1...`);
  const firstHtml = await rateLimitedFetch(DECISIONS_KG_URL);
  if (!firstHtml) {
    console.warn("  [WARN] Could not fetch Kartellgericht decisions page 1");
    return urls;
  }

  let $ = cheerio.load(firstHtml);

  // Extract detail links from page 1
  const extractDetailLinks = ($page: cheerio.CheerioAPI): string[] => {
    const links: string[] = [];
    $page('a[href*="/entscheidungen/detail/"]').each((_i, el) => {
      const href = $page(el).attr("href");
      if (href) {
        const absoluteUrl = href.startsWith("http")
          ? href
          : `${BASE_URL}${href.startsWith("/") ? "" : "/"}${href}`;
        links.push(absoluteUrl.split("?")[0]!.split("#")[0]!);
      }
    });
    return [...new Set(links)];
  };

  const page1Links = extractDetailLinks($);
  urls.push(...page1Links);
  console.log(`    Found ${page1Links.length} decision links on page 1`);

  // Discover pagination links (TYPO3 uses cHash that we must preserve)
  const paginationUrls: string[] = [];
  $("a[href*='currentPage']").each((_i, el) => {
    const href = $(el).attr("href");
    if (href) {
      // Decode HTML entities in href (&amp; → &)
      const decoded = href.replace(/&amp;/g, "&");
      const absoluteUrl = decoded.startsWith("http")
        ? decoded
        : `${BASE_URL}${decoded.startsWith("/") ? "" : "/"}${decoded}`;
      paginationUrls.push(absoluteUrl);
    }
  });
  const uniquePages = [...new Set(paginationUrls)];
  console.log(`  Found ${uniquePages.length} pagination links`);

  // -- Pages 2..N --
  for (const pageUrl of uniquePages) {
    const pageNum = pageUrl.match(/currentPage(?:%5D)?=(\d+)/)?.[1] ?? "?";
    console.log(`  Fetching Kartellgericht decisions page ${pageNum}...`);
    const html = await rateLimitedFetch(pageUrl);
    if (!html) {
      console.warn(`  [WARN] Could not fetch page ${pageNum}`);
      continue;
    }

    const $page = cheerio.load(html);
    const pageLinks = extractDetailLinks($page);
    if (pageLinks.length === 0) {
      console.log(`    No decision links on page ${pageNum}. Stopping.`);
      break;
    }
    urls.push(...pageLinks);
    console.log(`    Found ${pageLinks.length} decision links (total: ${urls.length})`);
  }

  const deduped = [...new Set(urls)];
  console.log(`  Discovered ${deduped.length} unique decision URLs`);
  return deduped;
}

// ---------------------------------------------------------------------------
// URL discovery — merger filings (year-based listings)
// ---------------------------------------------------------------------------

/**
 * Extract merger filings directly from year-based table pages.
 *
 * BWB merger year pages (/zusammenschluesse/<year>) contain an HTML table
 * with columns: Aktenzahl (case number), Unternehmen (parties), Datum des
 * Zusammenschlusses (date), Status. There are no individual detail pages —
 * all data is in the table rows.
 *
 * Merger start year on the site is 2006 (not 2002).
 */
async function discoverMergerData(): Promise<ParsedMerger[]> {
  const mergers: ParsedMerger[] = [];
  console.log("\n--- Extracting merger data from year tables ---");

  for (let year = MERGER_END_YEAR; year >= MERGER_START_YEAR; year--) {
    const yearUrl = `${BASE_URL}/zusammenschluesse/${year}`;
    console.log(`  Fetching mergers for ${year}...`);
    const html = await rateLimitedFetch(yearUrl);
    if (!html) {
      console.log(`    No merger page for ${year}`);
      continue;
    }

    const $ = cheerio.load(html);
    let rowCount = 0;

    $("table tbody tr").each((_i, el) => {
      const cells = $(el).find("td");
      if (cells.length < 3) return;

      const caseNumber = $(cells[0]).text().trim();
      const parties = $(cells[1]).text().trim();
      const dateRaw = $(cells[2]).text().trim();
      const status = cells.length >= 4 ? $(cells[3]).text().trim() : null;

      if (!caseNumber || !parties) return;

      const date = parseGermanDate(dateRaw);

      mergers.push({
        case_number: caseNumber,
        title: parties,
        date,
        sector: classifySector(parties, parties) ?? null,
        acquiring_party: extractAcquiringParty(parties),
        target: extractTarget(parties),
        summary: status ? `${parties} — ${status}` : parties,
        full_text: `${caseNumber} ${parties} ${dateRaw} ${status ?? ""}`.trim(),
        outcome: classifyMergerOutcome(status ?? ""),
        turnover: null,
      });
      rowCount++;
    });

    if (rowCount > 0) {
      console.log(`    Extracted ${rowCount} merger rows for ${year} (total: ${mergers.length})`);
    } else {
      console.log(`    No merger table rows for ${year}`);
    }
  }

  console.log(`  Extracted ${mergers.length} total merger filings`);
  return mergers;
}

/** Extract acquiring party from BWB merger parties string. */
function extractAcquiringParty(parties: string): string | null {
  // Parties are often separated by ; — first entity is typically acquirer
  const parts = parties.split(";").map((s) => s.trim()).filter(Boolean);
  return parts.length > 0 ? parts[0]! : null;
}

/** Extract target from BWB merger parties string. */
function extractTarget(parties: string): string | null {
  const parts = parties.split(";").map((s) => s.trim()).filter(Boolean);
  return parts.length > 1 ? parts.slice(1).join("; ") : null;
}

/** Classify merger outcome from status text. */
function classifyMergerOutcome(status: string): string | null {
  const text = status.toLowerCase();
  if (text.includes("fristablauf") || text.includes("freigabe")) return "cleared_phase1";
  if (text.includes("phase 2") || text.includes("phase ii")) return "cleared_phase2";
  if (text.includes("auflagen") || text.includes("bedingung")) return "cleared_with_conditions";
  if (text.includes("untersagt") || text.includes("verboten")) return "blocked";
  if (text.includes("zurückgezogen") || text.includes("zurückziehung")) return "withdrawn";
  if (text.includes("angemeldet")) return "pending";
  return null;
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

  // Solve Sucuri WAF challenge before any requests
  await solveSucuriChallenge();

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // -----------------------------------------------------------------------
  // Step 1: Discover decision URLs and extract merger data from tables
  // -----------------------------------------------------------------------

  const decisionUrls = await discoverDecisionUrls();
  const mergerData = await discoverMergerData();
  const newsUrls = await discoverNewsUrls();

  // Combine decision and news URLs (both produce decision records)
  const allDecisionUrls = [...new Set([...decisionUrls, ...newsUrls])];

  // Filter already-processed URLs (for --resume)
  const decisionUrlsToProcess = resume
    ? allDecisionUrls.filter((u) => !processedSet.has(u))
    : allDecisionUrls;

  const totalItems = decisionUrlsToProcess.length + mergerData.length;

  console.log(`\n--- Processing summary ---`);
  console.log(`  Decision/news URLs: ${decisionUrlsToProcess.length}`);
  console.log(`  Merger table rows:  ${mergerData.length}`);
  console.log(`  Total:              ${totalItems}`);

  if (resume) {
    const skipped = allDecisionUrls.length - decisionUrlsToProcess.length;
    if (skipped > 0) {
      console.log(`  Skipping ${skipped} already-processed URLs`);
    }
  }

  if (totalItems === 0) {
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
    const progress = `[${processed}/${decisionUrlsToProcess.length}]`;

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
  // Step 4: Insert merger data (already extracted from year-page tables)
  // -----------------------------------------------------------------------

  console.log(`\n--- Inserting ${mergerData.length} merger records ---`);

  for (const merger of mergerData) {
    if (dryRun) {
      console.log(
        `  [DRY RUN] Would insert merger: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
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
    console.log(`  Merger rows found:   ${mergerData.length}`);
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
