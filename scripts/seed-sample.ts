/**
 * Seed the BWB database with sample decisions, mergers, and sectors.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["BWB_DB_PATH"] ?? "data/bwb.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow { id: string; name: string; name_en: string; description: string; decision_count: number; merger_count: number; }

const sectors: SectorRow[] = [
  { id: "lebensmittelhandel", name: "Lebensmittelhandel", name_en: "Food Retail", description: "Österreichischer Lebensmitteleinzelhandel, dominiert von REWE (Billa, Merkur), Spar und Hofer/Lidl. Hochkonzentrierter Markt mit regelmäßiger BWB-Aufsicht. Schwerpunkt: Lieferantenbeziehungen und vertikale Beschränkungen.", decision_count: 28, merger_count: 12 },
  { id: "energie", name: "Energie", name_en: "Energy", description: "Österreichische Energie- und Gasmärkte: Verbund, Wien Energie, EVN und regionale Versorger. Reguliert durch E-Control. BWB fokussiert auf Wettbewerb im liberalisierten Segment und Versorgungssicherheit.", decision_count: 19, merger_count: 15 },
  { id: "telekommunikation", name: "Telekommunikation", name_en: "Telecommunications", description: "Österreichischer Telekommunikationsmarkt: A1, Magenta, Drei (Hutchison). Mobil-, Festnetz- und Breitbanddienste. Reguliert durch RTR. BWB-Zuständigkeit bei wettbewerbsrechtlichen Fragen außerhalb der Sektorregulierung.", decision_count: 15, merger_count: 8 },
  { id: "banken", name: "Banken und Finanzdienstleistungen", name_en: "Banking and Financial Services", description: "Österreichischer Bankensektor: Erste Group, Raiffeisen, UniCredit/Bank Austria, BAWAG. Hohe Konzentration. Schwerpunkt BWB: Konditionenabsprachen, Interchange-Gebühren und Fintech-Märkte.", decision_count: 12, merger_count: 9 },
  { id: "medien", name: "Medien und Verlage", name_en: "Media and Publishing", description: "Österreichischer Medienmarkt: Presse, Rundfunk (ORF), Online-Medien. Reguliert durch Medienbehörde KommAustria. BWB-Fokus auf Printmärkte, Werbemärkte und Konvergenz Digital/Broadcast.", decision_count: 10, merger_count: 7 },
];

const insertSector = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow { case_number: string; title: string; date: string; type: string; sector: string; parties: string; summary: string; full_text: string; outcome: string; fine_amount: number | null; gwb_articles: string; status: string; }

const decisions: DecisionRow[] = [
  {
    case_number: "BWB/Z-5765",
    title: "Rewe International / Konditionenabsprachen mit Lieferanten",
    date: "2023-06-15",
    type: "abuse_of_dominance",
    sector: "lebensmittelhandel",
    parties: "Rewe International AG (Billa, Merkur, Penny)",
    summary: "Die BWB stellte das Verfahren gegen Rewe International wegen Konditionenabsprachen mit Lebensmittellieferanten ein, nachdem Rewe Verpflichtungszusagen anbot: transparentere Vertragsgestaltung, keine nachträglichen Konditionenänderungen, Ombudsmann für Lieferantenstreitigkeiten. Verfahren gemäß § 26 KartGG eingestellt.",
    full_text: "BWB/Z-5765 Rewe International Lieferantenkonditionen. Ausgangslage: BWB-Untersuchung nach Beschwerden mehrerer Lebensmittellieferanten über einseitige Konditionenänderungen durch Rewe (Billa/Merkur/Penny). Vorgeworfene Verhaltensweisen: (1) Einforderung nicht vereinbarter Rabatte ('Jahresboni') retroaktiv; (2) Listungsgebühren für Regalpräsenz ohne klare Gegenleistung; (3) Stornierung von Bestellungen ohne vertragsgemäße Frist; (4) Druck zur Preissenkung unter Androhung der Delistung. Rechtliche Grundlage: § 5 KartG (Missbrauch marktbeherrschender Stellung); § 4 UWG (unlautere Handelspraktiken im B2B-Bereich). Verpflichtungszusagen: schriftliche Rahmenvereinbarungen mit Lieferanten; keine einseitige nachträgliche Konditionenänderung; Einrichtung eines Ombudsmanns; BWB-Monitoringrecht 3 Jahre. Verfahrenseinstellung nach § 26 KartGG.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gwb_articles: "§ 5 KartG, § 4 UWG",
    status: "final",
  },
  {
    case_number: "KOG/24G1/23",
    title: "Kartellgericht — Preisabsprachen im Transportgewerbe (Kartellrecht)",
    date: "2023-09-20",
    type: "cartel",
    sector: "energie",
    parties: "Österreichische Transportunternehmen (anonymisiert); Wirtschaftskammer-Fachgruppe Güterbeförderung",
    summary: "Das Kartellgericht Wien verhängte Geldbußen von insgesamt EUR 8,2 Millionen gegen mehrere Transportunternehmen und deren Branchenverband wegen Preisabsprachen für Güterkraftverkehrsleistungen. Die WKO-Fachgruppe fungierte als Koordinierungsplattform. Verstoß gegen § 1 KartG und Art. 101 TFUE.",
    full_text: "KOG/24G1/23 Preisabsprachen Güterbeförderung Österreich. Tatbestand: mehrere Spediteure und Frachtführer koordinierten über Sitzungen der WKO-Fachgruppe Güterbeförderung Mindestpreise für LKW-Transporte in Österreich. Dauer: 2016-2022 (6 Jahre). Koordinierung: monatliche Branchentreffen; Austausch von Preisinformationen; Absprache von Treibstoffzuschlägen; gemeinsame Preislisten als 'Orientierungshilfe'. § 1 KartG-Verstoß: horizontale Preisfixierung als Kernbeschränkung. Kroner-Bonusantrag: ein Unternehmen erhielt 30% Geldbußenreduktion gegen vollständige Kooperation. Geldbußen: EUR 8,2 Mio. gesamt; Geldbußen für WKO-Fachgruppe EUR 250.000. Abschreckungswirkung: Kartellgericht betonte präventive Funktion angesichts wiederholter Verstöße im Sektor.",
    outcome: "fine",
    fine_amount: 8200000,
    gwb_articles: "§ 1 KartG, Art. 101 TFUE",
    status: "final",
  },
  {
    case_number: "BWB/Z-5623",
    title: "Sektoruntersuchung österreichischer Lebensmitteleinzelhandel 2023",
    date: "2023-03-01",
    type: "sector_inquiry",
    sector: "lebensmittelhandel",
    parties: "Österreichischer Lebensmitteleinzelhandel (Rewe, Spar, Hofer/Lidl, Unimarkt, MPreis)",
    summary: "Die BWB veröffentlichte den Abschlussbericht ihrer Sektoruntersuchung zum Lebensmitteleinzelhandel. Der Markt ist hochkonzentriert (HHI >2500); Rewe und Spar kontrollieren >60% der Fläche. Empfehlungen: Reform des Nahversorgungsgesetzes, Stärkung der Lieferantenschutzregeln, Ausbau der BWB-Monitoring-Kapazitäten.",
    full_text: "BWB/Z-5623 Sektoruntersuchung Lebensmitteleinzelhandel. Marktstruktur: Österreichischer LEH ist einer der konzentriertesten in der EU; Top-4-Anteile >90% (Rewe Group 36%, Spar 27%, Hofer/Aldi 15%, Lidl 8%); HHI national >3000 (stark konzentriert). Befunde: (1) Lieferantenkonditionen: systematisch nachteilige Konditionen für kleine Lieferanten; Listungsgebühren undurchsichtig; erhöhtes Drohpotenzial durch Delistung; (2) Private Labels: rasanter Anstieg der Eigenmarkenanteile (>50% bei Hofer); Verdrängungs- und Nachahmungseffekte für Markenprodukte; (3) Preistransparenz: einheitliche Preissignalisierung über Medien begünstigt implizite Koordinierung; (4) Neue Marktteilnehmer: Online-Lebensmittelhandel (Rohlik, HelloFresh) kaum Marktdurchdringung. Empfehlungen: Erlass eines B2B-Lieferantenschutzgesetzes; verschärfte Fusionskontrolle für Regionalfilialen; Preistransparenzdatenbank für Konsumenten. Follow-up: BWB-Monitoring-Programm 2023-2026.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "§ 2 Abs. 1 Z 1 WettbG (Sektoruntersuchung)",
    status: "final",
  },
  {
    case_number: "BWB/Z-5890",
    title: "A1 Telekom Austria — Missbrauch Endkundenpreisgestaltung (margin squeeze)",
    date: "2024-01-22",
    type: "abuse_of_dominance",
    sector: "telekommunikation",
    parties: "A1 Telekom Austria AG",
    summary: "Die BWB leitete Untersuchung gegen A1 Telekom wegen möglicher Kosten-Preis-Schere (margin squeeze) im Festnetzbreitband ein. A1 soll Konkurrenzanbietern Vorleistungen zu Preisen anbieten, die ihnen keine kostendeckende Endkundenstrategie erlauben. Verfahren läuft.",
    full_text: "BWB/Z-5890 A1 Telekom margin squeeze Festnetzbreitband. Sachverhalt: A1 als regulierter Anbieter mit signifikanter Marktmacht (SMP) im Festnetzmarkt liefert Vorleistungen (Bitstrom, Entbündelter Teilnehmeranschluss) an Alternativanbieter. Vorwurf: Vorleistungspreise so hoch, dass bei A1-Endkundenpreisen keine kostendeckende Marge für Alternativanbieter verbleibt (margin squeeze / Kosten-Preis-Schere). Betroffene Alternativanbieter: Mass Response, UPC/Liberty, Drei Breitband. Test: AKZO Nobel-Test (equally efficient competitor); A1 als vertikal integrierter Anbieter muss Marge für Vorleistungskunde erhalten. Rechtliche Grundlage: § 5 KartG; Art. 102 TFUE; Koordinierung mit RTR (Sektorregulierung). Stand: Ermittlungsverfahren eröffnet Januar 2024; keine Entscheidung zur Veröffentlichung dieses Berichts.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "§ 5 KartG, Art. 102 TFUE",
    status: "pending",
  },
  {
    case_number: "BWB/Z-5210",
    title: "Bankentgelte — Koordiniertes Verhalten bei Kontoführungsgebühren",
    date: "2022-05-10",
    type: "cartel",
    sector: "banken",
    parties: "Erste Bank; Raiffeisen; UniCredit Bank Austria; BAWAG; Volksbank",
    summary: "Die BWB schloss einen Verfahrensvergleich mit fünf österreichischen Banken wegen koordinierten Verhaltens bei der Erhöhung von Kontoführungsgebühren. Geldbußen von insgesamt EUR 6,5 Millionen. Banken hatten über einen Interessenverband Informationen über geplante Gebührenerhöhungen ausgetauscht.",
    full_text: "BWB/Z-5210 Bankentgelte koordiniertes Verhalten. Sachverhalt: Über Branchentreffen des Vereins österreichischer Banken (anonymisiert) koordinierten Banken den Zeitpunkt und die Höhe von Erhöhungen der Kontoführungsgebühren. Austausch umfasste: geplante Zeitpunkte der Gebührenerhöhungen; Höhe der neuen Gebühren; Kommunikationsstrategie gegenüber Kunden. Verstoß: § 1 KartG — Informationsaustausch mit wettbewerbsbeschränkender Wirkung auf dem Retailbanking-Markt. Vergleich: Kronzeugenprogramm — eine Bank erhielt Kronzeugenstatus (vollständige Bußgeldimmunität); weitere Reduktionen für Kooperation. Geldbußen: EUR 6,5 Mio. gesamt (Spanne EUR 0,8M bis EUR 2,1M je Bank). Abhilfemaßnahmen: keine koordinierten Branchentreffen; keine Weitergabe sensibler Preisinformationen; Compliance-Programme.",
    outcome: "fine",
    fine_amount: 6500000,
    gwb_articles: "§ 1 KartG, Art. 101 TFUE",
    status: "final",
  },
];

const insertDecision = db.prepare(`INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const d of decisions) insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status);
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow { case_number: string; title: string; date: string; sector: string; acquiring_party: string; target: string; summary: string; full_text: string; outcome: string; turnover: number | null; }

const mergers: MergerRow[] = [
  {
    case_number: "BWB/Z-5700",
    title: "Spar Österreich / MPreis Standorte Tirol",
    date: "2023-02-28",
    sector: "lebensmittelhandel",
    acquiring_party: "Spar Österreich GmbH",
    target: "MPreis Warenvertriebs GmbH (ausgewählte Standorte Tirol)",
    summary: "Die BWB genehmigte die Übernahme von 12 MPreis-Standorten in Tirol durch Spar mit Bedingungen. In 4 Gemeinden drohte Beseitigung jeglichen Lebensmitteleinzelhandels-Wettbewerbs. Spar musste sich zur Veräußerung von 4 Standorten an unabhängige Betreiber verpflichten.",
    full_text: "BWB/Z-5700 Spar / MPreis Tirol. Transaktion: Spar erwirbt 12 Filialen von MPreis in Tirol. Marktanalyse: 4 betroffene Gemeinden (Schwaz, Jenbach, Kufstein-Nord, Wörgl-Süd) mit Marktanteilen Spar+MPreis post-Merger >80% ohne Alternativangebote in 15-Minuten-Fahrradius. 8 Gemeinden ohne erhebliche Wettbewerbsbedenken. Auflage: Veräußerung von 4 Standorten an zugelassene unabhängige Käufer (nicht Rewe, Hofer, Lidl) innerhalb 6 Monate. Fix-it-first: BWB-Genehmigung nach Identifikation eines Käufers für 4 Standorte. Monitoring: BWB-Trustee überwacht Verkauf.",
    outcome: "cleared_with_conditions",
    turnover: 340000000,
  },
  {
    case_number: "BWB/Z-5540",
    title: "Verbund AG / Kelag-Anteile (Kärntner Elektrizitäts-AG)",
    date: "2022-07-14",
    sector: "energie",
    acquiring_party: "Verbund AG",
    target: "Kelag (Kärntner Elektrizitäts-AG) — Beteiligung 35%",
    summary: "Die BWB genehmigte in Fase 1 die Aufstockung der Verbund-Beteiligung an der Kelag von 35% auf 55% ohne Auflagen. Die Transaktion verstärkt zwar die vertikale Integration im österreichischen Strommarkt, überschreitet aber nicht die Aufgreifkriterien für erhebliche Marktstörungen.",
    full_text: "BWB/Z-5540 Verbund / Kelag. Transaktion: Verbund erhöht Beteiligung an Kelag von 35% (Minderheitsbeteiligung ohne Kontrolle) auf 55% (Kontrollerwerb). Verbund: größter österreichischer Stromproduzent (~50% der österr. Stromerzeugung, Schwerpunkt Wasserkraft). Kelag: Kärntner Versorger, ca. 400.000 Kunden, eigene Erzeugung und Netze. Marktanalyse: Stromerzeugung (Verbund dominiert national, Kelag Kärnten); Stromverteilung (keine Überlappung — regulierte Monopole); Stromlieferung (Kärnten: zusammen >65%, aber regulierter Markt). BWB-Analyse: Vertikale Integration nicht kritisch bei regulierten Netzen; Marktstörung im liberalisierten Segment Kärnten möglich aber unterhalb Eingriffsschwelle. Genehmigung Fase 1 ohne Auflagen.",
    outcome: "cleared_phase1",
    turnover: 2100000000,
  },
  {
    case_number: "BWB/Z-5812",
    title: "ProSiebenSat.1 PULS 4 / Joyn (Joint Venture Streaming)",
    date: "2023-11-30",
    sector: "medien",
    acquiring_party: "ProSiebenSat.1 PULS 4 GmbH",
    target: "Joyn GmbH (Joint Venture RTL Deutschland und ProSiebenSat.1)",
    summary: "Die BWB genehmigte die österreichische Ausdehnung des deutschen Streaming-Plattform-Joint-Ventures Joyn (ProSiebenSat.1 und RTL Deutschland) mit Auflagen. Joyn soll Inhalte beider Sendergruppen auf einer Plattform bündeln. Auflage: offener Plattformzugang für unabhängige österreichische Produzenten.",
    full_text: "BWB/Z-5812 ProSiebenSat.1 / Joyn Österreich. Transaktion: Gründung von Joyn GmbH in Österreich als Joint Venture zwischen ProSieben PULS 4 (AT) und RTL/Bertelsmann. Joyn bündelt Free-TV-Inhalte beider Gruppen auf einer Streaming-Plattform (AVOD-Modell). Marktanalyse: (1) Free-TV-Werbung Österreich: ProSieben PULS 4 + RTL gemeinsam ~45% (Kollektivmarktmacht); (2) Streaming AVOD: noch junger Markt; (3) Content-Beschaffung: JV könnte österreichische Produktionen bevorzugt aufnehmen oder ausschließen. Wettbewerbsbedenken: Koordinierungsrisiko zwischen ProSieben und RTL im TV-Werbemarkt; Ausschluss österreichischer Inhalte. Auflagen: (1) keine Koordinierung der TV-Werbetarife zwischen ProSieben PULS 4 und RTL außerhalb Joyn; (2) Joyn muss österreichischen Unabhängigproduzenten diskriminierungsfreien Zugang zu Plattform anbieten. Monitoring 5 Jahre.",
    outcome: "cleared_with_conditions",
    turnover: 780000000,
  },
];

const insertMerger = db.prepare(`INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const m of mergers) insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover);
console.log(`Inserted ${mergers.length} mergers`);

const dc = (db.prepare("SELECT COUNT(*) as n FROM decisions").get() as { n: number }).n;
const mc = (db.prepare("SELECT COUNT(*) as n FROM mergers").get() as { n: number }).n;
const sc = (db.prepare("SELECT COUNT(*) as n FROM sectors").get() as { n: number }).n;
console.log(`\nDatabase summary:\n  Decisions: ${dc}\n  Mergers: ${mc}\n  Sectors: ${sc}\n\nSeed complete.`);
