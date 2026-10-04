const projects = [["wiktionary", "Wiktionary"], ["wikisource", "Wikisource"],
  ["wikiquote", "Wikiquote"], ["wikibooks", "Wikibooks"], ["wikinews", "Wikinews"],
  ["wikivoyage", "Wikivoyage"], ["wikiversity", "Wikiversity"], ["wiki", "Wikipedia"]];
const special = { commonswiki: "Wikimedia Commons", wikidatawiki: "Wikidata",
  metawiki: "Meta-Wiki", specieswiki: "Wikispecies", mediawikiwiki: "MediaWiki.org",
  wikifunctionswiki: "Wikifunctions", incubatorwiki: "Wikimedia Incubator" };
const languageNames = new Intl.DisplayNames(["en"], { type: "language" });
export function wikiName(code) {
  if (!code) return "Unknown project";
  if (special[code]) return special[code];
  for (const [suffix, project] of projects) {
    if (code.endsWith(suffix) && code.length > suffix.length) {
      const language = code.slice(0, -suffix.length).replaceAll("_", "-");
      try { return `${languageNames.of(language)} ${project}`; }
      catch { return `${project} (${language})`; }
    }
  }
  return code;
}
export const typeLabels = { edit: "Edits to existing pages", categorize: "Category updates",
  new: "New pages created", log: "Uploads, moves & admin actions" };
export const typeShort = { edit: "Edit", categorize: "Category", new: "New page", log: "Log action" };
export const colorOf = (type) => ({ edit: "var(--series-1)", categorize: "var(--series-2)",
  new: "var(--series-3)", log: "var(--series-4)" })[type] || "var(--series-other)";
export const fmt = (n) => n == null ? "—" : Intl.NumberFormat("en-US").format(n);
export const hhmm = (t) => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
