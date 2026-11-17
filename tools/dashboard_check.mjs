/**
 * Runs the dashboard's own JavaScript against the live API and inspects what it renders.
 *
 * The dashboard is the one part of this lab a test cannot reach through HTTP: the API can
 * answer perfectly while the page shows nothing, because a single renamed field throws in a
 * render function and leaves an empty panel. That failure looks like "the demo is broken"
 * and is invisible to every other check here.
 *
 * So this loads src/LinePulse.Core/wwwroot/index.html, extracts its script, and executes it
 * against a minimal DOM with the real fetch: whatever the page would draw, it draws here, and
 * the result is then searched for empty panels, NaN, undefined and unresolved placeholders.
 *
 * What this does NOT check is appearance -- layout, colour, contrast, whether a chart is
 * readable. Nothing headless on this machine can do that (Chrome's headless mode fails on
 * macOS 12 here), so that part has to be checked by opening the page.
 *
 * Usage, with the stack running:
 *     node tools/dashboard_check.mjs
 */

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PAGE = resolve(ROOT, "src/LinePulse.Core/wwwroot/index.html");
const BASE = process.env.LP_BASE ?? "http://127.0.0.1:5080";

// ---------------------------------------------------------------- a very small DOM

class Node_ {
  constructor(tag, ns = null) {
    this.tagName = tag;
    this.namespaceURI = ns;
    this.children = [];
    this.attributes = {};
    this.listeners = {};
    this.style = { cssText: "" };
    this.dataset = {};
    this._text = "";
    this.className = "";
    this.value = "";
    this.nodeType = 1;
  }

  set textContent(value) {
    this._text = String(value);
    this.children = [];
  }

  get textContent() {
    return this._text + this.children.map((c) => c.textContent ?? "").join("");
  }

  append(...kids) {
    for (const kid of kids.flat()) {
      if (kid === null || kid === undefined) continue;
      this.children.push(typeof kid === "object" ? kid : { textContent: String(kid) });
    }
  }

  replaceChildren(...kids) {
    this.children = [];
    this._text = "";
    this.append(...kids);
    // A browser selects the first option of a <select> as soon as it has options, and the
    // page reads that value to decide what to chart. Without this the stub reports an empty
    // selection, which is not a state a browser can be in.
    if (this.tagName === "select" && this.children.length > 0) {
      this.value = this.children[0].attributes?.value ?? "";
    }
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
    if (name === "class") this.className = String(value);
  }

  getAttribute(name) {
    return this.attributes[name] ?? null;
  }

  addEventListener(type, handler) {
    (this.listeners[type] ??= []).push(handler);
  }

  /** Flattened description of this subtree, for assertions. */
  describe() {
    const parts = [this._text];
    for (const kid of this.children) {
      parts.push(kid.describe ? kid.describe() : String(kid.textContent ?? ""));
    }
    const attrs = Object.entries(this.attributes).map(([k, v]) => `${k}=${v}`).join(" ");
    return `${this.tagName}${this.className ? "." + this.className : ""}${attrs ? "[" + attrs + "]" : ""}{${parts.join("|")}}`;
  }

  countTags(tag, found = { n: 0 }) {
    if (this.tagName === tag) found.n++;
    for (const kid of this.children) kid.countTags?.(tag, found);
    return found.n;
  }
}

const byId = new Map();
globalThis.document = {
  documentElement: new Node_("html"),
  getElementById: (id) => byId.get(id) ?? null,
  createElement: (tag) => new Node_(tag),
  createElementNS: (ns, tag) => new Node_(tag, ns),
  // The page wraps every text child in a real text node, so the stub has to offer one.
  // Without it the first el() call throws, init() stops before it requests anything, and
  // the page's own .catch handlers swallow it -- which is how a blank dashboard happens in
  // a browser too.
  createTextNode: (value) => {
    const node = new Node_("#text");
    node.nodeType = 3;
    node.textContent = String(value);
    return node;
  },
};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {} });
globalThis.location = { origin: BASE };

// The page's own fetch, but resolved against the live API.
const realFetch = globalThis.fetch;
const requested = [];
globalThis.fetch = (path, init) => {
  requested.push(String(path));
  return realFetch(String(path).startsWith("http") ? path : BASE + path, init);
};

// The live stream is not exercised here: an open EventSource never closes, which is exactly
// what stops a headless browser from ever finishing. The REST fallback renders the same
// panels, and the stream's event names are checked separately against the running service.
let streamOpened = 0;
globalThis.EventSource = class {
  constructor() { streamOpened++; }
  addEventListener() {}
  close() {}
};

// Timers are given to the page as scoped parameters rather than assigned onto globalThis.
// Replacing the global setTimeout breaks Node's own fetch implementation, which uses it
// internally for socket timeouts -- and a harness that corrupts the thing it is measuring
// with proves nothing.
const wait = (ms) => new Promise((done) => setTimeout(done, ms));

const timers = [];
const pageSetInterval = (fn, ms) => { timers.push({ fn, ms }); return timers.length; };
// The page debounces one refresh by a few hundred milliseconds; shortened so the check does
// not wait on it, but still asynchronous, because collapsing it to a synchronous call would
// change the order the page renders in.
const pageSetTimeout = (fn, ms) => setTimeout(fn, Math.min(ms ?? 0, 50));

// ------------------------------------------------------------------------- the page

const html = readFileSync(PAGE, "utf8");

// Every element the script looks up by id has to exist, as it does in the real page.
for (const id of new Set([...html.matchAll(/id="([\w-]+)"/g)].map((m) => m[1]))) {
  byId.set(id, new Node_("div"));
}

const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
if (!script) {
  console.error("FAIL  no inline script found in the page");
  process.exit(1);
}

const failures = [];
const notes = [];

try {
  // The page runs with its own timers; everything else it touches is the stubbed DOM above.
  new Function("setTimeout", "setInterval", script)(pageSetTimeout, pageSetInterval);
} catch (error) {
  failures.push(`the page's script threw on load: ${error.message}`);
}

// Let the fetches settle; the page renders from promises.
await wait(2500);

// ----------------------------------------------------------------------- assertions

function panel(id, { minLength = 40, mustContain = [], mustNotContain = [] } = {}) {
  const node = byId.get(id);
  if (!node) {
    failures.push(`#${id} does not exist on the page`);
    return "";
  }

  const rendered = node.describe();
  const text = node.textContent;
  if (text.trim().length < minLength) {
    failures.push(`#${id} rendered almost nothing (${text.trim().length} chars): ${text.trim().slice(0, 120)}`);
  }
  if (/waiting for|no lines reporting|nothing to show|no test records/i.test(text)) {
    failures.push(`#${id} shows its empty state: ${text.trim().slice(0, 120)}`);
  }
  for (const needle of mustContain) {
    if (!text.includes(needle) && !rendered.includes(needle)) {
      failures.push(`#${id} is missing ${JSON.stringify(needle)}`);
    }
  }
  for (const needle of mustNotContain) {
    if (text.includes(needle)) {
      failures.push(`#${id} contains ${JSON.stringify(needle)}`);
    }
  }
  for (const bad of ["NaN", "undefined", "null%", "Infinity", "[object Object]"]) {
    if (text.includes(bad)) {
      failures.push(`#${id} rendered ${bad}: ${text.slice(Math.max(0, text.indexOf(bad) - 60), text.indexOf(bad) + 40)}`);
    }
  }

  notes.push(`#${id}: ${text.trim().length} chars`);
  return text;
}

// Line cards: all four lines, each with its three OEE factors.
const cards = panel("cards", {
  minLength: 200,
  mustContain: ["IZM", "NIS", "L01", "MC01", "Availability", "Performance", "Quality", "OEE"],
});
for (const line of ["L01", "L02", "MC01", "L11"]) {
  if (!cards.includes(line)) failures.push(`the line cards do not mention ${line}`);
}
const percentages = cards.match(/\d+\.\d%/g) ?? [];
if (percentages.length < 16) {
  failures.push(`expected at least 16 percentages across four cards, found ${percentages.length}`);
}

// Downtime Pareto: an SVG with bars, and the micro-stop sentence.
const pareto = panel("pareto", { minLength: 20 });
const paretoSvg = byId.get("pareto")?.countTags("svg") ?? 0;
const bars = byId.get("pareto")?.countTags("rect") ?? 0;
if (paretoSvg < 1) failures.push("the downtime chart drew no SVG");
if (bars < 1) failures.push("the downtime chart drew no bars");
notes.push(`downtime chart: ${bars} bars`);
panel("paretoCallout", { minLength: 20 });

// Fixtures: a row per fixture with a status chip, and the drifting one must be visible.
const fixtures = panel("fixtures", { minLength: 200, mustContain: ["FX-01", "FX-02", "FX-03", "Fixture", "Status"] });
const statuses = ["ok", "warning", "alarm", "insufficient-data"].filter((s) => fixtures.includes(s));
if (statuses.length === 0) failures.push("no fixture status was rendered");
notes.push(`fixture statuses rendered: ${statuses.join(", ")}`);

// Site rollup: both sites, with shift windows.
panel("sites", { minLength: 150, mustContain: ["IZM", "NIS", "Shift", "First-pass yield", "local"] });

// Part genealogy: look up a real serial through the page's own code path.
const serial = (await (await globalThis.fetch("/api/lines")).json())[0];
const anyPart = await (await globalThis.fetch("/api/quality/fpy")).json();
if (!Array.isArray(anyPart) || anyPart.length === 0) {
  failures.push("the API reports no tested lines, so the genealogy panel cannot be checked");
} else {
  const input = byId.get("serial");
  const button = byId.get("lookup");
  const example = byId.get("examples");
  const tryButton = example?.children?.find?.((c) => c.listeners?.click?.length);
  if (!tryButton) {
    failures.push("the genealogy panel offers no example buttons");
  } else {
    tryButton.listeners.click[0]();
    await wait(1500);
    const part = panel("part", { minLength: 150, mustContain: ["Leak rate", "HOUSING LOT", "MACHINED WITH", "Tool"] });
    if (!/[A-Z]{3}-[A-Z0-9]+-\d{4}-\d{6}/.test(part)) {
      failures.push("the genealogy panel shows no serial number");
    }
  }
  void serial;
  void input;
  void button;
}

// Routes: the page must be talking to the API this repo documents.
const expected = ["/api/lines", "/api/quality/fixtures", "/api/sites/rollup", "/api/lines/IZM/L01/downtime"];
for (const route of expected) {
  if (!requested.some((r) => r.includes(route))) failures.push(`the page never requested ${route}`);
}
notes.push(`requests made: ${new Set(requested.map((r) => r.split("?")[0])).size} distinct routes`);
notes.push(`live stream opened: ${streamOpened} (stubbed here on purpose)`);
notes.push(`refresh timers registered: ${timers.map((t) => t.ms + "ms").join(", ")}`);

// ---------------------------------------------------------------------------- verdict

console.log("dashboard render check");
for (const note of notes) console.log(`  .  ${note}`);
if (failures.length === 0) {
  console.log("\nall panels rendered with live data; no NaN, undefined or empty states");
  console.log("NOT checked here: appearance (layout, colour, readability). Open the page for that.");
  process.exit(0);
}

console.log("");
for (const failure of failures) console.log(`  FAIL  ${failure}`);
process.exit(1);
