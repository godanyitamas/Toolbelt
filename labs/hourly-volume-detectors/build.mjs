import { writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { CFG, EVENTS, build as buildHours, ewmaLimit, flags, arl, robustFalseRate } from "./model.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
// The figure kit comes from the anatomy skill (github.com/wheresryan22/anatomy, MIT); set ANATOMY_DIR if it is not installed in the default place.
const SKILL = process.env.ANATOMY_DIR ?? join(homedir(), ".claude/skills/anatomy");
const k = await import(join(SKILL, "kit/iso-kit.mjs"));
const { kitScript } = await import(join(SKILL, "scripts/inline-kit.mjs"));
const theme = process.argv.includes("--light") ? "light" : "dark";

const HOURS = buildHours();
const WIDTH = 680;
const HEIGHT = 470;

// ---- world: x is time (one pin per hour, newest at the right), y across the bench, z up ----
const N = CFG.window;
const DX = 5;
const PIN = 2.2;
const pinX = (i) => 4 + DX * i;                   // i = 0 oldest … 47 now
const X_END = pinX(N - 1) + 7;                     // right end post
const B = 16;                                      // pin height at value 0
const ROWS = [
  { key: "z", y: -46, top: 40, scale: 3.6, clamp: 4.2, limit: CFG.zFlag },
  { key: "ewma", y: -2, top: 22, scale: 10, clamp: 1.5, limit: ewmaLimit() },
  { key: "cusum", y: 42, top: 4, scale: 2.6, clamp: 5.8, limit: CFG.h },
];
const BASE = { x: -26, y: -62, w: 346, d: 128, r: 6 };
const BASE_H = 10;
const FOOT_H = 4;
const HEAD_X = 256;                                // where each row's mechanism starts
const RACK_UNIT = 450;                             // messages per world unit on the reference rack
const RACK_X = (j) => HEAD_X + 4 + 5 * j;          // 7 reference blocks
const NOW_X = HEAD_X + 44;                         // the current hour's block
const DIAL = [HEAD_X + 26, ROWS[1].y];
const DIAL_R = 15;
const DIAL_Z = ROWS[1].top + 9;
const DIAL_TOP = DIAL_Z + 5;
const DIAL_SPAN = Math.PI * 1.5;
const DIAL_MAX = 3;
const LAMP = [HEAD_X + 26, ROWS[2].y];
const SWITCH = [-14, 52];

const P = k.fitProjection(
  [...k.boxCorners(BASE, -BASE_H - FOOT_H, 0), [BASE.x, ROWS[0].y, ROWS[0].top + 2 * B + 6], [BASE.x + BASE.w, ROWS[0].y, ROWS[0].top + 2 * B + 6]],
  WIDTH,
  HEIGHT,
  { pad: 22, azimuth: 64 },
);

const at = (point) => k.iso(point, P);
const S = (paths, style) => k.solidSvg(paths, style);
const L = (d, style) => k.lineSvg(d, style);
const D = (points, style) => k.dotsSvg(points, style);
const disc = (x, y, r, z, h, steps = 28, bevel = 0) => k.cylinder(x, y, r, z, h, P, steps, bevel);
const byDepth = (points) => [...points].sort((a, b) => k.depthOf([a[0], a[1], 0], P) - k.depthOf([b[0], b[1], 0], P));

const heightOf = (row, v) => B + Math.max(-row.clamp, Math.min(row.clamp, v)) * row.scale;
const pinSvg = (x, y, z, h, lit, capped) =>
  k.solidSvg(k.slabOf({ x: x - 1.1, y: y - 1.1, w: 2.2, d: 2.2, r: 0.5 }, z, Math.max(0.6, h), P, 2, 0.3), { tone: lit ? "hi" : "mid", crease: "lo", lit }) +
  (capped ? k.solidSvg(k.slabOf({ x: x - 1.9, y: y - 1.9, w: 3.8, d: 3.8, r: 0.7 }, z + Math.max(0.6, h), 1, P, 2, 0.3), { tone: lit ? "hi" : "mid", crease: "lo", lit }) : "");

// ---------------------------------------------------------------- bench
function bench() {
  const out = [`<path class="iso-halo" d="${k.haloOf(BASE, -BASE_H - FOOT_H, BASE_H, P)}"/>`];
  for (const [x, y] of byDepth(k.corners(BASE, 14))) {
    out.push(S(disc(x, y, 6.4, -BASE_H - FOOT_H, 1.4), { tone: "lo" }));
    out.push(S(disc(x, y, 5, -BASE_H - FOOT_H + 1.4, FOOT_H - 1.4), { tone: "mid" }));
  }
  out.push(S(k.slabOf(BASE, -BASE_H, BASE_H, P, 8, 1.4), { tone: "mid", inner: L(k.sideSeam(BASE, -BASE_H + 3, P, 8), { tone: "faint" }) }));
  out.push(L(k.planOutline(k.insetPlan(BASE, 4.5), 0, P), { tone: "lo" }));
  const screws = k.corners(BASE, 9);
  out.push(L(screws.map(([x, y]) => k.ring(x, y, 2, 0, P, 16)).join(""), { tone: "lo" }));
  out.push(D(screws.map(([x, y]) => at([x, y, 0])), { size: 0.5 }));
  // hours-ago ruler under the front row: a tick per pin, a major every 6 hours
  const strip = { x: pinX(0) - 3, y: ROWS[2].y + 9, w: pinX(N - 1) - pinX(0) + 6, d: 7, r: 1 };
  out.push(S(k.slabOf(strip, 0, 0.6, P, 3), { tone: "lo", crease: "none" }));
  const minor = [], major = [];
  for (let i = 0; i < N; i++) {
    const long = (N - 1 - i) % 6 === 0;
    (long ? major : minor).push(k.lineOnTop([pinX(i), strip.y], [pinX(i), strip.y + (long ? 3.8 : 2)], 0.6, P));
  }
  out.push(L(minor.join(""), { tone: "lo" }), L(major.join(""), { tone: "mid" }));
  out.push(D([at([strip.x + 1.4, strip.y + 3.5, 0.6]), at([strip.x + strip.w - 1.4, strip.y + 3.5, 0.6])], { size: 0.45 }));
  // "now" index: a small lit wedge under the newest pin
  const n = pinX(N - 1);
  out.push(S(k.extrude([[n - 1.6, strip.y + 7.2], [n + 1.6, strip.y + 7.2], [n, strip.y + 5]], 0.6, 0.6, P), { tone: "hi", crease: "none" }));
  // label plate with three rule lines, left of the ruler
  const plate = { x: BASE.x + 8, y: ROWS[2].y + 6, w: 20, d: 12, r: 1 };
  out.push(S(k.slabOf(plate, 0, 0.8, P, 3), { tone: "lo", crease: "none" }));
  out.push(L(k.planOutline(k.insetPlan(plate, 1.4), 0.8, P, 3), { tone: "faint" }));
  out.push(L([[3, 13], [6, 9], [9, 11]].map(([dy, len]) => k.lineOnTop([plate.x + 3, plate.y + dy], [plate.x + 3 + len, plate.y + dy], 0.8, P)).join(""), { tone: "lo", free: true }));
  return out.join("");
}

// mode switch: a rocker on a small boss; pooled scale = rocker toward the back
function switchBase() {
  const [x, y] = SWITCH;
  const out = [];
  out.push(S(k.slabOf({ x: x - 6, y: y - 5, w: 12, d: 10, r: 2 }, 0, 4, P, 4, 0.5), { tone: "mid" }));
  out.push(L(k.planOutline({ x: x - 3.4, y: y - 3, w: 6.8, d: 6, r: 1 }, 4, P, 3), { tone: "lo" }));
  out.push(D([at([x - 4.6, y - 3.6, 4]), at([x + 4.6, y + 3.6, 4])], { size: 0.4 }));
  return out.join("");
}
const rockerSvg = (pooled) => {
  const [x, y] = SWITCH;
  const lift = pooled ? [2.6, 0.8] : [0.8, 2.6];   // back half / front half height
  return [
    S(k.slabOf({ x: x - 3, y: y - 2.6, w: 6, d: 2.6, r: 0.6 }, 4, lift[0], P, 2, 0.3), { tone: "hi", crease: "lo" }),
    S(k.slabOf({ x: x - 3, y, w: 6, d: 2.6, r: 0.6 }, 4, lift[1], P, 2, 0.3), { tone: "mid", crease: "lo" }),
  ].join("");
};

// ---------------------------------------------------------------- one row: housing, posts, wires
function housing(row) {
  const plan = { x: -10, y: row.y - 7, w: HEAD_X + 52 + 10, d: 14, r: 2 };
  const out = [];
  out.push(`<path class="iso-halo" d="${k.haloOf(plan, 0, row.top, P)}" style="filter:blur(5px)"/>`);
  const seams = row.top > 12 ? L([6, row.top - 5].map((z) => k.sideSeam(plan, z, P, 3)).join(""), { tone: "faint" }) : "";
  out.push(S(k.slabOf(plan, 0, row.top, P, 3, 0.6), { tone: "mid", inner: seams }));
  out.push(L(k.planOutline({ x: pinX(0) - 2.4, y: row.y - 1.8, w: pinX(N - 1) - pinX(0) + 4.8, d: 3.6, r: 1.4 }, row.top, P, 3), { tone: "lo" }));
  out.push(D([[-6, row.y - 4], [-6, row.y + 4], [HEAD_X + 48, row.y - 4], [HEAD_X + 48, row.y + 4]].map(([x, y]) => at([x, y, row.top])), { size: 0.45 }));
  return out.join("");
}

const POST_H = 2 * B + 4;
const postSvg = (x, row) => {
  const out = [];
  out.push(S(k.slabOf({ x: x - 2.2, y: row.y - 5.4, w: 4.4, d: 4.4, r: 0.8 }, row.top, POST_H, P, 2, 0.4), { tone: "mid" }));
  out.push(D([at([x, row.y - 3.2, row.top + POST_H])], { size: 0.45, tone: "hi" }));
  return out.join("");
};

function wires(row) {
  const y = row.y - 3.2;
  const z0 = row.top + B;
  const line = (z) => k.segment([-1.4, y, z], [X_END - 1.2, y, z], P);
  const levels = [row.limit, -row.limit].map((v) => z0 + v * row.scale);
  return [
    k.dotsSvg([z0, ...levels].map((z) => at([-1.4, y, z])), { size: 0.75, tone: "mid" }),
    `<path class="iso-line" data-tone="lo" d="${line(z0)}" style="stroke-width:.8"/>`,
    `<path class="iso-line es-limit" data-tone="mid" d="${levels.map(line).join("")}"/>`,
  ].join("");
}

// ---------------------------------------------------------------- heads
function rackBase(row) {
  const out = [];
  const plan = { x: HEAD_X, y: row.y - 6, w: 50, d: 12, r: 1.4 };
  out.push(S(k.slabOf(plan, row.top, 2, P, 3, 0.4), { tone: "lo" }));
  for (const x of [HEAD_X + 1.8, HEAD_X + 48.2]) {
    out.push(S(k.slabOf({ x: x - 1.6, y: row.y - 5.6, w: 3.2, d: 3.2, r: 0.6 }, row.top + 2, POST_H - 2, P, 2, 0.3), { tone: "mid" }));
    out.push(D([at([x, row.y - 4, row.top + POST_H])], { size: 0.4, tone: "hi" }));
  }
  return out.join("");
}
const rackRods = (row, med, scale) => {
  const y = row.y - 4;
  const z = (v) => row.top + 2 + v / RACK_UNIT;
  const rod = (v) => k.segment([HEAD_X + 3.4, y, z(v)], [HEAD_X + 46.6, y, z(v)], P);
  const half = CFG.zFlag * scale;
  return `<path class="iso-line" data-tone="lo" d="${rod(med)}" style="stroke-width:.8"/><path class="iso-line es-limit" data-tone="mid" d="${rod(med + half)}${med - half > 0 ? rod(med - half) : ""}"/>`;
};
const blocksSvg = (row, refs, x, lit) => {
  const sorted = [...refs].sort((a, b) => a - b);
  const z0 = row.top + 2;
  const out = sorted.map((v, j) => k.solidSvg(k.slabOf({ x: RACK_X(j) - 1.8, y: row.y - 1.6, w: 3.6, d: 5, r: 0.5 }, z0, Math.max(0.6, v / RACK_UNIT), P, 2, 0.3), { tone: "mid", crease: "lo" }));
  out.push(k.solidSvg(k.slabOf({ x: NOW_X - 2, y: row.y - 1.6, w: 4, d: 5, r: 0.5 }, z0, Math.max(0.6, x / RACK_UNIT), P, 2, 0.3), { tone: lit ? "hi" : "mid", crease: "lo", lit }));
  return out.join("");
};

function dial() {
  const [x, y] = DIAL;
  const row = ROWS[1];
  const out = [];
  out.push(S(k.slabOf({ x: x - 9, y: y - 6, w: 18, d: 12, r: 2 }, row.top, 3, P, 3, 0.4), { tone: "lo" }));
  out.push(S(disc(x, y, 3, row.top + 3, DIAL_Z - row.top - 3, 18), { tone: "mid" }));
  out.push(S(disc(x, y, DIAL_R, DIAL_Z, DIAL_TOP - DIAL_Z, 64, 0.8), { tone: "hi", crease: "lo", inner: L(k.sideArc(x, y, DIAL_R, DIAL_TOP - 2, P, 40), { tone: "faint" }) }));
  out.push(L(k.knurl(x, y, DIAL_R, DIAL_TOP - 2, DIAL_TOP, 64, P, 0.3), { tone: "lo" }));
  out.push(L(k.ring(x, y, DIAL_R - 1.8, DIAL_TOP, P, 64), { tone: "lo" }));
  const ticks = k.radialTicks(x, y, DIAL_R - 2.4, 30, 5, DIAL_TOP, [1, 2.4], P, -DIAL_SPAN / 2, DIAL_SPAN);
  out.push(L(ticks.minor, { tone: "lo" }), L(ticks.major, { tone: "mid" }));
  // the ±limit band beyond which the EWMA alarms, as two closed arcs
  const arc = (r, from, to) => Array.from({ length: 13 }, (_, i) => { const a = from + ((to - from) * i) / 12; return [x + Math.sin(a) * r, y - Math.cos(a) * r]; });
  const lim = (ewmaLimit() / DIAL_MAX) * (DIAL_SPAN / 2);
  for (const [from, to] of [[lim, DIAL_SPAN / 2], [-DIAL_SPAN / 2, -lim]]) out.push(L(k.onTop([...arc(DIAL_R - 6.6, from, to), ...arc(DIAL_R - 5.6, from, to).reverse()], DIAL_TOP, P, true), { tone: "mid" }));
  out.push(L(k.ring(x, y, 1.4, DIAL_TOP, P, 16), { tone: "mid" }));
  return out.join("");
}
const needleAt = (v) => {
  const a = (Math.max(-DIAL_MAX * 1.02, Math.min(DIAL_MAX * 1.02, v)) / DIAL_MAX) * (DIAL_SPAN / 2);
  const [x, y] = DIAL;
  return k.lineOnTop([x - Math.sin(a) * 3, y + Math.cos(a) * 3], [x + Math.sin(a) * (DIAL_R - 3.4), y - Math.cos(a) * (DIAL_R - 3.4)], DIAL_TOP + 0.2, P);
};

function lamp() {
  const [x, y] = LAMP;
  const row = ROWS[2];
  const out = [];
  out.push(S(disc(x, y, 8, row.top, 2.4, 40, 0.5), { tone: "lo" }));
  out.push(D([0, 1, 2, 3].map((i) => { const a = Math.PI / 4 + (i * Math.PI) / 2; return at([x + Math.cos(a) * 6.2, y + Math.sin(a) * 6.2, row.top + 2.4]); }), { size: 0.4 }));
  out.push(S(disc(x, y, 2.4, row.top + 2.4, 14, 18), { tone: "mid", inner: L(k.sideArc(x, y, 2.4, row.top + 9, P, 12), { tone: "faint" }) }));
  out.push(S(disc(x, y, 5.6, row.top + 16.4, 3, 32, 0.4), { tone: "mid" }));
  out.push(L(k.knurl(x, y, 5.6, row.top + 16.4, row.top + 19.4, 24, P), { tone: "lo" }));
  return out.join("");
}
const lensSvg = (on) => {
  const [x, y] = LAMP;
  const z = ROWS[2].top + 19.4;
  return [
    k.solidSvg(disc(x, y, 4.6, z, 6, 32), { tone: on ? "hi" : "mid", crease: "lo", lit: on }),
    k.solidSvg(disc(x, y, 3.4, z + 6, 1.8, 28), { tone: on ? "hi" : "mid", crease: "lo", lit: on }),
    k.solidSvg(disc(x, y, 5.2, z + 7.8, 1.2, 32, 0.3), { tone: "mid" }),
  ].join("");
};

// ---------------------------------------------------------------- state at an hour
const H0 = N - 1;                                  // first hour with a full window
const H1 = HOURS.length - 1;
const MODE0 = "pooled";
const valuesAt = (H, h, mode) => {
  const win = H.slice(h - N + 1, h + 1);
  return ROWS.map((row) => win.map((r) => {
    const s = r[mode];
    const v = row.key === "z" ? s.z : row.key === "ewma" ? s.ewma : s.up >= s.down ? s.up : -s.down;
    return { v, lit: flags(s)[row.key], capped: Math.abs(v) > row.clamp };
  }));
};

function pinsSvg(rowIndex, values) {
  const row = ROWS[rowIndex];
  return values.map(({ v, lit, capped }, i) => `<g data-pin="${i}">${pinSvg(pinX(i), row.y, row.top, heightOf(row, v), lit, capped)}</g>`).join("");
}

const start = valuesAt(HOURS, H0, MODE0);
const first = HOURS[H0];
const body = [];
body.push(bench());
body.push(switchBase(), `<g data-part="rocker">${rockerSvg(true)}</g>`);
ROWS.forEach((row, ri) => {
  body.push(housing(row));
  body.push(postSvg(-3.6, row));
  body.push(wires(row));
  body.push(`<g data-row="${ri}">${pinsSvg(ri, start[ri])}</g>`);
  body.push(postSvg(X_END, row));
  if (row.key === "z") {
    body.push(rackBase(row));
    body.push(`<g data-part="rods">${rackRods(row, first.med, first[MODE0].scale)}</g>`);
    body.push(`<g data-part="blocks">${blocksSvg(row, first.refs, first.x, flags(first[MODE0]).z)}</g>`);
  }
  if (row.key === "ewma") {
    body.push(dial());
    body.push(`<path data-part="needle" class="iso-line" data-tone="hi" data-free="" d="${needleAt(first[MODE0].ewma)}" style="stroke-width:1.1"/>`);
  }
  if (row.key === "cusum") {
    body.push(lamp());
    body.push(`<g data-part="lens">${lensSvg(first[MODE0].signal)}</g>`);
  }
});

const svg = k.figureSvg({
  width: WIDTH,
  height: HEIGHT,
  label:
    "A bench with three rows of 48 pins, one pin per hour, newest at the right. The back row shows the robust z score of each hour's message volume against the same hour on previous weeks; a rack of reference blocks at its end shows those weeks and the current hour. The middle row shows the EWMA of that z score, read on a dial. The front row shows the two-sided CUSUM, with a lamp that lights on a signal. The outer rods on each row mark the alarm limits; a pin past them lights up, and a pin off the scale wears a wider cap.",
  body,
});

const clock = (h) => ["Mon", "Tue", "Wed", "Thu", "Fri"][Math.floor(h / 24)] + " " + String(h % 24).padStart(2, "0") + ":00";

// ---------------------------------------------------------------- numbers for the static table
const WEEK_SETS = [7, 4, 2];
const TABLE = WEEK_SETS.map((weeks) => {
  const c = { ...CFG, weeks };
  return { weeks, pooled: arl("pooled", c), local: arl("local", c), share: robustFalseRate(c, 100000) };
});
console.log("false-alarm table", JSON.stringify(TABLE));

const live = `
${readFileSync(join(HERE, "model.mjs"), "utf8").replace(/^export /gm, "").replace(/^import .*$/gm, "")}
const P = ${JSON.stringify(P)};
const N = ${N}, DX = ${DX}, B = ${B}, H0 = ${H0}, H1 = ${H1};
const ROWS = ${JSON.stringify(ROWS)};
const HEAD_X = ${HEAD_X}, RACK_UNIT = ${RACK_UNIT}, NOW_X = ${NOW_X}, POST_H = ${POST_H};
const DIAL = ${JSON.stringify(DIAL)}, DIAL_R = ${DIAL_R}, DIAL_TOP = ${DIAL_TOP}, DIAL_SPAN = ${DIAL_SPAN}, DIAL_MAX = ${DIAL_MAX};
const LAMP = ${JSON.stringify(LAMP)}, SWITCH = ${JSON.stringify(SWITCH)};
const pinX = ${pinX.toString()};
const RACK_X = ${RACK_X.toString()};
const disc = (x, y, r, z, h, steps = 28, bevel = 0) => cylinder(x, y, r, z, h, P, steps, bevel);
const S = (paths, style) => solidSvg(paths, style);
const heightOf = ${heightOf.toString()};
const pinSvg = ${pinSvg.toString().replace(/k\./g, "")};
const rackRods = ${rackRods.toString().replace(/k\./g, "")};
const blocksSvg = ${blocksSvg.toString().replace(/k\./g, "")};
const needleAt = ${needleAt.toString().replace(/k\./g, "")};
const lensSvg = ${lensSvg.toString().replace(/k\./g, "")};
const rockerSvg = ${rockerSvg.toString().replace(/k\./g, "")};
const valuesAt = ${valuesAt.toString()};
const clock = ${clock.toString()};
const DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
const SETS = {};
const dataFor = (w) => (SETS[w] ??= build({ ...CFG, weeks: w }));
const stage = document.querySelector(".iso-stage");
const svg = stage.querySelector("svg");
const q = (s) => svg.querySelector(s);
const rows = ROWS.map((_, i) => [...q('[data-row="' + i + '"]').children]);
const rods = q('[data-part="rods"]'), blocks = q('[data-part="blocks"]'), needle = q('[data-part="needle"]'), lens = q('[data-part="lens"]'), rocker = q('[data-part="rocker"]');
const readout = document.querySelector("[data-readout]");
const panel = document.querySelector(".es-panel");
const now = panel.querySelector("[data-now]");
const still = matchMedia("(prefers-reduced-motion: reduce)");
let mode = "pooled", weeks = 7, hour = H0;
let shown = valuesAt(dataFor(7), H0, mode).map((r) => r.map((p) => ({ ...p }))), needleAt0 = dataFor(7)[H0].pooled.ewma;
let frame = 0, last = 0, visible = false, touring = !still.matches, clockT = 0, dirty = true;
const fmt = (v) => (v < 0 ? "−" : "+") + Math.abs(v).toFixed(1);
const num = (v, d = 1) => (v < 0 ? "−" : "") + Math.abs(v).toFixed(d);
const int = (v) => Math.round(v).toLocaleString("en-US");
const scaleName = () => (mode === "pooled" ? "pooled scale" : weeks + "-point MAD");
function setHour(h) { h = Math.max(H0, Math.min(H1, Math.round(h))); if (h !== hour) { hour = h; dirty = true; } run(); }

function story(h) {
  if (h === EVENTS.outage) return "Scenario: a consumer outage. Only a quarter of this hour's volume was processed.";
  if (h === EVENTS.catchup) return "Scenario: the backlog from the outage is processed in this hour, so it looks like a spike although the source did not change.";
  if (h >= EVENTS.dropFrom && h < EVENTS.dropTo) return "Scenario: one producer of several has been quiet since Thu 02:00 (hour " + (h - EVENTS.dropFrom + 1) + " of " + (EVENTS.dropTo - EVENTS.dropFrom) + "), about 8 % less volume.";
  if (h >= EVENTS.dropTo && h < EVENTS.dropTo + 6) return "Scenario: the quiet producer is back since Fri 08:00. Nothing is wrong any more; watch how long EWMA takes to forget.";
  return "Scenario: nothing is wrong in this hour; any alarm here is a false alarm.";
}

function explain() {
  const H = dataFor(weeks), r = H[hour], s = r[mode], f = flags(s);
  const prev = hour > 0 ? H[hour - 1][mode] : null;
  const prevE = prev ? prev.ewma : 0;
  const prevUp = prev && !prev.signal ? prev.up : 0, prevDown = prev && !prev.signal ? prev.down : 0;
  const day = DAYS[Math.floor(r.h / 24)] + "s";
  const sorted = [...r.refs].sort((a, b) => a - b).map(int).join(", ");
  const wobble = mode === "pooled"
    ? "one noise figure for the whole week, " + (r.rel * 100).toFixed(1) + " % of normal"
    : "1.4826 × the median distance of these " + weeks + " values from their median";
  const tag = (on, yes, no) => '<b class="' + (on ? "es-alarm" : "es-ok") + '">' + (on ? yes : no) + "</b>";
  const lines = [
    '<p class="es-story">' + story(r.h) + "</p>",
    "<p><b>" + clock(r.h) + "</b>, the rightmost pin in each row. <b>" + int(r.x) + "</b> messages arrived.</p>",
    "<p><b>Normal</b> for this hour is <b>" + int(r.med) + "</b>: the median of the same hour on the previous " + weeks + " " + day + " (" + sorted + "). " +
      "The usual wobble is <b>±" + int(s.scale) + "</b> messages (" + wobble + ").</p>",
    "<p><b>Robust z</b> = (" + int(r.x) + " − " + int(r.med) + ") ÷ " + int(s.scale) + " = <b>" + num(s.z, 2) + "</b> wobbles. Limit ±" + CFG.zFlag + ": " +
      tag(f.z, "alarm", "no alarm") + (Math.abs(s.z) > ROWS[0].clamp ? " (off the pin scale, so the back-row pin wears a cap)" : "") + ".</p>",
    "<p><b>EWMA</b> = 0.2 × " + num(s.z, 2) + " + 0.8 × " + num(prevE, 2) + " (last hour) = <b>" + num(s.ewma, 2) + "</b>. Limit ±" + ewmaLimit().toFixed(1) + ": " + tag(f.ewma, "alarm", "no alarm") + ".</p>",
    "<p><b>CUSUM</b> high tally: " + num(prevUp, 1) + " + (" + num(s.z, 2) + " − 0.5) → <b>" + num(s.up, 1) + "</b>; low tally: " + num(prevDown, 1) + " + (" + num(-s.z, 2) + " − 0.5) → <b>" + num(s.down, 1) + "</b> (a tally never goes below 0). Limit " + CFG.h + ": " +
      tag(s.signal, "signal, both tallies restart at 0 next hour", "no signal") + ".</p>",
  ];
  if (weeks < 7) lines.push('<p class="es-story">Cold start: only ' + weeks + " weeks of history, so normal is the median of " + weeks + " values. " +
    (weeks === 2 ? "With two values the median is their average and the per-hour MAD is half their gap, which can be almost zero." : "The median is noisier than with 7 weeks, so z wobbles more even with a perfect scale.") + "</p>");
  now.innerHTML = lines.join("");
}

function firstAfter(key, from) {
  const H = dataFor(weeks);
  for (let h = from; h <= H1; h++) if (flags(H[h][mode])[key]) return h;
  return null;
}

function apply() {
  const H = dataFor(weeks), r = H[hour], s = r[mode], f = flags(s);
  rods.innerHTML = rackRods(ROWS[0], r.med, s.scale);
  blocks.innerHTML = blocksSvg(ROWS[0], r.refs, r.x, f.z);
  lens.innerHTML = lensSvg(s.signal);
  rocker.innerHTML = rockerSvg(mode === "pooled");
  const c = s.up >= s.down ? s.up : -s.down;
  const label = (mode === "pooled" ? "pooled" : weeks + "-pt MAD") + (weeks < 7 ? " · " + weeks + " wk" : "");
  const text = label + " · " + clock(r.h) + " · z " + fmt(s.z) + " · ewma " + fmt(s.ewma) + " · cusum " + fmt(c);
  readout.textContent = stage.clientWidth < 560 ? clock(r.h) + " · z " + fmt(s.z) + " · e " + fmt(s.ewma) + " · c " + fmt(c) : text;
  stage.setAttribute("aria-valuenow", String(r.h));
  stage.setAttribute("aria-valuetext", text + (f.z || f.ewma || f.cusum ? " · alarm: " + ["z", "ewma", "cusum"].filter((key) => f[key]).join(", ") : ""));
  panel.querySelectorAll("[data-mode]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.mode === mode)));
  panel.querySelectorAll("[data-weeks]").forEach((b) => b.setAttribute("aria-pressed", String(+b.dataset.weeks === weeks)));
  panel.querySelector("[data-play]").textContent = touring ? "Pause" : "Play";
  const ew = firstAfter("ewma", EVENTS.dropFrom), cu = firstAfter("cusum", EVENTS.dropFrom);
  const jump = (name, h, text) => { const b = panel.querySelector('[data-jump="' + name + '"]'); b.disabled = h === null; b.dataset.hour = h ?? ""; b.textContent = text + (h === null ? " (none)" : " · " + clock(h)); };
  jump("ewma", ew !== null && ew < EVENTS.dropTo ? ew : null, "First EWMA alarm in the dip");
  jump("cusum", cu !== null && cu < EVENTS.dropTo ? cu : null, "First CUSUM signal in the dip");
  explain();
}

function tick(t) {
  const dt = last ? Math.min((t - last) / 1000, 1 / 30) : 1 / 60;
  last = t;
  const H = dataFor(weeks);
  if (touring) {
    const f = flags(H[hour][mode]);
    const dwell = hour === H1 ? 4 : f.z || f.ewma || f.cusum || hour === EVENTS.outage || hour === EVENTS.catchup || hour === EVENTS.dropFrom ? 2.6 : 0.8;
    clockT += dt;
    if (clockT > dwell) { clockT = 0; hour = hour === H1 ? H0 : hour + 1; dirty = true; }
  }
  if (dirty) { dirty = false; apply(); }
  const want = valuesAt(H, hour, mode);
  const ease = still.matches ? 1 : 1 - Math.exp(-dt / 0.12);
  let moving = false;
  want.forEach((list, ri) => list.forEach((p, i) => {
    const cur = shown[ri][i];
    const next = cur.v + (p.v - cur.v) * ease;
    const done = Math.abs(p.v - next) < 0.002;
    const v = done ? p.v : next;
    if (!done) moving = true;
    const row = ROWS[ri];
    if (Math.abs(heightOf(row, v) - heightOf(row, cur.v)) > 0.02 || cur.lit !== p.lit || cur.capped !== p.capped || (done && cur.v !== p.v)) {
      rows[ri][i].innerHTML = pinSvg(pinX(i), row.y, row.top, heightOf(row, v), p.lit, p.capped);
    }
    shown[ri][i] = { v, lit: p.lit, capped: p.capped };
  }));
  const target = H[hour][mode].ewma;
  needleAt0 = still.matches ? target : needleAt0 + (target - needleAt0) * ease;
  needle.setAttribute("d", needleAt(needleAt0));
  frame = visible && (touring || moving || Math.abs(target - needleAt0) > 0.002) ? requestAnimationFrame(tick) : 0;
}
function run() { if (!frame && visible) { last = 0; frame = requestAnimationFrame(tick); } }
const stop = () => { touring = false; dirty = true; };
new IntersectionObserver((e) => { visible = e[e.length - 1].isIntersecting; if (visible) run(); }, { rootMargin: "120px 0px" }).observe(stage);
if (still.matches) hour = EVENTS.outage;
stage.addEventListener("pointermove", (event) => {
  if (event.pointerType === "touch") return;
  const box = svg.getBoundingClientRect();
  const share = Math.min(1, Math.max(0, ((event.clientX - box.left) / box.width - 0.1) / 0.8));
  stop();
  setHour(H0 + share * (H1 - H0));
});
const setMode = (m) => { mode = m; dirty = true; run(); };
const setWeeks = (w) => { weeks = w; dirty = true; run(); };
stage.addEventListener("click", () => setMode(mode === "pooled" ? "local" : "pooled"));
stage.addEventListener("keydown", (event) => {
  if (event.key === "s" || event.key === "S" || event.key === "Enter") { event.preventDefault(); setMode(mode === "pooled" ? "local" : "pooled"); return; }
  if (event.key === "w" || event.key === "W") { event.preventDefault(); setWeeks(weeks === 7 ? 4 : weeks === 4 ? 2 : 7); return; }
  if (event.key === " ") { event.preventDefault(); touring = !touring; clockT = 0; dirty = true; run(); return; }
  let to = null;
  if (event.key === "ArrowRight") to = hour + 1;
  if (event.key === "ArrowLeft") to = hour - 1;
  if (event.key === "Home") to = H0;
  if (event.key === "End") to = H1;
  if (to === null) return;
  event.preventDefault();
  stop();
  setHour(to);
});
panel.addEventListener("click", (event) => {
  const b = event.target.closest("button");
  if (!b) return;
  if (b.dataset.mode) setMode(b.dataset.mode);
  else if (b.dataset.weeks) setWeeks(+b.dataset.weeks);
  else if (b.hasAttribute("data-play")) { touring = !touring; clockT = 0; dirty = true; run(); }
  else if (b.dataset.step) { stop(); setHour(hour + +b.dataset.step); }
  else if (b.dataset.hour !== undefined && b.dataset.hour !== "") { stop(); setHour(+b.dataset.hour); }
});
apply();
run();
`;

const card = k.plateHtml({
  fig: "Fig 1",
  title: "Robust z · EWMA · CUSUM",
  hint: "Hover: hours · click: scale",
  readout: `pooled · ${clock(HOURS[H0].h)} · z ${HOURS[H0].pooled.z.toFixed(1)}`,
  keys: [
    { mark: "raised", label: "Back row: robust z of each hour. Rack: the reference weeks for the current hour, sorted, and the current hour standing apart" },
    { mark: "raised", label: `Middle row: EWMA of z (λ ${CFG.lambda}), read on the dial; limit ±${ewmaLimit().toFixed(1)}` },
    { mark: "raised", label: `Front row: CUSUM of z, the larger tally (up: too high, down: too low); k ${CFG.k}, limit ${CFG.h}` },
    { mark: "dim", label: "Rods: the dim middle rod is normal (0), the two outer rods are the alarm limits" },
    { mark: "lit", label: "White: an alarm in that hour (pin, current rack block, lamp). A wide cap: the value is off the pin scale" },
  ],
  caption: "Synthetic hourly message volume: nothing here is real data. The panel below explains the current hour step by step.",
  body: `<div class="iso-stage" tabindex="0" role="slider" aria-label="Detector bench: arrow keys step one hour, S switches the scale, W the history length, Space pauses" aria-valuemin="${H0}" aria-valuemax="${H1}" aria-valuenow="${H0}">${svg}</div>`,
});

const pct = (v) => (v * 100).toFixed(1) + " %";
const tableRows = TABLE.map((t) => `<tr><td>${t.weeks} weeks</td><td>${t.pooled.z.toFixed(0)} / ${t.pooled.ewma.toFixed(0)} / ${t.pooled.cusum.toFixed(0)}</td><td>${t.local.z.toFixed(0)} / ${t.local.ewma.toFixed(0)} / ${t.local.cusum.toFixed(0)}</td><td>${pct(t.share)}</td></tr>`).join("");

const PANEL = `
<section class="es-panel" aria-label="Controls and explanation">
  <div class="es-controls">
    <span class="es-group"><span class="es-label">Scale</span><button data-mode="pooled" aria-pressed="true">Pooled</button><button data-mode="local" aria-pressed="false">Per-hour MAD</button></span>
    <span class="es-group"><span class="es-label">History</span>${WEEK_SETS.map((w) => `<button data-weeks="${w}" aria-pressed="${w === 7}">${w} weeks</button>`).join("")}</span>
    <span class="es-group"><span class="es-label">Time</span><button data-step="-1" aria-label="One hour back">◀</button><button data-play>Pause</button><button data-step="1" aria-label="One hour forward">▶</button></span>
  </div>
  <div class="es-controls">
    <span class="es-group es-wrap"><span class="es-label">Jump to</span>
      <button data-hour="${EVENTS.outage}">Outage · ${clock(EVENTS.outage)}</button>
      <button data-hour="${EVENTS.catchup}">Catch-up · ${clock(EVENTS.catchup)}</button>
      <button data-hour="${EVENTS.dropFrom}">Producer goes quiet · ${clock(EVENTS.dropFrom)}</button>
      <button data-jump="ewma">First EWMA alarm in the dip</button>
      <button data-jump="cusum">First CUSUM signal in the dip</button>
      <button data-hour="${EVENTS.dropTo}">Producer back · ${clock(EVENTS.dropTo)}</button>
    </span>
  </div>
  <div class="es-now" data-now aria-live="polite"></div>

  <h2>How to read the bench</h2>
  <ul>
    <li><b>Time runs left to right.</b> Each row has 48 pins, one per hour; the rightmost pin is the current hour (the small triangle on the ruler), and the long ruler ticks are every 6 hours back from it. As time moves on, the whole pattern shifts left by one pin.</li>
    <li><b>Pin height is the value.</b> At the dim middle rod the value is normal (0); above it, more volume than normal; below it, less.</li>
    <li><b>The two outer rods are the alarm limits.</b> A pin that pokes past one turns <b>white</b>: that detector alarmed in that hour. A pin keeps the colour it had in its own hour, so white pins on the left are past alarms.</li>
    <li><b>A pin with a wide cap is off the scale.</b> The outage is −12 wobbles, far below the bottom of the pin scale; its pin is a short stub with a cap. The panel gives the true value.</li>
    <li><b>The right end of each row shows the current hour only.</b> Back row: a rack with the reference weeks for this hour, sorted low to high, and this hour's block standing apart; its rods are normal and normal ± 3.5 wobbles, and the block turns white on a z alarm. Middle row: a dial whose needle is the EWMA; the arcs mark the alarm zones. Front row: a lamp that lights when CUSUM signals.</li>
    <li><b>The rocker at the front left</b> shows the scale: tipped back for pooled, forward for per-hour MAD.</li>
  </ul>

  <h2>The three detectors, simply</h2>
  <ul>
    <li><b>Robust z: how strange is this one hour?</b> Normal is the median of the same hour on previous weeks; the usual wobble is a robust spread. z counts how many wobbles this hour is from normal. It has no memory: it catches sudden large changes at once and misses small ones that last.</li>
    <li><b>EWMA: what is the recent trend?</b> A running average of z that slowly forgets (each hour keeps 80 % of the old value). One wild hour moves it a little; a small dip that repeats drags it down. Its limit (±1.0) is tighter than z's because averaging cancels noise.</li>
    <li><b>CUSUM: is evidence piling up?</b> Two tallies, too high and too low. Each hour adds how far z is beyond an allowance of 0.5; normal hours drain the tally, and it never goes below 0. Past 5 it signals and restarts. The allowance is half the smallest lasting shift you want to catch, so choosing it is choosing which deviation is meaningful.</li>
  </ul>

  <h2>Pooled scale or per-hour MAD?</h2>
  <p>The wobble can be measured from the few past values of this exact hour (per-hour MAD) or as one figure for the whole week, as a share of normal, pooled over all 168 hours (pooled). With 7 values the per-hour MAD is often far too small by chance, so ordinary hours look extreme. Switch the scale and watch alarms appear where nothing happens.</p>
  <p><b>Caveat:</b> this simulation adds exactly 6 % noise to every hour, which is what the pooled scale assumes, so the comparison favours pooled by construction. Real night hours with low volume are relatively noisier. The likely answer for real data is in between: pool per regime (day and night, weekday and weekend) or use the per-hour MAD with the pooled figure as a floor. Measure it before choosing.</p>

  <h2>Too little history (cold start)</h2>
  <p>With fewer reference weeks, normal is the median of fewer values and is itself noisier, so z wobbles more even with a perfect scale; with per-hour MADs it gets much worse. Try 4 and 2 weeks. Options in practice: backfill the history from raw if it is still retained, use fewer weeks with a pooled scale, compare with the same hour on previous days of the same kind until enough weeks exist, or compute the values without alerting during warm-up. EWMA and CUSUM themselves warm up in hours; they inherit the baseline's weakness.</p>

  <h2>Hours to the first false alarm</h2>
  <p>If nothing is ever wrong, how many hours on average until each detector fires anyway (robust z / EWMA / CUSUM)? Bigger is better. Simulated with clean, independent normal noise (2000 runs each); real data, with correlated hours and holidays, will do worse. Per series: with many monitored series, false alarms add up.</p>
  <table class="es-table">
    <thead><tr><th>History</th><th>Pooled scale</th><th>Per-hour MAD</th><th>Hours with |z| &gt; 3.5, per-hour MAD</th></tr></thead>
    <tbody>${tableRows}</tbody>
  </table>
</section>`;

const PANEL_CSS = `<style>
.es-limit{stroke-width:1.15}
.es-panel{margin-top:4px;color:var(--anatomy-text);font-size:14px;line-height:1.6}
.es-panel h2{font-size:15px;margin:28px 0 8px;font-weight:600}
.es-panel ul{padding-left:18px;margin:0}
.es-panel li{margin:6px 0}
.es-panel p{margin:8px 0}
.es-controls{display:flex;flex-wrap:wrap;gap:10px 18px;margin:10px 0}
.es-group{display:inline-flex;flex-wrap:wrap;align-items:center;gap:6px}
.es-label{font-family:var(--anatomy-mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--anatomy-muted);margin-right:2px}
.es-panel button{font:12px var(--anatomy-mono);color:var(--anatomy-text);background:transparent;border:1px solid var(--anatomy-mid);border-radius:4px;padding:4px 9px;cursor:pointer}
.es-panel button[aria-pressed="true"]{background:var(--anatomy-text);color:var(--anatomy-card)}
.es-panel button:disabled{opacity:.45;cursor:default}
.es-now{border:1px solid var(--anatomy-mid);padding:10px 14px;margin:12px 0;font-variant-numeric:tabular-nums;min-height:15em}
.es-story{color:var(--anatomy-muted)}
.es-alarm{color:var(--anatomy-lit)}
.es-ok{color:var(--anatomy-muted);font-weight:400}
.es-table{border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums}
.es-table th,.es-table td{border-bottom:1px solid var(--anatomy-mid);padding:5px 12px 5px 0;text-align:left;vertical-align:top}
.es-table th{color:var(--anatomy-muted);font-weight:400}
</style>`;

const name = theme === "light" ? "detectors-light.html" : "detectors.html";
writeFileSync(join(HERE, name), k.pageHtml({ title: "Robust z, EWMA and CUSUM on hourly volume", theme, body: PANEL_CSS + card + PANEL, script: kitScript() + live, width: 760 }));
console.log("wrote", name);
