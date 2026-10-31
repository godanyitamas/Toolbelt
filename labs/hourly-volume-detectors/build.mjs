import { writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { CFG, EVENTS, build as buildHours, ewmaLimit, flags } from "./model.mjs";

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
const pinSvg = (x, y, z, h, lit) => k.solidSvg(k.slabOf({ x: x - 1.1, y: y - 1.1, w: 2.2, d: 2.2, r: 0.5 }, z, Math.max(0.6, h), P, 2, 0.3), { tone: lit ? "hi" : "mid", crease: "lo", lit });

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
    k.dotsSvg([z0, ...levels].map((z) => at([-1.4, y, z])), { size: 0.75, tone: "hi" }),
    `<path class="iso-line" data-tone="mid" d="${line(z0)}" style="stroke-width:.9"/>`,
    `<path class="iso-line" data-tone="hi" d="${levels.map(line).join("")}" style="stroke-width:.7"/>`,
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
  return `<path class="iso-line" data-tone="mid" d="${rod(med)}" style="stroke-width:.9"/><path class="iso-line" data-tone="hi" d="${rod(med + half)}${med - half > 0 ? rod(med - half) : ""}" style="stroke-width:.7"/>`;
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
  for (const [from, to] of [[lim, DIAL_SPAN / 2], [-DIAL_SPAN / 2, -lim]]) out.push(L(k.onTop([...arc(DIAL_R - 6.6, from, to), ...arc(DIAL_R - 5.6, from, to).reverse()], DIAL_TOP, P, true), { tone: "hi" }));
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
const valuesAt = (h, mode) => {
  const win = HOURS.slice(h - N + 1, h + 1);
  return ROWS.map((row) => win.map((r) => {
    const s = r[mode];
    const v = row.key === "z" ? s.z : row.key === "ewma" ? s.ewma : s.up >= s.down ? s.up : -s.down;
    return { v, lit: flags(s)[row.key] };
  }));
};

function pinsSvg(rowIndex, values) {
  const row = ROWS[rowIndex];
  return values.map(({ v, lit }, i) => `<g data-pin="${i}">${pinSvg(pinX(i), row.y, row.top, heightOf(row, v), lit)}</g>`).join("");
}

const start = valuesAt(H0, MODE0);
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
    "A bench with three rows of 48 pins, one pin per hour, newest at the right. The back row shows the robust z score of each hour's message volume against the same hour on the previous seven weeks; a rack of seven reference blocks at its end shows those weeks and the current hour. The middle row shows the EWMA of that z score, read on a dial. The front row shows the two-sided CUSUM, with a lamp that lights on a signal. Two thin rods on each row mark the alarm limits; a pin past them lights up.",
  body,
});

const clock = (h) => ["Mon", "Tue", "Wed", "Thu", "Fri"][Math.floor(h / 24)] + " " + String(h % 24).padStart(2, "0") + ":00";

const live = `
${readFileSync(join(HERE, "model.mjs"), "utf8").replace(/^export /gm, "").replace(/^import .*$/gm, "")}
const P = ${JSON.stringify(P)};
const HOURS = build();
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
const stage = document.querySelector(".iso-stage");
const svg = stage.querySelector("svg");
const q = (s) => svg.querySelector(s);
const rows = ROWS.map((_, i) => [...q('[data-row="' + i + '"]').children]);
const rods = q('[data-part="rods"]'), blocks = q('[data-part="blocks"]'), needle = q('[data-part="needle"]'), lens = q('[data-part="lens"]'), rocker = q('[data-part="rocker"]');
const readout = document.querySelector("[data-readout]");
const still = matchMedia("(prefers-reduced-motion: reduce)");
let mode = "pooled", hour = H0, shown = valuesAt(H0, mode).map((r) => r.map((p) => ({ ...p }))), needleAt0 = HOURS[H0].pooled.ewma;
let frame = 0, last = 0, visible = false, touring = true, clockT = 0, idleTimer = 0, dirty = true;
const fmt = (v) => (v < 0 ? "−" : "+") + Math.abs(v).toFixed(1);
function setHour(h) { h = Math.max(H0, Math.min(H1, Math.round(h))); if (h !== hour) { hour = h; dirty = true; } run(); }
function apply() {
  const r = HOURS[hour], s = r[mode], f = flags(s);
  rods.innerHTML = rackRods(ROWS[0], r.med, s.scale);
  blocks.innerHTML = blocksSvg(ROWS[0], r.refs, r.x, f.z);
  lens.innerHTML = lensSvg(s.signal);
  rocker.innerHTML = rockerSvg(mode === "pooled");
  const c = s.up >= s.down ? s.up : -s.down;
  const text = (mode === "pooled" ? "pooled" : "7-pt MAD") + " · " + clock(r.h) + " · z " + fmt(s.z) + " · ewma " + fmt(s.ewma) + " · cusum " + fmt(c);
  readout.textContent = stage.clientWidth < 560 ? clock(r.h) + " · z " + fmt(s.z) + " · e " + fmt(s.ewma) + " · c " + fmt(c) : text;
  stage.setAttribute("aria-valuenow", String(r.h));
  stage.setAttribute("aria-valuetext", text + (f.z || f.ewma || f.cusum ? " · alarm: " + ["z", "ewma", "cusum"].filter((key) => f[key]).join(", ") : ""));
}
function tick(now) {
  const dt = last ? Math.min((now - last) / 1000, 1 / 30) : 1 / 60;
  last = now;
  if (touring) { clockT += dt; if (clockT > (hour === H1 ? 3.5 : 0.9)) { clockT = 0; hour = hour === H1 ? H0 : hour + 1; dirty = true; } }
  if (dirty) { dirty = false; apply(); }
  const want = valuesAt(hour, mode);
  const ease = still.matches ? 1 : 1 - Math.exp(-dt / 0.12);
  let moving = false;
  want.forEach((list, ri) => list.forEach((p, i) => {
    const cur = shown[ri][i];
    const next = cur.v + (p.v - cur.v) * ease;
    const done = Math.abs(p.v - next) < 0.002;
    const v = done ? p.v : next;
    if (!done) moving = true;
    const row = ROWS[ri];
    if (Math.abs(heightOf(row, v) - heightOf(row, cur.v)) > 0.02 || cur.lit !== p.lit || (done && cur.v !== p.v)) {
      rows[ri][i].innerHTML = pinSvg(pinX(i), row.y, row.top, heightOf(row, v), p.lit);
    }
    shown[ri][i] = { v, lit: p.lit };
  }));
  const target = HOURS[hour][mode].ewma;
  needleAt0 = still.matches ? target : needleAt0 + (target - needleAt0) * ease;
  needle.setAttribute("d", needleAt(needleAt0));
  frame = visible && (touring || moving || Math.abs(target - needleAt0) > 0.002) ? requestAnimationFrame(tick) : 0;
}
function run() { if (!frame && visible) { last = 0; frame = requestAnimationFrame(tick); } }
const pause = () => { touring = false; clearTimeout(idleTimer); idleTimer = setTimeout(() => { touring = !still.matches; clockT = 0; run(); }, 5000); };
new IntersectionObserver((e) => { visible = e[e.length - 1].isIntersecting; if (visible) run(); }, { rootMargin: "120px 0px" }).observe(stage);
if (still.matches) { touring = false; hour = 92; dirty = true; }
stage.addEventListener("pointermove", (event) => {
  if (event.pointerType === "touch") return;
  const box = svg.getBoundingClientRect();
  const share = Math.min(1, Math.max(0, ((event.clientX - box.left) / box.width - 0.1) / 0.8));
  pause();
  setHour(H0 + share * (H1 - H0));
});
const toggle = () => { mode = mode === "pooled" ? "local" : "pooled"; dirty = true; run(); };
stage.addEventListener("click", () => { toggle(); });
stage.addEventListener("keydown", (event) => {
  if (event.key === "s" || event.key === "S" || event.key === " " || event.key === "Enter") { event.preventDefault(); toggle(); return; }
  let to = null;
  if (event.key === "ArrowRight") to = hour + 1;
  if (event.key === "ArrowLeft") to = hour - 1;
  if (event.key === "Home") to = H0;
  if (event.key === "End") to = H1;
  if (to === null) return;
  event.preventDefault();
  pause();
  setHour(to);
});
apply();
run();
`;

const card = k.plateHtml({
  fig: "Fig 1",
  title: "Robust z · EWMA · CUSUM",
  hint: "Hover to scrub hours · click to switch scale",
  readout: `pooled · ${clock(HOURS[H0].h)} · z ${HOURS[H0].pooled.z.toFixed(1)}`,
  keys: [
    { mark: "raised", label: "Back row: robust z per hour against the same hour on 7 previous weeks (rack: those weeks sorted, the median rod and the ±3.5 rods, and the current hour at the end)" },
    { mark: "raised", label: `Middle row: EWMA of z, λ ${CFG.lambda}, limit ±${ewmaLimit().toFixed(2)}` },
    { mark: "raised", label: `Front row: two-sided CUSUM of z, k ${CFG.k}, h ${CFG.h}, restarted after each signal` },
    { mark: "lit", label: "A pin past its limit rods, the current hour's block or the lamp: an alarm" },
  ],
  caption: `Synthetic hourly message volume, no real data. The baseline is the same hour of the week on the 7 previous weeks, so night shifts and weekends are compared with themselves; one of those weeks holds a replay at 09–13 h, which the median ignores. Tue 06:00: a consumer outage lets a quarter of the volume through, and Tue 07:00 processes the backlog; robust z flags both hours at once. From Thu 02:00 one producer of several goes quiet (−8 %, about −1.3 noise units): robust z never reaches ±${CFG.zFlag}, EWMA crosses its limit after 6 hours and CUSUM signals after 8, then again every 7–9 hours because its sums restart after each signal. Click to replace the pooled noise figure with each hour's own 7-point MAD: the pins turn jumpy and false alarms appear. On simulated clean data the first false alarm comes after about 200–690 hours with the pooled scale and after 15–21 hours with 7-point MADs.`,
  body: `<div class="iso-stage" tabindex="0" role="slider" aria-label="Detector bench: arrow keys step one hour, S switches the scale" aria-valuemin="${H0}" aria-valuemax="${H1}" aria-valuenow="${H0}">${svg}</div>`,
});

const name = theme === "light" ? "detectors-light.html" : "detectors.html";
writeFileSync(join(HERE, name), k.pageHtml({ title: "Robust z, EWMA and CUSUM on hourly volume", theme, body: card, script: kitScript() + live, width: 760 }));
console.log("wrote", name);
