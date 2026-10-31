// Hourly volume monitor: robust z against the same hour on previous weeks, then EWMA and
// two-sided CUSUM over that z. Synthetic data, seeded; nothing here comes from a real system.
export const CFG = {
  weeks: 7,            // reference: the same hour of week on the previous 7 weeks
  zFlag: 3.5,          // |robust z| beyond this is flagged (Iglewicz–Hoaglin modified z rule)
  lambda: 0.2,         // EWMA weight of the newest hour
  L: 3,                // EWMA limit in asymptotic EWMA standard deviations
  k: 0.5,              // CUSUM slack: half the shift (in z units) it is tuned to catch
  h: 5,                // CUSUM decision threshold
  noise: 0.06,         // hour-to-hour volume noise, share of the expected volume
  days: 5,             // shown period, Mon 00:00 to Fri 23:00 after the history
  window: 48,          // hours visible on each row
};
export const ewmaLimit = (c = CFG) => c.L * Math.sqrt(c.lambda / (2 - c.lambda));

// deterministic normal noise
export function rng(seed) {
  let s = seed >>> 0;
  const u = () => { s = (s + 0x6d2b79f5) >>> 0; let t = s; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  return () => Math.sqrt(-2 * Math.log(u() + 1e-12)) * Math.cos(2 * Math.PI * u());
}

// expected messages per hour: two daytime shifts on weekdays, lower at night and at weekends
export function expected(hourOfWeek) {
  const day = Math.floor(hourOfWeek / 24), hod = hourOfWeek % 24;
  const shift = hod >= 6 && hod < 22 ? 1 : 0.35;
  const ramp = hod === 6 || hod === 21 ? 0.7 : 1;
  return Math.round((day < 5 ? 1 : 0.55) * 12000 * shift * ramp);
}

export const median = (a) => { const s = [...a].sort((x, y) => x - y); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

// events in the shown period, by hour index from Monday 00:00
export const EVENTS = {
  outage: 30,          // Tue 06:00: a consumer outage, a quarter of the volume lands
  catchup: 31,         // Tue 07:00: the backlog is processed in this hour
  dropFrom: 74,        // Thu 02:00 on: one producer of several goes quiet, -8 % volume
  dropTo: 104,
};
const HISTORY_SPIKE = { week: 3, from: 0, to: 120, factor: 1.6 };   // a replay week in the history

// Pooled scale: the deviations of every hour of the last 7 weeks from its own same-hour median,
// as a share of that median, pooled over all 168 hours of the week. One stable relative noise
// figure instead of 168 seven-point MADs. POOL_FIX corrects the in-sample bias (one of seven
// deviations from a median of seven is always zero); found by simulation, see poolFix().
export const POOL_FIX = 1.217;

export function build(c = CFG, seed = 11) {
  const n = rng(seed);
  const start = c.weeks * 168;
  const total = start + c.days * 24;
  const x = [];
  for (let t = 0; t < total; t++) {
    let v = expected(t % 168) * (1 + c.noise * n());
    const w = Math.floor(t / 168), h = t - start;
    if (w === HISTORY_SPIKE.week && t % 168 >= HISTORY_SPIKE.from && t % 168 < HISTORY_SPIKE.to && t % 24 >= 9 && t % 24 < 13) v *= HISTORY_SPIKE.factor;
    if (h === EVENTS.outage) v *= 0.25;
    if (h === EVENTS.catchup) v = v + expected((t - 1) % 168) * 0.75;
    if (h >= EVENTS.dropFrom && h < EVENTS.dropTo) v *= 0.92;
    x.push(Math.max(0, Math.round(v)));
  }
  const refsAt = (t) => Array.from({ length: c.weeks }, (_, i) => x[t - 168 * (i + 1)]);
  // pooled relative scale over the reference weeks, recomputed at each Monday 00:00
  const pooledAt = (t) => {
    const devs = [];
    for (let hw = 0; hw < 168; hw++) {
      const at = t - (t % 168) + hw;                 // same week's hour-of-week slot
      const refs = refsAt(at);
      const med = median(refs);
      for (const r of refs) devs.push(Math.abs(r - med) / med);
    }
    return 1.4826 * median(devs) * POOL_FIX;
  };
  const rel = pooledAt(start);
  const states = { local: { e: 0, u: 0, d: 0 }, pooled: { e: 0, u: 0, d: 0 } };
  const hours = [];
  for (let t = start; t < total; t++) {
    const refs = refsAt(t);
    const med = median(refs);
    const mad = median(refs.map((r) => Math.abs(r - med)));
    const row = { h: t - start, x: x[t], refs, med, mad, rel };
    for (const [mode, scale] of [["local", 1.4826 * mad], ["pooled", rel * med]]) {
      const s = states[mode];
      const z = scale > 0 ? (x[t] - med) / scale : 0;
      s.e = c.lambda * z + (1 - c.lambda) * s.e;
      const up = Math.max(0, s.u + z - c.k), down = Math.max(0, s.d - z - c.k);
      const signal = up > c.h || down > c.h;
      row[mode] = { scale, z, ewma: s.e, up, down, signal };
      // restart both sums after a signal, as the tabular CUSUM is usually run
      s.u = signal ? 0 : up;
      s.d = signal ? 0 : down;
    }
    hours.push(row);
  }
  return hours;
}

export const flags = (r, c = CFG) => ({ z: Math.abs(r.z) > c.zFlag, ewma: Math.abs(r.ewma) > ewmaLimit(c), cusum: r.signal });

export function poolFix(trials = 4000, seed = 3) {
  const n = rng(seed);
  const devs = [];
  for (let i = 0; i < trials; i++) { const refs = Array.from({ length: 7 }, () => n()); const m = median(refs); for (const r of refs) devs.push(Math.abs(r - m)); }
  return 1 / (1.4826 * median(devs));
}

// Average run length (hours to the first alarm) under idealised in-control data: iid normal
// volumes, same-hour median of 7 reference weeks, either scale. shift is in true noise units.
export function arl(mode = "pooled", c = CFG, runs = 2000, seed = 5, shift = 0) {
  const n = rng(seed);
  const out = { z: 0, ewma: 0, cusum: 0 };
  const zOf = () => {
    const refs = Array.from({ length: c.weeks }, () => n());
    const med = median(refs);
    const scale = mode === "pooled" ? 1 : 1.4826 * median(refs.map((r) => Math.abs(r - med)));
    return (n() + shift - med) / scale;
  };
  for (const key of Object.keys(out)) {
    let sum = 0;
    for (let r = 0; r < runs; r++) {
      let e = 0, u = 0, d = 0, t = 0;
      for (;;) {
        t++;
        const z = zOf();
        e = c.lambda * z + (1 - c.lambda) * e; u = Math.max(0, u + z - c.k); d = Math.max(0, d - z - c.k);
        const hit = key === "z" ? Math.abs(z) > c.zFlag : key === "ewma" ? Math.abs(e) > ewmaLimit(c) : u > c.h || d > c.h;
        if (hit || t > 1e6) break;
      }
      sum += t;
    }
    out[key] = sum / runs;
  }
  return out;
}

// False-flag rate of the robust z itself when the baseline is only 7 noisy points.
export function robustFalseRate(c = CFG, trials = 200000, seed = 9) {
  const n = rng(seed);
  let hit = 0;
  for (let i = 0; i < trials; i++) {
    const refs = Array.from({ length: c.weeks }, () => n());
    const med = median(refs), mad = median(refs.map((r) => Math.abs(r - med)));
    if (Math.abs((n() - med) / (1.4826 * mad)) > c.zFlag) hit++;
  }
  return hit / trials;
}
