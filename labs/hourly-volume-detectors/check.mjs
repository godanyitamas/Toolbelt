// Reproduces the numbers quoted in README.md: scenario timings and average hours to the
// first false alarm under idealised in-control data. Run: node check.mjs
import { CFG, EVENTS, build, flags, arl, ewmaLimit, robustFalseRate } from "./model.mjs";

const clock = (h) => ["Mon", "Tue", "Wed", "Thu", "Fri"][Math.floor(h / 24)] + " " + String(h % 24).padStart(2, "0") + ":00";
// hours with no event and at least 12 hours after the last one
const quiet = (h) => !(h >= EVENTS.outage && h < EVENTS.catchup + 12) && !(h >= EVENTS.dropFrom && h < EVENTS.dropTo + 12);
console.log(`EWMA limit ±${ewmaLimit().toFixed(2)}`);
for (const weeks of [7, 4, 2]) {
  const c = { ...CFG, weeks };
  const hours = build(c);
  console.log(`\n${weeks} reference weeks, pooled relative noise ${hours[0].rel.toFixed(4)}`);
  for (const mode of ["pooled", "local"]) {
    const hit = (key, from, to) => hours.filter((r) => r.h >= from && r.h < to && flags(r[mode], c)[key]).map((r) => clock(r.h));
    console.log(`  ${mode} scale`);
    console.log("    outage/catch-up hours flagged by z:", hit("z", EVENTS.outage, EVENTS.catchup + 1).join(", ") || "none");
    console.log("    drop: z flags", hit("z", EVENTS.dropFrom, EVENTS.dropTo).join(", ") || "none");
    console.log("    drop: first EWMA alarm", hit("ewma", EVENTS.dropFrom, EVENTS.dropTo)[0] ?? "none");
    console.log("    drop: CUSUM signals", hit("cusum", EVENTS.dropFrom, EVENTS.dropTo).join(", ") || "none");
    const count = (key) => hours.filter((r) => quiet(r.h) && flags(r[mode], c)[key]).length;
    console.log(`    alarms in ${hours.filter((r) => quiet(r.h)).length} quiet hours: z ${count("z")}, ewma ${count("ewma")}, cusum ${count("cusum")}`);
  }
  const p = arl("pooled", c), l = arl("local", c);
  console.log(`  hours to first false alarm, iid normal noise (z/ewma/cusum): pooled ${p.z.toFixed(0)}/${p.ewma.toFixed(0)}/${p.cusum.toFixed(0)}, local ${l.z.toFixed(0)}/${l.ewma.toFixed(0)}/${l.cusum.toFixed(0)}`);
  console.log(`  share of hours with |z| > ${CFG.zFlag} using ${weeks}-point MADs: ${(robustFalseRate(c, 100000) * 100).toFixed(1)} %`);
}
