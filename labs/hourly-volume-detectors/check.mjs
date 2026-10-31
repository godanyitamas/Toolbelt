// Reproduces the numbers quoted in README.md: scenario timings and average hours to the
// first false alarm under idealised in-control data. Run: node check.mjs
import { CFG, EVENTS, build, flags, arl, ewmaLimit, robustFalseRate } from "./model.mjs";

const clock = (h) => ["Mon", "Tue", "Wed", "Thu", "Fri"][Math.floor(h / 24)] + " " + String(h % 24).padStart(2, "0") + ":00";
const hours = build();
console.log(`pooled relative noise ${hours[0].rel.toFixed(4)}, EWMA limit ±${ewmaLimit().toFixed(2)}`);
for (const mode of ["pooled", "local"]) {
  const hit = (key, from, to) => hours.filter((r) => r.h >= from && r.h < to && flags(r[mode])[key]).map((r) => clock(r.h));
  console.log(`\n${mode} scale`);
  console.log("  outage/catch-up hours flagged by z:", hit("z", EVENTS.outage, EVENTS.catchup + 1).join(", ") || "none");
  console.log("  drop: z flags", hit("z", EVENTS.dropFrom, EVENTS.dropTo).join(", ") || "none");
  console.log("  drop: first EWMA alarm", hit("ewma", EVENTS.dropFrom, EVENTS.dropTo)[0] ?? "none");
  console.log("  drop: CUSUM signals", hit("cusum", EVENTS.dropFrom, EVENTS.dropTo).join(", ") || "none");
  const quiet = (key) => hours.filter((r) => (r.h < EVENTS.outage || (r.h > EVENTS.catchup + 24 && r.h < EVENTS.dropFrom)) && flags(r[mode])[key]).map((r) => clock(r.h));
  console.log("  alarms in undisturbed hours: z", quiet("z").join(", ") || "none", "| cusum", quiet("cusum").join(", ") || "none");
}
console.log("\nhours to first false alarm, iid normal noise, 2000 runs each");
for (const mode of ["pooled", "local"]) {
  const r = arl(mode);
  console.log(`  ${mode}: z ${r.z.toFixed(0)}, ewma ${r.ewma.toFixed(0)}, cusum ${r.cusum.toFixed(0)}`);
}
console.log(`share of hours with |z| > ${CFG.zFlag} using 7-point MADs: ${(robustFalseRate() * 100).toFixed(1)} %`);
