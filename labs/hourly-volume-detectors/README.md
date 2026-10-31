---
type: lab
status: draft
created: 2026-10-31
topics:
  - observability
---

# Hourly volume detectors: robust z, EWMA, CUSUM (synthetic)

Supports `cases/unified-data-observability-and-lineage.md`, open question: which baseline defines a trend, and which volume deviation is meaningful? Built by an AI agent on request; the scenario and parameters are the agent's choices, not decisions.

![Detector bench: robust z, EWMA and CUSUM rows of hourly pins](detectors.png)

Open `detectors.html` in a browser for the live figure. Below the figure, a panel works through the current hour step by step (volume, normal, wobble, z, EWMA, CUSUM tallies), with buttons for the scale (pooled or per-hour MAD), the history length (7, 4 or 2 reference weeks), stepping hours and jumping to the events. The panel also explains how to read the bench.

## Setup (all simulated, seeded)

- **Volume:** 12,000 messages per hour in weekday shifts (06–22 h), 35 % at night, 55 % at weekends, 6 % hourly noise.
- **Baseline:** median of the same hour of the week on the previous 7 weeks (4 or 2 for a cold start). One history week holds a replay (+60 %, 09–13 h, Mon–Fri).
- **Scale (two variants):** *pooled*, one relative noise figure from all reference-week deviations over the 168 hours (1.4826 × median, times a small-sample bias correction found by simulation); or *local*, each hour's own MAD × 1.4826.
- **Detectors:** robust z flagged beyond ±3.5; EWMA of z with λ 0.2 and limit ±3·√(λ/(2−λ)) = ±1.0; two-sided tabular CUSUM of z with k 0.5 and h 5, restarted after each signal.
- **Events:** Wed 06:00 outage (a quarter of the volume), Wed 07:00 backlog processed; Thu 02:00 to Fri 07:00 one producer quiet (−8 %, about −1.3 noise units).

## Results (`node check.mjs`)

With 7 reference weeks:

| | pooled scale | per-hour 7-point MAD |
|---|---|---|
| Outage and catch-up flagged by robust z | both hours | both hours |
| −8 % shift: robust z | never | once (Thu 10:00) |
| −8 % shift: first EWMA alarm | Thu 08:00 (6 h) | Thu 10:00 (8 h) |
| −8 % shift: CUSUM signals | Thu 10:00, 17:00, Fri 02:00 | Thu 10:00, 16:00, Fri 02:00, 07:00 |
| Alarms in 65 quiet hours (z / EWMA / CUSUM) | 0 / 0 / 0 | 4 / 5 / 4 |

Hours to the first false alarm on clean, independent normal noise (z / EWMA / CUSUM), and the share of hours with |z| > 3.5 when the scale is the per-hour MAD:

| History | pooled scale | per-hour MAD | \|z\| > 3.5, per-hour MAD |
|---|---|---|---|
| 7 weeks | 686 / 263 / 202 | 15 / 21 / 18 | 6.8 % |
| 4 weeks | 475 / 198 / 168 | 7 / 10 / 9 | 13.8 % |
| 2 weeks | 235 / 125 / 97 | 5 / 6 / 6 | 20.4 % |

In the scenario with 2 weeks, the pooled scale gives 3 EWMA alarms and 1 CUSUM signal in the 65 quiet hours; with per-hour MADs, 16 / 29 / 14. With little history, an early alarm in the dip cannot be told apart from noise.

## What this does not show

Nothing here is measured on the real tables. The noise is exactly 6 % in every hour, which is what the pooled scale assumes, so the comparison favours pooled by construction; low-volume hours in real data are relatively noisier. Real hourly volume is also not independent: consecutive hours are correlated (an outage followed by a catch-up), counts are discrete, sparse sources can have a MAD of 0, and holidays break a same-hour-of-week baseline. Each of these changes the false-alarm numbers, and false alarms multiply with the number of monitored series.

## Rebuild

- `node check.mjs` reproduces the numbers above (Node 20+, no dependencies).
- `node build.mjs` rebuilds `detectors.html` (`--light` for the light theme). It needs the figure kit from the anatomy skill (github.com/wheresryan22/anatomy, MIT), found at `~/.claude/skills/anatomy` or `$ANATOMY_DIR`. Viewing the HTML needs nothing.
