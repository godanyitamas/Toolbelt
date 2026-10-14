# AGENTS.md — Engineering Casebook Rules

This repository is my personal **engineering casebook**: real technical problems from my work, turned into better mental models and engineering judgment. It is not an encyclopedia, a copy of official documentation, or a PKM system.

This file holds the rules an AI agent needs on every task. My career direction, technical context and longer-term strategy live in `README.md` under **Direction and strategy**; read that only when a task needs it (e.g. career analysis, architecture review).

---

## Core Philosophy

1. **Problems before theory.** Topics enter the repository because they became relevant to an actual problem, not because they exist.
2. **Think before asking AI.** I write down what I think is happening, what I am unsure about, my assumptions and my evidence first. A wrong initial explanation is useful because it makes learning visible.
3. **Evidence over plausibility.** Keep observations, hypotheses, verified facts, interpretations, conclusions and open questions distinct. Prefer documentation, measurements, experiments, source code, Spark UI / plans, logs, metrics and reproducible behavior. A plausible AI explanation is not a fact.
4. **AI challenges reasoning, it does not replace it.** Identify hidden assumptions, expose gaps, suggest competing explanations and experiments, find authoritative sources, review conclusions, connect to prior cases. Do not produce polished notes before I understand the subject, silently rewrite my conclusions, treat uncertainty as certainty, or optimize for volume.

Interaction pattern: **my model → AI challenge → investigation → AI review → my final model**

---

## Repository Structure

**Folders = what kind of artifact is this? Links / tags = what is it about?** Never organize top-level folders by technology (`spark/`, `kafka/`, …). Avoid deep folder trees.

```
.
├── README.md        # principles + direction and strategy (for me)
├── AGENTS.md        # rules for agents (this file)
├── inbox.md         # fast capture only; do not polish
├── cases/           # core: real problems and investigations
├── concepts/        # reusable mental models that emerged from several cases
├── templates/       # case.md, retrospective-case.md
└── .ai/workflows/   # stable, repository-specific agent workflows
```

- **Case:** create one when I did not understand a behavior, something surprised me, solutions have meaningful trade-offs, I investigated performance / reliability / cost / scalability / correctness, or the experience changed how I would approach a similar problem. Routine tickets do not need cases.
- **Concept:** only when several real cases show a reusable mental model would help. Never because a concept is important in general.
- **Templates** should reduce friction. If one makes me reluctant to write a case, simplify it.
- **`.ai/`** holds only stable workflows (what context to load, what reasoning to follow, what evidence standard, what checks before committing). Not a prompt collection.
- **New structure** (`decisions/`, `labs/`, `playbooks/`, `career/`) is added only when repeated use creates a real need. Their intended purposes are described in `README.md`.

---

## Working on Cases

Follow `.ai/workflows/new-case.md` for new cases. Case structure is defined by `templates/case.md` (or `templates/retrospective-case.md` for reconstructions from memory). The two most important sections are **What I currently think** and **What changed in my understanding?**

### Context loading rule

Do not read the entire repository by default.

1. This file is already loaded.
2. Read the relevant template.
3. Search `cases/` (titles, `topics:` front-matter) for related topics, technologies, or problems.
4. Read only the related cases needed to establish context — prefer the relevant sections over whole files.
5. Load other material only when the task depends on it.

### New case interaction rule

Do not immediately write polished Markdown. First help me reconstruct and test the reasoning. Ask targeted questions that distinguish competing explanations or establish evidence. Preserve uninvestigated points as open questions or hypotheses rather than filling the gap.

### Reviewing a case

Challenge before rewriting. Look for: unsupported assumptions; missing evidence; alternative explanations; correlation mistaken for causation; missing failure modes; incorrect Spark / JVM / distributed-systems assumptions; weak experimental design; conclusions stronger than the evidence; contradictions with existing cases.

A case is judged by one question: **did it improve how I will reason about a future problem?**

---

## Agent Safety Rule

Agents may **propose** changes to canonical knowledge; they do not silently rewrite established conclusions.

**agent proposes → I inspect → I accept, modify, or reject**

Do not commit, push, or open pull requests unless I ask.

---

## Metadata

Keep front-matter minimal:

```yaml
---
type: case
status: active
created: 2026-10-02
topics:
  - spark
  - memory
---
```

No scoring, confidence systems, difficulty levels, review intervals, or other maintenance-heavy metadata unless they prove genuinely useful.

---

## Confidentiality

This repository contains **generalized personal learning**, not employer or customer documentation. Never include: customer names; proprietary source code; credentials; internal URLs; production data; internal identifiers; unreleased architecture; confidential incident details; commercially sensitive costs; anything prohibited by employer/client policy.

Rewrite work experiences as generic engineering problems.

> Bad: Customer X's ingestion system uses internal service Y and costs €Z.
> Better: A high-volume binary-file ingestion system exhibited unexpectedly high memory usage.

---

## Tool Independence

Plain Markdown + Git, edited from Obsidian (desktop and Android via GitSync). Keep it portable; no single tool is a hard dependency. `AGENTS.md` is the canonical agent instruction file; vendor-specific files such as `CLAUDE.md` stay thin and point here.

---

## Default Instruction to Any AI Model

1. Read `AGENTS.md` first.
2. Load only the template and existing cases relevant to the current task.
3. Preserve the distinction between observation, hypothesis, and conclusion.
4. Challenge my reasoning before polishing it.
5. Ask what evidence supports a claim.
6. Prefer experiments and authoritative sources over confident speculation.
7. Connect new work to relevant existing cases or concepts.
8. Do not create unnecessary structure.
9. Do not turn the repository into generic documentation.
10. Do not invent project-specific details.
11. Protect confidential information.
12. Optimize for my long-term engineering judgment, not short-term note production.

If unsure whether to add something, prefer **less structure and more deliberate reasoning**.
