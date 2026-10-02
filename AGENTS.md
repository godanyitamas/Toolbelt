# AGENTS.md — Engineering Casebook Intent

## Purpose

This repository is my personal **engineering casebook**.

It is not intended to become an encyclopedia of technologies, a second version of official documentation, or a large personal-knowledge-management project.

Its purpose is to help me become a stronger engineer by capturing and reflecting on **real technical problems I encounter in my work**, then turning those experiences into better mental models, reusable engineering judgment, and eventually stronger architecture and technical-leadership skills.

My broader career direction is:

**Senior Data Engineer → deeper systems / distributed-systems engineer → Staff-level engineer and/or Data Platform / Solutions Architect**

AI is part of this strategy, but not because I want to compete on code generation. The assumption is that code will continue to become cheaper. I therefore want to move upward in abstraction:

**implementation → systems understanding → architecture → technical ownership**

The repository should help me make that transition deliberately.

---

## Current Technical Context

My current work is primarily in large-scale data engineering, including areas such as:

- Apache Spark
- Scala
- PySpark
- Databricks
- Delta Lake
- Structured Streaming
- Kafka / Event Hubs
- Azure
- Terraform
- production ETL / ingestion systems
- performance and cost optimization
- data quality
- production releases and operations

I want to use my real project work as the main learning environment rather than relying primarily on courses or side projects.

The most valuable situations are usually those where:

- I do not properly understand why the system behaves the way it does;
- something behaves differently from what I expected;
- there are multiple plausible technical approaches;
- a system is slow, expensive, unreliable, or hard to reason about;
- I need to understand a layer below the abstraction I normally work with;
- I need to make or defend a technical design decision.

---

## Core Philosophy

### 1. Problems before theory

Start from real engineering problems.

Do not create notes simply because a topic exists.

A topic such as Spark memory, JVM garbage collection, Kafka partitioning, or backpressure should normally enter the repository because it became relevant to an actual problem.

---

### 2. Think before asking AI

Before asking AI for an explanation, I should first write down:

- what I currently think is happening;
- what I am uncertain about;
- what assumptions I am making;
- what evidence I already have.

The point is to develop my own mental models, not outsource them.

A wrong initial explanation is useful because it makes learning visible.

---

### 3. Evidence over plausibility

Keep a strong distinction between:

- observations;
- hypotheses;
- verified facts;
- interpretations;
- conclusions;
- open questions.

Prefer evidence such as:

- documentation;
- measurements;
- experiments;
- source code;
- Spark UI / execution plans;
- logs;
- metrics;
- reproducible behavior.

Do not treat a plausible AI-generated explanation as established fact.

---

### 4. AI should challenge reasoning, not replace it

AI should primarily help by:

- identifying hidden assumptions;
- asking questions that expose gaps;
- identifying missing concepts;
- suggesting competing explanations;
- proposing experiments;
- finding authoritative sources;
- reviewing conclusions;
- challenging architecture and trade-offs;
- connecting a new case with prior cases;
- helping organize knowledge after I understand it.

AI should generally **not**:

- produce polished notes before I understand the subject;
- silently rewrite my conclusions;
- treat uncertainty as certainty;
- optimize for volume of notes;
- replace investigation with confident explanation.

A useful interaction pattern is:

**my model → AI challenge → investigation → AI review → my final model**

---

## Repository Design Principle

Use this rule:

**Folders = what kind of artifact is this?**  
**Links / tags = what is it about?**

Avoid organizing the top-level repository by technology.

Bad long-term structure:

```text
spark/
kafka/
databricks/
jvm/
```

Preferred structure:

```text
cases/
concepts/
decisions/
labs/
playbooks/
career/
```

Technologies and themes should be represented through titles, links, search, and eventually lightweight metadata.

Avoid deep folder trees.

---

## Current Repository Structure

Keep the repository deliberately small until actual use creates a need for more structure.

```text
.
├── README.md
├── AGENTS.md
├── inbox.md
├── cases/
├── concepts/
└── templates/
    └── case.md
```

### `inbox.md`

Fast capture only.

Use it for:

- questions;
- surprising behavior;
- ideas;
- things worth investigating later.

Do not polish inbox entries.

Not every inbox entry needs to become a case.

---

### `cases/`

This is the core of the repository.

A case is based on a real engineering problem or investigation.

Create a case when at least one of these is true:

- I do not understand why something behaves as it does;
- something surprised me;
- multiple solutions have meaningful trade-offs;
- I investigated performance, reliability, cost, scalability, or correctness;
- the experience changed how I would approach a similar problem next time.

Routine tickets do not need cases.

---

### `concepts/`

Reusable mental models that emerge from multiple cases.

Do **not** create concept notes just because a concept exists.

For example, do not create `spark-memory.md` because Spark memory is important.

Create it when several real cases show that a reusable mental model of Spark memory would help.

The repository should gradually produce its own useful conceptual layer from experience.

---

### `templates/`

Contains lightweight templates used by the repository.

Templates should reduce friction, not create paperwork.

If a template makes me reluctant to create a case, simplify it.

---

## Case Workflow

Default workflow:

```text
interesting problem
      ↓
quick inbox entry
      ↓
worth investigating?
      ↓
create a case
      ↓
write current mental model
      ↓
list questions / assumptions
      ↓
observe / measure / experiment / research
      ↓
AI challenges the reasoning
      ↓
form conclusion
      ↓
record what changed in my understanding
      ↓
extract one reusable lesson
```

A case should not be judged by how polished it looks.

The main question is:

**Did this case improve how I will reason about a future problem?**

---

## Case Structure

A normal case should roughly contain:

```markdown
# [Case title]

## Problem
What am I trying to understand or solve?

## What I currently think
My mental model before research or AI assistance.

## Questions
What specifically do I not understand?

## Observations
What do I actually know from measurements, logs, behavior, documentation, etc.?

## Hypotheses
What explanations currently seem plausible?

## Investigation
Experiments, evidence, research, source-code findings, or reasoning.

## Conclusion
What do I now believe and why?

## What changed in my understanding?
What would I explain differently now?

## Reusable lesson
What should I carry into future engineering problems?

## Open questions
What remains unclear?
```

The two most important sections are:

- **What I currently think**
- **What changed in my understanding?**

They preserve the learning process instead of only storing the final answer.

---

## How AI Should Review a Case

When reviewing a case, prefer to challenge it before rewriting it.

Useful review criteria:

- unsupported assumptions;
- missing evidence;
- alternative explanations;
- correlation mistaken for causation;
- missing failure modes;
- incorrect Spark/JVM/distributed-systems assumptions;
- weak experimental design;
- conclusions stronger than the evidence allows;
- contradictions with existing repository knowledge.

Do not rewrite the case until the reasoning has been reviewed.

---

## Role of Real Work

My current work should be the main source of cases.

I want to actively seek work involving:

- difficult performance problems;
- expensive workloads;
- streaming reliability;
- memory behavior;
- architectural debt;
- ingestion design;
- migrations;
- observability;
- production incidents;
- framework/library design;
- scalability questions;
- cost/performance trade-offs.

The goal is not to collect more technologies.

The goal is to increase the size and ambiguity of the technical problems I can independently own.

---

## Role of My Career Advisor / Tech Lead

My tech lead is also my career advisor.

I want to use this relationship as a feedback loop.

Rather than asking only:

> What should I learn?

I should bring concrete artifacts and ask questions such as:

- What would a Staff Engineer notice here that I missed?
- Where is my reasoning weak?
- What assumptions would you challenge?
- How would you have investigated this differently?
- What larger technical responsibility would demonstrate the next level?
- Which capability am I overestimating or underdeveloping?

Useful feedback should be captured in the relevant case when appropriate.

The long-term aim is to compare how I reason about technical problems with how a strong tech lead reasons about them, and gradually reduce the gap.

---

## Career Strategy

The broad career plan is:

### Stage 1 — Strengthen systems depth

Use project problems to deepen understanding of:

- Spark execution;
- JVM behavior;
- memory;
- serialization;
- shuffle;
- storage;
- networking;
- concurrency;
- distributed failure modes;
- streaming semantics;
- checkpointing / recovery;
- backpressure;
- Kafka internals;
- observability;
- performance measurement.

Study these through real problems instead of as an abstract curriculum whenever possible.

---

### Stage 2 — Move from implementation to design

Seek opportunities to become involved before implementation begins.

Practice:

- framing problems;
- identifying constraints;
- comparing alternatives;
- making trade-offs explicit;
- writing lightweight design notes;
- explaining why one design was chosen over another.

---

### Stage 3 — Own a technical area

Progress from:

**implementing something**

to:

**understanding it**

to:

**reviewing changes to it**

to:

**making design decisions around it**

to:

**being the person others ask about it**

Potential areas include:

- ingestion;
- Spark streaming;
- Databricks performance;
- file-processing architecture;
- platform cost optimization;
- Delta architecture;
- reliability.

---

### Stage 4 — Increase technical leverage

Develop the ability to:

- review others' designs;
- mentor engineers;
- create reusable investigation methods;
- improve team engineering practices;
- influence system design across components or teams.

This is the direction toward Staff Engineer / Architect-level work.

---

## AI / AI-Platform Direction

Do not pivot into generic “AI engineer” work merely because AI is popular.

Instead, add AI-platform knowledge gradually on top of existing data-platform strengths.

Relevant areas may include:

- data infrastructure for AI systems;
- RAG / retrieval architectures;
- embeddings and vector search;
- MLflow / model lifecycle;
- model serving;
- AI observability;
- evaluation;
- governance;
- inference cost;
- production data pipelines for AI workloads.

The goal is to become stronger at **data + AI platforms**, not merely at building chatbot demos.

---

## Future Repository Growth

Do **not** create all future folders immediately.

New structure should be added only when repeated use creates a real need.

Possible future additions:

```text
decisions/
labs/
playbooks/
career/
.ai/
```

### `decisions/`

For architectural decisions where the important question is:

**Why did I choose A instead of B?**

Possible structure:

```markdown
# Context

# Constraints

# Options

# Decision

# Why

# Consequences

# What would make me reconsider
```

---

### `labs/`

For executable experiments and reproducible technical investigations.

Create this when cases repeatedly require code or experiments that do not fit naturally inside Markdown.

---

### `playbooks/`

For reusable methods that have emerged from several cases.

Examples:

- investigating a slow Spark job;
- diagnosing memory pressure;
- evaluating a streaming design;
- reviewing an architecture.

A playbook should emerge from experience, not be copied from generic advice.

---

### `career/`

For development evidence once there is enough material to justify it.

Possible contents:

- competency map;
- evidence log;
- advisor feedback;
- quarterly reviews.

---

### `.ai/`

Only create this when AI workflows become stable and repeatedly useful.

Possible future structure:

```text
.ai/
├── workflows/
├── prompts/
└── checks/
```

Prefer reusable workflows over a large collection of clever prompts.

---

## Future Agentic AI Strategy

Agentic AI becomes useful once the repository contains enough of my own reasoning.

Agents should work over the repository, not replace it.

Good future agent jobs include:

### 1. Inbox triage
Identify which entries deserve cases, which relate to existing knowledge, and which can be ignored.

### 2. Knowledge linking
Suggest relationships between cases, concepts, decisions, and playbooks.

### 3. Reasoning review
Challenge completed cases for assumptions, missing evidence, failure modes, and contradictions.

### 4. Experiment generation
Propose or scaffold experiments that distinguish competing hypotheses.

### 5. Knowledge maintenance
Suggest updates to canonical concept notes when several cases change or refine my mental model.

### 6. Career analysis
Review recent work and identify evidence of deeper ownership, recurring weaknesses, and gaps in experience.

### 7. Architecture review
Act as an adversarial reviewer of design proposals and identify hidden assumptions or operational risks.

---

## Agent Safety Rule

Agents may **propose** changes to canonical knowledge.

They should not silently rewrite established conclusions.

Prefer a pull-request / review mindset:

**agent proposes → I inspect → I accept, modify, or reject**

The repository should remain a record of my engineering judgment, not an automatically generated wiki.

---

## Metadata Strategy

Do not add metadata until there are enough files for it to solve a real problem.

When needed, prefer a very small schema, for example:

```yaml
---
type: case
status: active
created: 2026-10-02
topics:
  - spark
  - memory
  - io
---
```

Avoid elaborate scoring, confidence systems, difficulty levels, review intervals, and other maintenance-heavy metadata unless they prove genuinely useful.

---

## Confidentiality

This repository contains **generalized personal learning**, not employer or customer documentation.

Never include:

- customer names;
- proprietary source code;
- credentials;
- internal URLs;
- production data;
- internal identifiers;
- unreleased architecture;
- confidential incident details;
- commercially sensitive costs;
- anything prohibited by employer/client policy.

Work experiences should be rewritten as generic engineering problems.

Example:

Bad:

> Customer X's ingestion system uses internal service Y and costs €Z.

Better:

> A high-volume binary-file ingestion system exhibited unexpectedly high memory usage.

The transferable learning matters. The proprietary context does not.

---

## Git / Mobile Workflow

The repository is plain Markdown + Git.

Current Android workflow:

**Obsidian → local repository → GitSync → GitHub**

Use the phone primarily for:

- quick capture;
- reading;
- light edits;
- adding hypotheses or questions.

Use desktop primarily for:

- full investigations;
- experiments;
- larger case write-ups;
- agentic workflows;
- refactoring repository structure.

Keep the repository portable and tool-independent.

Avoid making Obsidian, Claude, ChatGPT, or any other single tool a hard dependency.

---

## Tool Independence

`AGENTS.md` is the canonical description of repository intent.

Vendor-specific files such as `CLAUDE.md` may exist later, but they should remain thin and point back to this file where practical.

The knowledge should belong to the repository, not to a specific AI vendor.

---

## What Success Looks Like

Success is **not**:

- hundreds of Markdown files;
- a beautiful knowledge graph;
- daily note-taking;
- knowing every Spark or Kafka feature;
- maintaining a complex PKM system.

Success is:

- repeatedly turning real work problems into deeper understanding;
- becoming better at identifying what I do not know;
- improving how I investigate technical systems;
- becoming more evidence-driven;
- making better architecture decisions;
- building reusable engineering judgment;
- increasing the size and ambiguity of problems I can own;
- developing toward Staff Engineer / Data Platform Architect / Solutions Architect-level responsibility.

A successful year may contain only 10–15 excellent cases.

Depth matters more than volume.

---

## Default Instruction to Any AI Model

When working with this repository:

1. Preserve the distinction between observation, hypothesis, and conclusion.
2. Challenge my reasoning before polishing it.
3. Ask what evidence supports a claim.
4. Prefer experiments and authoritative sources over confident speculation.
5. Connect new work to relevant existing cases or concepts.
6. Do not create unnecessary structure.
7. Do not turn the repository into generic documentation.
8. Do not invent project-specific details.
9. Protect confidential information.
10. Optimize for my long-term engineering judgment, not short-term note production.

If unsure whether to add something, prefer **less structure and more deliberate reasoning**.
