# Engineering Casebook

A personal engineering casebook built from real problems I encounter in my work.

The purpose of this repository is not to collect everything I learn. It is to improve how I reason about technical problems and gradually develop from an implementation-focused Senior Data Engineer toward stronger systems, architecture, and technical ownership skills.

## What belongs here

Things worth capturing usually involve at least one of these:

- I don't properly understand why something behaves the way it does.
- Something behaved differently from what I expected.
- There are multiple reasonable technical approaches with meaningful trade-offs.
- I investigated a performance, reliability, cost, or scalability problem.
- I learned something that will change how I approach similar problems in the future.
- I made a technical decision worth remembering.

Routine implementation work does not need a note.

## Principles

### Problems before theory

Start from real engineering problems.

Do not create notes simply because a technology or concept exists.

### Think before asking AI

Before asking AI for an explanation, write down:

- what I currently think is happening;
- what I am uncertain about;
- what assumptions I am making.

The goal is to improve my own mental models, not outsource them.

### Evidence over plausibility

Distinguish between:

- observations;
- hypotheses;
- verified facts;
- interpretations;
- open questions.

Prefer documentation, measurements, experiments, source code, logs, execution plans, and other evidence over plausible explanations.

### AI should challenge, not replace, reasoning

Use AI to:

- expose assumptions;
- identify missing concepts;
- propose competing explanations;
- suggest experiments;
- review conclusions;
- challenge architecture and trade-offs;
- help organize knowledge after I understand it.

Avoid using AI merely to generate polished notes that I do not understand.

### Keep it useful

A good case should eventually help me solve another problem better.

One deeply understood case is more valuable than twenty shallow notes.

### Protect confidential information

This repository contains generalized personal learning, not customer or employer documentation.

Never include:

- customer names;
- proprietary source code;
- internal URLs or identifiers;
- credentials;
- production data;
- confidential architecture;
- sensitive incident details;
- commercially sensitive information.

Abstract work situations into transferable engineering problems.

## Structure

```text
.
├── README.md
├── inbox.md
├── cases/
├── concepts/
├── decisions/
└── templates/
    └── case.md
```

---

# Direction and strategy

> Moved from `AGENTS.md` so agents do not load it on every turn. This is context for me, not standing instructions for agents; agents read it only when a task needs it (e.g. career analysis or architecture review).

## Purpose and career direction

This repository is my personal **engineering casebook**.

It is not intended to become an encyclopedia of technologies, a second version of official documentation, or a large personal-knowledge-management project.

Its purpose is to help me become a stronger engineer by capturing and reflecting on **real technical problems I encounter in my work**, then turning those experiences into better mental models, reusable engineering judgment, and eventually stronger architecture and technical-leadership skills.

My broader career direction is:

**Senior Data Engineer → deeper systems / distributed-systems engineer → Staff-level engineer and/or Data Platform / Solutions Architect**

AI is part of this strategy, but not because I want to compete on code generation. The assumption is that code will continue to become cheaper. I therefore want to move upward in abstraction:

**implementation → systems understanding → architecture → technical ownership**

The repository should help me make that transition deliberately.


## Current technical context

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

```
decisions/
labs/
playbooks/
career/
.ai/
```

The `.ai/` directory is now justified for stable, reusable agent workflows. Keep it small and repository-specific.

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

Contains only stable, reusable workflows and checks for AI-assisted work on this repository.

Do not turn it into a prompt collection.

Prefer workflows that describe:

- what context to load;
- what reasoning process to follow;
- what questions to ask;
- what evidence standard to apply;
- what artifact to produce;
- what checks to perform before committing.

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

