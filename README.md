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
└── templates/
    └── case.md
