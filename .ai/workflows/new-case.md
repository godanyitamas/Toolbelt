# New Case Workflow

## Purpose

Use this workflow whenever I want to turn a real engineering experience, problem, investigation, or technical decision into a new case.

The goal is not to produce polished documentation quickly.

The goal is to reconstruct the reasoning, challenge it, preserve uncertainty, and capture what I learned in a form that improves future engineering judgment.

---

## Inputs

The user may begin with anything from:

- a one-sentence problem;
- a rough memory of an incident;
- a technical question;
- a performance investigation;
- an optimization;
- a design decision;
- a surprising system behavior;
- an already partially written case.

Do not require the user to prepare a structured brief first.

---

## Phase 1 — Orient

Before discussing the case in depth:

1. Read the repository `AGENTS.md`.
2. Read `templates/case.md`.
3. Search `cases/` for closely related problems, technologies, or investigations.
4. Read only the cases that are actually relevant.

Do not load the entire repository unless the task genuinely requires it.

If the case is clearly retrospective and based primarily on historical memory, use `templates/retrospective-case.md` instead.

---

## Phase 2 — Classify

Determine whether the material should become:

- a new case;
- an addition to an existing case;
- an inbox entry;
- or no repository artifact.

Prefer an existing case when the new experience is clearly part of the same investigation or learning thread.

Create a new case when the experience contains a distinct engineering problem, investigation, decision, or change in understanding.

Do not create a case merely because a technology or topic is interesting.

---

## Phase 3 — Capture the user's unaided model

Before providing a technical explanation, establish what I currently think.

Ask me to describe, in my own words:

- what happened;
- what I expected;
- what actually happened;
- what I think caused it;
- what I am unsure about;
- what evidence I already have.

Do not correct the mental model prematurely.

A wrong model is useful evidence because it shows what the investigation changed.

If I already provided this information, do not ask me to repeat it unnecessarily.

---

## Phase 4 — Challenge the reasoning

Before researching or drafting, identify the most important reasoning risks.

Challenge things such as:

- unsupported causal claims;
- assumptions presented as facts;
- correlation mistaken for causation;
- missing alternative explanations;
- measurements that do not actually distinguish hypotheses;
- incomplete understanding of system boundaries;
- missing failure modes;
- hidden trade-offs;
- conclusions based on a single observation;
- retrospective knowledge accidentally projected onto what I knew at the time.

Do not challenge every sentence mechanically.

Focus on the assumptions that could materially change the conclusion.

Turn important uncertainty into explicit questions.

---

## Phase 5 — Establish the investigation plan

For each important uncertainty, determine what evidence would resolve it.

Possible evidence includes:

- documentation;
- source code;
- execution plans;
- Spark UI;
- logs;
- metrics;
- system tables;
- controlled experiments;
- before/after measurements;
- reproducible behavior;
- architecture diagrams;
- discussions with subject-matter experts.

Prefer the cheapest reliable evidence first.

When competing explanations exist, prioritize experiments that distinguish them.

Do not recommend an experiment merely because it is technically interesting. It should answer a meaningful question.

---

## Phase 6 — Investigate

Help me work through the evidence.

Maintain explicit separation between:

### Observation

Something directly observed or measured.

### Hypothesis

An explanation that is plausible but not yet established.

### Verified fact

A claim supported by reliable evidence.

### Interpretation

A reasoned explanation built from the evidence.

### Conclusion

The current best explanation or engineering judgment.

### Open question

Something that remains unresolved.

When external research is used, distinguish authoritative documentation or source-code evidence from interpretation.

Do not upgrade a hypothesis to a fact simply because it sounds technically plausible.

---

## Phase 7 — Reassess the mental model

After investigation, explicitly compare:

**What I thought before → what the evidence showed → what I think now**

Ask:

- Which part of my original model was wrong?
- Which part was correct?
- Which assumptions were incomplete?
- What concept did I misunderstand?
- What would I do differently next time?

This is one of the most important parts of the case.

---

## Phase 8 — Draft the case

Only after the reasoning is sufficiently clear, create the case using:

`templates/case.md`

The case should contain enough detail to preserve the reasoning without becoming generic documentation.

Prioritize:

1. the actual engineering problem;
2. the initial mental model;
3. the important evidence;
4. competing hypotheses;
5. investigation;
6. conclusion and evidence strength;
7. what changed in understanding;
8. one concise reusable lesson;
9. remaining uncertainty.

Do not inflate the case with background knowledge that was not relevant to the investigation.

Do not turn the case into a tutorial unless the repository later develops a separate reason to do so.

---

## Phase 9 — Final reasoning review

Before committing, review the completed case against these questions:

### Evidence

- Are observations clearly separated from explanations?
- Are important measurements or experiments represented?
- Are claims supported by the evidence available?

### Causality

- Does the case claim causation where only correlation was established?
- Were alternative explanations considered where relevant?

### Technical reasoning

- Are there hidden assumptions?
- Are there important distributed-systems, Spark, JVM, storage, networking, or operational considerations missing?
- Are trade-offs represented fairly?

### Learning

- Is the change in mental model explicit?
- Is the reusable lesson actually derived from the case?
- Does the case improve future engineering judgment?

### Uncertainty

- Are unresolved questions preserved?
- Is the confidence in important conclusions proportional to the evidence?

### Confidentiality

- Has proprietary or sensitive information been generalized or removed?

Do not silently "fix" weak reasoning during this review. Surface it to the user.

---

## Phase 10 — Commit

When the case is ready:

1. Choose a concise, descriptive filename using lowercase kebab-case.
2. Create the file under `cases/`.
3. Use the current `templates/case.md` structure unless there is a justified reason to deviate.
4. Preserve uncertainty rather than inventing missing details.
5. Commit the file with a clear commit message.
6. Report what was created and any important unresolved questions.

If the user has not asked for the case to be committed yet, provide the draft for review instead.

---

## Interaction Rules

### Do not interrogate unnecessarily

The user may have already provided substantial context.

Ask only the questions needed to resolve meaningful ambiguity or improve the case.

### Do not write too early

A polished case written before the reasoning is tested can hide misunderstandings.

The preferred sequence is:

**capture → challenge → investigate → reassess → draft → review**

### Do not over-research

Research should answer concrete questions arising from the case.

Do not turn a case into an academic survey unless the user explicitly wants that.

### Do not invent missing history

Especially for old cases, distinguish:

- what the user remembers;
- what records establish;
- what is inferred today.

If the evidence is unavailable, record the uncertainty.

### Do not optimize for case count

A small number of deep cases is more valuable than many shallow ones.

---

## Useful Opening Interaction

When the user says something like:

> I have a new case about X.

A good first response should usually:

1. acknowledge the subject;
2. state that the case workflow will be used;
3. ask for the user's unaided account of what happened, unless enough has already been provided;
4. avoid explaining the technical issue prematurely.

For example:

> Let's reconstruct it before we write it. Tell me what happened, what you expected, what surprised you, what you initially thought was causing it, and what evidence you had at the time. I’ll challenge the reasoning before we turn it into the case.

Do not use this exact wording mechanically; adapt it to the situation.

---

## Definition of Done

A new case is ready when:

- the engineering problem is clear;
- the user's initial mental model is captured;
- important assumptions have been challenged;
- meaningful evidence has been identified;
- competing explanations have been considered where relevant;
- the current conclusion is proportional to the evidence;
- the change in understanding is explicit;
- the reusable lesson is concise;
- unresolved uncertainty is visible;
- confidentiality has been checked;
- the case can stand on its own without requiring the AI to remember the conversation that created it.

The final artifact should preserve **engineering judgment**, not merely the final answer.
