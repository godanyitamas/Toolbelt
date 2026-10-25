# Review Case Workflow

## Purpose

Challenge an existing case so that it improves how I will reason about a future problem.

The goal is not a better-written case. It is a case whose conclusions match its evidence, whose uncertainty is visible and whose parts do not contradict each other or the rest of the repository.

---

## When to use

- A case is about to move from `active` or `draft` to `done`.
- A decision record is about to be written from a case.
- New evidence arrived that may change a conclusion.
- I ask for a review.

---

## Context

1. `AGENTS.md` is already loaded.
2. Read the case.
3. Read the decision records it links to.
4. Search `cases/` for shared `topics:` values and read only the sections needed to check consistency (usually **What I currently think**, **Conclusion**, **Reusable lesson**).

---

## Review steps

Work through these in order. Skip a step when it has nothing material to say.

### 1. Restate the claim

Summarize in two or three sentences what the case concludes and on what evidence. If that is not possible, the case has no clear claim yet: report that first and stop.

### 2. Evidence

- Does each conclusion trace to an observation, measurement, experiment, documentation or source code?
- Is anything presented as verified that is still a hypothesis or an interpretation?
- Is a conclusion drawn from a single observation, one day or one environment?
- Is the strength of each conclusion stated, and does it match the evidence?

### 3. Causality and alternatives

- Is correlation presented as causation?
- Which competing explanation was not considered, and what observation would separate it from the current one?

### 4. Technical assumptions

Check Spark, JVM, storage, streaming and distributed-systems claims. Cite authoritative sources (documentation, source code) where they settle a point. If they don't, mark the claim as unverified rather than arguing from plausibility.

### 5. Consistency

- Within the case: do **What I currently think**, the handoff or summary, the design sections, **Conclusion**, **What changed in my understanding?** and **Reusable lesson** agree?
- With decision records: does the case contradict a decision, or has a decision's "what would make me reconsider" trigger fired?
- With other cases: does it contradict an established conclusion elsewhere?

### 6. Open hypotheses

For each important open hypothesis, propose the cheapest experiment that distinguishes it from its alternatives. State in advance which result would support it and which would refute it.

### 7. Learning

- Is the change in my mental model explicit, and in my words?
- Is the reusable lesson derived from this case, or is it generic advice?

### 8. Confidentiality

Flag anything that names a customer, internal system, identifier, URL, cost or confidential detail.

---

## Output

A findings list, ranked by how much each finding could change a conclusion. For each finding:

- **Where:** the section.
- **Finding:** what is wrong or missing.
- **Why it matters:** what conclusion it could change.
- **Proposal:** a question for me, an experiment, or replacement wording.

When a finding is a contradiction, show the current text and the proposed text side by side.

Keep it to the findings that matter, usually no more than ten. No sentence-level style edits unless I ask. "No material issues" is a valid result.

---

## Rules

- Propose; do not edit. Apply only what I accept, as surgical edits.
- Do not rewrite **What I currently think** or **What changed in my understanding?** in your own words without marking the text as a proposal for me to adjust.
- Do not upgrade a hypothesis to a fact during the review.
- Do not fill gaps with invented project details. Turn them into open questions.

---

## Definition of done

- Every finding names its location, its consequence and a concrete proposal.
- Contradictions are listed with replacement wording.
- I have accepted, modified or rejected each proposal, and only the accepted ones are applied.
