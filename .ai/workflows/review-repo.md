# Review Repository Workflow

## Purpose

Keep the casebook connected and honest as it grows: link related cases, notice reusable mental models, triage the inbox, and surface stale or contradictory knowledge.

This is not cleanup for its own sake. A finding earns a place only if acting on it would improve how I reason about future problems or make existing knowledge easier to find and trust.

---

## When to use

Whichever comes first:

- the inbox has five or more open entries;
- about three new cases have been added since the last review;
- a quarter has passed;
- I ask for a review.

---

## Context

Read broadly but shallowly. Open full files only when a finding needs it.

1. `AGENTS.md` is already loaded.
2. `inbox.md`.
3. For each case: front-matter, title, **What I currently think**, **Conclusion**, **Reusable lesson**, **Open questions** and **Connections**.
4. For each decision record: title, **Decision** and **What would make me reconsider**.
5. `git log` dates per file, for staleness.

---

## Checks

### 1. Inbox triage

For each open entry, propose one outcome with a one-line reason: new case, addition to an existing case (name it), concept candidate, or drop.

### 2. Connections

Find cases that share a mechanism, failure mode or decision but do not link to each other. Propose the link with one line on why the connection matters. Shared technology alone is not enough.

### 3. Concept candidates

Propose a concept only when several cases show the same reusable mental model. Name the cases and state the shared model in one or two sentences. Do not draft the concept note unless I ask.

### 4. Status and staleness

- `active` cases with no change in a long time: ask whether the investigation is still active and what the next step is.
- `draft` cases whose reasoning has since been verified: ask whether they are ready for a case review and `done`.

Staleness is a question, not a verdict.

### 5. Contradictions

- Between cases.
- Between a case and a decision record.
- Decision records whose "what would make me reconsider" trigger has fired, according to a later case.

### 6. Open questions answered elsewhere

An open question in one case that another case or decision answers.

### 7. Topics

Near-duplicate `topics:` values (e.g. singular and plural, synonyms). Propose one canonical value.

### 8. Structure

Propose new structure (`labs/`, `playbooks/`, `career/`) only when repeated use shows the need, per `README.md`. Flag a template that causes friction.

### 9. Confidentiality

Sweep for anything that names a customer, internal system, identifier, URL, cost or confidential detail.

---

## Output

A short report:

1. **Summary:** at most three lines on the state of the repository and the most important finding.
2. **Proposals:** grouped by check, highest-value first. Each states the files involved, the proposed change and why.
3. **Nothing to do:** checks that found nothing, listed in one line.

"No changes needed" is a valid result.

---

## Rules

- Propose only. Apply what I accept, surgically, grouped into commits by kind (links, topics, inbox, status).
- Do not delete content. Dropping an inbox entry means checking it off with a reason.
- Do not change a case's conclusions here. A finding that challenges a conclusion becomes a proposal to run `review-case.md` on that case.
- Career analysis is a separate job and is out of scope.

---

## Definition of done

- Every open inbox entry has a proposed outcome.
- Each proposal names the files involved and the reason.
- I have accepted, modified or rejected each proposal, and only the accepted ones are applied.
