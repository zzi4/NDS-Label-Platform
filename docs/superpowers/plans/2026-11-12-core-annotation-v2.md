# Core Annotation V2 Implementation Plan

> Execute in this session using isolated workspaces and independent file ownership. User authorization already includes implementation, sample API calls and website adaptation/deployment. Only bulk relabel approval remains pending.

**Goal:** Implement and test core V2 and compatible public website; present sample images/results without relabeling production.
**Architecture:** Isolated backend contract/trial CLI; strict dual-version statistics reader and V7 transport; twelve-feature UI with marked legacy projection.
**Tech Stack:** Python, pytest, PostgreSQL read-only, existing DeepSeek client, Streamlit, Plotly, GitHub/HF.
**Spec:** ../specs/2026-11-12-core-annotation-v2.md

## Global Constraints
- No production annotation writes, queue changes, bulk API calls, secret output or worker activation.
- Exact taxonomy spec shared byte-for-byte; all h semantics and public privacy preserved.
- Old V1 labels and V1–V6 transport remain accepted and distinguishable.

## Review Focus
- V2 one scene only, unknown not positive; complete/malformed/mixed-version rejection.
- Legacy scene ambiguity and material/vehicle/VRU grouping cannot invent observations.
- Projection must not enter publication or imply actual V2 reannotation.
- No test CLI import or trial execution opens a writer connection or alters production.
- UI filtering, hours, image dedupe and Q&A use consistent projected populations.

## Tasks
- [x] 1. Backend contract/trial CLI: failing behavior tests, V2 definitions/prompt/validator, bounded no-write runner; targeted and full tests.
- [x] 2. Statistics data layer: failing dual-version/projection/V7 tests; core_annotations.py, annotation_statistics.py, public_snapshot.py and exact taxonomy resource; v1 compatibility tests.
- [x] 3. Statistics UI: twelve-field default/original27 switch, eight single-scene data, hour distributions, presets, cards, no multi-select donut; targeted tests.
- [x] 4. Root integration: QA and HF allowlist/docs, independent review, full suites and local browser preview.
- [x] 5. Read-only sample selection, inspect images, at most8 DeepSeek calls, validate raw results, produce user-facing HTML/CSV/images report.
- [x] 6. Deploy compatible website via existing GitHub/HF and campus flow; verify old data unchanged, public auto updates retained. Present sample results and defer bulk.

## Completion evidence

Backend full suite: 325 passed. Final statistics full suite after projection review fix: 611 passed. Independent review approved. Eight real deepseek-flash trials returned complete V2 structures; image review flagged scene/lane concerns in04/08, so trial acceptance is pending and no bulk activation occurred. Selected old labels/revisions remained unchanged.

Statistics PR #53 merged as95bbcfb; workflow43178872138 succeeded; HF169b1a089de4784b8309c030caa9afea0a622576 RUNNING. Original public URL and campus desktop/390px checks passed. Production V1 worker remains onfb1cbf7; public publication remains V6/V1 source data. Trial results stay local.
