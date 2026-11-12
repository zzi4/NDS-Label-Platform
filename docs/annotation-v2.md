# Core representative-frame annotation V2

`annotation_schema_v2.py` defines the isolated `aerial_annotation_v2` contract and
`battery-frame-core-v2` prompt. The exact twelve-field taxonomy is in
`data/annotation-fields-v2.json`. V1 remains available in `annotation_schema.py`;
the active worker, its defaults, production database and queues are unchanged.

`FIELD_DEFINITIONS` exposes the taxonomy; `definitions()` returns a fresh copy.
`build_annotation_prompt()` includes the complete taxonomy and visual rules.
`validate_annotation(payload)` returns a list of errors, and `extract_labels(payload)`
returns independent validated labels or raises `ValueError`. Both accept the model
`{"labels": [...]}` envelope or its stored bare array. The trial runner requires
the envelope on the model wire.

The contract requires all twelve known fields exactly once. Every label contains
`field`, `state`, `values`, `scope="frame"`, `origin="model"`,
`evidence="representative"`, and optionally a nonempty `reason`. The four model
states are `observed`, `not_observed`, `unknown`, and `not_applicable`.
`observed` requires positive enum values, following each field's cardinality;
all other states require an empty values array. `unknown` and `not_applicable`
require a reason. An observed `scene.type` also requires a reason describing
visible evidence. Extra fields, V1 fields, booleans, duplicate fields or values,
invalid enums, measurements and numeric confidence properties are rejected.

Scene selection identifies the dominant subject before considering roundabout or
cross intersection, parking area, internal park road, and ordinary road segment.
An observed scene has exactly one of 高速路、快速路、城市主干路、城市街巷、十字路口、环岛、停车区域、园区道路.
Unknown is a state with no positive scene value. Parking-aisle crossings and
overpasses are excluded from cross intersections; roadside parking does not make
a road a parking area. Unsupported ordinary/rural scenes and ambiguous views
remain unknown. Main and secondary urban corridors can both use 城市主干路.
Lane count concerns one dominant direction. Vehicle and VRU labels use broad
classes. Unclear signs or markings remain unknown. No weather, temperature,
school/hospital use, geometry or batch-wide behavior is inferred. Prompt prose
sets these evidence boundaries; format validation cannot establish image truth.

## Private trial CLI

Use the existing worker virtual environment and a mode-0600 manifest containing
only existing representative JPEG frames, with absolute paths and safe unique IDs
(`run` is reserved for run metadata):

```json
[
  {"id": "sample-01", "frame_path": "/private/existing/frame.jpg"}
]
```

```bash
/home/stu1/Projects/NDS-Statistics-Platform/.venv/bin/python annotation_trial_v2.py \
  --env-file /private/worker.env \
  --manifest /private/trial-manifest.json \
  --output /private/new-trial-results \
  --max-frames 8
```

`--env-file` is optional when `DEEPSEEK_API_KEY` is already configured. It uses the
existing safe `annotation_worker.load_env_file` loader and requires a private file.
Only the API key and supported model setting are used by this runner; it never
calls `Config.environment()`, consumes DSNs, opens a database
connection, extracts video frames, writes annotations, or changes a queue.
The reused worker HTTP transport supports existing JPEG frames.

`--max-frames` defaults to eight and must be from one through eight. The complete
manifest is preflighted before any output or API calls; only its first selected
entries are called. There is exactly one request per selected entry, with
`max_tokens=4096`, no retries, and immediate stop on auth/quota or explicit fatal
transport errors. A prior `run.json` or selected result is refused so evidence
cannot be overwritten. Re-running with a new output directory is a separate paid
trial and must remain within the authorized total.

New output directories use mode 0700; existing directories must already be
private and owned by the current user. Files use mode 0600. Per-ID JSON stores
raw model content (with any literal API key redacted), exact versions, prompt and
frame SHA-256 hashes, model, usage, timing, validation errors, status, and labels
only when valid. Raw content is saved before JSON and schema validation. Invalid
JSON, duplicate JSON properties, malformed labels and old-field mixtures are
retained for review without repair. API failures have no raw content when the
transport received none. `run.json` tracks attempted IDs, statuses, and fatal stop.
Trial records are local review artifacts and are never published or stored as
production annotations.

Stdout contains only JSON identifiers and statuses. Configuration failure returns
exit code 2 and a generic status on stderr; any unsuccessful sample returns 1;
all selected samples valid returns 0. Credentials, frame paths, raw content and
exception details are never printed.

`run_trials(manifest, output, *, api_key, max_frames=8, client=None)` supports an
injected `client(config, frame, *, prompt, max_tokens)` for tests. It receives a
minimal repr-hidden `api_key` config and returns the same response structure as
`annotation_worker.call_model`. Tests use generated temporary JPEGs and fake
clients, forbid database access, and do not read private worker credentials or
call a real model. V2 production activation and bulk reannotation require later
review and are outside this change.
