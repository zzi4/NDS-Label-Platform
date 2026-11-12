# Core annotation V2 — approved scope 2026-11-12

User approved twelve core features, eight-way single scene selection, a few paid DeepSeek representative-frame tests, website adaptation preserving old results, and NO bulk relabel before reviewing examples.

Exact twelve definitions: `data/annotation-fields-v2.json`. The backend and statistics repository copies must be byte-identical. Schema aerial_annotation_v2, prompt battery-frame-core-v2.

Observed scene requires exactly one of 高速路、快速路、城市主干路、城市街巷、十字路口、环岛、停车区域、园区道路. Unknown is a state with empty values, never a ninth scene. Identify dominant subject before scene priority: roundabout/cross intersection, parking area, internal park road, then ordinary road segment. Parking-aisle crossings and overpasses are not cross intersections; roadside parking does not make a road a parking area. Unsupported ordinary/rural roads are not forced into urban streets. Main/secondary urban corridors may share 城市主干路 in this platform.

Model envelope labels array, exactly twelve known fields once. Retain fixed frame/model/representative provenance. Four allowed V2 model states observed/not_observed/unknown/not_applicable; no unlabeled output. Positive values only; unknown/not_applicable require nonempty reason, scene observed also requires visible-evidence reason. No numeric confidence, measurements or batch-wide inferences. Lane count is one dominant direction, broad vehicle/VRU types; unclear markings/signs are unknown rather than absent. No school/hospital or weather/geometry guesswork.

Do not mutate source DB, queues or active V1 worker. New V2 trial runner is isolated and reads existing frame files only. At most eight initial API calls; preserve raw responses locally with no credentials printed. Samples stay out of production data until user review. Old results continue running/displaying. V2 production activation and bulk requeue are deferred.

Website default is twelve-feature view, with explicit read-only V1 projection provenance and optional original27 view. Raw V1 annotations remain unchanged. Classify legacy scene from known intersection and road tags only; ambiguous/uncovered stays unknown. Legacy projection is never stored/exported as model V2. All visible measurements stay associated video h. No multi-select donut. Preserve original layout, city map, preset cards, gallery point dedupe, all-batch updates and public auto-publishing.

Transport: strict new V7 for explicit per-record schema_version whenever real V2 labels are present, with existing V6 duration/frame/update/coverage semantics. V1–V6 stay readable and unchanged for V1-only defaults. Publish reader first; current publisher emits existing V6 until real V2 is explicitly saved in future. No trial records published now.
