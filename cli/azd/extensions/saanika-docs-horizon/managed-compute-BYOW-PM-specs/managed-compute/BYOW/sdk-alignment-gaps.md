# SDK ↔ Spec Alignment Gaps — Managed Compute Models API

> **Purpose:** Field-level gap analysis between the managed-compute design specs and the TypeSpec-generated Python SDK (`project_client.models`). For PM and backend team review before Build release.
>
> **Specs compared:** `spec-models-register-local.md`, `spec-models-register-training-job.md`, `spec-models-deploy.md`
>
> **Date:** April 2026

---

## Priority Legend

| Priority | Meaning |
|---|---|
| **P0** | Must resolve before Build — breaks core workflows or contract mismatches |
| **P1** | Should resolve — UX regressions or missing metadata users need |
| **P2** | Can defer — naming preferences, documentation gaps |

---

## 1. `startPendingUpload`

| # | Area | Field | Spec | Generated SDK | Priority | Recommendation |
|---|---|---|---|---|---|---|
| 1.1 | URL | Path style | `…/versions/{version}:startPendingUpload` | `…/versions/{version}/startPendingUpload` | P2 | Align to `:action` style per Azure API guidelines |
| 1.2 | URL | Path param name | `modelName` | `name` | P2 | Either works; pick one and apply consistently across all model operations |
| 1.3 | Request | Body shape | Flat field `pendingUploadType` | Wrapped in `PendingUploadRequest` object | P2 | Spec should document the wrapper object name to match TypeSpec |
| 1.4 | Request | `pendingUploadType` value | `TemporaryBlobReference` | `BlobReference` | **P1** | **Decide which value wins.** `TemporaryBlobReference` is more descriptive (conveys SAS expiry). Recommend keeping spec value |
| 1.5 | Request | `pendingUploadId` | Not in spec | Present (optional) | P2 | Useful for idempotency. Spec should document as optional |
| 1.6 | Response | Blob info wrapper | `blobReference` | `blobReferenceForConsumption` | **P1** | SDK name is verbose. Alias to shorter property name or rename in TypeSpec |
| 1.7 | Response | `sasUri` | Present (flat) | Missing — nested inside `credential` object | **P1** | **Significant UX gap.** Spec assumes flat access; SDK requires `blob_ref.credential.sas_uri`. Align wire format or add convenience accessor |
| 1.8 | Response | `containerPath` | Present | Missing | **P1** | Users need this to construct upload paths. Add to TypeSpec |
| 1.9 | Response | `expiresOn` | Present | Missing | **P1** | Users need to know when SAS expires. Add to TypeSpec |
| 1.10 | Response | `blobUri` | Not in spec | Present | P2 | Spec should document if shipping. May replace `containerPath` + `sasUri` |

---

## 2. `create_or_update`

| # | Area | Field | Spec | Generated SDK | Priority | Recommendation |
|---|---|---|---|---|---|---|
| 2.1 | URL | HTTP method | `PUT` | `PATCH` | **P0** | **Contract mismatch.** PUT = create-or-replace (idempotent). PATCH = partial update. Fundamentally different semantics. Confirm intended method with backend |
| 2.2 | URL | Behavior | Async — 202 Accepted with polling URL | Sync — 200/201 | **P0** | **Confirm:** Is backend truly sync now, or is SDK hiding the LRO? If the backend changed to sync, spec needs update. If SDK hides LRO, document wire-level 202 |
| 2.3 | Request | `weightType` | Present (`"FullWeight"`, `"LoRA"`, `"DraftModel"`) | Missing — no equivalent field | **P0** | **Critical.** `weightType` is the discriminator that drives validation for LoRA vs FullWeight vs DraftModel. Must be added to TypeSpec before Build |
| 2.4 | Request | `baseModel` | Top-level flat string | Nested: `derivedModelInformation.baseModel` | **P0** | **UX impact.** `model.base_model` (spec) vs `model.derived_model_information.base_model` (SDK). Push for flat field or add convenience alias |
| 2.5 | Request | `blobUri` / artifact location | Not in request body (implicit from prior `startPendingUpload`) | Present on `ModelVersion` | **P1** | Spec design: blob location linked via `startPendingUpload` — no echo needed at commit. SDK expects client to pass it. Confirm intended contract |
| 2.6 | Response | `status` | `"Registered"` | Missing | **P1** | Users need registration status for async polling. Add to TypeSpec |
| 2.7 | Response | `createdAt` | Top-level field | Only via `systemData` | P2 | Spec should reference `systemData.createdAt` if that's the wire format |
| 2.8 | Response | `storageUri` | Present | Missing (only `blobUri`) | **P1** | `storageUri` is the canonical project-scoped reference. Add or alias |
| 2.9 | Response | `properties.sizeBytes` | Present | Missing | **P1** | Users need model size for capacity planning. Add to TypeSpec |
| 2.10 | Response | `properties.fileCount` | Present | Missing | P2 | Nice to have for upload verification. Lower priority |
| 2.11 | Response | `weightType` | Present | Missing | **P0** | Same as 2.3 — discriminator must appear in responses too |
| 2.12 | Response | `baseModel` | Top-level flat | Nested under `derivedModelInformation` | **P0** | Same as 2.4 |

---

## 3. `list`

| # | Area | Field | Spec | Generated SDK | Priority | Recommendation |
|---|---|---|---|---|---|---|
| 3.1 | Request | `source.jobId` query filter | Required — must be supported on Models API | Missing — `list()` accepts no filter params | **P0** | **Must-have for Build.** Training→deploy workflow requires discovering models by job ID. Add filter param to TypeSpec |
| 3.2 | SDK | `source_job_id` keyword arg | `project_client.models.list(source_job_id="…")` | Missing | **P0** | Same as 3.1 — SDK must expose this filter |
| 3.3 | Response | `source.sourceType` | `"TrainingJob"` (lineage block) | Missing — no `source` field on `ModelVersion` | **P1** | Lineage metadata needed for workflow traceability. Add to TypeSpec |
| 3.4 | Response | `source.jobId` | Present | Missing | **P1** | Same as 3.3 |
| 3.5 | Response | `weightType` | Present | Missing | **P0** | Same as 2.3/2.11 |
| 3.6 | Response | `baseModel` (flat) | Present | Nested under `derivedModelInformation` | **P0** | Same as 2.4/2.12 |
| 3.7 | Response | `status` | `"Registered"` | Missing | **P1** | Same as 2.6 |
| 3.8 | Response | `storageUri` | Present | Missing (only `blobUri`) | **P1** | Same as 2.8 |

---

## 4. Operations — Coverage Gaps

### In SDK but not in specs

| Operation | SDK Method | HTTP | Notes | Action |
|---|---|---|---|---|
| `get` | `project_client.models.get(...)` | `GET /models/{name}/versions/{version}` | Likely covered in `spec-models-deploy.md` under "show" CLI. Verify and cross-reference | **Verify** |
| `list_versions` | `project_client.models.list_versions(...)` | `GET /models/{name}/versions` | Not explicitly in specs. Useful for multi-version workflows | **Document in spec** |
| `delete` | `project_client.models.delete(...)` | `DELETE /models/{name}/versions/{version}` | Covered in `spec-models-deploy.md` via CLI `azd ai models delete`. Verify SDK mapping | **Verify** |
| `create_async` | `project_client.models.create_async(...)` | `POST /models/{name}/versions/{version}` | LRO variant. May be the actual async path the spec describes (202 + polling). If so, spec's `create_or_update` should map here, not to the sync PATCH | **P0 — Clarify relationship to `create_or_update`** |
| `get_credentials` | `project_client.models.get_credentials(...)` | `POST /models/{name}/versions/{version}/getCredentials` | Not in spec. Likely mechanism to get fresh SAS for downloading weights post-registration | **Document if shipping** |

### In specs but not in SDK

| Operation | Spec Location | HTTP | Notes | Action |
|---|---|---|---|---|
| `listPendingUploads` | `spec-models-register-local.md` | `GET /models/{modelName}/pendingUploads` | In scope per spec | **Add to TypeSpec or de-scope** |
| `getUploadStatus` | `spec-models-register-local.md` | `GET /models/{name}/versions/{version}/uploadStatus` | Already marked de-scoped for Build | **No action — already de-scoped** |

---

## 5. Cross-Cutting Concerns

| # | Area | Current State | Recommendation | Priority |
|---|---|---|---|---|
| 5.1 | API version | Specs pin `api-version=2025-06-01-preview` | Update to target v1 (GA) if contract is stable. Keep `preview` if gaps in this doc are still open | **P1** |
| 5.2 | SDK surface | Operations on `project_client.models.*` | Move all model operations under `project_client.beta.models.*` until contract stabilizes. Signals preview status to users | **P1** |
| 5.3 | `ModelVersion` schema | Generic TypeSpec model — missing managed-compute fields | The TypeSpec `ModelVersion` needs managed-compute extensions: `weightType`, `baseModel` (flat), `source`, `status`, `loraConfig`, `storageUri`, `sizeBytes` | **P0** |

---

## Summary: P0 Items (Must Resolve Before Build)

| # | Gap | Impact |
|---|---|---|
| 2.1 | PUT vs PATCH method mismatch | Breaks idempotent registration contract |
| 2.2 | Async (202) vs Sync behavior | Users can't poll for registration completion if truly sync |
| 2.3 / 2.11 / 3.5 | `weightType` missing from TypeSpec | Cannot distinguish FullWeight / LoRA / DraftModel — breaks multi-model-type workflows |
| 2.4 / 2.12 / 3.6 | `baseModel` flat vs nested | UX regression — `model.derived_model_information.base_model` vs `model.base_model` |
| 3.1 / 3.2 | `source.jobId` list filter missing | Cannot discover models from training jobs — breaks training→deploy workflow |
| 4 (`create_async`) | Relationship between sync PATCH and async POST unclear | May be duplicating or conflicting operations |
| 5.3 | `ModelVersion` schema incomplete | Most spec fields not representable in current TypeSpec |

---

## Proposed Next Steps

1. **Backend team:** Confirm wire-level HTTP methods (PUT vs PATCH) and async behavior (202 vs 200) for `create_or_update`
2. **TypeSpec owner:** Add `weightType`, `baseModel` (flat), `source`, `status`, `loraConfig`, `storageUri` to `ModelVersion` schema
3. **TypeSpec owner:** Add `source.jobId` filter parameter to `list` operation
4. **PM:** Decide on enum values (`TemporaryBlobReference` vs `BlobReference`) and field naming (`blobReference` vs `blobReferenceForConsumption`)
5. **Spec authors:** Update specs to match confirmed wire format (flat vs nested fields, wrapper objects)
6. **SDK team:** Confirm `beta.models.*` surface placement and timeline for promotion to stable
