# Ceph Dashboard Review Standards

Base path: `src/pybind/mgr/dashboard/`

---

## Frontend

### Reuse shared components

| Need | Use | Path |
|------|-----|------|
| Data list | `cd-table` + `cd-table-actions` | `shared/datatable/table/` |
| Key/value details | `cd-table-key-value` | `shared/datatable/table-key-value/` |
| Multi-step wizard | `cd-tearsheet` + `cd-tearsheet-step` | `shared/components/tearsheet/` |
| Page header | `cd-page-header` | `shared/components/page-header/` |
| Modals | `cds-modal` via `ModalCdsService` | `shared/services/modal-cds.service.ts` |
| Delete/confirm | `DeleteConfirmationModalComponent`, `ConfirmationModalComponent` | `shared/components/*-modal/` |
| Form footer | `cd-form-button-panel`, `cd-submit-button`, `cd-back-button` | `shared/components/` |
| Icons | `cd-icon` + `Icons` enum | `shared/components/icon/`, `shared/enum/icons.enum.ts` |
| Help | `cd-helper`, `cd-doc` | `shared/components/helper/`, `doc/` |

**Block:** bespoke data `<table>`, custom modals, direct `HttpClient` in feature components, hand-rolled multi-step forms.

### Carbon Design System (CDS) first

- Use `carbon-components-angular`: `cds-modal`, `cds-text-label`, `cds-select`, `cds-checkbox`, `cds-combo-box`, `cds-tabs`, `cds-loading`, `cds-grid`
- Use utility classes: `cds--type-heading-03`, `cds-mt-3`, `cds-mb-3`
- Use tokens: `var(--cds-border-subtle-01)`, `var(--cds-text-secondary)`
- New modals extend `BaseModal`; open via `ModalCdsService`

**Block:** new `NgbModal` / legacy `cd-modal`, Bootstrap grid for new UI, inline `style=""`, Font Awesome instead of `cd-icon`.

### CSS / SCSS

**Default:** no new `.scss` rules.

Priority: (1) Carbon APIs/utilities → (2) global `frontend/src/styles/vendor/_variables.scss` / `_style-overrides.scss` → (3) component SCSS only with a **comment explaining the Carbon limitation**.

**Block:** SCSS for margins/padding/fonts Carbon already provides; hard-coded colors; duplicate shared-component styles.

### API & state

- HTTP in `shared/api/*` extending `ApiClient`
- `@cdEncode` on services; `toStringEncoded()` for path params
- `getVersionHeaderValue()` for versioned endpoints
- Mutations: `TaskWrapperService.wrapTaskAroundCall()`
- Lists: `ListWithDetails`, `CdTableColumn[]`, `CdTableAction[]`, permission-gated actions

### Forms

- `CdFormGroup`, `CdValidators` (not ad-hoc `Validators.pattern`)
- `cd-form-button-panel` for footers
- CRUD: `FormField.key` must match POST/PUT body keys

### i18n

- TS: `$localize\`...\``
- HTML: `i18n`, `i18n-aria-label`, `i18n-title`
- Action labels: `ActionLabelsI18n` from `shared/constants/app.constants.ts`
- One translatable unit per `i18n` block

### Frontend tests

| Change | Test |
|--------|------|
| Utils, pipes, validators, services | Jest `*.spec.ts` |
| Forms, wizards, conditional UI | Component spec |
| User flow | Cypress e2e (if area has coverage) |
| Bug fix | Regression test |

---

## Backend

### Controller vs service

**Controllers** (`controllers/`): routing, permissions, delegate to services, serialize, document.

**Services** (`services/`): code that talks to Ceph or external daemons and
returns dashboard-ready data. Includes:
- Cluster commands (`CephService.send_command`, mgr maps)
- Domain logic per feature (`rbd.py`, `cephfs.py`, `orchestrator.py`, …)
- External API clients (`rgw_client.py`, `nvmeof_client.py`, …)
- Error wrapping (`DashboardException`, `handle_*_error` context managers)
Controllers call services; they do not contain multi-step Ceph logic,
data transformation, or raw rados/rbd exception handling.

**Block:** substantial Ceph/orchestrator logic in controllers.

References:
- `controllers/auth.py` — thin REST controller; delegates to `services/auth.py`; `@EndpointDoc` + OpenAPI schemas
- `controllers/rbd.py` — REST + `RbdService`; `handle_*_error` decorators; `@UIRouter` UI companion (`RbdStatus`)
- `controllers/cephfs.py` — REST + `services/cephfs.py`; `@EndpointDoc` schemas; `@UIRouter` UI helpers

### Routing & permissions

| Decorator | URL | Use |
|-----------|-----|-----|
| `@APIRouter('/path', Scope.X)` | `/api/...` | Public REST |
| `@UIRouter('/path', Scope.X)` | `/ui-api/...` | UI-only helpers |
| `@Router('/path')` | root | Internal |

**Require:** `Scope`, `@ReadPermission` / `@CreatePermission` / `@UpdatePermission` / `@DeletePermission`, `@Task(...)` for HTTP 202 ops.

### OpenAPI

On `@APIRouter` endpoints:

- `@APIDoc("Description", "Tag")` on class
- `@EndpointDoc("Summary", parameters={...}, responses={...})` on methods

```python
"pool_name": (str, "pool name"),
"options": ({"k": (int, "chunks")}, "options dict"),
```

**Block:** new/changed public endpoints without docs; API change without `openapi.yaml` update (`tox -e openapi-fix` then `openapi-check`).

### Errors

- `DashboardException` with correct HTTP codes
- `SecretStr` for sensitive CRUD fields
- No bare exceptions leaking internals

### Backend tests

| Change | Test |
|--------|------|
| Logic | `tests/test_*.py` (`tox -e py3`) |
| Public endpoint | Unit test + OpenAPI |
| Cluster integration | `qa/tasks/mgr/dashboard/test_*.py` |

---

## Severity guide

| Level | Examples |
|-------|----------|
| **blocker** | Missing permissions, wrong router, no `@EndpointDoc`, bespoke table vs `cd-table`, direct `HttpClient`, missing i18n, logic in controller |
| **should-fix** | No `TaskWrapperService`, no component test for complex form, SCSS without justification |
| **suggestion** | Could use `cd-table-key-value`, extract to shared component |
| **nit** | Naming, consistency with nearby legacy code |

---

## Key paths

```
frontend/src/app/shared/components/   # reusable UI
frontend/src/app/shared/datatable/    # cd-table, crud-table
frontend/src/app/shared/forms/        # CdValidators, crud-form
frontend/src/app/shared/api/          # HTTP services
controllers/                          # HTTP layer
controllers/auth.py                   # thin controller + service delegation
controllers/rbd.py                    # REST + domain service + UI router
controllers/cephfs.py                 # REST + cephfs service + UI helpers
controllers/rgw.py                    # external clients + CRUD endpoints
services/                             # business logic
tests/                                # Python unit tests
qa/tasks/mgr/dashboard/               # API integration tests
openapi.yaml                          # generated API spec
```
