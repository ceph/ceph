---
name: dashboard-review
description: "Review Ceph Manager Dashboard code (frontend and backend) against team standards: shared Angular components, Carbon/CDS patterns, minimal local SCSS, controller/service separation, OpenAPI docs, and test coverage. Use when the user invokes /dashboard-review, asks for a dashboard code review, or wants merge-readiness feedback on dashboard PRs."
disable-model-invocation: true
---

# Dashboard Review

Bob review-only skill for [`src/pybind/mgr/dashboard/`](src/pybind/mgr/dashboard/). Do not edit files unless the user explicitly asks to fix findings.

## Purpose

Use this skill to review dashboard changes for:
- correctness and regressions
- frontend pattern compliance
- backend controller/service separation
- API contract and OpenAPI coverage
- test coverage, i18n, and accessibility

Primary standards reference: [`src/pybind/mgr/dashboard/STANDARDS.md`](src/pybind/mgr/dashboard/STANDARDS.md)

## Bob operating rules

- Stay in review mode. Do not modify code, tests, docs, or configuration unless the user explicitly asks for fixes.
- Use Bob tools step by step and wait for confirmation after each tool call.
- Prefer read-only inspection tools first:
  - `obtain_git_diff`
  - `read_file`
  - `search_files`
  - `list_code_definition_names`
- Use `ask_followup_question` only if scope cannot be inferred.
- Use `submit_review_findings` for concrete issues that should appear in the Bob findings panel.
- After submitting findings, provide a concise human-readable summary with `attempt_completion`.
- Flag issues in changed code only, unless the change extends or reinforces a legacy anti-pattern.

## Recommended mode

Use this skill from Ask or Advanced mode when the user wants review feedback without edits.

## Scope selection workflow

Determine scope in this order:

1. If the user names specific files, review only those files and directly related companions.
2. If the user references a branch, PR, or comparison target, use `obtain_git_diff` against that branch and limit attention to [`src/pybind/mgr/dashboard/`](src/pybind/mgr/dashboard/).
3. If no explicit scope is given, review current dashboard changes using `obtain_git_diff` for local changes.
4. If scope is still ambiguous, ask one focused follow-up question. Otherwise default to all dashboard changes in the current work.

## Context gathering workflow

After scope is known:

1. Inspect the diff with `obtain_git_diff`.
2. Read changed dashboard files with `read_file`.
3. Read directly related companion files when needed, such as:
   - frontend specs: `*.spec.ts`
   - backend tests: `tests/test_*.py`
   - Cypress coverage for affected user flows
   - shared API services under `frontend/src/app/shared/api/`
   - Angular routing/module files if declarations or navigation changed
   - [`src/pybind/mgr/dashboard/openapi.yaml`](src/pybind/mgr/dashboard/openapi.yaml) when public API controllers change
4. Use `search_files` or `list_code_definition_names` to compare nearby established patterns when the correct dashboard pattern is unclear.
5. Do not broaden into unrelated legacy cleanup.

## Optional verification workflow

Run verification commands only if the user asks for them or if the review explicitly needs command evidence.

Preferred commands:
```bash
cd src/pybind/mgr/dashboard/frontend && npm run lint && npm run test:ci
cd src/pybind/mgr/dashboard && tox -e lint,py3
cd src/pybind/mgr/dashboard && tox -e openapi-check
```

Rules:
- Use `execute_command` one command at a time.
- Report command failures as findings.
- Do not fix failures unless the user asks for implementation help.

## Review checklist

Apply all relevant rules from [`src/pybind/mgr/dashboard/STANDARDS.md`](src/pybind/mgr/dashboard/STANDARDS.md).

### Frontend checks

- Shared components are used where applicable:
  - `cd-table`
  - `cd-table-actions`
  - `cd-table-key-value`
  - `cd-tearsheet`
  - `cd-page-header`
  - `cds-modal` via `ModalCdsService`
- No bespoke data tables, custom modal patterns, or direct `HttpClient` inside feature components.
- New UI follows Carbon/CDS patterns and utilities.
- New local SCSS is avoided unless justified by a comment explaining the Carbon limitation.
- Mutations use `TaskWrapperService.wrapTaskAroundCall()`.
- HTTP access lives in shared API services extending `ApiClient`.
- Forms use `CdFormGroup`, `CdValidators`, and matching `FormField.key` values.
- New user-visible strings use `$localize` or HTML i18n attributes.
- New actions use `ActionLabelsI18n` where applicable.
- Accessibility is covered for new UI, including translated aria/title attributes where needed.
- Tests match the change type:
  - service/util/validator logic -> Jest spec
  - forms/wizards/conditional UI -> component spec
  - user flows -> Cypress where the area already has e2e coverage
  - bug fixes -> regression coverage

### Backend checks

- Controllers remain thin and delegate business logic to services.
- Controllers do not contain substantial Ceph/orchestrator logic, heavy transformation, or raw exception handling.
- Router type is correct:
  - `@APIRouter` for public REST
  - `@UIRouter` for UI helpers
  - `@Router` only when appropriate
- Permissions are present and correct for each endpoint.
- Async HTTP 202 operations use `@Task(...)`.
- Public API endpoints include `@APIDoc` and `@EndpointDoc(...)`.
- Public API changes are reflected in [`src/pybind/mgr/dashboard/openapi.yaml`](src/pybind/mgr/dashboard/openapi.yaml).
- Errors use `DashboardException` appropriately and avoid leaking internals.
- Sensitive CRUD fields use `SecretStr` where applicable.
- Tests match the change type:
  - unit tests in `tests/test_*.py`
  - OpenAPI coverage for public endpoints
  - integration coverage in `qa/tasks/mgr/dashboard/test_*.py` when relevant

## Severity mapping

Map issues using the standards severity guide in [`src/pybind/mgr/dashboard/STANDARDS.md`](src/pybind/mgr/dashboard/STANDARDS.md):

- blocker
  - missing permissions
  - wrong router type
  - missing `@EndpointDoc`
  - bespoke table instead of `cd-table`
  - direct `HttpClient` in feature component
  - missing i18n on new strings
  - business logic in controller
- should-fix
  - missing `TaskWrapperService`
  - missing component/spec coverage for complex UI
  - unjustified new SCSS
- suggestion
  - better reuse of shared dashboard patterns
- nit
  - naming or local consistency issues

## Legacy debt guardrail

Do not flag pre-existing legacy debt unless the current change introduces a new instance or expands the anti-pattern.

Examples that should not be flagged unless newly introduced by the change:
- existing `NgbModal` or legacy `cd-modal` in untouched files
- unmodified component SCSS
- existing Formly Bootstrap `form-control`
- older docs terminology that says `@ApiController`

## Findings workflow

When you identify actionable issues:

1. Submit formal findings with `submit_review_findings`.
2. Include:
   - category
   - severity
   - concise title
   - clear explanation tied to dashboard standards
   - file path and exact line
   - suggested fix when possible
3. Submit only findings you can support from the inspected diff and file context.
4. If there are no actionable issues, do not submit empty findings.

## Final response format

After reviewing, provide a concise markdown summary in this structure:

```markdown
# Dashboard Review

**Scope:** [files / branch / local diff]
**Verdict:** ready | ready-with-nits | needs-changes

## Summary
[1-3 sentences]

## Blockers
| File | Issue | Fix |
|------|-------|-----|

## Should fix
| File | Issue | Fix |
|------|-------|-----|

## Suggestions
- ...

## Nits
- ...

## Pre-merge checklist
- [ ] Shared components used
- [ ] Carbon/CDS patterns followed
- [ ] No unjustified local SCSS
- [ ] Shared API services used; mutations wrapped with TaskWrapperService
- [ ] i18n and accessibility covered for new UI
- [ ] Backend logic stays in services
- [ ] Permissions and router type are correct
- [ ] `@EndpointDoc` and `openapi.yaml` updated if API changed
- [ ] Tests added for new logic and user flows
- [ ] Optional lint/test commands reviewed if run
```

Verdict rules:
- `needs-changes`: one or more blockers
- `ready-with-nits`: no blockers, but at least one should-fix, suggestion, or nit
- `ready`: no blockers and no should-fix items

## Invocation examples

- `/dashboard-review`
- `/dashboard-review @src/pybind/mgr/dashboard/frontend/src/app/ceph/block/block.module.ts`
- `/dashboard-review compare against main`
- `/dashboard-review include lint and test failures as findings`

## Reference

Full standards and component map: [`src/pybind/mgr/dashboard/STANDARDS.md`](src/pybind/mgr/dashboard/STANDARDS.md)
