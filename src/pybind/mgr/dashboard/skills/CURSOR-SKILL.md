---
name: dashboard-review
description: >-
  Review Ceph Manager Dashboard code (frontend and backend) against team standards:
  shared Angular components, Carbon/CDS patterns, minimal local SCSS, controller/service
  separation, OpenAPI docs, and test coverage. Use when the user invokes /dashboard-review,
  asks for a dashboard code review, or wants merge-readiness feedback on dashboard PRs.
disable-model-invocation: true
---

# Dashboard Review

Review-only skill for `src/pybind/mgr/dashboard/`. **Do not edit files** unless the user explicitly asks to fix findings.

## How to use

1. **When:** after your feature works and before opening a PR (or after addressing review comments).
2. **Mode:** use **Ask** so the agent reports findings without editing files.
3. **Invoke:** type `/dashboard-review` in chat, optionally with scope:
   - `/dashboard-review` — all dashboard changes on the current branch
   - `/dashboard-review @path/to/file.ts` — specific files only
4. **Run checks first** (optional but recommended); ask the skill to include failures as findings:
   ```bash
   cd src/pybind/mgr/dashboard/frontend && npm run lint && npm run test:ci
   cd src/pybind/mgr/dashboard && tox -e lint,py3
   ```
5. **Act on output:** fix **Blockers** before requesting human review; re-run until verdict is `ready` or `ready-with-nits`.

Full rules: [STANDARDS.md](STANDARDS.md)

## Workflow

1. **Determine scope**
   - User-named files → review those
   - Branch/PR → `git diff <base>...HEAD` limited to `src/pybind/mgr/dashboard/`
   - Uncommitted → `git diff` + `git diff --cached` for dashboard paths
   - If scope is unclear, ask once; default to all dashboard changes on the current branch

2. **Gather context** (read-only)
   - Changed `.ts`, `.html`, `.scss`, `.py` files
   - Related specs: `*.spec.ts`, `tests/test_*.py`
   - If API controllers changed: check `openapi.yaml` diff
   - Skim surrounding unchanged files only when needed to compare patterns

3. **Apply standards**
   - Follow [STANDARDS.md](STANDARDS.md) for all rules and checklists
   - Flag only issues in **changed code** unless the PR extends a legacy anti-pattern

4. **Optional verification** (run only if user asks or scope is large)
   ```bash
   cd src/pybind/mgr/dashboard/frontend && npm run lint && npm run test:ci
   cd src/pybind/mgr/dashboard && tox -e lint,py3
   # API changes:
   tox -e openapi-check
   ```
   Report command failures as findings; do not fix code unless asked.

5. **Produce the review** using the output format below.

## Output format

```markdown
# Dashboard Review

**Scope:** [files / branch / commit range]
**Verdict:** ready | ready-with-nits | needs-changes

## Summary
[1-3 sentences: overall quality and main blockers]

## Blockers
| File | Issue | Fix |
|------|-------|-----|
| ... | ... | Use `cd-table` / move logic to service / add `@EndpointDoc` / ... |

## Should fix
| File | Issue | Fix |
|------|-------|-----|

## Suggestions
- ...

## Nits
- ...

## Pre-merge checklist
- [ ] Shared components used (cd-table, tearsheet, cds-modal, page-header)
- [ ] Carbon/CDS; no unjustified local SCSS
- [ ] API via shared/api services; mutations use TaskWrapperService
- [ ] i18n on new user-visible strings
- [ ] Backend logic in services; permissions + router type correct
- [ ] @EndpointDoc + openapi.yaml if API changed
- [ ] Tests for new logic (unit/component; e2e for user flows)
- [ ] tox -e lint,py3 and npm lint/test:ci (if applicable)
```

**Severity mapping:** blocker → Blockers | should-fix → Should fix | suggestion → Suggestions | nit → Nits

## Review focus (priority order)

1. **Correctness & regressions** — logic bugs, missing error handling, permission gaps
2. **Pattern compliance** — shared components, CDS, controller/service split
3. **API contract** — `@EndpointDoc`, `openapi.yaml`, permissions, `@Task` for async ops
4. **Tests** — missing coverage for new behavior
5. **i18n & a11y** — untranslated strings, missing aria labels on new UI

## Do not flag (legacy debt)

Unless the PR **adds new** instances:

- Existing `NgbModal` / `cd-modal` in untouched files
- Unmodified component `.scss`
- Formly `form-control` (Bootstrap) in existing formly types
- Docs saying `@ApiController` (code uses `@APIRouter` / `@UIRouter`)

## Invocation examples

- `/dashboard-review` — review current branch dashboard changes
- `/dashboard-review @path/to/component.ts` — review specific files
- `/dashboard-review review only, no edits` — explicit read-only (default)

## Reference

Full rules, component map, and checklists: [STANDARDS.md](STANDARDS.md)
