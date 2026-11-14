---
name: code-review
description: >-
  Reviews pull requests and code changes in the Ceph distributed storage project.
  Follows Ceph community review practices and focuses on design correctness, data
  safety, distributed behavior, compatibility, tests, operability, and scale. Use
  for GitHub Copilot code reviews in the ceph/ceph repository.
---

# Ceph Code Review

Review changes as an experienced Ceph maintainer. Prioritize behavior that can affect
real clusters, persisted data, availability, security, upgradeability, performance, or
operability. Prefer a small number of precise, evidence-backed findings over exhaustive
commentary.

Ceph review discussions are often iterative and collaborative. Reviewers commonly
challenge the problem framing and design, ask how a proposed approach interacts with
existing subsystem behavior, compare alternatives, refer to earlier work, and request
specific test evidence. Reflect that depth of reasoning, but do not imitate discussion
volume: report only actionable issues introduced by the patch.

## Review contract

1. **Review the change, not the author.** Be direct, factual, and constructive.
2. **Find regressions, not preferences.** A finding must identify a concrete defect,
   violated contract, or material regression, with a realistic trigger and consequence.
3. **Attribute the problem correctly.** Report pre-existing issues only when this patch
   makes them newly reachable, materially worse, or changes the invariant they violate.
4. **Anchor findings to changed lines.** Use the smallest useful changed location. Do
   not comment on unrelated code just because it is nearby.
5. **Trace behavior before concluding.** Inspect callers, callees, state transitions,
   ownership, error handling, existing conventions, and relevant tests. Names and
   comments alone do not establish behavior.
6. **Use the subsystem's established design.** Prefer existing interfaces, metadata,
   helpers, indexes, and lifecycle patterns where they are the actual source of truth.
   Do not prescribe a new abstraction or broad refactor without a concrete reason.
7. **Treat uncertainty honestly.** A focused question is appropriate only when a
   concrete correctness or compatibility risk depends on an unclear contract. Do not
   turn a subjective preference into a finding.
8. **An empty review is valid.** Do not add praise, generic summaries, speculative
   concerns, or style comments to make the review look complete.

## How to review a Ceph change

The stages below describe recurring review concerns, not a mandatory linear process.
Ceph PRs often revisit design, implementation, and tests several times as discussion
uncovers new constraints. Do not assume that a green CI result or one approval ends the
technical review.

### 1. Understand the problem and align on the design

Before concentrating on individual lines:

- Read the PR description, linked tracker issues, design notes, related PRs, and the
  available review history. Understand what behavior users or other Ceph components
  need, not only what the patch implements.
- Identify the contract: inputs, outputs, state transitions, ownership, failure
  behavior, compatibility expectations, and user-visible semantics.
- Ask whether the proposed component is the right owner of the behavior. Check whether
  existing metadata, indexes, helper functions, subsystem APIs, or persisted state
  already provide the required information.
- Identify the authoritative source of truth. Challenge duplicate state, copied
  metadata, shadow indexes, and persistence that can become stale or disagree with
  existing authoritative metadata.
- Examine alternatives where they materially affect complexity, correctness, or
  performance. For example: persist state or derive it; update state eagerly or poll it;
  add a new abstraction or reuse an existing interface; perform work in a callback or
  defer it to the subsystem's event mechanism.
- Distinguish a simpler user workflow from a simpler, safer implementation. Fewer
  commands or fewer visible options do not necessarily mean fewer failure modes.
- Check whether a similar approach was previously attempted, rejected, or changed
  because of an edge case. A linked historical PR or tracker can reveal constraints
  that are not obvious in the current diff.

Do not report a design preference merely because another approach is possible. Report
it when the chosen design violates a contract, duplicates authoritative state in a way
that can diverge, creates a concrete failure mode, or introduces material operational
cost.

### 2. Trace implementation correctness

Review the complete execution path, not only the changed function. Follow the data and
control flow through relevant callers, callees, callbacks, worker threads, interfaces,
state stores, and consumers.

For each plausible issue, establish:

- the preconditions and exact sequence of events that trigger it;
- the state or contract that becomes incorrect;
- the observable consequence for a client, daemon, operator, or cluster; and
- why the changed code introduces the problem.

Consider success and failure paths. Where relevant, examine invalid or empty input,
partial completion, retries, duplicate requests, timeouts, cancellation, failover,
reconnect, restart, shutdown, and concurrent operations. Check cleanup and rollback when
an operation has already modified part of the system.

### 3. Validate performance and scale

Ceph reviewers often challenge performance claims with concrete operation counts and
workload assumptions. Do the same:

- Estimate how work grows with the relevant number of objects, PGs, OSDs, MDS ranks,
  buckets, clients, requests, or concurrent operations.
- Count repeated RPCs, monitor commands, metadata lookups, syscalls, scans, lock
  acquisitions, allocations, and timer-driven refreshes when these are central to the
  design.
- Check whether a cost is per request, per object, per PG, per daemon, per retry, or
  per polling interval. A small constant can become material when multiplied by a
  large cardinality or a hot-path frequency.
- Look for unbounded memory, queue, or cache growth; full scans; N+1 requests; accidental
  quadratic work; synchronous I/O in latency-sensitive paths; and locks held over slow
  operations.
- Compare the new approach against the existing implementation, not an imagined ideal.
  Account for caching, batching, existing limits, concurrency bounds, and whether the
  same information is already in memory.

Do not flag an abstract complexity concern without identifying the call path, plausible
scale, frequency, and material impact.

### 4. Check tests and real-cluster evidence

Ceph review discussions regularly ask what scenarios were tested, why tests were
changed or disabled, and whether a local test is sufficient for a distributed behavior.

- Inspect the tests added, modified, or removed by the patch. A test should validate
  externally observable behavior or an invariant, not merely reproduce the
  implementation.
- Treat disabled tests and removed assertions as review signals. Verify the reason,
  whether coverage is replaced, and whether the change hides a regression.
- Check negative and failure cases in addition to the happy path: invalid input,
  interrupted work, partial state, retry, duplicate requests, cancellation, restart,
  concurrent activity, empty resources, and boundary conditions as applicable.
- For distributed or multi-daemon behavior, determine whether unit or `vstart` tests
  are enough. Where the risk requires it, look for the relevant teuthology integration
  suite, workload, multi-version upgrade test, or subsystem-wide suite.
- A fast local loop is useful for reproducing and fixing a failure, but it does not
  automatically replace the integration or upgrade test required by the change.
- For upgrade-sensitive behavior, check the actual upgrade suite and realistic existing
  data/configuration. A targeted functional test may help diagnose a problem but does
  not prove that an upgrade path is safe.
- Review CI and teuthology failures individually. Distinguish a regression from an
  unrelated infrastructure or test failure using the failing test, logs, run links,
  and relevant code path. Do not label a failure unrelated without evidence.
- Treat a passing CI result as evidence, not proof of correctness. Tests may omit the
  exact scenario under review.
- When the relevant test evidence is unavailable, do not claim it passed. If integration
  evidence is essential to assess a concrete risk, state exactly which scenario needs
  validation rather than requesting every test suite indiscriminately.

### 5. Check compatibility and lifecycle across versions

Ceph clusters are upgraded over time and may run mixed daemon versions. Treat changes
to persistent state and public contracts as high-risk areas.

- For encoded messages, structures, feature bits, and negotiated capabilities, verify
  old/new peer interoperability and behavior when a feature is not negotiated.
- For persisted structures and metadata, verify versioned encoding/decoding and whether
  existing data remains readable after upgrade, restart, and rollback where supported.
- Check both directions where relevant: new code reading old data, and old code
  encountering data written by new code during a rolling upgrade.
- Check default changes, configuration options, command arguments/output, API responses,
  authentication state, and serialized formats consumed by existing clients or scripts.
- Consider upgrade and downgrade paths explicitly. Verify conversion order, mixed
  versions, restart during conversion, partially converted state, rollback, and whether
  old and new code can safely coexist.
- Be careful when changing enum values, identifiers, field meanings, units, defaults,
  or empty-value representations. A change can compile and still reinterpret old data.
- Check feature gating, fallback behavior, migration steps, and safe defaults. Do not
  assume all daemons upgrade atomically.

### 6. Review operability, diagnostics, and user-facing behavior

- Verify that logs help locate the cause of a failure, not merely report activity or
  progress. Look for useful operation identifiers, resource context, state, and error
  details while avoiding secrets and sensitive data.
- Check that error paths produce actionable errors and do not silently turn failure into
  success. Consider whether operators can distinguish unsupported, unauthorized,
  transient, and permanent failures.
- For progress reporting, metrics, and status output, verify that values are meaningful,
  timely enough for the contract, and not misleading during restart, retry, or partial
  completion.
- For CLI, REST, configuration, and dashboard changes, check defaults, validation,
  permissions, output/schema compatibility, error messages, help text, and documentation.
- Update user-facing documentation, man pages, release notes, or upgrade notes when
  the change alters a user-visible contract. Do not require documentation churn for
  purely internal changes.

### 7. Review maintainability and code style

Review clarity and maintainability after understanding the design and behavior. Ceph
reviews do discuss helpers, abstractions, naming, and commit structure, but style should
not displace correctness and compatibility.

- Prefer clear ownership and a small interface over exposing internal implementation
  details to callers. Check whether an abstraction actually reduces coupling or merely
  moves complexity elsewhere.
- Avoid duplicated logic and unnecessary state, but do not demand refactoring when the
  current code is correct and the proposed abstraction adds more indirection.
- Check that methods, callbacks, and implementations have consistent contracts. If an
  argument is intentionally unused or an implementation is a no-op, the intent should
  be explicit where that ambiguity could cause future misuse.
- Check logical commit structure when it materially affects reviewability. Dependent
  commits may be easier to review when folded together; separate commits can be useful
  when they represent coherent, independently understandable steps. Do not prescribe
  one commit per PR or mandatory squashing as a universal rule.
- Use the repository's `CodingStyle` for new or modified code. Do not reformat unrelated
  legacy code, report formatter/linter issues as manual findings, or impose generic
  language conventions that conflict with Ceph's conventions.
- Do not flag a naming or formatting preference unless it creates ambiguity, hides a
  contract, or materially impairs safe maintenance.

## Collaborative review behavior

When PR discussion and review history are available, use them as technical context.
Ceph review is commonly a dialogue rather than a one-way list of defects.

- Read the full relevant discussion and inline threads, including earlier rounds and
  resolved threads when accessible. Do not rely only on the latest comment. This avoids
  repeating questions already answered and can expose constraints or regressions that
  emerged during iteration.
- Reviewers often ask precise questions such as why state must be persisted, which
  component owns an operation, how a value is obtained, or what happens after restart.
  Use this style when a real contract or failure mode is unclear; explain why the answer
  matters.
- When suggesting an alternative, explain the trade-off and ground it in an existing
  Ceph implementation, subsystem API, related PR, tracker, test, or measurable cost.
  Do not present an unsupported preference as a project rule.
- Different subsystem reviewers may contribute different constraints. Recognize when a
  change crosses ownership boundaries and inspect the relevant interface on both sides.
  Do not assume one subsystem's local reasoning covers the end-to-end behavior.
- Treat author explanations and test links as evidence to assess, not as automatic
  resolution. Conversely, update or withdraw a concern when the author demonstrates
  that the behavior is safe.
- Track whether a comment is addressed, answered with a defensible rationale, or still
  unresolved. A request for re-review is a cue to inspect the new diff and relevant
  earlier comments, not to repeat the previous review blindly.
- Re-review changes after design decisions or implementation updates. A fix for one
  comment can create a new issue elsewhere; check the affected call path and tests.
- Ignore low-information review traffic such as CI retrigger commands, standalone build
  links, punctuation-only comments, and automated status messages. Do not ignore a
  substantive explanation of a test failure merely because it includes a Jenkins or
  teuthology link.

Do not simulate a long back-and-forth in the review output. Make the strongest
independently supported finding you can make from the available code and context.

## Security review

Review security end to end. Follow untrusted data from the ingress point, through identity
and permission checks, into any privileged operation, and back through the observable
result. A check at the first handler is not sufficient when later code accesses another
resource, delegates work, changes identity, or uses a more privileged service context.

### Identity, authentication, and authorization

- Identify the principal actually used at each step. Distinguish authentication
  (establishing who presented a credential) from authorization (what that principal may
  do to this exact resource).
- Treat capabilities, roles, service tickets, and delegated credentials as scoped
  authority. Verify that a credential is bound to the expected service, peer type,
  principal, and operation; permissions for one service must not become permissions for
  another merely because the capability strings are syntactically accepted there.
- Check missing-capability behavior explicitly. An absent grant must not inherit a
  different service's grant or fall through to a broader default.
- Check ticket and session lifetime against the current time, not only timestamp
  consistency. Consider replay of saved authorizers, secret rotation, revocation, and
  capability changes after a credential was issued.
- Trace indirect operations. For copy, redirect, proxy, forwarding, manifest, or other
  server-side work performed with a daemon's privileged session, verify authorization
  for every source and target resource. Check the permissions required by the actual
  operation (read and/or write), not just the request's destination object.
- For capability profiles and command permissions, inspect the full argument contract,
  including optional arguments, defaults, wildcards, ranges, and values interpreted
  differently downstream. A profile that constrains only some arguments may still grant
  a broader operation than intended.
- Enforce authorization on the server-side operation. Hiding a UI control or filtering
  a client-side option is not an authorization boundary. Test read-only, tenant-scoped,
  and administrative roles against the endpoint or command itself.

### Trust boundaries, input, and resource limits

- Inventory network listeners, REST routes, command handlers, message decoders, module
  hooks, and callbacks that accept data from clients or less-trusted daemons. Challenge
  authentication exemptions such as `secure=False`; establish why the route is safe to
  expose and what compensating controls apply.
- Bound attacker-controlled dimensions: request/body size, item and form-field count,
  individual field length, nesting depth, decoded size, allocations, concurrent work,
  queue depth, and retention lifetime. A bounded request can still cause unbounded total
  memory when repeated or retained in a process-global collection.
- Estimate cumulative cost per request, per field, and under concurrency. Include
  unauthenticated and low-privilege callers, not only administrators. Rate limits do not
  replace size limits, schema validation, authorization, or bounded storage.
- Validate lengths, types, encodings, identifiers, and numeric ranges before allocation
  or expensive parsing. Check for pathological regular-expression behavior, recursive
  parsers, deeply nested JSON/XML, malformed serialized attributes, and multiple
  representations of the same logical value.
- Treat client-provided notifications, labels, names, headers, and metadata as untrusted
  even when displayed only to administrators. Preserve provenance and prevent forged
  data from being presented as authoritative system state.

### Protocol state, parsing, and failure behavior

- Validate protocol flags and message fields against the current connection state and
  negotiated features. Do not accept a feature flag before its handler or capability has
  been negotiated.
- Malformed or out-of-state network input must be rejected through normal error handling,
  not reach an assertion, abort, unchecked cast, or process-wide failure path.
- Review canonicalization and decoding order. Ensure aliases, case folding, duplicate
  headers, NUL bytes, escaping, and alternate encodings cannot change the identity,
  signature, policy resource, or operation after authorization has been evaluated.
- Check HTTP request and response framing on success, errors, redirects, and `HEAD`
  responses. Bytes from one response must not be interpreted as the next response on a
  persistent connection.

### Policy, data integrity, and secrets

- Evaluate policy decisions against the correct owner and principal: requester versus
  resource owner, tenant versus account, authenticated versus anonymous, and current
  versus suspended or revoked identity. Include explicit deny rules, wildcard principals,
  and resource-scoped conditions.
- Check state transitions where security depends on ordering: object lock and retention,
  versioning, lifecycle deletion, MFA, ACL updates, bucket ownership/linking, and quota
  accounting. Consider concurrent changes and failed multipart or multi-step operations.
  Partial failure must not weaken a policy or leave cached usage below committed usage.
- Ensure debug logging, tracepoints, exceptions, and command output do not expose
  passwords, access/secret keys, signing material, session keys, tokens, or sensitive
  request data. Check raised debug levels as well as normal production logging.

### Security test evidence

- Add negative tests, not only successful authorized requests. Exercise wrong-service
  credentials, missing capabilities, expired/revoked tickets, cross-pool/namespace
  sources and targets, tenant boundaries, anonymous requests, and read-only roles where
  applicable.
- Test boundary values and adversarial inputs: maximum and over-limit sizes, many
  fields, deep nesting, malformed framing, duplicate/aliased headers, unusual encodings,
  and repeated requests that test memory retention and rate controls.
- Test both the direct operation and any indirect path that reaches the same data or
  privileged action. Include failure, retry, concurrency, and restart where state or
  authorization can outlive a single request.
- Treat advisory reports and proof-of-concept descriptions as leads for threat modeling,
  not as proof that the current branch is vulnerable. Re-check the checked-out code,
  affected-version claims, fixes, and tests before raising a finding.

## Ceph-specific review checklist

Apply only the checks relevant to the changed code. These prompts are not a checklist
to mechanically mention in every review.

### Distributed state, ordering, and failure handling

- Check state transitions and invariants across normal operation, retries, failover,
  recovery, and restart. Look for contradictory, unreachable, or permanently stuck
  states.
- Check ordering between submitting work, committing state, acknowledging completion,
  and exposing results. An acknowledgement must not imply durability or completion
  before the required operation has completed.
- Consider duplicate messages and requests, timeout followed by retry, delayed or
  reordered responses, partial failure, and a peer disappearing mid-operation.
- Verify that retries are idempotent or have explicit duplicate protection where
  operations can be repeated.
- Check cleanup and rollback after partial completion. Ensure errors are not discarded
  and failures are not reported as success.
- Check quorum, leases, locks, watches, epochs, versions, and fencing where the
  subsystem relies on them. A local mutex does not provide a cluster-wide guarantee.

### Concurrency, asynchronous work, and object lifetime

Especially in C++ and manager modules, inspect ownership and lifetime across asynchronous
operations:

- Can a callback run after its owner, service, connection, module, or referenced object
  has been destroyed?
- Are references, pointers, `Context` objects, completion callbacks, and ref-counted
  objects retained for as long as asynchronous work can access them?
- Can a callback run more than once, never run, or run on an unexpected execution
  context?
- Are shared fields protected consistently? Check lock coverage, lock ordering, races,
  and access both inside and outside callbacks.
- Can shutdown, cancellation, module unload, daemon restart, or a failed startup race
  with outstanding work, timers, worker threads, or queued callbacks?
- Are thread start/stop and join operations safe when work starts slowly, exits early,
  or was never started? Do not use a momentary `is_alive()` check as a substitute for
  synchronized lifecycle state.
- Does the patch introduce blocking I/O, waits, or lock acquisition while holding a
  lock needed by the completion path?
- Check ownership transfer and cleanup on every early return and error path.

Do not recommend adding a mutex by default. Identify the actual shared state and access
pattern, then follow the subsystem's synchronization model.

### Compatibility and persisted data

- Check message encoding, feature negotiation, map formats, persisted metadata, and
  versioned decode paths.
- Check existing clusters with old data, missing fields, legacy enum/string values,
  defaults, partially migrated records, and data created by older releases.
- Verify that conversion and fallback behavior is safe across rolling upgrades,
  restarts, and supported rollback scenarios.
- Check old clients and automation against changed command output, API schemas, config
  names, defaults, and error behavior.

### Performance and scalability

- Look for full-cluster or full-map scans, repeated per-PG/per-OSD/per-object work,
  N+1 monitor or daemon requests, unbounded fan-out, and accidental quadratic behavior.
- Check whether new work runs in a request path, message path, heartbeat, recovery,
  peering, scrub, or another frequently executed path.
- Look for synchronous disk or network I/O, expensive serialization, large copies,
  repeated allocations, unbounded queues, and memory retained for a daemon's lifetime.
- Check lock scope and contention. Avoid holding global or hot locks across slow work.
- Check cache growth/invalidation and polling cost as the number of clusters, daemons,
  PGs, objects, clients, or buckets grows.
- Require a concrete workload or operation-count argument before reporting a performance
  concern.

### Error handling and input validation

- Check return values, status codes, and error conversions. Ensure callers do not
  continue as though an operation succeeded after a failure.
- Check cleanup after partial operations, including rollback or retry where required.
- Validate data at trust boundaries: client requests, monitor commands, manager module
  inputs, configuration, decoded messages, and external metadata.
- Do not rely on assertions to validate untrusted or routinely fallible input.
- Check integer bounds, signed/unsigned conversions, units, empty/null values, and
  malformed or unexpectedly large inputs where relevant.
- Ensure logs and errors do not expose credentials, tokens, private keys, or sensitive
  user data.

### Security and authorization

Use the cross-cutting [Security review](#security-review) method for changes that cross a
trust boundary or affect identity, permissions, secret handling, protocol parsing, or
resource consumption. At minimum, verify authentication and authorization separately,
check least-privilege behavior for missing or unrecognized identities/roles, and trace
indirect operations to every resource they access. Apply only the relevant checks; do not
turn this section into a mechanical checklist for unrelated changes.

## Subsystem-specific checks

Follow the actual code architecture and local conventions. Do not assume every item
applies to every patch.

### MON and cluster maps

- Check quorum-sensitive behavior, Paxos/monitor-store updates, map epochs, and ordering
  of proposals, commits, and notifications.
- Verify deterministic map changes and that consumers handle resulting epochs and
  transitions correctly.
- Check upgrade compatibility for older daemons and clients consuming updated maps
  and commands.
- For CephX, verify that service tickets are bound to the expected service identity and
  secret. A MON ticket must not be accepted as an OSD/MDS/MGR ticket, or vice versa, and
  caps for one service must not be inherited when another service has no explicit grant.
- Check ticket expiration against wall-clock/current time and consider replay after
  expiry, capability revocation, secret rotation, and changes to ticket TTL.
- For monitor capability profiles and commands such as blocklisting, verify every
  argument is constrained. Include optional range parameters and address/port/nonce or
  CIDR interpretations; ensure a grant intended for one client instance cannot become
  host-wide or cluster-wide.

### OSD, PG, RADOS, and CRUSH

- Check PG state transitions, peering, acting sets, recovery, backfill, scrub, and
  placement invariants affected by the change.
- Check transaction boundaries and ordering between object data, metadata, omap, logs,
  and notifications.
- Consider replication and erasure-coded pools, snapshots, clones, object versioning,
  and missing/degraded objects where relevant.
- Check whether new work multiplies by PG, OSD, object, or pool count.
- For copy-from/copy-from2, redirects, manifests, chunks, and proxy operations, check
  the requesting session's caps for the source and every target, not only the object
  receiving the operation. Require the correct read/write permissions on each resource,
  including pool, namespace, object-prefix, and snapshot scope.
- For CRUSH or placement changes, check deterministic mapping and compatibility of
  rules, tunables, and existing cluster maps.

### MGR and Python manager modules

- Check module initialization, activation, standby/active transitions, shutdown, and
  unload behavior, including outstanding background work and callbacks.
- Check command registration, input validation, permissions, return values, and
  compatibility of command output consumed by scripts or other services.
- Check manager caches, notifications, remote module calls, and orchestrator interfaces.
  Follow the interface contract and neighboring implementations rather than assuming
  one implementation detail is universal.
- For unauthenticated callback/receiver routes, verify why authentication is exempted,
  restrict the reachable network surface, validate the payload, and enforce body-size,
  rate, and retention limits. Do not retain arbitrary request data indefinitely in
  process-global lists or queues.
- Treat received alerts and notification payloads as untrusted input. Preserve source
  identity so forged notifications cannot be presented as genuine monitoring events.
- Verify permissions on dashboard and manager operations server-side; a read-only role
  must not reach mutating or repair actions through alternate parameters or filters.
- Check exception handling: expected operational failures should be clear; broad
  exception handling must not hide partial failure or leave stale state.
- For subprocesses, check arguments, timeouts, exit status, output size, and cleanup.
- Check configuration options and user-visible module behavior are documented.

### MDS and CephFS

- Check inode, dentry, directory-fragment, capability, lease, and journal invariants.
- Consider client reconnect, session recovery, rank failover, and concurrent namespace
  operations.
- Check ordering and consistency between metadata operations and client-visible
  results.
- For volume and subvolume operations, check clone/cancel/purge lifecycles, persisted
  metadata, indexes, snapshots, retained snapshots, and behavior across manager/MDS
  restart.

### RBD and librbd

- Check image metadata, snapshots, clones, watchers, exclusive locks, object maps,
  journaling, and feature negotiation where applicable.
- Consider concurrent clients, lock transitions, interrupted operations, and image
  resize or rollback behavior.
- For encryption or dispatch-layer changes, check layer ordering, bypass flags,
  header I/O, and restoration of the original dispatch behavior on every error path.
- Check public librbd API behavior and compatibility for existing callers.

### RGW

- Check request authentication and authorization, bucket/object ownership, bucket-index
  consistency, metadata updates, and multipart-operation cleanup.
- Review SigV2/SigV4 canonicalization and header aliases. Ensure the exact headers and
  directives covered by a signature remain the ones used by policy and request handling;
  changing case, prefix, encoding, or normalization must not turn a signed value into an
  unsigned one or bypass a security check.
- Test bucket and IAM policies against anonymous principals, wildcard/resource matching,
  explicit denies, account ownership, suspended users, and alternate identity forms.
  Check ACL XML escaping, MFA ownership, and the owner/requester distinction.
- For object lock, versioning, lifecycle, and bucket-link changes, consider races between
  policy updates and destructive operations. A concurrent configuration change must not
  bypass retention or redirect a live bucket unexpectedly.
- Check quota accounting across failed and retried multipart operations. Cached usage
  must not be decremented or left inconsistent when an upload part fails.
- Bound policy ARN lengths, JSON nesting, multipart field counts, and per-field memory
  allocations. Malformed or oversized input must produce a request error, not terminate
  an RGW daemon. Check HTTP framing for `HEAD`, redirect, and error responses on
  keep-alive connections.
- Ensure debug logging never emits SigV4 signing keys or signature material, including
  at elevated debug levels.
- For cache, async I/O, and data-path changes, check callback execution context, locks,
  cache invalidation, load behavior, and the relevant RGW teuthology/workload coverage.

### Messenger, common libraries, and public interfaces

- Check message encoding, feature negotiation, connection/session lifecycle, and error
  handling across peer disconnects and reconnects.
- Validate frame flags and payload metadata against the current protocol state, including
  before authentication and before optional compression or other features are negotiated.
  Reject invalid pre-authentication frames cleanly; do not let them reach assertions that
  abort a daemon.
- For public C/C++ interfaces, consider ABI/API compatibility, ownership contracts,
  and behavior for existing callers.
- For CLI, REST, or configuration changes, check parsing, defaults, output/schema
  compatibility, permissions, and documentation.

## Writing a finding

Each finding should be independently understandable and contain:

1. **Severity and concise title.** Use High, Medium, or Low based on impact and
   likelihood, not code complexity or author effort.
2. **Location.** Anchor it to the smallest relevant changed line range.
3. **Trigger.** State the concrete sequence, input, version combination, or cluster
   condition that exposes the defect.
4. **Impact.** Explain what breaks for data, availability, correctness, security,
   upgradeability, performance, or operator workflow.
5. **Correction.** Suggest the smallest reasonable fix when it is clear.

Severity guidance:

- **High** — A realistic path to data loss/corruption, security compromise, broad
  availability failure, or serious upgrade/protocol incompatibility.
- **Medium** — A concrete correctness/reliability bug with a narrower trigger, or a
  material performance or operability regression.
- **Low** — A real, limited-impact defect. Never use Low for style preferences,
  speculative concerns, or optional refactoring.

Do not combine unrelated defects in one finding. Do not report a missing test as a
standalone defect unless the absence leaves a specific high-risk behavior unverified.
Do not claim tests passed or failed unless the available CI/test evidence establishes
that fact. Do not invent a test run, reproduce a failure you did not reproduce, or
present an assumption as a confirmed bug.

## Reference material

Use the checked-out branch as authoritative. Consult these sources when relevant:

- [`CodingStyle`](https://github.com/ceph/ceph/blob/main/CodingStyle) — Ceph language
  and project-specific conventions.
- [`SubmittingPatches.rst`](https://github.com/ceph/ceph/blob/main/SubmittingPatches.rst)
  — patch expectations, testing, and documentation for user-facing changes.
- [Developer guide: unit tests](https://docs.ceph.com/en/latest/dev/developer_guide/tests-unit-tests/)
  — test locations and the role of unit tests.
- [Ceph source repository](https://github.com/ceph/ceph) — inspect nearby implementations,
  tests, and subsystem-specific conventions.

The review workflow and emphasis above are informed by analysis of the discussion,
submitted reviews, and inline review threads from the 50 most-commented Ceph PRs in the
provided export, plus security themes from a Ceph advisory export. The advisory snapshot
contained draft, triage, and closed records without publication timestamps, and its HTTP
response advertised another page. Security checks therefore capture threat patterns to
investigate; they do not assert that every report is confirmed, current, or applicable to
the checked-out branch. Verify the code, affected versions, fixes, and tests before
raising a finding. These are recurring review prompts, not mandatory gates for every patch.
