# Intelligent-tiering-design

This document captures the MVP design and outlines planned future work of Intelligent-Tiering (ITE) for RGW, i.e, moving objects between a hot tier and a cold/cloud tier based on how they are actually accessed, instead of LC transition rules set by the admin.

The arc is from individual objects to ML-driven groups: in the MVP, objects are scored and moved based on their own access history; the end goal (Future work) is to automatically discover which S3 objects belong together and how they behave, then tier, restore and pre-warm them as intelligent groups, with no manual tuning.

## Requirement:

- Tier objects based on actual access, not a fixed age window
- Detect periodic access (daily/weekly/quarterly) and pre-warm the object before the next expected access
- Keep hot data on the fast pool, move cold data to cheaper pool or cloud. Transparent to S3 clients, i.e, storage-class as seen by the client never changes
- Should scale to billions of objects with negligible overhead on the request path
- Beyond the MVP (see Future work):
  - Fit the knobs to the workload: named profiles per bucket or object (half-life, cold-after, pre-warm), picked by tag or by the admin
  - Rule-based profile assignment at PUT: pick the profile, and with it the initial placement, from object size/prefix/tag or content-type, (instead of using Lua script)
  - Learn the workload instead of configuring it: cluster objects that behave alike, suggest or apply the matching profile, with admin consent as a config option
  - Treat objects as groups: objects usually read together are tiered, restored and pre-warmed together

## Feature recap:

### Current LC capabilities/limitations:

- LC transition rules work on a fixed age per prefix/tag. No awareness of access pattern or periodicity, eg., a report read every quarter gets transitioned on day N and pays a slow read every cycle
- Scratch/write-once files sit in the hot pool for the whole LC window even if nobody reads them
- Same rule for every kind of data
- Rules need manual tuning as workloads change

### AWS S3 Intelligent-Tiering:

- Client writes with one storage-class INTELLIGENT_TIERING. Object keeps that class for life, S3 moves it between access tiers underneath
- New objects start in Frequent Access. 30 days no access -> Infrequent Access, 90 days -> Archive Instant Access. Opt-in: Archive Access (90-730 days), Deep Archive Access (180-730 days)
- GET/PUT/copy on an object in Infrequent or Archive Instant Access moves it straight back to Frequent Access. The two archive tiers need an explicit restore
- Objects < 128KB are not monitored/tiered. There is a per-object monitoring fee

### Why that is not enough:

- The shape is right (one class, service moves bytes underneath) and ITE keeps it
- The decision logic is not. It is a fixed ladder of days-since-last-access, so same blind spots as LC: no periodicity, no pre-warm, one stray access resets the object to hot, same thresholds for every workload

## Design overview:

![ITE at a glance: one sink, four stores, five background threads, two pools](./intelligent-tiering-images/1-ITE-design-overview.png)

Top to bottom: *the sink is the only step on a live request. Everything below is a background thread that reads one store and writes the next.*

## 1. Storage-class:

- New storage-class INTELLIGENT_TIERING. Set on every object at PUT once ITE is enabled on the bucket.
- Unlike a regular storage-class (one data pool), this one has two placements
  - hot: fast pool, eg., replicated SSD
  - cold: cheaper pool, eg., EC HDD, or a cloud-s3 endpoint
- RGW reads/writes whichever placement the object is currently in. ITE moves objects between the two in the background.
- This two-placement storage-class is the only new RGW plumbing needed. Everything else reuses what is already there
- Defined at zonegroup level like any other storage-class. Each zone creates its own pools for it
- For MVP: exactly two placements with in RGW
  - Later we may add support for multiple placements., for eg., cloud-placement similar to DeepArchive in AWS S3
- For this MVP, LC transition rules and ITE are mutually exclusive on a bucket. Setting one where the other exists -> error.
  - LC expiration still works as-is.
  - Later we could add support to select INTELLIGENT_TIERING storage class via LC rules. See [Future work: Enable ITE via LC rules](#m2xysj7v4j6.1191)

## 2. Access events (access-log sink):

- New ITE sink hooked into the existing ops-log fan out. No new hook on the request path
- On every completed request the sink checks
  - ITE enabled on this bucket? (in-memory cache, no per-request lookup)
  - op is one of GET/PUT/HEAD/DELETE/COPY/CompleteMultipartUpload and has an object name
  - caller is USER
- If yes, appends one event {bucket id, object instance, UTC ts, op, caller} to a sharded append-only queue in a dedicated ITE pool. Shard = hash(bucket, object), so all events of an object land on the same shard
  - This push is the only work ITE does on a live request
- Caller type = who caused the request
  - USER: any client identity, incl. anonymous
  - REPLICATION: multisite system user
  - LIFECYCLE / GC / TIERING: internal threads mark themselves. TIERING = ITE's own pre-warm/restore IO
  - MVP logs only USER events. Rest are dropped at the sink. Logging internal callers later is just a minor change if needed, the field is already in the event
  - User GET that triggers a restore counts as access. Pre-warm done by ITE does not (else object looks hot because of our own prediction)
  - Replication writes never count. A replicated object starts cold in the receiving zone and warms up only on local reads
- COPY and multipart
  - CopyObject = read on the source (like GET) + write on the destination (like PUT). Destination is a new instance
  - Multipart = one event at CompleteMultipartUpload, as PUT. UploadPart is not logged, object doesn't exist yet
  - RestoreObject is not logged. The GET issued after restore is the access, and that is tracked anyway
  - LIST, bucket ops, failed requests: not logged
- Why a new log instead of reusing existing ones
  - datalog / bilog: index changes only, no reads. Trimmed once sync consumes them
  - ops log: 20+ fields, coupled to other uses
  - S3 bucket logging: lightweight mode drops reads, full mode is verbose text that needs parsing
  - ITE event: 5 fields, own retention, not affected by sync trimming

## 3. Aggregator:

- Hourly background thread on every RGW. Turns raw events into per-day counts
- Per raw access event shard (mentioned above)
  - take the per-shard lock (only one RGW works a shard at a time)
  - read events after the last committed marker
  - group in memory by (bucket, instance, day, op, caller). eg., 450 GETs on `report.pdf` on Jan 15 -> one count of 450
  - write counts into a per-bucket, per-day count object (hash-sharded), one count per instance, op and caller
  - counts + marker go in one atomic write. Replay after a crash sees the marker and skips, so no double counting
  - trim the shard. Raw events are kept 48h max
- Retention
  - one object per day per shard. Days older than the window (90d default, 180d max) are deleted as whole objects. No key-by-key trimming, nothing grows unbounded
  - 16 shards x 180 days = 2880 small objects per bucket, max
  - raising shard count only affects new days
- Note: instance here = name + version-id. Counts are keyed by name, so they continue across overwrites. See section 6

## 4. Temperature scorer:

- Daily thread. Gives every object one number, its temperature, for how actively it is used right now. Temperature is an exponentially smoothed daily access rate (weighted accesses per day).
- Discrete EWMA over UTC days, scoring as done in the prototype in [1]
  - `temperature = alpha * Y + (1 - alpha) * temperature_prev`, applied once per day window
  - `Y` = weighted count of the day's USER accesses (weights below). A day with no access just decays the value
  - `alpha = 1 - 2^(-1 / half-life)`, so with no new access the temperature halves every half-life. Default half-life 7 days -> alpha ~0.094
- Sample Weights: GET 1.0, PUT 0.5 (object is alive, but a write is not read pressure), HEAD 0.1, DELETE 0
  - COPY = GET on source + PUT on destination. Multipart complete = PUT
  - no per-caller weight, only USER events reach the scorer
- No history needed. Per object we keep (temperature, timestamp). Scorer decays the stored value to today, applies the latest day's weighted counts, writes it back
- Objects with no access are not rewritten. Decay is a pure function of (stored temperature, stored timestamp, now): `temperature_now = temperature_stored * 2^(-days_since / half-life)`, so the decision engine decays lazily when it reads the entry. No sweep
- Reads only the latest day's count objects. Candidates = objects present in that window, not a bucket scan
- We may reuse the existing decay counter (MDS uses it for load balancing, half-life based), with our own wall-clock timestamp
- Temperature is just a number. Decision engine (section 7) is what calls an object hot/lukewarm/cold
- eg., half-life 7d, alpha 0.094
  - `Ram/checkup.pdf` read once a day: temperature converges to 1.0, hot
  - `Ram/checkup.pdf` read 2, 10 and 20 days ago: 0.094 x (0.82 + 0.37 + 0.14) = 0.125 -> above cool, below warm, lukewarm
  - `scratch_dump.csv` written once 59 days ago, never read: 0.5 x 0.094 x 2^(-59/7) ~ 0.0001, cold
  - A: 1000 GETs on day 0 -> day 10: 94 x 2^(-10/7) = 35, hot. B: 1 GET on day 3 -> day 10: 0.094 x 2^(-7/7) = 0.047, cold. A is hotter than B although B was read more recently

## 5. FFT periodic detector:

- Temperature alone can't tell a quarterly report from a dead scratch file, both are cold most of the time. FFT finds the repeat cycle so we can pre-warm before the next read instead of after
- Weekly thread (a weekly/monthly cycle doesn't change day to day)
- For each object with > 3 accesses in the span
  - build daily count series over the last 90 days (span, per-bucket config)
  - run FFT, pick the strongest frequency above confidence 0.5
  - trust the period only if >= 3 full cycles fit inside the span
  - predicted next access = last access + period. Stored with the object
  - no clear peak -> aperiodic, temperature only
- Span is the main knob. Longest detectable period = span / 3
  - 30d -> daily, weekly. 90d -> upto monthly. 180d -> ~2 months. Quarterly reports need 270d+
  - default 90d keeps the count store small
- No FFT in ceph tree.
- A small self-contained radix-2 routine may be implemented, input is a few hundred numbers per object
- Reads the day objects in order and builds every object's series in one pass. No per-object lookup, no extra index

## 6. ITE state, object lifecycle and evaluation:

### Per-object state:

- Fields: temperature + ts, FFT (periodic flag, period, next access, confidence), tier state (HOT/PSEUDO/COMMITTED) + time entered PSEUDO, commit count, restore count
- MVP: stored on the bucket index entry. Not in S3 tags or user metadata. Visible only to the admin
  - why index entry: scorers and decision engine are batch jobs that need to look at every object cheaply. Bucket listing already does that
- State belongs to the data instance (name + version-id) and restarts on every data write. Access counts belong to the name
  - reshard, ACL/tag update, ITE's own move -> same instance, state carried through
  - overwrite, new version, replication write with new content -> new instance, starts cold
  - no separate discriminator is stored: the data write rewrites the index entry, and that rewrite is the reset. The mover detects a racing overwrite by comparing the entry's mtime with the one recorded at decision time (section 8)

### Object lifecycle:

![object tier states: 3 states, 5 transitions](./intelligent-tiering-images/2-object-tier-states.png)

HOT and PSEUDO keep bytes in the hot placement; only the three mover transitions (COMMIT, RESTORE, PRE-WARM) move bytes.

- Create (PUT / COPY dest / multipart complete): class INTELLIGENT_TIERING, bytes in hot placement, state HOT, no history. Nothing moves for the first 30 days (cold-after) anyway
- Move to cold: age > cold-after, temperature < cool (0.1), no predicted access -> PSEUDO. Stays cold through grace (~half-life) -> COMMIT queued -> mover moves bytes -> COMMITTED
- Overwrite, unversioned bucket: new PUT rewrites the index entry with a fresh ITE block -> starts HOT with an empty history (temperature restarts from the PUT weight). Access counts are keyed by name, so FFT keeps summing across the rewrite and a rewrite cycle is still seen. Old bytes freed by normal RGW GC, nothing new there. Old counts simply age out with retention
- Overwrite, versioned bucket: previous version is its own instance, scored and tiered on its own. Old version nobody reads goes cold while current stays hot. FFT sums counts across version-ids of the name, so a report re-uploaded as new versions still shows its cycle. Any pre-warm it predicts targets the current version (what an un-versioned GET resolves to); old versions are candidates for RESTORE on demand only
- Delete: index entry removed -> ITE fields gone with it, nothing to clean up. Bytes GC'ed from whichever placement. DELETE event has weight 0, logged for audit only. Recreate under the same name is a fresh PUT: same as an overwrite, state starts empty and the name's counts continue after the gap
  - versioned bucket: DELETE without version-id = delete marker only. Data, state, placement untouched. DELETE with version-id removes that instance
  - pending COMMIT/PRE-WARM/RESTORE for a deleted or overwritten instance: mover re-reads the index entry before moving, finds the entry gone or its mtime changed since the decision and silently drops it

## 7. Decision engine:

- Daily thread. Input: temperature, FFT prediction, current state. Output: one action per object

* Writes the two free state changes itself. Queues everything else for the mover. Never moves data
* One set of bucket-level knobs for all objects

| Knob | Default | Meaning |
| --- | --- | --- |
| half-life | 7 days | temperature halves every half-life with no access (section 4). Also sets PSEUDO grace period |
| warm threshold | 0.3 /day | above this the object is hot. ~2 reads a week |
| cool threshold | 0.1 /day | between warm and cool = lukewarm, hold. ~1 read every 10 days. One stray GET (alpha ~0.094) stays below it |
| cold-after | 30 days | min age before an object can be demoted. Residency floor, not a trigger |
| max-age backstop | off | optional. Past this age, demote regardless of temperature |
| pre-warm | on, 7 day lead | restore a periodic object this many days before predicted access |
| min tier size | 4 KiB | smaller objects are never tiered, move costs more than it saves |
| FFT confidence | 0.5 | min peak strength to trust a period |
| FFT span | 90 days | history window for the detector (section 5) |

- Rules, in order checked
  1. size < min tier size -> skip
  2. age < cold-after -> hold
  3. max-age set and exceeded -> demote regardless
  4. temperature > warm -> hold. If COMMITTED and back above warm -> promote
  5. FFT predicts access within pre-warm lead and pre-warm on -> queue PRE-WARM
  6. temperature > cool -> hold (lukewarm)
  7. already PSEUDO and cold for longer than grace -> queue COMMIT
  8. else -> mark PSEUDO
- Temperature is the primary signal. cold-after and max-age only bound it from either side
- RESTORE is not produced by the daily pass. It is queued when a COMMITTED object on cloud is actually read
- ### How objects are evaluated:
  - Option A (MVP): decision engine walks the whole bucket index once a day, one sequential pass. Simple, stateless, reuses the listing. Cost grows linearly with object count, most entries are read only to be held
  - Option B: due-date scheduling. An alternative design. Each evaluation computes the first day the object could change tier (cold-after expiry, grace expiry, predicted access minus pre-warm lead) and writes a schedule entry for that day. Engine visits only due objects, plus anything re-accessed since (re-scheduled early). Daily cost scales with due objects, not total object count. Needs a time-bucketed schedule store and a reconciler for crashes. This is what the external prototype in [1] does
  - Option C: Using S3 inventory to track these objects?
  -

## 8. [Lazy mover:](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html)

- The only component that moves bytes. Background, throttled batches, never on a client request. Same style as LC/GC/Restore worker threads
- Decision engine pushes actions (instance, action, the entry's mtime at decision time) to a sharded move queue in the ITE pool. Mover runs on every RGW, drains the queue under a per-shard lock, acks an action only once the bytes are durably in place. Queue is trimmed only after ack, never on a timer
- Before each move: re-read the object's index entry and compare its mtime with the one the decision engine recorded in the action. Entry gone or mtime different -> the instance was deleted or overwritten since the action was queued -> skip silently, not an error. Nothing younger than cold-after moves to cold either way
- A move = change of placement + tier state flip. Storage-class never changes, so nothing is visible to the S3 client
- Every move is copy -> verify -> delete source. A crash leaves at worst an orphan copy to clean, never lost data
- Reuses existing paths
  - COMMIT: same transition path used today for storage-class / cloud transition
  - RESTORE / PRE-WARM: the existing cloud-tier restore path, i.e, the Glacier style restore that writes the object back into the cluster
  - cold placement (pool or cloud-s3) is assumed configured before ITE is enabled. ITE only moves objects in and out of it
- Only byte move a live request can cause: read-through restore of an object on a cloud placement. Existing path

### Tiering states:

![Tiering states: three states, two free transitions, three byte moves](./intelligent-tiering-images/3-tiering-states-transitions.png)

- HOT: active, bytes in hot placement
- PSEUDO: marked as cold candidate, bytes NOT moved. Grace period derived from half-life. Read again here -> back to HOT for free. Pre-warmed objects land here too
- COMMITTED: confirmed cold, bytes moved to cold placement
- Invariant: HOT and PSEUDO = bytes in hot pool. COMMITTED = bytes in cold. So only three transitions move bytes, and those are the mover's whole job
  - COMMIT: PSEUDO -> COMMITTED (hot -> cold)
  - PRE-WARM: COMMITTED -> hot, ahead of a predicted access
  - RESTORE: COMMITTED -> hot, on real demand
- Who writes state
  - decision engine owns the two free transitions (HOT <-> PSEUDO)
  - mover owns the three byte moves. Writes COMMITTED only after bytes are in cold. Bumps commit/restore counts
- COMMITTED object on a local cold pool: reads served in place and feed temperature. Promoted back to HOT only when temperature crosses warm (sustained reads), one stray read won't bounce it
- COMMITTED object on a cloud endpoint: first read can't be served in place -> restores at once

## 9. Example:

- Bucket `hospital-records`, ITE enabled with defaults. FFT span set to 270d since it holds quarterly reports
- Day 0: upload `patients/Ram/checkup.pdf`, `reports/quarterly_q1.pdf`, `exports/scratch_dump.csv`. All three get INTELLIGENT_TIERING, land in hot pool, state HOT, no history
- Days 1-30
  - doctor opens `checkup.pdf` every day. analyst reads `quarterly_q1.pdf` ~10 times a day in week 1 then stops. nobody opens `scratch_dump.csv`
  - each read -> USER event via the sink. nightly LC scan and sync to the second zone are LIFECYCLE/REPLICATION, dropped
  - hourly aggregator folds events into that day's count objects
  - nightly scorer: `checkup.pdf` ~1.0. `quarterly_q1.pdf` reaches ~5.0 by day 7 and halves every week after, ~0.5 by day 30. `scratch_dump.csv` 0.047 from its PUT, ~0 soon after
  - decision engine holds all three, none is past cold-after yet
- Day 31
  - `checkup.pdf` > warm -> hold
  - `scratch_dump.csv` < cool, no prediction -> PSEUDO. nothing moves
  - `quarterly_q1.pdf` still ~0.5 > warm -> hold. A week of heavy reads takes ~40 days to fade below cool
- Days 38-40
  - `scratch_dump.csv` stayed cold through grace (~1 week) -> COMMIT queued
  - mover copies to cold placement, verifies, deletes hot copy, then writes COMMITTED
  - `s3api list-objects` still shows INTELLIGENT_TIERING on everything
- Day ~47: `quarterly_q1.pdf` < cool -> PSEUDO. Days ~54-56: cold through grace -> COMMIT, moved to cold
- Day 90: quarter end, analyst reads `quarterly_q1.pdf` again
  - cold = local pool: read served from there, feeds temperature. one read adds 0.094, still cold. four reads the same day give 0.38 > warm -> promoted back to hot
  - cold = cloud endpoint: first read triggers read-through RESTORE at once. the read counts, a user caused it
  - either way object is HOT for the burst, cools after, committed again ~day 140
- Day 270: weekly FFT sees three quarter-end bursts in the span, trusts a 90d period, predicted next access = day 360
  - day 353: PRE-WARM queued (7 day lead). mover brings it to hot. the pre-warm is TIERING, doesn't count as access
  - day 360: analyst's read is served hot
  - `scratch_dump.csv` has no pattern, stays COMMITTED
- Day 400: doctor uploads a new `checkup.pdf`. New instance, starts with no history even though the old one was hot. Next day's reads warm it up again

## 10. Multisite / replication:

- Objects in an ITE bucket replicate between zones exactly as today. However **ITE state does NOT replicate**
- ITE runs per zone (like AWS Intelligent-Tiering decides per region). Each zone has its own fifo, count store, per-object state and worker threads. Scores on the access it sees locally
- Same object can be hot in one zone and cold in another. That is intended. A zone saves fast storage by cooling data its own users never read
- Why it stays consistent with no cross-zone sync
  - storage-class is set once at PUT and replicated with the object. No tiering action ever rewrites it -> every zone shows INTELLIGENT_TIERING regardless of where each zone put the bytes
  - replication writes are tagged REPLICATION and dropped at the sink. sync traffic never looks like demand. replicated object starts cold in the receiving zone, warms only on local reads
  - ITE fields never ride the sync stream. bilog carries index ops, not the entry payload
- Shared cloud cold endpoint: before uploading, a zone checks by ETag if the instance is already there. If yes, only drops its local hot copy. Commit is idempotent across zones
- Test plan has the two-zone scenario (same object stays hot where users read it, goes cold where nobody does)

## 11. Where it plugs into RGW:

- Storage goes through SAL. Everything ITE needs from storage is four abstract contracts in the driver interface. Workers are written once, a backend becomes ITE capable by implementing them. RADOS first, store and filter drivers get the same interfaces

| Contract | What it is |
| --- | --- |
| 1. write buffer | append-only sharded event queue. append / read after cursor / trim upto cursor |
| 2. aggregated counts | windowed counter store. idempotent apply of deltas + marker, read one window, iterate windows, drop a window |
| 3. per-object ITE state | read/write one object's fields. enumerate all objects of a bucket in one sequential pass |
| 4. move queue | append-only sharded action queue. append / read after cursor / ack upto cursor |
| 5. two-placement storage-class | one class resolves to hot + cold placement. report which one an instance is in, move it between them |

- Workers (aggregator, temperature scorer, FFT detector, decision engine, lazy mover) are background threads started at RGW boot, same as LC/GC workers
- Patterns reused
  - The existing restore subsystem is the template: per-zone pool, sharded queue, per-zone worker thread with clean shutdown
  - EWMA decay reuses the existing decay counter (the one MDS load balancing uses)
  - sharded day objects scale the same way the bucket index does
- Cluster-level config knobs: number of write-buffer shards, how long raw events stay in the buffer, how long aggregated day objects are kept, aggregator interval, hash shards per bucket per day window, window granularity (day or week), per-shard lock lifetime
- Time handling, so the feature is testable without waiting days
  - debug interval knobs for each worker (aggregator, temperature scorer, FFT detector, decision engine, mover), same style as `rgw_lc_debug_interval` for LC. The external prototype in [1] was tested the same way, with 1 day = 60 seconds

* Scoring/decision knobs (half-life, warm, cool, cold-after, pre-warm lead, min tier size, FFT confidence, FFT span) are bucket-level settings, listed in section 7

## 12. Admin CLI:

- Driven through admin CLI subcommands. Same operations exposed over the admin REST API

* Operations: enable ITE on a bucket; show a bucket's ITE config and knobs; set a knob on a bucket (eg., FFT span); disable ITE on a bucket (config preserved, see note); view the ITE state of an object or a bucket

- ITE state (temperature, tier state, FFT prediction) is admin-only in the MVP. Not exposed to end users over S3
- Config is per bucket. Global default for new buckets is future work
- Note: MVP focuses on green-field buckets. Once ITE is enabled it stays enabled, a working disable is a stretch item. The disable command above keeps the bucket's thresholds for re-enable
- Knobs per bucket = section 7 table
  - time knobs take a value with a unit suffix: `30s`, `10m`, `2h`, `7d`. eg., half-life `7d` in production, `7m` in a test
- LC transition rule and ITE can't both be set on a bucket. LC expiration can

## 13. Test plan:

- Setup: dev cluster with 3 RGW instances and a cold storage-class pre-configured.
- Driven with the S3 CLI, the admin CLI and a small history injection tool
- Fast clock: tests run with short windows and worker debug intervals
  - Unit tests , performance, security, regression

* driven by cold storage-class, driven by synthetic workloads (steady, bursty, daily/weekly/quarterly periodic, write-once).
  - Then a scale run at tens of millions of objects to check that overhead grows linearly with object count
  - Could refer to the testing done for the prototype in [1] (GET throughput unchanged with tracking, transition speed within 3% of LC at 9 concurrent transitions)

## Open items:

- Evaluation and state store: Option A, state on the bucket index entry with a daily full walk, or Option B, separate name-keyed state store with due-date scheduling. B removes the bucket-index change and the daily walk, no longer differs on overwrite history (counts are keyed by name in both), needs event-driven cleanup plus a reconciler for deletes (section 6). Choosing B reopens the index-entry decision above

## MVP0 Scope:

- Fresh deployments only. ITE is enabled on new (green-field) buckets before data lands. Buckets with existing data are not handled
- Exactly two storage placements under INTELLIGENT_TIERING: one hot pool, one cold placement (pool or cloud-s3 endpoint). No chain of more than two
- One flat set of bucket-level knobs for every object. No workload profiles, no clustering, no per-object policy
- Once ITE is enabled on a bucket it stays enabled. Disable is a stretch item
- Enabled only through the admin CLI / admin API, not through an LC rule
- Only USER events are logged. Internal callers (LC, GC, replication, ITE's own moves) are dropped at the sink
- Event set is GET/PUT/HEAD/DELETE/COPY/CompleteMultipartUpload. Nothing else
- Multisite: per-zone decisions, nothing replicated apart from the object itself
- Evaluation is the daily full walk of the bucket index (Option A). Due-date scheduling is an open alternative, not in scope unless decided
- Numbers (cost, scale) are measured on the MVP, not claimed up front

## MVP1/Later Scope:

Details for each of these will be followed up later.

### Brown-field buckets:

- Enabling ITE on a bucket that already has data. Needs a one-time pass to set the class/placement on existing objects and seed their state

### More than two placements:

- MVP gives INTELLIGENT_TIERING exactly two placements. Natural follow-on is an ordered chain under the same class, eg., hot (replicated SSD) -> cold (EC HDD) -> archive (cloud). S3 clients still see one class
- Ordered chain under the same INTELLIGENT_TIERING class, eg., hot (SSD) -> cold (EC HDD) -> archive. Archive could be a cloud placement using the existing cloud-tier, eg., S3 Glacier Deep Archive, with restore via the cloud-tier restore path
- Tier state becomes placement index + pending flag. Each boundary gets its own thresholds and residency floor
- What changes
  - storage-class definition becomes an ordered list of placements instead of a pair
  - tier state becomes index of current placement + pending flag, instead of HOT/PSEUDO/COMMITTED. PSEUDO grace applies at every boundary
  - each boundary gets its own demotion rule. hot->cold keeps today's warm/cool + cold-after. cold->archive adds a lower temperature threshold and a longer residency floor in cold, so an object must be quiet a long time before paying the archive round trip
  - decision engine moves one step at a time going down, never skips a placement. Promotion goes straight back to hot in one move
  - mover needs nothing new. commit is already "one step colder", restore/pre-warm already "move to hot"
  - profiles carry per-boundary knobs. admin CLI lists the chain per bucket

### Quotas:

- Hot-tier capacity budgets and quota enforcement: a ceiling on bytes in the hot placement per bucket or user. Under pressure the decision engine commits the coldest PSEUDO objects before their grace runs out, and holds pre-warms once the budget is full

### Enable ITE via LC rules:

- Enable ITE through an LC rule: a lifecycle transition to INTELLIGENT_TIERING (the AWS way) turns ITE on for the matching prefix or tags, alongside the admin enable command

### Other deferred items:

- Global ITE default for new buckets: a zone-level setting so every new bucket starts with ITE on and the default thresholds, instead of enabling bucket by bucket
- Disable ITE on a bucket: stop scoring and moves and leave objects where they are, with a restore-all-to-hot option. Thresholds are kept so re-enable picks up where it left off
- Logging internal callers with zero scoring weight (audit, pre-warm hit rate): events from LC, replication and ITE's own mover are logged but do not heat the object, so pre-warm hits can be measured. The caller field is already in the event
- Additional S3 ops as events: UploadPartCopy, s3select, POST Object, append, range GETs, batch DeleteObjects, metadata ops. Each needs a weight and a call on whether it is a data write (state reset) or a read
- Per-upload generation number: only if a consumer needs per-upload scoping of access counts (rewrite count as a clustering feature, admin stats since the last rewrite). The MVP resets state on every data write and keeps counts with the name, which needs none

## Future work (ML-driven):

The MVP applies one knob set to every object in a bucket. The items below make the knobs fit the workload: first by hand (profiles), then by learning (clustering, co-access, Markov), then by weighing cost and priority.

### Workload profiles:

- Named presets for half-life / cold-after / pre-warm (analytical, archive, streaming, content, compliance, data-lake, custom). Picked via admin command or S3 bucket tag, so a bucket gets sensible thresholds without hand-tuning each knob
  - Profile = named preset for half-life, cold-after and whether pre-warm is honoured. PSEUDO grace follows the profile's half-life
  - Six built in. Custom profiles via the admin CLI / API from day one
- How a profile is assigned to an object
  - by the admin: admin command or S3 bucket tag, one profile for the whole bucket
  - by rule, at PUT (and CompleteMultipartUpload): per-bucket rules matching object size, prefix, tag or content-type select the profile for the new object. First matching rule wins, bucket profile is the fallback
  - by learning: behavioural clustering (below) suggests or applies the profile that fits how the object is actually accessed
- Initial placement is part of the profile
  - MVP: every new object starts in the hot placement
  - with profiles, a profile can start objects directly in the cold placement, state COMMITTED. eg., rule `size > 1 GiB and prefix backups/` -> archive profile -> written straight to cold, never occupies the hot pool
  - this covers the storage-class selection at PUT that is done today with a Lua preRequest script (by size and other object parameters), without scripting. Storage-class seen by the client stays INTELLIGENT_TIERING
  - objects placed cold at PUT are still scored. Sustained reads promote them like any other COMMITTED object

| Profile | Half-life | Cold after | Pre-warm | Typical data |
| --- | --- | --- | --- | --- |
| analytical | 14d | 60d | yes | quarterly reports, dashboards |
| archive | 3d | 7d | no | backups, scratch |
| streaming | 1d | 7d | no | IoT logs, time series |
| content | 7d | 30d | yes | media assets |
| compliance | 14d | 90d | yes | legal, regulatory |
| data-lake | 7d | 30d | yes | mixed (default) |

### Behavorial clustering:

- Groups objects that behave alike and suggests the profile that fits each group. Runs as a weekly batch over signals the scorer already has
  - Question it answers: which objects in this bucket behave alike, and does the assigned profile match reality
  - Use clustering algorithms to detect and group such objects.

### Priority and cost-aware decisions:

- Priority per profile, capacity pressure, a per-action cost model and yo-yo detection: an object that keeps bouncing between hot and cold gets a longer residency before its next move

## References:

1. Nguyen The Ngoc Ha (strycks), *Access-based tiering for RGW: external prototype*, <https://docs.google.com/document/d/15fpVLhBAfRi1gCTaTTG0tGm7eZKBtyHJn92qDIKGTOE/edit?tab=t.0>. Prototype that runs outside radosgw on Reef 18.2.5: Lua post-request hook feeding a local ingress daemon, discrete daily EWMA score with per-tier thresholds, due-date evaluation, transitions through SAL `Object::transition()`. Source of the temperature formulation in section 4 and the due-date scheduling alternative (Option B) in section 7
2. AWS S3 Intelligent-Tiering, <https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering.html>. Reference for the one-class, service-moves-bytes shape recapped in "Feature recap"
3. AWS S3 lifecycle transition considerations, <https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html>. Linked from section 8
