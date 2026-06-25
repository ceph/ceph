# PR #69576 Review Comment Tracker

## Addressed Comments

### 1. Signed-off-by prohibition (gregsfortytwo, idryomov, mmgaggle)
- **Comment IDs**: 3435928954, 3435936055, 3435938377, 3435940296, 3444260301, 3443383484, 3444072359
- **Action**: Strengthened S-o-b language throughout AGENTS.md. AI agents MUST NOT add S-o-b tags. Aligned with Linux kernel policy. Removed all phrasing that implied AI could add S-o-b with human approval.
- **Commit**: `doc: AGENTS.md: strengthen Signed-off-by prohibition for AI agents`
- **Status**: Done

### 2. RGW test location clarification (yuvalif)
- **Comment ID**: 3436278802
- **Action**: Added notes to qa/AGENTS.md about RGW test split: src/test/rgw has unit+standalone tests, qa/ has setup code only, most RGW tests in separate s3-tests repo.
- **Commit**: `doc: qa/AGENTS.md: clarify RGW test locations`
- **Status**: Done

### 3. Fix dependency graph + replace ASCII diagram (yuvalif, JonBailey1993/Bob)
- **Comment ID**: 3436325550 (yuvalif), external feedback from JonBailey1993
- **Action**: Replaced ASCII box diagram with text-based dependency list. Fixed incorrect dependencies: rgw depends on librados (not os/crush directly, not librbd). ASCII diagrams are unreliable for LLM parsing.
- **Commit**: `doc: AGENTS.md: replace ASCII diagram with text dependency list`
- **Status**: Done

### 4. Mention radosgw-admin and librgw in Purpose (yuvalif)
- **Comment ID**: 3436356957
- **Action**: Added brief mention of radosgw-admin and librgw to the Purpose section of src/rgw/AGENTS.md. They were already in the Key Files table but not immediately visible.
- **Commit**: `doc: src/rgw/AGENTS.md: mention radosgw-admin and librgw in Purpose`
- **Status**: Done
