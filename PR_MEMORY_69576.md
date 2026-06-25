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
