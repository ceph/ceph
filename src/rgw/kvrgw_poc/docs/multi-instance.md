# Multi-instance deployment

**As-built reference:** [poc_as_built.md](../poc_as_built.md)

## Architecture

One **versitygw** frontend per process, each with in-process **C++** (`libkvrgw.so`). An **nginx GW** on `:9080` round-robins HTTP to frontends on `:9081` … `:9080+N`. All instances share **one FDB cluster** and **one `KVRGW_DATA`** blob directory.

```
S3 client → GW :9080 → versitygw :9081..908N (cgo libkvrgw.so) → FDB + shared data/
```

Each backend runs its own **sweeper** and **GC worker** (intentional race exercise; coordination deferred).

## Quick start

```bash
./scripts/install_lb.sh           # nginx (required once)
./scripts/reload.sh --clean 3     # 3 frontends + GW
./scripts/stop.sh 3               # stop all
```

## Commands

| Command | Purpose |
|---------|---------|
| `./scripts/reload.sh [--clean] [N]` | Rebuild, start N frontends + GW, verify, fast tests |
| `./scripts/start_multi.sh [--clean] [N]` | Start without rebuild |
| `./scripts/start_lb.sh [N]` | Start/restart GW only |
| `./scripts/stop_lb.sh` | Stop GW only |
| `./scripts/stop.sh [N]` | Stop GW + N pairs |

## Environment

| Variable | Default | Notes |
|----------|---------|-------|
| `KVRGW_INSTANCES` | `1` | Frontend process count |
| `KVRGW_GW_PORT` | `9080` | GW listen port (always GW, never frontend) |
| `KVRGW_HTTP_BASE_PORT` | `9081` | Frontend for instance `i` = `9081+i` |
| `KVRGW_DATA` | `./data` | **Shared** across all backends |
| `KVRGW_TENANT_NAME` | `kv-poc` | AddTenant once via GW |

Per-instance paths (instance `i`):

| Resource | Path |
|----------|------|
| Admin socket | `/tmp/kvrgw-admin-{i}.sock` |
| Frontend HTTP | `:9081+i` |
| Logs | `.logs/backend-{i}.log`, `.logs/frontend-{i}.log` |

## Clients and tests

Always use the **GW** endpoint:

```bash
export ENDPOINT=http://127.0.0.1:9080
aws --endpoint-url "$ENDPOINT" s3 ls
s5cmd --endpoint-url "$ENDPOINT" ls s3://
# s3cmd.cfg: host_base = 127.0.0.1:9080
```

Direct instance debug (bypass GW): `curl http://127.0.0.1:9082/...`

## GC admin fan-out

Configured instance count is stored in **`.run/instances`**. At runtime, scripts probe **live** backends (process + admin socket) and fan out only to live indices. If an instance is killed, call `kvrgw_refresh_gw_live` (via test helpers) so nginx drops dead upstreams.

`gc_ctl.sh` applies `set-gc-config` / `wait-applied` to **live** admin sockets when `KVRGW_INSTANCES>1`.

- **set-gc-config** — each instance returns its own `HANDLE` (saved in `.run/gc-handles.last`)
- **Startup (N>1):** one random backend gets `suspended=1` via direct admin socket (others unchanged)
- **wait-applied** — waits on each instance using **its** handle from the last set
- **get-gc-config** — policy fields must match all instances; `active_age`/`pending_age` may differ; each instance must be self-consistent (`pending_age` = `active_age` or `active_age+1`)
- FDB inspect commands (`count`, `list`, …) run once (shared FDB)

## Limitations

- **Shared `KVRGW_DATA` required** for round-robin GET after PUT (separate data dirs need sticky routing, not RR).
- **Cross-host** scaling needs shared object store (RADOS); not this POC.
- GC/sweeper coordination and FDB-persisted GC policy: later stage.

## Chaos tests (manual / `--chaos` tier)

Requires **`reload.sh --clean 3`** first.

| Script | What |
|--------|------|
| `test_kill_single_instance.sh` | SIGKILL instance 1; integration tests on 2/3 cluster |
| `test_power_cycle_upload_easy.sh` | 64K upload → restart instance 1 → verify → 2nd 64K bucket |
| `test_power_cycle_upload_hard.sh` | 64K upload + aws list page2 while killing instance 1 → restart → s5cmd ls → verify both |

Shared verifier: `verify_bucket_all.sh` — aws list + s5cmd ls + s5cmd sync download + `diff -rq` (~1 min / 64K bucket).
