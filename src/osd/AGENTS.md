# src/osd/ — Object Storage Daemon

Core storage daemon. Manages Placement Groups (PGs), replication, erasure coding, peering, recovery, scrubbing.

Entry points: `OSD.h/cc` (daemon), `PrimaryLogPG.cc` (`do_op()`/`do_osd_ops()` for client I/O), `PeeringState.h` (state machine).

EC has two implementations switched by `ECSwitch.h`: FastEC (`ECBackend.cc`) is the active target; legacy (`ECBackendL.cc`) is frozen — do not add features to it.

`OSDMap.h` is used cluster-wide (mon, mds, clients), not just by OSD.

See `scrubber/AGENTS.md` for scrubbing subsystem.
