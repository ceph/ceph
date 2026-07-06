# src/tools/ — CLI Tools

`ceph-objectstore-tool` opens the OSD's ObjectStore directly. The **OSD must be stopped first** — running it on a live OSD corrupts data.

`ceph-dencoder` is used in encoding compatibility tests. Adding a new encoded type requires registering it with the dencoder.

`rbd-mirror` (`rbd_mirror/`) is a long-running daemon, not just a CLI tool, despite living in `tools/`.
