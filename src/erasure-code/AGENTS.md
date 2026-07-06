# src/erasure-code/ — Erasure Coding Framework

Pluggable EC framework. `ErasureCodeInterface.h` defines the interface; plugins live in subdirectories (`jerasure/`, `isa/`, `lrc/`, `clay/`, `shec/`).

EC has two OSD-side implementations switched by `src/osd/ECSwitch.h`:
- **FastEC** (`src/osd/ECBackend.cc`, `ECCommon.cc`) — active development target. All new work goes here.
- **Classic/Legacy EC** (`src/osd/ECBackendL.cc`, `ECCommonL.cc`) — frozen, likely to be deleted. Do not add features.

`minimum_to_decode()` is critical for read optimization — it tells the OSD which shards to read to minimize I/O.

ISA-L (`isa/`) requires Intel CPUs. CLAY codes minimize repair bandwidth but have higher CPU cost.
