# src/erasure-code/ — Erasure Coding Framework

Pluggable EC framework. `ErasureCodeInterface.h` defines the interface; plugins live in subdirectories (`jerasure/`, `isa/`, `lrc/`, `clay/`, `shec/`).

EC has two OSD-side implementations switched by `src/osd/ECSwitch.h`:
- **FastEC** (`src/osd/ECBackend.cc`, `ECCommon.cc`) — active development target. All new work goes here.
- **Classic/Legacy EC** (`src/osd/ECBackendL.cc`, `ECCommonL.cc`) — frozen, likely to be deleted. Do not add features.

`minimum_to_decode()` is critical for read optimization — it tells the OSD which shards to read to minimize I/O.

Plugin status:
- **ISA-L** (`isa/`) — default plugin, expected long-term supported. Hardware-accelerated on x86-64 (Intel + AMD), ARM NEON, and RISC-V; base C functions work on any architecture.
- **Jerasure** (`jerasure/`) — previous default, expected long-term supported.
- **LRC** (`lrc/`) — not supported in FastEC (optimised EC). May never be. Do not assume it will work with FastEC.
- **Clay** (`clay/`) and **SHEC** (`shec/`) — expected to be deleted in the near future. Do not add features.
