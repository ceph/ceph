# Ceph

C++ (check CMakeLists.txt for version if relevant), CMake, Python for management.

## Build

```
./install-deps.sh
./do_cmake.sh
 cd build && ninja
```
If build directory exists, do not re-run install-deps or do-cmake

Never clean the build directory without explicit permission.

Minimal build for vstart uses target "vstart"

## Rules

Read and follow these files — they are the single source of truth:
- `SubmittingPatches.rst` — commit messages, PR format, backports

Key rules:
- No Signed-off-by: AI agents MUST NOT add `Signed-off-by` tags. Only humans certify DCO.
- Assisted-by: all AI-generated commits must include `Assisted-by: <tool>`. Do not use co-authored-by.
- Minimal diffs: only change lines necessary for the functional change. No whitespace-only, style-only, or include-reordering changes to unmodified code.
- User-visible changes: update docs in `doc/`. Developer info goes in code comments or `doc/dev/`.
- Design docs: complex features need a design doc in `doc/dev/`. Never modify an approved design doc during implementation without permission.

Config options are defined in `src/common/options/*.yaml.in`.
