# CMake Presets

This directory contains shared [CMake presets](https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html) for configuring Ceph builds. Presets are loaded from the repository root via `CMakePresets.json`.

## Quick start

Generate local preset files first (required once per clone, or after deleting them):

```bash
./cmake/presets/generate_host.sh
```

This creates `host.json` and `user.json` from their `.example` templates if they do not already exist, then writes detected host settings into `host.json`. `do_cmake.sh` runs the same bootstrap automatically.

List available configure presets:

```bash
cmake --list-presets=configure
```

Configure a build tree:

```bash
cmake --preset client-min-debug
cmake --build build-client-min-debug
```

Most IDEs with CMake support (VS Code, CLion, Qt Creator, etc.) will also discover these presets automatically from the project root.

## Available presets

### Base presets

| Preset | Build type | Build directory |
|--------|------------|-----------------|
| `debug` | Debug | `build-debug` |
| `release` | RelWithDebInfo | `build-relwithdebinfo` |

### Component presets

| Preset | Description | Build directory |
|--------|-------------|-----------------|
| `client-min-debug` | Minimal client build (Debug) | `build-client-min-debug` |
| `client-min-release` | Minimal client build (Release) | `build-client-min-release` |
| `rgw-min-dev` | Minimal RGW build for vstart (Debug) | `build-rgw-min-debug` |
| `rgw-min-release` | Minimal RGW build (Release) | `build-rgw-min-release` |

Component presets inherit all settings from `debug` or `release` and add their own `WITH_*` cache variables on top.

## File layout

```
CMakePresets.json          # Root entry point; includes files from this directory
cmake/presets/
  defaults.json            # Project-wide debug/release defaults (_default-*)
  host.json                # Auto-detected host settings (_host), gitignored
  host.json.example        # Template for bootstrapping host.json
  user.json                # Local overrides (_user-*), gitignored
  user.json.example        # Template for bootstrapping user.json
  detect_host.sh           # Shared platform detection logic
  generate_host.sh         # Writes host.json from detect_host.sh
  base.json                # Composes debug/release from defaults + host + user
  client.json              # Client component presets
  rgw.json                 # RGW component presets
```

Inheritance flows like this:

```
defaults.json (_default-debug, _default-release)
       +
host.json     (_host)              ← auto-detected by do_cmake.sh / generate_host.sh
       +
user.json     (_user-debug, _user-release)   ← manual overrides
       ↓
base.json     (debug, release)
       ↓
client.json / rgw.json  (component presets)
```

## Host detection (`host.json`)

Platform-dependent settings — Python version, compiler, sccache/ccache, and distro-specific options — are detected by `detect_host.sh`. This logic is shared between `do_cmake.sh` and the CMake presets.

### Using presets only

Run host detection before using presets in an IDE or with `cmake --preset`:

```bash
./cmake/presets/generate_host.sh
```

If either `user.json` or `host.json` is missing, it is created from the matching `.example` file. Re-run after changing your toolchain to refresh `host.json`.

### Using `do_cmake.sh`

`do_cmake.sh` bootstraps any missing local preset files, then writes `cmake/presets/host.json` before configuring. After running `do_cmake.sh`, presets will reflect the same host settings without a separate step.

### What gets detected

| Setting | Detection |
|---------|-----------|
| `WITH_PYTHON3` | Distro and version from `/etc/os-release` |
| `CMAKE_C_COMPILER` / `CMAKE_CXX_COMPILER` | Highest available `gcc-N` / `g++-N` (11–20) |
| `WITH_SCCACHE` / `WITH_CCACHE` | Enabled if `sccache` or `ccache` is on `PATH` |
| `WITH_RADOSGW_AMQP_ENDPOINT` / `WITH_RADOSGW_KAFKA_ENDPOINT` | Set to `OFF` on openSUSE/SLES and FreeBSD |

`host.json` and `user.json` are gitignored because they are machine-specific.

## Local customization

If `user.json` does not exist yet, run `./cmake/presets/generate_host.sh` to create it from `user.json.example`. Then edit `user.json` for personal preferences.

Use `user.json` for settings that are personal preference rather than host detection — for example `USE_TRACEFLOW`, `ENABLE_GIT_VERSION`, or `CMAKE_EXPORT_COMPILE_COMMANDS`. Host-level settings like compiler and Python version belong in `host.json` (via `generate_host.sh`) so they stay in sync with `do_cmake.sh`.

Example `user.json`:

```json
{
  "version": 10,
  "configurePresets": [
    {
      "name": "_user-debug",
      "hidden": true,
      "cacheVariables": {
        "CMAKE_EXPORT_COMPILE_COMMANDS": "ON",
        "USE_TRACEFLOW": "ON",
        "ENABLE_GIT_VERSION": "OFF"
      }
    },
    {
      "name": "_user-release",
      "hidden": true,
      "cacheVariables": {
        "CMAKE_EXPORT_COMPILE_COMMANDS": "ON",
        "USE_TRACEFLOW": "ON",
        "ENABLE_GIT_VERSION": "OFF"
      }
    }
  ]
}
```

`user.json` is gitignored; local edits will not affect rebases.

### About `CMakeUserPresets.json`

CMake also supports a root-level `CMakeUserPresets.json` for per-user settings. However, defining presets named `debug` or `release` there will conflict with the project presets and produce a duplicate-preset error. Use `cmake/presets/user.json` instead — it integrates with the inheritance chain used by all component presets.

## Creating a new component preset

Follow the pattern used by `client.json` and `rgw.json`:

1. **Create a new file** in this directory, e.g. `mycomponent.json`.

2. **Include `base.json`** so `debug` and `release` are reachable for inheritance:

   ```json
   {
     "version": 10,
     "include": ["base.json"],
     "configurePresets": [ ... ]
   }
   ```

3. **Define a hidden preset** with the component-specific `cacheVariables` shared across build types:

   ```json
   {
     "name": "mycomponent-min",
     "hidden": true,
     "cacheVariables": {
       "WITH_CEPHFS": "OFF",
       "WITH_RBD": "ON"
     }
   }
   ```

4. **Define visible presets** that inherit from a base build type and the hidden preset:

   ```json
   {
     "name": "mycomponent-min-debug",
     "inherits": ["debug", "mycomponent-min"],
     "binaryDir": "${sourceDir}/build-mycomponent-min-debug"
   },
   {
     "name": "mycomponent-min-release",
     "inherits": ["release", "mycomponent-min"],
     "binaryDir": "${sourceDir}/build-mycomponent-min-release"
   }
   ```

5. **Register the file** in the root `CMakePresets.json`:

   ```json
   {
     "version": 10,
     "include": [
       "cmake/presets/base.json",
       "cmake/presets/client.json",
       "cmake/presets/rgw.json",
       "cmake/presets/mycomponent.json"
     ]
   }
   ```

6. **Verify** the new presets load correctly:

   ```bash
   cmake --list-presets=configure
   cmake --preset mycomponent-min-debug -N
   ```

### Inheritance rules

- A preset can only inherit from presets defined in the same file or in files that file includes (directly or indirectly). This is why every component file includes `base.json`.
- When a preset inherits from multiple parents, settings are merged in order; later presets override earlier ones for conflicting fields.
- Use `"hidden": true` for intermediate presets that should not appear in `--list-presets` output.
- Always set a unique `binaryDir` for each visible preset to avoid build directory collisions.
