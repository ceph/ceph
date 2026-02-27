# Ceph Config Diff Tool

This program is a Python-based tool designed to compare the configuration options of Ceph by cloning the repository and analyzing the files present in the `src/common/options` directory. It supports three modes of operation: `diff-branch`, `diff-tag`, and `diff-branch-remote-repo`.

Note: This tool **does not** compare the configuration changes occuring on a running Ceph cluster.

## Features

- **Compare Branches**: Compare configuration options between two branches in the same repository.
- **Compare Tags**: Compare configuration options between two tags in the same repository.
- **Compare Branches Across Repositories**: Compare configuration options between branches in different repositories.

## Usage

Run the program using the following command:

```bash
python3 config_diff.py <mode> [options]
```

### Modes

1. **`diff-branch`**: Compare configuration options between two branches in the same Ceph repository.
   ```bash
   python3 config_diff.py diff-branch --ref-branch <branch1> --cmp-branch <branch2> [--ref-repo <repo-url>] [--skip-clone] [--format <output-format>]
   ```

    - `--ref-branch`: The reference branch to compare against.
    - `--cmp-branch`: The branch to compare.
    - `--ref-repo`: (Optional) The repository URL. Defaults to the Ceph upstream repository.
    - `--skip-clone`: (Optional) Skips cloning repositories for diff. **Note**: When using this flag, the script must be run from a valid Ceph upstream repository or a forked repository that has access to the branches present in the upstream repository or already contains those branches.
    - `--format`: (Optional) Specify the output format for the configuration diff. Options are `json` or `posix-diff`. Default is `json`.


2. **`diff-tag`**: Compare configuration options between two tags in the same Ceph repository.
   ```bash
   python3 config_diff.py diff-tag --ref-tag <tag1> --cmp-tag <tag2> [--ref-repo <repo-url>] [--posix-diff]
   ```

    - `--ref-tag`: The reference tag to compare against.
    - `--cmp-tag`: The tag to compare.
    - `--ref-repo`: (Optional) The repository URL. Defaults to the Ceph upstream repository.
    - `--skip-clone`: (Optional) Skips cloning repositories for diff. **Note**: When using this flag, the script must be run from a valid Ceph upstream repository or a forked repository that has access to the branches present in the upstream repository or already contains those branches.
    - `--format`: (Optional) Specify the output format for the configuration diff. Options are `json` or `posix-diff`. Default is `json`.

3. **`diff-branch-remote-repo`**: Compare configuration options between branches in different repositories.
   ```bash
   python3 config_diff.py diff-branch-remote-repo --ref-branch <branch1> --cmp-branch <branch2> --remote-repo <repo-url> [--ref-repo <repo-url>] [--posix-diff]
   ```

    - `--ref-branch`: The reference branch to compare against.
    - `--cmp-branch`: The branch to compare.
    - `--remote-repo`: The remote repository URL for the branch to compare.
    - `--ref-repo`: (Optional) The repository URL for the reference branch. Defaults to the Ceph upstream repository.
    - `--ref-commit-sha`: (Optional) The commit sha for the reference branch that is used for reference
    - `--cmp-commit-sha`: (Optional) The commit sha of the comparing branch
    - `--skip-clone`: (Optional) Skips cloning repositories for diff. **Note**: When using this flag, the script must be run from a valid Ceph upstream repository or a forked repository that has access to the branches present in the upstream repository or already contains those branches.
    - `--format`: (Optional) Specify the output format for the configuration diff. Options are `json` or `posix-diff`. Default is `json`.

### Example Commands

1. Compare two branches in the same repository:
   ```bash
   python3 config_diff.py diff-branch --ref-branch main --cmp-branch feature-branch
   ```
  
  The above command checks how the configuration options present in the branch
  `feature-branch` has changed from the `main` branch

2. Compare two tags in the same repository:
   ```bash
   python3 config_diff.py diff-tag --ref-tag v1.0.0 --cmp-tag v1.1.0
   ```
  The above command checks how the configuration options present in the tag
  `v1.1.0` has changed from the `v1.0.0` branch

3. Compare branches across repositories:
   ```bash
   python3 config_diff.py diff-branch-remote-repo --ref-branch main --cmp-branch feature-branch --remote-repo https://github.com/username/ceph
   ```
  

  The above command checks how the configuration options present in the
  `feature-branch` of the remote repository `https://github.com/username/ceph`
  has changed from the `main` branch of the ceph upstream repo. This command is
  used by the `diff-ceph-config.yml` GitHub Action to check for any
  configuration changes on the any PR's that are raised.

### Important Notes for `--skip-clone` Flag

- When using the `--skip-clone` flag, the script assumes it is being run from a valid Ceph upstream repository or a forked repository.
- If using a forked repository, ensure that the forked repository has access to the branches present in the upstream repository or already contains those branches.

  
## Output

The program generates a JSON output containing the following structure:

```json
{
  "added": {
    "daemon1": ["config1", "config2"]
  },
  "deleted": {
    "daemon2": ["config3"]
  },
  "modified": {
    "daemon3": {
      "config4": {
        "key1": {
          "before": "old_value",
          "after": "new_value"
        }
      }
    }
  }
}
```

- **`added`**: Configuration options added in the comparing version.
- **`deleted`**: Configuration options removed in the comparing version.
- **`modified`**: Configuration options modified between the two versions.

## Example:

### diff in JSON

```json
{
    "added": {
        "mgr.yaml.in": [
            "mgr_client_bytes_test"
        ]
    },
    "deleted": {
        "osd.yaml.in": [
            "osd_scrub_load_threshold"
        ]
    },
    "modified": {
        "global.yaml.in": {
            "mon_debug_no_require_tentacle": {
                "default": {
                    "before": false,
                    "after": true
                }
            }
        }
    }
}
```

### POSIX like diff:

```diff
+ added: bluefs_wal_envelope_mode
- removed: rgw_d4n_l1_write_open_flags
! changed: mon_nvmeofgw_skip_failovers_interval: old: 16
! changed: mon_nvmeofgw_skip_failovers_interval: new: 32
```