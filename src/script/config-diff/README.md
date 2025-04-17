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
   python3 config_diff.py diff-branch --ref-branch <branch1> --cmp-branch <branch2> [--ref-repo <repo-url>] [--posix-diff]
   ```

   - `--ref-branch`: The reference branch to compare against.
   - `--cmp-branch`: The branch to compare.
   - `--ref-repo`: (Optional) The repository URL. Defaults to the Ceph upstream repository.
   - `--posix-diff`: (Optional) Print the diff in POSIX-diff like format.

2. **`diff-tag`**: Compare configuration options between two tags in the same Ceph repository.
   ```bash
   python3 config_diff.py diff-tag --ref-tag <tag1> --cmp-tag <tag2> [--ref-repo <repo-url>] [--posix-diff]
   ```

   - `--ref-tag`: The reference tag to compare against.
   - `--cmp-tag`: The tag to compare.
   - `--ref-repo`: (Optional) The repository URL. Defaults to the Ceph upstream repository.
   - `--posix-diff`: (Optional) Print the diff in POSIX-diff like format.

3. **`diff-branch-remote-repo`**: Compare configuration options between branches in different repositories.
   ```bash
   python3 config_diff.py diff-branch-remote-repo --ref-branch <branch1> --cmp-branch <branch2> --remote-repo <repo-url> [--ref-repo <repo-url>] [--posix-diff]
   ```

   - `--ref-branch`: The reference branch to compare against.
   - `--cmp-branch`: The branch to compare.
   - `--remote-repo`: The remote repository URL for the branch to compare.
   - `--ref-repo`: (Optional) The repository URL for the reference branch. Defaults to the Ceph upstream repository.
   - `--posix-diff`: (Optional) Print the diff in POSIX-diff like format.

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
+++ mgr.yaml.in (added) +++
+ mgr_client_bytes_test
--- osd.yaml.in (deleted) ---
osd_scrub_load_threshold
!!! global.yaml.in (modifed) !!!
! mon_debug_no_require_tentacle
  - before: false
  + after: true
```