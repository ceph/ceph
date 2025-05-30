# Extreme SnapTrim Test Script

This script is designed to test the performance of `snaptrim` in an extreme scenario by creating a single placement group (PG) with multiple snapshots and performing I/O operations before and after creating snapshots.

## Requirements

- A working Ceph development environment with `vstart.sh`.
- Sufficient disk space for the specified RBD image size (default 100GB).

## Usage

```sh
./extreme_snaptrim_test.sh [options] <pool_name> <rbd_name>

Options:
  --dry-run: Only show commands that would be executed, but do not execute them.
  --verbose: Show detailed output.
  
Example:
  ./extreme_snaptrim_test.sh snap-test-pool rbd-test-0