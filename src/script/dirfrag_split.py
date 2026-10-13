#!/usr/bin/env python3

"""
dirfrag_split.py: a tool for visualizing/splitting CephFS dirfrags

Copyright (C) 2026 IBM Corp.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU Lesser General Public License along
with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import argparse
import json
import logging
import math
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def run_ceph(cmd_list):
    full_cmd = ["cephadm", "shell", "--", "ceph"] + cmd_list
    logging.info("Executing command: %s", " ".join(full_cmd))
    res = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
    return res.stdout

def get_default_fs_name():
    """Detects default legacy CephFS filesystem name (FSCID 1) from `fs dump`."""
    try:
        out = run_ceph(["fs", "dump", "--format=json"])
        data = json.loads(out)
        legacy_fscid = data.get("legacy_client_fscid", 1)
        for fs in data.get("filesystems", []):
            if fs.get("id") == legacy_fscid or fs.get("fscid") == legacy_fscid:
                fs_name = fs["mdsmap"]["fs_name"]
                logging.info("Auto-detected default legacy filesystem (FSCID %d): '%s'", legacy_fscid, fs_name)
                return fs_name
    except Exception as e:
        logging.warning("Could not auto-detect default FS (%s). Falling back to 'teuthology'.", e)
    return "teuthology"

def frag_to_bin_suffix(frag_str, bits):
    """Converts hex frag string (e.g., '200000/3') to binary star suffix (e.g., '001*')."""
    clean_hex = frag_str.split('/')[0]
    padded_hex = clean_hex.ljust(8, '0')
    val = int(padded_hex, 16)
    bin_str = f"{val:032b}"
    return bin_str[:bits] + "*"

def get_dump_dir_frags(fs_name, path):
    """
    Parses `dump dir <path>` on rank 0 to extract binary fragment suffixes
    (e.g., '1111111111*') and their authoritative rank (`replica_state.authority[0]`).
    """
    frags = []
    try:
        out = run_ceph(["tell", f"mds.{fs_name}:0", "dump", "dir", path])
        data = json.loads(out)

        for df in data:
            df_str = df.get("dirfrag", "")
            suffix = df_str.split(".")[-1] if "." in df_str else df_str

            authority = df.get("replica_state", {}).get("authority", [])
            auth_rank = int(authority[0]) if authority else 0
            frags.append({"suffix": suffix, "rank": auth_rank})

    except Exception as e:
        logging.error("Failed to fetch/parse 'dump dir %s': %s", path, e)

    return frags

def split_path_idempotent(fs_name, path, target_bits, dry_run=False):
    ls_out = run_ceph(["tell", f"mds.{fs_name}:0", "dirfrag", "ls", path])
    frags = json.loads(ls_out)

    dump_frags = get_dump_dir_frags(fs_name, path)
    auth_map = {f["suffix"]: f["rank"] for f in dump_frags}
    target_frags_total = 2**target_bits

    logging.info(
        "Targeting bit depth %d (2^%d = %d total fragments) on FS '%s' for '%s'",
        target_bits, target_bits, target_frags_total, fs_name, path
    )

    for frag in frags:
        frag_str = frag["str"]      # e.g., "0/3" or "200000/3"
        current_bits = frag["bits"] # e.g., 3 or 10

        if current_bits >= target_bits:
            logging.info(
                "[SKIP] Fragment %s already has %d bits (>= %d target, giving 2^%d = %d sub-pieces).",
                frag_str, current_bits, target_bits, current_bits, 2**current_bits
            )
            continue

        bits_to_add = target_bits - current_bits
        sub_pieces = 2**bits_to_add

        bin_suffix = frag_to_bin_suffix(frag_str, current_bits)
        target_rank = auth_map.get(bin_suffix, 0)

        logging.info(
            "[SPLIT] Fragment %s (%d bits, suffix %s) -> adding %d bit(s) via MDS rank %d (splits into 2^%d = %d sub-pieces)...",
            frag_str, current_bits, bin_suffix, bits_to_add, target_rank, bits_to_add, sub_pieces
        )

        split_cmd = ["tell", f"mds.{fs_name}:{target_rank}", "dirfrag", "split", path, frag_str, str(bits_to_add)]

        if dry_run:
            full_cmd = ["cephadm", "shell", "--", "ceph"] + split_cmd
            logging.info("[DRY-RUN] Would execute: %s", " ".join(full_cmd))
            continue

        try:
            run_ceph(split_cmd)
            logging.info("Successfully split fragment %s via MDS rank %d", frag_str, target_rank)
        except subprocess.CalledProcessError as e:
            logging.error("Failed to split fragment %s via MDS rank %d: %s", frag_str, target_rank, e)

# -------------------------------------------------------------------
# ASCII Tree & Level Statistics
# -------------------------------------------------------------------

class RadixNode:
    def __init__(self, path=""):
        self.path = path
        self.is_leaf = False
        self.auth_rank = None
        self.children = {}

def build_radix_trie(frags):
    root = RadixNode("")
    for frag in frags:
        s = frag['suffix'].rstrip('*')
        if s == "":
            root.is_leaf = True
            root.auth_rank = frag['rank']
            continue

        curr = root
        i = 0
        while i < len(s):
            matched_child = None
            for child_key, child_node in curr.children.items():
                common_len = 0
                min_len = min(len(s) - i, len(child_key))
                while common_len < min_len and s[i + common_len] == child_key[common_len]:
                    common_len += 1

                if common_len > 0:
                    matched_child = (child_key, child_node, common_len)
                    break

            if matched_child:
                child_key, child_node, common_len = matched_child
                if common_len == len(child_key):
                    i += common_len
                    curr = child_node
                else:
                    existing_prefix = child_key[:common_len]
                    existing_suffix = child_key[common_len:]

                    split_node = RadixNode(existing_prefix)
                    split_node.children[existing_suffix] = child_node
                    child_node.path = existing_suffix

                    del curr.children[child_key]
                    curr.children[existing_prefix] = split_node

                    i += common_len
                    curr = split_node
            else:
                remaining = s[i:]
                new_node = RadixNode(remaining)
                curr.children[remaining] = new_node
                curr = new_node
                i = len(s)

        curr.is_leaf = True
        curr.auth_rank = frag['rank']

    return root

def count_leaf_frags(node):
    """Calculates total active leaf fragments under a given node."""
    if node.is_leaf and not node.children:
        return 1
    total = 1 if node.is_leaf else 0
    for child in node.children.values():
        total += count_leaf_frags(child)
    return total

def render_radix_tree(path_name, node, full_path="", prefix="", is_last=True, is_root=True):
    lines = []
    current_full = full_path + node.path
    leaf_count = count_leaf_frags(node)

    if is_root:
        root_leaf_info = f" [MDS Rank {node.auth_rank}] (Unfragmented)" if node.is_leaf else ""
        lines.append(f"📁 {path_name} (*) — Total Active Fragments: {leaf_count}{root_leaf_info}")
    else:
        connector = "└── " if is_last else "├── "
        bits = len(current_full)

        padded_bin = current_full.ljust(32, '0')
        val = int(padded_bin, 2)
        hex_str = f"{val >> 8:06x}"

        if node.is_leaf:
            info = f" [MDS Rank {node.auth_rank}] ({hex_str}/{bits})"
        else:
            info = f" ({leaf_count} frags below)"

        lines.append(f"{prefix}{connector}{node.path}*{info}")

    child_prefix = prefix if is_root else (prefix + ("    " if is_last else "│   "))

    sorted_children = sorted(node.children.items(), key=lambda x: x[0])
    for idx, (edge_key, child_node) in enumerate(sorted_children):
        last_child = (idx == len(sorted_children) - 1)
        lines.extend(render_radix_tree(path_name, child_node, current_full, child_prefix, last_child, is_root=False))

    return lines

def visualize_dirfrag_tree(fs_name, path):
    frags = get_dump_dir_frags(fs_name, path)
    if not frags:
        logging.error("No dirfrag metadata returned for path '%s'.", path)
        return

    # Print summary breakdown by depth
    depth_counts = {}
    for f in frags:
        depth = len(f['suffix'].rstrip('*'))
        depth_counts[depth] = depth_counts.get(depth, 0) + 1

    summary_lines = ["\n📊 Dirfrag Depth Breakdown:"]
    for depth in sorted(depth_counts.keys()):
        count = depth_counts[depth]
        max_possible = 2**depth
        summary_lines.append(
            f"  • Depth {depth:2d} bits (2^{depth:<2d} = {max_possible:<5d} max): {count:<4d} active fragment(s)"
        )
    summary_lines.append(f"  • Total Active Leaf Fragments: {len(frags)}\n")
    print("\n".join(summary_lines))

    # Print ASCII Tree
    root_node = build_radix_trie(frags)
    tree_lines = render_radix_tree(path, root_node)
    print("\n".join(tree_lines) + "\n")

# -------------------------------------------------------------------
# CLI Parser
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CephFS Dirfrag Management and Visualization Utility"
    )
    parser.add_argument(
        "-f", "--fsname",
        default=None,
        help="CephFS filesystem name (default: auto-detect legacy filesystem)"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: split
    split_parser = subparsers.add_parser(
        "split",
        help="Idempotently split dirfrags up to a target bit depth N (2^N total fragments)"
    )
    split_parser.add_argument("path", help="CephFS directory path to split (e.g., /teuthology-archive)")
    split_parser.add_argument(
        "bits",
        type=int,
        help="Target bit depth N (e.g., 7 for 2^7 = 128 fragments)"
    )
    split_parser.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Preview split commands without executing"
    )

    # Subcommand: tree
    tree_parser = subparsers.add_parser(
        "tree",
        help="Visualize current dirfrag tree in ASCII art with per-level subtree counts and MDS authority ranks"
    )
    tree_parser.add_argument("path", help="CephFS directory path to visualize (e.g., /teuthology-archive)")

    args = parser.parse_args()
    fs_name = args.fsname if args.fsname else get_default_fs_name()

    if args.command == "split":
        if args.bits > 24:
            suggested_bits = math.ceil(math.log2(args.bits))
            parser.error(
                f"Bit depth {args.bits} is invalid (Ceph max bit depth is 24).\n"
                f"Did you mean --bits {suggested_bits} (for 2^{suggested_bits} = {2**suggested_bits} fragments)?"
            )
        split_path_idempotent(fs_name, args.path, args.bits, dry_run=args.dry_run)

    elif args.command == "tree":
        visualize_dirfrag_tree(fs_name, args.path)

if __name__ == "__main__":
    main()
