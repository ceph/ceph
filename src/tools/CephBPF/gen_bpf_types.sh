#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2026 Dongdong Tao <dongdong.tao@canonical.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEPH_SRC="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Output file can be passed as argument, defaults to current directory
if [ -n "$1" ]; then
    OUTPUT_FILE="$1"
else
    OUTPUT_FILE="./bpf_ceph_exported.h"
fi
TEMP_FILE="$OUTPUT_FILE.tmp"

# Array of source files to extract from: "file_path:description"
declare -a SOURCE_FILES=(
    "$CEPH_SRC/src/include/ceph_fs.h:Message Types"
    "$CEPH_SRC/src/msg/Message.h:Message Types"
    "$CEPH_SRC/src/include/rados.h:OSD Definitions"
    "$CEPH_SRC/src/os/bluestore/BlueStore.h:BlueStore Latency Enums"
)

echo "Generating $OUTPUT_FILE from Ceph headers..."

# Start the output file with header
cat > "$TEMP_FILE" << 'EOF'
/* SPDX-License-Identifier: LGPL-2.1 or LGPL-3.0 */
/* Auto-generated from Ceph headers - DO NOT EDIT directly */
/*
 * To regenerate this file, run:
 *   src/tracing/eBPF/gen_bpf_types.sh
 *
 * This file contains BPF-compatible definitions extracted from:
 *   - src/include/ceph_fs.h (message types)
 *   - src/msg/Message.h (message types)
 *   - src/include/rados.h (OSD operations and flags)
 *   - src/os/bluestore/BlueStore.h (BlueStore latency enums)
 *
 * Sections are marked with BPF-EXPORT-START/END comments in source files.
 * BPF-specific structs and helper definitions remain in
 * bpf_ceph_types.h and are manually maintained.
 */

#ifndef BPF_OSD_TYPES_H
#define BPF_OSD_TYPES_H

EOF

# Function to extract all BPF-EXPORT blocks from a file
# Args: $1 = source file path, $2 = description
extract_bpf_blocks() {
    local source_file="$1"
    local description="$2"
    local relative_path="${source_file#$CEPH_SRC/}"

    if [ ! -f "$source_file" ]; then
        echo "  WARNING: File not found: $source_file"
        return
    fi

    # Use awk to find and extract all BPF-EXPORT blocks
    awk -v desc="$description" -v relpath="$relative_path" '
        /\/\* BPF-EXPORT-START \*\// {
            in_block = 1
            block_num++
            content = ""
            next
        }

        /\/\* BPF-EXPORT-END \*\// {
            if (in_block) {
                # Print section header
                print "/* ========================================================================"
                if (block_num == 1) {
                    printf " * %s (from %s)\n", desc, relpath
                } else {
                    printf " * %s - Block %d (from %s)\n", desc, block_num, relpath
                }
                print " * ======================================================================== */"
                print "/* BPF-EXPORT-START */"

                # Print extracted content
                print content

                print "/* BPF-EXPORT-END */"
                print ""
            }
            in_block = 0
            content = ""
            next
        }

        in_block {
            content = content $0 "\n"
        }
    ' "$source_file" >> "$TEMP_FILE"

    # Count blocks found
    local block_count=$(grep -c "BPF-EXPORT-START" "$source_file" 2>/dev/null || echo "0")

    if [ "$block_count" -eq 0 ]; then
        echo "  WARNING: No BPF-EXPORT blocks found in $source_file"
    elif [ "$block_count" -eq 1 ]; then
        echo "  Extracted 1 block from $relative_path"
    else
        echo "  Extracted $block_count blocks from $relative_path"
    fi
}

# Process each source file
for entry in "${SOURCE_FILES[@]}"; do
    IFS=':' read -r file_path description <<< "$entry"
    extract_bpf_blocks "$file_path" "$description"
done

# Add footer with manual definitions include
cat >> "$TEMP_FILE" << 'EOF'
/* ========================================================================
 * BPF-Specific Definitions (manually maintained)
 * Include the manually maintained BPF-specific structs and helpers
 * ======================================================================== */
#include "bpf_ceph_types.h"

#endif /* BPF_OSD_TYPES_H */
EOF

# Move temp file to output
mv "$TEMP_FILE" "$OUTPUT_FILE"

echo ""
echo "Successfully generated $OUTPUT_FILE"
echo ""
echo "Note: BPF-specific structs are in bpf_ceph_types.h"
echo "      Edit that file for BPF-specific definitions."
