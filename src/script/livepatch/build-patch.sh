#!/usr/bin/env bash
#
# build-patch.sh <build dir> <patch.cc> <binary> <reference source>
#                <old symbol regex> <new symbol> [<output dir>]
#
# Build a libpulp live patch for a daemon of a WITH_LIVEPATCH build; see
# doc/dev/livepatch.rst.
#
#   patch.cc           defines the replacement, an extern "C" function
#                      <new symbol> that takes the object as first argument
#   binary             the executable to patch: a name in <build dir>/bin,
#                      or a path to a copy of it
#   reference source   the file the replaced function is defined in, relative
#                      to src/; patch.cc is compiled with its flags
#   old symbol regex   extended regex that matches exactly one mangled symbol
#                      of the binary
#
# Writes <patch>.so and <patch>.dsc next to patch.cc, or to <output dir>.
set -e

if [ $# -lt 6 ]; then
    sed -n '3,16s/^# \{0,1\}//p' "$0"
    exit 1
fi
build=$(readlink -f "$1")
src=$(readlink -f "$2")
case "$3" in
    */*) bin=$(readlink -f "$3") ;;
    *) bin=$(readlink -f "$build/bin/$3") ;;
esac
ref=$4
pattern=$5
new=$6
out=$(readlink -f "${7:-$(dirname "$src")}")/$(basename "$src" .cc)

# a function the compiler split has a second symbol, <name>.cold
old=$(nm --defined-only "$bin" | awk '{print $3}' | grep -E "$pattern" |
          grep -v '[.]' | sort -u)
if [ "$(echo "$old" | grep -c .)" != 1 ]; then
    echo "'$pattern' must match exactly one symbol of $bin, it matches:" >&2
    echo "$old" >&2
    exit 1
fi

cmd=$(ninja -C "$build" -t compdb | python3 -c '
import json, sys
for e in json.load(sys.stdin):
    if e["file"].endswith("/src/" + sys.argv[1]):
        print(e["command"])
        break' "$ref")
if [ -z "$cmd" ]; then
    echo "no compile command for src/$ref in $build" >&2
    exit 1
fi
# keep the compiler and its flags; drop the output, input and dependency
# arguments, and the patchable entries: nothing patches the patch
flags=$(echo "$cmd" | sed -E 's/ -o [^ ]+//; s/ -c [^ ]+//; s/ -MD//;
    s/ -MT [^ ]+//; s/ -MF [^ ]+//; s/-fpatchable-function-entry=[0-9,]+//')
flags=${flags#*ccache }
(cd "$build" && $flags -fPIC -fno-access-control -c "$src" -o "$out.o")
${CXX:-g++} -shared -o "$out.so" "$out.o"
rm -f "$out.o"
printf '%s\n@%s\n%s:%s\n' "$out.so" "$bin" "$old" "$new" > "$out.dsc"
ulp packer "$out.dsc" > /dev/null
echo "$out.so: replaces $(echo "$old" | c++filt) in $bin"
