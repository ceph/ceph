#!/bin/sh
set -e

usage () {
      printf '%s: usage: %s [-d srcdir] [-o output_basename] COMMAND [ARGS..]\n' "$(basename "$0")" "$(basename "$0")" 1>&2
      exit 1
}

OUTPUT_BASENAME=coverage
SRCDIR=.

while getopts  "d:o:h" flag
do
    case $flag in
    d) SRCDIR=$OPTARG;;
    o) OUTPUT_BASENAME=$OPTARG;;
    *) usage;;
    esac
done

shift $(($OPTIND - 1))

lcov -d $SRCDIR -z > /dev/null 2>&1
lcov -d $SRCDIR -c -i -o "${OUTPUT_BASENAME}_base_full.info" > /dev/null 2>&1
"$@"
lcov -d $SRCDIR -c -o "${OUTPUT_BASENAME}_tested_full.info" > /dev/null 2>&1
lcov -r "${OUTPUT_BASENAME}_base_full.info" /usr/include\* -o "${OUTPUT_BASENAME}_base.info" > /dev/null 2>&1
lcov -r "${OUTPUT_BASENAME}_tested_full.info" /usr/include\* -o "${OUTPUT_BASENAME}_tested.info" > /dev/null 2>&1
lcov -a "${OUTPUT_BASENAME}_base.info" -a "${OUTPUT_BASENAME}_tested.info" -o "${OUTPUT_BASENAME}.info" | tail -n 3
