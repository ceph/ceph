#!/bin/sh
set -e

export GCOV_PREFIX_STRIP=@@GCOV_PREFIX_STRIP@@

usage () {
      printf '%s: usage: %s OUTPUTDIR COMMAND [ARGS..]\n' "$(basename "$0")" "$(basename "$0")" 1>&2
      exit 1
}

export GCOV_PREFIX="$1"
[ -n "$GCOV_PREFIX" ] || usage
shift

case "$GCOV_PREFIX" in
    /*)
	# absolute path -> ok
	;;
    *)
	# make it absolute
	GCOV_PREFIX="$PWD/$GCOV_PREFIX"
	;;
esac

exec "$@"
