#!/bin/sh -e

# Full RGW distributed rate-limit QA: unit tests plus cluster integration tests.
mydir=$(dirname "$0")
sh "$mydir/test_rgw_ratelimit_unit.sh"
sh "$mydir/test_rgw_ratelimit_integration.sh"

exit 0
