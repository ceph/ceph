#!/bin/sh -e

# Unit tests for RGW distributed rate limiting (no cluster required).
unittest_rgw_ratelimit_distributed
unittest_rgw_ratelimit

exit 0
