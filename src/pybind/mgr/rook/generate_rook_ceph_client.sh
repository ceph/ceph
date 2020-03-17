#!/bin/sh

set -e

script_location="$(dirname "$(readlink -f "$0")")"
cd "$script_location"

rm -rf rook_client


cp -r ./rook-client-python/rook_client .
rm -rf rook_client/cassandra
rm -rf rook_client/edgefs
rm -rf rook_client/tests
