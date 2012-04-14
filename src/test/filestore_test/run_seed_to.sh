#!/bin/sh

set -e

seed=$1
killat=$2

echo seed $seed
echo kill at $killat

# run forever, until $killat...
to=1000000000

rm -fr a a.fail a.recover
test_filestore_idempotent_sequence run-sequence-to $to a a/journal \
    --filestore-xattr-use-omap --test-seed $seed --osd-journal-size 100 \
    --filestore-kill-at $killat \
    --log-file a.fail --debug-filestore 20 || true

stop=`test_filestore_idempotent_sequence get-last-op a a/journal --filestore-xattr-use-omap \
    --log-file a.recover --debug-filestore 20`

echo stopped at $stop

rm -rf b b.clean
test_filestore_idempotent_sequence run-sequence-to $stop b b/journal \
    --filestore-xattr-use-omap --test-seed $seed --osd-journal-size 100 \
    --log-file b.clean --debug-filestore 20

if test_filestore_idempotent_sequence diff a a/journal b b/journal \
    --filestore-xattr-use-omap; then
    echo OK
    exit 0
fi

echo "FAIL"
echo " see:"
echo "   a.fail     -- leading up to failure"
echo "   a.recovery -- journal replay"
echo "   b.clean    -- the clean reference"
exit 1

