#!/usr/bin/env bash

# set -e -x

. "`dirname $0`/test-rgw-common.sh"

temp_prefix="/tmp/`basename $0`-$$"

short=${temp_prefix}-short
short_w_blank=${temp_prefix}-short-w-blank
long=${temp_prefix}-long
unsorted=${temp_prefix}-unsorted
empty=${temp_prefix}-empty
fake=${temp_prefix}-fake

out1=${temp_prefix}-out1
out2=${temp_prefix}-out2

cat >"${short}" <<EOF
bear
fox
hippo
zebra
EOF

cat >"${short_w_blank}" <<EOF
bear
fox
hippo

zebra
EOF

cat >"${long}" <<EOF
badger
cuttlefish
fox
llama
octopus
penguin
seal
squid
whale
yak
zebra
EOF

cat >"${unsorted}" <<EOF
bear
hippo
fox
zebra
EOF

touch $empty

#### testing ####

# test perfect match
ceph-diff-sorted $long $long >"${out1}"
$assert $? -eq 0
$assert $(cat $out1 | wc -l) -eq 0

# test non-match; use /bin/diff to verify
/bin/diff $short $long >"${out2}"
ceph-diff-sorted $short $long >"${out1}"
$assert $? -eq 1
$assert $(cat $out1 | grep '^<' | wc -l) -eq $(cat $out2 | grep '^<' | wc -l)
$assert $(cat $out1 | grep '^>' | wc -l) -eq $(cat $out2 | grep '^>' | wc -l)

/bin/diff $long $short >"${out2}"
ceph-diff-sorted $long $short >"${out1}"
$assert $? -eq 1
$assert $(cat $out1 | grep '^<' | wc -l) -eq $(cat $out2 | grep '^<' | wc -l)
$assert $(cat $out1 | grep '^>' | wc -l) -eq $(cat $out2 | grep '^>' | wc -l)

# test w blank line
ceph-diff-sorted $short $short_w_blank 2>/dev/null
$assert $? -eq 4

ceph-diff-sorted $short_w_blank $short 2>/dev/null
$assert $? -eq 4

# test unsorted input
ceph-diff-sorted $short $unsorted >"${out2}" 2>/dev/null
$assert $? -eq 4

ceph-diff-sorted $unsorted $short >"${out2}" 2>/dev/null
$assert $? -eq 4

# test bad # of args
ceph-diff-sorted 2>/dev/null
$assert $? -eq 2

ceph-diff-sorted $short 2>/dev/null
$assert $? -eq 2

# test bad file path

ceph-diff-sorted $short $fake 2>/dev/null
$assert $? -eq 3

ceph-diff-sorted $fake $short 2>/dev/null
$assert $? -eq 3

#### clean-up ####

/bin/rm -f $short $short_w_blank $long $unsorted $empty $out1 $out2
