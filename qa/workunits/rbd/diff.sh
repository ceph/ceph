#!/usr/bin/env bash
set -ex

function cleanup() {
    rbd snap purge foo || :
    rbd rm foo || :
    rbd snap purge foo.copy || :
    rbd rm foo.copy || :
    rbd snap purge foo.copy2 || :
    rbd rm foo.copy2 || :
    rbd snap purge foo.fv1 || :
    rbd rm foo.fv1 || :
    rbd snap purge foo.fv2 || :
    rbd rm foo.fv2 || :
    rm -f foo.diff foo.out foo.fv1 foo.fv2
}

function checksum() {
    local image=$1

    rbd export $image - | md5sum | awk '{print $1}'
}

function compare() {
    local image1=$1
    local image2=$2

    if [ "$(checksum $image1)" != "$(checksum $image2)" ]; then
        echo $image2 does not match $image1
        return 1
    fi
}

cleanup

rbd create foo --size 1000
rbd export --export-format 1 foo foo.fv1
rbd export --export-format 2 foo foo.fv2

rbd bench --io-type write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand

#rbd cp foo foo.copy
rbd create foo.copy --size 1000
rbd export-diff foo - | rbd import-diff - foo.copy
rbd export-diff foo - | rbd import-diff - --to-file foo.fv1
rbd export-diff foo - | rbd import-diff - --to-file foo.fv2

rbd import --export-format 2 foo.fv2 foo.fv2
rbd import --export-format 1 foo.fv1 foo.fv1
compare foo foo.copy
compare foo foo.fv1
compare foo foo.fv2
rbd rm foo.fv1
rbd rm foo.fv2

rbd snap create foo --snap=two
rbd bench --io-type write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand
rbd snap create foo --snap=three
rbd snap create foo.copy --snap=two

rbd export-diff foo@two --from-snap three foo.diff && exit 1 || true  # wrong snap order
rm -f foo.diff

rbd export-diff foo@three --from-snap two foo.diff
rbd import-diff foo.diff foo.copy
rbd import-diff foo.diff --to-file foo.fv1
rbd import-diff foo.diff --to-file foo.fv2
rbd import-diff foo.diff foo.copy && exit 1 || true   # this should fail with EEXIST on the end snap
rbd snap ls foo.copy | grep three

rbd create foo.copy2 --size 1000
rbd import-diff foo.diff foo.copy2 && exit 1 || true   # this should fail bc the start snap dne

rbd import --export-format 1 foo.fv1 foo.fv1
rbd import --export-format 2 foo.fv2 foo.fv2
rbd snap ls foo.fv2 | grep three
compare foo foo.copy
compare foo foo.fv1
compare foo foo.fv2

cleanup

echo OK

