#!/usr/bin/env bash
set -ex

function cleanup() {
    rbd snap purge foo || :
    rbd rm foo || :
    rbd snap purge foo.copy || :
    rbd rm foo.copy || :
    rbd snap purge foo.copy2 || :
    rbd rm foo.copy2 || :
    rm -f foo.diff foo.out
}

function prepare() {
    rbd create foo --size 1000
    rbd bench --io-type write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand

    #rbd cp foo foo.copy
    rbd create foo.copy --size 1000
    rbd export-diff foo - | rbd import-diff - foo.copy

    rbd snap create foo --snap=two
    rbd bench --io-type write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand
    rbd snap create foo --snap=three
    rbd snap create foo.copy --snap=two
    rbd create foo.copy2 --size 1000
}

function test_by_snap_name() {
    prepare
    rbd export-diff foo@two --from-snap three foo.diff && exit 1 || true  # wrong snap order
    rm -f foo.diff

    rbd export-diff foo@three --from-snap two foo.diff
    rbd import-diff foo.diff foo.copy
    rbd import-diff foo.diff foo.copy && exit 1 || true   # this should fail with EEXIST on the end snap
    rbd snap ls foo.copy | grep three

    rbd import-diff foo.diff foo.copy2 && exit 1 || true   # this should fail bc the start snap dne

    rbd export foo foo.out
    orig=`md5sum foo.out | awk '{print $1}'`
    rm foo.out
    rbd export foo.copy foo.out
    copy=`md5sum foo.out | awk '{print $1}'`

    if [ "$orig" != "$copy" ]; then
        echo does not match
        exit 1
    fi
}

function test_by_snap_id() {
    prepare
    rbd clone --rbd-default-clone-format 2 foo@two foo.clone.two
    rbd snap rm --snap two foo
    snap_ids=( `rbd snap ls -a foo --format json|jq -r .[].id` )
    rbd export-diff foo --snap-id ${snap_ids[0]} --from-snap-id ${snap_ids[1]} foo.diff && exit 1 || true  # wrong snap order
    rm -f foo.diff

    rbd export-diff foo --snap-id ${snap_ids[1]} --from-snap-id ${snap_ids[0]} foo.diff
    rbd import-diff foo.diff foo.copy
    rbd import-diff foo.diff foo.copy && exit 1 || true   # this should fail with EEXIST on the end snap
    rbd snap ls foo.copy | grep three

    rbd import-diff foo.diff foo.copy2 && exit 1 || true   # this should fail bc the start snap dne

    rbd export foo foo.out
    orig=`md5sum foo.out | awk '{print $1}'`
    rm foo.out
    rbd export foo.copy foo.out
    copy=`md5sum foo.out | awk '{print $1}'`

    if [ "$orig" != "$copy" ]; then
        echo does not match
        exit 1
    fi
}
cleanup

test_by_snap_name

cleanup

test_by_snap_id

cleanup

echo OK

