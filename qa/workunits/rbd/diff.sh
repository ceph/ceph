#!/bin/bash -ex

function cleanup() {
    rbd snap purge foo || :
    rbd rm foo || :
    rbd snap purge foo.copy || :
    rbd rm foo.copy || :
    rbd snap purge foo.copy2 || :
    rbd rm foo.copy2 || :
    rm -f foo.diff foo.out
}

cleanup

rbd create foo --size 1000
rbd bench-write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand

#rbd cp foo foo.copy
rbd create foo.copy --size 1000
rbd export-diff foo - | rbd import-diff - foo.copy

rbd snap create foo --snap=two
rbd bench-write foo --io-size 4096 --io-threads 5 --io-total 4096000 --io-pattern rand
rbd snap create foo --snap=three
rbd snap create foo.copy --snap=two

rbd export-diff foo@two --from-snap three foo.diff && exit 1 || true  # wrong snap order
rm foo.diff

rbd export-diff foo@three --from-snap two foo.diff
rbd import-diff foo.diff foo.copy
rbd import-diff foo.diff foo.copy && exit 1 || true   # this should fail with EEXIST on the end snap
rbd snap ls foo.copy | grep three

rbd create foo.copy2 --size 1000
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

cleanup

echo OK

