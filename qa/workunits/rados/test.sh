#!/bin/sh

die() {
    echo "${@}"
    exit 1
}

for f in ./test_rados_api_*; do
    if [ -x "${f}" ]; then
        "${f}" || die "${f} failed."
    fi
done

exit 0
