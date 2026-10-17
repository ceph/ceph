#!/usr/bin/env bash
set -eu

rbd_fuse="$1"
export CEPH_CONF=/dev/null
export CEPH_ARGS="--no-mon-config"

check_options() {
    local output
    # -V is handled by FUSE after the preceding options, while --version
    # would exit during Ceph's argument parsing without exercising FUSE.
    if output=$("$rbd_fuse" "$@" -V 2>&1); then
        case "$output" in
            *"FUSE library version"*) ;;
            *)
                printf 'FUSE version output missing: %s\n' "$output" >&2
                exit 1
                ;;
        esac
    else
        printf 'rbd-fuse option parsing failed for %s\n%s\n' "$*" "$output" >&2
        exit 1
    fi
}

# Pool and namespace defaults must be safe to replace under libfuse3,
# including when the same string option is supplied more than once.
check_options
check_options -p rbd
check_options -pcustom-pool
check_options --poolname=custom-pool
check_options -s custom-namespace
check_options -scustom-namespace
check_options --namespace=custom-namespace
check_options -s ""
check_options --namespace=
check_options -p first-pool -p second-pool -s first-ns -s second-ns
check_options --poolname=first --poolname=second --namespace=first --namespace=second
check_options -p first --poolname=second -s first --namespace=second

echo 'rbd-fuse option parsing: OK'
