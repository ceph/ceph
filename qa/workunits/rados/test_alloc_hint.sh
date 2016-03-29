#!/bin/bash

set -ex
shopt -s nullglob # fns glob expansion in expect_alloc_hint_eq()

#
# Helpers
#

function get_xml_val() {
    local xml="$1"
    local tag="$2"

    local regex=".*<${tag}>(.*)</${tag}>.*"
    if [[ ! "${xml}" =~ ${regex} ]]; then
        echo "'${xml}' xml doesn't match '${tag}' tag regex" >&2
        return 2
    fi

    echo "${BASH_REMATCH[1]}"
}

function get_conf_val() {
    set -e

    local entity="$1"
    local option="$2"

    local val
    val="$(sudo ceph daemon "${entity}" config get --format=xml "${option}")"
    val="$(get_xml_val "${val}" "${option}")"

    echo "${val}"
}

function setup_osd_data() {
    for (( i = 0 ; i < "${NUM_OSDS}" ; i++ )); do
        OSD_DATA[i]="$(get_conf_val "osd.$i" "osd_data")"
    done
}

function setup_pgid() {
    local poolname="$1"
    local objname="$2"

    local pgid
    pgid="$(ceph osd map "${poolname}" "${objname}" --format=xml)"
    pgid="$(get_xml_val "${pgid}" "pgid")"

    PGID="${pgid}"
}

function expect_alloc_hint_eq() {
    local expected_extsize="$1"

    for (( i = 0 ; i < "${NUM_OSDS}" ; i++ )); do
        # Make sure that stuff is flushed from the journal to the store
        # by the time we get to it, as we prod the actual files and not
        # the journal.
        sudo ceph daemon "osd.${i}" "flush_journal"

        # e.g., .../25.6_head/foo__head_7FC1F406__19
        #       .../26.bs1_head/bar__head_EFE6384B__1a_ffffffffffffffff_1
        local fns=$(sudo sh -c "ls ${OSD_DATA[i]}/current/${PGID}*_head/${OBJ}_*")
        local count="${#fns[@]}"
        if [ "${count}" -ne 1 ]; then
            echo "bad fns count: ${count}" >&2
            return 2
        fi

        local extsize
        extsize="$(sudo xfs_io -c extsize "${fns[0]}")"
        local extsize_regex="^\[(.*)\] ${fns[0]}$"
        if [[ ! "${extsize}" =~ ${extsize_regex} ]]; then
            echo "extsize doesn't match extsize_regex: ${extsize}" >&2
            return 2
        fi
        extsize="${BASH_REMATCH[1]}"

        if [ "${extsize}" -ne "${expected_extsize}" ]; then
            echo "FAIL: alloc_hint: actual ${extsize}, expected ${expected_extsize}" >&2
            return 1
        fi
    done
}

#
# Global setup
#

EC_K="2"
EC_M="1"
NUM_OSDS="$((EC_K + EC_M))"

NUM_PG="12"
NUM_PGP="${NUM_PG}"

LOW_CAP="$(get_conf_val "osd.0" "filestore_max_alloc_hint_size")"
HIGH_CAP="$((LOW_CAP * 10))" # 10M, assuming 1M default cap
SMALL_HINT="$((LOW_CAP / 4))" # 256K, assuming 1M default cap
BIG_HINT="$((LOW_CAP * 6))" # 6M, assuming 1M default cap

setup_osd_data

#
# ReplicatedBackend tests
#

POOL="alloc_hint-rep"
ceph osd pool create "${POOL}" "${NUM_PG}"
ceph osd pool set "${POOL}" size "${NUM_OSDS}"

OBJ="foo"
setup_pgid "${POOL}" "${OBJ}"
rados -p "${POOL}" create "${OBJ}"

# Empty object, SMALL_HINT - expect SMALL_HINT
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${SMALL_HINT}" "${SMALL_HINT}"
expect_alloc_hint_eq "${SMALL_HINT}"

# Try changing to BIG_HINT (1) - expect LOW_CAP (BIG_HINT > LOW_CAP)
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${BIG_HINT}" "${BIG_HINT}"
expect_alloc_hint_eq "${LOW_CAP}"

# Bump the cap to HIGH_CAP
ceph tell 'osd.*' injectargs "--filestore_max_alloc_hint_size ${HIGH_CAP}"

# Try changing to BIG_HINT (2) - expect BIG_HINT (BIG_HINT < HIGH_CAP)
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${BIG_HINT}" "${BIG_HINT}"
expect_alloc_hint_eq "${BIG_HINT}"

ceph tell 'osd.*' injectargs "--filestore_max_alloc_hint_size ${LOW_CAP}"

# Populate object with some data
rados -p "${POOL}" put "${OBJ}" /etc/passwd

# Try changing back to SMALL_HINT - expect BIG_HINT (non-empty object)
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${SMALL_HINT}" "${SMALL_HINT}"
expect_alloc_hint_eq "${BIG_HINT}"

OBJ="bar"
setup_pgid "${POOL}" "${OBJ}"

# Non-existent object, SMALL_HINT - expect SMALL_HINT (object creation)
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${SMALL_HINT}" "${SMALL_HINT}"
expect_alloc_hint_eq "${SMALL_HINT}"

ceph osd pool delete "${POOL}" "${POOL}" --yes-i-really-really-mean-it

#
# ECBackend tests
#

PROFILE="alloc_hint-ecprofile"
POOL="alloc_hint-ec"
ceph osd erasure-code-profile set "${PROFILE}" k=2 m=1 ruleset-failure-domain=osd
ceph osd erasure-code-profile get "${PROFILE}" # just so it's logged
ceph osd pool create "${POOL}" "${NUM_PG}" "${NUM_PGP}" erasure "${PROFILE}"

OBJ="baz"
setup_pgid "${POOL}" "${OBJ}"
rados -p "${POOL}" create "${OBJ}"

# Empty object, SMALL_HINT - expect scaled-down SMALL_HINT
rados -p "${POOL}" set-alloc-hint "${OBJ}" "${SMALL_HINT}" "${SMALL_HINT}"
expect_alloc_hint_eq "$((SMALL_HINT / EC_K))"

ceph osd pool delete "${POOL}" "${POOL}" --yes-i-really-really-mean-it

#
# Global teardown
#

echo "OK"
