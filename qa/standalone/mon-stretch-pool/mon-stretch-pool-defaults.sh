#!/usr/bin/env bash
#
# ceph osd pool default set and get, and how ceph osd pool create uses the
# defaults they manage.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7181" # git grep '\<7181\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function default_field() {
    ceph osd pool default get -f json | jq -r ".$1.$2"
}

function pool_field() {
    ceph osd pool ls detail -f json | jq --arg p $1 ".[]|select(.pool_name==\$p)|.$2"
}

# osd pool default get shows every default that osd pool create uses, with
# its value and where the value comes from. This test checks that all of
# them are listed, with k and m but no profile name, and the values on a new
# cluster: pool_type replicated, num_zones 1 (the built-in default),
# crush_rule none, zone_failure_domain datacenter, osd_failure_domain host
# and root default. pg_autoscale_mode is off, from the command line, where
# run_mon sets it.
function TEST_get_shows_every_default() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default get || return 1
    local param
    for param in pool_type num_zones crush_rule zone_failure_domain \
                 osd_failure_domain root class replica min_size \
                 k m pg_num pgp_num pg_autoscale_mode \
                 bulk crimson ; do
        test "$(ceph osd pool default get -f json | jq "has(\"$param\")")" = true || return 1
    done
    # k and m, but no profile name
    test "$(ceph osd pool default get -f json | jq 'has("erasure_code_profile")')" = false || return 1
    test "$(default_field pool_type value)" = replicated || return 1
    test "$(default_field num_zones value)" = 1 || return 1
    test "$(default_field num_zones source)" = default || return 1
    test "$(default_field crush_rule value)" = none || return 1
    test "$(default_field zone_failure_domain value)" = datacenter || return 1
    test "$(default_field osd_failure_domain value)" = host || return 1
    test "$(default_field root value)" = default || return 1
    # run_mon sets these on the command line
    test "$(default_field pg_autoscale_mode value)" = off || return 1
    test "$(default_field pg_autoscale_mode source)" = cmdline || return 1
}

# osd pool default set writes the defaults to the global section of the
# configuration database, and osd pool default get shows where each comes
# from. --replica 2 --min_size 1 --pg_num 16 sets all three, and replica is
# shown as the option osd_pool_default_size, from the global section. A
# later --pg_num alone leaves replica as it was. --bulk turns the bulk
# default on and --bulk=false turns it off.
function TEST_set_writes_global_values() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --replica 2 --min_size 1 --pg_num 16 || return 1
    test "$(ceph config get mon osd_pool_default_size)" = 2 || return 1
    test "$(default_field replica value)" = 2 || return 1
    test "$(default_field replica option)" = osd_pool_default_size || return 1
    test "$(default_field replica source)" = global || return 1
    test "$(default_field min_size value)" = 1 || return 1
    test "$(default_field pg_num value)" = 16 || return 1
    # parameters that are not given are left alone
    ceph osd pool default set --pg_num 32 || return 1
    test "$(default_field replica value)" = 2 || return 1
    ceph osd pool default set --bulk || return 1
    test "$(default_field bulk value)" = true || return 1
    ceph osd pool default set --bulk=false || return 1
    test "$(default_field bulk value)" = false || return 1
}

# osd pool default set has no --size: the default number of copies is set
# with --replica. --size 4 is refused and replica stays 3.
function TEST_set_refuses_size() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --size 4 && return 1
    test "$(default_field replica value)" = 3 || return 1
}

# The replica default is kept in the option osd_pool_default_size, so osd
# pool default get shows a value set with config set global
# osd_pool_default_size 2 as replica 2, from that option.
function TEST_get_replica_from_size_option() {
    local dir=$1
    run_mon $dir a || return 1

    ceph config set global osd_pool_default_size 2 || return 1
    test "$(default_field replica value)" = 2 || return 1
    test "$(default_field replica option)" = osd_pool_default_size || return 1
}

# osd pool default set makes the checks that osd pool create would make of
# the same values. It refuses failure domains that are not CRUSH bucket
# types; a root, class or rule that does not exist; a rule given with
# --root; a --min_size above --replica; a --pgp_num above --pg_num; --crimson
# before osd set-allow-crimson; and --replica 1 unless --yes-i-really-mean-it
# is given. The refused commands leave zone_failure_domain and crush_rule at
# their built-in values.
function TEST_set_checks() {
    local dir=$1
    run_mon $dir a || return 1

    expect_failure $dir "is not a CRUSH bucket type" \
        ceph osd pool default set --zone_failure_domain nosuchtype || return 1
    expect_failure $dir "is not a CRUSH bucket type" \
        ceph osd pool default set --osd_failure_domain nosuchtype || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --root nosuchroot || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --class nosuchclass || return 1
    expect_failure $dir "does not exist" \
        ceph osd pool default set --rule nosuchrule || return 1
    expect_failure $dir "cannot specify both crush rule" \
        ceph osd pool default set --rule replicated_rule --root default || return 1
    expect_failure $dir "pool min_size must be between 1 and replica, which is set to 2" \
        ceph osd pool default set --replica 2 --min_size 3 || return 1
    expect_failure $dir "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'" \
        ceph osd pool default set --pg_num 8 --pgp_num 16 || return 1
    expect_failure $dir "data loss" \
        ceph osd pool default set --replica 1 || return 1
    ceph osd pool default set --replica 1 --yes-i-really-mean-it || return 1
    expect_failure $dir "set-allow-crimson" \
        ceph osd pool default set --crimson || return 1
    # nothing was written by the failures
    test "$(default_field zone_failure_domain value)" = datacenter || return 1
    test "$(default_field crush_rule value)" = none || return 1
}

# osd pool default set --num_zones 2 makes new pools go over two zones, so
# it checks the monitors as the first pool over two zones would. With the
# one monitor of this cluster, --num_zones 2 --zone_failure_domain zone is
# refused and num_zones stays 1.
function TEST_set_num_zones_needs_stretchable_cluster() {
    local dir=$1
    run_mon $dir a || return 1

    expect_failure $dir "Failed to validate monitor stretch mode" \
        ceph osd pool default set --num_zones 2 --zone_failure_domain zone || return 1
    test "$(default_field num_zones value)" = 1 || return 1
}

# osd pool default set --rule writes the rule's id to the option
# osd_pool_default_crush_rule, and osd pool default get shows the rule by
# name. --rule replicated_rule writes 0, and --rule none, for no default
# rule, writes -1.
function TEST_set_rule() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --rule replicated_rule || return 1
    test "$(ceph config get mon osd_pool_default_crush_rule)" = 0 || return 1
    test "$(default_field crush_rule value)" = replicated_rule || return 1
    ceph osd pool default set --rule none || return 1
    test "$(ceph config get mon osd_pool_default_crush_rule)" = -1 || return 1
}

function profile_of() {
    ceph osd pool get $1 erasure_code_profile -f json | jq -r .erasure_code_profile
}

# osd pool default set --k and --m change the default erasure code profile,
# default. A new erasure coded pool that names no profile, or names default,
# copies it to a profile of its own, <pool>-k<k>-m<m>. --k and --m are
# refused while the default pool type is replicated, and --replica while it
# is erasure. A set is applied whole or not at all: with a bad --pgp_num,
# the pool type and k stay as they were. Once set, k is shown as kept in the
# OSDMap, and the option osd_pool_default_erasure_code_profile is left
# alone. --k with an option kept in the configuration (--pg_num) sets both.
# Pools before, named and named2, created first, keep their k=2 m=2
# profiles, and pool after, created once k is 5 and m 3, gets after-k5-m3.
# --erasure_code_profile is refused.
function TEST_set_k_m_in_default_profile() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool create before 8 8 erasure || return 1
    # naming default names no profile
    ceph osd pool create named 8 8 erasure default || return 1
    ceph osd pool create named2 --pool_type erasure \
        --erasure_code_profile default || return 1
    local option="$(ceph config get mon osd_pool_default_erasure_code_profile)"
    # k and m are refused while the default type is replicated
    expect_failure $dir "cannot specify k/m parameters for replicated pools (osd_pool_default_type)" \
        ceph osd pool default set --k 4 --m 3 || return 1
    # applied whole or not at all
    expect_failure $dir "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'" \
        ceph osd pool default set --pool_type erasure --k 4 --m 3 --pg_num 8 --pgp_num 16 || return 1
    test "$(ceph osd erasure-code-profile get default -f json | jq -r .k)" = 2 || return 1
    test "$(default_field pool_type value)" = replicated || return 1
    ceph osd pool default set --pool_type erasure --k 4 --m 3 || return 1
    test "$(default_field k value)" = 4 || return 1
    test "$(default_field m value)" = 3 || return 1
    test "$(default_field k source)" = osdmap || return 1
    test "$(ceph osd erasure-code-profile get default -f json | jq -r .k)" = 4 || return 1
    # the option is left alone
    test "$(ceph config get mon osd_pool_default_erasure_code_profile)" = "$option" || return 1
    # together with an option
    ceph osd pool default set --k 5 --pg_num 16 || return 1
    test "$(default_field k value)" = 5 || return 1
    test "$(default_field pg_num value)" = 16 || return 1
    # replica is refused while the default type is erasure coded
    expect_failure $dir "replica is only applicable to replicated pools (osd_pool_default_type)" \
        ceph osd pool default set --replica 2 || return 1

    # new pools copy the default profile, existing pools keep theirs
    ceph osd pool create after 8 8 erasure || return 1
    test "$(profile_of after)" = after-k5-m3 || return 1
    for pool in before named named2; do
        test "$(profile_of $pool)" = $pool-k2-m2 || return 1
        test "$(ceph osd erasure-code-profile get $pool-k2-m2 -f json | jq -r .k)" = 2 || return 1
    done

    expect_failure $dir "erasure_code_profile" \
        ceph osd pool default set --erasure_code_profile default || return 1
}

function default_profile_field() {
    ceph osd erasure-code-profile get default -f json | jq -r ".\"$1\" // empty"
}

# The default erasure code profile always holds the placement of erasure
# coded pools: on a new cluster it has crush-root default,
# crush-zone-failure-domain datacenter, crush-failure-domain host and a
# crush-device-class. osd erasure-code-profile set default, given no zone
# failure domain, fills in datacenter. While the default pool type is
# replicated, osd pool default set --root, --osd_failure_domain,
# --zone_failure_domain and --no-class write the options and leave the
# profile alone. With --pool_type erasure they write the profile instead,
# even values that are the built-in ones (host), and leave the options;
# --no-class empties the profile's device class and --class ssd sets it.
function TEST_set_placement_in_default_profile() {
    local dir=$1
    run_mon $dir a || return 1
    test "$(default_profile_field crush-root)" = default || return 1
    test "$(default_profile_field crush-zone-failure-domain)" = datacenter || return 1
    test "$(default_profile_field crush-failure-domain)" = host || return 1
    test "$(ceph osd erasure-code-profile get default -f json | \
            jq 'has("crush-device-class")')" = true || return 1

    ceph osd crush add-bucket altroot root || return 1
    ceph osd crush class create ssd || return 1
    ceph osd erasure-code-profile set default plugin=isa \
        technique=reed_sol_van k=2 m=2 crush-failure-domain=osd \
        crush-device-class=ssd || return 1
    # set fills in what it is not given
    test "$(default_profile_field crush-zone-failure-domain)" = datacenter || return 1

    # a replicated default type leaves the default profile alone
    ceph osd pool default set --root altroot --osd_failure_domain rack \
        --zone_failure_domain host --no-class || return 1
    test "$(default_profile_field crush-failure-domain)" = osd || return 1
    test "$(default_profile_field crush-device-class)" = ssd || return 1
    test "$(default_profile_field crush-root)" = default || return 1
    test "$(default_profile_field crush-zone-failure-domain)" = datacenter || return 1
    test "$(ceph config get mon osd_pool_default_osd_failure_domain)" = rack || return 1

    # an erasure default type writes it, the built-in values too, and leaves
    # the options
    ceph osd pool default set --pool_type erasure --root altroot \
        --osd_failure_domain host --zone_failure_domain rack --no-class \
        || return 1
    test "$(default_profile_field crush-root)" = altroot || return 1
    test "$(default_profile_field crush-zone-failure-domain)" = rack || return 1
    test "$(default_profile_field crush-failure-domain)" = host || return 1
    test "$(ceph config get mon osd_pool_default_osd_failure_domain)" = rack || return 1
    test "$(ceph config get mon osd_pool_default_zone_failure_domain)" = host || return 1
    test "$(ceph osd erasure-code-profile get default -f json | \
            jq -r '."crush-device-class"')" = "" || return 1
    ceph osd pool default set --class ssd || return 1
    test "$(default_profile_field crush-device-class)" = ssd || return 1
}

# osd pool default set writes the global section, so it refuses a value
# that another source would override: --autoscale_mode, which run_mon sets
# on mon.a's command line, and --replica while osd_pool_default_size is set
# in the mon section of the configuration database. Once that is removed,
# --replica 2 is accepted.
function TEST_set_refuses_overridden_values() {
    local dir=$1
    run_mon $dir a || return 1

    # run_mon sets this one on the command line
    expect_failure $dir "local configuration of mon.a" \
        ceph osd pool default set --autoscale_mode on || return 1
    ceph config set mon osd_pool_default_size 3 || return 1
    expect_failure $dir "section mon of the configuration database" \
        ceph osd pool default set --replica 2 || return 1
    ceph config rm mon osd_pool_default_size || return 1
    ceph osd pool default set --replica 2 || return 1
}

# The health warning POOL_DEFAULT_CONFIG_OVERRIDE says that a default set
# by osd pool default set is overridden. A value masked to another host
# (global/host:nosuchhost osd_pool_default_size 4) does not apply to this
# monitor, so after --replica 2 it raises no warning.
function TEST_health_ignores_mask_of_other_host() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --replica 2 || return 1
    ceph config set global/host:nosuchhost osd_pool_default_size 4 || return 1
    sleep 10
    ceph health detail | grep -q POOL_DEFAULT_CONFIG_OVERRIDE && return 1
    return 0
}

# A value that overrides one written by osd pool default set raises the
# health warning POOL_DEFAULT_CONFIG_OVERRIDE. After --replica 2 there is no
# warning; config set mon osd_pool_default_size 3 raises it, naming the
# option and the mon section, and removing that value clears it.
function TEST_health_warns_of_override() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd pool default set --replica 2 || return 1
    ceph health detail | grep -q POOL_DEFAULT_CONFIG_OVERRIDE && return 1
    ceph config set mon osd_pool_default_size 3 || return 1
    local i
    for i in $(seq 1 30); do
        ceph health detail | grep -q POOL_DEFAULT_CONFIG_OVERRIDE && break
        sleep 1
    done
    ceph health detail | grep "osd_pool_default_size is set in section mon" || return 1
    ceph config rm mon osd_pool_default_size || return 1
    for i in $(seq 1 30); do
        ceph health detail | grep -q POOL_DEFAULT_CONFIG_OVERRIDE || return 0
        sleep 1
    done
    return 1
}

# osd pool create takes the values that osd pool default set gives. After
# --replica 2 --pg_num 4, pool rep gets size 2, pg_num 4 and num_zones 1.
# The legacy --size still sets the size of a pool in one zone: rep3, created
# with --size 3, gets size 3.
function TEST_pool_create_uses_defaults() {
    local dir=$1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    ceph osd pool default set --replica 2 --pg_num 4 || return 1
    ceph osd pool create rep || return 1
    test "$(pool_field rep size)" = 2 || return 1
    test "$(pool_field rep pg_num)" = 4 || return 1
    test "$(ceph osd pool get rep num_zones -f json | jq .num_zones)" = 1 || return 1
    # the legacy --size still gives the size of a single-zone pool
    ceph osd pool create rep3 --size 3 || return 1
    test "$(pool_field rep3 size)" = 3 || return 1
}

# The default rule must be a replicated rule, as replicated pools fall back
# on it. osd pool default set --rule ecr, an erasure rule made by
# create-erasure, is refused, and replicated pool r can still be created.
function TEST_set_rule_must_be_replicated() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile set p21 k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure ecr p21 || return 1
    ! ceph osd pool default set --rule ecr > $dir/out 2>&1 || return 1
    grep "rule 'ecr' is not a replicated rule" $dir/out || return 1
    ceph osd pool create r 8 8 || return 1
}

function pool_autoscale_mode() {
    ceph osd pool get $1 pg_autoscale_mode -f json | jq -r .pg_autoscale_mode
}

function pool_rule() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

# osd pool default set, osd pool create and osd pool set also take
# crush_rule and pg_autoscale_mode by their names in earlier releases, rule
# and autoscale_mode. Under each name, this test sets the default rule to
# replicated_rule and back to none, creates a pool on rule second (made by
# create-replicated, so not the default) and moves it to replicated_rule.
# Under each name it also creates a pool with autoscale mode warn and sets
# it to on. The default autoscale mode is refused under both names, as
# run_mon sets it on the command line. Giving both names at once is refused.
function TEST_old_param_names() {
    local dir=$1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1

    local name
    for name in crush_rule rule ; do
        ceph osd pool default set --$name replicated_rule || return 1
        test "$(default_field crush_rule value)" = replicated_rule || return 1
        ceph osd pool default set --$name none || return 1
        test "$(default_field crush_rule value)" = none || return 1
        # replicated_rule is the default, so a second rule shows the name is used
        ceph osd crush rule create-replicated second default osd || return 1
        ceph osd pool create pool_$name --$name second || return 1
        test "$(pool_rule pool_$name)" = second || return 1
        ceph osd pool set pool_$name $name replicated_rule || return 1
        test "$(pool_rule pool_$name)" = replicated_rule || return 1
    done
    for name in pg_autoscale_mode autoscale_mode ; do
        # run_mon sets osd_pool_default_pg_autoscale_mode on the command line
        expect_failure $dir "local configuration of mon.a" \
            ceph osd pool default set --$name on || return 1
        ceph osd pool create pool_$name --$name warn || return 1
        test "$(pool_autoscale_mode pool_$name)" = warn || return 1
        ceph osd pool set pool_$name $name on || return 1
        test "$(pool_autoscale_mode pool_$name)" = on || return 1
    done
    expect_failure $dir "cannot specify both --crush_rule and --rule" \
        ceph osd pool default set --crush_rule none --rule none || return 1
    expect_failure $dir "cannot specify both --pg_autoscale_mode and --autoscale_mode" \
        ceph osd pool create both --pg_autoscale_mode on --autoscale_mode on || return 1
}

main mon-stretch-pool-defaults "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-pool-defaults.sh"
# End:
