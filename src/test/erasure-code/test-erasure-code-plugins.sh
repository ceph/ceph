#!/bin/bash -x

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

arch=$(uname -m)

case $arch in
    i[[3456]]86*|x86_64*|amd64*)
        legacy_jerasure_plugins=(jerasure_generic jerasure_sse3 jerasure_sse4)
        legacy_shec_plugins=(shec_generic shec_sse3 shec_sse4)
        plugins=(jerasure shec lrc isa)
        ;;
    aarch64*|arm*) 
        legacy_jerasure_plugins=(jerasure_generic jerasure_neon)
        legacy_shec_plugins=(shec_generic shec_neon)
        plugins=(jerasure shec lrc)
        ;;
    *)
        echo "unsupported platform ${arch}."
        return 1
        ;;
esac

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:17110" # git grep '\<17110\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_preload_warning() {
    local dir=$1
    
    for plugin in ${legacy_jerasure_plugins[*]} ${legacy_shec_plugins[*]}; do
        setup $dir || return 1
        run_mon $dir a --osd_erasure_code_plugins="${plugin}" || return 1 
	run_mgr $dir x || return 1
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
        run_osd $dir 0 --osd_erasure_code_plugins="${plugin}" || return 1 
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
        grep "WARNING: osd_erasure_code_plugins contains plugin ${plugin}" $dir/mon.a.log || return 1
        grep "WARNING: osd_erasure_code_plugins contains plugin ${plugin}" $dir/osd.0.log || return 1
        teardown $dir || return 1
    done
    return 0
}

function TEST_preload_no_warning() {
    local dir=$1

    for plugin in ${plugins[*]}; do
        setup $dir || return 1
        run_mon $dir a --osd_erasure_code_plugins="${plugin}" || return 1 
	run_mgr $dir x || return 1
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
        run_osd $dir 0 --osd_erasure_code_plugins="${plugin}" || return 1 
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
        ! grep "WARNING: osd_erasure_code_plugins contains plugin" $dir/mon.a.log || return 1
        ! grep "WARNING: osd_erasure_code_plugins contains plugin" $dir/osd.0.log || return 1
        teardown $dir || return 1
    done

    return 0
}

function TEST_preload_no_warning_default() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1 
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1 
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    ! grep "WARNING: osd_erasure_code_plugins" $dir/mon.a.log || return 1
    ! grep "WARNING: osd_erasure_code_plugins" $dir/osd.0.log || return 1
    teardown $dir || return 1

    return 0
}

function TEST_ec_profile_warning() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1 
    run_mgr $dir x || return 1
    for id in $(seq 0 2) ; do
        run_osd $dir $id || return 1 
    done
    wait_for_clean || return 1

    for plugin in ${legacy_jerasure_plugins[*]}; do
        ceph osd erasure-code-profile set prof-${plugin} crush-failure-domain=osd technique=reed_sol_van plugin=${plugin} || return 1
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
        grep "WARNING: erasure coding profile prof-${plugin} uses plugin ${plugin}" $dir/mon.a.log || return 1
    done

    for plugin in ${legacy_shec_plugins[*]}; do
        ceph osd erasure-code-profile set prof-${plugin} crush-failure-domain=osd plugin=${plugin} || return 1
        CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
        grep "WARNING: erasure coding profile prof-${plugin} uses plugin ${plugin}" $dir/mon.a.log || return 1
    done

    teardown $dir || return 1
}

main test-erasure-code-plugins "$@"
