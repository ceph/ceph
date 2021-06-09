#!/usr/bin/env bash

set -ex

function run_perf_c2c() {
    # First get some background system info
    uname -a > uname.out
    lscpu > lscpu.out
    cat /proc/cmdline > cmdline.out
    timeout -s INT 10 vmstat -w 1 > vmstat.out || true
    sudo dmesg >& dmesg.out
    cat /proc/cpuinfo > cpuinfo.out
    ps axo psr,time,stat,ppid,pid,pcpu,comm > ps.1.out
    ps -eafT > ps.2.out
    sudo sysctl -a > sysctl.out

    nodecnt=`lscpu|grep "NUMA node(" |awk '{print $3}'`
    for ((i=0; i<$nodecnt; i++))
    do
       sudo cat /sys/devices/system/node/node${i}/meminfo > meminfo.$i.out
    done
    sudo more `sudo find /proc -name status` > proc_parent_child_status.out
    sudo more /proc/*/numa_maps > numa_maps.out

    #
    # Get separate kernel and user perf-c2c stats
    #
    sudo perf c2c record -a --ldlat=70 --all-user -o perf_c2c_a_all_user.data sleep 5
    sudo perf c2c report --stdio -i perf_c2c_a_all_user.data > perf_c2c_a_all_user.out 2>&1
    sudo perf c2c report --full-symbols --stdio -i perf_c2c_a_all_user.data > perf_c2c_full-sym_a_all_user.out 2>&1

    sudo perf c2c record --call-graph dwarf -a --ldlat=70 --all-user -o perf_c2c_g_a_all_user.data sleep 5
    sudo perf c2c report -g --stdio -i perf_c2c_g_a_all_user.data > perf_c2c_g_a_all_user.out 2>&1

    sudo perf c2c record -a --ldlat=70 --all-kernel -o perf_c2c_a_all_kernel.data sleep 4
    sudo perf c2c report --stdio -i perf_c2c_a_all_kernel.data > perf_c2c_a_all_kernel.out 2>&1

    sudo perf c2c record --call-graph dwarf --ldlat=70 -a --all-kernel -o perf_c2c_g_a_all_kernel.data sleep 4

    sudo perf c2c report -g --stdio -i perf_c2c_g_a_all_kernel.data > perf_c2c_g_a_all_kernel.out 2>&1

    #
    # Get combined kernel and user perf-c2c stats
    #
    sudo perf c2c record -a --ldlat=70 -o perf_c2c_a_both.data sleep 4
    sudo perf c2c report --stdio -i perf_c2c_a_both.data > perf_c2c_a_both.out 2>&1

    sudo perf c2c record --call-graph dwarf --ldlat=70 -a --all-kernel -o perf_c2c_g_a_both.data sleep 4
    sudo perf c2c report -g --stdio -i perf_c2c_g_a_both.data > perf_c2c_g_a_both.out 2>&1

    #
    # Get all-user physical addr stats, in case multiple threads or processes are
    # accessing shared memory with different vaddrs.
    #
    sudo perf c2c record --phys-data -a --ldlat=70 --all-user -o perf_c2c_a_all_user_phys_data.data sleep 5
    sudo perf c2c report --stdio -i perf_c2c_a_all_user_phys_data.data > perf_c2c_a_all_user_phys_data.out 2>&1
}

function run() {
    local dir=$1
    shift
    (
	rm -fr $dir
	mkdir $dir
	cd $dir
	ceph_test_c2c --threads $(($(nproc) * 2)) "$@" &
	sleep 30 # let it warm up
	run_perf_c2c
	kill $! || { echo "ceph_test_c2c WAS NOT RUNNING" ; exit 1 ; }
    ) || exit 1
}

function bench() {
    optimized=$(timeout 30 ceph_test_c2c --threads $(($(nproc) * 2)) --sharding 2> /dev/null || true)
    not_optimized=$(timeout 30 ceph_test_c2c --threads $(($(nproc) * 2)) 2> /dev/null || true)
    if ! (( $optimized > ( $not_optimized * 2 ) )) ; then
	echo "the optimization is expected to be at least x2 faster"
	exit 1
    fi
}

run with-sharding --sharding
run without-sharding
bench
