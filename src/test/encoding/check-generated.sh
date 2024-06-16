#!/usr/bin/env bash
set -e

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

dir=$1

test_selected_type() {
    local type=$1
    local result_dir=$2
    local failed=0
    local numtests=0
    local pids=""
    local result=$(mktemp ${result_dir}/$type-result-XXXXXXXX)
    local tmp1=$(mktemp /tmp/typ-XXXXXXXX)
    local tmp2=$(mktemp /tmp/typ-XXXXXXXX)
    local tmp3=$(mktemp /tmp/typ-XXXXXXXX)
    local tmp4=$(mktemp /tmp/typ-XXXXXXXX)
    local num=$(ceph-dencoder type $type count_tests)
    local deterministic=0
    if ceph-dencoder type "$type" is_deterministic; then
        deterministic=1
    fi

    echo "$num $type"
    for n in $(seq 1 1 $num 2>/dev/null); do
        run_in_background pids save_stdout "$tmp1" ceph-dencoder type "$type" select_test "$n" dump_json
        run_in_background pids save_stdout "$tmp2" ceph-dencoder type "$type" select_test "$n" encode decode dump_json
        run_in_background pids save_stdout "$tmp3" ceph-dencoder type "$type" select_test "$n" copy dump_json
        run_in_background pids save_stdout "$tmp4" ceph-dencoder type "$type" select_test "$n" copy_ctor dump_json
        wait_background pids

        if [ $? -ne 0 ]; then
            echo "**** $type test $n encode+decode check failed ****"
            echo "   ceph-dencoder type $type select_test $n encode decode"
            failed=$(($failed + 3))
            continue
        fi

        # nondeterministic classes may dump nondeterministically.  compare
        # the sorted json output.  this is a weaker test, but is better
        # than nothing.
        if [ $deterministic -eq 0 ]; then
            echo "  sorting json output for nondeterministic object"
            for f in $tmp1 $tmp2 $tmp3 $tmp4; do
                sort $f | sed 's/,$//' > $f.new
                mv $f.new $f
            done
        fi

        if ! cmp $tmp1 $tmp2; then
            echo "**** $type test $n dump_json check failed ****"
            echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
            echo "   ceph-dencoder type $type select_test $n encode decode dump_json > $tmp2"
            diff $tmp1 $tmp2
            failed=$(($failed + 1))
        fi

        if ! cmp $tmp1 $tmp3; then
            echo "**** $type test $n copy dump_json check failed ****"
            echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
            echo "   ceph-dencoder type $type select_test $n copy dump_json > $tmp2"
            diff $tmp1 $tmp2
            failed=$(($failed + 1))
        fi

        if ! cmp $tmp1 $tmp4; then
            echo "**** $type test $n copy_ctor dump_json check failed ****"
            echo "   ceph-dencoder type $type select_test $n dump_json > $tmp1"
            echo "   ceph-dencoder type $type select_test $n copy_ctor dump_json > $tmp2"
            diff $tmp1 $tmp2
            failed=$(($failed + 1))
        fi

        if [ $deterministic -ne 0 ]; then
            run_in_background pids ceph-dencoder type "$type" select_test $n encode export "$tmp1"
            run_in_background pids ceph-dencoder type "$type" select_test $n encode decode encode export "$tmp2"
            wait_background pids

            if ! cmp $tmp1 $tmp2; then
                echo "**** $type test $n binary reencode check failed ****"
                echo "   ceph-dencoder type $type select_test $n encode export $tmp1"
                echo "   ceph-dencoder type $type select_test $n encode decode encode export $tmp2"
                diff <(hexdump -C $tmp1) <(hexdump -C $tmp2)
                failed=$(($failed + 1))
            fi
        fi

        numtests=$(($numtests + 3))
    done
    rm -f $tmp1 $tmp2 $tmp3 $tmp4
    echo -e "numtests: $numtests\nfailed: $failed" > $result
}

# Using $MAX_PARALLEL_JOBS jobs if defined, unless the number of logical
# processors
if [ $(uname) == FreeBSD -o $(uname) == Darwin ]; then
    NPROC=$(sysctl -n hw.ncpu)
    max_parallel_jobs=${MAX_PARALLEL_JOBS:-${NPROC}}
else
    max_parallel_jobs=${MAX_PARALLEL_JOBS:-$(nproc)}
fi

results=$(mktemp -d /tmp/check-generated-result-XXXXXXXX)
running_jobs=0
pids=""
echo "checking ceph-dencoder generated test instances..."
echo "numgen type"
while read type; do
    run_in_background pids test_selected_type $type $results
    running_jobs=$(($running_jobs + 1))
    if [ "$running_jobs" -eq "$max_parallel_jobs" ]; then
        wait_background pids
        running_jobs=0
    fi
done < <(ceph-dencoder list_types)
wait_background pids

numtests=$(cat $results/* 2>/dev/null | grep "numtests: " | awk '{sum += $2} END {print sum}')
failed=$(cat $results/* 2>/dev/null | grep "failed: " | awk '{sum += $2} END {print sum}')
rm -rf $results

if [ $failed -gt 0 ]; then
    echo "FAILED $failed / $numtests tests."
    exit 1
fi
echo "passed $numtests tests."
