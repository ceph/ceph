#!/usr/bin/env bash

# cross shell: Are we sourced?
# Source: https://stackoverflow.com/a/28776166/3185053
([[ -n $ZSH_EVAL_CONTEXT && $ZSH_EVAL_CONTEXT =~ :file$ ]] ||
 [[ -n $KSH_VERSION && $(cd "$(dirname -- "$0")" &&
    printf '%s' "${PWD%/}/")$(basename -- "$0") != "${.sh.file}" ]] ||
 [[ -n $BASH_VERSION ]] && (return 0 2>/dev/null)) && sourced=1 || sourced=0

if [ "$sourced" -eq 0 ] ; then
    set -eo pipefail
fi

if [[ "$1" = "-h" || "$1" = "--help" ]]; then
	echo "Usage (run from ./):"
	echo -e "\t./run-backend-api-tests.sh"
	echo -e "\t./run-backend-api-tests.sh [tests]..."
	echo
	echo "Example:"
	echo -e "\t./run-backend-api-tests.sh tasks.mgr.dashboard.test_pool.DashboardTest"
	echo
	echo "Or source this script. This allows to re-run tests faster:"
	echo -e "\tsource run-backend-api-tests.sh"
	echo -e "\trun_teuthology_tests [tests]..."
	echo -e "\tcleanup_teuthology"
	echo

	exit 0
fi

get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

[ -z "$BUILD_DIR" ] && BUILD_DIR=build
CURR_DIR=`pwd`
LOCAL_BUILD_DIR="$CURR_DIR/../../../../$BUILD_DIR"

setup_teuthology() {
    TEMP_DIR=`mktemp -d`
    cd $TEMP_DIR

    virtualenv --python=${TEUTHOLOGY_PYTHON_BIN:-/usr/bin/python3} venv
    source venv/bin/activate
    pip install 'setuptools >= 12'
    pip install git+https://github.com/ceph/teuthology#egg=teuthology[test]
    pushd $CURR_DIR
    pip install -r requirements.txt -c constraints.txt
    popd

    deactivate
}

setup_coverage() {
    # In CI environment we cannot install coverage in system, so we install it in a dedicated venv
    # so only coverage is available when adding this path.
    cd $TEMP_DIR
    virtualenv --python=/usr/bin/python3 coverage-venv
    source coverage-venv/bin/activate
    cd $CURR_DIR
    pip install coverage==4.5.2
    COVERAGE_PATH=$(python -c "import sysconfig; print(sysconfig.get_paths()['platlib'])")
    deactivate
}

display_log() {
    local daemon=$1
    shift
    local lines=$1
    shift

    local log_files=$(find "$CEPH_OUT_DIR" -iname "${daemon}.*.log" | tr '\n' ' ')
    for log_file in ${log_files[@]}; do
        printf "\n\nDisplaying last ${lines} lines of: ${log_file}\n\n"
        tail -n ${lines} $log_file
        printf "\n\nEnd of: ${log_file}\n\n"
    done
    printf "\n\nTEST FAILED.\n\n"
}

on_tests_error() {
    if [[ -n "$JENKINS_HOME" ]]; then
        CEPH_OUT_DIR=${CEPH_OUT_DIR:-"$LOCAL_BUILD_DIR"/out}
        display_log "mgr" 60000
        display_log "osd" 60000
    fi
}

run_teuthology_tests() {
    trap on_tests_error ERR

    cd "$LOCAL_BUILD_DIR"
    find ../src/pybind/mgr/dashboard/ -name '*.pyc' -exec rm -f {} \;

    OPTIONS=''
    TEST_CASES=''
    if [[ "$@" == '' || "$@" == '--create-cluster-only' ]]; then
      TEST_CASES=`for i in \`ls $LOCAL_BUILD_DIR/../qa/tasks/mgr/dashboard/test_*\`; do F=$(basename $i); M="${F%.*}"; echo -n " tasks.mgr.dashboard.$M"; done`
      # Mgr selftest module tests have to be run at the end as they stress the mgr daemon.
      TEST_CASES="tasks.mgr.test_dashboard $TEST_CASES tasks.mgr.test_module_selftest"
      if [[ "$@" == '--create-cluster-only' ]]; then
        OPTIONS="$@"
      fi
    else
      for t in "$@"; do
        TEST_CASES="$TEST_CASES $t"
      done
    fi

    export PATH=$LOCAL_BUILD_DIR/bin:$PATH
    source $TEMP_DIR/venv/bin/activate # Run after setting PATH as it does the last PATH export.
    export LD_LIBRARY_PATH=$LOCAL_BUILD_DIR/lib/cython_modules/lib.3/:$LOCAL_BUILD_DIR/lib
    local source_dir=$(dirname "$LOCAL_BUILD_DIR")
    local pybind_dir=$source_dir/src/pybind
    local python_common_dir=$source_dir/src/python-common
    # In CI environment we set python paths inside build (where you find the required frontend build: "dist" dir).
    if [[ -n "$JENKINS_HOME" ]]; then
        export PYBIND=$LOCAL_BUILD_DIR/src/pybind
        pybind_dir=$PYBIND
    fi
    export PYTHONPATH=$source_dir/qa:$LOCAL_BUILD_DIR/lib/cython_modules/lib.3/:$pybind_dir:$python_common_dir:${COVERAGE_PATH}
    export RGW=${RGW:-1}

    export COVERAGE_ENABLED=true
    export COVERAGE_FILE=.coverage.mgr.dashboard
    find . -iname "*${COVERAGE_FILE}*" -type f -delete

    python ../qa/tasks/vstart_runner.py --ignore-missing-binaries $OPTIONS $(echo $TEST_CASES)

    deactivate
    cd $CURR_DIR
}

cleanup_teuthology() {
    cd "$LOCAL_BUILD_DIR"
    killall ceph-mgr
    sleep 10
    if [[ "$COVERAGE_ENABLED" == 'true' ]]; then
        source $TEMP_DIR/coverage-venv/bin/activate
        (coverage combine && coverage report) || true
        deactivate
    fi
    ../src/stop.sh
    sleep 5

    cd $CURR_DIR
    rm -rf $TEMP_DIR

    unset TEMP_DIR
    unset CURR_DIR
    unset LOCAL_BUILD_DIR
    unset COVERAGE_PATH
    unset setup_teuthology
    unset setup_coverage
    unset on_tests_error
    unset run_teuthology_tests
    unset cleanup_teuthology
}

setup_teuthology
setup_coverage
run_teuthology_tests --create-cluster-only

# End sourced section. Do not exit shell when the script has been sourced.
if [ "$sourced" -eq 1 ] ; then
    return
fi

run_teuthology_tests "$@"
cleanup_teuthology
