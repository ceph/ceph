#!/usr/bin/env bash

set -eo pipefail

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

get_build_py_version() {
    CURR_DIR=`pwd`
    BUILD_DIR="$CURR_DIR/../../../../build"
    cd $BUILD_DIR

    CEPH_MGR_PY_VERSION_MAJOR=$(get_cmake_variable MGR_PYTHON_VERSION | cut -d '.' -f1)
    if [ -n "$CEPH_MGR_PY_VERSION_MAJOR" ]; then
        CEPH_PY_VERSION_MAJOR=${CEPH_MGR_PY_VERSION_MAJOR}
    else
        if [ $(get_cmake_variable WITH_PYTHON2) = ON ]; then
            CEPH_PY_VERSION_MAJOR=2
        else
            CEPH_PY_VERSION_MAJOR=3
        fi
    fi
}

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
    virtualenv --python=${TEUTHOLOGY_PYTHON_BIN:-/usr/bin/python} coverage-venv
    source coverage-venv/bin/activate
    cd $CURR_DIR
    pip install coverage==4.5.2
    COVERAGE_PATH=$(python -c "import sysconfig; print(sysconfig.get_paths()['platlib'])")
    deactivate
}

on_tests_error() {
    if [[ -n "$JENKINS_HOME" ]]; then
        CEPH_OUT_DIR=${CEPH_OUT_DIR:-"$BUILD_DIR"/out}
        MGR_LOG_FILES=$(find "$CEPH_OUT_DIR" -iname "mgr.*.log" | tr '\n' ' ')
        MGR_LOG_FILE_LAST_LINES=60000
        for mgr_log_file in ${MGR_LOG_FILES[@]}; do
            printf "\n\nDisplaying last ${MGR_LOG_FILE_LAST_LINES} lines of: $mgr_log_file\n\n"
            tail -n ${MGR_LOG_FILE_LAST_LINES} $mgr_log_file
            printf "\n\nEnd of: $mgr_log_file\n\n"
        done
        printf "\n\nTEST FAILED.\n\n"
    fi
}

run_teuthology_tests() {
    trap on_tests_error ERR

    cd "$BUILD_DIR"
    find ../src/pybind/mgr/dashboard/ -name '*.pyc' -exec rm -f {} \;

    OPTIONS=''
    TEST_CASES=''
    if [[ "$@" == '' || "$@" == '--create-cluster-only' ]]; then
      TEST_CASES=`for i in \`ls $BUILD_DIR/../qa/tasks/mgr/dashboard/test_*\`; do F=$(basename $i); M="${F%.*}"; echo -n " tasks.mgr.dashboard.$M"; done`
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

    export PATH=$BUILD_DIR/bin:$PATH
    source $TEMP_DIR/venv/bin/activate # Run after setting PATH as it does the last PATH export.
    export LD_LIBRARY_PATH=$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$BUILD_DIR/lib
    local source_dir=$(dirname "$BUILD_DIR")
    local pybind_dir=$source_dir/src/pybind
    local python_common_dir=$source_dir/src/python-common
    # In CI environment we set python paths inside build (where you find the required frontend build: "dist" dir).
    if [[ -n "$JENKINS_HOME" ]]; then
        export PYBIND=$BUILD_DIR/src/pybind
        pybind_dir=$PYBIND
    fi
    export PYTHONPATH=$source_dir/qa:$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$pybind_dir:$python_common_dir:${COVERAGE_PATH}
    export RGW=${RGW:-1}

    export COVERAGE_ENABLED=true
    export COVERAGE_FILE=.coverage.mgr.dashboard
    find . -iname "*${COVERAGE_FILE}*" -type f -delete

    python ../qa/tasks/vstart_runner.py --ignore-missing-binaries $OPTIONS $TEST_CASES

    deactivate
    cd $CURR_DIR
}

cleanup_teuthology() {
    cd "$BUILD_DIR"
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
    unset BUILD_DIR
    unset COVERAGE_PATH
    unset get_build_py_version
    unset setup_teuthology
    unset setup_coverage
    unset on_tests_error
    unset run_teuthology_tests
    unset cleanup_teuthology
}

get_build_py_version
setup_teuthology
setup_coverage
run_teuthology_tests --create-cluster-only

# End sourced section. Do not exit shell when the script has been sourced.
set +e
return 2>/dev/null || set -eo pipefail

run_teuthology_tests "$@"
cleanup_teuthology
