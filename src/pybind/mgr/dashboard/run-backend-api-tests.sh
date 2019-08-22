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
    TEUTHOLOGY_PY_REQS="
apache-libcloud==2.2.1
asn1crypto==0.22.0
backports.ssl-match-hostname==3.5.0.1
bcrypt==3.1.4
certifi==2018.1.18
cffi==1.10.0
chardet==3.0.4
configobj==5.0.6
cryptography==2.1.4
enum34==1.1.6
gevent==1.2.2
greenlet==0.4.13
idna==2.5
ipaddress==1.0.18
Jinja2==2.9.6
manhole==1.5.0
MarkupSafe==1.0
netaddr==0.7.19
packaging==16.8
paramiko==2.4.0
pexpect==4.4.0
psutil==5.4.3
ptyprocess==0.5.2
pyasn1==0.2.3
pycparser==2.17
PyNaCl==1.2.1
pyparsing==2.2.0
python-dateutil==2.6.1
PyYAML==3.12
requests==2.18.4
six==1.10.0
urllib3==1.22
"

    cd $TEMP_DIR
    virtualenv --python=${TEUTHOLOGY_PYTHON_BIN:-/usr/bin/python} venv
    source venv/bin/activate
    pip install 'setuptools >= 12'
    eval pip install $TEUTHOLOGY_PY_REQS
    pip install -r $CURR_DIR/requirements.txt

    git clone --depth 1 https://github.com/ceph/teuthology.git

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
    COVERAGE_PATH=$(python -c "import sysconfig; print(sysconfig.get_paths()['purelib'])")
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
    export PYTHONPATH=$TEMP_DIR/teuthology:$source_dir/qa:$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$pybind_dir:$python_common_dir:${COVERAGE_PATH}
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
