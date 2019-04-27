#!/usr/bin/env bash

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

# creating temp directory to store virtualenv and teuthology

get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

setup_teuthology() {
    TEMP_DIR=`mktemp -d`

    CURR_DIR=`pwd`
    BUILD_DIR="$CURR_DIR/../../../../build"

    read -r -d '' TEUTHOLOGY_PY_REQS <<EOF
apache-libcloud==2.2.1 \
asn1crypto==0.22.0 \
backports.ssl-match-hostname==3.5.0.1 \
bcrypt==3.1.4 \
certifi==2018.1.18 \
cffi==1.10.0 \
chardet==3.0.4 \
configobj==5.0.6 \
cryptography==2.1.4 \
enum34==1.1.6 \
gevent==1.2.2 \
greenlet==0.4.13 \
idna==2.5 \
ipaddress==1.0.18 \
Jinja2==2.9.6 \
manhole==1.5.0 \
MarkupSafe==1.0 \
netaddr==0.7.19 \
packaging==16.8 \
paramiko==2.4.0 \
pexpect==4.4.0 \
psutil==5.4.3 \
ptyprocess==0.5.2 \
pyasn1==0.2.3 \
pycparser==2.17 \
PyNaCl==1.2.1 \
pyparsing==2.2.0 \
python-dateutil==2.6.1 \
PyYAML==3.12 \
requests==2.18.4 \
six==1.10.0 \
urllib3==1.22
EOF



    cd $TEMP_DIR

    virtualenv --python=/usr/bin/python venv
    source venv/bin/activate
    eval pip install $TEUTHOLOGY_PY_REQS
    pip install -r $CURR_DIR/requirements.txt
    deactivate

    git clone --depth 1 https://github.com/ceph/teuthology.git

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

    cd $CURR_DIR

    COVERAGE_VERSION=$(cat requirements.txt | grep coverage)
    if [[ "$CEPH_MGR_PY_VERSION_MAJOR" == '3' ]]; then
        pip3 install "$COVERAGE_VERSION"
    else
        pip install "$COVERAGE_VERSION"
    fi
}

run_teuthology_tests() {
    cd "$BUILD_DIR"
    find ../src/pybind/mgr/dashboard/ -name '*.pyc' -exec rm -f {} \;
    source $TEMP_DIR/venv/bin/activate

    OPTIONS=''
    TEST_CASES=''
    if [[ "$@" == '' || "$@" == '--create-cluster-only' ]]; then
      TEST_CASES=`for i in \`ls $BUILD_DIR/../qa/tasks/mgr/dashboard/test_*\`; do F=$(basename $i); M="${F%.*}"; echo -n " tasks.mgr.dashboard.$M"; done`
      TEST_CASES="tasks.mgr.test_module_selftest tasks.mgr.test_dashboard $TEST_CASES"
      if [[ "$@" == '--create-cluster-only' ]]; then
        OPTIONS="$@"
      fi
    else
      for t in "$@"; do
        TEST_CASES="$TEST_CASES $t"
      done
    fi

    export PATH=$BUILD_DIR/bin:$PATH
    export LD_LIBRARY_PATH=$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$BUILD_DIR/lib
    export PYTHONPATH=$TEMP_DIR/teuthology:$BUILD_DIR/../qa:$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$BUILD_DIR/../src/pybind
    if [[ -z "$RGW" ]]; then
        export RGW=1
    fi

    export COVERAGE_ENABLED=true
    export COVERAGE_FILE=.coverage.mgr.dashboard
    find . -iname "*${COVERAGE_FILE}*" -type f -delete

    eval python ../qa/tasks/vstart_runner.py $OPTIONS $TEST_CASES

    deactivate
    cd $CURR_DIR
}

cleanup_teuthology() {
    cd "$BUILD_DIR"
    killall ceph-mgr
    sleep 10
    if [[ "$COVERAGE_ENABLED" == 'true' ]]; then
        source $TEMP_DIR/venv/bin/activate
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
    unset setup_teuthology
    unset run_teuthology_tests
    unset cleanup_teuthology
}

setup_teuthology
run_teuthology_tests --create-cluster-only

# End sourced section
return 2> /dev/null

run_teuthology_tests "$@"
cleanup_teuthology
