#!/usr/bin/env bash

# run from ./

# creating temp directory to store virtualenv and teuthology
TEMP_DIR=`mktemp -d`

get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

read -r -d '' TEUTHOLOFY_PY_REQS <<EOF
apache-libcloud==2.2.1 \
asn1crypto==0.22.0 \
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


CURR_DIR=`pwd`

cd $TEMP_DIR

virtualenv --python=/usr/bin/python venv
source venv/bin/activate
eval pip install $TEUTHOLOFY_PY_REQS
pip install -r $CURR_DIR/requirements.txt
deactivate

git clone https://github.com/ceph/teuthology.git

cd $CURR_DIR
cd ../../../../build

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

export COVERAGE_ENABLED=true
export COVERAGE_FILE=.coverage.mgr.dashboard

MGR=2 RGW=1 ../src/vstart.sh -n -d
sleep 10

source $TEMP_DIR/venv/bin/activate
BUILD_DIR=`pwd`

TEST_CASES=`for i in \`ls $BUILD_DIR/../qa/tasks/mgr/dashboard_v2/test_*\`; do F=$(basename $i); M="${F%.*}"; echo -n " tasks.mgr.dashboard_v2.$M"; done`

export PATH=$BUILD_DIR/bin:$PATH
export LD_LIBRARY_PATH=$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/:$BUILD_DIR/lib
export PYTHONPATH=$TEMP_DIR/teuthology:$BUILD_DIR/../qa:$BUILD_DIR/lib/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}/
eval python ../qa/tasks/vstart_runner.py tasks.mgr.test_dashboard_v2 $TEST_CASES

deactivate
killall ceph-mgr
sleep 10
../src/stop.sh
sleep 5

cd $CURR_DIR
rm -rf $TEMP_DIR

