#!/usr/bin/env bash

set -e

stop() {
    if [ "$REMOTE" == "false" ]; then
        cd ${FULL_PATH_BUILD_DIR}
        ../src/stop.sh
    fi
    exit $1
}

BASE_URL=''
DEVICE=''
E2E_LOGIN_USER=''
E2E_LOGIN_PWD=''
REMOTE='false'

while getopts 'd:p:r:u:' flag; do
  case "${flag}" in
    d) DEVICE=$OPTARG;;
    p) E2E_LOGIN_PWD=$OPTARG;;
    r) REMOTE='true'
       BASE_URL=$OPTARG;;
    u) E2E_LOGIN_USER=$OPTARG;;
  esac
done

if [ "$DEVICE" == "" ]; then
    if [ -x "$(command -v google-chrome)" ] || [ -x "$(command -v google-chrome-stable)" ]; then
        DEVICE="chrome"
    elif [ -x "$(command -v docker)" ]; then
        DEVICE="docker"
    else
        echo "ERROR: Chrome and Docker not found. You need to install one of  \
them to run the e2e frontend tests."
        stop 1
    fi
fi

DASH_DIR=`pwd`

[ -z "$BUILD_DIR" ] && BUILD_DIR=build

cd ../../../../${BUILD_DIR}
FULL_PATH_BUILD_DIR=`pwd`

if [ "$BASE_URL" == "" ]; then
    MGR=2 RGW=1 ../src/vstart.sh -n -d
    sleep 10

    # Create an Object Gateway User
    ./bin/radosgw-admin user create --uid=dev --display-name=Developer --system
    # Set the user-id
    ./bin/ceph dashboard set-rgw-api-user-id dev
    # Obtain and set access and secret key for the previously created user. $() is safer than backticks `..`
    ./bin/ceph dashboard set-rgw-api-access-key $(./bin/radosgw-admin user info --uid=dev | jq -r .keys[0].access_key)
    ./bin/ceph dashboard set-rgw-api-secret-key $(./bin/radosgw-admin user info --uid=dev | jq -r .keys[0].secret_key)
    # Set SSL verify to False
    ./bin/ceph dashboard set-rgw-api-ssl-verify False

    BASE_URL=$(./bin/ceph mgr services | jq -r .dashboard)
fi

export BASE_URL E2E_LOGIN_USER E2E_LOGIN_PWD

cd $DASH_DIR/frontend

[[ "$(command -v npm)" == '' ]] && . ${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/node-env/bin/activate

if [ "$DEVICE" == "chrome" ]; then
    npm run e2e:ci || stop 1
    stop 0
elif [ "$DEVICE" == "docker" ]; then
    failed=0
    cat <<EOF > .env
BASE_URL
E2E_LOGIN_USER
E2E_LOGIN_PWD
EOF
    docker run --rm -v $(pwd):/ceph --env-file .env --name=e2e --network=host --entrypoint "" \
        docker.io/rhcsdashboard/e2e npm run e2e:ci || failed=1
    stop $failed
else
    echo "ERROR: Device not recognized. Valid devices are 'chrome' and 'docker'."
    stop 1
fi
