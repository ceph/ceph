#!/usr/bin/env bash

set -e

stop() {
    if [ "$REMOTE" == "false" ]; then
        cd $BUILD_DIR
        ../src/stop.sh
    fi
    exit $1
}

BASE_URL=''
DEVICE=''
REMOTE='false'

while getopts 'd:r:' flag; do
  case "${flag}" in
    d) DEVICE=$OPTARG;;
    r) REMOTE='true'
       # jq is expecting a string literal, otherwise it will fail on the url ':'.
       # We need to ensure that jq gets a json string for assignment; we achieve
       # that by introducing literal double quotes (i.e., '"').
       BASE_URL='"'$OPTARG'"';;
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

cd ../../../../build
BUILD_DIR=`pwd`

if [ "$BASE_URL" == "" ]; then
    MGR=2 RGW=1 ../src/vstart.sh -n -d
    sleep 10

    # Create an Object Gateway User
    ./bin/radosgw-admin user create --uid=dev --display-name=Developer --system
    # Set the user-id
    ./bin/ceph dashboard set-rgw-api-user-id dev
    # Obtain and set access and secret key for the previously created user
    RGW_ACCESS_KEY_FILE="/tmp/rgw-user-access-key.txt"
    printf "$(./bin/radosgw-admin user info --uid=dev | jq -r .keys[0].access_key)" > "${RGW_ACCESS_KEY_FILE}"
    ./bin/ceph dashboard set-rgw-api-access-key -i "${RGW_ACCESS_KEY_FILE}"
    RGW_SECRET_KEY_FILE="/tmp/rgw-user-secret-key.txt"
    printf "$(./bin/radosgw-admin user info --uid=dev | jq -r .keys[0].secret_key)" > "${RGW_SECRET_KEY_FILE}"
    ./bin/ceph dashboard set-rgw-api-secret-key -i "${RGW_SECRET_KEY_FILE}"
    # Set SSL verify to False
    ./bin/ceph dashboard set-rgw-api-ssl-verify False

    BASE_URL=`./bin/ceph mgr services | jq .dashboard`
fi

cd $DASH_DIR/frontend
jq .[].target=$BASE_URL proxy.conf.json.sample > proxy.conf.json

[ -z $(command -v npm) ] && . $BUILD_DIR/src/pybind/mgr/dashboard/node-env/bin/activate
npm ci

if [ $DEVICE == "chrome" ]; then
    npm run e2e -- --dev-server-target --baseUrl=$(echo $BASE_URL | tr -d '"') || stop 1
    stop 0
elif [ $DEVICE == "docker" ]; then
    failed=0
    docker run -d -v $(pwd):/workdir --net=host --name angular-e2e-container rogargon/angular-e2e || failed=1
    docker exec angular-e2e-container npm run e2e || failed=1
    docker stop angular-e2e-container
    docker rm angular-e2e-container
    stop $failed
else
    echo "ERROR: Device not recognized. Valid devices are 'chrome' and 'docker'."
    stop 1
fi
