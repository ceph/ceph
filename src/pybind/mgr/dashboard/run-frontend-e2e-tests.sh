#!/usr/bin/env bash

set -e

start_ceph() {
    cd $FULL_PATH_BUILD_DIR

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

    CYPRESS_BASE_URL=$(./bin/ceph mgr services | jq -r .dashboard)
}

stop() {
    if [ "$REMOTE" == "false" ]; then
        cd ${FULL_PATH_BUILD_DIR}
        ../src/stop.sh
    fi
    exit $1
}

check_device_available() {
    failed=false

    if [ "$DEVICE" == "docker" ]; then
        [ -x "$(command -v docker)" ] || failed=true
    else
        cd $DASH_DIR/frontend
        npx cypress verify

        case "$DEVICE" in
            chrome)
                [ -x "$(command -v google-chrome)" ] || [ -x "$(command -v google-chrome-stable)" ] || failed=true
                ;;
            chromium)
                [ -x "$(command -v chromium)" ] || failed=true
                ;;
        esac
    fi

    if [ "$failed" = "true" ]; then
            echo "ERROR: $DEVICE not found. You need to install $DEVICE or \
    use a different device. Supported devices: chrome (default), chromium, electron or docker."
        stop 1
    fi
}

: ${CYPRESS_BASE_URL:=''}
: ${CYPRESS_LOGIN_PWD:=''}
: ${CYPRESS_LOGIN_USER:=''}
: ${DEVICE:="chrome"}
: ${NO_COLOR:=1}
: ${CYPRESS_ARGS:=''}
: ${REMOTE:='false'}

while getopts 'd:p:r:u:' flag; do
  case "${flag}" in
    d) DEVICE=$OPTARG;;
    p) CYPRESS_LOGIN_PWD=$OPTARG;;
    r) REMOTE='true'
       CYPRESS_BASE_URL=$OPTARG;;
    u) CYPRESS_LOGIN_USER=$OPTARG;;
  esac
done

DASH_DIR=`pwd`
[ -z "$BUILD_DIR" ] && BUILD_DIR=build
cd ../../../../${BUILD_DIR}
FULL_PATH_BUILD_DIR=`pwd`

[[ "$(command -v npm)" == '' ]] && . ${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/node-env/bin/activate

: ${CYPRESS_CACHE_FOLDER:="${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/cypress"}

export CYPRESS_BASE_URL CYPRESS_CACHE_FOLDER CYPRESS_LOGIN_USER CYPRESS_LOGIN_PWD NO_COLOR

check_device_available

if [ "$CYPRESS_BASE_URL" == "" ]; then
    start_ceph
fi

cd $DASH_DIR/frontend

case "$DEVICE" in
    docker)
        failed=0
        docker run \
            -v $(pwd):/e2e \
            -w /e2e \
            --env CYPRESS_BASE_URL \
            --env CYPRESS_LOGIN_USER \
            --env CYPRESS_LOGIN_PWD \
            --name=e2e \
            --network=host \
            cypress/included:4.4.0 || failed=1
        stop $failed
        ;;
    *)
        npx cypress run $CYPRESS_ARGS --browser $DEVICE --headless || stop 1
        ;;
esac

stop 0
