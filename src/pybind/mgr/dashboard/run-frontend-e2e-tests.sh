#!/usr/bin/env bash

set -e

start_ceph() {
    cd $FULL_PATH_BUILD_DIR

    MGR=2 RGW=1 ../src/vstart.sh -n -d
    sleep 10

    set -x

    # Create an Object Gateway User
    ./bin/ceph dashboard set-rgw-credentials

    # Set SSL verify to False
    ./bin/ceph dashboard set-rgw-api-ssl-verify False

    CYPRESS_BASE_URL=$(./bin/ceph mgr services | jq -r .dashboard)

    set +x
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
                [ -x "$(command -v chrome)" ] || [ -x "$(command -v google-chrome)" ] ||
                [ -x "$(command -v google-chrome-stable)" ] || failed=true
                ;;
            chromium)
                [ -x "$(command -v chromium)" ] || [ -x "$(command -v chromium-browser)" ] || failed=true
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

[[ "$(command -v npm)" == '' ]] && . ${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/frontend/node-env/bin/activate

: ${CYPRESS_CACHE_FOLDER:="${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/cypress"}

export CYPRESS_BASE_URL CYPRESS_CACHE_FOLDER CYPRESS_LOGIN_USER CYPRESS_LOGIN_PWD NO_COLOR

check_device_available

if [ "$CYPRESS_BASE_URL" == "" ]; then
    start_ceph
fi

cd $DASH_DIR/frontend

# Remove existing XML results
rm -f cypress/reports/results-*.xml || true

case "$DEVICE" in
    docker)
        failed=0
        CYPRESS_VERSION=$(cat package.json | grep '"cypress"' | grep -o "[0-9]\.[0-9]\.[0-9]")
        docker run \
            -v $(pwd):/e2e \
            -w /e2e \
            --env CYPRESS_BASE_URL \
            --env CYPRESS_LOGIN_USER \
            --env CYPRESS_LOGIN_PWD \
            --name=e2e \
            --network=host \
            cypress/included:${CYPRESS_VERSION} || failed=1
        stop $failed
        ;;
    *)
        npx cypress run $CYPRESS_ARGS --browser $DEVICE --headless || stop 1
        ;;
esac

stop 0
