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
REMOTE='false'

while getopts 'd:r:' flag; do
  case "${flag}" in
    d) DEVICE=$OPTARG;;
    r) REMOTE='true'
       BASE_URL=$OPTARG;;
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

    # Disable PG autoscaling for new pools. This is a temporary workaround.
    # e2e tests for pools should be adapted to remove this workaround after
    # these issues are resolved:
    # - https://tracker.ceph.com/issues/38227
    # - https://tracker.ceph.com/issues/42638
    ./bin/ceph config set global osd_pool_default_pg_autoscale_mode off

    BASE_URL=$(./bin/ceph mgr services | jq -r .dashboard)
fi

export BASE_URL

cd $DASH_DIR/frontend
jq .[].target=\"$BASE_URL\" proxy.conf.json.sample > proxy.conf.json

[[ "$(command -v npm)" == '' ]] && . ${FULL_PATH_BUILD_DIR}/src/pybind/mgr/dashboard/node-env/bin/activate

if [ "$DEVICE" == "chrome" ]; then
    npm run e2e:ci || stop 1
    stop 0
elif [ "$DEVICE" == "docker" ]; then
    failed=0
    docker run -d -v $(pwd):/workdir --net=host --name angular-e2e-container rogargon/angular-e2e || failed=1
    docker exec -e BASE_URL=$BASE_URL angular-e2e-container npm run e2e:ci || failed=1
    docker stop angular-e2e-container
    docker rm angular-e2e-container
    stop $failed
else
    echo "ERROR: Device not recognized. Valid devices are 'chrome' and 'docker'."
    stop 1
fi
