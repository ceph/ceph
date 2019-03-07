#!/usr/bin/env bash

set -e

stop() {
    if [ "$REMOTE" == "false" ]; then
        cd $BUILD_DIR
        ../src/stop.sh
    fi
    exit
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
        stop
    fi
fi

DASH_DIR=`pwd`

cd ../../../../build
BUILD_DIR=`pwd`

if [ "$BASE_URL" == "" ]; then
    MGR=2 RGW=1 ../src/vstart.sh -n -d
    sleep 10

    BASE_URL=`./bin/ceph mgr services | jq .dashboard`
    BASE_URL=${BASE_URL//\"}
fi

cd $DASH_DIR/frontend
. $BUILD_DIR/src/pybind/mgr/dashboard/node-env/bin/activate
npm ci
npm run build -- --prod

if [ $DEVICE == "chrome" ]; then
    npm run e2e -- --serve=false --base-href $BASE_URL || stop
elif [ $DEVICE == "docker" ]; then
    docker run -d -v $(pwd):/workdir --net=host --name angular-e2e-container rogargon/angular-e2e || stop
    docker exec angular-e2e-container npm run e2e -- --serve=false --base-href $BASE_URL
    docker stop angular-e2e-container
    docker rm angular-e2e-container
else
    echo "ERROR: Device not recognized. Valid devices are 'chrome' and 'docker'."
fi

stop
