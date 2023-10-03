#!/usr/bin/env bash

set -eEx

: ${CEPH_DEV_FOLDER:=${PWD}}

cd src/pybind/mgr/dashboard/frontend

# FRONTEND_BUILD_OPTS='--configuration=production'

npm ci
npm run build ${FRONTEND_BUILD_OPTS}

cd ${CEPH_DEV_FOLDER}

cp ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/ci/rook/Dockerfile src/pybind/mgr

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr

docker build -t 'ceph/rook-dashboard:e2e' .

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/ci/rook

# original="image."
# image="image: ceph\/rook-dashboard:e2e$"
# imagePullPolicy="imagePullPolicy: IfNotPresent"


wget -O cluster-test.yaml https://raw.githubusercontent.com/rook/rook/master/deploy/examples/cluster-test.yaml
# sed 's/image. */image: ceph\/rook-dashboard:e2e$/&\n\1use: package_manager/' cluster-test.yaml > cluster-test-updated.yaml
# sed 's/image.*/image: ceph\/rook-dashboard:e2e$&\n\1imagePullPolicy: IfNotPresent/' cluster-test.yaml > cluster-test-updated.yaml
sed -i -e "s|image: *|image: ceph\/rook-dashboard:e2e$|; /image: ceph\/rook-dashboard:e2e$/a imagePullPolicy: IfNotPresent" cluster-test.yaml > cluster-test-updated.yaml

minikube delete
minikube start --memory="4096" --cpus="2" --driver="kvm2" --disk-size=7g --extra-disks=1
eval $(minikube docker-env -p minikube)

KUBECTL="minikube kubectl --"

$KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/crds.yaml
$KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/common.yaml
$KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/operator.yaml
$KUBECTL create -f cluster-test-updated.yaml

$KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/dashboard-external-http.yaml

$KUBECTL rollout status deployment rook-ceph-operator -n rook-ceph --timeout=120s
PHASE=$($KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}')
echo "PHASE: $PHASE"

while [[ -z $($KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}' | grep "Ready") ]]; do
    echo "Waiting for cluster to be ready..."
    sleep 5
done

DASHBOARD_PASSWORD=$($KUBECTL -n rook-ceph get secret rook-ceph-dashboard-password -o jsonpath="{['data']['password']}" | base64 --decode && echo)
IP_ADDR=$($KUBECTL get po --selector="app=rook-ceph-mgr" -n rook-ceph --output jsonpath='{.items[*].status.hostIP}')
PORT="$($KUBECTL -n rook-ceph -o=jsonpath='{.spec.ports[?(@.name == "dashboard")].nodePort}' get services rook-ceph-mgr-dashboard-external-http)"

BASE_URL="http://$IP_ADDR:$PORT"
echo "IP_ADDRES: $BASE_URL"
echo "PASSWORD: $DASHBOARD_PASSWORD"

: ${CYPRESS_BASE_URL:=$BASE_URL}
: ${CYPRESS_LOGIN_USER:='admin'}
: ${CYPRESS_LOGIN_PWD:=$DASHBOARD_PASSWORD}
: ${CYPRESS_ARGS:=''}


export CYPRESS_BASE_URL CYPRESS_LOGIN_USER CYPRESS_LOGIN_PWD

cypress_run () {
    local specs="$1"
    local timeout="$2"
    local override_config="excludeSpecPattern=*.po.ts,retries=0,specPattern=${specs},chromeWebSecurity=false"
    if [[ -n "$timeout" ]]; then
        override_config="${override_config},defaultCommandTimeout=${timeout}"
    fi

    rm -f cypress/reports/results-*.xml || true

    npx --no-install cypress run ${CYPRESS_ARGS} --browser chrome --headless --config "$override_config"
}

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/frontend

cypress_run "cypress/e2e/**/*.e2e-spec.ts"
