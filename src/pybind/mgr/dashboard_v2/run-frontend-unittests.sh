#!/usr/bin/env bash

set -e

cd $CEPH_ROOT/src/pybind/mgr/dashboard_v2/frontend

npm run build -- --prod
npm run test -- --browsers PhantomJS --watch=false
npm run lint
