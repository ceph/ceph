#!/usr/bin/env bash

set -e

cd $CEPH_ROOT/src/pybind/mgr/dashboard/frontend

.  $CEPH_ROOT/build/src/pybind/mgr/dashboard/node-env/bin/activate

npm run build -- --prod
npm run test -- --browsers PhantomJS --watch=false
npm run lint

deactivate
