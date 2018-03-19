#!/usr/bin/env bash

set -e

cd $CEPH_ROOT/src/pybind/mgr/dashboard/frontend

npm run build -- --prod
npm run test -- --browsers PhantomJS --watch=false
npm run lint
