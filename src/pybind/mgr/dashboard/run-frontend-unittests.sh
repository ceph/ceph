#!/usr/bin/env bash

set -e

cd $CEPH_ROOT/src/pybind/mgr/dashboard/frontend

config='src/unit-test-configuration.ts'
if [ -e $config ]; then
  mv $config ${config}_old
fi
cp ${config}.sample $config

.  $CEPH_ROOT/build/src/pybind/mgr/dashboard/node-env/bin/activate

npm run build -- --prod
npm run test:ci
npm run lint

rm $config
if [ -e ${config}_old ]; then
  mv ${config}_old $config
fi

deactivate
