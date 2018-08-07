#!/usr/bin/env bash

failed=false
cd $CEPH_ROOT/src/pybind/mgr/dashboard/frontend
if [ `uname` != "FreeBSD" ]; then
  .  $CEPH_ROOT/build/src/pybind/mgr/dashboard/node-env/bin/activate
fi

# Build
npm run build -- --prod --progress=false || failed=true

# Unit Tests
config='src/unit-test-configuration.ts'
if [ -e $config ]; then
  mv $config ${config}_old
fi
cp ${config}.sample $config

npm run test:ci || failed=true

rm $config
if [ -e ${config}_old ]; then
  mv ${config}_old $config
fi

# Linting
npm run lint --silent
if [ $? -gt 0 ]; then
  failed=true
  echo -e "\nTry running 'npm run lint -- --fix' to fix some linting errors."
fi

npm run prettier:lint --silent
if [ $? -gt 0 ]; then
  failed=true
  echo -e "\nTry running 'npm run prettier' to fix linting errors."
fi

if [ `uname` != "FreeBSD" ]; then
  deactivate
fi 

if [ "$failed" = "true" ]; then
  exit 1
fi
