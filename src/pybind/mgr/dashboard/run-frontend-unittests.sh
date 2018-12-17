#!/usr/bin/env bash

failed=false
: ${CEPH_ROOT:=$PWD/../../../../}
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
  echo -e "\nTry running 'npm run fix' to fix some linting errors. \
Some errors might need a manual fix."
fi

# I18N
npm run i18n
i18n_modified=`git status -s src/locale/messages.xlf`
if [[ ! -z $i18n_modified ]]; then
  echo "Please run 'npm run i18n' and commit the modified 'messages.xlf' file."
  failed=true
fi

i18n_lint=`grep -En "<source> |<source>$| </source>" src/locale/messages.xlf`
if [[ ! -z $i18n_lint ]]; then
  echo -e "The following source translations in 'messages.xlf' need to be \
fixed, please check the I18N suggestions in 'HACKING.rst':\n"
  echo "${i18n_lint}"
  failed=true
fi

if [ `uname` != "FreeBSD" ]; then
  deactivate
fi

if [ "$failed" = "true" ]; then
  exit 1
fi
