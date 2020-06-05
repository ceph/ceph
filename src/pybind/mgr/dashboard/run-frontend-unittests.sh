#!/usr/bin/env bash

failed=false
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
: ${CEPH_ROOT:=$SCRIPTPATH/../../../../}

cd $CEPH_ROOT/src/pybind/mgr/dashboard/frontend
[ -z "$BUILD_DIR" ] && BUILD_DIR=build
if [ `uname` != "FreeBSD" ]; then
  .  $CEPH_ROOT/${BUILD_DIR}/src/pybind/mgr/dashboard/node-env/bin/activate
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
npm run i18n:extract
if [ $? -gt 0 ]; then
  failed=true
  echo -e "\nTranslations extraction has failed."
else
  i18n_lint=`awk '/<source> |<source>$| <\/source>/,/<\/context-group>/ {printf "%-4s ", NR; print}' src/locale/messages.xlf`
  if [ "$i18n_lint" ]; then
    echo -e "\nThe following source translations in 'messages.xlf' need to be \
  fixed, please check the I18N suggestions in 'HACKING.rst':\n"
    echo "${i18n_lint}"
    failed=true
  fi
fi

# npm resolutions
npm run fix:audit
resolutions=`git status | grep package-lock.json`
if [ "$resolutions" ]; then
  echo "Please run 'npm run fix:audit' before committing."
  failed=true
fi

if [ `uname` != "FreeBSD" ]; then
  deactivate
fi

if [ "$failed" = "true" ]; then
  exit 1
fi
