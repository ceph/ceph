#!/usr/bin/env bash

set -e

cd $(dirname $0)/frontend

npm run build -- --prod
npm run test -- --browsers PhantomJS --watch=false
npm run lint
