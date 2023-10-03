#!/usr/bin/env bash

set -ex

: ${CYPRESS_BASE_URL:=''}
: ${CYPRESS_LOGIN_USER:='admin'}
: ${CYPRESS_LOGIN_PWD:='password'}
: ${CYPRESS_ARGS:=''}
: ${DASHBOARD_PORT:='8443'}
