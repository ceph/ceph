#!/bin/bash

set -e
set -o pipefail


KEEPALIVED_DEBUG=${KEEPALIVED_DEBUG:-false}
KEEPALIVED_KUBE_APISERVER_CHECK=${KEEPALIVED_KUBE_APISERVER_CHECK:-false}
KEEPALIVED_CONF=${KEEPALIVED_CONF:-/etc/keepalived/keepalived.conf}
KEEPALIVED_VAR_RUN=${KEEPALIVED_VAR_RUN:-/var/run/keepalived}

if [[ ${KEEPALIVED_DEBUG,,} == 'true' ]]; then
  kd_cmd="/usr/sbin/keepalived -n -l -D -f $KEEPALIVED_CONF"
else
  kd_cmd="/usr/sbin/keepalived -n -l -f $KEEPALIVED_CONF"
fi

KEEPALIVED_CMD=${KEEPALIVED_CMD:-"$kd_cmd"}

rm -fr "$KEEPALIVED_VAR_RUN"

exec $KEEPALIVED_CMD