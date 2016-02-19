#!/bin/bash
#
# Script to run Tempest's test for verifying radosgw compliance
# with Swift API (a.k.a OpenStack Object Storage API v1).
set -ex

ROOTDIR=$(dirname $(readlink -f ${BASH_SOURCE[0]}))

KEYSTONE_HOME_DIR=/tmp/keystone
KEYSTONE_REPO_URL=https://git.openstack.org/openstack/keystone.git
KEYSTONE_RELEASE=stable/kilo
KEYSTONE_IP=127.0.0.1
KEYSTONE_PORT=5010
KEYSTONE_PORT_ADMIN=35367

RADOSGW_URL=http://127.0.0.1:8000/swift/v1

TEMPEST_HOME_DIR=/tmp/tempest
TEMPEST_REPO_URL=https://github.com/openstack/tempest.git
TEMPEST_RELEASE=rgw_testing
TEMPEST_RELEASE=master

keystone_download()
{
  rm -rf ${KEYSTONE_HOME_DIR}
  mkdir -p ${KEYSTONE_HOME_DIR}

  git clone --quiet -- ${KEYSTONE_REPO_URL} ${KEYSTONE_HOME_DIR}
  cd ${KEYSTONE_HOME_DIR} \
      && git checkout ${KEYSTONE_RELEASE}
}

keystone_make_venv()
{
  cd ${KEYSTONE_HOME_DIR}

  python tools/install_venv.py > /dev/null
}

keystone_configure()
{
  cd ${KEYSTONE_HOME_DIR}

  cp etc/keystone.conf.sample etc/keystone.conf

  sed -e "s/#public_port = 5000/public_port = ${KEYSTONE_PORT}/" \
      -i etc/keystone.conf
  sed -e "s/#admin_port = 35357/admin_port = ${KEYSTONE_PORT_ADMIN}/" \
      -i etc/keystone.conf

  # We also have to create DB schema for Keystone.
  tools/with_venv.sh keystone-manage db_sync
}

keystone_stop()
{
  if [[ -n "${KEYSTONE_SHELL_PID}" ]]; then
    kill -HUP ${KEYSTONE_SHELL_PID}
  fi
}

keystone_start()
{
  cd ${KEYSTONE_HOME_DIR}

  trap keystone_stop EXIT

  # tools/with_env.sh cannot be used here due to problems with
  # tearing down Keystone. We need to start a new shell instead,
  # activate the venv in its context and run keystone-all.
  (
    source .venv/bin/activate
    trap 'kill $(jobs -p)' EXIT
    keystone-all &
    wait
  ) &

  # Save PID of the subshell spawned for Keystone controll.
  KEYSTONE_SHELL_PID=$!

  # FIXME: a dirty hack
  sleep 2
}

get_id()
{
  echo `"$@" | grep ' id ' | awk '{print $4}'`
}

keystone_put_authinfo()
{
  cd ${KEYSTONE_HOME_DIR}

  export OS_AUTH_URL="http://${KEYSTONE_IP}:${KEYSTONE_PORT}/v2.0/"
  export SERVICE_ENDPOINT="http://${KEYSTONE_IP}:${KEYSTONE_PORT_ADMIN}/v2.0/"
  export SERVICE_TOKEN=ADMIN

  # tenants
  ADMIN_TENANT_ID=$(get_id tools/with_venv.sh keystone tenant-create \
                            --name admin \
                            --description "Admin Tenant")

  TEMPEST_TENANT_ID=$(get_id tools/with_venv.sh keystone tenant-create \
                            --name tempest \
                            --description "Tempest")

  # users
  ADMIN_USER_ID=$(get_id tools/with_venv.sh keystone user-create \
                            --name admin \
                            --pass ${SERVICE_TOKEN})

  TEMPEST_USER1_ID=$(get_id tools/with_venv.sh keystone user-create \
                            --name tempest_u1 \
                            --pass tempest \
                            --tenant-id "${TEMPEST_TENANT_ID}")

  TEMPEST_USER2_ID=$(get_id tools/with_venv.sh keystone user-create \
                            --name tempest_u2 \
                            --pass tempest \
                            --tenant-id "${TEMPEST_TENANT_ID}")

  # roles
  ADMIN_ROLE_ID=$(get_id tools/with_venv.sh keystone role-create \
                            --name admin)

  MEMBER_ROLE_ID=$(get_id tools/with_venv.sh keystone role-create \
                            --name Member)

  # users privileges
  tools/with_venv.sh keystone user-role-add \
                            --user-id ${ADMIN_USER_ID} \
                            --role-id ${ADMIN_ROLE_ID} \
                            --tenant-id ${ADMIN_TENANT_ID}

  tools/with_venv.sh keystone user-role-add \
                            --user-id ${TEMPEST_USER1_ID} \
                            --role-id ${MEMBER_ROLE_ID} \
                            --tenant-id ${TEMPEST_TENANT_ID}

  tools/with_venv.sh keystone user-role-add \
                            --user-id ${TEMPEST_USER2_ID} \
                            --role-id ${MEMBER_ROLE_ID} \
                            --tenant-id ${TEMPEST_TENANT_ID}

  # keystone service
  KEYSTONE_SERVICE_ID=$(get_id tools/with_venv.sh keystone service-create \
                            --name keystone \
                            --type identity \
                            --description "Keystone Identity Service")

  tools/with_venv.sh keystone endpoint-create --region RegionOne \
                            --service-id ${KEYSTONE_SERVICE_ID} \
                            --publicurl "http://127.0.1:\$(public_port)s/v2.0" \
                            --adminurl "http://127.0.0.1:\$(admin_port)s/v2.0" \
                            --internalurl "http://127.0.0.1:\$(public_port)s/v2.0"

  # object store service
  SWIFT_SERVICE_ID=$(get_id tools/with_venv.sh keystone service-create \
                            --name swift \
                            --type "object-store" \
                            --description "Swift Service")

  tools/with_venv.sh keystone endpoint-create --region RegionOne \
                            --service-id ${SWIFT_SERVICE_ID} \
                            --publicurl "${RADOSGW_URL}" \
                            --adminurl "${RADOSGW_URL}" \
                            --internalurl "${RADOSGW_URL}"
}

deploy_keystone()
{
  keystone_download

  if [[ -z "${KESTONE_SKIP_VENV}" ]]; then
    keystone_make_venv
  fi

  keystone_configure
  keystone_start
  keystone_put_authinfo
}

tempest_download()
{
  rm -rf ${TEMPEST_HOME_DIR}
  mkdir -p ${TEMPEST_HOME_DIR}

  git clone --quiet -- ${TEMPEST_REPO_URL} ${TEMPEST_HOME_DIR}
  cd ${TEMPEST_HOME_DIR} \
      && git checkout ${TEMPEST_RELEASE}
}

tempest_make_venv()
{
  cd ${TEMPEST_HOME_DIR}

  python tools/install_venv.py > /dev/null
}

tempest_configure()
{
  cp -f ${ROOTDIR}/tempest/etc/tempest.conf ${TEMPEST_HOME_DIR}/etc/
  cp -f ${ROOTDIR}/tempest/etc/accounts.yaml ${TEMPEST_HOME_DIR}/etc/

  sed -e "s/<KEYSTONE_IP>/${KEYSTONE_IP}/g" \
      -i ${TEMPEST_HOME_DIR}/etc/tempest.conf
  sed -e "s/<KEYSTONE_PORT>/${KEYSTONE_PORT}/g" \
      -i ${TEMPEST_HOME_DIR}/etc/tempest.conf
}

tempest_start()
{
  cd ${TEMPEST_HOME_DIR}

  ./run_tempest.sh  -V tempest.api.object_storage \
      -C ${TEMPEST_HOME_DIR}/etc/tempest.conf
}

deploy_tempest()
{
  tempest_download
  tempest_make_venv
  tempest_configure
}

deploy_keystone
deploy_tempest

tempest_start
