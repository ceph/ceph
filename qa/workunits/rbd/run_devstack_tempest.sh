#!/bin/bash -ex

STACK_BRANCH=stable/ocata

STACK_USER=${STACK_USER:-stack}
STACK_GROUP=${STACK_GROUP:-stack}
TEMPEST_USER=${TEMPEST_USER:-tempest}

STACK_HOME_PATH=${STACK_HOME_PATH:-/home/stack}
STACK_OPT_PATH=${STACK_OPT_PATH:-/opt/stack}
STACK_LOG_PATH=${STACK_LOG_PATH:-/mnt/log/stack}

cleanup() {
  echo "**** cleanup"

  # ensure teuthology can clean up the logs
  [ -d ${STACK_LOG_PATH} ] && chmod -R a+rwx ${STACK_LOG_PATH}

  mkdir ${STACK_LOG_PATH}/etc
  cp -dpr /etc/cinder ${STACK_LOG_PATH}/etc || true
  cp -dpr /etc/glance ${STACK_LOG_PATH}/etc || true
  cp -dpr /etc/nova ${STACK_LOG_PATH}/etc || true

  # kill all OpenStack services
  if [ -d ${STACK_OPT_PATH}/devstack ]; then
    cd ${STACK_OPT_PATH}/devstack
    sudo -H -u ${STACK_USER} ./unstack.sh || true
  fi
}

trap cleanup INT TERM EXIT

# devstack configuration adapted from upstream gate
cat<<EOF > ${STACK_HOME_PATH}/local.conf
[[local|localrc]]
Q_USE_DEBUG_COMMAND=True
NETWORK_GATEWAY=10.1.0.1
USE_SCREEN=False
DATA_DIR=${STACK_OPT_PATH}/data
ACTIVE_TIMEOUT=90
BOOT_TIMEOUT=90
ASSOCIATE_TIMEOUT=60
TERMINATE_TIMEOUT=60
MYSQL_PASSWORD=secretmysql
DATABASE_PASSWORD=secretdatabase
RABBIT_PASSWORD=secretrabbit
ADMIN_PASSWORD=secretadmin
SERVICE_PASSWORD=secretservice
SERVICE_TOKEN=111222333444
SWIFT_HASH=1234123412341234
ROOTSLEEP=0
NOVNC_FROM_PACKAGE=True
ENABLED_SERVICES=c-api,c-bak,c-sch,c-vol,ceilometer-acentral,ceilometer-acompute,ceilometer-alarm-evaluator,ceilometer-alarm-notifier,ceilometer-anotification,ceilometer-api,ceilometer-collector,cinder,dstat,g-api,g-reg,horizon,key,mysql,n-api,n-cauth,n-cond,n-cpu,n-novnc,n-obj,n-sch,peakmem_tracker,placement-api,q-agt,q-dhcp,q-l3,q-meta,q-metering,q-svc,rabbit,s-account,s-container,s-object,s-proxy,tempest
SKIP_EXERCISES=boot_from_volume,bundle,client-env,euca
SYSLOG=False
SCREEN_LOGDIR=${STACK_LOG_PATH}/screen-logs
LOGFILE=${STACK_LOG_PATH}/devstacklog.txt
VERBOSE=True
FIXED_RANGE=10.1.0.0/20
IPV4_ADDRS_SAFE_TO_USE=10.1.0.0/20
FLOATING_RANGE=172.24.5.0/24
PUBLIC_NETWORK_GATEWAY=172.24.5.1
FIXED_NETWORK_SIZE=4096
VIRT_DRIVER=libvirt
SWIFT_REPLICAS=1
LOG_COLOR=False
UNDO_REQUIREMENTS=False
CINDER_PERIODIC_INTERVAL=10

export OS_NO_CACHE=True
OS_NO_CACHE=True
CEILOMETER_BACKEND=mysql
LIBS_FROM_GIT=
DATABASE_QUERY_LOGGING=True
EBTABLES_RACE_FIX=True
CINDER_SECURE_DELETE=False
CINDER_VOLUME_CLEAR=none
LIBVIRT_TYPE=kvm
VOLUME_BACKING_FILE_SIZE=24G
TEMPEST_HTTP_IMAGE=http://git.openstack.org/static/openstack.png
FORCE_CONFIG_DRIVE=False

CINDER_ENABLED_BACKENDS=ceph:ceph
TEMPEST_STORAGE_PROTOCOL=ceph
REMOTE_CEPH=True
enable_plugin devstack-plugin-ceph git://git.openstack.org/openstack/devstack-plugin-ceph
EOF

cat<<EOF > ${STACK_HOME_PATH}/start.sh
#!/bin/bash -ex
cd ${STACK_OPT_PATH}
git clone https://git.openstack.org/openstack-dev/devstack -b ${STACK_BRANCH}

# TODO workaround for https://github.com/pypa/setuptools/issues/951
git clone https://git.openstack.org/openstack/requirements.git -b ${STACK_BRANCH}
sed -i 's/appdirs===1.4.0/appdirs===1.4.3/' requirements/upper-constraints.txt

cd devstack
cp ${STACK_HOME_PATH}/local.conf .

export PYTHONUNBUFFERED=true
export PROJECTS="openstack/devstack-plugin-ceph"

./stack.sh
EOF

# execute devstack
chmod 0755 ${STACK_HOME_PATH}/start.sh
sudo -H -u ${STACK_USER} ${STACK_HOME_PATH}/start.sh

# execute tempest
chown -R ${TEMPEST_USER}:${STACK_GROUP} ${STACK_OPT_PATH}/tempest
chown -R ${TEMPEST_USER}:${STACK_GROUP} ${STACK_OPT_PATH}/data/tempest
chmod -R o+rx ${STACK_OPT_PATH}/devstack/files

cd ${STACK_OPT_PATH}/tempest
sudo -H -u ${TEMPEST_USER} tox -eall-plugin -- '(?!.*\[.*\bslow\b.*\])(^tempest\.(api|scenario)|(^cinder\.tests.tempest))' --concurrency=3
