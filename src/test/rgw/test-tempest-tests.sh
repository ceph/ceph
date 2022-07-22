#!/bin/bash

# usage: ./keystone_task.sh {KEYSTONE_BRANCH} {KEYSTONE DIR}
set -x

# -e stops the script if any failures happen, -x is for debug output
# set -ex

KEYSTONE_BRANCH=${1:-master}
TEST_DIR=${2:-$(pwd)}
KEYSTONE_DIR=$TEST_DIR/keystone
TOX_DIR=$TEST_DIR/tox-venv


echo "### STEP 1: Deploy tox ###"
mkdir $TOX_DIR
# TODO: add tox version as an argument
TOX_VERSION='3.15.0'
python -m venv $TOX_DIR
source $TOX_DIR/bin/activate && pip install 'tox==3.15.0'

echo "### STEP 2: Download Keystone ###"

git clone -b $KEYSTONE_BRANCH https://github.com/openstack/keystone.git $KEYSTONE_DIR

# TODO: put logic in to checkout a specific SHA1
# 1. read SHA1 as an arguement of script
# 2. run git reset --hard SHA1 inside of KEYSTONE_DIR
# 3. Possible change in requirements.txt of pysaml version

echo "### STEP 3: Install Packages ###"

# patch bindep step not included
source $TOX_DIR/bin/activate && pip install bindep
source $TOX_DIR/bin/activate && bindep --brief --file $KEYSTONE_DIR/bindep.txt
# TODO: postgresql-devel is required
#sudo dnf install postgresql-devel
# possibly install postgres and mariadb aka the output packages from above bindep command, but is this really necessary?

echo "### STEP 4: Setup Venv ###"
cd $KEYSTONE_DIR && source $TOX_DIR/bin/activate && tox -e venv --notest
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && pip install 'python-openstackclient==5.2.1' 'osc-lib==2.0.0'

echo "### STEP 5: Configure Instance ###"
KEYREPO_DIR=$KEYSTONE_DIR/etc/fernet-keys
cd $KEYSTONE_DIR && source $TOX_DIR/bin/activate && tox -e genconfig
cd $KEYSTONE_DIR && cp -f etc/keystone.conf.sample etc/keystone.conf
cd $KEYSTONE_DIR && sed -e "s^#key_repository =.*^key_repository = $KEYREPO_DIR^" -i etc/keystone.conf
$HOSTNAME=$(hostname -s)
ARCHIVE_DIR=$TEST_DIR/archive
mkdir -p $ARCHIVE_DIR
LOG_FILE=$ARCHIVE_DIR/keystone.$HOSTNAME.log
cd $KEYSTONE_DIR && sed -e "s^#log_file =.*^log_file = $LOG_FILE^" -i $KEYSTONE_DIR/etc/keystone.conf
cd $KEYSTONE_DIR && cp $KEYSTONE_DIR/etc/keystone.conf $ARCHIVE_DIR/keystone.$HOSTNAME.conf
cd $KEYSTONE_DIR && mkdir -p $KEYREPO_DIR
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage fernet_setup
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage db_sync

echo "### STEP 6: Run Keystone ###"
# start the public endpoint
PUBLIC_PORT=5000
PUBLIC_HOST=localhost
$KEYSTONE_DIR/.tox/venv/bin/python $KEYSTONE_DIR/.tox/venv/bin/keystone-wsgi-public --host $PUBLIC_HOST --port $PUBLIC_PORT &
KEYSTONE_PUBLIC_PID=$(pgrep -f keystone-wsgi-public)

# start the public endpoint
ADMIN_PORT=35357
ADMIN_HOST=localhost
$KEYSTONE_DIR/.tox/venv/bin/python $KEYSTONE_DIR/.tox/venv/bin/keystone-wsgi-admin --host $ADMIN_HOST --port $ADMIN_PORT &
KEYSTONE_ADMIN_PID=$(pgrep -f keystone-wsgi-admin)

# sleep driven synchronization
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && sleep 15

echo "### STEP 7: Fill Keystone ###"
PUBLIC_URL=http://$PUBLIC_HOST:$PUBLIC_PORT/v3
ADMIN_URL=http://$ADMIN_HOST:$ADMIN_PORT/v3

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage bootstrap --bootstrap-password ADMIN --bootstrap-region-id RegionOne --bootstrap-internal-url $PUBLIC_URL --bootstrap-admin-url $ADMIN_URL --bootstrap-public-url $PUBLIC_URL

# These only get run if the domains, projects, users, roles, role-mappings, and services sections are in the keystone task config.

#cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack domain crate name 
#cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack project create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL --description 'Encryption Tenant' --domain default rgwcrypt --debug

#d $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack user create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL --domain default --password rgwcrypt-pass --project rgwcrypt rgwcrypt-user --debug

#d $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack role create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL Member --debug
#d $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack role create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL creator --debug

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack service create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL --description 'Swift Service' --name swift object-store --debug

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && sleep 3

RGW_ENDPOINT=http://localhost:8000
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack endpoint create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $ADMIN_URL swift public "$RGW_ENDPOINT/v1/KEY_$(tenant_id)s" --debug

# radosgw needs to be started up after this point
cd $TEST_DIR
MON=1 OSD=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d
ps ax | grep ceph

echo "### STEP 8: Download Tempest ###"

TEMPEST_BRANCH=${3:-master}
TEMPEST_DIR=$TEST_DIR/tempest
git clone -b $TEMPEST_BRANCH https://github.com/openstack/tempest.git $TEMPEST_DIR

echo "### STEP 9: Setup Venv for Tempest ###"
cd $TEMPEST_DIR && source $TOX_DIR/bin/activate && tox -e venv --notest
cd $TEMPEST_DIR && source .tox/venv/bin/activate && tempest init --workspace-path $TEMPEST_DIR/workspace.yaml rgw

echo "### STEP 10: Configure Instance for Tempest ###"
cp $TEST_DIR/tempest.conf $TEMPEST_ETC_DIR/tempest.conf

echo "### STEP 10: Run Tempest ###"
cd $TEMPEST_DIR && source .tox/venv/bin/activate && tempest run --workspace-path $TEMPEST_DIR/workspace.yaml --workspace rgw --regex '^tempest.api.object_storage' --black-regex '.*test_account_quotas_negative.AccountQuotasNegativeTest.test_user_modify_quota|.*test_container_acl_negative.ObjectACLsNegativeTest.*|.*test_container_services_negative.ContainerNegativeTest.test_create_container_metadata_.*|.*test_container_staticweb.StaticWebTest.test_web_index|.*test_container_staticweb.StaticWebTest.test_web_listing_css|.*test_container_synchronization.*|.*test_object_services.PublicObjectTest.test_access_public_container_object_without_using_creds|.*test_object_services.ObjectTest.test_create_object_with_transfer_encoding'

echo "### CLEAN UP BEGINNING ###"

cd $TEST_DIR
#rm -rf $TEMPEST_DIR
../src/stop.sh
# Stoping Keyston Admin Instance
kill $KEYSTONE_ADMIN_PID
# Stoping Keyston Public Instance
kill $KEYSTONE_PUBLIC_PID

rm -rf $ARCHIVE_DIR
rm -rf $KEYSTONE_DIR
rm -rf $TOX_DIR
echo "### CLEAN UP: Keystone Dir Removed ###"
