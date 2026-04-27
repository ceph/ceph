#!/bin/bash
#
# To run this test script with a cluster created via vstart.sh:
# $PATH needs to be set for radosgw-admin executables.
# $AWS_ENDPOINT_URL needs to be set to the endpoint of the RGW
#
# Example when ceph source is cloned into $HOME and a vstart cluster is already running with a radosgw:
# $ PATH=~/ceph/build/bin/:$PATH AWS_ENDPOINT_URL=http://localhost:8000 ~/ceph/qa/workunits/rgw/test_account_migration.sh
#

set -ex

# determine radosgw endpoint if not specified
if [ -z ${AWS_ENDPOINT_URL} ]
then
	# in teuthology, the rgw task stores the radosgw endpoint in ${TESTDIR}/url_file
	url=$(cat ${TESTDIR}/url_file)
	export AWS_ENDPOINT_URL=$url
fi

# install awscli with pip
python3 -m venv account-migration-virtualenv
source account-migration-virtualenv/bin/activate
pip install --upgrade pip awscli

# create a test user
userinfo=$(radosgw-admin user create --uid test-account-migration \
	--display-name "MigratedUser" \
	--email accountmigration@example.com)
export AWS_ACCESS_KEY_ID=$(echo $userinfo | jq -r .keys[0].access_key)
export AWS_SECRET_ACCESS_KEY=$(echo $userinfo | jq -r .keys[0].secret_key)

# create a bucket and upload an object
aws s3 mb s3://testmigrate
aws s3api put-object --bucket testmigrate --key obj

# create an account and migrate the user as account root
accountid=$(radosgw-admin account create | jq -r .id)
radosgw-admin user modify --uid test-account-migration --account-root --account-id=$accountid

# verify the migrated user still has access
aws s3api head-object --bucket testmigrate --key obj

# replace account-root flag with managed policy
aws iam attach-user-policy --region us-east-1 --user-name MigratedUser \
	--policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
radosgw-admin user modify --uid test-account-migration --account-root=0

# verify the migrated user still has access
aws s3api head-object --bucket testmigrate --key obj

# clean up
radosgw-admin bucket rm --bucket testmigrate --purge-objects
radosgw-admin user rm --uid test-account-migration
radosgw-admin account rm --account-id=$accountid
deactivate
rm -rf account-migration-virtualenv

exit 0
