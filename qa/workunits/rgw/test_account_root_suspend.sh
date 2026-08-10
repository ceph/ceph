#!/bin/bash
#
# Verify that suspending an account root user also suspends account-owned
# buckets (BUCKET_SUSPENDED), while suspending a non-root account user does not.
#
# To run with vstart:
#   PATH=~/ceph/build/bin/:$PATH AWS_ENDPOINT_URL=http://localhost:8000 \
#     ~/ceph/qa/workunits/rgw/test_account_root_suspend.sh
#

set -ex

if [ -z ${AWS_ENDPOINT_URL} ]
then
	url=$(cat ${TESTDIR}/url_file)
	export AWS_ENDPOINT_URL=$url
fi

BUCKET_SUSPENDED=1

python3 -m venv account-root-suspend-virtualenv
source account-root-suspend-virtualenv/bin/activate
pip install --upgrade pip awscli

# create a user, bucket, then migrate as account root
userinfo=$(radosgw-admin user create --uid test-account-root-suspend \
	--display-name "AccountRootSuspend" \
	--email accountrootsuspend@example.com)
export AWS_ACCESS_KEY_ID=$(echo $userinfo | jq -r .keys[0].access_key)
export AWS_SECRET_ACCESS_KEY=$(echo $userinfo | jq -r .keys[0].secret_key)

aws s3 mb s3://test-account-root-suspend
aws s3api put-object --bucket test-account-root-suspend --key obj

accountid=$(radosgw-admin account create | jq -r .id)
radosgw-admin user modify --uid test-account-root-suspend \
	--account-root --account-id=$accountid

# second (non-root) user in the same account with S3 access
iaminfo=$(radosgw-admin user create --uid test-account-member-suspend \
	--display-name "AccountMemberSuspend" \
	--account-id=$accountid --gen-secret --gen-access-key)
IAM_ACCESS_KEY=$(echo $iaminfo | jq -r .keys[0].access_key)
IAM_SECRET_KEY=$(echo $iaminfo | jq -r .keys[0].secret_key)
aws iam attach-user-policy --region us-east-1 --user-name AccountMemberSuspend \
	--policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

flags=$(radosgw-admin bucket stats --bucket test-account-root-suspend | jq -r .flags)
test $((flags & BUCKET_SUSPENDED)) -eq 0

# suspending account root must suspend account-owned buckets
radosgw-admin user suspend --uid test-account-root-suspend
flags=$(radosgw-admin bucket stats --bucket test-account-root-suspend | jq -r .flags)
test $((flags & BUCKET_SUSPENDED)) -eq $BUCKET_SUSPENDED

# non-root member is also blocked via BUCKET_SUSPENDED
set +e
AWS_ACCESS_KEY_ID=$IAM_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$IAM_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
rc=$?
set -e
test $rc -ne 0

# re-enable account root restores buckets
radosgw-admin user enable --uid test-account-root-suspend
flags=$(radosgw-admin bucket stats --bucket test-account-root-suspend | jq -r .flags)
test $((flags & BUCKET_SUSPENDED)) -eq 0
AWS_ACCESS_KEY_ID=$IAM_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$IAM_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj

# suspending a non-root account member must not suspend account buckets
radosgw-admin user suspend --uid test-account-member-suspend
flags=$(radosgw-admin bucket stats --bucket test-account-root-suspend | jq -r .flags)
test $((flags & BUCKET_SUSPENDED)) -eq 0
# root can still access
aws s3api head-object --bucket test-account-root-suspend --key obj

# clean up
radosgw-admin user enable --uid test-account-member-suspend || true
radosgw-admin bucket rm --bucket test-account-root-suspend --purge-objects
radosgw-admin user rm --uid test-account-member-suspend
radosgw-admin user rm --uid test-account-root-suspend
radosgw-admin account rm --account-id=$accountid
deactivate
rm -rf account-root-suspend-virtualenv

exit 0
