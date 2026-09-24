#!/bin/bash
#
# Verify account suspend/enable and per-user suspend behavior for account
# members. Account-owned buckets are frozen by account suspend, not by
# user suspend of the account root.
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

bucket_suspended() {
	radosgw-admin bucket stats --bucket "$1" | jq -r .suspended
}

account_suspended() {
	radosgw-admin account get --account-id="$1" \
		| jq -r '.AccountInfo.suspended // .suspended'
}

python3 -m venv account-root-suspend-virtualenv
source account-root-suspend-virtualenv/bin/activate
pip install --upgrade pip awscli

# create a user, bucket, then migrate as account root
userinfo=$(radosgw-admin user create --uid test-account-root-suspend \
	--display-name "AccountRootSuspend" \
	--email accountrootsuspend@example.com)
ROOT_ACCESS_KEY=$(echo $userinfo | jq -r .keys[0].access_key)
ROOT_SECRET_KEY=$(echo $userinfo | jq -r .keys[0].secret_key)
export AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY

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

test "$(bucket_suspended test-account-root-suspend)" = "false"

# suspending the account root blocks that user, not account-owned buckets
radosgw-admin user suspend --uid test-account-root-suspend
test "$(bucket_suspended test-account-root-suspend)" = "false"

set +e
AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
root_rc=$?
AWS_ACCESS_KEY_ID=$IAM_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$IAM_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
member_rc=$?
set -e
test $root_rc -ne 0
test $member_rc -eq 0

# re-enable account root restores root access
radosgw-admin user enable --uid test-account-root-suspend
test "$(bucket_suspended test-account-root-suspend)" = "false"
AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj

# suspending a non-root account member must not suspend account buckets
radosgw-admin user suspend --uid test-account-member-suspend
test "$(bucket_suspended test-account-root-suspend)" = "false"
AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
set +e
AWS_ACCESS_KEY_ID=$IAM_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$IAM_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
member_rc=$?
set -e
test $member_rc -ne 0

radosgw-admin user enable --uid test-account-member-suspend

# account suspend freezes account-owned buckets and blocks all members
radosgw-admin account suspend --account-id=$accountid
test "$(account_suspended "$accountid")" = "1"
test "$(bucket_suspended test-account-root-suspend)" = "true"
set +e
AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
root_rc=$?
AWS_ACCESS_KEY_ID=$IAM_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$IAM_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj
member_rc=$?
set -e
test $root_rc -ne 0
test $member_rc -ne 0

radosgw-admin account enable --account-id=$accountid
test "$(account_suspended "$accountid")" = "0"
test "$(bucket_suspended test-account-root-suspend)" = "false"
AWS_ACCESS_KEY_ID=$ROOT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$ROOT_SECRET_KEY \
	aws s3api head-object --bucket test-account-root-suspend --key obj

# clean up
radosgw-admin bucket rm --bucket test-account-root-suspend --purge-objects
radosgw-admin user rm --uid test-account-member-suspend
radosgw-admin user rm --uid test-account-root-suspend
radosgw-admin account rm --account-id=$accountid
deactivate
rm -rf account-root-suspend-virtualenv

exit 0
