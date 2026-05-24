#!/usr/bin/env bash
#
# test_rgw_role_pagination.sh
# Verify that 'radosgw-admin role list' paginates correctly beyond the 100-item chunk limit.

set -e

# Fallback to standard command if the variable isn't provided by the environment
RGW_ADMIN=${RGW_ADMIN:-radosgw-admin}

NUM_ROLES=105
PREFIX="TestRole"
POLICY_DOC='{"Statement": [{"Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["sts:AssumeRole"]}]}'

echo "Creating ${NUM_ROLES} roles to trigger pagination limit..."
for i in $(seq 1 $NUM_ROLES); do
    "$RGW_ADMIN" role create --role-name="${PREFIX}${i}" --assume-role-policy-doc="${POLICY_DOC}" > /dev/null
done

echo "Fetching role list (if the pagination bug exists, this will hang infinitely)..."
# We pipe the JSON output into 'jq' to count the total number of objects in the 'Roles' array
ROLE_COUNT=$("$RGW_ADMIN" role list 2>/dev/null | jq 'length')

echo "Found ${ROLE_COUNT} roles."

# Assert that we successfully retrieved at least the 105 roles we just created
if [ "$ROLE_COUNT" -lt "$NUM_ROLES" ]; then
    echo "FAIL: Expected at least ${NUM_ROLES} roles, but only retrieved ${ROLE_COUNT}."
    exit 1
fi

echo "Cleaning up test roles..."
for i in $(seq 1 $NUM_ROLES); do
    "$RGW_ADMIN" role delete --role-name="${PREFIX}${i}" > /dev/null
done

echo "SUCCESS: Role pagination works correctly!"
exit 0
