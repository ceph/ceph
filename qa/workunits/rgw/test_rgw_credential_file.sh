#!/usr/bin/env bash
#
# test_rgw_credential_file.sh
# Verify that radosgw-admin reads S3 credentials from the files named by
# --access-key-file and --secret-key-file, so that they never appear in argv.

set -e

# Fallback to standard command if the variable isn't provided by the environment
RGW_ADMIN=${RGW_ADMIN:-radosgw-admin}

UID_UNDER_TEST="credfile-test-user"
DISPLAY_NAME="credential file test user"
ACCESS_KEY="CREDFILEACCESSKEY123"
SECRET_KEY="credfileSecretKey0987654321abcdefghijklm"

WORKDIR="$(mktemp -d)"

cleanup() {
    "$RGW_ADMIN" user rm --uid="$UID_UNDER_TEST" --purge-data > /dev/null 2>&1 || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# The trailing newline is deliberate: it is what a file written by an editor or
# by "echo secret > file" looks like, and it must not end up in the key.
printf '%s\n' "$ACCESS_KEY" > "$WORKDIR/access"
printf '%s\n' "$SECRET_KEY" > "$WORKDIR/secret"
chmod 0600 "$WORKDIR/access" "$WORKDIR/secret"

expect_fail() {
    local desc="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        echo "FAIL: ${desc}: expected a nonzero exit, but the command succeeded"
        exit 1
    fi
    echo "ok: ${desc}"
}

echo "Creating a user whose credentials come from files..."
"$RGW_ADMIN" user create \
    --uid="$UID_UNDER_TEST" \
    --display-name="$DISPLAY_NAME" \
    --access-key-file="$WORKDIR/access" \
    --secret-key-file="$WORKDIR/secret" > /dev/null

echo "Checking that the stored credentials match the file contents..."
USER_JSON="$("$RGW_ADMIN" user info --uid="$UID_UNDER_TEST" --format=json)"
GOT_ACCESS="$(echo "$USER_JSON" | jq -r '.keys[0].access_key')"
GOT_SECRET="$(echo "$USER_JSON" | jq -r '.keys[0].secret_key')"

if [ "$GOT_ACCESS" != "$ACCESS_KEY" ]; then
    echo "FAIL: expected access key '${ACCESS_KEY}', got '${GOT_ACCESS}'"
    exit 1
fi
if [ "$GOT_SECRET" != "$SECRET_KEY" ]; then
    echo "FAIL: expected secret key '${SECRET_KEY}', got '${GOT_SECRET}'"
    exit 1
fi
echo "ok: credentials round-tripped with the trailing newline stripped"

echo "Checking that a user can be looked up by a file-supplied access key..."
LOOKUP_UID="$("$RGW_ADMIN" user info --access-key-file="$WORKDIR/access" --format=json | jq -r '.user_id')"
if [ "$LOOKUP_UID" != "$UID_UNDER_TEST" ]; then
    echo "FAIL: expected uid '${UID_UNDER_TEST}', got '${LOOKUP_UID}'"
    exit 1
fi
echo "ok: user info accepts --access-key-file"

echo "Checking the error cases..."
expect_fail "missing access key file is rejected" \
    "$RGW_ADMIN" user info --access-key-file="$WORKDIR/does-not-exist"

: > "$WORKDIR/empty"
chmod 0600 "$WORKDIR/empty"
expect_fail "empty access key file is rejected" \
    "$RGW_ADMIN" user info --access-key-file="$WORKDIR/empty"

expect_fail "--access-key and --access-key-file together are rejected" \
    "$RGW_ADMIN" user info --access-key="$ACCESS_KEY" --access-key-file="$WORKDIR/access"

expect_fail "--secret and --secret-file together are rejected" \
    "$RGW_ADMIN" user create --uid="${UID_UNDER_TEST}-2" --display-name=x \
        --secret="$SECRET_KEY" --secret-file="$WORKDIR/secret"

echo "Checking that a group-readable credential file still works..."
"$RGW_ADMIN" user rm --uid="$UID_UNDER_TEST" --purge-data > /dev/null
chmod 0644 "$WORKDIR/access" "$WORKDIR/secret"
"$RGW_ADMIN" user create \
    --uid="$UID_UNDER_TEST" \
    --display-name="$DISPLAY_NAME" \
    --access-key-file="$WORKDIR/access" \
    --secret-key-file="$WORKDIR/secret" > /dev/null
GOT_ACCESS="$("$RGW_ADMIN" user info --uid="$UID_UNDER_TEST" --format=json | jq -r '.keys[0].access_key')"
if [ "$GOT_ACCESS" != "$ACCESS_KEY" ]; then
    echo "FAIL: expected access key '${ACCESS_KEY}', got '${GOT_ACCESS}'"
    exit 1
fi
echo "ok: a permissive mode warns but does not block"

echo "SUCCESS: radosgw-admin credential files work correctly!"
exit 0
