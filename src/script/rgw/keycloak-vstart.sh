#!/bin/bash
#
# Bootstrap a local Keycloak instance for RGW WebIdentity testing.
#
# Creates a Keycloak container via podman, provisions a realm/client/user,
# extracts tokens and thumbprint, creates the OIDC provider in RGW, and
# writes the [webidentity] section to s3tests.conf.
#
# Prerequisites:
#   - podman, jq, openssl, curl
#   - radosgw running (from rgw-vstart.sh)
#   - S3TEST_CONF pointing to s3tests.conf (or ~/dev/s3tests.conf)
#
# Usage:
#   keycloak-vstart.sh              # start + provision
#   keycloak-vstart.sh --stop       # stop + remove container

set -euo pipefail

KC_PORT=8080
KC_REALM=demorealm
KC_CLIENT=my_client
KC_USER=testuser
KC_PASS=testuser
KC_ADMIN=admin
KC_ADMIN_PASS=admin
KC_CONTAINER=keycloak-vstart
KC_IMAGE=quay.io/keycloak/keycloak:24.0

BUILD_DIR="$(pwd)"
CONF="$BUILD_DIR/ceph.conf"
S3CONF="${S3TEST_CONF:-$HOME/dev/s3tests.conf}"

if [[ "${1:-}" == "--stop" ]]; then
	echo "==> stopping Keycloak container"
	podman stop "$KC_CONTAINER" 2>/dev/null || true
	podman rm "$KC_CONTAINER" 2>/dev/null || true
	exit 0
fi

for cmd in podman jq openssl curl; do
	if ! command -v "$cmd" &>/dev/null; then
		echo "error: $cmd is required" >&2
		exit 1
	fi
done

# --- start Keycloak (reuse if already running) ---

if podman ps --format '{{.Names}}' | grep -q "^${KC_CONTAINER}$"; then
	echo "==> Keycloak container already running"
else
	# remove stopped container if it exists
	podman rm "$KC_CONTAINER" 2>/dev/null || true

	echo "==> starting Keycloak on port $KC_PORT"
	podman run -d --name "$KC_CONTAINER" \
		--network=host \
		-e KEYCLOAK_ADMIN="$KC_ADMIN" \
		-e KEYCLOAK_ADMIN_PASSWORD="$KC_ADMIN_PASS" \
		"$KC_IMAGE" start-dev --http-port="$KC_PORT" --health-enabled=true

	echo -n "==> waiting for Keycloak to start"
	for i in $(seq 1 60); do
		if curl -sf "http://localhost:${KC_PORT}/realms/master" &>/dev/null; then
			echo " ready"
			break
		fi
		echo -n "."
		sleep 2
	done

	if ! curl -sf "http://localhost:${KC_PORT}/realms/master" &>/dev/null; then
		echo " FAILED"
		echo "Keycloak did not become ready in 120s" >&2
		exit 1
	fi
fi

# --- get admin token ---

echo "==> authenticating as admin"
ADMIN_TOKEN=$(curl -sf -X POST \
	"http://localhost:${KC_PORT}/realms/master/protocol/openid-connect/token" \
	-d "grant_type=password" \
	-d "client_id=admin-cli" \
	-d "username=${KC_ADMIN}" \
	-d "password=${KC_ADMIN_PASS}" | jq -r '.access_token')

if [[ -z "$ADMIN_TOKEN" || "$ADMIN_TOKEN" == "null" ]]; then
	echo "error: failed to get admin token" >&2
	exit 1
fi

# --- create realm (ignore if exists) ---

echo "==> creating realm $KC_REALM"
curl -sf -X POST "http://localhost:${KC_PORT}/admin/realms" \
	-H "Authorization: Bearer $ADMIN_TOKEN" \
	-H "Content-Type: application/json" \
	-d '{"realm":"'"$KC_REALM"'","enabled":true,"accessTokenLifespan":1800}' \
	2>/dev/null || true

# --- create client ---

echo "==> creating client $KC_CLIENT"
curl -sf -X POST \
	"http://localhost:${KC_PORT}/admin/realms/${KC_REALM}/clients" \
	-H "Authorization: Bearer $ADMIN_TOKEN" \
	-H "Content-Type: application/json" \
	-d '{"clientId":"'"$KC_CLIENT"'","directAccessGrantsEnabled":true,"serviceAccountsEnabled":true,"redirectUris":["http://localhost:8000/*"]}' \
	2>/dev/null || true

# --- get client UUID + secret ---

CLIENT_UUID=$(curl -sf \
	"http://localhost:${KC_PORT}/admin/realms/${KC_REALM}/clients" \
	-H "Authorization: Bearer $ADMIN_TOKEN" | \
	jq -r '.[] | select(.clientId=="'"$KC_CLIENT"'") | .id')

CLIENT_SECRET=$(curl -sf \
	"http://localhost:${KC_PORT}/admin/realms/${KC_REALM}/clients/${CLIENT_UUID}/client-secret" \
	-H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.value')

echo "    client_uuid=$CLIENT_UUID"

# --- create user ---

echo "==> creating user $KC_USER"
curl -sf -X POST \
	"http://localhost:${KC_PORT}/admin/realms/${KC_REALM}/users" \
	-H "Authorization: Bearer $ADMIN_TOKEN" \
	-H "Content-Type: application/json" \
	-d '{"username":"'"$KC_USER"'","enabled":true,"emailVerified":true,"email":"testuser@example.com","firstName":"Test","lastName":"User","credentials":[{"type":"password","value":"'"$KC_PASS"'","temporary":false}],"attributes":{"https://aws.amazon.com/tags":["{\"principal_tags\":{\"Department\":[\"Engineering\",\"Marketing\"]}}"]}}'  \
	2>/dev/null || true

# --- get tokens ---

echo "==> obtaining tokens"

USER_TOKEN=$(curl -sf -X POST \
	"http://localhost:${KC_PORT}/realms/${KC_REALM}/protocol/openid-connect/token" \
	-d "grant_type=password" \
	-d "client_id=${KC_CLIENT}" \
	-d "client_secret=${CLIENT_SECRET}" \
	-d "username=${KC_USER}" \
	-d "password=${KC_PASS}" \
	-d "scope=openid" | jq -r '.access_token')

TOKEN=$(curl -sf -X POST \
	"http://localhost:${KC_PORT}/realms/${KC_REALM}/protocol/openid-connect/token" \
	-d "grant_type=client_credentials" \
	-d "client_id=${KC_CLIENT}" \
	-d "client_secret=${CLIENT_SECRET}" \
	-d "scope=openid" | jq -r '.access_token')

# --- get thumbprint ---

echo "==> extracting thumbprint"
CERT=$(curl -sf \
	"http://localhost:${KC_PORT}/realms/${KC_REALM}/protocol/openid-connect/certs" | \
	jq -r '.keys[] | select(.use=="sig") | .x5c[0]')

THUMBPRINT=$(echo "$CERT" | base64 -d | \
	openssl x509 -inform DER -fingerprint -sha1 -noout 2>/dev/null | \
	sed 's/.*=//;s/://g')

echo "    thumbprint=$THUMBPRINT"

# --- introspect for aud/sub/azp ---

echo "==> introspecting token"
INTROSPECT=$(curl -sf -X POST \
	"http://localhost:${KC_PORT}/realms/${KC_REALM}/protocol/openid-connect/token/introspect" \
	-u "${KC_CLIENT}:${CLIENT_SECRET}" \
	-d "token=${TOKEN}")

AUD=$(echo "$INTROSPECT" | jq -r '.aud // empty')
SUB=$(echo "$INTROSPECT" | jq -r '.sub // empty')
AZP=$(echo "$INTROSPECT" | jq -r '.azp // empty')

echo "    aud=$AUD sub=$SUB azp=$AZP"

# --- write [webidentity] to s3tests.conf ---
# Note: the OIDC provider in RGW is created by the test itself via
# the IAM CreateOpenIDConnectProvider API, not by radosgw-admin.

echo "==> writing [webidentity] to $S3CONF"

# remove existing section if present
if grep -q '^\[webidentity\]' "$S3CONF" 2>/dev/null; then
	sed -i '/^\[webidentity\]/,/^\[/{/^\[webidentity\]/d;/^\[/!d}' "$S3CONF"
fi

cat >> "$S3CONF" << EOF

[webidentity]
token = ${TOKEN}
thumbprint = ${THUMBPRINT}
aud = ${AUD}
sub = ${SUB}
azp = ${AZP}
user_token = ${USER_TOKEN}
KC_REALM = ${KC_REALM}
EOF

echo ""
echo "==> Keycloak ready on port ${KC_PORT}"
echo "    realm=${KC_REALM} client=${KC_CLIENT} user=${KC_USER}"
echo "    OIDC URL: http://localhost:${KC_PORT}/realms/${KC_REALM}"
echo ""
echo "To stop: $0 --stop"
