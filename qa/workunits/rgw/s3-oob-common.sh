# shellcheck shell=bash
# Sourced by the workunits that move S3 objects out of band over libfabric
# (run-s3-oob.sh, run-s3-oob-soak.sh): the gateway, the client's provider,
# domain and address, the S3 user, and the OSDs' counters.
#
# Environment:
#   RGW_ENDPOINT  the gateway (default: $TESTDIR/url_file, else
#                 http://localhost:8000)
#   OOB_PROVIDER  libfabric provider of the client (default: the first
#                 libfabric provider in the cluster's
#                 oob_client_transports, else tcp)
#   OOB_DOMAIN    its domain, or - for any (default: for verbs, the RDMA
#                 device of the interface that carries OOB_NODE, else -)
#   OOB_NODE      the client address the OSDs reach (default: the source
#                 address of the route to the gateway)
#
# On a soft-RoCE device the ofi_rxm provider fails to create its completion
# queues unless FI_UNIVERSE_SIZE is small: this sets 16 when it is unset.

if [ -z "$RGW_ENDPOINT" ]; then
    if [ -n "$TESTDIR" ] && [ -f "$TESTDIR/url_file" ]; then
        RGW_ENDPOINT=$(cat "$TESTDIR/url_file")
    else
        RGW_ENDPOINT=http://localhost:8000
    fi
fi
RGW_ENDPOINT=${RGW_ENDPOINT%/}

# the value of a Ceph option as this host's configuration gives it
oob_conf() {
    ceph-conf --lookup "$1" 2>/dev/null ||
        ceph config get client "$1" 2>/dev/null || true
}

if [ -z "$OOB_PROVIDER" ]; then
    for t in $(oob_conf oob_client_transports | tr ',' ' '); do
        case "$t" in
            cuobj|rc) ;;
            *) OOB_PROVIDER=$t; break ;;
        esac
    done
    : ${OOB_PROVIDER:=tcp}
fi
# a configuration file spells verbs;ofi_rxm as verbs_ofi_rxm
OOB_PROVIDER=$(echo "$OOB_PROVIDER" | sed 's/_ofi_/;ofi_/')

if [ -z "$OOB_NODE" ]; then
    host=${RGW_ENDPOINT#*://}
    host=${host%%[:/]*}
    addr=$(getent ahostsv4 "$host" | awk 'NR == 1 { print $1 }')
    OOB_NODE=$(ip -4 route get "${addr:-127.0.0.1}" |
                   sed -n 's/.* src \([0-9.]*\).*/\1/p')
    : ${OOB_NODE:=127.0.0.1}
fi

# the RDMA device that has a GID on the interface that carries address $1
rdma_dev_of_addr() {
    local addr=$1 iface d
    iface=$(ip -o -4 addr show |
                awk -v a="$addr" '{ split($4, p, "/"); if (p[1] == a) print $2 }' |
                head -n 1)
    [ -n "$iface" ] || return 0
    for d in /sys/class/infiniband/*; do
        [ -e "$d" ] || continue
        if cat "$d"/ports/*/gid_attrs/ndevs/* 2>/dev/null | grep -qx "$iface"; then
            basename "$d"
            return 0
        fi
    done
}

if [ -z "$OOB_DOMAIN" ]; then
    OOB_DOMAIN=-
    case "$OOB_PROVIDER" in
        verbs*)
            dev=$(rdma_dev_of_addr "$OOB_NODE")
            if [ -n "$dev" ]; then
                OOB_DOMAIN=$dev
            fi
            ;;
    esac
fi

case "$OOB_PROVIDER" in
    verbs*)
        # a software RDMA device (soft-RoCE) has no device of its own
        if [ "$OOB_DOMAIN" != - ] &&
               [ ! -e "/sys/class/infiniband/$OOB_DOMAIN/device" ]; then
            export FI_UNIVERSE_SIZE=${FI_UNIVERSE_SIZE:-16}
        fi
        # registering a window with an RDMA device locks its memory
        if [ "$(ulimit -l)" != unlimited ]; then
            ulimit -l unlimited 2>/dev/null ||
                sudo -n prlimit --pid $$ --memlock=unlimited:unlimited ||
                echo "WARNING: locked memory is limited to $(ulimit -l) KiB"
        fi
        ;;
esac

echo "client: provider $OOB_PROVIDER, domain $OOB_DOMAIN, node $OOB_NODE," \
     "gateway $RGW_ENDPOINT"

s3oob_uid=s3oob
access=S3OOBACCESSKEY0000001
secret=s3oobsecretkey0000000000000000000000001

# the S3 user of the workunits
s3oob_user() {
    if ! radosgw-admin user info --uid=$s3oob_uid > /dev/null 2>&1; then
        radosgw-admin user create --uid=$s3oob_uid --display-name=$s3oob_uid \
            --access-key=$access --secret=$secret > /dev/null
    fi
}

# The SigV4 headers of a request to S3: s3_sign METHOD URL [FILE], with
# FILE as the body, prints Host, x-amz-date, x-amz-content-sha256 and
# Authorization, one per line. The workunits sign here, and not with
# curl's --aws-sigv4, because what curl signs depends on its version: the
# curl 7.76.1 of CentOS Stream 9 signs x-amz-content-sha256 but does not
# send it, and the gateway refuses its requests.
s3_sign() {
    python3 - "$1" "$2" "${3:-}" "$access" "$secret" <<'SIGN'
import datetime
import hashlib
import hmac
import sys
from urllib.parse import quote, urlsplit

method, url, body, access, secret = sys.argv[1:6]
u = urlsplit(url)
h = hashlib.sha256()
if body:
    with open(body, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
payload = h.hexdigest()
now = datetime.datetime.now(datetime.timezone.utc)
amz_date = now.strftime('%Y%m%dT%H%M%SZ')
scope = now.strftime('%Y%m%d') + '/us-east-1/s3/aws4_request'
query = []
for item in filter(None, u.query.split('&')):
    name, _, value = item.partition('=')
    query.append((quote(name, safe='~'), quote(value, safe='~')))
headers = [('host', u.netloc), ('x-amz-content-sha256', payload),
           ('x-amz-date', amz_date)]
canonical = '\n'.join([
    method, quote(u.path or '/', safe='/~'),
    '&'.join('%s=%s' % q for q in sorted(query)),
    ''.join('%s:%s\n' % kv for kv in headers),
    ';'.join(k for k, _ in headers), payload])
to_sign = '\n'.join(['AWS4-HMAC-SHA256', amz_date, scope,
                     hashlib.sha256(canonical.encode()).hexdigest()])
key = ('AWS4' + secret).encode()
for part in scope.split('/'):
    key = hmac.new(key, part.encode(), hashlib.sha256).digest()
sig = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
print('Host: ' + u.netloc)
print('x-amz-date: ' + amz_date)
print('x-amz-content-sha256: ' + payload)
print('Authorization: AWS4-HMAC-SHA256 Credential=%s/%s, '
      'SignedHeaders=%s, Signature=%s' %
      (access, scope, ';'.join(k for k, _ in headers), sig))
SIGN
}

# a plain S3 request over HTTP, signed with SigV4: s3 METHOD PATH [FILE],
# with FILE as the body; the response's body goes to stdout
s3() {
    local method=$1 path=$2 body=${3:-} signed line
    local args=(-sS -f -X "$method")
    signed=$(s3_sign "$method" "$RGW_ENDPOINT/$path" "$body") || return 1
    while IFS= read -r line; do
        args+=(-H "$line")
    done <<< "$signed"
    if [ -n "$body" ]; then
        args+=(--data-binary @"$body")
    fi
    curl "${args[@]}" "$RGW_ENDPOINT/$path"
}

# the sum of one field of "ofi status" over every OSD
ofi_sum() {
    local field=$1 total=0 n v
    for n in $(ceph osd ls); do
        v=$(ceph tell osd.$n ofi status 2>/dev/null |
                jq -r "[.. | objects | .\"$field\"? // empty] | add // 0")
        total=$((total + ${v:-0}))
    done
    echo $total
}
