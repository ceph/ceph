#!/usr/bin/env python3
"""Create the impersonation fixture bucket and its objects over S3.

The bucket cannot simply be a directory placed in the data root: an
unmarked directory has no owner RGW can resolve, so every request
against it is refused with AccessDenied before any of this is
reached.  Creating it through S3 gives it an owner the gateway
recognises;  the caller then adjusts ownership and mode on disk,
which is the part that needs root.

Signed with the standard library only, as probes/adminget.py does --
this runs from rgw-vstart.sh, which cannot assume botocore.

Credentials are passed in rather than read from s3tests.conf, which
rgw-vstart.sh does not always generate -- the SAMPLE it copies from
is absent in some trees.
"""

import datetime
import hashlib
import hmac
import sys
import time
import urllib.error
import urllib.request

REGION = "us-east-1"


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _quote(v):
    """Percent-encode for a canonical query string (RFC 3986 unreserved)."""
    out = []
    for b in str(v).encode():
        if (0x41 <= b <= 0x5A or 0x61 <= b <= 0x7A or 0x30 <= b <= 0x39
                or b in b"-_.~"):
            out.append(chr(b))
        else:
            out.append(f"%{b:02X}")
    return "".join(out)


def request(method, path, body, ak, sk, host, port, query=None):
    """Sign and send.

    `query` is a dict, and belongs in the canonical *query string*
    rather than in the path -- SigV4 signs the two separately, sorted
    by key, so folding one into the other signs a different request
    than it sends.  Requests with no query never notice.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload = hashlib.sha256(body).hexdigest()
    hostport = f"{host}:{port}"

    canonical_query = "&".join(
        f"{_quote(k)}={_quote(v)}" for k, v in sorted((query or {}).items()))

    canonical = (
        f"{method}\n{path}\n{canonical_query}\n"
        f"host:{hostport}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz}\n\n"
        f"host;x-amz-content-sha256;x-amz-date\n{payload}"
    )
    scope = f"{day}/{REGION}/s3/aws4_request"
    to_sign = (
        f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n"
        f"{hashlib.sha256(canonical.encode()).hexdigest()}"
    )
    key = _sign(_sign(_sign(_sign(("AWS4" + sk).encode(), day), REGION),
                      "s3"), "aws4_request")
    sig = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()

    url = f"http://{hostport}{path}"
    if canonical_query:
        url += "?" + canonical_query

    req = urllib.request.Request(
        url, data=body, method=method,
        headers={
            "Host": hostport,
            "x-amz-date": amz,
            "x-amz-content-sha256": payload,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
                f"Signature={sig}"
            ),
        })
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def main():
    if len(sys.argv) != 8:
        print("usage: nsfs-impersonate-fixture.py <host> <port> "
              "<access-key> <secret-key> <bucket> <s3-user> <posix-uid>",
              file=sys.stderr)
        return 2
    host, port, ak, sk, bucket, uid, posix_uid = sys.argv[1:8]

    def call(method, path, body=b""):
        status, out = request(method, path, body, ak, sk, host, port)
        if status not in (200, 204):
            print(f"{method} {path} -> {status}: {out[:200]!r}",
                  file=sys.stderr)
            return False
        return True

    # vstart returns before the frontend has bound its port, so the
    # first request can arrive at nothing.  Wait for it rather than
    # race it.
    deadline = time.monotonic() + 30
    while True:
        try:
            request("GET", "/", b"", ak, sk, host, port)
            break
        except urllib.error.URLError as e:
            if time.monotonic() >= deadline:
                print(f"gateway never answered on {host}:{port}: {e}",
                      file=sys.stderr)
                return 1
            time.sleep(0.25)

    # With impersonation on, every identity that can authenticate
    # needs a POSIX record -- including this one, or the object
    # writes below are refused before they reach the filesystem.
    # vstart grants testid the nsfs capability, so it can provision
    # itself.  Bound to the uid the fixture user owns, so the seeded
    # objects come out owned by a real identity rather than by the
    # gateway.
    s, o = request("PUT", "/admin/nsfs/identity", b"", ak, sk, host, port,
                   query={"identity": uid, "uid": posix_uid,
                          "gid": posix_uid})
    if s != 200:
        print(f"binding {uid} to uid {posix_uid} -> {s}: {o[:200]!r}",
              file=sys.stderr)
        return 1

    # A second run gets BucketAlreadyOwnedByYou, which is a 409 and
    # not a failure:  the option has to be idempotent.
    status, out = request("PUT", f"/{bucket}", b"", ak, sk, host, port)
    if status not in (200, 409):
        print(f"creating {bucket} -> {status}: {out[:200]!r}", file=sys.stderr)
        return 1

    if not call("PUT", f"/{bucket}/open.txt", b"open to everyone\n"):
        return 1
    if not call("PUT", f"/{bucket}/grouped.txt", b"gated on a group\n"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
