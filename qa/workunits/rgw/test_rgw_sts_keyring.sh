#!/usr/bin/env bash
#
# exercise the sts keyring lifecycle against a live cluster
#
set -ex

list() {
  radosgw-admin sts keyring list
}

count_keys() {
  list | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))'
}

sealing_key() {
  list | python3 -c 'import json,sys; print(next(k["key_id"] for k in json.load(sys.stdin) if k["seals"]))'
}

oldest_key() {
  list | python3 -c 'import json,sys; print(json.load(sys.stdin)[-1]["key_id"])'
}

# the suite installs one key and rotates once before the s3 tests
count=$(count_keys)
seal=$(sealing_key)

# rotate with a generated key
radosgw-admin sts keyring rotate
[ "$(count_keys)" -eq $((count + 1)) ]
[ "$(sealing_key)" != "$seal" ]
seal=$(sealing_key)

# rotate with a supplied key
keyfile=$(mktemp)
trap 'rm -f "$keyfile"' EXIT
printf '%s=%s\n' "$(openssl rand -hex 20)" "$(openssl rand -base64 32)" > "$keyfile"
radosgw-admin sts keyring rotate --infile="$keyfile"
[ "$(count_keys)" -eq $((count + 2)) ]
[ "$(sealing_key)" != "$seal" ]

# rotate --max-keys=1 discards the sealing key, so it must ask first
if radosgw-admin sts keyring rotate --max-keys=1; then
  exit 1
fi
[ "$(count_keys)" -eq $((count + 2)) ]

# rotate --max-keys retires the oldest keys in the same step
radosgw-admin sts keyring rotate --max-keys=2
[ "$(count_keys)" -eq 2 ]

# removing the sealing key must ask first
if radosgw-admin sts keyring rm --key-id "$(sealing_key)"; then
  exit 1
fi

# removing an unknown key fails
if radosgw-admin sts keyring rm --key-id ffffffffffffffffffffffffffffffffffffffff; then
  exit 1
fi

# retire the oldest verification key
radosgw-admin sts keyring rm --key-id "$(oldest_key)"
[ "$(count_keys)" -eq 1 ]

# the only key can never be removed
if radosgw-admin sts keyring rm --key-id "$(sealing_key)" --yes-i-really-mean-it; then
  exit 1
fi

# confirmed rotate --max-keys=1 replaces the whole keyring
seal=$(sealing_key)
radosgw-admin sts keyring rotate --max-keys=1 --yes-i-really-mean-it
[ "$(count_keys)" -eq 1 ]
[ "$(sealing_key)" != "$seal" ]

# trim never removes the sealing key
radosgw-admin sts keyring trim | grep 'nothing to trim'

# the stored legacy key: store, inspect, guard replacement, remove
radosgw-admin sts keyring init --legacy
radosgw-admin sts keyring list --legacy | python3 -c '
import json, sys
out = json.load(sys.stdin)
assert out["present"]
assert len(out["sha256"]) == 64
'
if radosgw-admin sts keyring init --legacy; then
  exit 1
fi
radosgw-admin sts keyring init --legacy --yes-i-really-mean-it
radosgw-admin sts keyring rm --legacy --yes-i-really-mean-it
radosgw-admin sts keyring list --legacy | python3 -c '
import json, sys
assert not json.load(sys.stdin)["present"]
'

echo OK
