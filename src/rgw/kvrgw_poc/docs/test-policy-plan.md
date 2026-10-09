# Bucket Policy Test Plan

## Setup

- Create tenant
- Create bucket `policy-test`
- Upload 3 test objects: `obj-a`, `obj-b`, `obj-c`

---

## Group 1 — Deny Write: PUT rejected

```
1.1  PutBucketPolicy(deny_write)
1.2  PUT object → expect 403
1.3  GET obj-a → expect 200
1.4  HEAD obj-a → expect 200
1.5  LIST → expect 200
1.6  DeleteBucketPolicy()
```

## Group 2 — Deny Write: DELETE rejected

```
2.1  PutBucketPolicy(deny_write)
2.2  DELETE obj-a → expect 403
2.3  GET obj-a → expect 200
2.4  HEAD obj-a → expect 200
2.5  LIST → expect 200
2.6  DeleteBucketPolicy()
```

## Group 3 — Deny Write: DELETE-multi rejected

```
3.1  PutBucketPolicy(deny_write)
3.2  DELETE-multi (obj-a, obj-b, obj-c) → expect 403 for all keys
3.3  GET obj-a → expect 200
3.4  HEAD obj-a → expect 200
3.5  LIST → expect 200
3.6  DeleteBucketPolicy()
```

## Group 4 — Deny List: LIST rejected, others pass

```
4.1  PutBucketPolicy(deny_list)
4.2  LIST → expect 403
4.3  GET obj-a → expect 200
4.4  HEAD obj-a → expect 200
4.5  PUT obj-new → expect 200
4.6  DELETE obj-new → expect 200
4.7  DELETE-multi (obj-a) → expect 200
4.8  DeleteBucketPolicy()
```

Re-upload obj-a (deleted in 4.7).

## Group 5 — Deny Read: reads rejected, writes and list pass

```
5.1  PutBucketPolicy(deny_read)
5.2  sleep 4 seconds (wait for cache TTL expiry)
5.3  GET obj-a → expect 403
5.4  HEAD obj-a → expect 403
5.5  PUT obj-new → expect 200
5.6  DELETE obj-new → expect 200
5.7  DELETE-multi (obj-b) → expect 200
5.8  LIST → expect 200
5.9  DeleteBucketPolicy()
```

Re-upload obj-b (deleted in 5.7).

## Group 6 — Deny Read: GET TTL + refresh-before-reject

```
6.1  PutBucketPolicy(deny_read)
6.2  GET obj-a → expect 200 (stale cache, TTL not expired)
6.3  sleep 4 seconds
6.4  GET obj-a → expect 403 (cache refreshed, sees deny)
6.5  DeleteBucketPolicy()
6.6  GET obj-a → expect 200 (refresh-before-reject: cached deny → refresh → allow)
```

## Group 7 — Deny Read: HEAD TTL + refresh-before-reject

```
7.1  PutBucketPolicy(deny_read)
7.2  HEAD obj-a → expect 200 (stale cache)
7.3  sleep 4 seconds
7.4  HEAD obj-a → expect 403 (cache refreshed)
7.5  DeleteBucketPolicy()
7.6  HEAD obj-a → expect 200 (refresh-before-reject)
```

## Group 8 — Deny DeleteBucket

```
8.1  Delete all objects in bucket
8.2  PutBucketPolicy(deny_delete_bucket)
8.3  DELETE bucket → expect 403
8.4  DeleteBucketPolicy()
8.5  DELETE bucket → expect 200
```

## Group 9 — No policy (sanity check)

```
9.1  Create bucket `policy-sanity`
9.2  PUT obj-x → expect 200
9.3  GET obj-x → expect 200
9.4  HEAD obj-x → expect 200
9.5  LIST → expect 200
9.6  DELETE obj-x → expect 200
9.7  DELETE-multi (empty) → expect 200
9.8  DELETE bucket → expect 200
```

---

## Policy JSON Templates

### Deny Write

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": ["s3:PutObject", "s3:DeleteObject"],
    "Resource": "arn:aws:s3:::policy-test/*"
  }]
}
```

### Deny List

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::policy-test"
  }]
}
```

### Deny Read

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": ["s3:GetObject", "s3:HeadObject"],
    "Resource": "arn:aws:s3:::policy-test/*"
  }]
}
```

### Deny DeleteBucket

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:DeleteBucket",
    "Resource": "arn:aws:s3:::policy-test"
  }]
}
```

---

## Notes

- Groups 1–4: write/list denial is immediate (hard enforcement in txn or fresh B read for list). No sleep needed.
- Groups 5–7: read denial depends on cache TTL (3s). Must sleep >3s to observe deny.
- Refresh-before-reject (6.6, 7.6): proves that clearing a policy takes effect immediately for subsequent requests that would be denied by stale cache.
- Re-upload objects between groups when prior tests delete them.
