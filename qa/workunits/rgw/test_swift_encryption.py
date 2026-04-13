#!/usr/bin/env python3

import logging as log
import os
import requests
import boto3
import hashlib
from common import exec_cmd

TEST_UID = "test-user"
TEST_SUBUSER = "test-user:swift"
TEST_SWIFT_SECRET = "swiftsecret"
TEST_ACCESS_KEY = "s3access"
TEST_SECRET_KEY = "s3secret"

BUCKET_NAME = "test-encryption-bucket"
SEGMENTS_BUCKET = f"{BUCKET_NAME}-segments"
KMS_KEY_ID = "testkey-1"


def setup_user(uid, subuser, s3_access, s3_secret, swift_secret):
    exec_cmd(f'radosgw-admin user create --uid={uid} --display-name="Test {uid}" --access-key={s3_access} --secret={s3_secret}')
    exec_cmd(f"radosgw-admin subuser create --uid={uid} --subuser={subuser} --access=full")
    exec_cmd(f"radosgw-admin key create --subuser={subuser} --key-type=swift --secret={swift_secret}")


def teardown_user(uid):
    exec_cmd(f"radosgw-admin user rm --uid={uid} --purge-data")


def get_swift_auth(endpoint, subuser, secret):
    headers = {"X-Auth-User": subuser, "X-Auth-Key": secret}
    resp = requests.get(f"{endpoint}/auth/1.0", headers=headers)
    resp.raise_for_status()
    return resp.headers["X-Storage-Url"], resp.headers["X-Auth-Token"]


def get_md5(data):
    return hashlib.md5(data).hexdigest()


def main():
    log.basicConfig(level=log.DEBUG)
    endpoint = os.environ.get("RGW_URL", "http://localhost:8000")

    log.debug("TEST: Setup user and credentials")
    setup_user(TEST_UID, TEST_SUBUSER, TEST_ACCESS_KEY, TEST_SECRET_KEY, TEST_SWIFT_SECRET)

    try:
        swift_url, swift_token = get_swift_auth(endpoint, TEST_SUBUSER, TEST_SWIFT_SECRET)
        swift_headers = {"X-Auth-Token": swift_token}

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=TEST_ACCESS_KEY,
            aws_secret_access_key=TEST_SECRET_KEY,
        )

        log.debug("TEST: Create buckets and apply SSE-KMS encryption")
        kms_config = {
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "aws:kms",
                        "KMSMasterKeyID": KMS_KEY_ID,
                    }
                }
            ]
        }

        for bkt in [BUCKET_NAME, SEGMENTS_BUCKET]:
            s3.create_bucket(Bucket=bkt)
            s3.put_bucket_encryption(Bucket=bkt, ServerSideEncryptionConfiguration=kms_config)

        log.debug("TEST: S3 Upload -> S3/Swift Download")
        content_s3 = b"Hello from S3 API!"
        expected_md5_s3 = get_md5(content_s3)

        s3.put_object(Bucket=BUCKET_NAME, Key="s3-uploaded.txt", Body=content_s3)

        res_s3 = s3.get_object(Bucket=BUCKET_NAME, Key="s3-uploaded.txt")
        assert (get_md5(res_s3["Body"].read()) == expected_md5_s3), "FAIL: S3 -> S3 download mismatch"

        r_swift = requests.get(f"{swift_url}/{BUCKET_NAME}/s3-uploaded.txt", headers=swift_headers)
        r_swift.raise_for_status()
        assert (get_md5(r_swift.content) == expected_md5_s3), "FAIL: S3 -> Swift download mismatch"

        log.debug("TEST: Swift Upload -> Swift/S3 Download")
        content_swift = b"Hello from Swift API!"
        expected_md5_swift = get_md5(content_swift)

        r_put = requests.put(
            f"{swift_url}/{BUCKET_NAME}/swift-uploaded.txt",
            headers=swift_headers,
            data=content_swift,
        )
        r_put.raise_for_status()

        r_swift2 = requests.get(f"{swift_url}/{BUCKET_NAME}/swift-uploaded.txt", headers=swift_headers)
        assert (get_md5(r_swift2.content) == expected_md5_swift), "FAIL: Swift -> Swift download mismatch"

        res_s3_2 = s3.get_object(Bucket=BUCKET_NAME, Key="swift-uploaded.txt")
        assert (get_md5(res_s3_2["Body"].read()) == expected_md5_swift), "FAIL: Swift -> S3 download mismatch"

        log.debug("TEST: Verify Backend Encryption Metadata")
        meta1 = s3.head_object(Bucket=BUCKET_NAME, Key="s3-uploaded.txt")
        assert (meta1.get("ServerSideEncryption") == "aws:kms"), "FAIL: S3 object missing SSE metadata"

        meta2 = s3.head_object(Bucket=BUCKET_NAME, Key="swift-uploaded.txt")
        assert (meta2.get("ServerSideEncryption") == "aws:kms"), "FAIL: Swift object missing SSE metadata"

        log.debug("TEST: Swift SLO Multipart Upload")
        part1, part2, part3 = b"Part 1 ", b"Part 2 ", b"Part 3"
        slo_content = part1 + part2 + part3
        expected_md5_slo = get_md5(slo_content)

        manifest = []
        for i, part_data in enumerate([part1, part2, part3], 1):
            key = f"slo-test/{i:03d}"
            r_part = requests.put(
                f"{swift_url}/{SEGMENTS_BUCKET}/{key}",
                headers=swift_headers,
                data=part_data,
            )
            r_part.raise_for_status()
            etag = r_part.headers["ETag"].strip('"')
            manifest.append(
                {
                    "path": f"/{SEGMENTS_BUCKET}/{key}",
                    "etag": etag,
                    "size_bytes": len(part_data),
                }
            )

        meta_seg = s3.head_object(Bucket=SEGMENTS_BUCKET, Key="slo-test/001")
        assert (meta_seg.get("ServerSideEncryption") == "aws:kms"), "FAIL: SLO segment missing SSE metadata"

        r_manifest = requests.put(
            f"{swift_url}/{BUCKET_NAME}/slo-object.txt?multipart-manifest=put",
            headers=swift_headers,
            json=manifest,
        )
        r_manifest.raise_for_status()

        r_slo_swift = requests.get(f"{swift_url}/{BUCKET_NAME}/slo-object.txt", headers=swift_headers)
        assert (get_md5(r_slo_swift.content) == expected_md5_slo), "FAIL: SLO Swift -> Swift download mismatch"

        res_slo_s3 = s3.get_object(Bucket=BUCKET_NAME, Key="slo-object.txt")
        assert (get_md5(res_slo_s3["Body"].read()) == expected_md5_slo), "FAIL: SLO Swift -> S3 download mismatch"

    finally:
        log.debug(f"Deleting user {TEST_UID} and purging test data")
        teardown_user(TEST_UID)


if __name__ == "__main__":
    main()
