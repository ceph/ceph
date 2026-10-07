#!/usr/bin/env python3
"""Regression test for tracker #81348: IAM user/group names unique per account.

Requires vstart RGW with STS (see test_iam_duplicate_account_names.sh).
Prints TEST_OK or exits non-zero on failure.
"""
import json
import os
import random
import subprocess
import sys
import threading
import time

import boto3
import botocore
from botocore.config import Config

EP = os.environ.get("RGW_ENDPOINT", "http://localhost:8000")
REGION = "us-east-1"
ROUNDS = int(os.environ.get("IAM_DUP_ROUNDS", "40"))
CFG = Config(retries={"max_attempts": 1}, max_pool_connections=64,
             connect_timeout=10, read_timeout=60)


def admin(*args):
    out = subprocess.run(["radosgw-admin", *args], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"radosgw-admin {' '.join(args)}: {out.stderr.strip()}")
    return json.loads(out.stdout) if out.stdout.strip() else None


def code(fn, *a, **kw):
    try:
        return "OK", fn(*a, **kw)
    except botocore.exceptions.ClientError as e:
        return e.response["Error"].get("Code", "?"), e.response["ResponseMetadata"].get("HTTPStatusCode")


def race(*fns):
    res = [None] * len(fns)
    bar = threading.Barrier(len(fns))

    def run(i, f):
        bar.wait()
        time.sleep(random.random() * 0.002)
        res[i] = f()

    ts = [threading.Thread(target=run, args=(i, f)) for i, f in enumerate(fns)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return res


class Account:
    def __init__(self):
        self.id = "RGW" + "".join(random.choice("0123456789") for _ in range(17))
        name = "acct-" + self.id[-6:]
        admin("account", "create", "--account-id", self.id, "--account-name", name)
        u = admin("user", "create", "--account-id", self.id, "--account-root",
                  "--display-name", "root", "--uid", "root-" + self.id[-8:])
        self.key = u["keys"][0]["access_key"]
        self.secret = u["keys"][0]["secret_key"]
        self.iam = boto3.client(
            "iam", endpoint_url=EP, region_name=REGION,
            aws_access_key_id=self.key, aws_secret_access_key=self.secret, config=CFG)


def check_concurrent_create(kind):
    for i in range(ROUNDS):
        acct = Account()
        name = f"dup{i}"
        if kind == "users":
            res = race(*[(lambda n=name, a=acct: code(a.iam.create_user, UserName=n))
                         for _ in range(6)])
            id_key = "User"
            id_field = "UserId"
        else:
            res = race(*[(lambda n=name, a=acct: code(a.iam.create_group, GroupName=n))
                         for _ in range(6)])
            id_key = "Group"
            id_field = "GroupId"
        ok = [r for r in res if r[0] == "OK"]
        if len(ok) > 1:
            ids = sorted({r[1][id_key][id_field] for r in ok})
            print(f"FAIL: {len(ok)} concurrent Create{kind} of {name} succeeded: {ids}",
                  file=sys.stderr)
            return False
    return True


def check_concurrent_rename():
    for i in range(ROUNDS):
        acct = Account()
        acct.iam.create_user(UserName=f"x{i}")
        acct.iam.create_user(UserName=f"y{i}")
        r1, r2 = race(
            lambda a=acct, n=i: code(a.iam.update_user, UserName=f"x{n}", NewUserName=f"z{n}"),
            lambda a=acct, n=i: code(a.iam.update_user, UserName=f"y{n}", NewUserName=f"z{n}"))
        if r1[0] == "OK" and r2[0] == "OK":
            print(f"FAIL: two UpdateUser renames to z{i} both succeeded", file=sys.stderr)
            return False
    return True


def main():
    if not check_concurrent_create("users"):
        return 1
    if not check_concurrent_create("groups"):
        return 1
    if not check_concurrent_rename():
        return 1
    print("TEST_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
