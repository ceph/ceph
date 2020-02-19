"""
Add your constants here

"""

cephqe_scripts = {
    "DIR": {"v1": {"script": "rgw/v1/tests/s3/", "config": "rgw/v1/tests/s3/yamls"},
            "v2": {"script": "rgw/v2/tests/s3_swift/", "config": "rgw/v2/tests/s3_swift/configs"}
    },
    "MASTER_BRANCH": "master",
    "REPO_NAME": "ceph-qe-scripts",
    "WIP_BRANCH": None
}

HEALTH = {
    "error": "HEALTH_ERR",
    "warn": "HEALTH_WARN",
    "good": "HEALTH_OK"
}
