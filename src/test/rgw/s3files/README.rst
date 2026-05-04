==================
S3 Files API Tests
==================

Conformance, divergence, and semantic tests for the RGW S3 Files
API, driven through ``boto3``'s ``s3files`` client. The boto3
client is generated upstream from the same Smithy model that is
vendored at ``src/rgw/spec/aws/s3files/``, so this suite enforces
shape-conformance with the AWS spec by construction.

The corresponding feature design lives at
``doc/dev/radosgw/s3_files_api.rst``.

Prerequisites
=============

* Start the cluster using the ``vstart.sh`` script with RGW
  enabled and the ``s3files`` entry present in
  ``rgw_enable_apis``.
* Ensure the RGW endpoint is accessible (default:
  ``http://localhost:8000``).
* ``boto3`` >= 1.42.0 (older versions lack the ``s3files``
  client).

Configuration
=============

Copy ``s3files_tests.conf.SAMPLE`` to ``s3files_tests.conf`` and
update the values to match your test environment:

* ``host`` — RGW hostname (default: localhost)
* ``port`` — RGW port (default: 8000)
* ``scheme`` — ``http`` or ``https`` (default: http)
* ``access_key`` — S3 access key
* ``secret_key`` — S3 secret key
* ``user_id`` — Ceph account/user owning the test resources

Running the Tests
=================

From within the ``src/test/rgw/s3files`` directory::

   S3FILES_TESTS_CONF=s3files_tests.conf tox

Run a subset by marker::

   S3FILES_TESTS_CONF=s3files_tests.conf tox -- -m smoke
   S3FILES_TESTS_CONF=s3files_tests.conf tox -- -m conformance
   S3FILES_TESTS_CONF=s3files_tests.conf tox -- -m divergence
   S3FILES_TESTS_CONF=s3files_tests.conf tox -- -m read_after_write

Interactive walkthrough
=======================

For an interactive tour of the API by hand — useful for exploring
shapes, error envelopes, or smoke-checking a fresh cluster — see
``walkthrough.sh``. It exercises the full Smithy operation surface
(create / get / list / update / delete / tag / policy / sync /
mount-target / pagination) via the AWS CLI, with a handful of
representative negative cases. Sections are independent enough to
copy-paste individually.

::

   ZONE_ID=$(./bin/radosgw-admin zonegroup get \
     | python3 -c 'import json,sys; print(json.load(sys.stdin)["zones"][0]["id"].replace("-",""))') \
   ./walkthrough.sh

Test Coverage
=============

Smoke
-----

* Boto3 client construction (validates the boto3 version pin)
* RGW endpoint reachability (transport-level)

Conformance (planned, per-op)
-----------------------------

One file per AWS S3 Files operation. Each test exercises positive
shape and asserts on the declared error set, using boto3-typed
exceptions. Failing tests until the corresponding handler ships.

Divergences (planned)
---------------------

Captures intentional Ceph deviations from the AWS shape:
``subnetId`` reinterpreted as a Ceph zone-id, caller-supplied
IP fields ignored, AccessPoint id surfacing in the NFS
pseudo-path, and similar.

Read-after-write (planned)
--------------------------

Layered semantic tests confirming create/get/delete round-trips
correctly persist state — catches the "200 returned but didn't
actually persist" failure mode that shape conformance alone
misses.
