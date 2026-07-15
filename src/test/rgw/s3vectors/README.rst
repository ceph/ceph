===============
s3vectors Tests
===============

* Start the cluster using the `vstart.sh` script
* Run the test from within the `src/test/rgw/s3vectors` directory:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox`
* To run a specific tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py::<test_name>`
* To run a group of tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py -m "<marker name>"`
* In case of multisite environment, you can set a "secondary" site in the conf file. See: `s3vtests.conf.multisite`

Storage Backend
---------------

The ``s3vector_backend`` setting in the test config controls which storage
backend the tests exercise. It must match the ``rgw_s3vector_backend`` value
in ``ceph.conf``:

* ``local`` — LanceDB stores data on the local filesystem (no S3 bucket needed)
* ``s3`` — LanceDB stores data in an S3 bucket
* ``sal`` — LanceDB stores data via RGW's SAL layer

When set to ``s3`` or ``sal``, the tests automatically create and clean up a
regular S3 bucket with the same name as each vector bucket.

