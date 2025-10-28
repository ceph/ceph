===============
s3vectors Tests
===============

* Start the cluster using the `vstart.sh` script
* Run the test from within the `src/test/rgw/s3vectors` directory:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox`
* To run a specific tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py::<test_name>`
* To run a group of tests use:
  `S3VTESTS_CONF=s3vtests.conf.SAMPLE tox -- s3vector_test.py -m "<marker name>"

