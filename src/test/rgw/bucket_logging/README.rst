=========================
Bucket Logging Tests
=========================

This test suite covers RGW-specific bucket logging functionality, including:

* radosgw-admin bucket logging commands (list, info, flush)
* Cleanup tests verifying temporary log objects are properly deleted

Prerequisites
=============

* Start the cluster using the ``vstart.sh`` script with RGW enabled
* Ensure the RGW endpoint is accessible (default: http://localhost:8000)

Configuration
=============

Copy ``bucket_logging_tests.conf.SAMPLE`` to ``bucket_logging_tests.conf`` and 
update the values to match your test environment:

* ``host`` - RGW hostname (default: localhost)
* ``port`` - RGW port (default: 8000)
* ``access_key`` - S3 access key
* ``secret_key`` - S3 secret key

Running the Tests
=================

From within the ``src/test/rgw/bucket_logging`` directory:

.. code-block:: bash

   BUCKET_LOGGING_TESTS_CONF=bucket_logging_tests.conf tox

By default, the tests run against ``Standard`` bucket logging mode. To run the
suite against ``Journal`` mode instead, pass the ``--logging-type`` flag:

.. code-block:: bash

   BUCKET_LOGGING_TESTS_CONF=bucket_logging_tests.conf tox -- --logging-type=Journal

Journal mode is a Ceph extension to the S3 bucket logging API. Running the
tests in Journal mode requires the boto3 ``LoggingType`` field, which is
provided by ``examples/rgw/boto3/service-2.sdk-extras.json``. Copy that file
to ``~/.aws/models/s3/2006-03-01/`` before running with ``--logging-type=Journal``;
otherwise the suite exits early with a clear error.

Test Coverage
=============

Admin Commands
--------------

* ``radosgw-admin bucket logging list --bucket <source bucket>``
* ``radosgw-admin bucket logging info --bucket <source bucket>``
* ``radosgw-admin bucket logging info --bucket <log bucket>``
* ``radosgw-admin bucket logging flush --bucket <source bucket>``

Cleanup Tests
-------------

Tests that verify temporary log objects are properly deleted when:

* Log bucket is deleted
* Logging is disabled on source bucket
* Logging configuration is changed
* Source bucket is deleted
