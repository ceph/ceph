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

Running the Tests
=================

From within the ``src/test/rgw/bucket_logging`` directory:

.. code-block:: bash

   BUCKET_LOGGING_TESTS_CONF=bucket_logging_tests.conf.SAMPLE tox

Configuration
=============

Copy ``bucket_logging_tests.conf.SAMPLE`` to ``bucket_logging_tests.conf`` and 
update the values to match your test environment:

* ``host`` - RGW hostname (default: localhost)
* ``port`` - RGW port (default: 8000)
* ``access_key`` - S3 access key
* ``secret_key`` - S3 secret key

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
