=================
 Swift API Tests
=================

RGW-specific Swift API tests in ``test_swift.py``. ctest runs them as
``run-rgw-swift-tests`` against a private vstart cluster.

To run them against your own vstart cluster, with its RGW port in
``CEPH_RGW_PORT`` (default 8000)::

        python3 test_swift.py [-k PATTERN]
