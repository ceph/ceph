=======
rbench
=======

A simple load tester tool that can set of a specified count of requests in
parallel against an endpoint. Currently s3 authentication is supported, since
all the requests are fired off in parallel, there is no guarantee of ordering,
so creating buckets has to be done before hand (which is configurable)

For getting started run::

  ./bootstrap.sh
  # fix eg/rgw_tests.conf to match local paths
  ./scheduler-venv/bin/rbench rgw_tests.conf


For running the tests similar to teuthology::

  TEST_CONF=rgw_tests.conf ./scheduler-venv/bin/pytest -v
