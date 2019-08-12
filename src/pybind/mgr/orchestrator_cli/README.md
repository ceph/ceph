# Orchestrator CLI

See also [../../../doc/mgr/orchestrator_cli.rst](../../../doc/mgr/orchestrator_cli.rst).

## Running the Teuthology tests

To run the API tests against a real Ceph cluster, we leverage the Teuthology
framework and the `test_orchestrator` backend.

``source`` the script and run the tests manually::

    $ pushd ../dashboard ; source ./run-backend-api-tests.sh ; popd
    $ run_teuthology_tests tasks.mgr.test_orchestrator_cli
    $ cleanup_teuthology  

 