# Multi Site Test Framework
This framework allows you to write and run tests against a **local** multi-cluster environment. The framework is using the `mstart.sh` script in order to setup the environment according to a configuration file, and then uses the [nose](https://nose.readthedocs.io/en/latest/) test framework to actually run the tests.
Tests are written in python2.7, but can invoke shell scripts, binaries etc.
## Running Tests
Entry point for all tests is `/path/to/ceph/src/test/rgw/test_multi.py`. And the actual tests are located inside the `/path/to/ceph/src/test/rgw/rgw_multi` subdirectory.
So, to run all tests use:
```
$ cd /path/to/ceph/src/test/rgw/
$ nosetests test_multi.py
```
This will assume a configuration file called `/path/to/ceph/src/test/rgw/test_multi.conf` exists.
To use a different configuration file, set the `RGW_MULTI_TEST_CONF` environment variable to point to that file.
Since we use the same entry point file for all tests, running specific tests is possible using the following format:
```
$ nosetests test_multi.py:<specific_test_name>
```
To run multiple tests based on wildcard string, use the following format:
```
$ nosetests test_multi.py -m "<wildcard string>"
```
Note that the test to run, does not have to be inside the `test_multi.py` file.
Note that different options for running specific and multiple tests exists in the [nose documentation](https://nose.readthedocs.io/en/latest/usage.html#options), as well as other options to control the execution of the tests.
## Configuration
### Environment Variables
Following RGW environment variables are taken into consideration when running the tests:
 - `RGW_FRONTEND`: used to change frontend to 'civetweb' or 'beast' (default)
 - `RGW_VALGRIND`: used to run the radosgw under valgrind. e.g. RGW_VALGRIND=yes
Other environment variables used to configure elements other than RGW can also be used as they are used in vstart.sh. E.g. MON, OSD, MGR, MSD
The configuration file for the run has 3 sections:
### Default
This section holds the following parameters:
 - `num_zonegroups`: number of zone groups (integer, default 1)
 - `num_zones`: number of regular zones in each group (integer, default 3)
 - `num_az_zones`: number of archive zones (integer, default 0, max value 1)
 - `gateways_per_zone`: number of RADOS gateways per zone (integer, default 2)
 - `no_bootstrap`: whether to assume that the cluster is already up and does not need to be setup again. If set to "false", it will try to re-run the cluster, so, `mstop.sh` must be called beforehand. Should be set to false, anytime the configuration is changed. Otherwise, and assuming the cluster is already up, it should be set to "true" to save on execution time (boolean, default false)
 - `log_level`: console log level of the logs in the tests, note that any program invoked from the test my emit logs regardless of that setting (integer, default 20)
    - 20 and up -> DEBUG
    - 10 and up -> INFO
    - 5 and up -> WARNING
    - 1 and up -> ERROR
    - CRITICAL is always logged
- `log_file`: log file name. If not set, only console logs exists (string, default None)
- `file_log_level`: file log level of the logs in the tests. Similar to `log_level`
- `tenant`: name of tenant (string, default None)
- `checkpoint_retries`: *TODO* (integer, default 60)
- `checkpoint_delay`: *TODO* (integer, default 5)
- `reconfigure_delay`: *TODO* (integer, default 5)          
### Elasticsearch
*TODO*
### Cloud
*TODO*
## Writing Tests
New tests should be added into the `/path/to/ceph/src/test/rgw/rgw_multi` subdirectory.
- Base classes are in: `/path/to/ceph/src/test/rgw/rgw_multi/multisite.py`
- `/path/to/ceph/src/test/rgw/rgw_multi/tests.py` holds the majority of the tests, but also many utility and infrastructure functions that could be used in other tests files 
