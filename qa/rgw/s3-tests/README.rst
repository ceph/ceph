========================
 S3 compatibility tests
========================

This is a set of unofficial Amazon AWS S3 compatibility
tests, that can be useful to people implementing software
that exposes an S3-like API. The tests use the Boto2 and Boto3 libraries.

The tests use the Tox tool. To get started, ensure you have the ``tox``
software installed; e.g. on Debian/Ubuntu::

	sudo apt-get install tox

You will need to create a configuration file with the location of the
service and two different credentials. A sample configuration file named
``s3tests.conf.SAMPLE`` has been provided in this repo. This file can be
used to run the s3 tests on a Ceph cluster started with vstart.

Once you have that file copied and edited, you can run the tests with::

	S3TEST_CONF=your.conf tox

You can specify which directory of tests to run::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional

You can specify which file of tests to run::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_s3.py

You can specify which test to run::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_s3.py::test_bucket_list_empty

Some tests have attributes set based on their current reliability and
things like AWS not enforcing their spec stricly. You can filter tests
based on their attributes::

	S3TEST_CONF=aws.conf tox -- -m 'not fails_on_aws'

Most of the tests have both Boto3 and Boto2 versions. Tests written in
Boto2 are in the ``s3tests`` directory. Tests written in Boto3 are
located in the ``s3test_boto3`` directory.

You can run only the boto3 tests with::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional

========================
 STS compatibility tests
========================

This section contains some basic tests for the AssumeRole, GetSessionToken and AssumeRoleWithWebIdentity API's. The test file is located under ``s3tests_boto3/functional``.

To run the STS tests, the vstart cluster should be started with the following parameter (in addition to any parameters already used with it)::

        vstart.sh -o rgw_sts_key=abcdefghijklmnop -o rgw_s3_auth_use_sts=true

Note that the ``rgw_sts_key`` can be set to anything that is 128 bits in length.
After the cluster is up the following command should be executed::

      radosgw-admin caps add --tenant=testx --uid="9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef" --caps="roles=*"

You can run only the sts tests (all the three API's) with::

        S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_sts.py

You can filter tests based on the attributes. There is a attribute named ``test_of_sts`` to run AssumeRole and GetSessionToken tests and ``webidentity_test`` to run the AssumeRoleWithWebIdentity tests. If you want to execute only ``test_of_sts`` tests you can apply that filter as below::

        S3TEST_CONF=your.conf tox -- -m test_of_sts s3tests_boto3/functional/test_sts.py

For running ``webidentity_test`` you'll need have Keycloak running.

In order to run any STS test you'll need to add "iam" section to the config file. For further reference on how your config file should look check ``s3tests.conf.SAMPLE``.

========================
 IAM policy tests
========================

This is a set of IAM policy tests.
This section covers tests for user policies such as Put, Get, List, Delete, user policies with s3 actions, conflicting user policies etc
These tests uses Boto3 libraries. Tests are written in the ``s3test_boto3`` directory.

These iam policy tests uses two users with profile name "iam" and "s3 alt" as mentioned in s3tests.conf.SAMPLE.
If Ceph cluster is started with vstart, then above two users will get created as part of vstart with same access key, secrete key etc as mentioned in s3tests.conf.SAMPLE.
Out of those two users, "iam" user is with capabilities --caps=user-policy=* and "s3 alt" user is without capabilities.
Adding above capabilities to "iam" user is also taken care by vstart (If Ceph cluster is started with vstart).

To run these tests, create configuration file with section "iam" and "s3 alt" refer s3tests.conf.SAMPLE.
Once you have that configuration file copied and edited, you can run all the tests with::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_iam.py

You can also specify specific test to run::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_iam.py::test_put_user_policy

Some tests have attributes set such as "fails_on_rgw".
You can filter tests based on their attributes::

	S3TEST_CONF=your.conf tox -- s3tests_boto3/functional/test_iam.py -m 'not fails_on_rgw'

========================
 Bucket logging tests
========================

Ceph has extensions for the bucket logging S3 API. For the tests to cover these extensions, the following file: `examples/rgw/boto3/service-2.sdk-extras.json` from the Ceph repo,
should be copied to the: `~/.aws/models/s3/2006-03-01/` directory on the machine where the tests are run.
If the file is not present, the tests will still run, but the extension tests will be skipped. In this case, the bucket logging object roll time must be decreased manually from its default of
300 seconds to 5 seconds::

  vstart.sh -o rgw_bucket_logging_object_roll_time=5

Then the tests can be run with::

  S3TEST_CONF=your.conf tox -- -m 'bucket_logging'

To run the only bucket logging tests that do not need extension of rollover time, use::

  S3TEST_CONF=your.conf tox -- -m 'bucket_logging and not fails_without_logging_rollover'
