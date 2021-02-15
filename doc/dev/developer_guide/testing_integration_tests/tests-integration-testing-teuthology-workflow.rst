.. _tests-integration-testing-teuthology-workflow:

Integration Tests using Teuthology Workflow
===========================================

Scheduling Test Run
-------------------

Getting binaries
****************

Ceph binaries must be built for your branch before you can use teuthology to run integration tests on them. Follow these steps to build the Ceph binaries:

#. Push the branch to the `ceph-ci`_ repository. This triggers the process of
   building the binaries on the Jenkins CI.

#. To ensure that the build process has been initiated, confirm that the branch
   name has appeared in the list of "Latest Builds Available" at `Shaman`_.
   Soon after you start the build process, the testing infrastructrure adds
   other, similarly-named builds to the list of "Latest Builds Available".
   The names of these new builds will contain the names of various Linux
   distributions of Linux and will be used to test your build against those
   Linux distributions. 

#. Wait for the packages to be built and uploaded to `Chacra`_, and wait for
   the repositories offering the packages to be created. The entries for the
   branch names in the list of "Latest Builds Available" on `Shaman`_ will turn
   green to indicate that the packages have been uploaded to `Chacra`_ and to
   indicate that their repositories have been created.  Wait until each entry
   is coloured green. This usually takes between two and three hours depending
   on the availability of the machines.
   
   The Chacra URL for a particular build can be queried from `the Chacra site`_.

.. note:: The branch to be pushed on ceph-ci can be any branch. The branch does
   not have to be a PR branch.

.. note:: If you intend to push master or any other standard branch, check
   `Shaman`_ beforehand since it might already have completed builds for it.

.. _the Chacra site: https://shaman.ceph.com/api/search/?status=ready&project=ceph


Triggering Tests
****************

After you have built Ceph binaries for your branch, you can run tests using
teuthology. This procedure explains how to run tests using teuthology.

#. Log in to the teuthology machine:

   .. prompt:: bash $

       ssh <username>@teuthology.front.sepia.ceph.com

   This requires that you have access to the Sepia lab. Learn about requesting
   access here: 
   
       https://ceph.github.io/sepia/adding_users/

#. Install teuthology in a virtual environment and activate that virtual
   environment. Follow the relevant instructions in `Running Your First Test`_.

#. Run the ``teuthology-suite`` command:

   .. prompt:: bash $

        teuthology-suite -v \
        -m smithi \
        -c wip-devname-feature-x \
        -s fs \
        -p 110 \
        --filter "cephfs-shell" \
        -e foo@gmail.com \
        -R fail

   The options in the above command are defined here: 

      =============  =========================================================
         Option        Meaning
      =============  =========================================================
        -v            verbose
        -m            machine name
        -c            the name of the branch that was pushed on ceph-ci
        -s            test-suite name
        -p            the higher the number, the lower the priority of 
                      the job
        --filter      filter tests in a given suite. The argument
                      passed to this filter specifies which test you 
                      want to run
        -e <email>    When tests finish or time out, send an email to the
                      specified address. Can also be specified in 
                      ~/.teuthology.yaml as 'results_email'
        -R            A comma-separated list of statuses to be used
                      with --rerun. Supported statuses: 'dead',
                      'fail', 'pass', 'queued', 'running', 'waiting'
                      [default: fail,dead]
      =============  =========================================================

   .. note:: The priority number present in the command above is a placeholder. 
      Do not use it in your own tests. See `Testing Priority`_ for information 
      about recommended values.

   .. note:: Do not issue a command without a priority number. The default 
      value is 1000, a value so large that your job is unlikely ever to run.

   Run ``teuthology-suite --help`` to read descriptions of these and other 
   available options.

#. Wait for the tests to run. ``teuthology-suite`` prints a link to
   `Pulpito`_ where the test results can be viewed.




.. _teuthology_testing_qa_changes:

Testing QA changes (without re-building binaires)
*************************************************

If you are making changes only in the ``qa/`` directory, you do not have to
rebuild the binaries before you re-run tests. If you make changes only in
``qa/``, you can use the binaries built for the ceph-ci branch to re-run tests.
You just have to make sure to tell the ``teuthology-suite`` command to use a
separate branch for running the tests.

The separate branch can be passed to the command by using ``--suite-repo`` and
``--suite-branch``. The first option (``--suite-repo``) accepts the link to the GitHub fork where your PR branch exists and the second option (``--suite-branch``) accepts the name of the PR branch.

For example, if you want to make changes in ``qa/`` after testing ``branch-x``
(which shows up in ceph-ci as a branch named ``wip-username-branch-x``), you
can do so by running following command:

.. prompt:: bash $

   teuthology-suite -v \
   -m smithi \
   -c wip-username-branch-x \
   -s fs \
   -p 50
   --filter cephfs-shell

Then make modifications locally, update the PR branch, and trigger tests from
your PR branch as follows:

.. prompt:: bash $

   teuthology-suite -v \
   -m smithi \
   -c wip-username-branch-x \
   -s fs -p 50 \
   --filter cephfs-shell \
   --suite-repo https://github.com/$username/ceph \
   --suite-branch branch-x

You can verify that the tests were run using this branch by looking at the
values for the keys ``suite_branch``, ``suite_repo`` and ``suite_sha1`` in the
job config printed at the beginning of the teuthology job.

.. note:: If you are making changes that are not in the ``qa/`` directory, 
          you must follow the standard process of triggering builds, waiting 
          for the builds to finish, then triggering tests and waiting for 
          the test results. 

About Suites and Filters
************************

See `Suites Inventory`_ for a list of available suites of integration tests.
Each directory under ``qa/suites`` in the Ceph repository is an integration
test suite, and arguments appropriate to follow ``-s`` can be found there.

Keywords for filtering tests can be found in
``qa/suites/<suite-name>/<subsuite-name>/tasks`` and can be used as arguments
for ``--filter``. Each YAML file in that directory can trigger tests; using the
name of the file without its filename extension as an argument to the
``--filter`` triggers those tests. 

For example, in the command above in the :ref:`Testing QA Changes
<teuthology_testing_qa_changes>` section, `cephfs-shell`` is specified. 
This works because there is a file named ``cephfs-shell.yaml`` in
``qa/suites/fs/basic_functional/tasks/``.

If the filename doesn't suggest what kind of tests it triggers, search the
contents of the file for the ``modules`` attribute. For ``cephfs-shell.yaml``
the ``modules`` attribute is ``tasks.cephfs.test_cephfs_shell``. This means
that it triggers all tests in ``qa/tasks/cephfs/test_cephfs_shell.py``.

Viewing Tests Results
---------------------

Pulpito Dashboard
*****************

Once the teuthology job is scheduled, the status/results for test run could
be checked from https://pulpito.ceph.com/.
It could be used for quickly checking out job logs, their status, etc.

Teuthology Archives
*******************

Once the tests have finished running, the log for the job can be obtained by
clicking on job ID at the Pulpito page for your tests. It's more convenient to
download the log and then view it rather than viewing it in an internet browser
since these logs can easily be up to size of 1 GB. It is easier to
ssh into the teuthology machine again (``teuthology.front.sepia.ceph.com``), and
access the following path::

    /ceph/teuthology-archive/<test-id>/<job-id>/teuthology.log

For example, for above test ID path is::

   /ceph/teuthology-archive/teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi/4588482/teuthology.log

This way the log can be viewed remotely without having to wait too
much.

.. note:: To access archives more conveniently, ``/a/`` has been symbolically
   linked to ``/ceph/teuthology-archive/``. For instance, to access the previous
   example, we can use something like::

   /a/teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi/4588482/teuthology.log

Killing Tests
-------------
Sometimes a teuthology job might not complete running for several minutes or
even hours after tests that were trigged have completed running and other
times wrong set of tests can be triggered is filter wasn't chosen carefully.
To save resource it's better to termniate such a job. Following is the command
to terminate a job::

      teuthology-kill -r teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi

Let's call the argument passed to ``-r`` as test ID. It can be found
easily in the link to the Pulpito page for the tests you triggered. For
example, for the above test ID, the link is - http://pulpito.front.sepia.ceph.com/teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi/

Re-running Tests
----------------
You can pass ``--rerun`` option, with test ID as an argument to it, to
``teuthology-suite`` command. Generally, this is useful in cases where teuthology test
batch has some failed/dead jobs that we might want to retrigger. We can trigger
jobs based on their status using::

   teuthology-suite -v \
    -m smithi \
    -c wip-rishabh-fs-test_cephfs_shell-fix \
    -p 50 \
    --rerun teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi \
    -R fail,dead,queued,running \
    -e $CEPH_QA_MAIL

The meaning of the rest the options is already covered in `Triggering Tests`_
section.

Naming the ceph-ci branch
-------------------------
There are no hard conventions (except for the case of stable branch; see
next paragraph) for how the branch pushed on ceph-ci is named. But, to make
builds and tests easily identitifiable on Shaman and Pulpito respectively,
prepend it with your name. For example branch ``feature-x`` can be named
``wip-yourname-feature-x`` while pushing on ceph-ci.

In case you are using one of the stable branches (e.g.  nautilis, mimic,
etc.), include the name of that stable branch in your ceph-ci branch name.
For example, ``feature-x`` PR branch should be named as
``wip-feature-x-nautilus``. *This is not just a matter of convention but this,
more essentially, builds your branch in the correct environment.*

Delete the branch from ceph-ci, once it's not required anymore. If you are
logged in at GitHub, all your branches on ceph-ci can be easily found here -
https://github.com/ceph/ceph-ci/branches.

.. _ceph-ci: https://github.com/ceph/ceph-ci
.. _Chacra: https://github.com/ceph/chacra/blob/master/README.rst
.. _Pulpito: http://pulpito.front.sepia.ceph.com/
.. _Running Your First Test: ../../running-tests-locally/#running-your-first-test
.. _Shaman: https://shaman.ceph.com/builds/ceph/
.. _Suites Inventory: ../tests-integration-testing-teuthology-intro/#suites-inventory
.. _Testing Priority: ../tests-integration-testing-teuthology-intro/#testing-priority
.. _Triggering Tests: ../tests-integration-testing-teuthology-workflow/#triggering-tests
.. _tests-sentry-developers-guide: ../tests-sentry-developers-guide/
