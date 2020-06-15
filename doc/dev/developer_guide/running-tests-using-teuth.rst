Running Integration Tests using Teuthology
==========================================

Getting binaries
----------------
To run integration tests using teuthology, you need to have Ceph binaries
built for your branch. Follow these steps to initiate the build process -

#. Push the branch to `ceph-ci`_ repository. This triggers the process of
   building the binaries.

#. To confirm that the build process has been initiated, spot the branch name
   at `Shaman`_. Little after the build process has been initiated, the single
   entry with your branch name would multiply, each new entry for a different
   combination of distro and flavour.

#. Wait until the packages are built and uploaded, and the repository offering
   them are created. This is marked by colouring the entries for the branch
   name green. Preferably, wait until each entry is coloured green. Usually,
   it takes around 2-3 hours depending on the availability of the machines.

.. note:: Branch to be pushed on ceph-ci can be any branch, it shouldn't
   necessarily be a PR branch.

.. note:: In case you are pushing master or any other standard branch, check
   `Shaman`_ beforehand since it already might have builds ready for it.

Triggering Tests
----------------
After building is complete, proceed to trigger tests -

#. Log in to the teuthology machine::

       ssh <username>@teuthology.front.sepia.ceph.com

   This would require Sepia lab access. To know how to request it, see: https://ceph.github.io/sepia/adding_users/

#. Next, get teuthology installed. Run the first set of commands in
   `Running Your First Test`_ for that. After that, activate the virtual
   environment in which teuthology is installed.

#. Run the ``teuthology-suite`` command::

        teuthology-suite -v -m smithi -c wip-devname-feature-x -s fs -p 110 --filter "cephfs-shell"

   Following are the options used in above command with their meanings -
        -v          verbose
        -m          machine name
        -c          branch name, the branch that was pushed on ceph-ci
        -s          test-suite name
        -p          higher the number, lower the priority of the job
        --filter    filter tests in given suite that needs to run, the arg to
                    filter should be the test you want to run

.. note:: The priority number present in the command above is just a
   placeholder. It might be highly inappropriate for the jobs you may want to
   trigger. See `Testing Priority`_ section to pick a priority number.

.. note:: Don't skip passing a priority number, the default value is 1000
   which way too high; the job probably might never run.

#. Wait for the tests to run. ``teuthology-suite`` prints a link to the
   `Pulpito`_ page created for the tests triggered.

Other frequently used/useful options are ``-d`` (or ``--distro``),
``--distroversion``, ``--filter-out``, ``--timeout``, ``flavor``, ``-rerun``,
``-l`` (for limiting number of jobs) , ``-n`` (for how many times job would
run) and ``-e`` (for email notifications). Run ``teuthology-suite --help``
to read description of these and every other options available.

Testing QA changes (without re-building binaires)
-------------------------------------------------
While writing a PR you might need to test your PR repeatedly using teuthology.
If you are making non-QA changes, you need to follow the standard process of
triggering builds, waiting for it to finish and then triggering tests and
wait for the result. But if changes you made are purely changes in qa/,
you don't need rebuild the binaries. Instead you can test binaries built for
the ceph-ci branch and instruct ``teuthology-suite`` command to use a separate
branch for running tests. The separate branch can be passed to the command
by using ``--suite-repo`` and ``--suite-branch``. Pass the link to the GitHub
fork where your PR branch exists to the first option and pass the PR branch
name to the second option.

For example, if you want to make changes in ``qa/`` after testing ``branch-x``
(of which has ceph-ci branch is ``wip-username-branch-x``) by running
following command::

    teuthology-suite -v -m smithi -c wip-username-branch-x -s fs -p 50 --filter cephfs-shell

You can make the modifications locally, update the PR branch and then
trigger tests from your PR branch as follows::

    teuthology-suite -v -m smithi -c wip-username-branch-x -s fs -p 50 --filter cephfs-shell --suite-repo https://github.com/username/ceph --suite-branch branch-x

You can verify if the tests were run using this branch by looking at values
for the keys ``suite_branch``, ``suite_repo`` and ``suite_sha1`` in the job
config printed at the very beginning of the teuthology job.

About Suites and Filters
------------------------
See `Suites Inventory`_ for a list of suites of integration tests present
right now. Alternatively, each directory under ``qa/suites`` in Ceph
repository is an integration test suite, so looking within that directory
to decide an appropriate argument for ``-s`` also works.

For picking an argument for ``--filter``, look within
``qa/suites/<suite-name>/<subsuite-name>/tasks`` to get keywords for filtering
tests. Each YAML file in there can trigger a bunch of tests; using the name of
the file, without the extension part of the file name, as an argument to the
``--filter`` will trigger those tests. For example, the sample command above
uses ``cephfs-shell`` since there's a file named ``cephfs-shell.yaml`` in
``qa/suites/fs/basic_functional/tasks/``. In case, the file name doesn't hint
what bunch of tests it would trigger, look at the contents of the file for
``modules`` attribute. For ``cephfs-shell.yaml`` the ``modules`` attribute
is ``tasks.cephfs.test_cephfs_shell`` which means it'll trigger all tests in
``qa/tasks/cephfs/test_cephfs_shell.py``.

Killing Tests
-------------
Sometimes a teuthology job might not complete running for several minutes or
even hours after tests that were trigged have completed running and other
times wrong set of tests can be triggered is filter wasn't chosen carefully.
To save resource it's better to termniate such a job. Following is the command
to terminate a job::

        teuthology-kill -r teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi

Let's call the the argument passed to ``-r`` as test ID. It can be found
easily in the link to the Pulpito page for the tests you triggered. For
example, for the above test ID, the link is - http://pulpito.front.sepia.ceph.com/teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi/

Re-running Tests
----------------
Pass ``--rerun`` option, with test ID as an argument to it, to
``teuthology-suite`` command::

    teuthology-suite -v -m smithi -c wip-rishabh-fs-test_cephfs_shell-fix -p 50 --rerun teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi

The meaning of rest of the options is already covered in `Triggering Tests`
section.

Teuthology Archives
-------------------
Once the tests have finished running, the log for the job can be obtained by
clicking on job ID at the Pulpito page for your tests. It's more convenient to
download the log and then view it rather than viewing it in an internet
browser since these logs can easily be upto size of 1 GB. What's much more
easier is to log in to the teuthology machine again
(``teuthology.front.sepia.ceph.com``), and access the following path::

    /ceph/teuthology-archive/<test-id>/<job-id>/teuthology.log

For example, for above test ID path is::

    /ceph/teuthology-archive/teuthology-2019-12-10_05:00:03-smoke-master-testing-basic-smithi/4588482/teuthology.log

This way the log remotely can be viewed remotely without having to wait too
much.

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
.. _Pulpito: http://pulpito.front.sepia.ceph.com/
.. _Running Your First Test: ../running-tests-locally/#running-your-first-test
.. _Shaman: https://shaman.ceph.com/builds/ceph/
.. _Suites Inventory: ../tests-integration-tests/#suites-inventory
.. _Testing Priority: ../tests-integration-tests/#testing-priority
