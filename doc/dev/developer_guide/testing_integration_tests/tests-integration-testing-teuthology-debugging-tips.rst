.. _tests-integration-testing-teuthology-debugging-tips:

Analyzing and Debugging A Teuthology Job
========================================

To learn more about how to schedule an integration test, refer to `Scheduling
Test Run`_.

Viewing Test Results
--------------------

When a teuthology run has been completed successfully, use `pulpito`_ dashboard
to view the results::

   http://pulpito.front.sepia.ceph.com/<job-name>/<job-id>/

.. _pulpito: https://pulpito.ceph.com

or ssh into the teuthology server to view the results of the integration test:

  .. prompt:: bash $

    ssh <username>@teuthology.front.sepia.ceph.com

and access `teuthology archives`_, as in this example:

  .. prompt:: bash $

     nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/

.. note:: This requires you to have access to the Sepia lab. To learn how to
          request access to the Sepia lab, see: 
          https://ceph.github.io/sepia/adding_users/

Identifying Failed Jobs
-----------------------

On pulpito, a job in red means either a failed job or a dead job. A job is
combination of daemons and configurations defined in the yaml fragments in
`qa/suites`_ . Teuthology uses these configurations and runs the tasks listed
in `qa/tasks`_, which are commands that set up the test environment and test
Ceph's components. These tasks cover a large subset of use cases and help to
expose bugs not exposed by `make check`_ testing.

.. _make check: ../tests-integration-testing-teuthology-intro/#make-check

A job failure might be caused by one or more of the following reasons:

* environment setup (`testing on varied
  systems <https://github.com/ceph/ceph/tree/master/qa/distros/supported>`_):
  testing compatibility with stable releases for supported versions.

* permutation of config values: for instance, `qa/suites/rados/thrash
  <https://github.com/ceph/ceph/tree/master/qa/suites/rados/thrash>`_ ensures
  that we run thrashing tests against Ceph under stressful workloads so that we
  can catch corner-case bugs. The final setup config yaml file used for testing
  can be accessed at::

  /a/<job-name>/<job-id>/orig.config.yaml

More details about config.yaml can be found at `detailed test config`_

Triaging the cause of failure
------------------------------

When a job fails, you will need to read its teuthology log in order to triage
the cause of its failure. Use the job's name and id from pulpito to locate your
failed job's teuthology log::

   http://qa-proxy.ceph.com/<job-name>/<job-id>/teuthology.log

Open the log file::

   /a/<job-name>/<job-id>/teuthology.log

For example:

  .. prompt:: bash $ 

     nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/teuthology.log

Every job failure is recorded in the teuthology log as a Traceback and is 
added to the job summary.

Find the ``Traceback`` keyword and search the call stack and the logs for
issues that caused the failure. Usually the traceback will include the command
that failed.

.. note:: The teuthology logs are deleted from time to time. If you are unable
          to access the link in this example, just use any other case from
          http://pulpito.front.sepia.ceph.com/

Reporting the Issue
-------------------

In short: first check to see if your job failure was caused by a known issue,
and if it wasn't, raise a tracker ticket. 

After you have triaged the cause of the failure and you have determined that it
wasn't caused by the changes that you made to the code, this might indicate
that you have encountered a known failure in the upstream branch (in the
example we're considering in this section, the upstream branch is "octopus").
If the failure was not caused by the changes you made to the code, go to
https://tracker.ceph.com and look for tracker issues related to the failure by
using keywords spotted in the failure under investigation.

If you find a similar issue on https://tracker.ceph.com, leave a comment on
that issue explaining the failure as you understand it and make sure to
include a link to your recent test run. If you don't find a similar issue,
create a new tracker ticket for this issue and explain the cause of your job's
failure as thoroughly as you can. If you're not sure what caused the job's
failure, ask one of the team members for help.

Debugging an issue using interactive-on-error
---------------------------------------------

When you encounter a job failure during testing, you should attempt to
reproduce it. This is where ``--interactive-on-error`` comes in. This
section explains how to use ``interactive-on-error`` and what it does. 

When you have verified that a job has failed, run the same job again in
teuthology but add the `interactive-on-error`_ flag::

    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block $<your-config-yaml> --interactive-on-error

Use either `custom config.yaml`_ or the yaml file from the failed job. If
you use the yaml file from the failed job, copy ``orig.config.yaml`` to
your local directory::

    ideepika@teuthology:~/teuthology$ cp /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/orig.config.yaml test.yaml
    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block test.yaml --interactive-on-error

If a job fails when the ``interactive-on-error`` flag is used, teuthology
will lock the machines required by ``config.yaml``. Teuthology will halt
the testing machines and hold them in the state that they were in at the
time of the job failure. You will be put into an interactive python
session. From there, you can ssh into the system to investigate the cause
of the job failure.  

After you have investigated the failure, just terminate the session.
Teuthology will then clean up the session and unlock the machines.

Suggested Resources
--------------------

  * `Testing Ceph: Pains & Pleasures <https://www.youtube.com/watch?v=gj1OXrKdSrs>`_
  * `Teuthology Training <https://www.youtube.com/playlist?list=PLrBUGiINAakNsOwHaIM27OBGKezQbUdM->`_
  * `Intro to Teuthology <https://www.youtube.com/watch?v=WiEUzoS6Nc4>`_

.. _Scheduling Test Run: ../tests-integration-testing-teuthology-workflow/#scheduling-test-run
.. _detailed test config: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html
.. _teuthology archives: ../tests-integration-testing-teuthology-workflow/#teuthology-archives
.. _qa/suites: https://github.com/ceph/ceph/tree/master/qa/suites
.. _qa/tasks: https://github.com/ceph/ceph/tree/master/qa/tasks
.. _interactive-on-error: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html#troubleshooting
.. _custom config.yaml: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html#test-configuration
.. _testing priority: ../tests-integration-testing-teuthology-intro/#testing-priority
.. _thrash: https://github.com/ceph/ceph/tree/master/qa/suites/rados/thrash
