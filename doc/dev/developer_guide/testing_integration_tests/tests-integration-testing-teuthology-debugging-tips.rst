.. _tests-integration-testing-teuthology-debugging-tips:

Analyzing and Debugging A Teuthology Job
========================================

To learn more about how to schedule an integration test, refer to `Scheduling
Test Run`_.

When a teuthology run has been completed successfully, use `pulpito`_ dasboard
to view the results::

   http://pulpito.front.sepia.ceph.com/<job-name>/<job-id>/

.. _pulpito: https://pulpito.ceph.com

or ssh into the teuthology server::

    ssh <username>@teuthology.front.sepia.ceph.com

and access `teuthology archives`_, like this for example:

  .. prompt:: bash $

     nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/

.. note:: This requires you to have access to the Sepia lab. To learn how to
          request access to the Speia lab, see: 
          https://ceph.github.io/sepia/adding_users/

On pulpito, jobs in red specify either a failed job or a dead job.
A job is combination of daemons and configurations that are formed using
`qa/suites`_ yaml fragments.
Teuthology uses these configurations and runs the tasks that are present in
`qa/tasks`_, which are commands used for setting up the test environment and
testing Ceph's components.
These tasks cover a large subset of use cases and help to  
expose the bugs that aren't caught by `make check`_ testing.

.. _make check: ../tests-integration-testing-teuthology-intro/#make-check

A job failure might be caused by one or more of the following reasons:

* environment setup (`testing on varied
  systems <https://github.com/ceph/ceph/tree/master/qa/distros/supported>`_):
  testing compatibility with stable realeases for supported versions.

* permutation of config values: for instance, `qa/suites/rados/thrash
  <https://github.com/ceph/ceph/tree/master/qa/suites/rados/thrash>`_ ensures
  running thrashing tests against Ceph under stressful workloads, so that we
  are able to catch corner-case bugs. The final setup config yaml used for
  testing can be accessed at::

  /a/<job-name>/<job-id>/orig.config.yaml

More details about config.yaml can be found at `detailed test config`_

Triaging the cause of failure
------------------------------

To triage a job failure, open the teuthology log for it using either the job
name or the job id (from the pulpito page):

   http://qa-proxy.ceph.com/<job-name>/<job-id>/teuthology.log

Open the log file:

   /a/<job-name>/<job-id>/teuthology.log

for example in our case::

  nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/teuthology.log

A job failure is recorded in the teuthology log as a Traceback and is 
added to the job summary.

To analyze a job failure, locate the ``Traceback`` keyword and examine the call
stack and logs for issues that caused the failure. Usually the traceback
will include the command that failed.

.. note:: the teuthology logs are deleted every once in a while, if you are
          unable to access example link, please feel free to refer any other 
          case from http://pulpito.front.sepia.ceph.com/

Reporting the Issue
-------------------

After you have triaged the cause of the failure and you have determined that the
failure was not caused by the developer's code change, this might indicate a 
known failure for the upstream branch (in our case, the upstream branch is
octopus). If the failure was not caused by a developer's code change, go to 
https://tracker.ceph.com and look for tracker issues related to the failure by using keywords spotted in the failure under investigation.

If a similar issue has been reported via a tracker.ceph.com ticket, add to it a
link to the new test run and any relevant feedback. If you don't find a ticket
referring to an issue similar to the one that you have discovered, create a new
tracker ticket for it. If you are not familiar with the cause of failure, ask
one of the team members for help.

Debugging an issue using interactive-on-error
---------------------------------------------

It is important to be able to reproduce an issue when investigating its cause.
Run a job similar to the failed job, using the `interactive-on-error`_ mode in
teuthology::

    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block $<your-config-yaml> --interactive-on-error

For this job, use either `custom config.yaml`_ or the yaml file from
the failed job. If you intend to use the yaml file from the failed job, copy 
``orig.config.yaml`` to your local dir and change the `testing priority`_
accordingly, like so::

    ideepika@teuthology:~/teuthology$ cp /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/orig.config.yaml test.yaml
    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block test.yaml --interactive-on-error


In the event of job failure, teuthology will lock the machines required by
``config.yaml``. Teuthology will halt at an interactive python session. 
By sshing into the targets, we can investigate their ctx values.  After we have
investigated the system, we can manually terminate the session and let
teuthology clean the session up.

Suggested Resources
--------------------

  * `Testing Ceph: Pains & Pleasures <https://www.youtube.com/watch?v=gj1OXrKdSrs>`_

.. _Scheduling Test Run: ../tests-integration-testing-teuthology-workflow/#scheduling-test-run
.. _detailed test config: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html
.. _teuthology archives: ../tests-integration-testing-teuthology-workflow/#teuthology-archives
.. _qa/suites: https://github.com/ceph/ceph/tree/master/qa/suites
.. _qa/tasks: https://github.com/ceph/ceph/tree/master/qa/tasks
.. _interactive-on-error: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html#troubleshooting
.. _custom config.yaml: https://docs.ceph.com/projects/teuthology/en/latest/detailed_test_config.html#test-configuration
.. _testing priority: ../tests-integration-testing-teuthology-intro/#testing-priority
.. _thrash: https://github.com/ceph/ceph/tree/master/qa/suites/rados/thrash
