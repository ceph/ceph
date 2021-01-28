.. _tests-integration-testing-teuthology-debugging-tips:

Analyzing and Debugging A Teuthology Job
========================================

For scheduling an integration test please refer to, `Scheduling Test Run`_.

Once a teuthology run is successfully completed, we can access the results using
pulpito dashboard, which looks like:

http://pulpito.front.sepia.ceph.com/<job-name>/<job-id>/

or via sshing into teuthology server::

    ssh <username>@teuthology.front.sepia.ceph.com

and accessing `teuthology archives`_, for example::
  
  nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/

.. note:: This would require Sepia lab access. To know how to request it, see:
          https://ceph.github.io/sepia/adding_users/

On pulpito, the jobs in red specify either a failed or dead job.
Here, a job is combination of daemons and configurations that are formed using
`qa/suites`_ yaml fragments.
Taking these configurations, teuthology runs the tasks that are present in
`qa/tasks`_, which are commands used for setting up the test environment and
testing Ceph's components.
These tasks help us in covering large subset of usecase scenarios and hence
exposing the bugs which were uncaught by `make check`_ testing.

.. _make check: ../tests-integration-testing-teuthology-intro/#make-check

A job failure hence might be because of:

* environment setup(`testing on varied systems<https://github.com/ceph/ceph/tree/master/qa/distros/supported>_`):
  testing compatibility with stable realeases for supported versions.

* permutation of config values: for instance, qa/suites/rados/thrash ensures to
  test Ceph under stressful workload, so that we be able to catch corner case
  bugs.
  The final setup config yaml that would be used for testing can be accessed
  at::

  /a/<job-name>/<job-id>/orig.config.yaml

More details about config.yaml can be found on `detailed test config`_

Triaging the cause of failure
------------------------------

To triage a job failure, open the teuthology log for it using either(from the
pulpito page):

http://qa-proxy.ceph.com/<job-name>/<job-id>/teuthology.log

and then opening log file with signature as:

   /a/<job-name>/<job-id>/teuthology.log

for example in our case::

  nano /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/teuthology.log

Generally, a job failure is recorded in teuthology log as a Traceback which gets
added to job summary.
While analyzing a job failure, we generally start looking for ``Traceback``
keyword and further see the call stack and logs that might had lead to failure.
Most of the time, traceback will also be including the failing command.

.. note:: the teuthology logs are deleted every once in a while, if you are
      unable to access example link, please feel free to refer any other case from
      http://pulpito.front.sepia.ceph.com/

Reporting the Issue
-------------------

Once the cause of failure is triaged, and is something which might not be
related to the developer's code change, this indicates that it might be a
generic failure for the upstream branch (in our case octopus), in which case, we
look for related failure keywords on https://tracker.ceph.com/.
If a similar issue has been reported via a tracker.ceph.com ticket, please add
any relevant feedback to it. Otherwise, please create a new tracker ticket for
it. If you are not familiar with the cause of failure, someone else will look at
it.

Debugging an issue using interactive-on-error
---------------------------------------------

To investigate an issue, the first step would be to try to reproduce it, for
that purpose. For this purpose you can run a job similar to the failed job,
using `interactive-on-error`_ mode in teuthology::

    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block $<your-config-yaml> --interactive-on-error

we can either have a `custom config.yaml`_ or use the one from failed job; for
which copy the ``orig.config.yaml`` to your local dir and change the `testing
priority`_ accordingly, which would look like::

    ideepika@teuthology:~/teuthology$ cp /a/teuthology-2021-01-06_07:01:02-rados-master-distro-basic-smithi/5759282/orig.config.yaml test.yaml
    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block test.yaml --interactive-on-error


Teuthology will then lock the machines required by the ``config.yaml``, when
their is job failure, which halts at an interactive python session which let's
us investigate the ctx values and the targets via sshing into them, once we have
investigated the system, we can manually terminate the session and let
teuthology cleanup.

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
