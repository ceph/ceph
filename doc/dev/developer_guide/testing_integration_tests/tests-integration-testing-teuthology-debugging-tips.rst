.. _tests-integration-testing-teuthology-debugging-tips:

Analysing and Debugging A Teuthology Job
-----------------------------------------

For scheduling an integration test please refer to, `Scheduling Test Run`_
Here, we will be discussing how to analyse failed/dead jobs to root cause the problem and amend it.

Triaging the cause of failure
------------------------------

Once a teuthology run is successfully completed, we can access the results using
pulpito dashboard for example:

http://pulpito.front.sepia.ceph.com/ideepika-2020-11-03_04:03:28-rados-wip-yuri-testing-2020-10-28-0947-octopus-distro-basic-smithi/ which might look something

This run has 2 job run failures. To triage, open the teuthology log for it using either:

http://pulpito.front.sepia.ceph.com/<job-name>/<job-id>/teuthology.log

or via sshing into teuthology server using::

    ssh teuthology.front.sepia.ceph.com

and then opening log file with signature as:

   /a/<job-name>/<job-id>/teuthology.log

for example in our case::

  nano /a/ideepika-2020-11-03_04:03:28-rados-wip-yuri-testing-2020-10-28-0947-octopus-distro-basic-smithi/5585704/teuthology.log

Generally, a job failure is recorded in teuthology log as a Traceback which gets
added to job summary.  While analysing a job failure, we generally start looking
for ``Traceback`` keyword and further see the call stack and logs that might had
lead to failure Most of the time, traceback will also be including the failing
command.

.. note:: the teuthology logs are deleted every once in a while, if you are
      unable to access example link, please feel free to refer any other case from
      http://pulpito.front.sepia.ceph.com/

Reporting the Issue
-------------------

Once the cause of failure is triaged, and is something which might not be
related to the developer's code change, this indicates that it might be a
generic failure for the upstream branch(in our case octopus), in which case, we
look for related failure keywords on https://tracker.ceph.com/ If a similar
issue has been reported via a tracker.ceph.com ticket, please add any relevant
feedback to it. Otherwise, please create a new tracker ticket for it. If you are
not familiar with the cause of failure, someone else will look at it.

Debugging An Issue
------------------

If you want to work on a tracker issue, assign it to yourself, and try to
reproduce that issue.  For this purpose you can run a job similar to the failed
job, using interactive-on-error mode in teuthology::

    ideepika@teuthology:~/teuthology$ ./virtualenv/bin/teuthology -v --lock --block $<your-config-yaml> --interactive-on-error

More details on using teuthology command please read `detailed test config`_

.. _Scheduling Test Run: ../tests-integration-testing-teuthology-workflow.rst/#scheduling-test-run
.. _detailed test config: https://github.com/ceph/teuthology/blob/master/docs/detailed_test_config.rst
