Running Tests in the Cloud
==========================

In this chapter, we will explain in detail how use an OpenStack
tenant as an environment for Ceph `integration testing`_.

Assumptions and caveat
----------------------

We assume that:

1. you are the only person using the tenant
2. you have the credentials
3. the tenant supports the ``nova`` and ``cinder`` APIs

Caveat: be aware that, as of this writing (July 2016), testing in
OpenStack clouds is a new feature. Things may not work as advertised.
If you run into trouble, ask for help on `IRC`_ or the `Mailing list`_, or
open a bug report at the `ceph-workbench bug tracker`_.

.. _`ceph-workbench bug tracker`: http://ceph-workbench.dachary.org/root/ceph-workbench/issues

Prepare tenant
--------------

If you have not tried to use ``ceph-workbench`` with this tenant before,
proceed to the next step.

To start with a clean slate, login to your tenant via the Horizon dashboard
and:

* terminate the ``teuthology`` and ``packages-repository`` instances, if any
* delete the ``teuthology`` and ``teuthology-worker`` security groups, if any
* delete the ``teuthology`` and ``teuthology-myself`` key pairs, if any

Also do the above if you ever get key-related errors ("invalid key", etc.)
when trying to schedule suites.

Getting ceph-workbench
----------------------

Since testing in the cloud is done using the ``ceph-workbench ceph-qa-suite``
tool, you will need to install that first. It is designed
to be installed via Docker, so if you don't have Docker running on your
development machine, take care of that first. You can follow `the official
tutorial <https://docs.docker.com/engine/installation/>`_ to install if
you have not installed yet.

Once Docker is up and running, install ``ceph-workbench`` by following the
`Installation instructions in the ceph-workbench documentation
<http://ceph-workbench.readthedocs.io/en/latest/#installation>`_.

Linking ceph-workbench with your OpenStack tenant
-------------------------------------------------

Before you can trigger your first teuthology suite, you will need to link
``ceph-workbench`` with your OpenStack account.

First, download a ``openrc.sh`` file by clicking on the "Download OpenStack
RC File" button, which can be found in the "API Access" tab of the "Access
& Security" dialog of the OpenStack Horizon dashboard.

Second, create a ``~/.ceph-workbench`` directory, set its permissions to
700, and move the ``openrc.sh`` file into it. Make sure that the filename
is exactly ``~/.ceph-workbench/openrc.sh``.

Third, edit the file so it does not ask for your OpenStack password
interactively. Comment out the relevant lines and replace them with
something like::

    export OS_PASSWORD="aiVeth0aejee3eep8rogho3eep7Pha6ek"

When ``ceph-workbench ceph-qa-suite`` connects to your OpenStack tenant for
the first time, it will generate two keypairs: ``teuthology-myself`` and
``teuthology``.

.. If this is not the first time you have tried to use
.. ``ceph-workbench ceph-qa-suite`` with this tenant, make sure to delete any
.. stale keypairs with these names!

Run the dummy suite
-------------------

You are now ready to take your OpenStack teuthology setup for a test
drive::

    $ ceph-workbench ceph-qa-suite --suite dummy

Be forewarned that the first run of ``ceph-workbench ceph-qa-suite`` on a
pristine tenant will take a long time to complete because it downloads a VM
image and during this time the command may not produce any output.

The images are cached in OpenStack, so they are only downloaded once.
Subsequent runs of the same command will complete faster.

Although ``dummy`` suite does not run any tests, in all other respects it
behaves just like a teuthology suite and produces some of the same
artifacts.

The last bit of output should look something like this::

  pulpito web interface: http://149.202.168.201:8081/
  ssh access           : ssh -i /home/smithfarm/.ceph-workbench/teuthology-myself.pem ubuntu@149.202.168.201 # logs in /usr/share/nginx/html

What this means is that ``ceph-workbench ceph-qa-suite`` triggered the test
suite run. It does not mean that the suite run has completed. To monitor
progress of the run, check the Pulpito web interface URL periodically, or
if you are impatient, ssh to the teuthology machine using the ssh command
shown and do::

    $ tail -f /var/log/teuthology.*

The `/usr/share/nginx/html` directory contains the complete logs of the
test suite. If we had provided the ``--upload`` option to the
``ceph-workbench ceph-qa-suite`` command, these logs would have been
uploaded to http://teuthology-logs.public.ceph.com.

Run a standalone test
---------------------

The standalone test explained in `Reading a standalone test`_ can be run
with the following command::

    $ ceph-workbench ceph-qa-suite --suite rados/singleton/all/admin-socket.yaml

This will run the suite shown on the current ``master`` branch of
``ceph/ceph.git``. You can specify a different branch with the ``--ceph``
option, and even a different git repo with the ``--ceph-git-url`` option. (Run
``ceph-workbench ceph-qa-suite --help`` for an up-to-date list of available
options.)

The first run of a suite will also take a long time, because ceph packages
have to be built, first. Again, the packages so built are cached and
``ceph-workbench ceph-qa-suite`` will not build identical packages a second
time.

Interrupt a running suite
-------------------------

Teuthology suites take time to run. From time to time one may wish to
interrupt a running suite. One obvious way to do this is::

    ceph-workbench ceph-qa-suite --teardown

This destroys all VMs created by ``ceph-workbench ceph-qa-suite`` and
returns the OpenStack tenant to a "clean slate".

Sometimes you may wish to interrupt the running suite, but keep the logs,
the teuthology VM, the packages-repository VM, etc. To do this, you can
``ssh`` to the teuthology VM (using the ``ssh access`` command reported
when you triggered the suite -- see `Run the dummy suite`_) and, once
there::

    sudo /etc/init.d/teuthology restart

This will keep the teuthology machine, the logs and the packages-repository
instance but nuke everything else.

Upload logs to archive server
-----------------------------

Since the teuthology instance in OpenStack is only semi-permanent, with
limited space for storing logs, ``teuthology-openstack`` provides an
``--upload`` option which, if included in the ``ceph-workbench ceph-qa-suite``
command, will cause logs from all failed jobs to be uploaded to the log
archive server maintained by the Ceph project. The logs will appear at the
URL::

    http://teuthology-logs.public.ceph.com/$RUN

where ``$RUN`` is the name of the run. It will be a string like this::

    ubuntu-2016-07-23_16:08:12-rados-hammer-backports---basic-openstack

Even if you don't providing the ``--upload`` option, however, all the logs can
still be found on the teuthology machine in the directory
``/usr/share/nginx/html``.

Provision VMs ad hoc
--------------------

From the teuthology VM, it is possible to provision machines on an "ad hoc"
basis, to use however you like. The magic incantation is::

    teuthology-lock --lock-many $NUMBER_OF_MACHINES \
        --os-type $OPERATING_SYSTEM \
        --os-version $OS_VERSION \
        --machine-type openstack \
        --owner $EMAIL_ADDRESS

The command must be issued from the ``~/teuthology`` directory. The possible
values for ``OPERATING_SYSTEM`` AND ``OS_VERSION`` can be found by examining
the contents of the directory ``teuthology/openstack/``. For example::

    teuthology-lock --lock-many 1 --os-type ubuntu --os-version 16.04 \
        --machine-type openstack --owner foo@example.com

When you are finished with the machine, find it in the list of machines::

    openstack server list

to determine the name or ID, and then terminate it with::

    openstack server delete $NAME_OR_ID

Deploy a cluster for manual testing
-----------------------------------

The `teuthology framework`_ and ``ceph-workbench ceph-qa-suite`` are
versatile tools that automatically provision Ceph clusters in the cloud and
run various tests on them in an automated fashion. This enables a single
engineer, in a matter of hours, to perform thousands of tests that would
keep dozens of human testers occupied for days or weeks if conducted
manually.

However, there are times when the automated tests do not cover a particular
scenario and manual testing is desired. It turns out that it is simple to
adapt a test to stop and wait after the Ceph installation phase, and the
engineer can then ssh into the running cluster. Simply add the following
snippet in the desired place within the test YAML and schedule a run with the
test::

    tasks:
    - exec:
        client.0:
          - sleep 1000000000 # forever

(Make sure you have a ``client.0`` defined in your ``roles`` stanza or adapt
accordingly.)

The same effect can be achieved using the ``interactive`` task::

    tasks:
    - interactive

By following the test log, you can determine when the test cluster has entered
the "sleep forever" condition. At that point, you can ssh to the teuthology
machine and from there to one of the target VMs (OpenStack) or teuthology
worker machines machine (Sepia) where the test cluster is running.

The VMs (or "instances" in OpenStack terminology) created by
``ceph-workbench ceph-qa-suite`` are named as follows:

``teuthology`` - the teuthology machine

``packages-repository`` - VM where packages are stored

``ceph-*`` - VM where packages are built

``target*`` - machines where tests are run

The VMs named ``target*`` are used by tests. If you are monitoring the
teuthology log for a given test, the hostnames of these target machines can
be found out by searching for the string ``Locked targets``::

    2016-03-20T11:39:06.166 INFO:teuthology.task.internal:Locked targets:
      target149202171058.teuthology: null
      target149202171059.teuthology: null

The IP addresses of the target machines can be found by running ``openstack
server list`` on the teuthology machine, but the target VM hostnames (e.g.
``target149202171058.teuthology``) are resolvable within the teuthology
cluster.

.. _Integration testing: ../tests-integration-tests
.. _IRC:  ../essentials/#irc
.. _Mailing List: ../essentials/#mailing-list
.. _Reading A Standalone Test: ../testing-integration-tests/#reading-a-standalone-test
.. _teuthology framework: https://github.com/ceph/teuthology
