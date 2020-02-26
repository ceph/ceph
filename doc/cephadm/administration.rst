.. _cephadm:

====================
cephadm orchestrator
====================

The cephadm orchestrator is an orchestrator module that does not rely
on a separate system such as Rook or Ansible, but rather manages nodes
in a cluster by establishing an SSH connection and issuing explicit
management commands.

Orchestrator modules only provide services to other modules, which in turn
provide user interfaces.  To try out the cephadm module, you might like
to use the :ref:`Orchestrator CLI <orchestrator-cli-module>` module.

Requirements
============

- Python 3
- Podman or Docker
- LVM2

Configuration
=============

The cephadm orchestrator can be configured to use an SSH configuration file. This is
useful for specifying private keys and other SSH connection options.

::

    # ceph config set mgr mgr/cephadm/ssh_config_file /path/to/config

An SSH configuration file can be provided without requiring an accessible file
system path as the method above does.

::

    # ceph cephadm set-ssh-config -i /path/to/config

To clear this value use the command:

::

    # ceph cephadm clear-ssh-config

Health checks
=============

CEPHADM_STRAY_HOST
------------------

One or more hosts have running Ceph daemons but are not registered as
hosts managed by *cephadm*.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orch ps`).

You can manage the host(s) with::

  ceph orch host add *<hostname>*

Note that you may need to configure SSH access to the remote host
before this will work.

Alternatively, you can manually connect to the host and ensure that
services on that host are removed and/or migrated to a host that is
managed by *cephadm*.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_hosts false

CEPHADM_STRAY_DAEMON
--------------------

One or more Ceph daemons are running but not are not managed by
*cephadm*, perhaps because they were deploy using a different tool, or
were started manually.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orch ps`).

**FIXME:** We need to implement and document an adopt procedure here.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_daemons false

CEPHADM_HOST_CHECK_FAILED
-------------------------

One or more hosts have failed the basic cephadm host check, which verifies
that (1) the host is reachable and cephadm can be executed there, and (2)
that the host satisfies basic prerequisites, like a working container
runtime (podman or docker) and working time synchronization.
If this test fails, cephadm will no be able to manage services on that host.

You can manually run this check with::

  ceph cephadm check-host *<hostname>*

You can remove a broken host from management with::

  ceph orch host rm *<hostname>*

You can disable this health warning with::

  ceph config set mgr mgr/cephadm/warn_on_failed_host_check false


Converting an existing cluster to cephadm
=========================================

Cephadm allows you to (pretty) easily convert an existing Ceph cluster that
has been deployed with ceph-deploy, ceph-ansible, DeepSea, or similar tools.

Limitations
-----------

* Cephadm only works with BlueStore OSDs.  If there are FileStore OSDs
  in your cluster you cannot manage them.

Adoption Process
----------------

#. Get the ``cephadm`` command line too on each host.  You can do this with curl or by installing the package.  The simplest approach is::

     [each host] # curl --silent --remote-name --location https://github.com/ceph/ceph/raw/master/src/cephadm/cephadm
     [each host] # chmod +x cephadm

#. Prepare each host for use by ``cephadm``::

     [each host] # ./cephadm prepare-host

#. List all Ceph daemons on the current host::

     # ./cephadm ls

   You should see that all existing daemons have a type of ``legacy``
   in the resulting output.

#. Determine which Ceph version you will use.  You can use any Octopus
   release or later.  For example, ``docker.io/ceph/ceph:v15.2.0``.  The default
   will be the latest stable release, but if you are upgrading from an earlier
   release at the same time be sure to refer to the upgrade notes for any
   special steps to take while upgrading.

   The image is passed to cephadm with::

     # ./cephadm --image $IMAGE <rest of command goes here>

#. Adopt each monitor::

     # ./cephadm adopt --style legacy --name mon.<hostname>

#. Adopt each manager::

     # ./cephadm adopt --style legacy --name mgr.<hostname>

#. Enable cephadm::

     # ceph mgr module enable cephadm
     # ceph orch set backend cephadm

#. Generate an SSH key::

     # ceph cephadm generate-key
     # ceph cephadm get-pub-key

#. Install the SSH key on each host to be managed::

     # echo <ssh key here> | sudo tee /root/.ssh/authorized_keys

   Note that ``/root/.ssh/authorized_keys`` should have mode ``0600`` and
   ``/root/.ssh`` should have mode ``0700``.

#. Tell cephadm which hosts to manage::

     # ceph orch host add <hostname> [ip-address]

   This will perform a ``cephadm check-host`` on each host before
   adding it to ensure it is working.  The IP address argument is only
   required if DNS doesn't allow you to connect to each host by it's
   short name.

#. Verify that the monitor and manager daemons are visible::

     # ceph orch ps

#. Adopt all remainingg daemons::

      # ./cephadm adopt --style legacy --name <osd.0>
      # ./cephadm adopt --style legacy --name <osd.1>
      # ./cephadm adopt --style legacy --name <mds.foo>

   Repeat for each host and daemon.

#. Check the ``ceph health detail`` output for cephadm warnings about
   stray cluster daemons or hosts that are not yet managed.
