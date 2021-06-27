============================
Deploying a new Ceph cluster
============================

Cephadm creates a new Ceph cluster by "bootstrapping" on a single
host, expanding the cluster to encompass any additional hosts, and
then deploying the needed services.

.. highlight:: console


.. _cephadm-host-requirements:

Requirements
============

- Python 3
- Systemd
- Podman or Docker for running containers
- Time synchronization (such as chrony or NTP)
- LVM2 for provisioning storage devices

Any modern Linux distribution should be sufficient.  Dependencies
are installed automatically by the bootstrap process below.

See the section :ref:`Compatibility With Podman
Versions<cephadm-compatibility-with-podman>` for a table of Ceph versions that
are compatible with Podman. Not every version of Podman is compatible with
Ceph.



.. _get-cephadm:

Install cephadm
===============

The ``cephadm`` command can 

#. bootstrap a new cluster
#. launch a containerized shell with a working Ceph CLI
#. aid in debugging containerized Ceph daemons

There are two ways to install ``cephadm``:

#. a :ref:`curl-based installation<cephadm_install_curl>` method
#. :ref:`distribution-specific installation methods<cephadm_install_distros>`

.. _cephadm_install_curl:

curl-based installation
-----------------------

* Use ``curl`` to fetch the most recent version of the
  standalone script. 
  
  .. prompt:: bash #
     :substitutions:

     curl --silent --remote-name --location https://github.com/ceph/ceph/raw/|stable-release|/src/cephadm/cephadm

  Make the ``cephadm`` script executable:

  .. prompt:: bash #

   chmod +x cephadm

  This script can be run directly from the current directory:

  .. prompt:: bash #

   ./cephadm <arguments...>

* Although the standalone script is sufficient to get a cluster started, it is
  convenient to have the ``cephadm`` command installed on the host.  To install
  the packages that provide the ``cephadm`` command, run the following
  commands:

  .. prompt:: bash #
     :substitutions:

     ./cephadm add-repo --release |stable-release|
     ./cephadm install

  Confirm that ``cephadm`` is now in your PATH by running ``which``:

  .. prompt:: bash #

    which cephadm

  A successful ``which cephadm`` command will return this:

  .. code-block:: bash

    /usr/sbin/cephadm

.. _cephadm_install_distros:

distribution-specific installations
-----------------------------------

.. important:: The methods of installing ``cephadm`` in this section are distinct from the curl-based method above. Use either the curl-based method above or one of the methods in this section, but not both the curl-based method and one of these.

Some Linux distributions  may already include up-to-date Ceph packages.  In
that case, you can install cephadm directly. For example:

  In Ubuntu:

  .. prompt:: bash #

     apt install -y cephadm

  In Fedora:

  .. prompt:: bash #

     dnf -y install cephadm

  In SUSE:

  .. prompt:: bash #

     zypper install -y cephadm



Bootstrap a new cluster
=======================

What to know before you bootstrap
---------------------------------

The first step in creating a new Ceph cluster is running the ``cephadm
bootstrap`` command on the Ceph cluster's first host. The act of running the
``cephadm bootstrap`` command on the Ceph cluster's first host creates the Ceph
cluster's first "monitor daemon", and that monitor daemon needs an IP address.
You must pass the IP address of the Ceph cluster's first host to the ``ceph
bootstrap`` command, so you'll need to know the IP address of that host.

.. note:: If there are multiple networks and interfaces, be sure to choose one
   that will be accessible by any host accessing the Ceph cluster.

Running the bootstrap command
-----------------------------

Run the ``ceph bootstrap`` command:

.. prompt:: bash # 

   cephadm bootstrap --mon-ip *<mon-ip>*

This command will:

* Create a monitor and manager daemon for the new cluster on the local
  host.
* Generate a new SSH key for the Ceph cluster and add it to the root
  user's ``/root/.ssh/authorized_keys`` file.
* Write a copy of the public key to ``/etc/ceph/ceph.pub``.
* Write a minimal configuration file to ``/etc/ceph/ceph.conf``. This
  file is needed to communicate with the new cluster.
* Write a copy of the ``client.admin`` administrative (privileged!)
  secret key to ``/etc/ceph/ceph.client.admin.keyring``.
* Add the ``_admin`` label to the bootstrap host.  By default, any host
  with this label will (also) get a copy of ``/etc/ceph/ceph.conf`` and
  ``/etc/ceph/ceph.client.admin.keyring``.

Further information about cephadm bootstrap 
-------------------------------------------

The default bootstrap behavior will work for most users. But if you'd like
immediately to know more about ``cephadm bootstrap``, read the list below.  

Also, you can run ``cephadm bootstrap -h`` to see all of ``cephadm``'s
available options.

* Larger Ceph clusters perform better when (external to the Ceph cluster)
  public network traffic is separated from (internal to the Ceph cluster)
  cluster traffic. The internal cluster traffic handles replication, recovery,
  and heartbeats between OSD daemons.  You can define the :ref:`cluster
  network<cluster-network>` by supplying the ``--cluster-network`` option to the ``bootstrap``
  subcommand. This parameter must define a subnet in CIDR notation (for example
  ``10.90.90.0/24`` or ``fe80::/64``).

* ``cephadm bootstrap`` writes to ``/etc/ceph`` the files needed to access
  the new cluster. This central location makes it possible for Ceph
  packages installed on the host (e.g., packages that give access to the
  cephadm command line interface) to find these files.

  Daemon containers deployed with cephadm, however, do not need
  ``/etc/ceph`` at all.  Use the ``--output-dir *<directory>*`` option
  to put them in a different directory (for example, ``.``). This may help
  avoid conflicts with an existing Ceph configuration (cephadm or
  otherwise) on the same host.

* You can pass any initial Ceph configuration options to the new
  cluster by putting them in a standard ini-style configuration file
  and using the ``--config *<config-file>*`` option.  For example::

      $ cat <<EOF > initial-ceph.conf
      [global]
      osd crush chooseleaf type = 0
      EOF
      $ ./cephadm bootstrap --config initial-ceph.conf ...

* The ``--ssh-user *<user>*`` option makes it possible to choose which ssh
  user cephadm will use to connect to hosts. The associated ssh key will be
  added to ``/home/*<user>*/.ssh/authorized_keys``. The user that you 
  designate with this option must have passwordless sudo access.

* If you are using a container on an authenticated registry that requires
  login, you may add the three arguments:
 
  #. ``--registry-url <url of registry>``

  #. ``--registry-username <username of account on registry>``

  #. ``--registry-password <password of account on registry>`` 

  OR

  * ``--registry-json <json file with login info>`` 
  
  Cephadm will attempt to log in to this registry so it can pull your container
  and then store the login info in its config database. Other hosts added to
  the cluster will then also be able to make use of the authenticated registry.

.. _cephadm-enable-cli:

Enable Ceph CLI
===============

Cephadm does not require any Ceph packages to be installed on the
host.  However, we recommend enabling easy access to the ``ceph``
command.  There are several ways to do this:

* The ``cephadm shell`` command launches a bash shell in a container
  with all of the Ceph packages installed. By default, if
  configuration and keyring files are found in ``/etc/ceph`` on the
  host, they are passed into the container environment so that the
  shell is fully functional. Note that when executed on a MON host,
  ``cephadm shell`` will infer the ``config`` from the MON container
  instead of using the default configuration. If ``--mount <path>``
  is given, then the host ``<path>`` (file or directory) will appear
  under ``/mnt`` inside the container:

  .. prompt:: bash #

     cephadm shell

* To execute ``ceph`` commands, you can also run commands like this:

  .. prompt:: bash #

     cephadm shell -- ceph -s

* You can install the ``ceph-common`` package, which contains all of the
  ceph commands, including ``ceph``, ``rbd``, ``mount.ceph`` (for mounting
  CephFS file systems), etc.:

  .. prompt:: bash #
     :substitutions:

     cephadm add-repo --release |stable-release|
     cephadm install ceph-common

Confirm that the ``ceph`` command is accessible with:

.. prompt:: bash #
 
  ceph -v


Confirm that the ``ceph`` command can connect to the cluster and also
its status with:

.. prompt:: bash #

  ceph status

Adding Hosts
============

Next, add all hosts to the cluster by following :ref:`cephadm-adding-hosts`.

By default, a ``ceph.conf`` file and a copy of the ``client.admin`` keyring
are maintained in ``/etc/ceph`` on all hosts with the ``_admin`` label, which is initially
applied only to the bootstrap host. We usually recommend that one or more other hosts be
given the ``_admin`` label so that the Ceph CLI (e.g., via ``cephadm shell``) is easily
accessible on multiple hosts.  To add the ``_admin`` label to additional host(s),

  .. prompt:: bash #

    ceph orch host label add *<host>* _admin

Adding additional MONs
======================

A typical Ceph cluster has three or five monitor daemons spread
across different hosts.  We recommend deploying five
monitors if there are five or more nodes in your cluster.

Please follow :ref:`deploy_additional_monitors` to deploy additional MONs.

Adding Storage
==============

To add storage to the cluster, either tell Ceph to consume any
available and unused device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

Or See :ref:`cephadm-deploy-osds` for more detailed instructions.

Using Ceph
==========

To use the *Ceph Filesystem*, follow :ref:`orchestrator-cli-cephfs`.

To use the *Ceph Object Gateway*, follow :ref:`cephadm-deploy-rgw`.

To use *NFS*, follow :ref:`deploy-cephadm-nfs-ganesha`

To use *iSCSI*, follow :ref:`cephadm-iscsi`


.. _cluster network: ../rados/configuration/network-config-ref#cluster-network
