.. _cephadm_deploying_new_cluster:

============================
Deploying a new Ceph cluster
============================

Cephadm creates a new Ceph cluster by bootstrapping a single
host, expanding the cluster to encompass any additional hosts, and
then deploying the needed services.

.. highlight:: console

.. _cephadm-host-requirements:

Requirements
============

- Python 3
- Systemd
- Podman or Docker for running containers
- Time synchronization (such as Chrony or the legacy ``ntpd``)
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

.. note:: 
   Recent versions of cephadm are distributed as an executable compiled from
   source code.  Unlike for earlier versions of Ceph it is no longer sufficient
   to copy a single script from Ceph's git tree and run it. If you wish to run
   cephadm using a development version you should create your own build of
   cephadm. See the `compiling-cephadm` in the documentation for newer releases for details on how to create your own
   standalone cephadm executable.

.. _cephadm_install_distros:

distribution-specific installations
-----------------------------------

.. important:: The methods of installing ``cephadm`` in this section are distinct from the curl-based method above. Use either the curl-based method above or one of the methods in this section, but not both the curl-based method and one of these.

Some Linux distributions  may already include up-to-date Ceph packages.  In
that case, you can install cephadm directly. For example:

  In Ubuntu:

  .. prompt:: bash #

     apt install -y cephadm

  In CentOS Stream:

  .. prompt:: bash #
     :substitutions:

     dnf search release-ceph
     dnf install --assumeyes centos-release-ceph-|stable-release|
     dnf install --assumeyes cephadm

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
cluster's first Monitor daemon.
You must pass the IP address of the Ceph cluster's first host to the ``ceph
bootstrap`` command, so you'll need to know the IP address of that host.

.. important:: ``ssh`` must be installed and running in order for the
   bootstrapping procedure to succeed.

.. note:: If there are multiple networks and interfaces, be sure to choose one
   that will be accessible by any host accessing the Ceph cluster.

.. important:: When installing the Quincy release of Ceph, you might see the
   following error message:

   .. code-block:: console 
   
      Deploying ceph-exporter service with default
      placement...  Non-zero exit code 22

   If you see this error message, add the ``--skip-monitoring-stack`` flag to
   your ``cephadm bootstrap`` command. To do this, run a command of the
   following form:

   .. prompt:: bash $

      cephadm bootstrap --mon-ip {monitor IP address} --skip-monitoring-stack

   This command should result in a successfully-deployed cluster. Expand the
   successfully deployed cluster by deploying the monitoring stack without
   ``ceph-exporter`` by running the following commands:

   .. prompt:: bash $

      ceph orch apply prometheus
      ceph orch apply grafana
      ceph orch apply node-exporter
      ceph orch apply alertmanager

   This error occurs because some Quincy releases contain a faulty
   ``ceph-exporter``. After this fault was discovered, ``ceph-exporter`` was
   removed from Quincy. If your binary of ``cephadm`` is from the release with
   the fault, you will hit this error.


Running the bootstrap command
-----------------------------

Run the ``ceph bootstrap`` command:

.. prompt:: bash #

   cephadm bootstrap --mon-ip *<mon-ip>*

This command will:

* Create a Monitor and a Manager daemon for the new cluster on the local
  host.
* Generate a new SSH key for the Ceph cluster and add it to the root
  user's ``/root/.ssh/authorized_keys`` file.
* Write a copy of the public key to ``/etc/ceph/ceph.pub``.
* Write a minimal configuration file to ``/etc/ceph/ceph.conf``. This
  file is needed to communicate with Ceph daemons.
* Write a copy of the ``client.admin`` administrative (privileged!)
  secret key to ``/etc/ceph/ceph.client.admin.keyring``.
* Add the ``_admin`` label to the bootstrap host.  By default, any host
  with this label will (also) get a copy of ``/etc/ceph/ceph.conf`` and
  ``/etc/ceph/ceph.client.admin.keyring``.

.. _cephadm-bootstrap-further-info:

Further information about cephadm bootstrap
-------------------------------------------

The default bootstrap process will work for most users. But if you'd like
immediately to know more about ``cephadm bootstrap``, read the list below.

Also, you can run ``cephadm bootstrap -h`` to see all of ``cephadm``'s
available options.

* By default, Ceph daemons send their log output to stdout/stderr, which is picked
  up by the container runtime (docker or podman) and (on most systems) sent to
  journald.  If you want Ceph to write traditional log files to ``/var/log/ceph/$fsid``,
  use the ``--log-to-file`` option during bootstrap.

* Larger Ceph clusters perform best when (external to the Ceph cluster)
  public network traffic is separated from (internal to the Ceph cluster)
  cluster traffic. The internal cluster traffic handles replication, recovery,
  and heartbeats between OSD daemons.  You can define the :ref:`cluster
  network<cluster-network>` by supplying the ``--cluster-network`` option to the ``bootstrap``
  subcommand. This parameter must be a subnet in CIDR notation (for example
  ``10.90.90.0/24`` or ``fe80::/64``).

* ``cephadm bootstrap`` writes to ``/etc/ceph`` files needed to access
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

* The ``--ssh-user *<user>*`` option makes it possible to designate which SSH
  user cephadm will use to connect to hosts. The associated SSH key will be
  added to ``/home/*<user>*/.ssh/authorized_keys``. The user that you
  designate with this option must have passwordless sudo access.

* If you are using a container image from a registry that requires
  login, you may add the argument:

  * ``--registry-json <path to json file>``

  example contents of JSON file with login info::

      {"url":"REGISTRY_URL", "username":"REGISTRY_USERNAME", "password":"REGISTRY_PASSWORD"}

  Cephadm will attempt to log in to this registry so it can pull your container
  and then store the login info in its config database. Other hosts added to
  the cluster will then also be able to make use of the authenticated container registry.

* See :ref:`cephadm-deployment-scenarios` for additional examples for using ``cephadm bootstrap``.

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

Add all hosts to the cluster by following the instructions in
:ref:`cephadm-adding-hosts`.

By default, a ``ceph.conf`` file and a copy of the ``client.admin`` keyring are
maintained in ``/etc/ceph`` on all hosts that have the ``_admin`` label. This
label is initially applied only to the bootstrap host. We recommend
that one or more other hosts be given the ``_admin`` label so that the Ceph CLI
(for example, via ``cephadm shell``) is easily accessible on multiple hosts. To add
the ``_admin`` label to additional host(s), run a command of the following form:

  .. prompt:: bash #

    ceph orch host label add *<host>* _admin


Adding additional MONs
======================

A typical Ceph cluster has three or five Monitor daemons spread
across different hosts.  We recommend deploying five
Monitors if there are five or more nodes in your cluster. Most clusters do not
benefit from seven or more Monitors.

Please follow :ref:`deploy_additional_monitors` to deploy additional MONs.

Adding Storage
==============

To add storage to the cluster, you can tell Ceph to consume any
available and unused device(s):

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

See :ref:`cephadm-deploy-osds` for more detailed instructions.

Enabling OSD memory autotuning
------------------------------

.. warning:: By default, cephadm enables ``osd_memory_target_autotune`` on bootstrap, with ``mgr/cephadm/autotune_memory_target_ratio`` set to ``.7`` of total host memory.

See :ref:`osd_autotune`.

To deploy hyperconverged Ceph with TripleO, please refer to the TripleO documentation: `Scenario: Deploy Hyperconverged Ceph <https://docs.openstack.org/project-deploy-guide/tripleo-docs/latest/features/cephadm.html#scenario-deploy-hyperconverged-ceph>`_

In other cases where the cluster hardware is not exclusively used by Ceph (converged infrastructure),
reduce the memory consumption of Ceph like so:

  .. prompt:: bash #

    # converged only:
    ceph config set mgr mgr/cephadm/autotune_memory_target_ratio 0.2

Then enable memory autotuning:

  .. prompt:: bash #

    ceph config set osd osd_memory_target_autotune true


Using Ceph
==========

To use the *Ceph Filesystem*, follow :ref:`orchestrator-cli-cephfs`.

To use the *Ceph Object Gateway*, follow :ref:`cephadm-deploy-rgw`.

To use *NFS*, follow :ref:`deploy-cephadm-nfs-ganesha`

To use *iSCSI*, follow :ref:`cephadm-iscsi`

.. _cephadm-deployment-scenarios:

Different deployment scenarios
==============================

Single host
-----------

To deploy a Ceph cluster running on a single host, use the
``--single-host-defaults`` flag when bootstrapping. For use cases, see
:ref:`one-node-cluster`. Such clusters are generally not suitable for
production.


The ``--single-host-defaults`` flag sets the following configuration options::

  global/osd_crush_chooseleaf_type = 0
  global/osd_pool_default_size = 2
  mgr/mgr_standby_modules = False

For more information on these options, see :ref:`one-node-cluster` and
``mgr_standby_modules`` in :ref:`mgr-administrator-guide`.

.. _cephadm-airgap:

Deployment in an isolated environment
-------------------------------------

You might need to install cephadm in an environment that is not connected
directly to the Internet (an "isolated" or "airgapped"
environment). This requires the use of a custom container registry. Either
of two kinds of custom container registry can be used in this scenario: (1) a
Podman-based or Docker-based insecure registry, or (2) a secure registry.

The practice of installing software on systems that are not connected directly
to the internet is called "airgapping" and registries that are not connected
directly to the internet are referred to as "airgapped".

Make sure that your container image is inside the registry. Make sure that you
have access to all hosts that you plan to add to the cluster.

#. Run a local container registry:

   .. prompt:: bash #

      podman run --privileged -d --name registry -p 5000:5000 -v /var/lib/registry:/var/lib/registry --restart=always registry:2

#. If you are using an insecure registry, configure Podman or Docker with the
   hostname and port where the registry is running.

   .. note:: You must repeat this step for every host that accesses the local
             insecure registry.

#. Push your container image to your local registry. Here are some acceptable
   kinds of container images:

   * Ceph container image. See :ref:`containers`.
   * Prometheus container image
   * Node exporter container image
   * Grafana container image
   * Alertmanager container image

#. Create a temporary configuration file to store the names of the monitoring
   images. (See :ref:`cephadm_monitoring-images`):

   .. prompt:: bash $

      cat <<EOF > initial-ceph.conf

   ::

      [mgr]
      mgr/cephadm/container_image_prometheus *<hostname>*:5000/prometheus
      mgr/cephadm/container_image_node_exporter *<hostname>*:5000/node_exporter
      mgr/cephadm/container_image_grafana *<hostname>*:5000/grafana
      mgr/cephadm/container_image_alertmanager *<hostname>*:5000/alertmanger

#. Run bootstrap using the ``--image`` flag and pass the name of your
   container image as the argument of the image flag. For example:

   .. prompt:: bash #

      cephadm --image *<hostname>*:5000/ceph/ceph bootstrap --mon-ip *<mon-ip>*

.. _cluster network: ../rados/configuration/network-config-ref#cluster-network

.. _cephadm-bootstrap-custom-ssh-keys:

Deployment with custom SSH keys
-------------------------------

Bootstrap allows users to create their own private/public SSH key pair
rather than having cephadm generate them automatically.

To use custom SSH keys, pass the ``--ssh-private-key`` and ``--ssh-public-key``
fields to bootstrap. Both parameters require a path to the file where the
keys are stored:

.. prompt:: bash #

  cephadm bootstrap --mon-ip <ip-addr> --ssh-private-key <private-key-filepath> --ssh-public-key <public-key-filepath>

This setup allows users to use a key that has already been distributed to hosts
the user wants in the cluster before bootstrap.

.. note:: In order for cephadm to connect to other hosts you'd like to add
   to the cluster, make sure the public key of the key pair provided is set up
   as an authorized key for the ssh user being used, typically root. If you'd
   like more info on using a non-root user as the ssh user, see :ref:`cephadm-bootstrap-further-info`

.. _cephadm-bootstrap-ca-signed-keys:

Deployment with CA signed SSH keys
----------------------------------

As an alternative to standard public key authentication, cephadm also supports
deployment using CA signed keys. Before bootstrapping it's recommended to set up
the CA public key as a trusted CA key on hosts you'd like to eventually add to
the cluster. For example:

.. prompt:: bash

  # we will act as our own CA, therefore we'll need to make a CA key
  [root@host1 ~]# ssh-keygen -t rsa -f ca-key -N ""

  # make the ca key trusted on the host we've generated it on
  # this requires adding in a line in our /etc/sshd_config
  # to mark this key as trusted
  [root@host1 ~]# cp ca-key.pub /etc/ssh
  [root@host1 ~]# vi /etc/ssh/sshd_config
  [root@host1 ~]# cat /etc/ssh/sshd_config | grep ca-key
  TrustedUserCAKeys /etc/ssh/ca-key.pub
  # now restart sshd so it picks up the config change
  [root@host1 ~]# systemctl restart sshd

  # now, on all other hosts we want in the cluster, also install the CA key
  [root@host1 ~]# scp /etc/ssh/ca-key.pub host2:/etc/ssh/

  # on other hosts, make the same changes to the sshd_config
  [root@host2 ~]# vi /etc/ssh/sshd_config
  [root@host2 ~]# cat /etc/ssh/sshd_config | grep ca-key
  TrustedUserCAKeys /etc/ssh/ca-key.pub
  # and restart sshd so it picks up the config change
  [root@host2 ~]# systemctl restart sshd

Once the CA key has been installed and marked as a trusted key, you are ready
to use a private key/CA signed cert combination for SSH. Continuing with our
current example, we will create a new key-pair for for host access and then
sign it with our CA key

.. prompt:: bash

  # make a new key pair
  [root@host1 ~]# ssh-keygen -t rsa -f cephadm-ssh-key -N ""
  # sign the private key. This will create a new cephadm-ssh-key-cert.pub
  # note here we're using user "root". If you'd like to use a non-root
  # user the arguments to the -I and -n params would need to be adjusted
  # Additionally, note the -V param indicates how long until the cert
  # this creates will expire
  [root@host1 ~]# ssh-keygen -s ca-key -I user_root -n root -V +52w cephadm-ssh-key
  [root@host1 ~]# ls
  ca-key  ca-key.pub  cephadm-ssh-key  cephadm-ssh-key-cert.pub  cephadm-ssh-key.pub

  # verify our signed key is working. To do this, make sure the generated private
  # key ("cephadm-ssh-key" in our example) and the newly signed cert are stored
  # in the same directory. Then try to ssh using the private key
  [root@host1 ~]# ssh -i cephadm-ssh-key host2

Once you have your private key and corresponding CA signed cert and have tested
SSH authentication using that key works, you can pass those keys to bootstrap
in order to have cephadm use them for SSHing between cluster hosts

.. prompt:: bash

  [root@host1 ~]# cephadm bootstrap --mon-ip <ip-addr> --ssh-private-key cephadm-ssh-key --ssh-signed-cert cephadm-ssh-key-cert.pub

Note that this setup does not require installing the corresponding public key
from the private key passed to bootstrap on other nodes. In fact, cephadm will
reject the ``--ssh-public-key`` argument when passed along with ``--ssh-signed-cert``.
This is not because having the public key breaks anything, but rather because it is not at all needed
and helps the bootstrap command differentiate if the user wants the CA signed
keys setup or standard pubkey encryption. What this means is that SSH key rotation
would simply be a matter of getting another key signed by the same CA and providing
cephadm with the new private key and signed cert. No additional distribution of
keys to cluster nodes is needed after the initial setup of the CA key as a trusted key,
no matter how many new private key/signed cert pairs are rotated in.
