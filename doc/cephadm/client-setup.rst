.. _cephadm-client-setup:

===============================
 Setting Up a Ceph Client Host
===============================

.. meta::
   :description: Give a client host the minimal configuration file and the keyring that it needs to connect to a Ceph cluster.
   :ceph-page-type: procedure

A :term:`client <Ceph Client>` host needs two files to connect to a Ceph
cluster: a minimal configuration file and a :term:`keyring<Keyring>`. This
procedure creates both on a cluster host and copies them to the client host.
If :term:`cephadm` manages the client host, it can keep both files there for
you: see :ref:`client_keyrings_and_configs`.

Prerequisites
=============

- A running Ceph cluster, and a root shell on a cluster host that holds the
  configuration file and the ``client.admin`` keyring. With cephadm, by
  default, these are the hosts that have the ``_admin`` label, such as the
  bootstrap host (see :ref:`cephadm-special-host-labels`).
- The ``ceph`` command on that host. If it is not installed there, put
  ``cephadm shell --`` before each ``ceph`` command that you run on that
  host (see :ref:`cephadm-enable-cli`).
- The ``ceph-common`` package on the client host. Most client hosts need only
  this package and its dependencies. It supplies the ``ceph``, ``rados``,
  ``rbd``, and ``mount.ceph`` commands, among others. To get it, add the
  Ceph repository as described in :ref:`packages`, then install
  ``ceph-common`` with the package manager of the client host.
- Root access on the client host, which the examples call ``client1701``.
  The steps copy the files with ``scp`` over a root SSH login from the
  cluster host; you can copy them another way instead.
- For the example in step 2, the name of a :term:`CephFS<Ceph File System>`
  file system, to use in place of ``cephfs1701``. ``ceph fs ls`` lists the
  file systems of the cluster.

.. _config-file-setup:
.. _keyring-setup:

Procedure
=========

#. On the cluster host, write a minimal configuration file:

   .. prompt:: bash #

      ceph config generate-minimal-conf > /root/minimal-ceph.conf

   The command prints nothing. With ``cephadm shell --``, cephadm prints
   lines such as ``Inferring fsid`` on the terminal; they do not go into the
   file. The file holds a comment line with the cluster ID and a
   ``[global]`` section with an ``fsid`` line and a ``mon_host`` line, which
   tells the client how to reach the :term:`Monitors <Ceph Monitor>`. If the
   command fails, it prints an error and leaves the file empty: correct the
   error and run the command again before you go on.

#. Create a CephX user for the client, with the capabilities that the client
   needs, and write its keyring to a file. Choose a name that no user has
   yet: if the user exists, the command changes its capabilities. This
   example creates ``client.user1701`` with read and write access to the file
   system ``cephfs1701``:

   .. prompt:: bash #

      ceph fs authorize cephfs1701 client.user1701 / rw > /root/ceph.client.user1701.keyring

   The command prints nothing. The file holds a ``[client.user1701]`` section
   with a ``key`` line and three ``caps`` lines. If the command fails, it
   prints an error and leaves the file empty: correct the error and run the
   command again before you go on.

   Most clusters run with authentication (:term:`CephX`) enabled, so a client
   cannot connect without a key. To create a user for another kind of client,
   run ``ceph auth get-or-create`` with the capabilities that the client
   needs: see :ref:`user-management`.

#. On the cluster host, make the keyring readable by ``root`` only, because
   the key in it is a secret:

   .. prompt:: bash #

      chmod 600 /root/ceph.client.user1701.keyring

   The command prints nothing on success.

#. Copy the configuration file to the client host as
   ``/etc/ceph/ceph.conf``.

   .. warning:: This step and the next one replace ``/etc/ceph/ceph.conf``
      and ``/etc/ceph/ceph.client.user1701.keyring`` on the client host if
      they exist. If the client host already has an ``/etc/ceph/ceph.conf``,
      for example because it is a client of another cluster, copy that file
      to another place first.

   .. prompt:: bash #

      scp /root/minimal-ceph.conf root@client1701:/etc/ceph/ceph.conf

   ``scp`` may first ask you to accept the host key of ``client1701`` and
   for the password of ``root``. On success, it then prints at most a
   progress line, such as ``minimal-ceph.conf  100%``.

#. Copy the keyring to ``/etc/ceph`` on the client host, under the same name:

   .. prompt:: bash #

      scp -p /root/ceph.client.user1701.keyring root@client1701:/etc/ceph/

   On success, the command prints at most a progress line. The ``-p`` option
   keeps the file mode, so that only ``root`` can read the keyring on the
   client host too. A client looks for the key of ``client.user1701`` in
   ``/etc/ceph/ceph.client.user1701.keyring`` first, then in
   ``/etc/ceph/ceph.keyring``.

Verification
============

- On the client host, connect to the cluster as the new user:

  .. prompt:: bash #

     ceph -n client.user1701 -s

  The output starts with ``cluster:`` and an ``id:`` line that shows the
  cluster ID from the ``fsid`` line of ``/etc/ceph/ceph.conf``.

- Check who can read the keyring:

  .. prompt:: bash #

     ls -l /etc/ceph/ceph.client.user1701.keyring

  The line starts with ``-rw-------`` and shows ``root`` as the owner.

Troubleshooting
===============

- **"ceph: command not found".** The ``ceph`` command is not installed on
  the host. On the cluster host, put ``cephadm shell --`` before the command.
  On the client host, install the ``ceph-common`` package. Both are listed
  in `Prerequisites`_.
- **"filesystem cephfs1701 does not exist".** The cluster has no file system
  with the name from the example. In step 2, replace ``cephfs1701`` with a
  name that ``ceph fs ls`` prints, then run the command again.
- **"no update for caps of client.user1701", and the keyring file is
  empty.** Step 2 ran again for a user that already has these capabilities,
  and in that case it prints no keyring. Write the keyring with
  ``ceph auth get client.user1701 > /root/ceph.client.user1701.keyring``.
- **"Permission denied" from "scp".** Root cannot log in over SSH from the
  cluster host to the client host. Set up a root SSH login, as listed in
  `Prerequisites`_, or copy the two files another way.
- **"No such file or directory" or "Failure" from "scp", for a path under
  "/etc/ceph".** The client host has no ``/etc/ceph`` directory. Install
  the ``ceph-common`` package, which creates it, as listed in
  `Prerequisites`_.
- **"error calling conf_read_file".** The ``ceph`` command found no
  configuration file that it can read. Check that ``/etc/ceph/ceph.conf``
  exists on the client host and that the user who runs the command can read
  it.
- **"cannot identify monitors to contact", then "RADOS object not found
  (error connecting to the cluster)".** ``/etc/ceph/ceph.conf`` on the
  client host has no usable ``mon_host`` line. If the file is empty, step 1
  failed: run step 1 again, then copy the file again.
- **"no keyring found; disabled cephx authentication", then "RADOS
  permission denied (error connecting to the cluster)".** The command found
  no keyring that it can read. Without ``-n``, it runs as ``client.admin``
  and looks for ``/etc/ceph/ceph.client.admin.keyring``: add
  ``-n client.user1701``. Otherwise check the name of the keyring file, and
  run the command as ``root``, because only ``root`` can read the file. If
  you keep the two files outside ``/etc/ceph``, pass both paths, with
  ``--conf`` and ``--keyring``: the minimal configuration file does not name
  a keyring. Older releases also print "unable to find a keyring on" and the
  paths that the command tried.
- **"RADOS permission denied (error connecting to the cluster)", with no
  "no keyring found" line before it.** The Monitors rejected the key. Either
  the keyring does not hold the key that the cluster holds for the user, or
  the user has no Monitor capability, as happens when a user is created with
  ``ceph auth get-or-create`` and no capabilities. On the cluster host,
  compare the keyring with the output of ``ceph auth get client.user1701``.
- **The command prints nothing for five minutes, then "RADOS timed out
  (error connecting to the cluster)".** The client cannot reach a Monitor.
  Check that the client host can reach the addresses on the ``mon_host``
  line of ``/etc/ceph/ceph.conf`` on ports 3300 and 6789.

Next Steps
==========

- Mount the file system on the client host. See
  :doc:`/cephfs/mount-using-kernel-driver`.
- Give another client host the same two files: repeat steps 4 and 5 only.
- Have cephadm write ``ceph.conf`` to the hosts that have a label such as
  ``bare_config``. See :ref:`ceph_conf_distribution_label`.

Additional Resources
====================

- :doc:`/cephfs/client-auth`
- :doc:`/cephfs/mount-prerequisites`
- :ref:`configuring-ceph`
