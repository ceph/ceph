.. _install-windows-basic-config:

========================================
 Configuring the Ceph Client on Windows
========================================

.. meta::
   :description: Create the ceph.conf file and copy the keyring that the Ceph client on Windows needs to connect to a cluster.
   :ceph-page-type: procedure

Create the configuration file and copy the :term:`keyring<Keyring>` that the
:term:`Ceph client <Ceph Client>` on Windows reads when it connects to a
cluster.

Prerequisites
=============

- The Ceph client installed on the host. See :ref:`install-windows`.
- The addresses of the cluster's :term:`Monitors <Ceph Monitor>`. On a
  cluster host, ``ceph config generate-minimal-conf`` prints them on its
  ``mon_host`` line.
- A keyring that holds the key of a :term:`CephX` user. To create a user for
  :term:`CephFS<Ceph File System>` and get its key, see
  :doc:`/cephfs/mount-prerequisites`. For an :term:`RBD` user, see "Create
  a Block Device User" in :doc:`/rbd/rados-rbd-cmds`.
- PowerShell opened with Run as administrator. The installer lets only
  administrators and the SYSTEM account use ``C:\ProgramData\ceph``.

Procedure
=========

.. warning::

   Back up ``C:\ProgramData\ceph\keyring`` and
   ``C:\ProgramData\ceph\ceph.conf`` first if they exist: steps 2 and 3
   replace them. With the sample configuration, every client user on the
   host reads its key from that one keyring file, so replacing it removes the
   keys of the other users.

#. Create the directory that the sample configuration uses for socket files
   and, if you enable it, the log file:

   .. prompt:: powershell

      New-Item -ItemType Directory -Force -Path C:\ProgramData\ceph\out

   PowerShell lists the ``out`` directory.

#. Copy the keyring of your CephX user to the path that the sample
   configuration sets:

   .. prompt:: powershell

      Copy-Item .\ceph.client.user1701.keyring C:\ProgramData\ceph\keyring

   The command prints nothing on success.

#. Open ``C:\ProgramData\ceph\ceph.conf`` in Notepad from the PowerShell
   window, so that Notepad can save to the folder:

   .. prompt:: powershell

      notepad C:\ProgramData\ceph\ceph.conf

   Notepad offers to create the file if it does not exist. Enter the
   following content, replace the ``mon host`` addresses with those of your
   Monitors, and save the file:

   .. code-block:: ini

      [global]
          mon host = 192.0.2.11,192.0.2.12,192.0.2.13

          log to stderr = true
          ; Uncomment the following in order to use the Windows Event Log
          ; log to syslog = true

          run dir = C:/ProgramData/ceph/out

          ; Use the following to change the cephfs client log level
          ; debug client = 2
      [client]
          keyring = C:/ProgramData/ceph/keyring
          ; log file = C:/ProgramData/ceph/out/$name.$pid.log
          admin socket = C:/ProgramData/ceph/out/$name.$pid.asok

          ; client_permissions = true
          ; client_mount_uid = 1000
          ; client_mount_gid = 1000

   Notepad saves the file without an error.

   ``%ProgramData%\ceph\ceph.conf`` is the default location of the file on
   Windows and usually expands to ``C:\ProgramData\ceph\ceph.conf``.

   Only ``mon host`` and ``keyring`` are needed to connect. The other
   settings are optional: ``log to stderr`` writes log messages to the
   console, ``run dir`` sets the directory for process ID and socket files,
   and ``admin socket`` sets the path of the socket that ``ceph daemon``
   commands use. For ``client_permissions``, ``client_mount_uid``, and
   ``client_mount_gid``, see :ref:`ceph-dokan`.

   Use forward slashes (``/``) as path separators in ``ceph.conf``: the
   parser reads a backslash as an escape character.

#. If you will map RBD images, restart the ``ceph-rbd`` service so that it
   reads the new configuration.

   .. warning::

      If RBD images are mapped on this host, stop the workloads that use
      them first. Restarting the service disconnects every mapped image and
      then maps only the persistent ones again. A disk that is still in use
      is disconnected by force, without waiting for pending writes: data
      that is not yet written is lost.

   .. prompt:: powershell

      Restart-Service ceph-rbd

   If no RBD images are mapped, the command prints nothing. Otherwise it can
   take several seconds to return: the service disconnects the mapped images,
   then maps the persistent ones again before it reports that it is running,
   and PowerShell can print "WARNING: Waiting for service 'Ceph RBD Mapping
   Service (ceph-rbd)' to start..." while it waits.

Verification
============

- Check that the client reads the Monitor addresses from the file:

  .. prompt:: powershell

     ceph-conf --lookup mon_host

  The output is the addresses from step 3, for example
  ``192.0.2.11,192.0.2.12,192.0.2.13``. The command reads the file only and
  does not contact the cluster.

- Check the keyring path that the client uses:

  .. prompt:: powershell

     ceph-conf --lookup keyring

  The output is ``C:/ProgramData/ceph/keyring``.

- Check that the keyring file is in place:

  .. prompt:: powershell

     Test-Path C:\ProgramData\ceph\keyring

  The output is ``True``.

- If you will map RBD images, check that the client reaches the cluster and
  authenticates. This example uses ``client.user1701``, whose keyring step 2
  copies, and a :term:`pool<Pools>` named ``rbd1701``:

  .. prompt:: powershell

     rbd ls --id user1701 rbd1701

  The output lists the images in the pool, one per line, or nothing if the
  pool has none. For a CephFS user, mounting the file system is the first
  contact with the cluster. See :ref:`ceph-dokan`.

Troubleshooting
===============

- **"Cannot find path ... because it does not exist."** ``Copy-Item`` in step
  2 looks for the keyring in the current directory, which in PowerShell
  opened with Run as administrator is often ``C:\Windows\system32``. Change
  to the folder that holds the keyring, for example with
  ``Set-Location $HOME\Downloads``, or give its full path, and run step 2
  again.
- **"did not load config file, using default settings."** The client did
  not find ``C:\ProgramData\ceph\ceph.conf``, or could not read it. Check
  the file name and location, and run the command in PowerShell opened with
  Run as administrator: other users cannot read the file.
- **"global_init: error reading config file."** The rest of the message
  gives the line and position of a syntax error in ``ceph.conf``. Compare the
  file with the sample in step 3. An error in line 1 at position 1 can mean
  that the file starts with a byte order mark (BOM): save it again as UTF-8
  without a BOM.
- **"no keyring found; disabled cephx authentication".** The client did not
  find a keyring at the path that ``ceph.conf`` sets. If
  ``ceph-conf --lookup keyring`` prints a path without separators, such as
  ``C:ProgramDatacephkeyring``, the ``keyring`` line uses backslashes: use
  forward slashes.
- **"rbd: couldn't connect to the cluster!" or "failed to fetch mon
  config".** The client cannot reach a Monitor, or the Monitor rejected its
  key. A command that cannot reach a Monitor waits up to five minutes before
  it fails. Check the ``mon host`` addresses, that the host can reach them on
  ports 3300 and 6789, and that the keyring holds the key of the user that
  the command runs as: ``client.admin`` unless you pass ``--id``.

Next Steps
==========

- Map RBD images as disks. See :doc:`/rbd/rbd-windows`.
- Mount CephFS file systems. See :ref:`ceph-dokan`.

Additional Resources
====================

- :ref:`install-windows-troubleshooting`
- :ref:`configuring-ceph`
- :ref:`ceph-metavariables`
- :ref:`msgr2_ceph_conf`
- :doc:`/cephfs/client-auth`
- :ref:`user-management`
