.. _ceph-volume-overview:

Overview
--------
The ``ceph-volume`` tool is a single purpose command line tool to deploy
logical volumes as OSDs. It maintains an API similar to that of the older
``ceph-disk`` tool when preparing, activating, and creating OSDs.

Unlike ``ceph-disk``, it does not interact with or rely on udev rules. Those
rules allowed automatic detection of previously set up devices, which were in
turn fed into ``ceph-disk`` to activate them.

Cephadm shell
-------------
Do not run ``ceph-volume`` from a container session that was started with
``cephadm shell`` while relying on that shell's default bind mounts. By design,
``cephadm shell`` omits several host bind mounts that ``ceph-volume`` expects
(for example /run/udev, /run/lvm, etc.). Invoking ``ceph-volume`` in that
environment is likely to fail or behave incorrectly.

Running ``ceph-volume`` yourself, outside of what ``ceph orch`` / cephadm
drives, is not the normal operational path: it is mainly for debugging,
testing, or development.

.. note:: Advanced use only

   If you truly understand the implications, you can extend the default container
   environment by passing ``cephadm shell`` a single ``-m`` (or ``--mount``)
   option followed by every bind mount you need, for example:

   .. code-block:: bash

      cephadm shell -m /dev:/dev /run/udev:/run/udev /sys:/sys /run/lvm:/run/lvm /run/lock/lvm:/run/lock/lvm /:/rootfs

   From **inside** that shell, if you still need the ``client.bootstrap-osd``
   keyring (``cephadm shell`` does not expose it by default), you can obtain it
   with the cluster tools available in the container, for example:

   .. code-block:: bash

      ceph auth get client.bootstrap-osd -o /var/lib/ceph/bootstrap-osd/ceph.keyring

   Prefer doing this inside the enriched shell rather than generating key
   material on the host and bind-mounting it in: the latter is easy to get
   wrong, can leave sensitive files behind on the host, and is generally more
   intrusive than running the same command from within the shell session.

.. _ceph-disk-replaced:

Replacing ``ceph-disk``
-----------------------
``ceph-disk`` was the original OSD provisioning tool. It relied on GPT
partitions and ``UDEV`` rules to label, discover, and activate devices. That
approach was slow and hard to debug, and because it was tied to GPT partitions
it could not work with technologies such as LVM. For these reasons
``ceph-disk`` was deprecated in Ceph 13.0.0 and has since been removed.

``ceph-volume`` replaces it with a modular design: OSDs that were originally
deployed with ``ceph-disk`` (plain disks with GPT partitions) are managed by
:ref:`ceph-volume-simple`, while new OSDs are provisioned with
:ref:`ceph-volume-lvm`.

``ceph-volume lvm``
-------------------
By making use of :term:`LVM tags`, the :ref:`ceph-volume-lvm` subcommand is
able to store and later re-discover and query devices associated with OSDs so
that they can later be activated.
