.. _ceph-volume-overview:

Overview
--------
The ``ceph-volume`` tool is a single-purpose command line tool to deploy
logical volumes as OSDs.

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


Modularity
----------
``ceph-volume`` was designed to be a modular tool because we anticipate that
there are going to be lots of ways that people provision the hardware devices
that we need to consider. There are already two: GPT partition-based devices
(handled by :ref:`ceph-volume-simple`) and LVM. SPDK devices where we manage NVMe devices directly from userspace are
on the immediate horizon, where LVM won't work there since the kernel isn't
involved at all.

``ceph-volume lvm``
-------------------
By making use of :term:`LVM tags`, the :ref:`ceph-volume-lvm` subcommand is
able to store and later re-discover and query devices associated with OSDs so
that they can later be activated.

LVM performance penalty
-----------------------
In short: we haven't been able to notice any significant performance penalties
associated with the change to LVM. By being able to work closely with LVM, the
ability to work with other device mapper technologies was a given: there is no
technical difficulty in working with anything that can sit below a Logical Volume.
