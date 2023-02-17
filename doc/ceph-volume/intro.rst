.. _ceph-volume-overview:

Overview
--------
The ``ceph-volume`` tool aims to be a single purpose command line tool to deploy
logical volumes as OSDs, trying to maintain a similar API to ``ceph-disk`` when
preparing, activating, and creating OSDs.

It deviates from ``ceph-disk`` by not interacting or relying on the udev rules
that come installed for Ceph. These rules allow automatic detection of
previously setup devices that are in turn fed into ``ceph-disk`` to activate
them.

.. _ceph-disk-replaced:

Replacing ``ceph-disk``
-----------------------
The ``ceph-disk`` tool was created at a time when the project was required to
support many different types of init systems (upstart, sysvinit, etc...) while
being able to discover devices. This caused the tool to concentrate initially
(and exclusively afterwards) on GPT partitions. Specifically on GPT GUIDs,
which were used to label devices in a unique way to answer questions like:

* is this device a Journal?
* an encrypted data partition?
* was the device left partially prepared?

To solve these, it used ``UDEV`` rules to match the GUIDs, that would call
``ceph-disk``, and end up in a back and forth between the ``ceph-disk`` systemd
unit and the ``ceph-disk`` executable. The process was very unreliable and time
consuming (a timeout of close to three hours **per OSD** had to be put in
place), and would cause OSDs to not come up at all during the boot process of
a node.

It was hard to debug, or even replicate these problems given the asynchronous
behavior of ``UDEV``.

Since the world-view of ``ceph-disk`` had to be GPT partitions exclusively, it meant
that it couldn't work with other technologies like LVM, or similar device
mapper devices. It was ultimately decided to create something modular, starting
with LVM support, and the ability to expand on other technologies as needed.


GPT partitions are simple?
--------------------------
Although partitions in general are simple to reason about, ``ceph-disk``
partitions were not simple by any means. It required a tremendous amount of
special flags in order to get them to work correctly with the device discovery
workflow. Here is an example call to create a data partition::

    /sbin/sgdisk --largest-new=1 --change-name=1:ceph data --partition-guid=1:f0fc39fd-eeb2-49f1-b922-a11939cf8a0f --typecode=1:89c57f98-2fe5-4dc0-89c1-f3ad0ceff2be --mbrtogpt -- /dev/sdb

Not only creating these was hard, but these partitions required devices to be
exclusively owned by Ceph. For example, in some cases a special partition would
be created when devices were encrypted, which would contain unencrypted keys.
This was ``ceph-disk`` domain knowledge, which would not translate to a "GPT
partitions are simple" understanding. Here is an example of that special
partition being created::

    /sbin/sgdisk --new=5:0:+10M --change-name=5:ceph lockbox --partition-guid=5:None --typecode=5:fb3aabf9-d25f-47cc-bf5e-721d181642be --mbrtogpt -- /dev/sdad


Modularity
----------
``ceph-volume`` was designed to be a modular tool because we anticipate that
there are going to be lots of ways that people provision the hardware devices
that we need to consider. There are already two: legacy ceph-disk devices that
are still in use and have GPT partitions (handled by :ref:`ceph-volume-simple`),
and lvm. SPDK devices where we manage NVMe devices directly from userspace are
on the immediate horizon, where LVM won't work there since the kernel isn't
involved at all.

``ceph-volume lvm``
-------------------
By making use of :term:`LVM tags`, the :ref:`ceph-volume-lvm` sub-command is
able to store and later re-discover and query devices associated with OSDs so
that they can later be activated.

LVM performance penalty
-----------------------
In short: we haven't been able to notice any significant performance penalties
associated with the change to LVM. By being able to work closely with LVM, the
ability to work with other device mapper technologies was a given: there is no
technical difficulty in working with anything that can sit below a Logical Volume.
