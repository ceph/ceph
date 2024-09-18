.. _ceph-volume-lvm-encryption:

Encryption
==========

Logical volumes can be encrypted using ``dmcrypt`` by specifying the
``--dmcrypt`` flag when creating OSDs. When using LVM, logical volumes can be
encrypted in different ways. ``ceph-volume`` does not offer as many options as
LVM does, but it encrypts logical volumes in a way that  is consistent and
robust.

In this case, ``ceph-volume lvm`` follows this constraint:

* Non-LVM devices (such as partitions) are encrypted with the same OSD key.


LUKS
----
There are currently two versions of LUKS, 1 and 2. Version 2 is a bit easier to
implement but not widely available in all Linux distributions supported by
Ceph. 

.. note:: Version 1 of LUKS is referred to in this documentation as "LUKS".
   Version 2 is of LUKS is referred to in this documentation as "LUKS2".


LUKS on LVM
-----------
Encryption is done on top of existing logical volumes (this is not the same as
encrypting the physical device). Any single logical volume can be encrypted,
leaving other volumes unencrypted. This method also allows for flexible logical
volume setups, since encryption will happen once the LV is created.


Workflow
--------
When setting up the OSD, a secret key is created. That secret key is passed
to the monitor in JSON format as ``stdin`` to prevent the key from being
captured in the logs.

The JSON payload looks something like this::

        {
            "cephx_secret": CEPHX_SECRET,
            "dmcrypt_key": DMCRYPT_KEY,
            "cephx_lockbox_secret": LOCKBOX_SECRET,
        }

The naming convention for the keys is **strict**, and they are named like that
for the hardcoded (legacy) names used by ceph-disk.

* ``cephx_secret`` : The cephx key used to authenticate
* ``dmcrypt_key`` : The secret (or private) key to unlock encrypted devices
* ``cephx_lockbox_secret`` : The authentication key used to retrieve the
  ``dmcrypt_key``. It is named *lockbox* because ceph-disk used to have an
  unencrypted partition named after it, which was used to store public keys and
  other OSD metadata.

The naming convention is strict because Monitors supported the naming
convention of ceph-disk, which used these key names. In order to maintain 
compatibility and prevent ceph-disk from breaking, ceph-volume uses the same
naming convention *although it does not make sense for the new encryption
workflow*.

After the common steps of setting up the OSD during the "prepare stage" (
with :term:`bluestore`), the logical volume is left ready
to be activated, regardless of the state of the device (encrypted or
decrypted).

At the time of its activation, the logical volume is decrypted. The OSD starts
after the process completes correctly.

Summary of the encryption workflow for creating a new OSD
----------------------------------------------------------

#. OSD is created. Both lockbox and dmcrypt keys are created and sent to the
   monitors in JSON format, indicating an encrypted OSD.

#. All complementary devices (like journal, db, or wal) get created and
   encrypted with the same OSD key. Key is stored in the LVM metadata of the
   OSD.

#. Activation continues by ensuring devices are mounted, retrieving the dmcrypt
   secret key from the monitors, and decrypting before the OSD gets started.
