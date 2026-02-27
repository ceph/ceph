.. _fscrypt:

Fscrypt Encryption on CephFS
============================

Fscrypt is an encryption implementation at the file system level. This file
system encryption allows for encrypting on a per-directory level. This allows
for file systems to have encrypted and regular non-encrypted portions. Fscrypt
encryption encrypts file names and data contents.

How Fscrypt Encryption Works
----------------------------

Encryption Keys
~~~~~~~~~~~~~~~

Each fscrypt tree has a master encryption key. This master key will provide the
"secret" that is needed to encrypt directories. This key can be up to 256 bits
in length.

Policies
~~~~~~~~

An fscrypt root is assigned to an encryption policy. This policy contains items such
as which encryption cipher to use and a master key id. This tells the client how
to encrypt/decrypt and to validate a given master key id to the encrypted inode.

Encryption happens completely on the client side. The MDS and OSD are not aware
of encryption policies or master keys. There has been minimal change to those
server components. They continue for the most part to store file names and data
contents (which in this case happen to be encrypted).

Access Semantics
~~~~~~~~~~~~~~~~

There are semantics that allow different access depending on if the client has
the master key present for a directory or not.

With the key

* You can access the filesystem as you normally would.
* You can see filenames, data contents and link targets.

Without the key

* You do not see plaintext file names or link targets.
* You cannot open a file.
* You cannot access data contents in any form, not even the encrypted versions.
* You cannot truncate a file.
* You will see other metadata such as times, mode, ownership, and extended attributes.

.. note::
   You cannot backup or restore without the key.

Learning by Example
~~~~~~~~~~~~~~~~~~~

Consider a filesystem named ``cephfs``. A client has two master keys and two
directories (``encdir1`` and ``encdira``). Each directory can have a different encryption
master key. For example, ``encdir1`` can have ``key1`` and then ``encdira`` can use ``keyb``.
Then a regular directory (``regdir``) will also be present. Please note that ``regdir`` is an
unencrypted directory and shown for multi-tenant purposes. Figure 1 below
illustrates this.

When a policy is set on the directory, the directory must be empty. Then any subsequent
directories, files or links created in the subtree will inherit policy information
from its parent directory.

.. figure:: cephfs_fscrypt_overview.svg

   Figure 1

Key Management
--------------

Each client has a unique view of the filesystem and the fscrypt tree. For this
example please refer to Figure 2 below. There are three clients, the first two
have a newer version of the CephFS client that includes the fscrypt feature
and the third does not. The key management of the keys are on a per-client basis.
What one client does pertaining to fscrypt the other is not aware of. Let's take
a closer look to see the nuances in detail.

The first client, Client 1, has the master key present and is able to view the
encrypted tree transparently, this is the unlocked mode.

The second, Client 2, does not have the master key and does not have full
functionality, this is the locked mode. During locked mode, users cannot view
plaintext filenames or data contents. When a user lists the directory contents, it
will see a hashed version of the encrypted file name. Then when an ``open()`` occurs,
an error will be returned and operation will be denied. Things such as file
sizes, mode, timestamps and other inode metadata will be stored plaintext and
are available in this mode.

Finally, Client 3, is using an older version of CephFS client and does not have
fscrypt feature present. In this mode, users have the same view as before, but
are able to do some data operations to encrypted files. This mode is not
recommend and not supported.

.. figure:: cephfs_fscrypt_multiclient.svg

   Figure 2

CephFS Support
--------------

There are two implementations of fscrypt within CephFS. It is supported in the
CephFS kernel client. This implementation extends capabilities that exist within
the kernel libraries and utilizes the kernel crypto keyring.

Secondly, the userspace client supports fscrypt within ``ceph-fuse`` and ``libcephfs``.
Both of these versions are meant to be interoperable, but with some limitations.

Userspace Limitations
---------------------

A custom fscrypt CLI will be needed to use userspace fscrypt. This is due to
permanent configurations in the kernel (which ``ceph-fuse`` utilizes) that are incorrectly
defined. Instead, there's a fscrypt command line utility that is maintained
as a part of the Ceph project. This version includes necessary changes to configure
and use fscrypt.

This version is available at: https://github.com/ceph/fscrypt/tree/wip-ceph-fuse

Currently a subset of fscrypt ciphers are supported in user space.
They are:

- AES-256-XTS for contents
- AES-256-CBC-CTS for filenames

Any other ciphers used during setting a policy on a folder will be rejected.

How to Use
----------

Setup system-wide encryption. This will initialize ``/etc/fscrypt.conf``

.. prompt:: bash #
 
       fscrypt setup

Setup mount-wide encryption. This has to be applied to the mount point for the
filesystem. This will setup internal fscrypt CLI config files for managing and
keeping track of encryption keys

.. prompt:: bash #

       fscrypt setup <mount pt>

To setup a dir to be encrypted (it must be empty)

.. prompt:: bash #

       fscrypt encrypt <dir>

To lock an encrypted dir

.. prompt:: bash #

       fscrypt lock <dir>

To unlock an encrypted dir

.. prompt:: bash #

       fscrypt unlock <dir>

To view status of a directory (it can be a regular or encrypted dir)

.. prompt:: bash #

       fscrypt status <dir>

Behavior of Master Key in Snapshots and Clones
----------------------------------------------

All snapshots and clones derived from an fscrypt directory will have their lock
state tied together. This means that all derived datasets will be locked or
unlocked at the same time.

For example, consider:

#. Encrypted ``encdir1`` is unlocked.
#. Snapshot of ``encdir1`` is created as ``encdir1_snap``.
#. Clone of snapshot ``encdir1_snap`` is created as ``encdir1_snap_clone1``.

In this current state, ``encdir1``, ``encdir1_snap`` and ``encdir1_snap_clone1`` are unlocked
and file names and data is accessible as expected in each state. If you perform a
lock on any of the three, all three will become locked.

.. note::

       Snapshot names are not encrypted.
