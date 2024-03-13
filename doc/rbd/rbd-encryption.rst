======================
 Image Encryption
======================

.. index:: Ceph Block Device; encryption

Starting with the Pacific release, image-level encryption can be handled
internally by RBD clients. This means you can set a secret key that will be
used to encrypt a specific RBD image. This page describes the scope of the
RBD encryption feature.

.. note::
   The ``krbd`` kernel module does not support encryption at this time.

.. note::
   External tools (e.g. dm-crypt, QEMU) can be used as well to encrypt
   an RBD image, and the feature set and limitation set for that use may be
   different than described here.

Encryption Format
=================

By default, RBD images are not encrypted. To encrypt an RBD image, it needs to
be formatted to one of the supported encryption formats. The format operation
persists encryption metadata to the image. The encryption metadata usually
includes information such as the encryption format and version, cipher
algorithm and mode specification, as well as information used to secure the
encryption key. The encryption key itself is protected by a user-kept secret
(usually a passphrase), which is never persisted. The basic encryption format
operation will require specifying the encryption format and a secret.

Some of the encryption metadata may be stored as part of the image data,
typically an encryption header will be written to the beginning of the raw
image data. This means that the effective image size of the encrypted image may
be lower than the raw image size. See the `Supported Formats`_ section for more
details.

.. note::
   Currently only flat images (i.e. not cloned) can be formatted.
   Clones of an encrypted image are inherently encrypted using the same format
   and secret.

.. note::
   Any data written to the image prior to its format may become unreadable,
   though it may still occupy storage resources.

.. note::
   Images with the `journal feature`_ enabled cannot be formatted and encrypted
   by RBD clients.

Encryption Load
=================

Formatting an image is a necessary pre-requisite for enabling encryption.
However, formatted images will still be treated as raw unencrypted images by
all of the RBD APIs. In particular, an encrypted RBD image can be opened
by the same APIs as any other image, and raw unencrypted data can be
read / written. Such raw IOs may risk the integrity of the encryption format,
for example by overriding encryption metadata located at the beginning of the
image.

In order to safely perform encrypted IO on the formatted image, an additional
*encryption load* operation should be applied after opening the image. The
encryption load operation requires supplying the encryption format and a secret
for unlocking the encryption key. Following a successful encryption load
operation, all IOs for the opened image will be encrypted / decrypted.
For a cloned image, this includes IOs for ancestor images as well. The
encryption key will be stored in-memory by the RBD client until the image is
closed.

.. note::
   Once encryption has been loaded, no other encryption load / format
   operations can be applied to the context of the opened image.

.. note::
   Once encryption has been loaded, API calls for retrieving the image size
   using the opened image context will return the effective image size.

.. note::
   Encryption load can be automatically applied when mounting RBD images as
   block devices via `rbd-nbd`_.

Supported Formats
=================

LUKS
~~~~~~~

Both LUKS1 and LUKS2 are supported. The data layout is fully compliant with the
LUKS specification. Thus, images formatted by RBD can be loaded using external
LUKS-supporting tools such as dm-crypt or QEMU. Furthermore, existing LUKS
data, created outside of RBD, can be imported (by copying the raw LUKS data
into the image) and loaded by RBD encryption.

.. note::
   The LUKS formats are supported on Linux-based systems only.

.. note::
   Currently, only AES-128 and AES-256 encryption algorithms are supported.
   Additionally, xts-plain64 is currently the only supported encryption mode.

To use the LUKS format, start by formatting the image::

    $ rbd encryption format {pool-name}/{image-name} {luks1|luks2} {passphrase-file} [–cipher-alg {aes-128 | aes-256}]

The encryption format operation generates a LUKS header and writes it to the
beginning of the image. The header is appended with a single keyslot holding a
randomly-generated encryption key, and is protected by the passphrase read from
`passphrase-file`.

.. note::
   If the content of `passphrase-file` ends with a newline character, it will
   be stripped off.

By default, AES-256 in xts-plain64 mode (which is the current recommended mode,
and the usual default for other tools) will be used. The format operation
allows selecting AES-128 as well. Adding / removing passphrases is currently
not supported by RBD, but can be applied to the raw RBD data using compatible
tools such as cryptsetup.

The LUKS header size can vary (up to 136MiB in LUKS2), but is usually up to
16MiB, depending on the version of `libcryptsetup` installed. For optimal
performance, the encryption format will set the data offset to be aligned with
the image object size. For example expect a minimum overhead of 8MiB if using
an imageconfigured with an 8MiB object size.

In LUKS1, sectors, which are the minimal encryption units, are fixed at 512
bytes. LUKS2 supports larger sectors, and for better performance we set
the default sector size to the maximum of 4KiB. Writes which are either smaller
than a sector, or are not aligned to a sector start, will trigger a guarded
read-modify-write chain on the client, with a considerable latency penalty.
A batch of such unaligned writes can lead to IO races which will further
deteriorate performance. Thus it is advisable to avoid using RBD encryption
in cases where incoming writes cannot be guaranteed to be sector-aligned.

To mount a LUKS-encrypted image run::

    $ rbd -p {pool-name} device map -t nbd -o encryption-format={luks1|luks2},encryption-passphrase-file={passphrase-file}

Note that for security reasons, both the encryption format and encryption load
operations are CPU-intensive, and may take a few seconds to complete. For the
encryption operations of actual image IO, assuming AES-NI is enabled,
a relative small microseconds latency should be added, as well as a small
increase in CPU utilization.

Examples
========

Create a LUKS2-formatted image with the effective size of 50GiB:

.. prompt:: bash $

    rbd create --size 50G mypool/myimage
    rbd encryption format mypool/myimage luks2 passphrase.bin
    rbd resize --size 50G --encryption-passphrase-file passphrase.bin mypool/myimage

``rbd resize`` command at the end grows the image to compensate for the
overhead associated with the LUKS2 header.

Given a LUKS2-formatted image, create a LUKS2-formatted clone with the
same effective size:

.. prompt:: bash $

    rbd snap create mypool/myimage@snap
    rbd snap protect mypool/myimage@snap
    rbd clone mypool/myimage@snap mypool/myclone
    rbd encryption format mypool/myclone luks2 clone-passphrase.bin

Given a LUKS2-formatted image with the effective size of 50GiB, create
a LUKS1-formatted clone with the same effective size:

.. prompt:: bash $

    rbd snap create mypool/myimage@snap
    rbd snap protect mypool/myimage@snap
    rbd clone mypool/myimage@snap mypool/myclone
    rbd encryption format mypool/myclone luks1 clone-passphrase.bin
    rbd resize --size 50G --allow-shrink --encryption-passphrase-file clone-passphrase.bin --encryption-passphrase-file passphrase.bin mypool/myclone

Since LUKS1 header is usually smaller than LUKS2 header, ``rbd resize``
command at the end shrinks the cloned image to get rid of unneeded
space allowance.

Given a LUKS1-formatted image with the effective size of 50GiB, create
a LUKS2-formatted clone with the same effective size:

.. prompt:: bash $

    rbd resize --size 51G mypool/myimage
    rbd snap create mypool/myimage@snap
    rbd snap protect mypool/myimage@snap
    rbd clone mypool/myimage@snap mypool/myclone
    rbd encryption format mypool/myclone luks2 clone-passphrase.bin
    rbd resize --size 50G --allow-shrink --encryption-passphrase-file passphrase.bin mypool/myimage
    rbd resize --size 50G --allow-shrink --encryption-passphrase-file clone-passphrase.bin --encryption-passphrase-file passphrase.bin mypool/myclone

Since LUKS2 header is usually bigger than LUKS1 header, ``rbd resize``
command at the beginning temporarily grows the parent image to reserve
some extra space in the parent snapshot and consequently the cloned
image. This is necessary to make all parent data accessible in the
cloned image. ``rbd resize`` commands at the end shrink the parent
image back to its original size (this does not impact the parent
snapshot) and also the cloned image to get rid of unused reserved
space.

The same applies to creating a formatted clone of an unformatted
(plaintext) image since an unformatted image does not have a header at
all.

To map a formatted clone, provide encryption formats and passphrases
for the clone itself and all of its explicitly formatted parent images.
The order in which ``encryption-format`` and ``encryption-passphrase-file``
options should be provided is based on the image hierarchy: start with
that of the cloned image, then its parent and so on.

Here is an example of a command that maps a formatted clone:

.. prompt:: bash #

   rbd device map -t nbd -o encryption-passphrase-file=clone-passphrase.bin -o encryption-passphrase-file=passphrase.bin mypool/myclone

.. _journal feature: ../rbd-mirroring/#enable-image-journaling-feature
.. _Supported Formats: #supported-formats
.. _rbd-nbd: ../../man/8/rbd-nbd
