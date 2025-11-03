CephFS Fscrypt
================

Fscrypt is an encryption implementation at the file system level. This file
system encryption allows for encrypting on a per directory level. This allows
for file systems to have encrypted and regular non-encrypted portions.
Fscrypt encryption encrypts file names and data contents.

This page will cover important key concepts that are part of our cephfs
implementation. If you would like to see the full kernel specification
please visit: https://docs.kernel.org/filesystems/fscrypt.html

Key Derivation Process
-------------------------
The master key is the cornerstone to derive all keys within an fscrypt context.

When an fscrypt policy is applied to an empty directory, a per-directory key
is generated using the master key and a randomly generated nonce. This
per-directory key will then be used to encrypt names of directories,
filenames, and symlinks. Each name within a directory is encrypted with the same
per-directory key to allow for faster metadata operations.

When each file gets created, it will use the parent directory key to generate
a per-file key. This per-file key will be used to encrypt the data contents
of a file. The data contents of a file are broken up into fscrypt blocks in
the size of 4K each. These block will each be encrypted a on a per-block
basis that is derived from the per-file key.

Generating filenames
-------------------
When a new inode is created, the name provided is encrypted using the
per-directory key. The plaintext file name will be encrypted. This cipher
text is then converted to a base64 format. This is to ensure that there are no
illegal characters in the directory name (dname). If the length of the base64
encoded name is small enough to fit within a dname entry it will be stored. If
the encoded text is longer than ``NAME_MAX``, the cipher text will be stored in
the ``alternate_name`` field. This cipher text will then be truncated to a smaller
size to be base64 encoded and stored in the dname.

Per-block encryption keys
-------------------
When data blocks are written to a file each block will be written in fscrypt
block sized chunks (4096 bytes) with a unique key. Each per-block key will be
derived from the per-file key+block_num. This means that each encrypted block
written will have a unique key.

Exploring the write path
------------------------
FSCrypt stores data in a granularity of an fscrypt block size of 4096 bytes.
When a file is written data it is split into 4K chunks and then encrypted into a
4K encrypted block. If a write is smaller than 4K in size, it will be
zero-padded up to 4K and then encrypted.

The inode holds two values for sizes of encrypted files:

* Logical size represents the plaintext unencrypted version of the data. This
  is stored in a virtual extended attribute named ``ceph.fscrypt.file``.

* Real size is the encrypted size, which is the 4K padded, that is stored on the
  osds.

When modifying existing files and blocks, a read modify write workload may be
needed. For instance, if a single block needs to be partially updated as, shown
in Figure 1, then a rmw workload is needed. First the block is read, then
decrypted based on the key+blocknum, then new data is merged with the plaintext
version and then encrypted before being written out to the osds.

.. image:: cephfs_fscrypt_rmw_partially_aligned.svg
Figure 1

To determine if a rmw is needed the offset and len of write is analyzed. 

* If an offset is not aligned to an fscrypt block a rmw described above will be
  needed. This checks to see if a read is needed for the start of a write.

* If the offset+len is not aligned to an fscrypt block rmw will be needed here. 
  This checks to see if a read is needed for the end of a write.

* If more than two blocks are part of a write, any adjacent blocks will not
  require a read. This is because a complete overwrite of an fscrypt block will
  be performed and any previous data read will not be needed. This behavior is
  shown in Figure 2. In this case, only blocks 3 and 5 will need to be read.

.. image:: cephfs_fscrypt_rmw_3blocks.svg
Figure 2

Space Amplification
---------------------
In nearly all cases, using encryption will cause space amplification. Any data
sets that aren’t uniformly aligned to fscrypt block boundaries will have this.
The ``max_size`` quota is based off this amplified real size.

Truncates
-------------------------
In cases where a truncate call is not fscrypt block aligned, it will require
rmw on the end block. Since a truncate call is handled by the mds, this rmw
operation is partially handled by the mds. First, the client reads the last block.
Then, as shown in Figure 3, the client requests a truncate (1), mds then does the
write directly to the osds(2,3) before returning status back to the client(4).

.. image:: cephfs_fscrypt_truncate_handshake.svg
Figure 3
