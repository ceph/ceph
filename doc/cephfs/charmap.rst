.. _charmap:

CephFS Directory Entry Name Normalization and Case Folding
==========================================================

CephFS allows configuring directory trees to **normalize** and possibly **case
fold** directory entry names. This is typically a useful property for file
systems exported by gateways like Samba which enforce a case-insensitive view
of the file system, typically with performance penalties on file systems which
are not case-insensitive.

The following virtual extended attributes control the **character mapping**
rules for directory entries:

* ``ceph.dir.casesensitive``: A boolean setting for the case sensitivity of the directory. If true, case fold the directory entry names.
* ``ceph.dir.normalization``: A string setting for the type of Unicode normalization to apply for directory entry names. Currently the normalization forms D (``nfd``), C (``nfc``), KD (``nfkd``), and KC (``nfkc``) are understood by the client.
* ``ceph.dir.encoding``: A string setting for the encoding to use and enforce for directory entry names. The default and presently only supported encoding is UTF-8 (``utf8``).

There is also a convenience virtual extended attribute that is useful for
getting the JSON encoding of the case sensitivity, normalization, and encoding
configurations:

* ``ceph.dir.charmap``: The complete character mapping configuration for a directory.

It can also be used to **remove** all settings and restore the default CephFS behavior
for directory entry names: uninterpreted bytes without ``/`` that are NUL terminated.

Note the following restrictions on manipulating any of these extended attributes:

* The directory must be empty.
* The directory must not be part of a snapshot.

New subdirectories created under a directory with a ``charmap`` configuration will
inherit (copy) the parent's configuration.

.. note:: The charmap configuration applies only to the entries in the
          directory not the name of the directory itself.

.. note:: You can remove a ``charmap`` on a subdirectory which inherited
          the configuration so long as the preconditions apply: it is empty
          and not part of an existing snapshot.


Normalization
-------------

The ``ceph.dir.normalization`` attribute accepts the following normalization forms:

* **nfd**: Form D (Canonical Decomposition)
* **nfc**: Form C (Canonical Decomposition, followed by Canonical Composition)
* **nfkd**: Form KD (Compatibility Decomposition)
* **nfkc**: Form KC (Compatibility Decomposition, followed by Canonical Composition)

The default normalization for a character mapping configuration is ``nfd``.

.. note:: For more information about Unicode normalization forms, please see `Unicode normalization standard documents`_.

Whenever a directory entry name is generated during path traversal or lookup,
the client will apply the normalization to the name before submitting any
operation to the MDS. On the MDS side, the directory entry names which
are stored are only these normalized names.

For example, to set the normalization on a directory:

::

    $ setfattr -n ceph.dir.normalization -v "" foo/
    
    $ getfattr -n ceph.dir.charmap foo/
    # file: foo/
    ceph.dir.charmap="{\"casesensitive\":true,\"normalization\":\"nfd\",\"encoding\":\"utf8\"}"
    
    $ getfattr -n ceph.dir.normalization foo/
    # file: foo/
    ceph.dir.normalization="nfd"

.. note:: Setting the empty string will cause the MDS to pick the default normalization.

All character mapping configurations must have a normalization enabled. Removing the normalization
will cause the default to be restored:

::

    $ setfattr -n ceph.dir.normalization -v nfc foo/
    $ getfattr -n ceph.dir.normalization foo/
    # file: foo/
    ceph.dir.normalization="nfc"
    
    $ setfattr -x ceph.dir.normalization foo/
    $ getfattr -n ceph.dir.normalization foo/
    # file: foo/
    ceph.dir.normalization="nfd"

To remove normlization on a directory, you must remove the ``ceph.dir.charmap``
configuration.

.. note:: The MDS maintains an ``alternate_name`` metadata (also used for
          encryption) for directory entries which allows the client to persist the
          original un-normalized name used by the application. The MDS does not
          interpret this metadata in any way; it's only used by clients to reconstruct
          the original name of the directory entry.


Case Folding
------------

The ``ceph.dir.casesensitive`` attribute accepts a boolean value. By default,
names are case-sensitive (as normal in a POSIX file system). Setting this value
to false will make the named entries in the directory (and its descendent
directories) case-insensitive.

Case folding requires that names are also normalized. By default, after setting
a directory to be case-insensitive, the ``charmap`` will be:

::

    $ setfattr -n ceph.dir.casesensitive -v 0 foo/
    $ getfattr -n ceph.dir.casesensitive foo/
    # file: foo/
    ceph.dir.casesensitive="0"

    $ getfattr -n ceph.dir.charmap foo/
    # file: foo/
    ceph.dir.charmap="{\"casesensitive\":false,\"normalization\":\"nfd\",\"encoding\":\"utf8\"}"

Note that setting the case sensitivity on a directory will cause the default
normalization to be selected.

.. note:: Normalization is applied before case folding. The directory entry name used
          by the MDS is the case folded and normalized name.


Removing Character Mapping
--------------------------

If a directory is empty and not part of a snapshot, the ``charmap`` can be
removed:

::

   $ setfattr -x ceph.dir.charmap foo/

One can confirm that this restores the normal CephFS behavior:

::

   $ getfattr -n ceph.dir.charmap foo/
   foo/: ceph.dir.charmap: No such attribute

If the attribute does not exist, then there is no character mapping for the
directory. Note that a (future) child or parent directory may have a charmap
configuration but it will have no effect on this directory. A charmap
configuration is only inherited at directory creation.


.. note:: The default charmap includes normalization that cannot be disabled.
          The only way to turn off this functionality is by removing
          this ``charmap`` virtual extended attribute.


Restricting Incompatible Client Access
--------------------------------------

The MDS protects access to directory trees with a ``charmap`` via a new client
feature bit.  The MDS will not allow a client that does not understand the
``charmap`` feature to modify a directory with a ``charmap`` configuration
except to unlink files or remove subdirectories.

You can also require that all clients understand the ``charmap`` feature
to use the file system at all:

.. prompt:: bash #

    ceph fs required_client_features <fs_name> add charmap

.. note:: The kernel driver does not understand the ``charmap`` feature
          and probably will not because existing kernel libraries have
          opinionated case folding and normalization forms. For this reason,
          adding ``charmap`` to the required client features is not
          recommended.

Permissions
-----------

As with other CephFS virtual extended atributes, a client may only set the
``charmap`` configuration on a directory with the **p** MDS auth cap.  Viewing
the configuration does not require this cap.


.. _Unicode normalization standard documents: https://unicode.org/reports/tr15/
