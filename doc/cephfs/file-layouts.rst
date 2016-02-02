
File layouts
============

The layout of a file controls how its contents are mapped to Ceph RADOS objects.  You can
read and write a file's layout using *virtual extended attributes* or xattrs.

The name of the layout xattrs depends on whether a file is a regular file or a directory.  Regular
files' layout xattrs are called ``ceph.file.layout``, whereas directories' layout xattrs are called
``ceph.dir.layout``.  Where subsequent examples refer to ``ceph.file.layout``, substitute ``dir`` as appropriate
when dealing with directories.

.. tip::

    Your linux distribution may not ship with commands for manipulating xattrs by default,
    the required package is usually called ``attr``.

Layout fields
-------------

pool
    String, giving ID or name.  Which RADOS pool a file's data objects will be stored in.

stripe_unit
    Integer in bytes.  The size (in bytes) of a block of data used in the RAID 0 distribution of a file. All stripe units for a file have equal size. The last stripe unit is typically incomplete–i.e. it represents the data at the end of the file as well as unused “space” beyond it up to the end of the fixed stripe unit size.

stripe_count
    Integer.  The number of consecutive stripe units that constitute a RAID 0 “stripe” of file data.

object_size
    Integer in bytes.  File data is chunked into RADOS objects of this size.

Reading layouts with ``getfattr``
---------------------------------

Read the layout information as a single string:

.. code-block:: bash

    $ touch file
    $ getfattr -n ceph.file.layout file
    # file: file
    ceph.file.layout="stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=cephfs_data"

Read individual layout fields:

.. code-block:: bash

    $ getfattr -n ceph.file.layout.pool file
    # file: file
    ceph.file.layout.pool="cephfs_data"
    $ getfattr -n ceph.file.layout.stripe_unit file
    # file: file
    ceph.file.layout.stripe_unit="4194304"
    $ getfattr -n ceph.file.layout.stripe_count file
    # file: file
    ceph.file.layout.stripe_count="1"
    $ getfattr -n ceph.file.layout.object_size file
    # file: file
    ceph.file.layout.object_size="4194304"    

.. note::

    When reading layouts, the pool will usually be indicated by name.  However, in 
    rare cases when pools have only just been created, the ID may be output instead.

Directories do not have an explicit layout until it is customized.  Attempts to read
the layout will fail if it has never been modified: this indicates that layout of the
next ancestor directory with an explicit layout will be used.

.. code-block:: bash

    $ mkdir dir
    $ getfattr -n ceph.dir.layout dir
    dir: ceph.dir.layout: No such attribute
    $ setfattr -n ceph.dir.layout.stripe_count -v 2 dir
    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"


Writing layouts with ``setfattr``
---------------------------------

Layout fields are modified using ``setfattr``:

.. code-block:: bash

    $ ceph osd lspools
    0 rbd,1 cephfs_data,2 cephfs_metadata,

    $ setfattr -n ceph.file.layout.stripe_unit -v 1048576 file2
    $ setfattr -n ceph.file.layout.stripe_count -v 8 file2
    $ setfattr -n ceph.file.layout.object_size -v 10485760 file2
    $ setfattr -n ceph.file.layout.pool -v 1 file2  # Setting pool by ID
    $ setfattr -n ceph.file.layout.pool -v cephfs_data file2  # Setting pool by name

.. note::

    When the layout fields of a file are modified using ``setfattr``, this file must be empty, otherwise an error will occur.

.. code-block:: bash

    # touch an empty file
    $ touch file1
    # modify layout field successfully
    $ setfattr -n ceph.file.layout.stripe_count -v 3 file1

    # write something to file1
    $ echo "hello world" > file1
    $ setfattr -n ceph.file.layout.stripe_count -v 4 file1
    setfattr: file1: Directory not empty
    
Inheritance of layouts
----------------------

Files inherit the layout of their parent directory at creation time.  However, subsequent
changes to the parent directory's layout do not affect children.

.. code-block:: bash

    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # Demonstrate file1 inheriting its parent's layout
    $ touch dir/file1
    $ getfattr -n ceph.file.layout dir/file1
    # file: dir/file1
    ceph.file.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # Now update the layout of the directory before creating a second file
    $ setfattr -n ceph.dir.layout.stripe_count -v 4 dir
    $ touch dir/file2

    # Demonstrate that file1's layout is unchanged
    $ getfattr -n ceph.file.layout dir/file1
    # file: dir/file1
    ceph.file.layout="stripe_unit=4194304 stripe_count=2 object_size=4194304 pool=cephfs_data"

    # ...while file2 has the parent directory's new layout
    $ getfattr -n ceph.file.layout dir/file2
    # file: dir/file2
    ceph.file.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"


Files created as descendents of the directory also inherit the layout, if the intermediate
directories do not have layouts set:

.. code-block:: bash

    $ getfattr -n ceph.dir.layout dir
    # file: dir
    ceph.dir.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"
    $ mkdir dir/childdir
    $ getfattr -n ceph.dir.layout dir/childdir
    dir/childdir: ceph.dir.layout: No such attribute
    $ touch dir/childdir/grandchild
    $ getfattr -n ceph.file.layout dir/childdir/grandchild
    # file: dir/childdir/grandchild
    ceph.file.layout="stripe_unit=4194304 stripe_count=4 object_size=4194304 pool=cephfs_data"

    
Adding a data pool to the MDS
---------------------------------

Before you can use a pool with CephFS you have to add it to the Metadata Servers.

.. code-block:: bash

    $ ceph mds add_data_pool cephfs_data_ssd
    # Pool should now show up
    $ ceph fs ls
    .... data pools: [cephfs_data cephfs_data_ssd ]

Make sure that your cephx keys allows the client to access this new pool.
