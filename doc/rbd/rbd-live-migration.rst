======================
 Image Live-Migration
======================

.. index:: Ceph Block Device; live-migration

RBD images can be live-migrated between different pools within the same cluster
or between different image formats and layouts. When started, the source image
will be deep-copied to the destination image, pulling all snapshot history and
optionally keeping any link to the source image's parent to help preserve
sparseness.

This copy process can safely run in the background while the new target image is
in-use. There is currently a requirement to temporarily stop using the source
image before preparing a migration. This helps to ensure that the client using
the image is updated to point to the new target image.

.. note::
   Image live-migration requires the Ceph Nautilus release or later. The krbd
   kernel module does not support live-migration at this time.


.. ditaa::

           +-------------+               +-------------+
           | {s} c999    |               | {s}         |
           |  Live       | Target refers |  Live       |
           |  migration  |<-------------*|  migration  |
           |  source     |  to Source    |  target     |
           |             |               |             |
           | (read only) |               | (writable)  |
           +-------------+               +-------------+

               Source                        Target

The live-migration process is comprised of three steps:

#. **Prepare Migration:** The initial step creates the new target image and
   cross-links the source and target images. Similar to `layered images`_,
   attempts to read uninitialized extents within the target image will
   internally redirect the read to the source image, and writes to
   uninitialized extents within the target will internally deep-copy the
   overlapping source image block to the target image.


#. **Execute Migration:** This is a background operation that deep-copies all
   initialized blocks from the source image to the target. This step can be
   run while clients are actively using the new target image.


#. **Finish Migration:** Once the background migration process has completed,
   the migration can be committed or aborted. Committing the migration will
   remove the cross-links between the source and target images, and will
   remove the source image. Aborting the migration will remove the cross-links,
   and will remove the target image.

Prepare Migration
=================

The live-migration process is initiated by running the `rbd migration prepare`
command, providing the source and target images::

    $ rbd migration prepare migration_source [migration_target]

The `rbd migration prepare` command accepts all the same layout optionals as the
`rbd create` command, which allows changes to the immutable image on-disk
layout. The `migration_target` can be skipped if the goal is only to change the
on-disk layout, keeping the original image name.

All clients using the source image must be stopped prior to preparing a
live-migration. The prepare step will fail if it finds any running clients with
the image open in read/write mode. Once the prepare step is complete, the
clients can be restarted using the new target image name. Attempting to restart
the clients using the source image name will result in failure.

The `rbd status` command will show the current state of the live-migration::

    $ rbd status migration_target
    Watchers: none
    Migration:
        	source: rbd/migration_source (5e2cba2f62e)
        	destination: rbd/migration_target (5e2ed95ed806)
        	state: prepared

Note that the source image will be moved to the RBD trash to avoid mistaken
usage during the migration process::

    $ rbd info migration_source
    rbd: error opening image migration_source: (2) No such file or directory
    $ rbd trash ls --all
    5e2cba2f62e migration_source


Execute Migration
=================

After preparing the live-migration, the image blocks from the source image
must be copied to the target image. This is accomplished by running the
`rbd migration execute` command::

    $ rbd migration execute migration_target
    Image migration: 100% complete...done.

The `rbd status` command will also provide feedback on the progress of the
migration block deep-copy process::

    $ rbd status migration_target
    Watchers:
    	watcher=1.2.3.4:0/3695551461 client.123 cookie=123
    Migration:
        	source: rbd/migration_source (5e2cba2f62e)
        	destination: rbd/migration_target (5e2ed95ed806)
        	state: executing (32% complete)


Commit Migration
================

Once the live-migration has completed deep-copying all data blocks from the
source image to the target, the migration can be committed::

    $ rbd status migration_target
    Watchers: none
    Migration:
        	source: rbd/migration_source (5e2cba2f62e)
        	destination: rbd/migration_target (5e2ed95ed806)
        	state: executed
    $ rbd migration commit migration_target
    Commit image migration: 100% complete...done.

If the `migration_source` image is a parent of one or more clones, the `--force`
option will need to be specified after ensuring all descendent clone images are
not in use.

Committing the live-migration will remove the cross-links between the source
and target images, and will remove the source image::

    $ rbd trash list --all


Abort Migration
===============

If you wish to revert the prepare or execute step, run the `rbd migration abort`
command to revert the migration process::

        $ rbd migration abort migration_target
        Abort image migration: 100% complete...done.

Aborting the migration will result in the target image being deleted and access
to the original source image being restored::

        $ rbd ls
        migration_source


.. _layered images: ../rbd-snapshot/#layering
