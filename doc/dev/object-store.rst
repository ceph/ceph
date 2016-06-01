====================================
 Object Store Architecture Overview
====================================

.. graphviz::

  /*
   * Rough outline of object store module dependencies
   */

  digraph object_store {
    size="7,7";
    node [color=lightblue2, style=filled, fontname="Serif"];

    "testrados" -> "librados"
    "testradospp" -> "librados"

    "rbd" -> "librados"

    "radostool" -> "librados"

    "radosgw-admin" -> "radosgw"

    "radosgw" -> "librados"

    "radosacl" -> "librados"

    "librados" -> "objecter"

    "ObjectCacher" -> "Filer"

    "dumpjournal" -> "Journaler"

    "Journaler" -> "Filer"

    "SyntheticClient" -> "Filer"
    "SyntheticClient" -> "objecter"

    "Filer" -> "objecter"

    "objecter" -> "OSDMap"

    "ceph-osd" -> "PG"
    "ceph-osd" -> "ObjectStore"

    "crushtool" -> "CrushWrapper"

    "OSDMap" -> "CrushWrapper"

    "OSDMapTool" -> "OSDMap"

    "PG" -> "ReplicatedPG"
    "PG" -> "ObjectStore"
    "PG" -> "OSDMap"

    "ReplicatedPG" -> "ObjectStore"
    "ReplicatedPG" -> "OSDMap"

    "ObjectStore" -> "FileStore"

    "FileStore" -> "xfs"
    "FileStore" -> "btrfs"
    "FileStore" -> "ext4"
  }


.. todo:: write more here
