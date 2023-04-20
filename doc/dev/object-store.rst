====================================
 Object Store Architecture Overview
====================================

.. graphviz::

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

    "PG" -> "PrimaryLogPG"
    "PG" -> "ObjectStore"
    "PG" -> "OSDMap"

    "PrimaryLogPG" -> "ObjectStore"
    "PrimaryLogPG" -> "OSDMap"

    "ObjectStore" -> "BlueStore"

    "BlueStore" -> "rocksdb"
  }


.. todo:: write more here

