=====================
Exported data schemas
=====================

Ceph exposes system state through a variety of APIs and CLI tools in
machine-readable formats such as JSON. Many internal tools and sub-systems, as
well as external projects, have been developed assuming that the schema of these
data sources are stable. However, these formats are not officially stable.

.. important:: These schemata cannot be relied upon for long term stability, and
   are subject to change with each release of Ceph.

In an effort to support consumers of these data sources, this page provides
documentation for a variety of data schemas found in Ceph. These schemas are
validated for each version of Ceph, which means that this documentation can be
relied upon for surfacing changes to data source schemas that may affect users
and applications that consume these data sources.

Base Schema
===========

Common definitions used in other schemas.

.. literalinclude:: schema/base.json
  :language: json

Monitor Map
===========

.. literalinclude:: schema/mon_map.json
  :language: json

OSD Map
=======

.. literalinclude:: schema/osd_map.json
  :language: json

Filesystem Map
==============

.. literalinclude:: schema/fs_map.json
  :language: json

MDS Map
=======

.. literalinclude:: schema/mds_map.json
  :language: json

Manager Map
===========

.. literalinclude:: schema/mgr_map.json
  :language: json

Service Map
===========

.. literalinclude:: schema/service_map.json
  :language: json

CRUSH Map
=========

.. literalinclude:: schema/crush_map.json
  :language: json

object_stat_sum_t
=================

.. literalinclude:: schema/object_stat_sum.json
  :language: json

object_stat_collection_t
========================

.. literalinclude:: schema/object_stat_collection.json
  :language: json

pool_stat_t
===========

.. literalinclude:: schema/pool_stat.json
  :language: json
