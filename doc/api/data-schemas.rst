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
