=============
RGW upgrading to Jewel versions 10.2.0, 10.2.1, 10.2.2 and 10.2.3
=============

.. versionadded:: Jewel

Upgrade of :term:`Ceph Object Gateway` to older versions of jewel (up to 10.2.3 included) caused issues. This document describes the needed recovery procedure.

Mixed version of :term:`Ceph Object Gateway` is not supported

Backup of old configuration
================
rados mkpool .rgw.root.backup
rados cppool .rgw.root .rgw.root.backup

Fix confgiuration after upgrade
================
Stop all :term:`Ceph Object Gateway` running in the cluster.

Run the following commands:::

  $ rados rmpool .rgw.root

  $ radosgw-admin zonegroup get --rgw-zonegroup=default | sed 's/"id":.*/"id": "default",/g' | sed 's/"master_zone.*/"master_zone":"default",/g' > default-zg.json

  $ raodsgw-admin zone get --zone-id=default > default-zone.json

  $ radosgw-admin realm create --rgw-realm=myrealm

  $ radosgw-admin zonegroup set --rgw-zonegroup=default --default < default-zg.json

  $ radosgw-admin zone set --rgw-zone=default --default < default-zone.json

  $ radosgw-admin period update --commit

Start all :term:`Ceph Object Gateway` in the cluster.

