.. _cephfs-nfs:


=======================
CephFS Exports over NFS
=======================

CephFS namespaces can be exported over NFS protocol using the `NFS-Ganesha NFS server`_

Requirements
============

-  Latest Ceph file system with mgr enabled
-  ``nfs-ganesha``, ``nfs-ganesha-ceph``, ``nfs-ganesha-rados-grace`` and
   ``nfs-ganesha-rados-urls`` packages (version 3.3 and above)

.. note:: From Pacific, the nfs mgr module must be enabled prior to use.

Ganesha Configuration Hierarchy
===============================

Cephadm and rook starts nfs-ganesha daemon with `bootstrap configuration`
containing minimal ganesha configuration, creates empty rados `common config`
object in `nfs-ganesha` pool and watches this config object. The `mgr/nfs`
module adds rados export object urls to the common config object. If cluster
config is set, it creates `user config` object containing custom ganesha
configuration and adds it url to common config object.

.. ditaa::


                             rados://$pool/$namespace/export-$i        rados://$pool/$namespace/userconf-nfs.$cluster_id
                                      (export config)                          (user config)

                        +----------+    +----------+    +----------+      +---------------------------+
                        |          |    |          |    |          |      |                           |
                        | export-1 |    | export-2 |    | export-3 |      | userconf-nfs.$cluster_id  |
                        |          |    |          |    |          |      |                           |
                        +----+-----+    +----+-----+    +-----+----+      +-------------+-------------+
                             ^               ^                ^                         ^
                             |               |                |                         |
                             +--------------------------------+-------------------------+
                                        %url |
                                             |
                                    +--------+--------+
                                    |                 |  rados://$pool/$namespace/conf-nfs.$svc
                                    |  conf+nfs.$svc  |  (common config)
                                    |                 |
                                    +--------+--------+
                                             ^
                                             |
                                   watch_url |
                     +----------------------------------------------+
                     |                       |                      |
                     |                       |                      |            RADOS
             +----------------------------------------------------------------------------------+
                     |                       |                      |            CONTAINER
           watch_url |             watch_url |            watch_url |
                     |                       |                      |
            +--------+-------+      +--------+-------+      +-------+--------+
            |                |      |                |      |                |  /etc/ganesha/ganesha.conf
            |   nfs.$svc.a   |      |   nfs.$svc.b   |      |   nfs.$svc.c   |  (bootstrap config)
            |                |      |                |      |                |
            +----------------+      +----------------+      +----------------+

Create NFS Ganesha Cluster
==========================

.. code:: bash

    $ ceph nfs cluster create <cluster_id> [<placement>] [--ingress --virtual-ip <ip>]

This creates a common recovery pool for all NFS Ganesha daemons, new user based on
``cluster_id``, and a common NFS Ganesha config RADOS object.

.. note:: Since this command also brings up NFS Ganesha daemons using a ceph-mgr
   orchestrator module (see :doc:`/mgr/orchestrator`) such as "mgr/cephadm", at
   least one such module must be enabled for it to work.

   Currently, NFS Ganesha daemon deployed by cephadm listens on the standard
   port. So only one daemon will be deployed on a host.

``<cluster_id>`` is an arbitrary string by which this NFS Ganesha cluster will be
known.

``<placement>`` is an optional string signifying which hosts should have NFS Ganesha
daemon containers running on them and, optionally, the total number of NFS
Ganesha daemons on the cluster (should you want to have more than one NFS Ganesha
daemon running per node). For example, the following placement string means
"deploy NFS Ganesha daemons on nodes host1 and host2 (one daemon per host)::

    "host1,host2"

and this placement specification says to deploy single NFS Ganesha daemon each
on nodes host1 and host2 (for a total of two NFS Ganesha daemons in the
cluster)::

    "2 host1,host2"

To deploy NFS with an HA front-end (virtual IP and load balancer), add the
``--ingress`` flag and specify a virtual IP address. This will deploy a combination
of keepalived and haproxy to provide an high-availability NFS frontend for the NFS
service.

For more details, refer :ref:`orchestrator-cli-placement-spec` but keep
in mind that specifying the placement via a YAML file is not supported.

Delete NFS Ganesha Cluster
==========================

.. code:: bash

    $ ceph nfs cluster rm <cluster_id>

This deletes the deployed cluster.

List NFS Ganesha Cluster
========================

.. code:: bash

    $ ceph nfs cluster ls

This lists deployed clusters.

Show NFS Ganesha Cluster Information
====================================

.. code:: bash

    $ ceph nfs cluster info [<cluster_id>]

This displays ip and port of deployed cluster.

.. note:: This will not work with rook backend. Instead expose port with
   kubectl patch command and fetch the port details with kubectl get services
   command::

   $ kubectl patch service -n rook-ceph -p '{"spec":{"type": "NodePort"}}' rook-ceph-nfs-<cluster-name>-<node-id>
   $ kubectl get services -n rook-ceph rook-ceph-nfs-<cluster-name>-<node-id>

Set Customized NFS Ganesha Configuration
========================================

.. code:: bash

    $ ceph nfs cluster config set <cluster_id> -i <config_file>

With this the nfs cluster will use the specified config and it will have
precedence over default config blocks.

Example use cases

1) Changing log level

  It can be done by adding LOG block in the following way::

   LOG {
    COMPONENTS {
        ALL = FULL_DEBUG;
    }
   }

2) Adding custom export block

  The following sample block creates a single export. This export will not be
  managed by `ceph nfs export` interface::

   EXPORT {
     Export_Id = 100;
     Transports = TCP;
     Path = /;
     Pseudo = /ceph/;
     Protocols = 4;
     Access_Type = RW;
     Attr_Expiration_Time = 0;
     Squash = None;
     FSAL {
       Name = CEPH;
       Filesystem = "filesystem name";
       User_Id = "user id";
       Secret_Access_Key = "secret key";
     }
   }

.. note:: User specified in FSAL block should have proper caps for NFS-Ganesha
   daemons to access ceph cluster. User can be created in following way using
   `auth get-or-create`::

         # ceph auth get-or-create client.<user_id> mon 'allow r' osd 'allow rw pool=nfs-ganesha namespace=<nfs_cluster_name>, allow rw tag cephfs data=<fs_name>' mds 'allow rw path=<export_path>'

Reset NFS Ganesha Configuration
===============================

.. code:: bash

    $ ceph nfs cluster config reset <cluster_id>

This removes the user defined configuration.

.. note:: With a rook deployment, ganesha pods must be explicitly restarted
   for the new config blocks to be effective.

Create CephFS Export
====================

.. warning:: Currently, the nfs interface is not integrated with dashboard. Both
   dashboard and nfs interface have different export requirements and
   create exports differently. Management of dashboard created exports is not
   supported.

.. code:: bash

    $ ceph nfs export create cephfs <fsname> <cluster_id> <pseudo_path> [--readonly] [--path=/path/in/cephfs]

This creates export RADOS objects containing the export block, where

``<fsname>`` is the name of the FS volume used by the NFS Ganesha cluster
that will serve this export.

``<cluster_id>`` is the NFS Ganesha cluster ID.

``<pseudo_path>`` is the export position within the NFS v4 Pseudo Filesystem where the export will be available on the server.  It must be an absolute path and unique.

``<path>`` is the path within cephfs. Valid path should be given and default
path is '/'. It need not be unique. Subvolume path can be fetched using:

.. code::

   $ ceph fs subvolume getpath <vol_name> <subvol_name> [--group_name <subvol_group_name>]

.. note:: Export creation is supported only for NFS Ganesha clusters deployed using nfs interface.

Delete CephFS Export
====================

.. code:: bash

    $ ceph nfs export rm <cluster_id> <pseudo_path>

This deletes an export in an NFS Ganesha cluster, where:

``<cluster_id>`` is the NFS Ganesha cluster ID.

``<pseudo_path>`` is the pseudo root path (must be an absolute path).

List CephFS Exports
===================

.. code:: bash

    $ ceph nfs export ls <cluster_id> [--detailed]

It lists exports for a cluster, where:

``<cluster_id>`` is the NFS Ganesha cluster ID.

With the ``--detailed`` option enabled it shows entire export block.

Get CephFS Export
=================

.. code:: bash

    $ ceph nfs export info <cluster_id> <pseudo_path>

This displays export block for a cluster based on pseudo root name,
where:

``<cluster_id>`` is the NFS Ganesha cluster ID.

``<pseudo_path>`` is the pseudo root path (must be an absolute path).


Create or update CephFS Export via JSON specification
=====================================================

An existing export can be dumped in JSON format with:

.. prompt:: bash #

    ceph nfs export info *<pseudo_path>*

An export can be created or modified by importing a JSON description in the
same format:

.. prompt:: bash #

    ceph nfs export apply -i <json_file>

For example,::

   $ ceph nfs export info mynfs /cephfs > update_cephfs_export.json
   $ cat update_cephfs_export.json
   {
     "export_id": 1,
     "path": "/",
     "cluster_id": "mynfs",
     "pseudo": "/cephfs",
     "access_type": "RW",
     "squash": "no_root_squash",
     "security_label": true,
     "protocols": [
       4
     ],
     "transports": [
       "TCP"
     ],
     "fsal": {
       "name": "CEPH",
       "user_id": "nfs.mynfs.1",
       "fs_name": "a",
       "sec_label_xattr": ""
     },
     "clients": []
   }

The exported JSON can be modified and then reapplied.  Below, *pseudo*
and *access_type* are modified.  When modifying an export, the
provided JSON should fully describe the new state of the export (just
as when creating a new export), with the exception of the
authentication credentials, which will be carried over from the
previous state of the export where possible.

::

   $ ceph nfs export apply -i update_cephfs_export.json
   $ cat update_cephfs_export.json
   {
     "export_id": 1,
     "path": "/",
     "cluster_id": "mynfs",
     "pseudo": "/cephfs_testing",
     "access_type": "RO",
     "squash": "no_root_squash",
     "security_label": true,
     "protocols": [
       4
     ],
     "transports": [
       "TCP"
     ],
     "fsal": {
       "name": "CEPH",
       "user_id": "nfs.mynfs.1",
       "fs_name": "a",
       "sec_label_xattr": ""
     },
     "clients": []
   }


Mount
=====

After the exports are successfully created and NFS Ganesha daemons are no longer in
grace period. The exports can be mounted by

.. code:: bash

    $ mount -t nfs -o port=<ganesha-port> <ganesha-host-name>:<ganesha-pseudo_path> <mount-point>

.. note:: Only NFS v4.0+ is supported.

Troubleshooting
===============

Checking NFS-Ganesha logs with

1) ``cephadm``

   .. code:: bash

      $ cephadm logs --fsid <fsid> --name nfs.<cluster_id>.hostname

2) ``rook``

   .. code:: bash

      $ kubectl logs -n rook-ceph rook-ceph-nfs-<cluster_id>-<node_id> nfs-ganesha

Log level can be changed using `nfs cluster config set` command.

.. _NFS-Ganesha NFS Server: https://github.com/nfs-ganesha/nfs-ganesha/wiki
