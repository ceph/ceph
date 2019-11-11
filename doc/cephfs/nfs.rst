===
NFS
===

CephFS namespaces can be exported over NFS protocol using the
`NFS-Ganesha NFS server <https://github.com/nfs-ganesha/nfs-ganesha/wiki>`_.

Requirements
============

-  Ceph file system (preferably latest stable luminous or higher versions)
-  In the NFS server host machine, 'libcephfs2' (preferably latest stable
   luminous or higher), 'nfs-ganesha' and 'nfs-ganesha-ceph' packages (latest
   ganesha v2.5 stable or higher versions)
-  NFS-Ganesha server host connected to the Ceph public network

Configuring NFS-Ganesha to export CephFS
========================================

NFS-Ganesha provides a File System Abstraction Layer (FSAL) to plug in different
storage backends. `FSAL_CEPH <https://github.com/nfs-ganesha/nfs-ganesha/tree/next/src/FSAL/FSAL_CEPH>`_
is the plugin FSAL for CephFS. For each NFS-Ganesha export, FSAL_CEPH uses a
libcephfs client, user-space CephFS client, to mount the CephFS path that
NFS-Ganesha exports.

Setting up NFS-Ganesha with CephFS, involves setting up NFS-Ganesha's
configuration file, and also setting up a Ceph configuration file and cephx
access credentials for the Ceph clients created by NFS-Ganesha to access
CephFS.

NFS-Ganesha configuration
-------------------------

A sample ganesha.conf configured with FSAL_CEPH can be found here,
`<https://github.com/nfs-ganesha/nfs-ganesha/blob/next/src/config_samples/ceph.conf>`_.
It is suitable for a standalone NFS-Ganesha server, or an active/passive
configuration of NFS-Ganesha servers managed by some sort of clustering
software (e.g., Pacemaker). Important details about the options are
added as comments in the sample conf. There are options to do the following:

- minimize Ganesha caching wherever possible since the libcephfs clients
  (of FSAL_CEPH) also cache aggressively

- read from Ganesha config files stored in RADOS objects

- store client recovery data in RADOS OMAP key-value interface

- mandate NFSv4.1+ access

- enable read delegations (need at least v13.0.1 'libcephfs2' package
  and v2.6.0 stable 'nfs-ganesha' and 'nfs-ganesha-ceph' packages)

Configuration for libcephfs clients
-----------------------------------

Required ceph.conf for libcephfs clients includes:

* a [client] section with ``mon_host`` option set to let the clients connect
  to the Ceph cluster's monitors, usually generated via ``ceph config generate-minimal-conf``, e.g., ::

    [global]
            mon host = [v2:192.168.1.7:3300,v1:192.168.1.7:6789], [v2:192.168.1.8:3300,v1:192.168.1.8:6789], [v2:192.168.1.9:3300,v1:192.168.1.9:6789]

Mount using NFSv4 clients
=========================

It is preferred to mount the NFS-Ganesha exports using NFSv4.1+ protocols
to get the benefit of sessions.

Conventions for mounting NFS resources are platform-specific. The
following conventions work on Linux and some Unix platforms:

From the command line::

  mount -t nfs -o nfsvers=4.1,proto=tcp <ganesha-host-name>:<ganesha-pseudo-path> <mount-point>

Current limitations
===================

- Per running ganesha daemon, FSAL_CEPH can only export one Ceph file system
  although multiple directories in a Ceph file system may be exported.

Exporting over NFS clusters deployed using rook
===============================================

This tutorial assumes you have a kubernetes cluster deployed. If not `minikube
<https://kubernetes.io/docs/setup/learning-environment/minikube/>`_ can be used
to setup a single node cluster. In this tutorial minikube is used.

.. note:: Configuration of this tutorial should not be used in a a real
          production cluster. For the purpose of simplification, the security
          aspects of Ceph are overlooked in this setup.

`Rook <https://rook.io/docs/rook/master/ceph-quickstart.html>`_ Setup And Cluster Deployment
--------------------------------------------------------------------------------------------

Clone the rook repository::

        git clone https://github.com/rook/rook.git

Deploy the rook operator::

        cd cluster/examples/kubernetes/ceph
        kubectl create -f common.yaml
        kubectl create -f operator.yaml

.. note:: Nautilus release or latest Ceph image should be used.

Before proceding check if the pods are running::

        kubectl -n rook-ceph get pod


.. note::
        For troubleshooting on any pod use::

                kubectl describe -n rook-ceph pod <pod-name>

If using minikube cluster change the **dataDirHostPath** to **/data/rook** in
cluster-test.yaml file. This is to make sure data persists across reboots.

Deploy the ceph cluster::

        kubectl create -f cluster-test.yaml

To interact with Ceph Daemons, let's deploy toolbox::

        kubectl create -f ./toolbox.yaml

Exec into the rook-ceph-tools pod::

        kubectl -n rook-ceph exec -it $(kubectl -n rook-ceph get pod -l "app=rook-ceph-tools" -o jsonpath='{.items[0].metadata.name}') bash

Check if you have one Ceph monitor, manager, OSD running and cluster is healthy::

        [root@minikube /]# ceph -s
           cluster:
                id:     3a30f44c-a9ce-4c26-9f25-cc6fd23128d0
                health: HEALTH_OK

           services:
                mon: 1 daemons, quorum a (age 14m)
                mgr: a(active, since 13m)
                osd: 1 osds: 1 up (since 13m), 1 in (since 13m)

           data:
                pools:   0 pools, 0 pgs
                objects: 0 objects, 0 B
                usage:   5.0 GiB used, 11 GiB / 16 GiB avail
                pgs:

.. note:: Single monitor should never be used in real production deployment. As
          it can cause single point of failure.

Create a Ceph File System
-------------------------
Using ceph-mgr volumes module, we will create a ceph file system::

        [root@minikube /]# ceph fs volume create myfs

By default replicated size for OSD is 3. Since we are using only one OSD. It can cause error. Let's fix this up by setting replicated size to 1.::

        [root@minikube /]# ceph osd pool set cephfs.myfs.meta size 1
        [root@minikube /]# ceph osd pool set cephfs.myfs.data size 1

.. note:: The replicated size should never be less than 3 in real production deployment.

Check Cluster status again::

        [root@minikube /]# ceph -s
          cluster:
            id:     3a30f44c-a9ce-4c26-9f25-cc6fd23128d0
            health: HEALTH_OK

          services:
            mon: 1 daemons, quorum a (age 27m)
            mgr: a(active, since 27m)
            mds: myfs:1 {0=myfs-a=up:active} 1 up:standby-replay
            osd: 1 osds: 1 up (since 56m), 1 in (since 56m)

          data:
            pools:   2 pools, 24 pgs
            objects: 22 objects, 2.2 KiB
            usage:   5.1 GiB used, 11 GiB / 16 GiB avail
            pgs:     24 active+clean

          io:
            client:   639 B/s rd, 1 op/s rd, 0 op/s wr

Create a NFS-Ganesha Server Cluster
-----------------------------------
Add Storage for NFS-Ganesha Servers to prevent recovery conflicts::

        [root@minikube /]# ceph osd pool create nfs-ganesha 64
        pool 'nfs-ganesha' created
        [root@minikube /]# ceph osd pool set nfs-ganesha size 1
        [root@minikube /]# ceph orchestrator nfs add mynfs nfs-ganesha ganesha

Here we have created a NFS-Ganesha cluster called "mynfs" in "ganesha"
namespace with "nfs-ganesha" OSD pool.

Scale out NFS-Ganesha cluster::

        [root@minikube /]# ceph orchestrator nfs update mynfs 2

Configure NFS-Ganesha Exports
-----------------------------
Initially rook creates ClusterIP service for the dashboard. With this service
type, only the pods in same kubernetes cluster can access it.

Expose Ceph Dashboard port::

        kubectl patch service -n rook-ceph -p '{"spec":{"type": "NodePort"}}' rook-ceph-mgr-dashboard
        kubectl get service -n rook-ceph rook-ceph-mgr-dashboard
        NAME                      TYPE       CLUSTER-IP       EXTERNAL-IP   PORT(S)          AGE
        rook-ceph-mgr-dashboard   NodePort   10.108.183.148   <none>        8443:31727/TCP   117m

This makes the dashboard reachable outside kubernetes cluster and the service
type is changed to NodePort service.

Create JSON file for dashboard::

        $ cat ~/export.json
        {
              "cluster_id": "mynfs",
              "path": "/",
              "fsal": {"name": "CEPH", "user_id":"admin", "fs_name": "myfs", "sec_label_xattr": null},
              "pseudo": "/cephfs",
              "tag": null,
              "access_type": "RW",
              "squash": "no_root_squash",
              "protocols": [4],
              "transports": ["TCP"],
              "security_label": true,
              "daemons": ["mynfs.a", "mynfs.b"],
              "clients": []
        }

.. note:: Don't use this JSON file for real production deployment. As here the
          ganesha servers are given client-admin access rights.

We need to download and run this `script
<https://raw.githubusercontent.com/ceph/ceph/master/src/pybind/mgr/dashboard/run-backend-rook-api-request.sh>`_
to pass the JSON file contents. Dashboard creates NFS-Ganesha export file
based on this JSON file.::

        ./run-backend-rook-api-request.sh POST /api/nfs-ganesha/export "$(cat <json-file-path>)"

Expose the NFS Servers::

        kubectl patch service -n rook-ceph -p '{"spec":{"type": "NodePort"}}' rook-ceph-nfs-mynfs-a
        kubectl patch service -n rook-ceph -p '{"spec":{"type": "NodePort"}}' rook-ceph-nfs-mynfs-b
        kubectl get services -n rook-ceph rook-ceph-nfs-mynfs-a rook-ceph-nfs-mynfs-b
        NAME                    TYPE       CLUSTER-IP       EXTERNAL-IP   PORT(S)          AGE
        rook-ceph-nfs-mynfs-a   NodePort   10.101.186.111   <none>        2049:31013/TCP   72m
        rook-ceph-nfs-mynfs-b   NodePort   10.99.216.92     <none>        2049:31587/TCP   63m

.. note:: Ports are chosen at random by Kubernetes from a certain range.
          Specific port number can be added to nodePort field in spec.

Testing access to NFS Servers
-----------------------------
Open a root shell on the host and mount one of the NFS servers::

        mkdir -p /mnt/rook
        mount -t nfs -o port=31013 $(minikube ip):/cephfs /mnt/rook

Normal file operations can be performed on /mnt/rook if the mount is successful.

.. note:: If minikube is used then VM host is the only client for the servers.
          In a real kubernetes cluster, multiple hosts can be used as clients,
          only when kubernetes cluster node IP addresses are accessible to
          them.
