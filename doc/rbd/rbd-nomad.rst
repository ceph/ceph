=========================
 Block Devices and Nomad
=========================

Like Kubernetes, Nomad can use Ceph Block Device thanks to `ceph-csi`_, 
which allow to dynamically provision RBD images or import existing one.

Every nomad version can use `ceph-csi`_, however we'll here describe the
latest version available at writing time, Nomad v1.1.2 .

To use Ceph Block Devices with Nomad, you must install
and configure ``ceph-csi`` within your Nomad environment. The following
diagram depicts the Nomad/Ceph technology stack.

.. ditaa::
            +-------------------------+-------------------------+
            |      Container          |          ceph--csi      |
            |                         |            node         |
            |          ^              |                 ^       |
            |          |              |                 |       |
            +----------+--------------+-------------------------+
            |          |                                |       |
            |          v                                |       |
            |                       Nomad               |       |
            |                                           |       |
            +---------------------------------------------------+
            |                       ceph--csi                   |
            |                       controller                  |
            +--------+------------------------------------------+
                     |                                  |
                     | configures       maps            |
                     +---------------+ +----------------+
                                     | | 
                                     v v
            +------------------------+ +------------------------+
            |                        | |        rbd--nbd        |
            |     Kernel Modules     | +------------------------+
            |                        | |         librbd         |
            +------------------------+-+------------------------+
            |                   RADOS Protocol                  |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. note::
    Nomad has many task drivers, but we'll only use a Docker container in this example.

.. important::
   ``ceph-csi`` uses the RBD kernel modules by default which may not support all
   Ceph `CRUSH tunables`_ or `RBD image features`_.

Create a Pool
=============

By default, Ceph block devices use the ``rbd`` pool. Create a pool for
Nopmad persistent storage. Ensure your Ceph cluster is running, then create
the pool. ::

        $ ceph osd pool create nomad

See `Create a Pool`_ for details on specifying the number of placement groups
for your pools, and `Placement Groups`_ for details on the number of placement
groups you should set for your pools.

A newly created pool must be initialized prior to use. Use the ``rbd`` tool
to initialize the pool::

        $ rbd pool init nomad

Configure ceph-csi
==================

Setup Ceph Client Authentication
--------------------------------

Create a new user for nomad and `ceph-csi`. Execute the following and
record the generated key::

    $ ceph auth get-or-create client.nomad mon 'profile rbd' osd 'profile rbd pool=nomad' mgr 'profile rbd pool=nomad'
    [client.nomad]
        key = AQAlh9Rgg2vrDxAARy25T7KHabs6iskSHpAEAQ==


Configure Nomad  
---------------

By default Nomad doesn't allow containers to use privileged mode.
Edit the nomad configuration file by adding this configuration block to `/etc/nomad.d/nomad.hcl`::

    plugin "docker" {
        config {
        allow_privileged = true
        }
    }


Nomad must have `rbd` module loaded, check if it's the case.::

        $ lsmod |grep rbd
        rbd                    94208  2
        libceph               364544  1 rbd

If it's not the case, load it.::

        $ sudo modprobe rbd

And restart Nomad.



Create ceph-csi controller and plugin nodes
===========================================

The `ceph-csi`_ plugin requieres two components:

- **Controller plugin**: Communicates with the provider's API.
- **Node plugin**: execute tasks on the client.

.. note::
    We'll set the ceph-csi's version in those files see `ceph-csi release`_ for other versions.

Configure controller plugin
---------------------------

The controller plugin requires Cpeh monitor addresses of for the Ceph cluster.
Collect both the Ceph cluster unique `fsid` and the monitor addresses::

        $ ceph mon dump
        <...>
        fsid b9127830-b0cc-4e34-aa47-9d1a2e9949a8
        <...>
        0: [v2:192.168.1.1:3300/0,v1:192.168.1.1:6789/0] mon.a
        1: [v2:192.168.1.2:3300/0,v1:192.168.1.2:6789/0] mon.b
        2: [v2:192.168.1.3:3300/0,v1:192.168.1.3:6789/0] mon.c

Generate a `ceph-csi-plugin-controller.nomad` file similar to the example below, substituting
the `fsid` for "clusterID", and the monitor addresses for "monitors"::


        job "ceph-csi-plugin-controller" {
          datacenters = ["dc1"]
        group "controller" {
            network {
              port "metrics" {}
            }
            task "ceph-controller" {
        template {
                data        = <<EOF
        [{
            "clusterID": "b9127830-b0cc-4e34-aa47-9d1a2e9949a8",
            "monitors": [
                "192.168.1.1",
          "192.168.1.2",
          "192.168.1.3"
            ]
        }]
        EOF
                destination = "local/config.json"
                change_mode = "restart"
              }
              driver = "docker"
              config {
                image = "quay.io/cephcsi/cephcsi:v3.3.1"
                volumes = [
                  "./local/config.json:/etc/ceph-csi-config/config.json"
                ]
                mounts = [
                  {
                    type     = "tmpfs"
                    target   = "/tmp/csi/keys"
                    readonly = false
                    tmpfs_options = {
                      size = 1000000 # size in bytes
                    }
                  }
                ]
                args = [
                  "--type=rbd",
                  "--controllerserver=true",
                  "--drivername=rbd.csi.ceph.com",
                  "--endpoint=unix://csi/csi.sock",
                  "--nodeid=${node.unique.name}",
            "--instanceid=${node.unique.name}-controller",
                  "--pidlimit=-1",
            "--logtostderr=true",
                  "--v=5",
                  "--metricsport=$${NOMAD_PORT_metrics}"
                ]
              }
           resources {
                cpu    = 500
                memory = 256
              }
              service {
                name = "ceph-csi-controller"
                port = "metrics"
                tags = [ "prometheus" ]
              }
        csi_plugin {
                id        = "ceph-csi"
                type      = "controller"
                mount_dir = "/csi"
              }
            }
          }
        }

Configure plugin node
---------------------
Generate a `ceph-csi-plugin-node.nomad` file similar to the example below, substituting
the `fsid` for "clusterID", and the monitor addresses for "monitors"::


        job "ceph-csi-plugin-nodes" {
          datacenters = ["dc1"]
          type        = "system"
          group "nodes" {
            network {
              port "metrics" {}
            }
        
            task "ceph-node" {
              driver = "docker"
              template {
                data        = <<EOF
        [{
            "clusterID": "b9127830-b0cc-4e34-aa47-9d1a2e9949a8",
            "monitors": [
                "192.168.1.1",
          "192.168.1.2",
          "192.168.1.3"
            ]
        }]
        EOF
                destination = "local/config.json"
                change_mode = "restart"
              }
              config {
                image = "quay.io/cephcsi/cephcsi:v3.3.1"
                volumes = [
                  "./local/config.json:/etc/ceph-csi-config/config.json"
                ]
                mounts = [
                  {
                    type     = "tmpfs"
                    target   = "/tmp/csi/keys"
                    readonly = false
                    tmpfs_options = {
                      size = 1000000 # size in bytes
                    }
                  }
                ]
                args = [
                  "--type=rbd",
                  "--drivername=rbd.csi.ceph.com",
                  "--nodeserver=true",
                  "--endpoint=unix://csi/csi.sock",
                  "--nodeid=${node.unique.name}",
                  "--instanceid=${node.unique.name}-nodes",
                  "--pidlimit=-1",
            "--logtostderr=true",
                  "--v=5",
                  "--metricsport=$${NOMAD_PORT_metrics}"
                ]
                privileged = true
              }
           resources {
                cpu    = 500
                memory = 256
              }
              service {
                name = "ceph-csi-nodes"
                port = "metrics"
                tags = [ "prometheus" ]
              }
        csi_plugin {
                id        = "ceph-csi"
                type      = "node"
                mount_dir = "/csi"
              }
            }
          }
        }

Start plugin controller and node
--------------------------------
Run::

        nomad job run ceph-csi-plugin-controller.nomad
        nomad job run ceph-csi-plugin-nodes.nomad

`ceph-csi`_ image will be downloaded, after few minutes check plugin status::

        $ nomad plugin status ceph-csi
        ID                   = ceph-csi
        Provider             = rbd.csi.ceph.com
        Version              = 3.3.1
        Controllers Healthy  = 1
        Controllers Expected = 1
        Nodes Healthy        = 1
        Nodes Expected       = 1

        Allocations
        ID        Node ID   Task Group  Version  Desired  Status   Created    Modified
        23b4db0c  a61ef171  nodes       4        run      running  3h26m ago  3h25m ago
        fee74115  a61ef171  controller  6        run      running  3h26m ago  3h25m ago

Using Ceph Block Devices
========================

Create rbd image
----------------

`ceph-csi` requires the cephx credentials for communicating with the Ceph
cluster. Generate a `ceph-volume.hcl` file similar to the example below,
using the newly created nomad user id and cephx key::

        id = "ceph-mysql"
        name = "ceph-mysql"
        type = "csi"
        plugin_id = "ceph-csi"
        capacity_max = "200G"
        capacity_min = "100G"

        capability {
          access_mode     = "single-node-writer"
          attachment_mode = "file-system"
        }

        secrets {
          userID  = "admin"
          userKey = "AQAlh9Rgg2vrDxAARy25T7KHabs6iskSHpAEAQ=="
        }

        parameters {
          clusterID = "b9127830-b0cc-4e34-aa47-9d1a2e9949a8"
          pool = "nomad"
          imageFeatures = "layering"
        }

Once generated, create the volume::

        $ nomad volume create ceph-volume.hcl

Use rbd image with a container
------------------------------

As example we'll modify Hashicorp learn `nomad sateful`_ example 

Generate a mysql.nomad file similar to the example below.::

        job "mysql-server" {
          datacenters = ["dc1"]
          type        = "service"
          group "mysql-server" {
            count = 1
            volume "ceph-mysql" {
              type      = "csi"
                attachment_mode = "file-system"
                access_mode     = "single-node-writer"
              read_only = false
              source    = "ceph-mysql"
            }
            network {
              port "db" {
                static = 3306
              }
            }
            restart {
              attempts = 10
              interval = "5m"
              delay    = "25s"
              mode     = "delay"
            }
            task "mysql-server" {
              driver = "docker"
              volume_mount {
                volume      = "ceph-mysql"
                destination = "/srv"
                read_only   = false
              }
              env {
                MYSQL_ROOT_PASSWORD = "password"
              }
              config {
                image = "hashicorp/mysql-portworx-demo:latest"
                args  = ["--datadir", "/srv/mysql"]
                ports = ["db"]
              }
              resources {
                cpu    = 500
                memory = 1024
              }
              service {
                name = "mysql-server"
                port = "db"
                check {
                  type     = "tcp"
                  interval = "10s"
                  timeout  = "2s"
                }
              }
            }
          }
        }

Start the job::

        $ nomad job run mysql.nomad

Check job's status::

        nomad job status mysql-server
        ...
        Status        = running
        ...
        Allocations
        ID        Node ID   Task Group    Version  Desired  Status   Created  Modified
        38070da7  9ad01c63  mysql-server  0        run      running  6s ago   3s ago

To check data are actually persistant, you can modify database, purge the job then create it using the same file.
It will reuse the same RBD image.

.. _ceph-csi: https://github.com/ceph/ceph-csi/
.. _csi: https://www.nomadproject.io/docs/internals/plugins/csi
.. _Create a Pool: ../../rados/operations/pools#createpool
.. _Placement Groups: ../../rados/operations/placement-groups
.. _CRUSH tunables: ../../rados/operations/crush-map/#tunables
.. _RBD image features: ../rbd-config-ref/#image-features
.. _nomad sateful: https://learn.hashicorp.com/tutorials/nomad/stateful-workloads-csi-volumes?in=nomad/stateful-workloads#create-the-job-file
.. _ceph-csi release: https://github.com/ceph/ceph-csi#ceph-csi-container-images-and-release-compatibility