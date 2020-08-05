.. _octopus_gsg:

Installing Ceph Octopus 
=======================

This procedure is a step-by-step walkthrough of the installation of a three-node
Ceph clutser. If you have never installed Ceph before and you don't know where
to begin, begin here.

Removing an old cluster 
-----------------------

If you mess up the installation at any point during the procedure, you can run
this two-step procedure to return your system to the beginning state. If you
mess up or you lose your place or you just want to start over from the
beginning, now you know how.

1. Run ``cephadm ls`` and make a note of the filesystem ID (fsid)::

        [root@192-168-1-113 ~]# cephadm ls [     {         "style":
        "cephadm:v1",         "name": "mon.192-168-1-113",         "fsid":
        "695cb1be-a0d8-11ea-89f0-48f17fce7093",         "enabled": true,        
        "state": "running",         "container_id":
        "9ea843bd19b47804c4dc1ee829b3a3d244afc37d4b9a737725bb957b0b64bc5b",
                "container_image_name": "docker.io/ceph/ceph:v15",        
        "container_image_id":
        "4569944bb86c3f9b5286057a558a3f852156079f759c9734e54d4f64092be9fa",

2. Use the command ``cephadm rm cluster`` to remove the cluster. Use the FSID
   that you made a note of in the previous step as the argument of the
   ``--fsid`` flag. Use the ``--force`` flag::

        [root@192-168-1-113 ~]# cephadm rm-cluster --fsid
        695cb1be-a0d8-11ea-89f0-48f17fce7093 --force

Octopus Installation Procedure 
------------------------------ 

#. The command ``cephadm bootstrap`` creates a new cluster. It must be run with
   the ``mon-ip`` flag. In this example, the ``--allow-fqdn-hostname`` flag has
   also been included::

        [root@192-168-1-113 ~]# cephadm bootstrap --mon-ip 192.168.1.113
        --allow-fqdn-hostname 
        
        INFO:cephadm:Verifying podman|docker is present...
        INFO:cephadm:Verifying lvm2 is present...  INFO:cephadm:Verifying time
        synchronization is in place...  INFO:cephadm:Unit chronyd.service is
        enabled and running INFO:cephadm:Repeating the final host check...
        INFO:cephadm:podman|docker (/usr/bin/podman) is present
        INFO:cephadm:systemctl is present INFO:cephadm:lvcreate is present
        INFO:cephadm:Unit chronyd.service is enabled and running
        INFO:cephadm:Host looks OK INFO:root:Cluster fsid:
        501812ec-a426-11ea-b71d-48f17fce7093 INFO:cephadm:Verifying IP
        192.168.1.113 port 3300 ...  INFO:cephadm:Verifying IP 192.168.1.113
        port 6789 ...  INFO:cephadm:Mon IP 192.168.1.113 is in CIDR network
        192.168.1.0/24 INFO:cephadm:Pulling latest docker.io/ceph/ceph:v15
        container...  INFO:cephadm:Extracting ceph user uid/gid from container
        image...  INFO:cephadm:Creating initial keys...  INFO:cephadm:Creating
        initial monmap...  INFO:cephadm:Creating mon...  INFO:cephadm:Waiting
        for mon to start...  INFO:cephadm:Waiting for mon...
        INFO:cephadm:Assimilating anything we can from ceph.conf...
        INFO:cephadm:Generating new minimal ceph.conf...
        INFO:cephadm:Restarting the monitor...  INFO:cephadm:Setting mon
        public_network...  INFO:cephadm:Creating mgr...  INFO:cephadm:Wrote
        keyring to /etc/ceph/ceph.client.admin.keyring INFO:cephadm:Wrote config
        to /etc/ceph/ceph.conf INFO:cephadm:Waiting for mgr to start...
        INFO:cephadm:Waiting for mgr...  INFO:cephadm:mgr not available, waiting
        (1/10)...  INFO:cephadm:mgr not available, waiting (2/10)...
        INFO:cephadm:Enabling cephadm module...  INFO:cephadm:Waiting for the
        mgr to restart...  INFO:cephadm:Waiting for Mgr epoch 5...
        INFO:cephadm:Setting orchestrator backend to cephadm...
        INFO:cephadm:Generating ssh key...  INFO:cephadm:Wrote public SSH key to
        to /etc/ceph/ceph.pub INFO:cephadm:Adding key to root@localhost's
        authorized_keys...  INFO:cephadm:Adding host 192-168-1-113...
        INFO:cephadm:Deploying mon service with default placement...
        INFO:cephadm:Deploying mgr service with default placement...
        INFO:cephadm:Deploying crash service with default placement...
        INFO:cephadm:Enabling mgr prometheus module...  INFO:cephadm:Deploying
        prometheus service with default placement...  INFO:cephadm:Deploying
        grafana service with default placement...  INFO:cephadm:Deploying
        node-exporter service with default placement...  INFO:cephadm:Deploying
        alertmanager service with default placement...  INFO:cephadm:Enabling
        the dashboard module...  INFO:cephadm:Waiting for the mgr to restart...
        INFO:cephadm:Waiting for Mgr epoch 13...  INFO:cephadm:Generating a
        dashboard self-signed certificate...  INFO:cephadm:Creating initial
        admin user...  INFO:cephadm:Fetching dashboard port number...
        INFO:cephadm:Ceph Dashboard is now available at:

        URL: https://192-168-1-113:8443/ User: admin Password: 1nuk3cgx5m

        INFO:cephadm:You can access the Ceph CLI with:

        sudo /usr/sbin/cephadm shell --fsid 501812ec-a426-11ea-b71d-48f17fce7093
        -c /etc/ceph/ceph.conf -k /etc/ceph/ceph.client.admin.keyring

        INFO:cephadm:Please consider enabling telemetry to help improve Ceph:

        ceph telemetry on

        For more information see:

        https://docs.ceph.com/docs/master/mgr/telemetry/

        INFO:cephadm:Bootstrap complete.

   For more information on bootstrapping a Ceph cluster, see
   :ref:`cluster_bootstrap`.

#. Enter the cephadm shell::

        [root@192-168-1-113 zdover]# cephadm shell INFO:cephadm:Inferring fsid
        c56ac3b8-a4cc-11ea-82a0-48f17fce7093 INFO:cephadm:Using recent ceph
        image docker.io/ceph/ceph:v15

   .. note:: 
      As of Octopus it is not necessary to install `cephadm-shell`.
      `cephadm-shell` is installed during the bootstrap procedure. 
      If this doesn't mean anything to you, know that it used to be 
      necessary at this point in the installation procedure to install 
      a program called `cephadm-shell` so that you could interact with 
      Ceph, but the Ceph release called "Octopus" automatically installs 
      that program, making its explicit installation unnecessary.


#. Add the Octopus-release repository by running the ``cephadm add-repo``
   command::

        [ceph: root@192-168-1-113 /]# cephadm add-repo --release octopus
        INFO:root:Writing repo to /etc/yum.repos.d/ceph.repo...
        INFO:cephadm:Enabling EPEL...  INFO:cephadm:Enabling supplementary copr
        repo ktdreyer/ceph-el8...  [ceph: root@192-168-1-113 /]# 

   .. note:: The above command took some time to run (~5 minutes).


#. Ensure that SSH is enabled on the node on which the bootstrap command was
   run::

        [zdover@192-168-1-102 ~]$ ssh zdover@192.168.1.113
        zdover@192.168.1.113's password: 
        Activate the web console with: systemctl enable --now cockpit.socket

#. Confirm that Ceph is installed on the first node::

        Last login: Tue May 26 16:27:59 2020 [zdover@192-168-1-113 ~]$ ceph -v
        ceph version 15.2.2 (0c857e985a29d90501a285f242ea9c008df49eb8) octopus
        (stable)

#. Use ``ceph status`` to check that the new system is in place::

        [zdover@192-168-1-113 ~]$ ceph status [errno 13] RADOS permission denied
        (error connecting to the cluster) [zdover@192-168-1-113 ~]$ sudo ceph
        status

        [sudo] password for zdover: 
        cluster: id:
        c56ac3b8-a4cc-11ea-82a0-48f17fce7093 health: HEALTH_WARN Reduced data
        availability: 1 pg inactive 1 pgs not deep-scrubbed in time 1 pgs not
        scrubbed in time OSD count 0 < osd_pool_default_size 3

        services: mon: 1 daemons, quorum 192-168-1-113 (age 3w) mgr:
        192-168-1-113.zqfzvl(active, since 4w) osd: 0 osds: 0 up, 0 in

        data: pools: 1 pools, 1 pgs objects: 0 objects, 0 B usage: 0 B used, 0 B
        / 0 B avail pgs: 100.000% pgs unknown 1 unknown

#. Copy the public key from the first node to the second node::

        [zdover@192-168-1-113 ~]$ su Password: [root@192-168-1-113 zdover]#
        ssh-copy-id -f -i /etc/ceph/ceph.pub root@192.168.1.102
        /usr/bin/ssh-copy-id: INFO: Source of key(s) to be installed:
        "/etc/ceph/ceph.pub" root@192.168.1.102's password: 

        Number of key(s) added: 1


#. Log into the second machine, by running the command: ``ssh
   'root@192.168.1.102'``::

        Make sure that only the key(s) you wanted were added:
        [root@192-168-1-113 zdover]# ssh root@192.168.1.102 root@192.168.1.102's
        password: Activate the web console with: systemctl enable --now
        cockpit.socket

        Last login: Thu May 28 22:34:31 2020 from 192.168.1.113

#. When you are satisfied that you can log into the second node, log out::

        [root@192-168-1-102 ~]# exit logout Connection to 192.168.1.102 closed.


#. Set the hostname of the second node. In the example, the hostname is set 
    to the IP address of the host::

        [zdover@192-168-1-102 ~]$ hostname 192.168.1.102 hostname: you must be
        root to change the host name [zdover@192-168-1-102 ~]$ sudo hostname
        192.168.1.102 [sudo] password for zdover: [zdover@192-168-1-102 ~]$ 

#. On the first node, use the ``ceph orch host add`` command to add the second
    node to the cluster::

        [root@192-168-1-113 zdover]# ceph orch host add 192.168.1.102 Added host
        '192.168.1.102'

#. Set the hostname on the third node::

        [zdover@192-168-1-112 ~]$ sudo hostname 192.168.1.112

        We trust you have received the usual lecture from the local System
        Administrator. It usually boils down to these three things:

            #1) Respect the privacy of others.      #2) Think before you type.
            #3) With great power comes great responsibility.

        [sudo] password for zdover: [zdover@192-168-1-112 ~]$

#. Copy the public key to the third node::

        [root@192-168-1-113 ~]# ssh-copy-id -f -i /etc/ceph/ceph.pub
        root@192.168.1.112 /usr/bin/ssh-copy-id: INFO: Source of key(s) to be
        installed: "/etc/ceph/ceph.pub" The authenticity of host '192.168.1.112
        (192.168.1.112)' can't be established.  ECDSA key fingerprint is
        SHA256:vQGcYvSM+YuQtrtjvHJdj+8C9ROb+tgld969lM6dG0w.  Are you sure you
        want to continue connecting (yes/no/[fingerprint])? yes
        root@192.168.1.112's password:

        Number of key(s) added: 1

#. Now try logging into the machine, with: ``ssh 'root@192.168.1.112'`` and
    check to make sure that only the key(s) you wanted were added.


#. On the first node, run the ``ceph orch host add`` command to add the third
    node to the cluster::

        [root@192-168-1-113 ~]# ceph orch host add 192.168.1.112 Added host
        '192.168.1.112'

#. Apply monitors to hosts two and three::

        [root@192-168-1-113 ~]# ceph orch apply mon 192.168.1.102,192.168.1.112
        Scheduled mon update...

#. Deploy OSDs on all available devices::

       [root@192-168-1-113~]# ceph orch apply osd --all-available-devices
