@osd
Feature: Tests related to OSD creation
  In order to be able to provide storage services
  As an system administrator
  I want to install a Ceph cluster in the following server infrastructure:
    - 3 nodes with 8Gb RAM, 4 CPUs, and 3 storage devices of 20Gb each.
    - Using Fedora32 image in each node
    - Configure ceph cluster in following way
      - with number of OSD 0


  Scenario: Create OSDs
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch device ls
        """
    Then I wait for 60 seconds until I get
        """
        ceph-node-00.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    Yes
        ceph-node-01.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    Yes
        ceph-node-02.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    Yes
        """
    Then I execute in cephadm_shell
        """
        ceph orch daemon add osd ceph-node-00.cephlab.com:/dev/vdb
        ceph orch daemon add osd ceph-node-01.cephlab.com:/dev/vdb
        ceph orch daemon add osd ceph-node-02.cephlab.com:/dev/vdb
        """
    Then I execute in cephadm_shell
        """
        ceph orch device ls
        """
    Then I wait for 60 seconds until I get
        """
        ceph-node-00.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    No
        ceph-node-01.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    No
        ceph-node-02.cephlab.com  /dev/vdb  hdd  Unknown  N/A    N/A    No
        """
    Then I execute in cephadm_shell
        """
        ceph -s
        """
    Then I get results which contain
        """
        services:
          mon: 3 daemons, quorum ceph-node-00.cephlab.com,ceph-node-01,ceph-node-02
          osd: 3 osds: 3 up
        """


  Scenario: Create block devices
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph -s
        """
    Then I get results which contain
        """
        cluster:
            health: HEALTH_OK
        """
    Then I execute in cephadm_shell
        """
        ceph osd pool create test_pool
        rbd pool init test_pool
        rbd pool stats test_pool
        """
    Then I get results which contain
        """
        Total Images: 0
        Total Snapshots: 0
        Provisioned Size: 0 B
        """
    Then I execute in cephadm_shell
        """
        rbd create test_image --size 4096 --image-feature layering -m <ceph-node-00:ips> -k /etc/ceph/ceph.keyring -p test_pool
        rbd ls test_pool
        """
    Then I get results which contain
        """
        test_image
        """
    Then I execute in cephadm_shell
        """
        rbd info test_pool/test_image
        """
    Then I get results which contain
        """
        size 4 GiB in 1024 objects
        order 22 (4 MiB objects)
        snapshot_count: 0
        format: 2
        features: layering
        """
    Then I execute in host
        """
        sudo modprobe rbd
        """
    Then Execute in cephadm_shell and save output as rbd_map_op
        """
        rbd map test_pool/test_image --id admin -k /etc/ceph/ceph.keyring
        """
    Then Using output I execute in host
        """
        sudo mkfs.ext4 -m0 <exec_output:rbd_map_op>
        """
    Then I get results which contain
        """
        Discarding device blocks: done
        Allocating group tables: done
        Writing inode tables: done
        Creating journal done
        Writing superblocks and filesystem accounting information: done
        """
    Then I execute in host
        """
        sudo mkdir -p /mnt/ceph-block-device
        """
    Then Using output I execute in host
        """
        sudo mount <exec_output:rbd_map_op> /mnt/ceph-block-device
        """
    Then I execute in host
        """
        df -h
        """
    Then I get results which contain
        """
        Filesystem      Size  Used Avail Use% Mounted on
        /dev/rbd0       3.9G   16M  3.9G   1% /mnt/ceph-block-device
        """
