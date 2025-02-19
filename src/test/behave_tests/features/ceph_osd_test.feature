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
