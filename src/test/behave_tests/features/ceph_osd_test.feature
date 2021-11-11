@osd
Feature: Tests related to OSD creation
  In order to be able to provide storage services
  As an system administrator
  I want to start with a Ceph cluster with:
    NODES | 2
    BOOTSTRAP_FLAG | skip-monitoring-stack | True


  Scenario: Create OSDs
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch device ls
        """
    Then I wait for 180 seconds until I get
        """
        ceph-node-00  /dev/vdb  hdd  Yes
        ceph-node-01  /dev/vdb  hdd  Yes
        """
    Then I execute in cephadm_shell
        """
        ceph orch daemon add osd ceph-node-00:/dev/vdb
        ceph orch daemon add osd ceph-node-01:/dev/vdb
        """
    Then I execute in cephadm_shell
        """
        ceph orch ps --daemon-type osd
        """
    Then I wait for 60 seconds until I get
        """
        osd.0  ceph-node-00
        osd.1  ceph-node-01
        """
    Then I execute in cephadm_shell
        """
        ceph -s
        """
    Then I get results which contain
        """
        services:
          mon: 2 daemons, quorum ceph-node-00,ceph-node-01
          osd: 2 osds: 2 up
        """
