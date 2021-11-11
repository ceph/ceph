@ceph_shell
Feature: Testing basic ceph shell commands
  In order to be able to provide storage services
  As an system administrator
  I want to start with a Ceph cluster with:
    NODES | 1
    CEPH_CONFIG | mgr | mgr/cephadm/use_agent | false

  Scenario: Execute ceph command to check status
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch status
        """
    Then I get results which contain
        """
        Backend: cephadm
        Available: Yes
        Paused: No
        """


  Scenario: Execute ceph command to check orch host list
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch host ls
        """
    Then I get results which contain
        """
        HOST    LABELS
        ceph-node-00    _admin
        """


  Scenario: Execute ceph command to check orch device list
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch device ls
        """
    Then I get results which contain
        """
        ceph-node-00    /dev/vdb  hdd
        ceph-node-00   /dev/vdc  hdd
        """


  Scenario: Execute ceph command to check orch
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch ls
        """
    Then I wait for 60 seconds until I get
        """
        NAME    RUNNING
        grafana 1/1
        mgr 1/2
        mon 1/5
        alertmanager 1/1
        node-exporter 1/1
        prometheus  1/1
        """
