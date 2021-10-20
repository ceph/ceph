@ceph_shell
Feature: Testing basic ceph shell commands
  In order to be able to provide storage services
  As an system administrator
  I want to install a Ceph cluster in the following server infrastructure:
    - 3 nodes with 8Gb RAM, 4 CPUs, and 3 storage devices of 20Gb each.
    - Using Fedora32 image in each node


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
        ceph-node-00.cephlab.com    _admin
        """


  Scenario: Execute ceph command to check orch device list
    Given I log as root into ceph-node-00
    When I execute in cephadm_shell
        """
        ceph orch device ls
        """
    Then I get results which contain
        """
        Hostname    Path    Type
        ceph-node-00.cephlab.com    /dev/vdb  hdd
        ceph-node-00.cephlab.com    /dev/vdc  hdd
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
        mgr 2/2
        mon 1/5
        prometheus  1/1
        """
