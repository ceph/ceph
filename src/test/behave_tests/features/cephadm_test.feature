
Feature: Install a basic Ceph cluster
  Order to be able to provide storage services
  As an system administrator
  I want to install a Ceph cluster in the following server infraestructure:
          - 3 nodes with 8Gb RAM, 4 CPUs, and 3 storage devices of 20Gb each.
          - Using Fedora32 image in each node


  Scenario: Execute commands in cluster nodes
    Given I log as root into ceph-node-00 and I execute
        """
        curl --silent --remote-name --location https://raw.githubusercontent.com/ceph/ceph/octopus/src/cephadm/cephadm
        chmod +x cephadm
        """
    When I execute
        """
        cephadm version
        """
    Then I get results which contain
        """
        ceph version quincy (dev)
        """
