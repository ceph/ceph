
Feature: Install a basic Ceph cluster
  Order to be able to provide storage services
  As an system administrator
  I want to install a Ceph cluster in the following server infraestructure:
          - 3 nodes with 8Gb RAM, 4 CPUs, and 3 storage devices of 20Gb each.
          - Using Fedora32 image in each node


  Scenario: Install cephadm
    Given I log as root into ceph-node-00 and I execute
        """
        curl --silent --remote-name --location https://raw.githubusercontent.com/ceph/ceph/octopus/src/cephadm/cephadm
        chmod +x cephadm
        """
    When I execute
        """
        cephadm version
        """
    Then I get
        """
        Using recent ceph image quay.ceph.io/ceph-ci/ceph@sha256:3de12b528d96767fae5adb4acdd773618b5e0ee7e2b197ae1c174419ddcec7bc
        ceph version 17.0.0-5873-g0509deb6 (0509deb6a895a98e3e582cbb849606bc559b963c) quincy (dev)

        """