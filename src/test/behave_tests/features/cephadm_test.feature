
Feature: Install a basic Ceph cluster
  Order to be able to provide storage services
  As an system administrator
  I want to install a Ceph cluster in the following server infraestructure:
          - s3 nodes with 8Gb RAM, 4 CPUs, and 3 storage devices of 20Gb each.
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
        Using recent ceph image quay.ceph.io/ceph-ci/ceph@sha256:18dd6d0b867457a19108e52ae1cb5676d79d9d4282af6a7d767ba18a8c480bbb
        ceph version 17.0.0-5858-g7a31ae31 (7a31ae31584fdb967c470974034070ce9e687ac7) quincy (dev)

        """