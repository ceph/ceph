Feature: Testing Rook orchestrator commands
    Ceph has been installed using the cluster CRD available in deploy/examples/cluster-test.yaml

    Scenario: Verify Prometheus metrics endpoint is working properly
      Given I can get prometheus server configuration
      Given the prometheus server is serving metrics

    Scenario: Verify some basic metrics are working properly
      Given I can get prometheus server configuration
      Given the prometheus server is serving metrics
      Then the response contains the metric "ceph_osd_in" where "ceph_daemon" is "osd.0" and value equal to 1
      Then the response contains the metric "ceph_osd_in" where "ceph_daemon" is "osd.1" and value equal to 1
      Then the response contains the metric "ceph_osd_in" where "ceph_daemon" is "osd.2" and value equal to 1
      Then the response contains the metric "ceph_mon_quorum_status" where "ceph_daemon" is "mon.a" and value equal to 1
