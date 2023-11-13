Feature: Testing Rook orchestrator commands
    Ceph has been installed using the cluster CRD available in deploy/examples/cluster-test.yaml and

    Scenario: Verify ceph cluster health
      When I run
          """
          ceph health | grep HEALTH
          """
      Then I get
          """
          HEALTH_OK
          """
