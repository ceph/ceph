Feature: Testing Rook orchestrator commands
    The commands run in a k8s cluster of 3 nodes.
    Ceph has been installed using the cluster CRD available in deploy/examples and

    Scenario: Verify ceph cluster health
      When I run
          """
          ceph health | grep HEALTH
          """
      Then I get
          """
          HEALTH_OK
          """

    Scenario: Verify rook orchestrator status
      When I run
          """
          ceph orch status | grep -e Backend -e Available
          """
      Then I get
          """
          Backend: rook
          Available: Yes
          """

    Scenario: Verify k8s nodes
        When I run
            """
            kubectl get nodes
            """
        Then I get something like
            """
            NAME                                 STATUS   ROLES           AGE   VERSION
            cephkube-ctlplane-0.karmalabs.corp   Ready    control-plane   ...   .+
            [a-zA-Z0-9\-\.]+     Ready    worker          ... + .+
            [a-zA-Z0-9\-\.]+     Ready    worker          ... + .+
            """
