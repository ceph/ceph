Feature: Testing Rook orchestrator commands
    Ceph has been installed using the cluster CRD available in deploy/examples/cluster-test.yaml

    Scenario: Verify ceph cluster health
      When I run ceph command
          """
          ceph health | grep HEALTH
          """
      Then I get
          """
          HEALTH_OK
          """

    Scenario: Verify rook orchestrator has been enabled correctly
      When I run ceph command
          """
          ceph mgr module ls | grep rook
          """
      Then I get something like
          """
          rook +on
          """

    Scenario: Verify rook orchestrator lists services correctly
        When I run ceph command
            """
            ceph orch ls
            """
        Then I get something like
            """
            NAME +PORTS +RUNNING +REFRESHED +AGE +PLACEMENT
            crash +1/1 .+
            mgr +1/1 .+
            mon +1/1 .+
            osd +3 .+
            """

    Scenario: Verify rook orchestrator lists daemons correctly
        When I run ceph command
            """
            ceph orch ps
            """
        Then I get something like
            """
            NAME +HOST +PORTS +STATUS +REFRESHED +AGE +MEM +USE +MEM +LIM +VERSION +IMAGE +ID
            ceph-exporter.exporter +minikube +running .+
            crashcollector.crash +minikube +running .+
            mgr.a +minikube +running .+
            mon.a +minikube +running .+
            osd.0 +minikube +running .+
            osd.1 +minikube +running .+
            osd.2 +minikube +running .+
            """

    Scenario: Verify rook orchestrator lists devices correctly
        When I run ceph command
            """
            ceph orch device ls
            """
        Then I get something like
            """
            HOST +PATH +TYPE +DEVICE +ID +SIZE +AVAILABLE +REFRESHED +REJECT +REASONS
            minikube +/dev/vdb  +unknown +None +10.0G .+
            minikube +/dev/vdc  +unknown +None +20.0G .+
            minikube +/dev/vdd  +unknown +None +20.0G .+
            minikube +/dev/vde  +unknown +None +20.0G .+
            """
