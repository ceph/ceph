Feature: Ceph Cluster Dashboard

  Scenario: "Test cluster health"
  Given the following series:
    | metrics                  | values |
    | ceph_health_status{job="ceph",cluster="mycluster"} | 1 |
  Then Grafana panel `Ceph health status` with legend `EMPTY` shows:
    | metrics | values |
    | ceph_health_status{job="ceph",cluster="mycluster"}      | 1 |

  Scenario: "Test Firing Alerts Warning"
    Given the following series:
      | metrics                  | values |
      | ALERTS{alertstate="firing",alertname="Ceph.1", severity="warning", cluster="mycluster"}  | 1 |
      | ALERTS{alertstate="firing",alertname="Ceph.1", severity="critical", cluster="someothercluster"}  | 1 |
      | ALERTS{alertstate="firing",alertname="Ceph.2", severity="critical", cluster="mycluster"}  | 1 |
    Then Grafana panel `Firing Alerts` with legend `Warning` shows:
      | metrics | values |
      | {}      | 1 |

  Scenario: "Test Firing Alerts Critical"
    Given the following series:
      | metrics                  | values |
      | ALERTS{alertstate="firing",alertname="Ceph.1", severity="warning", cluster="mycluster"}  | 1 |
      | ALERTS{alertstate="firing",alertname="Ceph.1", severity="warning", cluster="someothercluster"}  | 1 |
      | ALERTS{alertstate="firing",alertname="Ceph.2", severity="critical", cluster="mycluster"}  | 1 |
    Then Grafana panel `Firing Alerts` with legend `Critical` shows:
      | metrics | values |
      | {}      | 1 |

    Scenario: "Test Available Capacity"
    Given the following series:
      | metrics                  | values |
      | ceph_cluster_total_bytes{job="ceph",cluster="mycluster"}| 100 |
      | ceph_cluster_total_used_bytes{job="ceph",cluster="mycluster"}| 70 |
    Then Grafana panel `Available Capacity` with legend `EMPTY` shows:
      | metrics | values |
      | {job="ceph",cluster="mycluster"} | 0.3  |

  Scenario: "Test Cluster Capacity"
    Given the following series:
      | metrics                  | values |
      | ceph_cluster_total_bytes{job="ceph",cluster="mycluster"}| 100 |
    Then Grafana panel `Cluster Capacity` with legend `EMPTY` shows:
      | metrics | values |
      | ceph_cluster_total_bytes{job="ceph",cluster="mycluster"} | 100  |

  Scenario: "Test Used Capacity"
    Given the following series:
      | metrics                  | values |
      | ceph_cluster_total_used_bytes{job="ceph",cluster="mycluster"}| 100 |
    Then Grafana panel `Used Capacity` with legend `EMPTY` shows:
      | metrics | values |
      | ceph_cluster_total_used_bytes{job="ceph",cluster="mycluster"} | 100  |

  Scenario: "Test Write Throughput"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_op_w_in_bytes{job="ceph", cluster="mycluster", osd="osd.0"} | 500 500 500 |
      | ceph_osd_op_w_in_bytes{job="ceph", cluster="mycluster", osd="osd.1"}  | 500 120 110 |
    Then Grafana panel `Write Throughput` with legend `EMPTY` shows:
      | metrics | values |
      | {}   | 2    |

  Scenario: "Test Write IOPS"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_op_w{job="ceph",cluster="mycluster", osd="osd.0"} | 500 500 500 |
      | ceph_osd_op_w{job="ceph",cluster="mycluster", osd="osd.1"}  | 500 120 110 |
    Then Grafana panel `Write IOPS` with legend `EMPTY` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test Read Throughput"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_op_r_out_bytes{job="ceph", cluster="mycluster", osd="osd.0"} | 500 500 500 |
      | ceph_osd_op_r_out_bytes{job="ceph", cluster="mycluster", osd="osd.1"}  | 500 120 110 |
    Then Grafana panel `Read Throughput` with legend `EMPTY` shows:
      | metrics | values |
      | {}   | 2    |

  Scenario: "Test Read IOPS"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_op_r{job="ceph", cluster="mycluster", osd="osd.0"} | 500 500 500 |
      | ceph_osd_op_r{job="ceph", cluster="mycluster", osd="osd.1"}  | 500 120 110 |
    Then Grafana panel `Read IOPS` with legend `EMPTY` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test OSDs All"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_metadata{job="ceph", cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_osd_metadata{job="ceph", cluster="mycluster", osd="osd.2"} | 1 |
      | ceph_osd_metadata{job="ceph", cluster="mycluster", osd="osd.3"} | 1 |
    Then Grafana panel `OSDs` with legend `All` shows:
      | metrics | values |
      | {}   | 3 |

  Scenario: "Test OSDs In"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_in{job="ceph", cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_osd_in{job="ceph", cluster="mycluster", osd="osd.1"} | 1 |
      | ceph_osd_in{job="ceph", cluster="mycluster", osd="osd.2"} | 1 |
    Then Grafana panel `OSDs` with legend `In` shows:
      | metrics | values |
      | {}   | 3 |

  Scenario: "Test OSDs Out"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_in{cjob="ceph", cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_osd_in{job="ceph", cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_osd_in{job="ceph", cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `OSDs` with legend `Out` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test OSDs Up"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `OSDs` with legend `Up` shows:
      | metrics | values |
      | {}   | 1 |

  Scenario: "Test OSDs Down"
    Given the following series:
      | metrics                  | values |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_osd_up{job="ceph", cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `OSDs` with legend `Down` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test MGRs Standby"
    Given the following series:
      | metrics                  | values |
      | ceph_mgr_status{job="ceph",cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_mgr_status{job="ceph",cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_mgr_status{job="ceph",cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `MGRs` with legend `Standby` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test MGRs Active"
    Given the following series:
      | metrics                  | values |
      | ceph_mgr_status{job="ceph",cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_mgr_status{job="ceph",cluster="mycluster", osd="osd.1"} | 0 |
    Then Grafana panel `MGRs` with legend `Active` shows:
      | metrics | values |
      | {}   | 1 |

  Scenario: "Test Monitors Total"
    Given the following series:
      | metrics                  | values |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `Monitors` with legend `Total` shows:
      | metrics | values |
      | {}   | 3 |

  Scenario: "Test Monitors In Quorum"
    Given the following series:
      | metrics                  | values |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `Monitors` with legend `In Quorum` shows:
      | metrics | values |
      | {}   | 1 |

  Scenario: "Test Monitors out of Quorum"
    Given the following series:
      | metrics                  | values |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.0"} | 1 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.1"} | 0 |
      | ceph_mon_quorum_status{job="ceph",cluster="mycluster", osd="osd.2"} | 0 |
    Then Grafana panel `Monitors` with legend `MONs out of Quorum` shows:
      | metrics | values |
      | {}   | 2 |

  Scenario: "Test Total Capacity"
    Given the following series:
      | metrics                  | values |
      | ceph_cluster_total_bytes{job="ceph",cluster="mycluster", osd="osd.0"} | 100 |
    Then Grafana panel `Capacity` with legend `Total Capacity` shows:
      | metrics | values |
      | ceph_cluster_total_bytes{job="ceph", cluster="mycluster", osd="osd.0"}   | 100 |

  Scenario: "Test Used Capacity"
    Given the following series:
      | metrics                  | values |
      | ceph_cluster_total_used_bytes{job="ceph",cluster="mycluster", osd="osd.0"} | 100 |
    Then Grafana panel `Capacity` with legend `Used` shows:
      | metrics | values |
      | ceph_cluster_total_used_bytes{job="ceph",cluster="mycluster", osd="osd.0"}   | 100 |

    Scenario: "Test Cluster Throughput Write"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_op_w_in_bytes{job="ceph",cluster="mycluster", osd="osd.0"} | 1000 1000|
      | ceph_osd_op_w_in_bytes{job="ceph",cluster="mycluster", osd="osd.1"} | 2000 1500 |
    Then Grafana panel `Cluster Throughput` with legend `Write` shows:
      | metrics | values |
      | {}      | 25  |

  Scenario: "Test Cluster Throughput Read"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_op_r_out_bytes{job="ceph",cluster="mycluster", osd="osd.0"} | 1000 1000|
      | ceph_osd_op_r_out_bytes{job="ceph",cluster="mycluster", osd="osd.1"} | 2000 1500 |
    Then Grafana panel `Cluster Throughput` with legend `Read` shows:
      | metrics | values |
      | {}      | 25  |

    Scenario: "Test IOPS Read"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_op_r{job="ceph",cluster="mycluster", osd="osd.0"} | 1000 1000|
      | ceph_osd_op_r{job="ceph",cluster="mycluster", osd="osd.1"} | 2000 1500 |
    Then Grafana panel `IOPS` with legend `Read` shows:
      | metrics | values |
      | {}      | 25  |

  Scenario: "Test IOPS Write"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_op_w{job="ceph",cluster="mycluster", osd="osd.0"} | 1000 1000|
      | ceph_osd_op_w{job="ceph",cluster="mycluster", osd="osd.1"} | 2000 1500 |
    Then Grafana panel `IOPS` with legend `Write` shows:
      | metrics | values |
      | {}      | 25  |

    Scenario: "Test Pool Used Bytes"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_bytes_used{job="ceph", cluster="mycluster", pool_id="1"} | 10000 |
      | ceph_pool_bytes_used{job="ceph", cluster="mycluster", pool_id="2"} | 20000 |
      | ceph_pool_bytes_used{job="ceph", cluster="mycluster", pool_id="3"} | 30000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="1", name="pool1"} | 2000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="2", name="pool2"} | 4000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="3", name="pool3"} | 6000 |
    Then Grafana panel `Pool Used Bytes` with legend `{{name}}` shows:
      | metrics | values |
      | {job="ceph", cluster="mycluster", name="pool1", pool_id="1"}       | 20000000 |
      | {job="ceph", cluster="mycluster", name="pool2", pool_id="2"}       | 80000000 |
      | {job="ceph", cluster="mycluster", name="pool3", pool_id="3"}       | 180000000 |

  Scenario: "Test Pool Used RAW Bytes"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_stored_raw{job="ceph", cluster="mycluster", pool_id="1"} | 10000 |
      | ceph_pool_stored_raw{job="ceph", cluster="mycluster", pool_id="2"} | 20000 |
      | ceph_pool_stored_raw{job="ceph", cluster="mycluster", pool_id="3"} | 30000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="1", name="pool1"} | 2000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="2", name="pool2"} | 4000 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="3", name="pool3"} | 6000 |
    Then Grafana panel `Pool Used RAW Bytes` with legend `{{name}}` shows:
      | metrics | values |
      | {job="ceph", cluster="mycluster", name="pool1", pool_id="1"}       | 20000000 |
      | {job="ceph", cluster="mycluster", name="pool2", pool_id="2"}       | 80000000 |
      | {job="ceph", cluster="mycluster", name="pool3", pool_id="3"}       | 180000000 |

  Scenario: "Test Pool Objects Quota"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_quota_objects{job="ceph", cluster="mycluster", pool_id="1"} | 10 |
      | ceph_pool_quota_objects{job="ceph", cluster="mycluster", pool_id="2"} | 20 |
      | ceph_pool_quota_objects{job="ceph", cluster="mycluster", pool_id="3"} | 30 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="1", name="pool1"} | 10 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="2", name="pool2"} | 15 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="3", name="pool3"} | 15 |
    Then Grafana panel `Pool Objects Quota` with legend `{{name}}` shows:
      | metrics | values |
      | {job="ceph", cluster="mycluster", name="pool1", pool_id="1"}       | 100 |
      | {job="ceph", cluster="mycluster", name="pool2", pool_id="2"}       | 300 |
      | {job="ceph", cluster="mycluster", name="pool3", pool_id="3"}       | 450|

    Scenario: "Test Pool Quota Bytes"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_quota_bytes{job="ceph", cluster="mycluster", pool_id="1"} | 100 |
      | ceph_pool_quota_bytes{job="ceph", cluster="mycluster", pool_id="2"} | 200 |
      | ceph_pool_quota_bytes{job="ceph", cluster="mycluster", pool_id="3"} | 300 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="1", name="pool1"} | 100 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="2", name="pool2"} | 150 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="3", name="pool3"} | 150 |
    Then Grafana panel `Pool Quota Bytes` with legend `{{name}}` shows:
      | metrics | values |
      | {job="ceph", cluster="mycluster", name="pool1", pool_id="1"}       | 10000 |
      | {job="ceph", cluster="mycluster", name="pool2", pool_id="2"}       | 30000 |
      | {job="ceph", cluster="mycluster", name="pool3", pool_id="3"}       | 45000 |

  Scenario: "Test Objects Per Pool"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_objects{job="ceph", cluster="mycluster", pool_id="1"} | 100 |
      | ceph_pool_objects{job="ceph", cluster="mycluster", pool_id="2"} | 200 |
      | ceph_pool_objects{job="ceph", cluster="mycluster", pool_id="3"} | 300 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="1", name="pool1"} | 100 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="2", name="pool2"} | 150 |
      | ceph_pool_metadata{job="ceph", cluster="mycluster", pool_id="3", name="pool3"} | 150 |
    Then Grafana panel `Objects Per Pool` with legend `{{name}}` shows:
      | metrics | values |
      | {job="ceph", cluster="mycluster", name="pool1", pool_id="1"}       | 10000 |
      | {job="ceph", cluster="mycluster", name="pool2", pool_id="2"}       | 30000 |
      | {job="ceph", cluster="mycluster", name="pool3", pool_id="3"}       | 45000|

  Scenario: "Test OSD Type Count"
    Given the following series:
      | metrics                          | values |
      | ceph_pool_objects{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pool_objects{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `OSD Type Count` with legend `Total` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Backfill Toofull"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_backfill_toofull{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_backfill_toofull{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Backfill Toofull` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Remapped"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_remapped{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_remapped{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Remapped` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Backfill"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_backfill{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_backfill{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Backfill` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Peered"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_peered{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_peered{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Peered` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Down"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_down{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_down{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Down` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Repair"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_repair{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_repair{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Repair` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Recovering"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_recovering{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_recovering{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Recovering` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Deep"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_deep{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_deep{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Deep` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Wait Backfill"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_wait_backfill{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_wait_backfill{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Wait Backfill` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Creating"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_creating{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_creating{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Creating` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Forced Recovery"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_forced_recovery{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_forced_recovery{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Forced Recovery` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Forced Backfill"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_forced_backfill{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_forced_backfill{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Forced Backfill` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Incomplete"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_incomplete{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_incomplete{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Incomplete` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test PGs State Undersized"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_undersized{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_undersized{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `PGs State` with legend `Undersized` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test Stuck PGs Undersized"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_undersized{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_undersized{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `Stuck PGs` with legend `Undersized` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test Stuck PGs Stale"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_stale{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_stale{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `Stuck PGs` with legend `Stale` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test Stuck PGs Degraded"
    Given the following series:
      | metrics                          | values |
      | ceph_pg_degraded{job="ceph", cluster="mycluster", osd="osd.0"} | 10 |
      | ceph_pg_degraded{job="ceph", cluster="mycluster", osd="osd.1"} | 20 |
    Then Grafana panel `Stuck PGs` with legend `Degraded` shows:
      | metrics | values |
      | {}      | 30 |

  Scenario: "Test Recovery Operations"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_recovery_ops{job="ceph", cluster="mycluster", osd="osd.0"}| 250 200 |
      | ceph_osd_recovery_ops{job="ceph", cluster="mycluster", osd="osd.1"} | 800 100 |
    When variable `interval` is `120s`
    Then Grafana panel `Recovery Operations` with legend `OPS` shows:
      | metrics | values |
      |  {} | 5 |

  Scenario: "Test Ceph Versions OSD"
    Given the following series:
      | metrics                          | values |
      | ceph_osd_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)", ceph_daemon="osd.0", device_class="ssd"} | 1 |
      | ceph_osd_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", ceph_daemon="osd.1", device_class="hdd"} | 1 |
    Then Grafana panel `Ceph Versions` with legend `OSD Services` shows:
      | metrics | values |
      |  {ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)"} | 1 |
      |  {ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)"} | 1 |

  Scenario: "Test Ceph Versions Mon"
    Given the following series:
      | metrics                          | values |
      | ceph_mon_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)", hostname="somehostname"}| 1 |
      | ceph_mon_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", hostname="someotherhostname"}| 1 |
    Then Grafana panel `Ceph Versions` with legend `Mon Services` shows:
      | metrics | values |
      |  {ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)"} | 1 |
      |  {ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)"} | 1 |

  Scenario: "Test Ceph Versions MDS"
    Given the following series:
      | metrics                          | values |
      | ceph_mds_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)", hostname="someotherhostname", ceph_daemon="mds.someotherhostname",fs_id="1"}| 1 |
      | ceph_mds_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", hostname="somehostname", ceph_daemon="mds.somehostname",fs_id="1"}| 1 |
    Then Grafana panel `Ceph Versions` with legend `MDS Services` shows:
      | metrics | values |
      |  {ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)"} | 1 |
      |  {ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)"} | 1 |

  Scenario: "Test Ceph Versions RGW"
    Given the following series:
      | metrics                          | values |
      | ceph_rgw_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)", ceph_daemon="rgw.somehostname", hostname="somehostname"}| 1 |
      | ceph_rgw_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", ceph_daemon="rgw.someotherhostname", hostname="someotherhostname"}| 1 |
    Then Grafana panel `Ceph Versions` with legend `RGW Services` shows:
      | metrics | values |
      |  {ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)"} | 1 |
      |  {ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)"} | 1 |

  Scenario: "Test Ceph Versions MGR"
    Given the following series:
      | metrics                          | values |
      | ceph_mgr_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)", ceph_daemon="mgr.somehostname", hostname="somehostname"}| 1 |
      | ceph_mgr_metadata{job="ceph", cluster="mycluster", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", ceph_daemon="mgr.someotherhostname", hostname="someotherhostname"}| 1 |
    Then Grafana panel `Ceph Versions` with legend `MGR Services` shows:
      | metrics | values |
      |  {ceph_version="ceph version 18.2.1 (7fe91d5d5842e04be3b4f514d6dd990c54b29c76) reef (stable)"} | 1 |
      |  {ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)"} | 1 |
