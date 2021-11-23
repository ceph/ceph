Feature: RGW Host Detail Dashboard

Scenario: "Test $rgw_servers GET/PUT Latencies - GET"
  Given the following series:
    | metrics | values |
    | ceph_rgw_get_initial_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 50 100 |
    | ceph_rgw_get_initial_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 20 60 80 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `$rgw_servers GET/PUT Latencies` with legend `GET {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance_id="58892247"} | 2.5000000000000004 |

Scenario: "Test $rgw_servers GET/PUT Latencies - PUT"
  Given the following series:
    | metrics | values |
    | ceph_rgw_put_initial_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 15 35 55 |
    | ceph_rgw_put_initial_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 30 50 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `$rgw_servers GET/PUT Latencies` with legend `PUT {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance_id="58892247"} | 1 |

Scenario: "Test Bandwidth by HTTP Operation - GET"
  Given the following series:
    | metrics | values |
    | ceph_rgw_get_b{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 10 50 100 |
    | ceph_rgw_metadata{ceph_daemon="rgw.1", hostname="localhost", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.1`
  Then Grafana panel `Bandwidth by HTTP Operation` with legend `GETs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.1", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1.6666666666666667 |

Scenario: "Test Bandwidth by HTTP Operation - PUT"
  Given the following series:
    | metrics | values |
    | ceph_rgw_put_b{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 5 20 50 |
    | ceph_rgw_metadata{ceph_daemon="rgw.1", hostname="localhost", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.1`
  Then Grafana panel `Bandwidth by HTTP Operation` with legend `PUTs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.1", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1 |

Scenario: "Test HTTP Request Breakdown - Requests Failed"
  Given the following series:
    | metrics | values |
    | ceph_rgw_failed_req{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 5 7 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `HTTP Request Breakdown` with legend `Requests Failed {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 6.666666666666667e-02 |

Scenario: "Test HTTP Request Breakdown - GET"
  Given the following series:
    | metrics | values |
    | ceph_rgw_get{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 100 150 170 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `HTTP Request Breakdown` with legend `GETs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | .6666666666666666 |

Scenario: "Test HTTP Request Breakdown - PUT"
  Given the following series:
    | metrics | values |
    | ceph_rgw_put{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 70 90 160 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `HTTP Request Breakdown` with legend `PUTs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 2.3333333333333335 |

Scenario: "Test HTTP Request Breakdown - Other"
  Given the following series:
    | metrics | values |
    | ceph_rgw_req{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 175 250 345 |
    | ceph_rgw_get{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 100 150 170 |
    | ceph_rgw_put{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 70 90 160 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `HTTP Request Breakdown` with legend `Other {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | .16666666666666652 |

Scenario: "Test Workload Breakdown - Failures"
  Given the following series:
    | metrics | values |
    | ceph_rgw_failed_req{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 5 7 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `Workload Breakdown` with legend `Failures {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 6.666666666666667e-02 |

Scenario: "Test Workload Breakdown - GETs"
  Given the following series:
    | metrics | values |
    | ceph_rgw_get{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 100 150 170 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `Workload Breakdown` with legend `GETs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | .6666666666666666 |

Scenario: "Test Workload Breakdown - PUTs"
  Given the following series:
    | metrics | values |
    | ceph_rgw_put{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 70 90 160 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `Workload Breakdown` with legend `PUTs {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 2.3333333333333335 |

Scenario: "Test Workload Breakdown - Other"
  Given the following series:
    | metrics | values |
    | ceph_rgw_req{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 175 250 345 |
    | ceph_rgw_get{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 100 150 170 |
    | ceph_rgw_put{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 70 90 160 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  And variable `rgw_servers` is `rgw.foo`
  Then Grafana panel `Workload Breakdown` with legend `Other (DELETE,LIST) {{ceph_daemon}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph"} | .16666666666666652 |
