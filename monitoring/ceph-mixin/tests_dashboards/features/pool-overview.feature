Feature: Ceph Pools Overview Dashboard

  Background:
    Given the dashboard is `pool-overview`

Scenario: "Test Top $topk Client IOPS by Pool - write"
  Given the following series:
    | metrics | values |
    | ceph_pool_wr{pool_id="1", instance="127.0.0.1", job="ceph", cluster="mycluster"} | 10 50 100 |
    | ceph_pool_metadata{pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 1 1 1 |
  When interval is `30s`
  And variable `topk` is `15`
  Then Grafana panel `Top $topk Client IOPS by Pool` with legend `{{name}} - write` shows:
    | metrics | values |
    | {pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 1.5 |
