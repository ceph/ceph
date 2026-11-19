Feature: Ceph Pool Details Dashboard

  Background:
    Given the dashboard is `pool-detail`

Scenario: "Test $pool_name Client Throughput - reads"
  Given the following series:
    | metrics | values |
    | ceph_pool_rd_bytes{pool_id="1", instance="127.0.0.1", job="ceph", cluster="mycluster"} | 10 50 100 |
    | ceph_pool_metadata{pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 1 1 1 |
  When interval is `30s`
  And variable `pool_name` is `foo`
  Then Grafana panel `$pool_name Client Throughput` with legend `reads` shows:
    | metrics | values |
    | {pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 1.5 |

Scenario: "Test $pool_name Client Throughput - writes"
  Given the following series:
    | metrics | values |
    | ceph_pool_wr_bytes{pool_id="1", instance="127.0.0.1", job="ceph", cluster="mycluster"} | 5 20 50 |
    | ceph_pool_metadata{pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 1 1 1 |
  When interval is `30s`
  And variable `pool_name` is `foo`
  Then Grafana panel `$pool_name Client Throughput` with legend `writes` shows:
    | metrics | values |
    | {pool_id="1", instance="127.0.0.1", name="foo", job="ceph", cluster="mycluster"} | 7.5E-01 |
