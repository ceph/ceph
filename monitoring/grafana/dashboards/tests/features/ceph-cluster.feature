Feature: Ceph Cluster Dashboard

Scenario: "Test total PG States"
  Given the following series:
    | metrics | values |
    | ceph_pg_total{foo="var"} | 10 100 |
    | ceph_pg_total{foo="bar"} | 20 200 |
  Then Grafana panel `PG States` with legend `Total` shows:
    | metrics | values |
    | {} | 300 |