Feature: Hosts Overview Dashboard

Scenario: "Test network load succeeds"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
  When variable `osd_hosts` is `127.0.0.1`
  Then Grafana panel `Network Load` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 6 |

Scenario: "Test network load with bonding succeeds"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="bond0"} | 20 200 300 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="bond0"} | 20 200 300 |
    | bonding_slaves{instance="127.0.0.1", master="bond0"} | 2 |
  When variable `osd_hosts` is `127.0.0.1`
  Then Grafana panel `Network Load` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 6 |
