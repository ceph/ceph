Feature: Test tester

Scenario: "Simple query works"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="192.168.100.2", device="bond0"} | 20 200 |
    | node_network_transmit_bytes{instance="192.168.100.1", device="bond0"} | 20 200 |
    | node_bonding_slaves{instance="127.0.0.1", master="bond0"} | 2 |
  Then query `node_network_transmit_bytes{instance="127.0.0.1"} > 0` produces:
    | metrics | values |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 100 |

Scenario: "Query with evaluation time"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="192.168.100.2", device="bond0"} | 20 200 |
    | node_network_transmit_bytes{instance="192.168.100.1", device="bond0"} | 20 200 |
    | node_bonding_slaves{instance="127.0.0.1", master="bond0"} | 2 |
  When evaluation time is `0m`
  Then query `node_network_transmit_bytes{instance="127.0.0.1"} > 0` produces:
    | metrics | values |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 |

Scenario: "Query with evaluation time and variable value"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{instance="192.168.100.2", device="bond0"} | 20 200 |
    | node_network_transmit_bytes{instance="192.168.100.1", device="bond0"} | 20 200 |
    | node_bonding_slaves{instance="127.0.0.1", master="bond0"} | 2 |
  When evaluation time is `0m`
  And variable `osd_hosts` is `127.0.0.1`
  Then query `node_network_transmit_bytes{instance="$osd_hosts"} > 0` produces:
    | metrics | values |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 |

Scenario: "Query with interval time"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_receive_bytes{instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{instance="192.168.100.2", device="bond0"} | 20 200 300 |
    | node_network_transmit_bytes{instance="192.168.100.1", device="bond0"} | 20 200 300 |
    | node_bonding_slaves{instance="127.0.0.1", master="bond0"} | 2 |
  When evaluation time is `2h`
  And evaluation interval is `1h`
  And interval is `1h`
  And variable `osd_hosts` is `127.0.0.1`
  Then query `node_network_transmit_bytes{instance="$osd_hosts"} > 0` produces:
    | metrics | values |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth1"} | 200 |
    | node_network_transmit_bytes{instance="127.0.0.1", device="eth2"} | 200 |