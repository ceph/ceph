Feature: RGW Overview Dashboard

Scenario: "Test Average GET Latencies"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_get_obj_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 50 100 |
    | ceph_rgw_op_get_obj_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 20 60 80 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  Then Grafana panel `Average GET/PUT Latencies by RGW Instance` with legend `GET {{rgw_host}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph", rgw_host="foo"} | 1.5 |

Scenario: "Test Average PUT Latencies"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_put_obj_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 15 35 55 |
    | ceph_rgw_op_put_obj_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 30 50 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  Then Grafana panel `Average GET/PUT Latencies by RGW Instance` with legend `PUT {{rgw_host}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph", rgw_host="foo"} | 1 |

Scenario: "Test Total Requests/sec by RGW Instance"
  Given the following series:
    | metrics | values |
    | ceph_rgw_req{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 10 50 100 |
    | ceph_rgw_metadata{ceph_daemon="rgw.1", hostname="localhost", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1 1 1 |
  When interval is `30s`
  Then Grafana panel `Total Requests/sec by RGW Instance` with legend `{{rgw_host}}` shows:
    | metrics | values |
    | {rgw_host="1"} | 1.5 |

Scenario: "Test GET Latencies by RGW Instance"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_get_obj_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 50 100 |
    | ceph_rgw_op_get_obj_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 20 60 80 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When interval is `30s`
  Then Grafana panel `GET Latencies by RGW Instance` with legend `{{rgw_host}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph", rgw_host="foo"} | 1.5 |

Scenario: "Test Bandwidth Consumed by Type- GET"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_get_obj_bytes{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 10 50 100 |
  When evaluation time is `1m`
  And interval is `30s`
  Then Grafana panel `Bandwidth Consumed by Type` with legend `GETs` shows:
    | metrics | values |
    | {} | 1.5 |

Scenario: "Test Bandwidth Consumed by Type- PUT"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_put_obj_bytes{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 5 20 50 |
  When evaluation time is `1m`
  And interval is `30s`
  Then Grafana panel `Bandwidth Consumed by Type` with legend `PUTs` shows:
    | metrics | values |
    | {} | 7.5E-01 |

Scenario: "Test Bandwidth by RGW Instance"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_get_obj_bytes{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 10 50 100 |
    | ceph_rgw_op_put_obj_bytes{instance="127.0.0.1", instance_id="92806566", job="ceph"} | 5 20 50 |
    | ceph_rgw_metadata{ceph_daemon="rgw.1", hostname="localhost", instance="127.0.0.1", instance_id="92806566", job="ceph"} | 1 1 1 |
  When evaluation time is `1m`
  And interval is `30s`
  Then Grafana panel `Bandwidth by RGW Instance` with legend `{{rgw_host}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.1", instance_id="92806566", rgw_host="1"} | 2.25 |

Scenario: "Test PUT Latencies by RGW Instance"
  Given the following series:
    | metrics | values |
    | ceph_rgw_op_put_obj_lat_sum{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 15 35 55 |
    | ceph_rgw_op_put_obj_lat_count{instance="127.0.0.1", instance_id="58892247", job="ceph"} | 10 30 50 |
    | ceph_rgw_metadata{ceph_daemon="rgw.foo", hostname="localhost", instance="127.0.0.1", instance_id="58892247", job="ceph"} | 1 1 1 |
  When evaluation time is `1m`
  And interval is `30s`
  Then Grafana panel `PUT Latencies by RGW Instance` with legend `{{rgw_host}}` shows:
    | metrics | values |
    | {ceph_daemon="rgw.foo", instance="127.0.0.1", instance_id="58892247", job="ceph", rgw_host="foo"} | 1 |

Scenario: "Test Total backend responses by HTTP code"
  Given the following series:
    | metrics | values |
    | haproxy_backend_http_responses_total{job="haproxy",code="200",instance="ingress.rgw.1",proxy="backend"} | 10 100 |
    | haproxy_backend_http_responses_total{job="haproxy",code="404",instance="ingress.rgw.1",proxy="backend"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  When variable `code` is `200`
  Then Grafana panel `Total responses by HTTP code` with legend `Backend {{ code }}` shows:
    | metrics | values |
    | {code="200"} | 1.5 |

Scenario: "Test Total frontend responses by HTTP code"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_http_responses_total{job="haproxy",code="200",instance="ingress.rgw.1",proxy="frontend"} | 10 100 |
    | haproxy_frontend_http_responses_total{job="haproxy",code="404",instance="ingress.rgw.1",proxy="frontend"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  When variable `code` is `200`
  Then Grafana panel `Total responses by HTTP code` with legend `Frontend {{ code }}` shows:
    | metrics | values |
    | {code="200"} | 1.5 |

Scenario: "Test Total http frontend requests by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_http_requests_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_http_requests_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Requests` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend response errors by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_response_errors_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_response_errors_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Response errors` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total frontend requests errors by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_request_errors_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_request_errors_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Requests errors` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend redispatch warnings by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_redispatch_warnings_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_redispatch_warnings_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Backend redispatch` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend retry warnings by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_retry_warnings_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_retry_warnings_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Backend retry` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total frontend requests denied by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_requests_denied_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_requests_denied_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Request denied` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend current queue by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_current_queue{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_current_queue{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total requests / responses` with legend `Backend Queued` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 200 |

Scenario: "Test Total frontend connections by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_connections_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_connections_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total number of connections` with legend `Front` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend connections attempts by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_connection_attempts_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_connection_attempts_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total number of connections` with legend `Back` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total backend connections error by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_connection_errors_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_connection_errors_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Total number of connections` with legend `Back errors` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 3 |

Scenario: "Test Total frontend bytes incoming by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_bytes_in_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_bytes_in_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Current total of incoming / outgoing bytes` with legend `IN Front` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 24 |

Scenario: "Test Total frontend bytes outgoing by instance"
  Given the following series:
    | metrics | values |
    | haproxy_frontend_bytes_out_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_frontend_bytes_out_total{job="haproxy",proxy="frontend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Current total of incoming / outgoing bytes` with legend `OUT Front` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 24 |

Scenario: "Test Total backend bytes incoming by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_bytes_in_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_bytes_in_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Current total of incoming / outgoing bytes` with legend `IN Back` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 24 |

Scenario: "Test Total backend bytes outgoing by instance"
  Given the following series:
    | metrics | values |
    | haproxy_backend_bytes_out_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 10 100 |
    | haproxy_backend_bytes_out_total{job="haproxy",proxy="backend",instance="ingress.rgw.1"} | 20 200 |
  When variable `ingress_service` is `ingress.rgw.1`
  Then Grafana panel `Current total of incoming / outgoing bytes` with legend `OUT Back` shows:
    | metrics | values |
    | {instance="ingress.rgw.1"} | 24 |
