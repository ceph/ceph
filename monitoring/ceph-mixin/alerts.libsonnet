{
  prometheusAlerts+:: std.parseYaml(importstr 'prometheus_alerts.yaml'),
}
