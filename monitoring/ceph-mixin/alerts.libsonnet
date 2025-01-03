{
  prometheusAlerts+:: (import 'prometheus_alerts.libsonnet') +
                      { _config:: $._config },
}
