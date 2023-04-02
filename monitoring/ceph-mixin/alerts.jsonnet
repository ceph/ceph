std.manifestYamlDoc(((import 'config.libsonnet') + (import 'alerts.libsonnet')).prometheusAlerts, indent_array_in_object=true, quote_keys=false)
