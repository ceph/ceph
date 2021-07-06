local grafana = import 'grafonnet-lib/grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local annotation = grafana.annotation;
local graphPanelSchema(title, nullPointMode, formatY1, formatY2, labelY1, labelY2, fill, datasource) = 
graphPanel.new(title=title,nullPointMode=nullPointMode,formatY1=formatY1,formatY2=formatY2,labelY1=labelY1,labelY2=labelY2,fill=fill,datasource=datasource);
local addTargetSchema(expr, intervalFactor, format, legendFormat) = 
prometheus.target(expr=expr,intervalFactor=intervalFactor,format=format,legendFormat=legendFormat);
local addTemplateSchema(name, datasource, query, refresh) = 
template.new(name=name,datasource=datasource,query=query,refresh=refresh);
 
dashboard.new(
  title='RGW Sync Overview',
  uid='rgw-sync-overview',
  time_from='now-1h',
  timezone=''
)

.addAnnotation(
  grafana.annotation.datasource(
    builtIn= 1,
    datasource= '-- Grafana --',
    enable= true,
    hide= true,
    iconColor= 'rgba(0, 211, 255, 1)',
    name= 'Annotations & Alerts',
    type= 'dashboard'
  )
)

.addRequired(type= 'grafana',id= 'grafana',name= 'Grafana',version= '5.0.0')

.addRequired(type= 'panel',id= 'graph',name= 'Graph',version= '5.0.0')

.addTemplate(
  grafana.template.datasource(
    'datasource',
    'prometheus',
    'default',
    label='Data Source'
  )
)

.addTemplate(
  addTemplateSchema('rgw_servers', '$datasource', 'prometehus', 1)
)

.addPanels([
  graphPanelSchema('Replication (throughput) from Source Zone', 'null as zero', 'Bps', 'short', null, null, 1, '$datasource')
  .addTargets([addTargetSchema('sum by (source_zone) (rate(ceph_data_sync_from_zone_fetch_bytes_sum[30s]))', 1, 'time_series', '{{source_zone}}')])
             + {gridPos: {h: 7, w: 8, x: 0, y: 0}},

  graphPanelSchema('Replication (objects) from Source Zone', 'null as zero', 'short', 'short', 'Objects/s', null, 1, '$Datasource')
  .addTargets([addTargetSchema('sum by (source_zone) (rate(ceph_data_sync_from_zone_fetch_bytes_count[30s]))', 1, 'time_series', '{{source_zone}}')])
             + {gridPos: {h: 7, w: 7.4, x: 8.3, y: 0}},

  graphPanelSchema('Polling Request Latency from Source Zone', 'null as zero', 'ms', 'short', null, null, 1, '$Datasource')
  .addTargets([addTargetSchema('sum by (source_zone) (rate(ceph_data_sync_from_zone_poll_latency_sum[30s]) * 1000)', 1, 'time_series', '{{source_zone}}')])
             + {gridPos: {h: 7, w: 8, x: 16, y: 0}},

  graphPanelSchema('Unsuccessful Object Replications from Source Zone', 'null as zero', 'short', 'short', 'Count/s', null, 1, '$Datasource')
  .addTargets([addTargetSchema('sum by (source_zone) (rate(ceph_data_sync_from_zone_fetch_errors[30s]))', 1, 'time_series', '{{source_zone}}')])
             + {gridPos: {h: 7, w: 8, x: 0, y: 7}},           
])
