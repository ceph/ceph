local grafana = import 'grafonnet-lib/grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local annotation = grafana.annotation;
local graphPanelSchema(title, nullPointMode, formatY1, formatY2, fill, datasource) = 
graphPanel.new(title=title,nullPointMode=nullPointMode,formatY1=formatY1,formatY2=formatY2,fill=fill,datasource=datasource);
local addTargetSchema(expr, intervalFactor, format, legendFormat) = 
prometheus.target(expr=expr,intervalFactor=intervalFactor,format=format,legendFormat=legendFormat);
local addTemplateSchema(name, datasource, query, refresh) = 
template.new(name=name,datasource=datasource,query=query,refresh=refresh);
 
dashboard.new(
  title='RBD Details',
  uid='YhCYGcuZz',
  description='Detailed Performance of RBD Images (IOPS/Throughput/Latency)',
  time_from='now-1h',
  timezone='utc'
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

.addRequired(type= 'grafana',id= 'grafana',name= 'Grafana',version= '5.3.3')

.addRequired(type= 'panel',id= 'graph',name= 'Graph',version= '5.0.0')

.addTemplate(
  grafana.template.datasource(
    'Datasource',
    'prometheus',
    {}
  )
)

.addTemplate(
  addTemplateSchema('Pool', '$Datasource', 'label_values(pool)', 1)
)

.addTemplate(
  addTemplateSchema('Image', '$Datasource', 'label_values(image)', 1)
)

.addPanels([
  graphPanelSchema('IOPS', 'null as zero', 'iops', 'iops', 1, '$Datasource')
  .addTargets([addTargetSchema('irate(ceph_rbd_write_ops{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Write'),
               addTargetSchema('irate(ceph_rbd_read_ops{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Read')])
             + {gridPos: {h: 9, w: 8, x: 0, y: 0}},

  graphPanelSchema('Throughput', 'null as zero', 'Bps', 'Bps', 1, '$Datasource')
  .addTargets([addTargetSchema('irate(ceph_rbd_write_bytes{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Write'),
               addTargetSchema('irate(ceph_rbd_read_bytes{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Read')])
             + {gridPos: {h: 9, w: 8, x: 8, y: 0}},

  graphPanelSchema('Average Latency', 'null as zero', 'ns', 'ns', 1, '$Datasource')
  .addTargets([addTargetSchema('irate(ceph_rbd_write_latency_sum{pool=\"$Pool\", image=\"$Image\"}[30s]) / irate(ceph_rbd_write_latency_count{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Write'),
               addTargetSchema('irate(ceph_rbd_read_latency_sum{pool=\"$Pool\", image=\"$Image\"}[30s]) / irate(ceph_rbd_read_latency_count{pool=\"$Pool\", image=\"$Image\"}[30s])', 1, 'time_series', 'Read')])
             + {gridPos: {h: 9, w: 8, x: 16, y: 0}},
])
