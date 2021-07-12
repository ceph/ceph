local grafana = import 'grafonnet-lib/grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local annotation = grafana.annotation;
local tablePanel = grafana.tablePanel;
local colors = ["rgba(245, 54, 54, 0.9)","rgba(237, 129, 40, 0.89)","rgba(50, 172, 45, 0.97)"];
local tableStyles(alias, colors, dateFormat, decimals, mappingType, pattern, type, unit) = 
{"alias": alias,"colors": colors,"dateFormat": dateFormat,"decimals": decimals,
"mappingType": mappingType,"pattern": pattern,"type": type,"unit": unit};
local graphPanelSchema(title, nullPointMode, formatY1, formatY2, fill, datasource) = 
graphPanel.new(title=title,nullPointMode=nullPointMode,formatY1=formatY1,formatY2=formatY2,fill=fill,datasource=datasource);
local addTargetSchema(expr, intervalFactor, format, legendFormat) = 
prometheus.target(expr=expr,intervalFactor=intervalFactor,format=format,legendFormat=legendFormat);
 
dashboard.new(
  title='RBD Overview',
  uid='41FrpeUiz',
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

.addRequired(type= 'grafana',id= 'grafana',name= 'Grafana',version= '5.4.2')

.addRequired(type= 'panel',id= 'graph',name= 'Graph',version= '5.0.0')

.addRequired(type= 'datasource',id= 'prometheus',name= 'Prometheus',version= '5.0.0')

.addRequired(type= 'panel',id= 'table',name= 'Table',version= '5.0.0')

.addTemplate(
  grafana.template.datasource(
    'datasource',
    'prometheus',
    'default',
    label='Data Source',
  )
)

.addPanels([
  graphPanelSchema('IOPS', 'null as zero', 'short', 'short', 1, '$datasource')
  .addTargets([addTargetSchema('round(sum(irate(ceph_rbd_write_ops[30s])))', 1, 'time_series', 'Writes'),
               addTargetSchema('round(sum(irate(ceph_rbd_read_ops[30s])))', 1, 'time_series', 'Reads')])
             + {gridPos: {h: 7, w: 8, x: 0, y: 0}},

  graphPanelSchema('Throughput', 'null as zero', 'Bps', 'short', 1, '$datasource')
  .addTargets([addTargetSchema('round(sum(irate(ceph_rbd_write_bytes[30s])))', 1, 'time_series', 'Write'),
               addTargetSchema('round(sum(irate(ceph_rbd_read_bytes[30s])))', 1, 'time_series', 'Read')])
             + {gridPos: {h: 7, w: 8, x: 8, y: 0}},

  graphPanelSchema('Average Latency', 'null as zero', 'ns', 'short', 1, '$datasource')
  .addTargets([addTargetSchema('round(sum(irate(ceph_rbd_write_latency_sum[30s])) / sum(irate(ceph_rbd_write_latency_count[30s])))', 1, 'time_series', 'Write'),
               addTargetSchema('round(sum(irate(ceph_rbd_read_latency_sum[30s])) / sum(irate(ceph_rbd_read_latency_count[30s])))', 1, 'time_series', 'Read')])
             + {gridPos: {h: 7, w: 8, x: 16, y: 0}},                      

  tablePanel.new(
      title='Highest IOPS',
      datasource='$datasource',
      sort={col:3, desc:true},
      styles=[
          tableStyles('Pool', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'pool', 'string', 'short'),
          tableStyles('Image', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'image', 'string', 'short'),
          tableStyles('IOPS', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'value', 'number', 'iops'),
          tableStyles('', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, '"/.*/', 'hidden', 'short'),
      ])
  .addTargets([addTargetSchema('topk(10, (sort((irate(ceph_rbd_write_ops[30s]) + on (image, pool, namespace) irate(ceph_rbd_read_ops[30s])))))', 1, 'table', '')])
             + {gridPos: {h: 7, w: 8, x: 0, y: 7}},

  tablePanel.new(
      title='Highest Throughput',
      datasource='$datasource',
      sort={col:3, desc:true},
      styles=[
          tableStyles('Pool', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'pool', 'string', 'short'),
          tableStyles('Image', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'image', 'string', 'short'),
          tableStyles('Throughput', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'Value', 'number', 'Bps'),
          tableStyles('', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, '"/.*/', 'hidden', 'short'),
      ])
  .addTargets([addTargetSchema('topk(10, sort(sum(irate(ceph_rbd_read_bytes[30s]) + irate(ceph_rbd_write_bytes[30s])) by (pool, image, namespace)))', 1, 'table', '')])
             + {gridPos: {h: 7, w: 8, x: 8, y: 7}},    

  tablePanel.new(
      title='Highest Latency',
      datasource='$datasource',
      sort={col:3, desc:true},
      styles=[
          tableStyles('Pool', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'pool', 'string', 'short'),
          tableStyles('Image', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'image', 'string', 'short'),
          tableStyles('Latency', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, 'Value', 'number', 'ns'),
          tableStyles('', colors, 'YYYY-MM-DD HH:mm:ss', 2, 1, '"/.*/', 'hidden', 'short'),
      ])
  .addTargets([addTargetSchema('topk(10,\n  sum(\n    irate(ceph_rbd_write_latency_sum[30s]) / clamp_min(irate(ceph_rbd_write_latency_count[30s]), 1) +\n    irate(ceph_rbd_read_latency_sum[30s]) / clamp_min(irate(ceph_rbd_read_latency_count[30s]), 1)\n  ) by (pool, image, namespace)\n)', 1, 'table', '')])
             + {gridPos: {h: 7, w: 8, x: 16, y: 7}},    
])
