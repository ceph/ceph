local g = import 'grafana.libsonnet';

local dashboardSchema(title, uid, time_from, refresh, schemaVersion, tags,timezone, timepicker) =
  g.dashboard.new(title=title, uid=uid, time_from=time_from, refresh=refresh, schemaVersion=schemaVersion, tags=tags, timezone=timezone, timepicker=timepicker);

local graphPanelSchema(title, nullPointMode, stack, formatY1, formatY2, labelY1, labelY2, min, fill, datasource) =
  g.graphPanel.new(title=title, nullPointMode=nullPointMode, stack=stack, formatY1=formatY1, formatY2=formatY2, labelY1=labelY1, labelY2=labelY2, min=min, fill=fill, datasource=datasource);

local addTargetSchema(expr, intervalFactor, format, legendFormat) =
  g.prometheus.target(expr=expr, intervalFactor=intervalFactor, format=format, legendFormat=legendFormat);

local addTemplateSchema(name, datasource, query, refresh, hide, includeAll, sort) =
  g.template.new(name=name, datasource=datasource, query=query, refresh=refresh, hide=hide, includeAll=includeAll, sort=sort);

local addAnnotationSchema(builtIn, datasource, enable, hide, iconColor, name, type) =
  g.annotation.datasource(builtIn=builtIn, datasource=datasource, enable=enable, hide=hide, iconColor=iconColor, name=name, type=type);

{
  "radosgw-sync-overview.json":
    local RgwSyncOverviewPanel(title, formatY1, labelY1, rgwMetric, x, y, w, h) =
      graphPanelSchema({}, title, '', 'null as zero', true, formatY1, 'short', labelY1, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema('sum by (source_zone) (rate(%s[30s]))' % rgwMetric, 1, 'time_series', '{{source_zone}}')]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RGW Sync Overview', '', 'rgw-sync-overview', 'now-1h', '15s', 16, ["overview"], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.0.0'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
       addTemplateSchema('rgw_servers', '$datasource', 'prometehus', 1, true, 1, '', '')
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addPanels([
      RgwSyncOverviewPanel(
        'Replication (throughput) from Source Zone', 'Bps', null, 'ceph_data_sync_from_zone_fetch_bytes_sum', 0, 0, 8, 7),
      RgwSyncOverviewPanel(
        'Replication (objects) from Source Zone', 'short', 'Objects/s', 'ceph_data_sync_from_zone_fetch_bytes_count', 8, 0, 8, 7),
      RgwSyncOverviewPanel(
        'Polling Request Latency from Source Zone', 'ms', null, 'ceph_data_sync_from_zone_poll_latency_sum', 16, 0, 8, 7),
      RgwSyncOverviewPanel(
        'Unsuccessful Object Replications from Source Zone', 'short', 'Count/s', 'ceph_data_sync_from_zone_fetch_errors', 0, 7, 8, 7)
    ])
}
{
  "radosgw-overview.json":
    local RgwOverviewPanel(title, description, formatY1, formatY2, expr1, legendFormat1, x, y, w, h) =
      graphPanelSchema({}, title, description, 'null', false, formatY1, formatY2, null, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr1, 1, 'time_series', legendFormat1)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RGW Overview', '', 'WAkugZpiz', 'now-1h', '15s', 16, ['overview'], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.0.0'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
       addTemplateSchema('rgw_servers', '$datasource', 'label_values(ceph_rgw_req, ceph_daemon)', 1, true, 1, '', '')
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addPanels([
      addRowSchema(false, true, 'RGW Overview - All Gateways') + {gridPos: {x: 0, y: 0, w: 24, h: 1}},
      RgwOverviewPanel(
        'Average GET/PUT Latencies', '', 's', 'short', 'rate(ceph_rgw_get_initial_lat_sum[30s]) / rate(ceph_rgw_get_initial_lat_count[30s])', 'GET AVG', 0, 1, 8, 7).addTargets(
        [addTargetSchema('rate(ceph_rgw_put_initial_lat_sum[30s]) / rate(ceph_rgw_put_initial_lat_count[30s])', 1, 'time_series', 'PUT AVG')]),
      RgwOverviewPanel(
        'Total Requests/sec by RGW Instance', '', 'none', 'short', 'sum by(rgw_host) (label_replace(rate(ceph_rgw_req[30s]), \"rgw_host\", \"$1\", \"ceph_daemon\", \"rgw.(.*)\"))', '{{rgw_host}}', 8, 1, 7, 7),
      RgwOverviewPanel(
        'GET Latencies by RGW Instance', 'Latencies are shown stacked, without a yaxis to provide a visual indication of GET latency imbalance across RGW hosts', 's', 'short', 'label_replace(rate(ceph_rgw_get_initial_lat_sum[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\") / \nlabel_replace(rate(ceph_rgw_get_initial_lat_count[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")', '{{rgw_host}}', 15, 1, 6, 7),
      RgwOverviewPanel(
        'Bandwidth Consumed by Type', 'Total bytes transferred in/out of all radosgw instances within the cluster', 'bytes', 'short', 'sum(rate(ceph_rgw_get_b[30s]))', 'GETs', 0, 8, 8, 6).addTargets(
        [addTargetSchema('sum(rate(ceph_rgw_put_b[30s]))', 1, 'time_series', 'PUTs')]),
      RgwOverviewPanel(
        'Bandwidth by RGW Instance', 'Total bytes transferred in/out through get/put operations, by radosgw instance', 'bytes', 'short', 'sum by(rgw_host) (\n  (label_replace(rate(ceph_rgw_get_b[30s]), \"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")) + \n  (label_replace(rate(ceph_rgw_put_b[30s]), \"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\"))\n)', '{{rgw_host}}', 8, 8, 7, 6),
      RgwOverviewPanel(
        'PUT Latencies by RGW Instance', 'Latencies are shown stacked, without a yaxis to provide a visual indication of PUT latency imbalance across RGW hosts', 's', 'short', 'label_replace(rate(ceph_rgw_put_initial_lat_sum[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\") / \nlabel_replace(rate(ceph_rgw_put_initial_lat_count[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")', '{{rgw_host}}', 15, 8, 6, 6)
    ])
}
{
  "radosgw-detail.json":
    local RgwDetailsPanel(aliasColors, title, description, formatY1, formatY2, expr1, expr2, legendFormat1, legendFormat2, x, y, w, h) =
      graphPanelSchema(aliasColors, title, description, 'null', false, formatY1, formatY2, null, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr1, 1, 'time_series', legendFormat1),addTargetSchema(expr2, 1, 'time_series', legendFormat2)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RGW Instance Detail', '', 'x5ARzZtmk', 'now-1h', '15s', 16, ['overview'], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.0.0'
    )
    .addRequired(
       type='panel', id='grafana-piechart-panel', name='Pie Chart', version='1.3.3'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
       addTemplateSchema('rgw_servers', '$datasource', 'label_values(ceph_rgw_req, ceph_daemon)', 1, true, 1, '', '')
    )
    .addPanels([
      addRowSchema(false, true, 'RGW Host Detail : $rgw_servers') + {gridPos: {x: 0, y: 0, w: 24, h: 1}},
      RgwDetailsPanel(
        {}, '$rgw_servers GET/PUT Latencies', '', 's', 'short', 'sum by (ceph_daemon) (rate(ceph_rgw_get_initial_lat_sum{ceph_daemon=~\"($rgw_servers)\"}[30s]) / rate(ceph_rgw_get_initial_lat_count{ceph_daemon=~\"($rgw_servers)\"}[30s]))', 'sum by (ceph_daemon)(rate(ceph_rgw_put_initial_lat_sum{ceph_daemon=~\"($rgw_servers)\"}[30s]) / rate(ceph_rgw_put_initial_lat_count{ceph_daemon=~\"($rgw_servers)\"}[30s]))', 'GET {{ceph_daemon}}', 'PUT {{ceph_daemon}}', 0, 1, 6, 8),
      RgwDetailsPanel(
        {}, 'Bandwidth by HTTP Operation', '', 'bytes', 'short', 'rate(ceph_rgw_get_b{ceph_daemon=~\"$rgw_servers\"}[30s])', 'rate(ceph_rgw_put_b{ceph_daemon=~\"$rgw_servers\"}[30s])', 'GETs {{ceph_daemon}}', 'PUTs {{ceph_daemon}}', 6, 1, 7, 8),
      RgwDetailsPanel(
        {"GETs": "#7eb26d","Other": "#447ebc","PUTs": "#eab839","Requests": "#3f2b5b","Requests Failed": "#bf1b00"},'HTTP Request Breakdown', '', 'short', 'short', 'rate(ceph_rgw_failed_req{ceph_daemon=~\"$rgw_servers\"}[30s])', 'rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s])', 'Requests Failed {{ceph_daemon}}', 'GETs {{ceph_daemon}}', 13, 1, 7, 8)
      .addTargets(
        [addTargetSchema('rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s])', 1, 'time_series', 'PUTs {{ceph_daemon}}'),addTargetSchema('rate(ceph_rgw_req{ceph_daemon=~\"$rgw_servers\"}[30s]) -\n  (rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s]) +\n   rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s]))', 1, 'time_series', 'Other {{ceph_daemon}}')]),
      addPieChartSchema(
        {"GETs": "#7eb26d","Other (HEAD,POST,DELETE)": "#447ebc","PUTs": "#eab839","Requests": "#3f2b5b","Failures": "#bf1b00"},'$datasource', '', 'Under graph', 'pie', 'Workload Breakdown', 'current')
      .addTarget(addTargetSchema('rate(ceph_rgw_failed_req{ceph_daemon=~\"$rgw_servers\"}[30s])', 1, 'time_series', 'Failures {{ceph_daemon}}'))
      .addTarget(addTargetSchema('rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s])', 1, 'time_series', 'GETs {{ceph_daemon}}'))
      .addTarget(addTargetSchema('rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s])', 1, 'time_series', 'PUTs {{ceph_daemon}}'))
      .addTarget(addTargetSchema('rate(ceph_rgw_req{ceph_daemon=~\"$rgw_servers\"}[30s]) -\n  (rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s]) +\n   rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s]))', 1, 'time_series', 'Other (DELETE,LIST) {{ceph_daemon}}')) + {gridPos: {x: 20, y: 1, w: 4, h: 8}}
    ])
}
{
  "rbd-details.json":
    local RbdDetailsPanel(title, formatY1, expr1, expr2, x, y, w, h) =
      graphPanelSchema({}, title, '', 'null as zero', false, formatY1, formatY1, null, null, 0, 1, '$Datasource')
      .addTargets(
        [addTargetSchema(expr1, 1, 'time_series', 'Write'),addTargetSchema(expr2, 1, 'time_series', 'Read')]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RBD Details', 'Detailed Performance of RBD Images (IOPS/Throughput/Latency)', 'YhCYGcuZz', 'now-1h', false, 16, [], '', {refresh_intervals:['5s','10s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.3.3'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
       g.template.datasource('Datasource', 'prometheus', 'default', label=null)
    )
    .addTemplate(
       addTemplateSchema('Pool', '$Datasource', 'label_values(pool)', 1, 0, false, 0, '', '')
    )
    .addTemplate(
       addTemplateSchema('Image', '$Datasource', 'label_values(image)', 1, 0, false, 0, '', '')
    )
    .addPanels([
      RbdDetailsPanel(
        'IOPS', 'iops', 'irate(ceph_rbd_write_ops{pool=\"$Pool\", image=\"$Image\"}[30s])','irate(ceph_rbd_read_ops{pool=\"$Pool\", image=\"$Image\"}[30s])', 0, 0, 8, 9),
      RbdDetailsPanel(
        'Throughput', 'Bps', 'irate(ceph_rbd_write_bytes{pool=\"$Pool\", image=\"$Image\"}[30s])', 'irate(ceph_rbd_read_bytes{pool=\"$Pool\", image=\"$Image\"}[30s])', 8, 0, 8, 9),
      RbdDetailsPanel(
        'Average Latency', 'ns', 'irate(ceph_rbd_write_latency_sum{pool=\"$Pool\", image=\"$Image\"}[30s]) / irate(ceph_rbd_write_latency_count{pool=\"$Pool\", image=\"$Image\"}[30s])', 'irate(ceph_rbd_read_latency_sum{pool=\"$Pool\", image=\"$Image\"}[30s]) / irate(ceph_rbd_read_latency_count{pool=\"$Pool\", image=\"$Image\"}[30s])', 16, 0, 8, 9)
    ])
}
{
  "rbd-overview.json":
    local RgwOverviewStyle(alias, pattern, type, unit) =
      addStyle(alias, null, ["rgba(245, 54, 54, 0.9)","rgba(237, 129, 40, 0.89)","rgba(50, 172, 45, 0.97)"], 'YYYY-MM-DD HH:mm:ss', 2, 1, pattern, [], type, unit, []);
    local RbdOverviewPanel(title, formatY1, expr1, expr2, legendFormat1, legendFormat2, x, y, w, h) =
      graphPanelSchema({}, title, '', 'null', false, formatY1, 'short', null, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr1, 1, 'time_series', legendFormat1),addTargetSchema(expr2, 1, 'time_series', legendFormat2)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RBD Overview', '', '41FrpeUiz', 'now-1h', '30s', 16, ["overview"], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.4.2'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addRequired(
       type='datasource', id='prometheus', name='Prometheus', version='5.0.0'
    )
    .addRequired(
       type='panel', id='table', name='Table', version='5.0.0'
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addPanels([
      RbdOverviewPanel(
        'IOPS', 'short', 'round(sum(irate(ceph_rbd_write_ops[30s])))','round(sum(irate(ceph_rbd_read_ops[30s])))', 'Writes', 'Reads', 0, 0, 8, 7),
      RbdOverviewPanel(
        'Throughput', 'Bps', 'round(sum(irate(ceph_rbd_write_bytes[30s])))','round(sum(irate(ceph_rbd_read_bytes[30s])))', 'Write', 'Read', 8, 0, 8, 7),
      RbdOverviewPanel(
        'Average Latency', 'ns', 'round(sum(irate(ceph_rbd_write_latency_sum[30s])) / sum(irate(ceph_rbd_write_latency_count[30s])))','round(sum(irate(ceph_rbd_read_latency_sum[30s])) / sum(irate(ceph_rbd_read_latency_count[30s])))', 'Write', 'Read', 16, 0, 8, 7),  
      addTableSchema(
        '$datasource', '', {"col": 3,"desc": true}, [RgwOverviewStyle('Pool', 'pool', 'string', 'short'),RgwOverviewStyle('Image', 'image', 'string', 'short'),RgwOverviewStyle('IOPS', 'Value', 'number', 'iops'), RgwOverviewStyle('', '/.*/', 'hidden', 'short')], 'Highest IOPS', 'table'
      )
      .addTarget(
        addTargetSchema('topk(10, (sort((irate(ceph_rbd_write_ops[30s]) + on (image, pool, namespace) irate(ceph_rbd_read_ops[30s])))))', 1, 'table', '')
      ) + {gridPos: {x: 0, y: 7, w: 8, h: 7}},
      addTableSchema(
        '$datasource', '', {"col": 3,"desc": true}, [RgwOverviewStyle('Pool', 'pool', 'string', 'short'),RgwOverviewStyle('Image', 'image', 'string', 'short'),RgwOverviewStyle('Throughput', 'Value', 'number', 'Bps'), RgwOverviewStyle('', '/.*/', 'hidden', 'short')], 'Highest Throughput', 'table'
      )
      .addTarget(
        addTargetSchema('topk(10, sort(sum(irate(ceph_rbd_read_bytes[30s]) + irate(ceph_rbd_write_bytes[30s])) by (pool, image, namespace)))', 1, 'table', '') 
      ) + {gridPos: {x: 8, y: 7, w: 8, h: 7}},
      addTableSchema(
        '$datasource', '', {"col": 3,"desc": true}, [RgwOverviewStyle('Pool', 'pool', 'string', 'short'),RgwOverviewStyle('Image', 'image', 'string', 'short'),RgwOverviewStyle('Latency', 'Value', 'number', 'ns'), RgwOverviewStyle('', '/.*/', 'hidden', 'short')], 'Highest Latency', 'table'
      )
      .addTarget(
        addTargetSchema('topk(10,\n  sum(\n    irate(ceph_rbd_write_latency_sum[30s]) / clamp_min(irate(ceph_rbd_write_latency_count[30s]), 1) +\n    irate(ceph_rbd_read_latency_sum[30s]) / clamp_min(irate(ceph_rbd_read_latency_count[30s]), 1)\n  ) by (pool, image, namespace)\n)', 1, 'table', '') 
      ) + {gridPos: {x: 16, y: 7, w: 8, h: 7}}
    ])
}
