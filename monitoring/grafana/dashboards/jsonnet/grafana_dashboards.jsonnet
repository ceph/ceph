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
      graphPanelSchema(title, 'null as zero', true, formatY1, 'short', labelY1, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema('sum by (source_zone) (rate(%s[30s]))' % rgwMetric, 1, 'time_series', '{{source_zone}}')]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'RGW Sync Overview', 'rgw-sync-overview', 'now-1h', '15s', 16, ["overview"], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
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
       addTemplateSchema('rgw_servers', '$datasource', 'prometehus', 1, 2, true, 1)
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
