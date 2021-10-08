local g = import 'grafana.libsonnet';

local dashboardSchema(title, description, uid, time_from, refresh, schemaVersion, tags, timezone, timepicker) =
  g.dashboard.new(title=title, description=description, uid=uid, time_from=time_from, refresh=refresh, schemaVersion=schemaVersion, tags=tags, timezone=timezone, timepicker=timepicker);

local graphPanelSchema(aliasColors, title, description, nullPointMode, stack, formatY1, formatY2, labelY1, labelY2, min, fill, datasource) =
  g.graphPanel.new(aliasColors=aliasColors, title=title, description=description, nullPointMode=nullPointMode, stack=stack, formatY1=formatY1, formatY2=formatY2, labelY1=labelY1, labelY2=labelY2, min=min, fill=fill, datasource=datasource);

local addTargetSchema(expr, intervalFactor, format, legendFormat) =
  g.prometheus.target(expr=expr, intervalFactor=intervalFactor, format=format, legendFormat=legendFormat);

local addTemplateSchema(name, datasource, query, refresh, includeAll, sort, label, regex) =
  g.template.new(name=name, datasource=datasource, query=query, refresh=refresh, includeAll=includeAll, sort=sort, label=label, regex=regex);

local addAnnotationSchema(builtIn, datasource, enable, hide, iconColor, name, type) =
  g.annotation.datasource(builtIn=builtIn, datasource=datasource, enable=enable, hide=hide, iconColor=iconColor, name=name, type=type);

local addRowSchema(collapse, showTitle, title) =
  g.row.new(collapse=collapse, showTitle=showTitle, title=title);

local addSingelStatSchema(datasource, format, title, description, valueName, colorValue, gaugeMaxValue, gaugeShow, sparklineShow, thresholds) =
  g.singlestat.new(datasource=datasource, format=format, title=title, description=description, valueName=valueName, colorValue=colorValue, gaugeMaxValue=gaugeMaxValue, gaugeShow=gaugeShow, sparklineShow=sparklineShow, thresholds=thresholds);

local addPieChartSchema(aliasColors, datasource, description, legendType, pieType, title, valueName) =
  g.pieChartPanel.new(aliasColors=aliasColors, datasource=datasource, description=description, legendType=legendType, pieType=pieType, title=title, valueName=valueName);

local addTableSchema(datasource, description, sort, styles, title, transform) =
  g.tablePanel.new(datasource=datasource, description=description, sort=sort, styles=styles, title=title, transform=transform);

local addStyle(alias, colorMode, colors, dateFormat, decimals, mappingType, pattern, thresholds, type, unit, valueMaps) =
  {'alias': alias, 'colorMode': colorMode, 'colors':colors, 'dateFormat':dateFormat, 'decimals':decimals, 'mappingType':mappingType, 'pattern':pattern, 'thresholds':thresholds, 'type':type, 'unit':unit, 'valueMaps':valueMaps};

{
  "cephfs-overview.json":
    local CephfsOverviewGraphPanel(title, formatY1, labelY1, expr, legendFormat, x, y, w, h) =
      graphPanelSchema({}, title, '', 'null', false, formatY1, 'short', labelY1, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr, 1, 'time_series', legendFormat)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'MDS Performance', '', 'tbO9LAiZz', 'now-1h', '15s', 16, [], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.3.2'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
       addTemplateSchema('mds_servers', '$datasource', 'label_values(ceph_mds_inodes, ceph_daemon)', 1, true, 1, 'MDS Server', '')
    )
    .addPanels([
      addRowSchema(false, true, 'MDS Performance') + {gridPos: {x: 0, y: 0, w: 24, h: 1}},
      CephfsOverviewGraphPanel(
        'MDS Workload - $mds_servers', 'none', 'Reads(-) / Writes (+)', 'sum(rate(ceph_objecter_op_r{ceph_daemon=~\"($mds_servers).*\"}[1m]))', 'Read Ops', 0, 1, 12, 9)
      .addTarget(addTargetSchema('sum(rate(ceph_objecter_op_w{ceph_daemon=~\"($mds_servers).*\"}[1m]))', 1, 'time_series', 'Write Ops'))
      .addSeriesOverride({"alias": "/.*Reads/","transform": "negative-Y"}
      ),
      CephfsOverviewGraphPanel(
        'Client Request Load - $mds_servers', 'none', 'Client Requests', 'ceph_mds_server_handle_client_request{ceph_daemon=~\"($mds_servers).*\"}', '{{ceph_daemon}}', 12, 1, 12, 9
      )
    ])
}