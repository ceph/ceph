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
  "pool-overview.json":
    local PoolOverviewSingleStatPanel(format, title, description, valueName, expr, targetFormat, x, y, w, h) =
      addSingelStatSchema('$datasource', format, title, description, valueName, false, 100, false, false, '')
      .addTarget(addTargetSchema(expr, 1, targetFormat, '')) + {gridPos: {x: x, y: y, w: w, h: h}};

    local PoolOverviewStyle(alias, pattern, type, unit, colorMode, thresholds, valueMaps) =
      addStyle(alias, colorMode, ["rgba(245, 54, 54, 0.9)","rgba(237, 129, 40, 0.89)","rgba(50, 172, 45, 0.97)"], 'YYYY-MM-DD HH:mm:ss', 2, 1, pattern, thresholds, type, unit, valueMaps);  
    
    local PoolOverviewGraphPanel(title, description, formatY1, labelY1, expr, targetFormat, legendFormat, x, y, w, h) =
      graphPanelSchema({}, title, description, 'null as zero', false, formatY1, 'short', labelY1, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr, 1, 'time_series', legendFormat)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'Ceph Pools Overview', '', 'z99hzWtmk', 'now-1h', '15s', 22, [], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'Dashboard1', label='Data Source')
    )
    .addTemplate(
       g.template.custom(label='TopK', name='topk', current='15', query='15')
    )
    .addPanels([
      PoolOverviewSingleStatPanel(
        'none', 'Pools', '', 'avg', 'count(ceph_pool_metadata)', 'table', 0, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'none', 'Pools with Compression', 'Count of the pools that have compression enabled', 'current', 'count(ceph_pool_metadata{compression_mode!=\"none\"})', '', 3, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'bytes', 'Total Raw Capacity', 'Total raw capacity available to the cluster', 'current', 'sum(ceph_osd_stat_bytes)', '', 6, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'bytes', 'Raw Capacity Consumed', 'Total raw capacity consumed by user data and associated overheads (metadata + redundancy)', 'current', 'sum(ceph_pool_bytes_used)', '', 9, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'bytes', 'Logical Stored ', 'Total of client data stored in the cluster', 'current', 'sum(ceph_pool_stored)', '', 12, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'bytes', 'Compression Savings', 'A compression saving is determined as the data eligible to be compressed minus the capacity used to store the data after compression', 'current', 'sum(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used)', '', 15, 0, 3, 3),    
      PoolOverviewSingleStatPanel(
        'percent', 'Compression Eligibility', 'Indicates how suitable the data is within the pools that are/have been enabled for compression - averaged across all pools holding compressed data\n', 'current', '(sum(ceph_pool_compress_under_bytes > 0) / sum(ceph_pool_stored_raw and ceph_pool_compress_under_bytes > 0)) * 100', 'table', 18, 0, 3, 3),
      PoolOverviewSingleStatPanel(
        'none', 'Compression Factor', 'This factor describes the average ratio of data eligible to be compressed divided by the data actually stored. It does not account for data written that was ineligible for compression (too small, or compression yield too low)', 'current', 'sum(ceph_pool_compress_under_bytes > 0) / sum(ceph_pool_compress_bytes_used > 0)', '', 21, 0, 3, 3),
      addTableSchema(
        '$datasource', '', {"col": 5,"desc": true}, [PoolOverviewStyle('', 'Time', 'hidden', 'short', null, [], []),PoolOverviewStyle('', 'instance', 'hidden', 'short', null, [], []),PoolOverviewStyle('', 'job', 'hidden', 'short', null, [], []),PoolOverviewStyle('Pool Name', 'name', 'string', 'short', null, [], []),PoolOverviewStyle('Pool ID', 'pool_id', 'hidden', 'none', null, [], []),PoolOverviewStyle('Compression Factor', 'Value #A', 'number', 'none', null, [], []),PoolOverviewStyle('% Used', 'Value #D', 'number', 'percentunit', 'value', ['70','85'], []),PoolOverviewStyle('Usable Free', 'Value #B', 'number', 'bytes', null, [], []),PoolOverviewStyle('Compression Eligibility', 'Value #C', 'number', 'percent', null, [], []),PoolOverviewStyle('Compression Savings', 'Value #E', 'number', 'bytes', null, [], []),PoolOverviewStyle('Growth (5d)', 'Value #F', 'number', 'bytes', 'value', ['0', '0'], []),PoolOverviewStyle('IOPS', 'Value #G', 'number', 'none', null, [], []),PoolOverviewStyle('Bandwidth', 'Value #H', 'number', 'Bps', null, [], []),PoolOverviewStyle('', '__name__', 'hidden', 'short', null, [], []),PoolOverviewStyle('', 'type', 'hidden', 'short', null, [], []),PoolOverviewStyle('', 'compression_mode', 'hidden', 'short', null, [], []),PoolOverviewStyle('Type', 'description', 'string', 'short', null, [], []),PoolOverviewStyle('Stored', 'Value #J', 'number', 'bytes', null, [], []),PoolOverviewStyle('', 'Value #I', 'hidden', 'short', null, [], []),PoolOverviewStyle('Compression', 'Value #K', 'string', 'short', null, [], [{"text": "ON","value": "1"}])], 'Pool Overview', 'table'
      )
      .addTargets(
        [addTargetSchema('(ceph_pool_compress_under_bytes / ceph_pool_compress_bytes_used > 0) and on(pool_id) (((ceph_pool_compress_under_bytes > 0) / ceph_pool_stored_raw) * 100 > 0.5)', 1, 'table', ''),
        addTargetSchema('ceph_pool_max_avail * on(pool_id) group_left(name) ceph_pool_metadata', 1, 'table', ''),
        addTargetSchema('((ceph_pool_compress_under_bytes > 0) / ceph_pool_stored_raw) * 100', 1, 'table', ''),
        addTargetSchema('(ceph_pool_percent_used * on(pool_id) group_left(name) ceph_pool_metadata)', 1, 'table', ''),
        addTargetSchema('(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used > 0)', 1, 'table', ''),
        addTargetSchema('delta(ceph_pool_stored[5d])', 1, 'table', ''),
        addTargetSchema('rate(ceph_pool_rd[30s]) + rate(ceph_pool_wr[30s])', 1, 'table', ''),
        addTargetSchema('rate(ceph_pool_rd_bytes[30s]) + rate(ceph_pool_wr_bytes[30s])', 1, 'table', ''),
        addTargetSchema('ceph_pool_metadata', 1, 'table', ''),
        addTargetSchema('ceph_pool_stored * on(pool_id) group_left ceph_pool_metadata', 1, 'table', ''),
        addTargetSchema('ceph_pool_metadata{compression_mode!=\"none\"}', 1, 'table', ''),
        addTargetSchema('', '', '', '')]
      ) + {gridPos: {x: 0, y: 3, w: 24, h: 6}},
      PoolOverviewGraphPanel(
        'Top $topk Client IOPS by Pool', 'This chart shows the sum of read and write IOPS from all clients by pool', 'short', 'IOPS', 'topk($topk,round((rate(ceph_pool_rd[30s]) + rate(ceph_pool_wr[30s])),1) * on(pool_id) group_left(instance,name) ceph_pool_metadata) ', 'time_series', '{{name}} ', 0, 9, 12, 8
      )
      .addTarget(
        addTargetSchema('topk($topk,rate(ceph_pool_wr[30s]) + on(pool_id) group_left(instance,name) ceph_pool_metadata) ', 1, 'time_series', '{{name}} - write')
      ),
      PoolOverviewGraphPanel(
        'Top $topk Client Bandwidth by Pool', 'The chart shows the sum of read and write bytes from all clients, by pool', 'Bps', 'Throughput', 'topk($topk,(rate(ceph_pool_rd_bytes[30s]) + rate(ceph_pool_wr_bytes[30s])) * on(pool_id) group_left(instance,name) ceph_pool_metadata)', 'time_series', '{{name}}', 12, 9, 12, 8
      ),
      PoolOverviewGraphPanel(
        'Pool Capacity Usage (RAW)', 'Historical view of capacity usage, to help identify growth and trends in pool consumption', 'bytes', 'Capacity Used', 'ceph_pool_bytes_used * on(pool_id) group_right ceph_pool_metadata', '', '{{name}}', 0, 17, 24, 7
      )
    ])
}
{
  "pool-detail.json":
    local PoolDetailSingleStatPanel(format, title, description, valueName, colorValue, gaugeMaxValue, gaugeShow, sparkLineShow, thresholds, expr, targetFormat, x, y, w, h) =
      addSingelStatSchema('$datasource', format, title, description, valueName, colorValue, gaugeMaxValue, gaugeShow, sparkLineShow, thresholds)
      .addTarget(addTargetSchema(expr, 1, targetFormat, '')) + {gridPos: {x: x, y: y, w: w, h: h}};
    
    local PoolDetailGraphPanel(alias, title, description, formatY1, labelY1, expr, targetFormat, legendFormat, x, y, w, h) =
      graphPanelSchema(alias, title, description, 'null as zero', false, formatY1, 'short', labelY1, null, null, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr, 1, 'time_series', legendFormat)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'Ceph Pool Details', '', '-xyV8KCiz', 'now-1h', '15s', 22, [], '', {refresh_intervals:['5s','10s','15s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
    )
    .addRequired(
       type='grafana', id='grafana', name='Grafana', version='5.3.2'
    )
    .addRequired(
       type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addRequired(
       type='panel', id='singlestat', name='Singlestat', version='5.0.0'
    )
    .addAnnotation(
      addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard')
    )
    .addTemplate(
       g.template.datasource('datasource', 'prometheus', 'Prometheus admin.virt1.home.fajerski.name:9090', label='Data Source')
    )
    .addTemplate(
       addTemplateSchema('pool_name', '$datasource', 'label_values(ceph_pool_metadata,name)', 1, false, 1, 'Pool Name', '')
    )
    .addPanels([
      PoolDetailSingleStatPanel(
        'percentunit', 'Capacity used', '', 'current', true, 1, true, true, '.7,.8', '(ceph_pool_stored / (ceph_pool_stored + ceph_pool_max_avail)) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 'time_series', 0, 0, 7, 7),
      PoolDetailSingleStatPanel(
        's', 'Time till full', 'Time till pool is full assuming the average fill rate of the last 6 hours', false, 100, false, false, '', 'current', '(ceph_pool_max_avail / deriv(ceph_pool_stored[6h])) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"} > 0', 'time_series', 7, 0, 5, 7),
      PoolDetailGraphPanel(
        {"read_op_per_sec": "#3F6833","write_op_per_sec": "#E5AC0E"},'$pool_name Object Ingress/Egress', '', 'ops', 'Objects out(-) / in(+) ', 'deriv(ceph_pool_objects[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 'time_series', 'Objects per second', 12, 0, 12, 7
      ),
      PoolDetailGraphPanel(
        {"read_op_per_sec": "#3F6833","write_op_per_sec": "#E5AC0E"},'$pool_name Client IOPS', '', 'iops', 'Read (-) / Write (+)', 'irate(ceph_pool_rd[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 'time_series', 'reads', 0, 7, 12, 7
      )
      .addSeriesOverride({"alias": "reads","transform": "negative-Y"})
      .addTarget(
        addTargetSchema('irate(ceph_pool_wr[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 1, 'time_series', 'writes')
      ),
      PoolDetailGraphPanel(
        {"read_op_per_sec": "#3F6833","write_op_per_sec": "#E5AC0E"},'$pool_name Client Throughput', '', 'Bps', 'Read (-) / Write (+)', 'irate(ceph_pool_rd_bytes[1m]) + on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 'time_series', 'reads', 12, 7, 12, 7
      )
      .addSeriesOverride({"alias": "reads","transform": "negative-Y"})
      .addTarget(
        addTargetSchema('irate(ceph_pool_wr_bytes[1m]) + on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 1, 'time_series', 'writes')
      ),
      PoolDetailGraphPanel(
        {"read_op_per_sec": "#3F6833","write_op_per_sec": "#E5AC0E"},'$pool_name Objects', '', 'short', 'Objects', 'ceph_pool_objects * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}', 'time_series', 'Number of Objects', 0, 14, 12, 7
      )
    ])
}