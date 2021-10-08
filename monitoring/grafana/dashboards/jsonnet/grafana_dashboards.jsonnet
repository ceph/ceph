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
  "hosts-overview.json":
    local HostsOverviewSingleStatPanel(format, title, description, valueName, expr, targetFormat, x, y, w, h) =
      addSingelStatSchema('$datasource', format, title, description, valueName, false, 100, false, false, '')
      .addTarget(addTargetSchema(expr, 1, targetFormat, '')) + {gridPos: {x: x, y: y, w: w, h: h}};
    
    local HostsOverviewGraphPanel(title, description, formatY1, expr, legendFormat, x, y, w, h) =
      graphPanelSchema({}, title, description, 'null', false, formatY1, 'short', null, null, 0, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr, 1, 'time_series', legendFormat)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'Host Overview', '', 'y0KGL0iZz', 'now-1h', '10s', 16, [], '', {refresh_intervals:['5s','10s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
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
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
       addTemplateSchema('osd_hosts', '$datasource', 'label_values(ceph_disk_occupation, exported_instance)', 1, true, 1, null, '([^.]*).*')
    )
    .addTemplate(
       addTemplateSchema('mon_hosts', '$datasource', 'label_values(ceph_mon_metadata, ceph_daemon)', 1, true, 1, null, 'mon.(.*)')
    )
    .addTemplate(
       addTemplateSchema('mds_hosts', '$datasource', 'label_values(ceph_mds_inodes, ceph_daemon)', 1, true, 1, null, 'mds.(.*)')
    )
    .addTemplate(
       addTemplateSchema('rgw_hosts', '$datasource', 'label_values(ceph_rgw_qlen, ceph_daemon)', 1, true, 1, null, 'rgw.(.*)')
    )
    .addPanels([
      HostsOverviewSingleStatPanel(
        'none', 'OSD Hosts', '', 'current', 'count(sum by (hostname) (ceph_osd_metadata))', 'time_series', 0, 0, 4, 5),
      HostsOverviewSingleStatPanel(
        'percentunit', 'AVG CPU Busy', 'Average CPU busy across all hosts (OSD, RGW, MON etc) within the cluster', 'current', 'avg(\n  1 - (\n    avg by(instance) \n      (irate(node_cpu_seconds_total{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]) or\n       irate(node_cpu{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]))\n    )\n  )', 'time_series', 4, 0, 4, 5),
      HostsOverviewSingleStatPanel(
        'percentunit', 'AVG RAM Utilization', 'Average Memory Usage across all hosts in the cluster (excludes buffer/cache usage)', 'current', 'avg (((node_memory_MemTotal{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_MemTotal_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"})- (\n  (node_memory_MemFree{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_MemFree_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"})  + \n  (node_memory_Cached{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_Cached_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}) + \n  (node_memory_Buffers{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_Buffers_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}) +\n  (node_memory_Slab{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_Slab_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"})\n  )) /\n (node_memory_MemTotal{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"} or node_memory_MemTotal_bytes{instance=~\"($osd_hosts|$rgw_hosts|$mon_hosts|$mds_hosts).*\"} ))', 'time_series', 8, 0, 4, 5),
      HostsOverviewSingleStatPanel(
        'none', 'Physical IOPS', 'IOPS Load at the device as reported by the OS on all OSD hosts', 'current', 'sum ((irate(node_disk_reads_completed{instance=~\"($osd_hosts).*\"}[5m]) or irate(node_disk_reads_completed_total{instance=~\"($osd_hosts).*\"}[5m]) )  + \n(irate(node_disk_writes_completed{instance=~\"($osd_hosts).*\"}[5m]) or irate(node_disk_writes_completed_total{instance=~\"($osd_hosts).*\"}[5m])))', 'time_series', 12, 0, 4, 5),
      HostsOverviewSingleStatPanel(
        'percent', 'AVG Disk Utilization', 'Average Disk utilization for all OSD data devices (i.e. excludes journal/WAL)', 'current', 'avg (\n  label_replace((irate(node_disk_io_time_ms[5m]) / 10 ) or\n   (irate(node_disk_io_time_seconds_total[5m]) * 100), \"instance\", \"$1\", \"instance\", \"([^.:]*).*\"\n  ) *\n  on(instance, device, ceph_daemon) label_replace(label_replace(ceph_disk_occupation{instance=~\"($osd_hosts).*\"}, \"device\", \"$1\", \"device\", \"/dev/(.*)\"), \"instance\", \"$1\", \"instance\", \"([^.:]*).*\")\n)', 'time_series', 16, 0, 4, 5),
      HostsOverviewSingleStatPanel(
        'bytes', 'Network Load', 'Total send/receive network load across all hosts in the ceph cluster', 'current', 'sum (\n  irate(node_network_receive_bytes{instance=~\"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*\",device!=\"lo\"}[1m]) or\n  irate(node_network_receive_bytes_total{instance=~\"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*\",device!=\"lo\"}[1m])\n  ) +\nsum (\n  irate(node_network_transmit_bytes{instance=~\"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*\",device!=\"lo\"}[1m]) or\n  irate(node_network_transmit_bytes_total{instance=~\"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*\",device!=\"lo\"}[1m])\n  )', 'time_series', 20, 0, 4, 5),
      HostsOverviewGraphPanel(
        'CPU Busy - Top 10 Hosts', 'Show the top 10 busiest hosts by cpu', 'percent', 'topk(10,100 * ( 1 - (\n    avg by(instance) \n      (irate(node_cpu_seconds_total{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]) or\n       irate(node_cpu{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]))\n    )\n  )\n)', '{{instance}}', 0, 5, 12, 9),
      HostsOverviewGraphPanel(
        'Network Load - Top 10 Hosts', 'Top 10 hosts by network load', 'Bps', 'topk(10, (sum by(instance) (\n  (\n  irate(node_network_receive_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\",device!=\"lo\"}[1m]) or\n  irate(node_network_receive_bytes_total{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\",device!=\"lo\"}[1m])\n  ) +\n  (\n  irate(node_network_transmit_bytes{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\",device!=\"lo\"}[1m]) or\n  irate(node_network_transmit_bytes_total{instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\",device!=\"lo\"}[1m])\n  ))\n  )\n)', '{{instance}}', 12, 5, 12, 9),    
    ])
}
{
  "host-details.json":
    local HostDetailsSingleStatPanel(format, title, description, valueName, expr, targetFormat, x, y, w, h) =
      addSingelStatSchema('$datasource', format, title, description, valueName, false, 100, false, false, '')
      .addTarget(addTargetSchema(expr, 1, targetFormat, '')) + {gridPos: {x: x, y: y, w: w, h: h}};
    
    local HostDetailsGraphPanel(alias, title, description, nullPointMode, formatY1, labelY1, expr, legendFormat, x, y, w, h) =
      graphPanelSchema(alias, title, description, nullPointMode, false, formatY1, 'short', labelY1, null, null, 1, '$datasource')
      .addTargets(
        [addTargetSchema(expr, 1, 'time_series', legendFormat)]) + {gridPos: {x: x, y: y, w: w, h: h}};

    dashboardSchema(
      'Host Details', '', 'rtOg0AiWz', 'now-1h', '10s', 16, ['overview'], '', {refresh_intervals:['5s','10s','30s','1m','5m','15m','30m','1h','2h','1d'],time_options:['5m','15m','1h','6h','12h','24h','2d','7d','30d']}
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
       g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
       addTemplateSchema('ceph_hosts', '$datasource', 'label_values(node_scrape_collector_success, instance) ', 1, false, 3, 'Hostname', '([^.:]*).*')
    )
    .addPanels([
      addRowSchema(false, true, '$ceph_hosts System Overview') + {gridPos: {x: 0, y: 0, w: 24, h: 1}},
      HostDetailsSingleStatPanel(
        'none', 'OSDs', '', 'current', 'count(sum by (ceph_daemon) (ceph_osd_metadata{hostname=\'$ceph_hosts\'}))', 'time_series', 0, 1, 3, 5
      ),
      HostDetailsGraphPanel(
        {"interrupt": "#447EBC","steal": "#6D1F62","system": "#890F02","user": "#3F6833","wait": "#C15C17"},'CPU Utilization', 'Shows the CPU breakdown. When multiple servers are selected, only the first host\'s cpu data is shown', 'null', 'percent', '% Utilization', 'sum by (mode) (\n  irate(node_cpu{instance=~\"($ceph_hosts)([\\\\.:].*)?\", mode=~\"(irq|nice|softirq|steal|system|user|iowait)\"}[1m]) or\n  irate(node_cpu_seconds_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\", mode=~\"(irq|nice|softirq|steal|system|user|iowait)\"}[1m])\n) / scalar(\n  sum(irate(node_cpu{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[1m]) or\n      irate(node_cpu_seconds_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[1m]))\n) * 100', '{{mode}}', 3, 1, 6, 10
      ),
      HostDetailsGraphPanel(
        {"Available": "#508642","Free": "#508642","Total": "#bf1b00","Used": "#bf1b00","total": "#bf1b00","used": "#0a50a1"},'RAM Usage', '', 'null', 'bytes', 'RAM used', 'node_memory_MemFree{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_MemFree_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"} ', 'Free', 9, 1, 6, 10)
      .addTargets(
        [addTargetSchema('node_memory_MemTotal{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_MemTotal_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"} ', 1, 'time_series', 'total'),
        addTargetSchema('(node_memory_Cached{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Cached_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"}) + \n(node_memory_Buffers{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Buffers_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"}) +\n(node_memory_Slab{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Slab_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"}) \n', 1, 'time_series', 'buffers/cache'),
        addTargetSchema('(node_memory_MemTotal{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_MemTotal_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"})- (\n  (node_memory_MemFree{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_MemFree_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"})  + \n  (node_memory_Cached{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Cached_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"}) + \n  (node_memory_Buffers{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Buffers_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"}) +\n  (node_memory_Slab{instance=~\"$ceph_hosts([\\\\.:].*)?\"} or node_memory_Slab_bytes{instance=~\"$ceph_hosts([\\\\.:].*)?\"})\n  )\n  \n', 1, 'time_series', 'used')])
      .addSeriesOverride({"alias": "total","color": "#bf1b00","fill": 0,"linewidth": 2,"stack": false}
      ),
      HostDetailsGraphPanel(
        {},'Network Load', 'Show the network load (rx,tx) across all interfaces (excluding loopback \'lo\')', 'null', 'decbytes', 'Send (-) / Receive (+)', 'sum by (device) (\n  irate(node_network_receive_bytes{instance=~\"($ceph_hosts)([\\\\.:].*)?\",device!=\"lo\"}[1m]) or \n  irate(node_network_receive_bytes_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\",device!=\"lo\"}[1m])\n)', '{{device}}.rx', 15, 1, 6, 10)
      .addTargets(
        [addTargetSchema('sum by (device) (\n  irate(node_network_transmit_bytes{instance=~\"($ceph_hosts)([\\\\.:].*)?\",device!=\"lo\"}[1m]) or\n  irate(node_network_transmit_bytes_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\",device!=\"lo\"}[1m])\n)', 1, 'time_series', '{{device}}.tx')])
      .addSeriesOverride({"alias": "/.*tx/","transform": "negative-Y"}
      ),
      HostDetailsGraphPanel(
        {},'Network drop rate', '', 'null', 'pps', 'Send (-) / Receive (+)', 'irate(node_network_receive_drop{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m]) or irate(node_network_receive_drop_total{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m])', '{{device}}.rx', 21, 1, 3, 5)
      .addTargets(
        [addTargetSchema('irate(node_network_transmit_drop{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m]) or irate(node_network_transmit_drop_total{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m])', 1, 'time_series', '{{device}}.tx')])
      .addSeriesOverride({"alias": "/.*tx/","transform": "negative-Y"}
      ),
      HostDetailsSingleStatPanel(
        'bytes', 'Raw Capacity', 'Each OSD consists of a Journal/WAL partition and a data partition. The RAW Capacity shown is the sum of the data partitions across all OSDs on the selected OSD hosts.', 'current', 'sum(ceph_osd_stat_bytes and on (ceph_daemon) ceph_disk_occupation{instance=~\"($ceph_hosts)([\\\\.:].*)?\"})', 'time_series', 0, 6, 3, 5
      ),
      HostDetailsGraphPanel(
        {},'Network error rate', '', 'null', 'pps', 'Send (-) / Receive (+)', 'irate(node_network_receive_errs{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m]) or irate(node_network_receive_errs_total{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m])', '{{device}}.rx', 21, 6, 3, 5)
      .addTargets(
        [addTargetSchema('irate(node_network_transmit_errs{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m]) or irate(node_network_transmit_errs_total{instance=~\"$ceph_hosts([\\\\.:].*)?\"}[1m])', 1, 'time_series', '{{device}}.tx')])
      .addSeriesOverride({"alias": "/.*tx/","transform": "negative-Y"}
      ),
      addRowSchema(false, true, 'OSD Disk Performance Statistics') + {gridPos: {x: 0, y: 11, w: 24, h: 1}},
      HostDetailsGraphPanel(
        {},'$ceph_hosts Disk IOPS', 'For any OSD devices on the host, this chart shows the iops per physical device. Each device is shown by it\'s name and corresponding OSD id value', 'connected', 'ops', 'Read (-) / Write (+)', 'label_replace(\n  (\n    irate(node_disk_writes_completed{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) or\n    irate(node_disk_writes_completed_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m])\n  ),\n  \"instance\",\n  \"$1\",\n  \"instance\",\n  \"([^:.]*).*\"\n)\n* on(instance, device, ceph_daemon) group_left\n  label_replace(\n    label_replace(\n      ceph_disk_occupation,\n      \"device\",\n      \"$1\",\n      \"device\",\n      \"/dev/(.*)\"\n    ),\n    \"instance\",\n    \"$1\",\n    \"instance\",\n    \"([^:.]*).*\"\n  )', '{{device}}({{ceph_daemon}}) writes', 0, 12, 11, 9)
      .addTargets(
        [addTargetSchema('label_replace(\n    (irate(node_disk_reads_completed{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) or irate(node_disk_reads_completed_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m])),\n    \"instance\",\n    \"$1\",\n    \"instance\",\n    \"([^:.]*).*\"\n)\n* on(instance, device, ceph_daemon) group_left\n  label_replace(\n    label_replace(\n      ceph_disk_occupation,\n      \"device\",\n      \"$1\",\n      \"device\",\n      \"/dev/(.*)\"\n    ),\n    \"instance\",\n    \"$1\",\n    \"instance\",\n    \"([^:.]*).*\"\n  )', 1, 'time_series', '{{device}}({{ceph_daemon}}) reads')])
      .addSeriesOverride({"alias": "/.*reads/","transform": "negative-Y"}
      ),
      HostDetailsGraphPanel(
        {},'$ceph_hosts Throughput by Disk', 'For OSD hosts, this chart shows the disk bandwidth (read bytes/sec + write bytes/sec) of the physical OSD device. Each device is shown by device name, and corresponding OSD id', 'connected', 'Bps', 'Read (-) / Write (+)', 'label_replace((irate(node_disk_bytes_written{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) or irate(node_disk_written_bytes_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m])), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\") * on(instance, device, ceph_daemon) group_left label_replace(label_replace(ceph_disk_occupation, \"device\", \"$1\", \"device\", \"/dev/(.*)\"), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\")', '{{device}}({{ceph_daemon}}) write', 12, 12, 11, 9)
      .addTargets(
        [addTargetSchema('label_replace((irate(node_disk_bytes_read{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) or irate(node_disk_read_bytes_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m])), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\") * on(instance, device, ceph_daemon) group_left label_replace(label_replace(ceph_disk_occupation, \"device\", \"$1\", \"device\", \"/dev/(.*)\"), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\")', 1, 'time_series', '{{device}}({{ceph_daemon}}) read')])
      .addSeriesOverride({"alias": "/.*read/","transform": "negative-Y"}
      ),
      HostDetailsGraphPanel(
        {},'$ceph_hosts Disk Latency', 'For OSD hosts, this chart shows the latency at the physical drive. Each drive is shown by device name, with it\'s corresponding OSD id', 'null as zero', 's', '', 'max by(instance,device) (label_replace((irate(node_disk_write_time_seconds_total{ instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) )  / clamp_min(irate(node_disk_writes_completed_total{ instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]), 0.001) or   (irate(node_disk_read_time_seconds_total{ instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) )  / clamp_min(irate(node_disk_reads_completed_total{ instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]), 0.001), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\")) *  on(instance, device, ceph_daemon) group_left label_replace(label_replace(ceph_disk_occupation{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}, \"device\", \"$1\", \"device\", \"/dev/(.*)\"), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\")', '{{device}}({{ceph_daemon}})', 0, 21, 11, 9
      ),
      HostDetailsGraphPanel(
        {},'$ceph_hosts Disk utilization', 'Show disk utilization % (util) of any OSD devices on the host by the physical device name and associated OSD id.', 'connected', 'percent', '%Util', 'label_replace(((irate(node_disk_io_time_ms{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) / 10 ) or  irate(node_disk_io_time_seconds_total{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}[5m]) * 100), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\") * on(instance, device, ceph_daemon) group_left label_replace(label_replace(ceph_disk_occupation{instance=~\"($ceph_hosts)([\\\\.:].*)?\"}, \"device\", \"$1\", \"device\", \"/dev/(.*)\"), \"instance\", \"$1\", \"instance\", \"([^:.]*).*\")', '{{device}}({{ceph_daemon}})', 12, 21, 11, 9
      )
    ])
}
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
