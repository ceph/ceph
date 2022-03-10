local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

{
  grafanaDashboards+:: {
    'radosgw-sync-overview.json':
      local RgwSyncOverviewPanel(title, formatY1, labelY1, rgwMetric, x, y, w, h) =
        u.graphPanelSchema({},
                           title,
                           '',
                           'null as zero',
                           true,
                           formatY1,
                           'short',
                           labelY1,
                           null,
                           0,
                           1,
                           '$datasource')
        .addTargets(
          [u.addTargetSchema('sum by (source_zone) (rate(%s[30s]))' % rgwMetric,
                             '{{source_zone}}')]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'RGW Sync Overview',
        '',
        'rgw-sync-overview',
        'now-1h',
        '15s',
        16,
        ['overview'],
        '',
        {
          refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
      )
      .addAnnotation(
        u.addAnnotationSchema(
          1,
          '-- Grafana --',
          true,
          true,
          'rgba(0, 211, 255, 1)',
          'Annotations & Alerts',
          'dashboard'
        )
      )
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.0.0'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addTemplate(
        u.addTemplateSchema('rgw_servers', '$datasource', 'prometehus', 1, true, 1, '', '')
      )
      .addTemplate(
        g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
      )
      .addPanels([
        RgwSyncOverviewPanel(
          'Replication (throughput) from Source Zone',
          'Bps',
          null,
          'ceph_data_sync_from_zone_fetch_bytes_sum',
          0,
          0,
          8,
          7
        ),
        RgwSyncOverviewPanel(
          'Replication (objects) from Source Zone',
          'short',
          'Objects/s',
          'ceph_data_sync_from_zone_fetch_bytes_count',
          8,
          0,
          8,
          7
        ),
        RgwSyncOverviewPanel(
          'Polling Request Latency from Source Zone',
          'ms',
          null,
          'ceph_data_sync_from_zone_poll_latency_sum',
          16,
          0,
          8,
          7
        ),
        RgwSyncOverviewPanel(
          'Unsuccessful Object Replications from Source Zone',
          'short',
          'Count/s',
          'ceph_data_sync_from_zone_fetch_errors',
          0,
          7,
          8,
          7
        ),
      ]),
    'radosgw-overview.json':
      local RgwOverviewPanel(
        title,
        description,
        formatY1,
        formatY2,
        expr1,
        legendFormat1,
        x,
        y,
        w,
        h,
        datasource='$datasource',
        legend_alignAsTable=false,
        legend_avg=false,
        legend_min=false,
        legend_max=false,
        legend_current=false,
        legend_values=false
            ) =
        u.graphPanelSchema(
          {},
          title,
          description,
          'null',
          false,
          formatY1,
          formatY2,
          null,
          null,
          0,
          1,
          datasource,
          legend_alignAsTable,
          legend_avg,
          legend_min,
          legend_max,
          legend_current,
          legend_values
        )
        .addTargets(
          [u.addTargetSchema(expr1, legendFormat1)]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'RGW Overview',
        '',
        'WAkugZpiz',
        'now-1h',
        '15s',
        16,
        ['overview'],
        '',
        {
          refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
      )
      .addAnnotation(
        u.addAnnotationSchema(
          1,
          '-- Grafana --',
          true,
          true,
          'rgba(0, 211, 255, 1)',
          'Annotations & Alerts',
          'dashboard'
        )
      )
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.0.0'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addTemplate(
        u.addTemplateSchema(
          'rgw_servers',
          '$datasource',
          'label_values(ceph_rgw_metadata, ceph_daemon)',
          1,
          true,
          1,
          '',
          ''
        )
      )
      .addTemplate(
        u.addTemplateSchema(
          'code',
          '$datasource',
          'label_values(haproxy_server_http_responses_total{instance=~"$ingress_service"}, code)',
          1,
          true,
          1,
          'HTTP Code',
          ''
        )
      )
      .addTemplate(
        u.addTemplateSchema(
          'ingress_service',
          '$datasource',
          'label_values(haproxy_server_status, instance)',
          1,
          true,
          1,
          'Ingress Service',
          ''
        )
      )
      .addTemplate(
        g.template.datasource('datasource',
                              'prometheus',
                              'default',
                              label='Data Source')
      )
      .addPanels([
        u.addRowSchema(false,
                       true,
                       'RGW Overview - All Gateways') +
        {
          gridPos: { x: 0, y: 0, w: 24, h: 1 },
        },
        RgwOverviewPanel(
          'Average GET/PUT Latencies',
          '',
          's',
          'short',
          'rate(ceph_rgw_get_initial_lat_sum[30s]) / rate(ceph_rgw_get_initial_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata',
          'GET AVG',
          0,
          1,
          8,
          7
        ).addTargets(
          [
            u.addTargetSchema(
              'rate(ceph_rgw_put_initial_lat_sum[30s]) / rate(ceph_rgw_put_initial_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata',
              'PUT AVG'
            ),
          ]
        ),
        RgwOverviewPanel(
          'Total Requests/sec by RGW Instance',
          '',
          'none',
          'short',
          'sum by (rgw_host) (label_replace(rate(ceph_rgw_req[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"))',
          '{{rgw_host}}',
          8,
          1,
          7,
          7
        ),
        RgwOverviewPanel(
          'GET Latencies by RGW Instance',
          'Latencies are shown stacked, without a yaxis to provide a visual indication of GET latency imbalance across RGW hosts',
          's',
          'short',
          'label_replace(\n    rate(ceph_rgw_get_initial_lat_sum[30s]) /\n    rate(ceph_rgw_get_initial_lat_count[30s]) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata,\n"rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
          '{{rgw_host}}',
          15,
          1,
          6,
          7
        ),
        RgwOverviewPanel(
          'Bandwidth Consumed by Type',
          'Total bytes transferred in/out of all radosgw instances within the cluster',
          'bytes',
          'short',
          'sum(rate(ceph_rgw_get_b[30s]))',
          'GETs',
          0,
          8,
          8,
          6
        ).addTargets(
          [u.addTargetSchema('sum(rate(ceph_rgw_put_b[30s]))',
                             'PUTs')]
        ),
        RgwOverviewPanel(
          'Bandwidth by RGW Instance',
          'Total bytes transferred in/out through get/put operations, by radosgw instance',
          'bytes',
          'short',
          'label_replace(sum by (instance_id) (\n    rate(ceph_rgw_get_b[30s]) + \n    rate(ceph_rgw_put_b[30s])\n) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
          '{{rgw_host}}',
          8,
          8,
          7,
          6
        ),
        RgwOverviewPanel(
          'PUT Latencies by RGW Instance',
          'Latencies are shown stacked, without a yaxis to provide a visual indication of PUT latency imbalance across RGW hosts',
          's',
          'short',
          'label_replace(\n    rate(ceph_rgw_put_initial_lat_sum[30s]) /\n    rate(ceph_rgw_put_initial_lat_count[30s]) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata,\n"rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
          '{{rgw_host}}',
          15,
          8,
          6,
          6
        ),
        u.addRowSchema(
          false, true, 'RGW Overview - HAProxy Metrics'
        ) + { gridPos: { x: 0, y: 12, w: 9, h: 12 } },
        RgwOverviewPanel(
          'Total responses by HTTP code',
          '',
          'short',
          'short',
          'sum(irate(haproxy_frontend_http_responses_total{code=~"$code",instance=~"$ingress_service",proxy=~"frontend"}[5m])) by (code)',
          'Frontend {{ code }}',
          0,
          12,
          5,
          12,
          '$datasource',
          true,
          true,
          true,
          true,
          true,
          true
        )
        .addTargets(
          [u.addTargetSchema('sum(irate(haproxy_backend_http_responses_total{code=~"$code",instance=~"$ingress_service",proxy=~"backend"}[5m])) by (code)', 'Backend {{ code }}')]
        )
        .addSeriesOverride([
          {
            alias: '/.*Back.*/',
            transform: 'negative-Y',
          },
          { alias: '/.*1.*/' },
          { alias: '/.*2.*/' },
          { alias: '/.*3.*/' },
          { alias: '/.*4.*/' },
          { alias: '/.*5.*/' },
          { alias: '/.*other.*/' },
        ]),
        RgwOverviewPanel(
          'Total requests / responses',
          '',
          'short',
          'short',
          'sum(irate(haproxy_frontend_http_requests_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])) by (instance)',
          'Requests',
          5,
          12,
          5,
          12,
          '$datasource',
          true,
          true,
          true,
          true,
          true,
          true
        )
        .addTargets(
          [
            u.addTargetSchema('sum(irate(haproxy_backend_response_errors_total{proxy=~"backend",instance=~"$ingress_service"}[5m])) by (instance)', 'Response errors', 'time_series', 2),
            u.addTargetSchema('sum(irate(haproxy_frontend_request_errors_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])) by (instance)', 'Requests errors'),
            u.addTargetSchema('sum(irate(haproxy_backend_redispatch_warnings_total{proxy=~"backend",instance=~"$ingress_service"}[5m])) by (instance)', 'Backend redispatch', 'time_series', 2),
            u.addTargetSchema('sum(irate(haproxy_backend_retry_warnings_total{proxy=~"backend",instance=~"$ingress_service"}[5m])) by (instance)', 'Backend retry', 'time_series', 2),
            u.addTargetSchema('sum(irate(haproxy_frontend_requests_denied_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])) by (instance)', 'Request denied', 'time_series', 2),
            u.addTargetSchema('sum(haproxy_backend_current_queue{proxy=~"backend",instance=~"$ingress_service"}) by (instance)', 'Backend Queued', 'time_series', 2),
          ]
        )
        .addSeriesOverride([
          {
            alias: '/.*Response.*/',
            transform: 'negative-Y',
          },
          {
            alias: '/.*Backend.*/',
            transform: 'negative-Y',
          },
        ]),
        RgwOverviewPanel(
          'Total number of connections',
          '',
          'short',
          'short',
          'sum(irate(haproxy_frontend_connections_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])) by (instance)',
          'Front',
          10,
          12,
          5,
          12,
          '$datasource',
          true,
          true,
          true,
          true,
          true,
          true
        )
        .addTargets(
          [
            u.addTargetSchema('sum(irate(haproxy_backend_connection_attempts_total{proxy=~"backend",instance=~"$ingress_service"}[5m])) by (instance)', 'Back'),
            u.addTargetSchema('sum(irate(haproxy_backend_connection_errors_total{proxy=~"backend",instance=~"$ingress_service"}[5m])) by (instance)', 'Back errors'),
          ]
        )
        .addSeriesOverride([
          {
            alias: '/.*Back.*/',
            transform: 'negative-Y',
          },
        ]),
        RgwOverviewPanel(
          'Current total of incoming / outgoing bytes',
          '',
          'short',
          'short',
          'sum(irate(haproxy_frontend_bytes_in_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])*8) by (instance)',
          'IN Front',
          15,
          12,
          6,
          12,
          '$datasource',
          true,
          true,
          true,
          true,
          true,
          true
        )
        .addTargets(
          [
            u.addTargetSchema('sum(irate(haproxy_frontend_bytes_out_total{proxy=~"frontend",instance=~"$ingress_service"}[5m])*8) by (instance)', 'OUT Front', 'time_series', 2),
            u.addTargetSchema('sum(irate(haproxy_backend_bytes_in_total{proxy=~"backend",instance=~"$ingress_service"}[5m])*8) by (instance)', 'IN Back', 'time_series', 2),
            u.addTargetSchema('sum(irate(haproxy_backend_bytes_out_total{proxy=~"backend",instance=~"$ingress_service"}[5m])*8) by (instance)', 'OUT Back', 'time_series', 2),
          ]
        )
        .addSeriesOverride([
          {
            alias: '/.*OUT.*/',
            transform: 'negative-Y',
          },
        ]),
      ]),
    'radosgw-detail.json':
      local RgwDetailsPanel(aliasColors,
                            title,
                            description,
                            formatY1,
                            formatY2,
                            expr1,
                            expr2,
                            legendFormat1,
                            legendFormat2,
                            x,
                            y,
                            w,
                            h) =
        u.graphPanelSchema(aliasColors,
                           title,
                           description,
                           'null',
                           false,
                           formatY1,
                           formatY2,
                           null,
                           null,
                           0,
                           1,
                           '$datasource')
        .addTargets(
          [u.addTargetSchema(expr1, legendFormat1), u.addTargetSchema(expr2, legendFormat2)]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'RGW Instance Detail',
        '',
        'x5ARzZtmk',
        'now-1h',
        '15s',
        16,
        ['overview'],
        '',
        {
          refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
      )
      .addAnnotation(
        u.addAnnotationSchema(
          1,
          '-- Grafana --',
          true,
          true,
          'rgba(0, 211, 255, 1)',
          'Annotations & Alerts',
          'dashboard'
        )
      )
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.0.0'
      )
      .addRequired(
        type='panel',
        id='grafana-piechart-panel',
        name='Pie Chart',
        version='1.3.3'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addTemplate(
        g.template.datasource('datasource',
                              'prometheus',
                              'default',
                              label='Data Source')
      )
      .addTemplate(
        u.addTemplateSchema('rgw_servers',
                            '$datasource',
                            'label_values(ceph_rgw_metadata, ceph_daemon)',
                            1,
                            true,
                            1,
                            '',
                            '')
      )
      .addPanels([
        u.addRowSchema(false, true, 'RGW Host Detail : $rgw_servers') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
        RgwDetailsPanel(
          {},
          '$rgw_servers GET/PUT Latencies',
          '',
          's',
          'short',
          'sum by (instance_id) (rate(ceph_rgw_get_initial_lat_sum[30s]) / rate(ceph_rgw_get_initial_lat_count[30s])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'sum by (instance_id) (rate(ceph_rgw_put_initial_lat_sum[30s]) / rate(ceph_rgw_put_initial_lat_count[30s])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'GET {{ceph_daemon}}',
          'PUT {{ceph_daemon}}',
          0,
          1,
          6,
          8
        ),
        RgwDetailsPanel(
          {},
          'Bandwidth by HTTP Operation',
          '',
          'bytes',
          'short',
          'rate(ceph_rgw_get_b[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'rate(ceph_rgw_put_b[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'GETs {{ceph_daemon}}',
          'PUTs {{ceph_daemon}}',
          6,
          1,
          7,
          8
        ),
        RgwDetailsPanel(
          {
            GETs: '#7eb26d',
            Other: '#447ebc',
            PUTs: '#eab839',
            Requests: '#3f2b5b',
            'Requests Failed': '#bf1b00',
          },
          'HTTP Request Breakdown',
          '',
          'short',
          'short',
          'rate(ceph_rgw_failed_req[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'rate(ceph_rgw_get[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'Requests Failed {{ceph_daemon}}',
          'GETs {{ceph_daemon}}',
          13,
          1,
          7,
          8
        )
        .addTargets(
          [
            u.addTargetSchema(
              'rate(ceph_rgw_put[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
              'PUTs {{ceph_daemon}}'
            ),
            u.addTargetSchema(
              '(\n    rate(ceph_rgw_req[30s]) -\n    (rate(ceph_rgw_get[30s]) + rate(ceph_rgw_put[30s]))\n) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
              'Other {{ceph_daemon}}'
            ),
          ]
        ),
        u.addPieChartSchema(
          {
            GETs: '#7eb26d',
            'Other (HEAD,POST,DELETE)': '#447ebc',
            PUTs: '#eab839',
            Requests: '#3f2b5b',
            Failures: '#bf1b00',
          }, '$datasource', '', 'Under graph', 'pie', 'Workload Breakdown', 'current'
        )
        .addTarget(u.addTargetSchema(
          'rate(ceph_rgw_failed_req[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'Failures {{ceph_daemon}}'
        ))
        .addTarget(u.addTargetSchema(
          'rate(ceph_rgw_get[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'GETs {{ceph_daemon}}'
        ))
        .addTarget(u.addTargetSchema(
          'rate(ceph_rgw_put[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'PUTs {{ceph_daemon}}'
        ))
        .addTarget(u.addTargetSchema(
          '(\n    rate(ceph_rgw_req[30s]) -\n    (rate(ceph_rgw_get[30s]) + rate(ceph_rgw_put[30s]))\n) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers"}',
          'Other (DELETE,LIST) {{ceph_daemon}}'
        )) + { gridPos: { x: 20, y: 1, w: 4, h: 8 } },
      ]),
  },
}
