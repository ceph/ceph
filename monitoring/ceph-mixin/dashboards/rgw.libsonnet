local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

(import 'utils.libsonnet') {
  'radosgw-sync-overview.json':
    local RgwSyncOverviewPanel(title, formatY1, labelY1, rgwMetric, x, y, w, h) =
      $.graphPanelSchema({},
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
        [
          $.addTargetSchema(
            'sum by (source_zone) (rate(%(rgwMetric)s{%(matchers)s}[$__rate_interval]))'
            % ($.matchers() + { rgwMetric: rgwMetric }),
            '{{source_zone}}'
          ),
        ]
      ) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: formatY1, custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RGW Sync Overview',
      '',
      'rgw-sync-overview',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
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
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema(
        'rgw_servers',
        '$datasource',
        'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
        1,
        true,
        1,
        '',
        'RGW Server'
      )
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
      $.graphPanelSchema(
        {},
        title,
        description,
        'null as zero',
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
        [$.addTargetSchema(expr1, legendFormat1)]
      ) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: formatY1, custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RGW Overview',
      '',
      'WAkugZpiz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
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
      g.template.datasource('datasource',
                            'prometheus',
                            'default',
                            label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema(
        'rgw_servers',
        '$datasource',
        'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
        1,
        true,
        1,
        '',
        'RGW Server'
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'code',
        '$datasource',
        'label_values(haproxy_server_http_responses_total{job=~"$job_haproxy", instance=~"$ingress_service"}, code)',
        1,
        true,
        1,
        'HTTP Code',
        ''
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'job_haproxy',
        '$datasource',
        'label_values(haproxy_server_status, job)',
        1,
        true,
        1,
        'job haproxy',
        '(.*)',
        multi=true,
        allValues='.+',
      ),
    )
    .addTemplate(
      $.addTemplateSchema(
        'ingress_service',
        '$datasource',
        'label_values(haproxy_server_status{job=~"$job_haproxy"}, instance)',
        1,
        true,
        1,
        'Ingress Service',
        ''
      )
    )
    .addPanels([
      $.addRowSchema(false,
                     true,
                     'RGW Overview - All Gateways') +
      {
        gridPos: { x: 0, y: 0, w: 24, h: 1 },
      },
      RgwOverviewPanel(
        'Average GET/PUT Latencies by RGW Instance',
        '',
        's',
        'short',
        |||
          label_replace(
            rate(ceph_rgw_get_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
              rate(ceph_rgw_get_initial_lat_count{%(matchers)s}[$__rate_interval]) *
              on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
            "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
          )
        ||| % $.matchers(),
        'GET {{rgw_host}}',
        0,
        1,
        8,
        7
      ).addTargets(
        [
          $.addTargetSchema(
            |||
              label_replace(
                rate(ceph_rgw_put_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
                  rate(ceph_rgw_put_initial_lat_count{%(matchers)s}[$__rate_interval]) *
                  on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
                "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
              )
            ||| % $.matchers(),
            'PUT {{rgw_host}}'
          ),
        ]
      ),
      RgwOverviewPanel(
        'Total Requests/sec by RGW Instance',
        '',
        'none',
        'short',
        |||
          sum by (rgw_host) (
            label_replace(
              rate(ceph_rgw_req{%(matchers)s}[$__rate_interval]) *
                on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
              "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
            )
          )
        ||| % $.matchers(),
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
        |||
          label_replace(
            rate(ceph_rgw_get_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
              rate(ceph_rgw_get_initial_lat_count{%(matchers)s}[$__rate_interval]) *
              on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
            "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
          )
        ||| % $.matchers(),
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
        'sum(rate(ceph_rgw_get_b{%(matchers)s}[$__rate_interval]))' % $.matchers(),
        'GETs',
        0,
        8,
        8,
        6
      ).addTargets(
        [$.addTargetSchema('sum(rate(ceph_rgw_put_b{%(matchers)s}[$__rate_interval]))' % $.matchers(),
                           'PUTs')]
      ),
      RgwOverviewPanel(
        'Bandwidth by RGW Instance',
        'Total bytes transferred in/out through get/put operations, by radosgw instance',
        'bytes',
        'short',
        |||
          label_replace(sum by (instance_id) (
            rate(ceph_rgw_get_b{%(matchers)s}[$__rate_interval]) +
              rate(ceph_rgw_put_b{%(matchers)s}[$__rate_interval])) *
              on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
            "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
          )
        ||| % $.matchers(),
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
        |||
          label_replace(
            rate(ceph_rgw_put_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
              rate(ceph_rgw_put_initial_lat_count{%(matchers)s}[$__rate_interval]) *
              on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s},
            "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"
          )
        ||| % $.matchers(),
        '{{rgw_host}}',
        15,
        8,
        6,
        6
      ),
      $.addRowSchema(
        false, true, 'RGW Overview - HAProxy Metrics'
      ) + { gridPos: { x: 0, y: 12, w: 9, h: 12 } },
      RgwOverviewPanel(
        'Total responses by HTTP code',
        '',
        'short',
        'short',
        |||
          sum(
            rate(
              haproxy_frontend_http_responses_total{code=~"$code", job=~"$job_haproxy", instance=~"$ingress_service", proxy=~"frontend"}[$__rate_interval]
            )
          ) by (code)
        |||,
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
        [
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_http_responses_total{code=~"$code", job=~"$job_haproxy", instance=~"$ingress_service", proxy=~"backend"}[$__rate_interval]
                )
              ) by (code)
            |||, 'Backend {{ code }}'
          ),
        ]
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
        |||
          sum(
            rate(
              haproxy_frontend_http_requests_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
            )
          ) by (instance)
        |||,
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
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_response_errors_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Response errors', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_frontend_request_errors_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Requests errors'
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_redispatch_warnings_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Backend redispatch', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_retry_warnings_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Backend retry', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_frontend_requests_denied_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Request denied', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                haproxy_backend_current_queue{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}
              ) by (instance)
            |||, 'Backend Queued', 'time_series', 2
          ),
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
        |||
          sum(
            rate(
              haproxy_frontend_connections_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
            )
          ) by (instance)
        |||,
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
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_connection_attempts_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Back'
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_connection_errors_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                )
              ) by (instance)
            |||, 'Back errors'
          ),
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
        |||
          sum(
            rate(
              haproxy_frontend_bytes_in_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
            ) * 8
          ) by (instance)
        |||,
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
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_frontend_bytes_out_total{proxy=~"frontend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                ) * 8
              ) by (instance)
            |||, 'OUT Front', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_bytes_in_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                ) * 8
              ) by (instance)
            |||, 'IN Back', 'time_series', 2
          ),
          $.addTargetSchema(
            |||
              sum(
                rate(
                  haproxy_backend_bytes_out_total{proxy=~"backend", job=~"$job_haproxy", instance=~"$ingress_service"}[$__rate_interval]
                ) * 8
              ) by (instance)
            |||, 'OUT Back', 'time_series', 2
          ),
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
      $.graphPanelSchema(aliasColors,
                         title,
                         description,
                         'null as zero',
                         false,
                         formatY1,
                         formatY2,
                         null,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [$.addTargetSchema(expr1, legendFormat1), $.addTargetSchema(expr2, legendFormat2)]
      ) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: formatY1, custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RGW Instance Detail',
      '',
      'x5ARzZtmk',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
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
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('rgw_servers',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          true,
                          1,
                          '',
                          '')
    )
    .addPanels([
      $.addRowSchema(false, true, 'RGW Host Detail : $rgw_servers') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      RgwDetailsPanel(
        {},
        '$rgw_servers GET/PUT Latencies',
        '',
        's',
        'short',
        |||
          sum by (instance_id) (
            rate(ceph_rgw_get_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
              rate(ceph_rgw_get_initial_lat_count{%(matchers)s}[$__rate_interval])
          ) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        |||
          sum by (instance_id) (
            rate(ceph_rgw_put_initial_lat_sum{%(matchers)s}[$__rate_interval]) /
              rate(ceph_rgw_put_initial_lat_count{%(matchers)s}[$__rate_interval])
          ) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
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
        |||
          rate(ceph_rgw_get_b{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        |||
          rate(ceph_rgw_put_b{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon)
            ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
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
        |||
          rate(ceph_rgw_failed_req{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s,ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        |||
          rate(ceph_rgw_get{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        'Requests Failed {{ceph_daemon}}',
        'GETs {{ceph_daemon}}',
        13,
        1,
        7,
        8
      )
      .addTargets(
        [
          $.addTargetSchema(
            |||
              rate(ceph_rgw_put{%(matchers)s}[$__rate_interval]) *
                on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
            ||| % $.matchers(),
            'PUTs {{ceph_daemon}}'
          ),
          $.addTargetSchema(
            |||
              (
                rate(ceph_rgw_req{%(matchers)s}[$__rate_interval]) -
                  (
                    rate(ceph_rgw_get{%(matchers)s}[$__rate_interval]) +
                      rate(ceph_rgw_put{%(matchers)s}[$__rate_interval])
                  )
              ) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
            ||| % $.matchers(),
            'Other {{ceph_daemon}}'
          ),
        ]
      ),
      $.simplePieChart(
        {
          GETs: '#7eb26d',
          'Other (HEAD,POST,DELETE)': '#447ebc',
          PUTs: '#eab839',
          Requests: '#3f2b5b',
          Failures: '#bf1b00',
        }, '', 'Workload Breakdown'
      )
      .addTarget($.addTargetSchema(
        |||
          rate(ceph_rgw_failed_req{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        'Failures {{ceph_daemon}}'
      ))
      .addTarget($.addTargetSchema(
        |||
          rate(ceph_rgw_get{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        'GETs {{ceph_daemon}}'
      ))
      .addTarget($.addTargetSchema(
        |||
          rate(ceph_rgw_put{%(matchers)s}[$__rate_interval]) *
            on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        'PUTs {{ceph_daemon}}'
      ))
      .addTarget($.addTargetSchema(
        |||
          (
            rate(ceph_rgw_req{%(matchers)s}[$__rate_interval]) -
              (
                rate(ceph_rgw_get{%(matchers)s}[$__rate_interval]) +
                  rate(ceph_rgw_put{%(matchers)s}[$__rate_interval])
              )
          ) * on (instance_id) group_left (ceph_daemon)
            ceph_rgw_metadata{%(matchers)s, ceph_daemon=~"$rgw_servers"}
        ||| % $.matchers(),
        'Other (DELETE,LIST) {{ceph_daemon}}'
      )) + { gridPos: { x: 20, y: 1, w: 4, h: 8 } },
    ]),
}
