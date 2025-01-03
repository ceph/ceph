local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'ceph-cluster-advanced.json': $.dashboardSchema(
    'Ceph Cluster - Advanced',
    'Ceph cluster overview',
    'dn13KBeTv',
    'now-6h',
    '1m',
    38,
    $._config.dashboardTags,
    ''
  ).addAnnotation(
    $.addAnnotationSchema(
      1,
      '-- Grafana --',
      true,  // enable
      true,  // hide
      'rgba(0, 211, 255, 1)',
      'Annotations & Alerts',
      'dashboard'
    )
  ).addRequired(
    type='grafana', id='grafana', name='Grafana', version='5.3.2'
  ).addRequired(
    type='panel', id='graph', name='Graph', version='5.0.0'
  ).addRequired(
    type='panel', id='heatmap', name='Heatmap', version='5.0.0'
  ).addRequired(
    type='panel', id='singlestat', name='Singlestat', version='5.0.0'
  ).addTemplate(
    g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
  ).addTemplate(
    $.addClusterTemplate()
  ).addTemplate(
    $.addCustomTemplate(
      name='interval',
      query='5s,10s,30s,1m,10m,30m,1h,6h,12h,1d,7d,14d,30d',
      current='$__auto_interval_interval',
      refresh=2,
      label='Interval',
      auto_count=10,
      auto_min='1m',
      options=[
        { selected: true, text: 'auto', value: '$__auto_interval_interval' },
        { selected: false, text: '5s', value: '5s' },
        { selected: false, text: '10s', value: '10s' },
        { selected: false, text: '30s', value: '30s' },
        { selected: false, text: '1m', value: '1m' },
        { selected: false, text: '10m', value: '10m' },
        { selected: false, text: '30m', value: '30m' },
        { selected: false, text: '1h', value: '1h' },
        { selected: false, text: '6h', value: '6h' },
        { selected: false, text: '12h', value: '12h' },
        { selected: false, text: '1d', value: '1d' },
        { selected: false, text: '7d', value: '7d' },
        { selected: false, text: '14d', value: '14d' },
        { selected: false, text: '30d', value: '30d' },
      ],
      auto=true,
    )
  ).addPanels(
    [
      $.addRowSchema(collapse=false, showTitle=true, title='CLUSTER STATE') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.addStatPanel(
        title='Ceph health status',
        unit='none',
        datasource='$datasource',
        gridPosition={ x: 0, y: 1, w: 3, h: 3 },
        colorMode='value',
        interval='1m',
        transparent=true,
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        pluginVersion='9.4.7'
      ).addMappings([
        {
          options: {
            '0': { text: 'HEALTHY' },
            '1': { text: 'WARNING' },
            '2': { text: 'ERROR' },
          },
          type: 'value',
        },
        { options: { match: null, result: { text: 'N/A' } }, type: 'special' },
      ])
      .addThresholds([
        { color: '#9ac48a' },
        { color: 'rgba(237, 129, 40, 0.89)', value: 1 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 2 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_health_status{%(matchers)s}' % $.matchers(),
        instant=true,
        interval='$interval',
        datasource='$datasource',
        step=300,
      )),

      $.addGaugePanel(
        title='Available Capacity',
        gridPosition={ h: 6, w: 3, x: 3, y: 1 },
        unit='percentunit',
        max=1,
        min=0,
        interval='1m',
        pluginVersion='9.4.7'
      ).addMappings([
        { options: { match: null, result: { text: 'N/A' } }, type: 'special' },
      ])
      .addThresholds([
        { color: 'rgba(245, 54, 54, 0.9)' },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.1 },
        { color: 'rgba(50, 172, 45, 0.97)', value: 0.3 },
      ])
      .addTarget($.addTargetSchema(
        expr='(ceph_cluster_total_bytes{%(matchers)s}-ceph_cluster_total_used_bytes{%(matchers)s})/ceph_cluster_total_bytes{%(matchers)s}' % $.matchers(),
        instant=true,
        interval='$interval',
        datasource='$datasource',
        step=300
      )),

      $.addStatPanel(
        title='Cluster Capacity',
        unit='decbytes',
        datasource='$datasource',
        gridPosition={ x: 6, y: 1, w: 3, h: 3 },
        graphMode='area',
        decimals=2,
        interval='1m',
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addMappings([
        { options: { match: null, result: { text: 'N/A' } }, type: 'special' },
      ]).addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)' },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 1.0 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_cluster_total_bytes{%(matchers)s}' % $.matchers(),
        instant=true,
        interval='$interval',
        datasource='$datasource',
        step=300
      )),

      $.addStatPanel(
        title='Write Throughput',
        unit='Bps',
        datasource='$datasource',
        gridPosition={ x: 9, y: 1, w: 3, h: 3 },
        decimals=1,
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addMappings([
        { options: { match: null, result: { text: 'N/A' } }, type: 'special' },
      ]).addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(irate(ceph_osd_op_w_in_bytes{%(matchers)s}[5m]))' % $.matchers(),
        instant=true,
        interval='$interval',
        datasource='$datasource',
      )),

      $.addStatPanel(
        title='Read Throughput',
        unit='Bps',
        datasource='$datasource',
        gridPosition={ x: 12, y: 1, w: 3, h: 3 },
        decimals=1,
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addMappings([
        { options: { match: null, result: { text: 'N/A' } }, type: 'special' },
      ]).addThresholds([
        { color: '#d44a3a' },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#9ac48a', value: 0 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(irate(ceph_osd_op_r_out_bytes{%(matchers)s}[5m]))' % $.matchers(),
        instant=true,
        interval='$interval',
        datasource='$datasource',
      )),

      $.addStatPanel(
        title='OSDs',
        datasource='$datasource',
        gridPosition={ h: 3, w: 6, x: 15, y: 1 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='auto',
        rootColorMode='Panel',
        displayName='',
        rootColors={
          crit: 'rgb(255, 0, 0)',
          disable: 'rgba(128, 128, 128, 0.9)',
          ok: 'rgba(50, 128, 45, 0.9)',
          warn: 'rgba(237, 129, 40, 0.9)',
        },
        cornerRadius=0,
        flipCard=false,
        flipTime=5,
        isAutoScrollOnOverflow=false,
        isGrayOnNoData=false,
        isHideAlertsOnDisable=false,
        isIgnoreOKColors=false,
        fontFormat='Regular',
        colorMode='background',
        unit='none',
        pluginVersion='9.4.7',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          aggregation='Last',
          alias='All',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ceph_osd_metadata{%(matchers)s})' % $.matchers(),
          legendFormat='All',
          interval='$interval',
          datasource='$datasource',
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='In',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ceph_osd_in{%(matchers)s})' % $.matchers(),
          legendFormat='In',
          interval='$interval',
          datasource='$datasource',
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Out',
          decimals=2,
          displayAliasType='Warning / Critical',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='sum(ceph_osd_in{%(matchers)s} == bool 0)' % $.matchers(),
          legendFormat='Out',
          interval='',
          warn=1,
          datasource='$datasource',
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Up',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='sum(ceph_osd_up{%(matchers)s})' % $.matchers(),
          legendFormat='Up',
          interval='',
          datasource='$datasource',
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Down',
          decimals=2,
          displayAliasType='Warning / Critical',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='sum(ceph_osd_up{%(matchers)s} == bool 0)' % $.matchers(),
          legendFormat='Down',
          interval='',
          warn=1,
          datasource='$datasource',
        ),
      ]),

      $.addStatPanel(
        title='MGRs',
        datasource='$datasource',
        gridPosition={ h: 6, w: 3, x: 21, y: 1 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='auto',
        rootColorMode='Panel',
        displayName='',
        rootColors={
          crit: 'rgba(245, 54, 54, 0.9)',
          disable: 'rgba(128, 128, 128, 0.9)',
          ok: 'rgba(50, 128, 45, 0.9)',
          warn: 'rgba(237, 129, 40, 0.9)',
        },
        cornerRadius=1,
        flipCard=false,
        flipTime=5,
        isAutoScrollOnOverflow=false,
        isGrayOnNoData=false,
        isHideAlertsOnDisable=false,
        isIgnoreOKColors=false,
        fontFormat='Regular',
        colorMode='background',
        unit='none',
        pluginVersion='9.4.7',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          aggregation='Last',
          alias='Active',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ceph_mgr_status{%(matchers)s} == 1) or vector(0)' % $.matchers(),
          legendFormat='Active',
          datasource='$datasource',
          instant=true,
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Standby',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ceph_mgr_status{%(matchers)s} == 0) or vector(0)' % $.matchers(),
          legendFormat='Standby',
          datasource='$datasource',
          instant=true,
        ),
      ]),

      $.addStatPanel(
        title='Firing Alerts',
        datasource='$datasource',
        gridPosition={ h: 3, w: 3, x: 0, y: 4 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='auto',
        rootColorMode='Panel',
        displayName='',
        rootColors={
          crit: 'rgba(245, 54, 54, 0.9)',
          disable: 'rgba(128, 128, 128, 0.9)',
          ok: 'rgba(50, 128, 45, 0.9)',
          warn: 'rgba(237, 129, 40, 0.9)',
        },
        cornerRadius=1,
        flipCard=false,
        flipTime=5,
        isAutoScrollOnOverflow=false,
        isGrayOnNoData=false,
        isHideAlertsOnDisable=false,
        isIgnoreOKColors=false,
        fontFormat='Regular',
        colorMode='background',
        unit='none',
        pluginVersion='9.4.7',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 1 },
      ])
      .addOverrides([
        { matcher: { id: 'byName', options: 'Critical' }, properties: [
          { id: 'color', value: { fixedColor: 'red', mode: 'fixed' } },
        ] },
        { matcher: { id: 'byName', options: 'Warning' }, properties: [
          { id: 'color', value: { fixedColor: '#987d24', mode: 'fixed' } },
        ] },
      ])
      .addTargets([
        $.addTargetSchema(
          aggregation='Last',
          alias='Active',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ALERTS{alertstate="firing",alertname=~"^Ceph.+", severity="critical", %(matchers)s}) OR vector(0)' % $.matchers(),
          legendFormat='Critical',
          datasource='$datasource',
          instant=true,
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Standby',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ALERTS{alertstate="firing",alertname=~"^Ceph.+", severity="warning", %(matchers)s}) OR vector(0)' % $.matchers(),
          legendFormat='Warning',
          datasource='$datasource',
          instant=true,
        ),
      ]),

      $.addStatPanel(
        title='Used Capacity',
        datasource='$datasource',
        gridPosition={ h: 3, w: 3, x: 6, y: 4 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='horizontal',
        graphMode='area',
        displayName='',
        maxDataPoints=100,
        colorMode='none',
        unit='decbytes',
        pluginVersion='9.4.7',
      )
      .addMappings([
        { options: { result: { text: 'N/A' } }, type: 'special' },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='ceph_cluster_total_used_bytes{%(matchers)s}' % $.matchers(),
          legendFormat='',
          datasource='$datasource',
          instant=true,
        ),
      ]),

      $.addStatPanel(
        title='Write IOPS',
        datasource='$datasource',
        gridPosition={ h: 3, w: 3, x: 9, y: 4 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='horizontal',
        graphMode='area',
        displayName='',
        maxDataPoints=100,
        colorMode='none',
        unit='ops',
        pluginVersion='9.4.7',
      )
      .addMappings([
        { options: { result: { text: 'N/A' } }, type: 'special' },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_osd_op_w{%(matchers)s}[1m]))' % $.matchers(),
          legendFormat='',
          datasource='$datasource',
          instant=true,
        ),
      ]),

      $.addStatPanel(
        title='Read IOPS',
        datasource='$datasource',
        gridPosition={ h: 3, w: 3, x: 12, y: 4 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='horizontal',
        graphMode='area',
        displayName='',
        maxDataPoints=100,
        colorMode='none',
        unit='ops',
        pluginVersion='9.4.7',
      )
      .addMappings([
        { options: { result: { text: 'N/A' } }, type: 'special' },
      ])
      .addThresholds([
        { color: '#d44a3a', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#9ac48a', value: 0 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_osd_op_r{%(matchers)s}[1m]))' % $.matchers(),
          legendFormat='',
          datasource='$datasource',
          instant=true,
        ),
      ]),

      $.addStatPanel(
        title='Monitors',
        datasource='$datasource',
        gridPosition={ h: 3, w: 6, x: 15, y: 4 },
        color={ mode: 'thresholds' },
        thresholdsMode='absolute',
        orientation='auto',
        rootColorMode='Panel',
        displayName='',
        rootColors={
          crit: 'rgba(245, 54, 54, 0.9)',
          disable: 'rgba(128, 128, 128, 0.9)',
          ok: 'rgba(50, 128, 45, 0.9)',
          warn: 'rgba(237, 129, 40, 0.9)',
        },
        cornerRadius=1,
        flipCard=false,
        flipTime=5,
        isAutoScrollOnOverflow=false,
        isGrayOnNoData=false,
        isHideAlertsOnDisable=false,
        isIgnoreOKColors=false,
        fontFormat='Regular',
        colorMode='background',
        unit='none',
        pluginVersion='9.4.7',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          aggregation='Last',
          alias='In Quorum',
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Text Only',
          expr='sum(ceph_mon_quorum_status{%(matchers)s})' % $.matchers(),
          legendFormat='In Quorum',
          datasource='$datasource',
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='Total',
          crit=1,
          decimals=2,
          displayAliasType='Always',
          displayType='Regular',
          displayValueWithAlias='When Alias Displayed',
          units='none',
          valueHandler='Text Only',
          expr='count(ceph_mon_quorum_status{%(matchers)s})' % $.matchers(),
          legendFormat='Total',
          datasource='$datasource',
          warn=2,
        ),
        $.addTargetSchema(
          aggregation='Last',
          alias='MONs out of Quorum',
          crit=1.6,
          decimals=2,
          displayAliasType='Warning / Critical',
          displayType='Annotation',
          displayValueWithAlias='Never',
          units='none',
          valueHandler='Number Threshold',
          expr='count(ceph_mon_quorum_status{%(matchers)s}) - sum(ceph_mon_quorum_status{%(matchers)s})' % $.matchers(),
          legendFormat='MONs out of Quorum',
          datasource='$datasource',
          warn=1.1,
          range=true,
        ),
      ]),
      $.addRowSchema(collapse=false, showTitle=true, title='CLUSTER STATS') + { gridPos: { x: 0, y: 7, w: 24, h: 1 } },
      $.addAlertListPanel(
        title='Alerts',
        datasource={
          type: 'datasource',
          uid: 'grafana',
        },
        gridPosition={ h: 8, w: 8, x: 0, y: 8 },
        alertInstanceLabelFilter='{alertname=~"^Ceph.+", %(matchers)s}' % $.matchers(),
        alertName='',
        dashboardAlerts=false,
        groupBy=[],
        groupMode='default',
        maxItems=20,
        sortOrder=1,
        stateFilter={
          'error': true,
          firing: true,
          noData: false,
          normal: false,
          pending: true,
        },
      ),

      $.timeSeriesPanel(
        title='Capacity',
        datasource='$datasource',
        gridPosition={ h: 8, w: 8, x: 8, y: 8 },
        fillOpacity=40,
        pointSize=5,
        showPoints='never',
        unit='bytes',
        displayMode='table',
        tooltip={ mode: 'multi', sort: 'desc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=2,
        thresholdsMode='percentage',
        sortBy='Last',
        sortDesc=true,
      )
      .addCalcs(['last'])
      .addThresholds([
        { color: 'green', value: null },
        { color: '#c0921f', value: 75 },
        { color: '#E02F44', value: 85 },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byName', options: 'Total Capacity' },
            properties: [{
              id: 'color',
              value: { fixedColor: 'red', mode: 'fixed' },
            }],
          },
          {
            matcher: { id: 'byName', options: 'Used' },
            properties: [
              {
                id: 'color',
                value: { fixedColor: 'green', mode: 'fixed' },
              },
              {
                id: 'custom.thresholdsStyle',
                value: { mode: 'dashed' },
              },
            ],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='ceph_cluster_total_bytes{%(matchers)s}' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            instant=false,
            legendFormat='Total Capacity',
            step=300,
            range=true,
          ),
          $.addTargetSchema(
            expr='ceph_cluster_total_used_bytes{%(matchers)s}' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            instant=false,
            legendFormat='Used',
            step=300,
            range=true,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Cluster Throughput',
        datasource='$datasource',
        gridPosition={ h: 8, w: 8, x: 16, y: 8 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='decbytes',
        displayMode='table',
        tooltip={ mode: 'multi', sort: 'desc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
      ).addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 85 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='sum(irate(ceph_osd_op_w_in_bytes{%(matchers)s}[5m]))' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Write',
            step=300,
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(irate(ceph_osd_op_r_out_bytes{%(matchers)s}[5m]))' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Read',
            step=300,
            range=true,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='IOPS',
        datasource='$datasource',
        gridPosition={ h: 8, w: 8, x: 0, y: 16 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='decbytes',
        displayMode='table',
        tooltip={ mode: 'multi', sort: 'desc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
      )
      .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='sum(irate(ceph_osd_op_w{%(matchers)s}[1m]))' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Write',
            step=300,
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(irate(ceph_osd_op_r{%(matchers)s}[1m]))' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Read',
            step=300,
            range=true,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Pool Used Bytes',
        datasource='$datasource',
        gridPosition={ h: 8, w: 8, x: 8, y: 16 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='bytes',
        tooltip={ mode: 'multi', sort: 'desc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='right',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='(ceph_pool_bytes_used{%(matchers)s}) *on (pool_id) group_left(name)(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='{{name}}',
            step=300,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Pool Used RAW Bytes',
        datasource='$datasource',
        gridPosition={ h: 8, w: 8, x: 16, y: 16 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='bytes',
        tooltip={ mode: 'multi', sort: 'desc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='table',
        placement='right',
      )
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byName', options: 'rbd Stored' },
            properties: [{
              id: 'color',
              value: { fixedColor: 'transparent', mode: 'fixed' },
            }],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='(ceph_pool_stored_raw{%(matchers)s}) *on (pool_id) group_left(name)(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='',
            legendFormat='{{name}}',
            step=300,
            range=true,
            hide=false,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Pool Objects Quota',
        datasource='$datasource',
        gridPosition={ h: 7, w: 8, x: 0, y: 24 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        interval='$interval',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='(ceph_pool_quota_objects{%(matchers)s}) *on (pool_id) group_left(name)(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='',
            legendFormat='{{name}}',
            step=300,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Pool Quota Bytes',
        datasource='$datasource',
        gridPosition={ h: 7, w: 8, x: 8, y: 24 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='bytes',
        tooltip={ mode: 'multi', sort: 'none' },
        interval='$interval',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='(ceph_pool_quota_bytes{%(matchers)s}) *on (pool_id) group_left(name)(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='',
            legendFormat='{{name}}',
            step=300,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Objects Per Pool',
        datasource='$datasource',
        gridPosition={ h: 7, w: 8, x: 16, y: 24 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=false,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='right',
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='(ceph_pool_objects{%(matchers)s}) * on (pool_id) group_left(name)(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='',
            legendFormat='{{name}}',
          ),
        ]
      ),

      $.addRowSchema(collapse=false, showTitle=true, title='OBJECTS') + { gridPos: { x: 0, y: 31, w: 24, h: 1 } },

      $.timeSeriesPanel(
        title='OSD Type Count',
        datasource='$datasource',
        gridPosition={ h: 12, w: 6, x: 0, y: 32 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=2,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'asc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=false,
      )
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byRegexp', options: '/^Total.*$/' },
            properties: [{
              id: 'custom.stacking',
              value: { group: false, mode: 'normal' },
            }],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='sum(ceph_pool_objects{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Total',
            range=true,
            step=200
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='PGs State',
        datasource='$datasource',
        gridPosition={ h: 12, w: 8, x: 6, y: 32 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=2,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'asc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='table',
        placement='right',
        showLegend=true,
      )
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addCalcs(['lastNotNull'])
      .addOverrides(
        [
          {
            matcher: { id: 'byRegexp', options: '/^Total.*$/' },
            properties: [{
              id: 'custom.stacking',
              value: { group: false, mode: 'normal' },
            }],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='sum(ceph_pg_active{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Active',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_clean{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Clean',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_peering{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Peering',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_degraded{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Degraded',
            range=true,
            step=300,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_stale{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Stale',
            range=true,
            step=300,
          ),
          $.addTargetSchema(
            expr='sum(ceph_unclean_pgs{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Unclean',
            range=true,
            step=300,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_undersized{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Undersized',
            range=true,
            step=300,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_incomplete{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Incomplete',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_forced_backfill{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Forced Backfill',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_forced_recovery{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Forced Recovery',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_creating{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Creating',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_wait_backfill{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Wait Backfill',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_deep{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Deep',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_scrubbing{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Scrubbing',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_recovering{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Recovering',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_repair{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Repair',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_down{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Down',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_peered{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Peered',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_backfill{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Backfill',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_remapped{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Remapped',
            range=true,
          ),
          $.addTargetSchema(
            expr='sum(ceph_pg_backfill_toofull{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            interval='$interval',
            legendFormat='Backfill Toofull',
            range=true,
          ),
        ]
      ),

      $.timeSeriesPanel(
        title='Stuck PGs',
        datasource='$datasource',
        gridPosition={ h: 6, w: 10, x: 14, y: 32 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=2,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'asc' },
        interval='$interval',
        stackingMode='normal',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='table',
        placement='right',
        showLegend=true,
      )
      .addCalcs(['mean', 'lastNotNull'])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byRegexp', options: '/^Total.*$/' },
            properties: [{
              id: 'custom.stacking',
              value: { group: false, mode: 'normal' },
            }],
          },
        ]
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_pg_degraded{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          legendFormat='Degraded',
          range=true,
          step=300,
        ),
        $.addTargetSchema(
          expr='sum(ceph_pg_stale{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          legendFormat='Stale',
          range=true,
          step=300,
        ),
        $.addTargetSchema(
          expr='sum(ceph_pg_undersized{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          legendFormat='Undersized',
          range=true,
          step=300,
        ),
      ]),

      $.timeSeriesPanel(
        title='Recovery Operations',
        datasource='$datasource',
        gridPosition={ h: 6, w: 10, x: 14, y: 38 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=2,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        interval='$interval',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=false,
      )
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_osd_recovery_ops{%(matchers)s}[$interval]))' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          legendFormat='OPS',
          step=300,
        ),
      ]),
      $.addRowSchema(false, true, 'LATENCY', collapsed=true)
      .addPanels([
        $.heatMapPanel(
          title='OSD Apply Latency Distribution',
          datasource='$datasource',
          gridPosition={ h: 8, w: 12, x: 0, y: 42 },
          colorMode='opacity',
          legendShow=true,
          optionsCalculate=true,
          optionsColor={
            exponent: 0.5,
            fill: '#b4ff00',
            mode: 'opacity',
            reverse: false,
            scale: 'exponential',
            scheme: 'Oranges',
            steps: 128,
          },
          optionsExemplars={ color: 'rgba(255,0,255,0.7)' },
          optionsFilterValues={ le: 1e-9 },
          optionsLegend={ show: true },
          optionsRowFrame={ layout: 'auto' },
          optionsToolTip={
            show: true,
            yHistogram: false,
          },
          optionsYAxis={
            axisPlacement: 'left',
            min: '0',
            reverse: false,
            unit: 'ms',
          },
          xBucketSize='',
          yAxisFormat='ms',
          yAxisLogBase=2,
          yAxisMin='0',
          yBucketSize=10,
          pluginVersion='9.4.7',
        ).addTarget($.addTargetSchema(
          expr='ceph_osd_apply_latency_ms{%(matchers)s}' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          instant=false,
        )),
        $.heatMapPanel(
          title='OSD Commit Latency Distribution',
          datasource='$datasource',
          gridPosition={ h: 8, w: 12, x: 12, y: 42 },
          colorMode='opacity',
          legendShow=true,
          cardColor='#65c5db',
          optionsColor={
            exponent: 0.5,
            fill: '#65c5db',
            mode: 'opacity',
            reverse: false,
            scale: 'exponential',
            scheme: 'Oranges',
            steps: 128,
          },
          optionsCalculate=true,
          optionsCalculation={
            yBuckets: {
              mode: 'count',
              scale: { log: 2, type: 'log' },
            },
          },
          optionsExemplars={ color: 'rgba(255,0,255,0.7)' },
          optionsFilterValues={ le: 1e-9 },
          optionsLegend={ show: true },
          optionsRowFrame={ layout: 'auto' },
          optionsToolTip={
            show: true,
            yHistogram: false,
          },
          optionsYAxis={
            axisPlacement: 'left',
            min: '0',
            reverse: false,
            unit: 'ms',
          },
          xBucketSize='',
          yAxisFormat='ms',
          yAxisLogBase=2,
          yAxisMin='0',
          yBucketSize=10,
          pluginVersion='9.4.7',
        ).addTarget($.addTargetSchema(
          expr='ceph_osd_commit_latency_ms{%(matchers)s}' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          instant=false,
        )),
        $.heatMapPanel(
          title='OSD Read Op Latency Distribution',
          datasource='$datasource',
          gridPosition={ h: 8, w: 12, x: 0, y: 50 },
          colorMode='opacity',
          legendShow=true,
          cardColor='#806eb7',
          optionsColor={
            exponent: 0.5,
            fill: '#806eb7',
            mode: 'opacity',
            reverse: false,
            scale: 'exponential',
            scheme: 'Oranges',
            steps: 128,
          },
          optionsCalculate=true,
          optionsCalculation={
            yBuckets: {
              mode: 'count',
              scale: { log: 2, type: 'log' },
            },
          },
          optionsExemplars={ color: 'rgba(255,0,255,0.7)' },
          optionsFilterValues={ le: 1e-9 },
          optionsLegend={ show: true },
          optionsRowFrame={ layout: 'auto' },
          optionsToolTip={
            show: true,
            yHistogram: false,
          },
          optionsYAxis={
            axisPlacement: 'left',
            decimals: 2,
            min: '0',
            reverse: false,
            unit: 'ms',
          },
          xBucketSize='',
          yAxisFormat='ms',
          yAxisLogBase=2,
          yAxisMin='0',
          yBucketSize=null,
          pluginVersion='9.4.7',
        ).addTarget($.addTargetSchema(
          expr='rate(ceph_osd_op_r_latency_sum{%(matchers)s}[5m]) / rate(ceph_osd_op_r_latency_count{%(matchers)s}[5m]) >= 0' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          instant=false,
        )),

        $.heatMapPanel(
          title='OSD Write Op Latency Distribution',
          datasource='$datasource',
          gridPosition={ h: 8, w: 12, x: 12, y: 50 },
          colorMode='opacity',
          legendShow=true,
          cardColor='#f9934e',
          optionsColor={
            exponent: 0.5,
            fill: '#f9934e',
            mode: 'opacity',
            reverse: false,
            scale: 'exponential',
            scheme: 'Oranges',
            steps: 128,
          },
          optionsCalculate=true,
          optionsCalculation={
            yBuckets: {
              mode: 'count',
              scale: { log: 2, type: 'log' },
            },
          },
          optionsExemplars={ color: 'rgba(255,0,255,0.7)' },
          optionsFilterValues={ le: 1e-9 },
          optionsLegend={ show: true },
          optionsRowFrame={ layout: 'auto' },
          optionsToolTip={
            show: true,
            yHistogram: false,
          },
          optionsYAxis={
            axisPlacement: 'left',
            decimals: 2,
            min: '0',
            reverse: false,
            unit: 'ms',
          },
          xBucketSize='',
          yAxisFormat='ms',
          yAxisLogBase=2,
          yAxisMin='0',
          yBucketSize=null,
          pluginVersion='9.4.7',
        ).addTarget($.addTargetSchema(
          expr='rate(ceph_osd_op_w_latency_sum{%(matchers)s}[5m]) / rate(ceph_osd_op_w_latency_count{%(matchers)s}[5m]) >= 0' % $.matchers(),
          datasource='$datasource',
          interval='$interval',
          legendFormat='',
          instant=false,
        )),
        $.timeSeriesPanel(
          title='Recovery Operations',
          datasource='$datasource',
          gridPosition={ h: 7, w: 12, x: 0, y: 58 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='ms',
          tooltip={ mode: 'multi', sort: 'none' },
          interval='$interval',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
        )
        .addThresholds([
          { color: 'green' },
          { color: 'red', value: 80 },
        ])
        .addTargets([
          $.addTargetSchema(
            expr='avg(rate(ceph_osd_op_r_latency_sum{%(matchers)s}[5m]) / rate(ceph_osd_op_r_latency_count{%(matchers)s}[5m]) >= 0)' % $.matchers(),
            datasource='$datasource',
            legendFormat='Read',
          ),
          $.addTargetSchema(
            expr='avg(rate(ceph_osd_op_w_latency_sum{%(matchers)s}[5m]) / rate(ceph_osd_op_w_latency_count{%(matchers)s}[5m]) >= 0)' % $.matchers(),
            datasource='$datasource',
            legendFormat='Write',
          ),
        ]),

        $.timeSeriesPanel(
          title='AVG OSD Apply + Commit Latency',
          datasource='$datasource',
          gridPosition={ h: 7, w: 12, x: 12, y: 58 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='ms',
          tooltip={ mode: 'multi', sort: 'none' },
          interval='$interval',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
        )
        .addCalcs(['lastNotNull', 'max'])
        .addThresholds([
          { color: 'green' },
          { color: 'red', value: 80 },
        ])
        .addTargets([
          $.addTargetSchema(
            expr='avg(ceph_osd_apply_latency_ms{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            legendFormat='apply',
            interval='$interval',
            metric='ceph_osd_perf_apply_latency_seconds',
            step=4,
          ),
          $.addTargetSchema(
            expr='avg(ceph_osd_commit_latency_ms{%(matchers)s})' % $.matchers(),
            datasource='$datasource',
            legendFormat='commit',
            interval='$interval',
            metric='ceph_osd_perf_commit_latency_seconds',
            step=4,
          ),
        ]),
      ])
      + { gridPos: { x: 0, y: 44, w: 24, h: 1 } },
      $.addRowSchema(collapse=true, showTitle=true, title='', collapsed=false) + { gridPos: { x: 0, y: 45, w: 24, h: 1 } },

      $.addTableExtended(
        datasource='$datasource',
        title='Ceph Versions',
        gridPosition={ h: 6, w: 24, x: 0, y: 46 },
        options={
          footer: {
            fields: '',
            reducer: ['sum'],
            countRows: false,
            enablePagination: false,
            show: false,
          },
          frameIndex: 1,
          showHeader: true,
        },
        custom={ align: 'left', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green' },
          ],
        },
        overrides=[{
          matcher: { id: 'byName', options: 'Time' },
          properties: [
            { id: 'custom.hidden', value: true },
          ],
        }],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        {
          id: 'merge',
          options: {},
        },
        {
          id: 'organize',
          options: {
            excludeByName: {},
            indexByName: {},
            renameByName: {
              Time: '',
              'Value #A': 'OSD Services',
              'Value #B': 'Mon Services',
              'Value #C': 'MDS Services',
              'Value #D': 'RGW Services',
              'Value #E': 'MGR Services',
              ceph_version: 'Ceph Version',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='count by (ceph_version)(ceph_osd_metadata{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='OSD Services',
          range=false,
        ),
        $.addTargetSchema(
          expr='count by (ceph_version)(ceph_mon_metadata{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='Mon Services',
          range=false,
        ),
        $.addTargetSchema(
          expr='count by (ceph_version)(ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          legendFormat='MDS Services',
          range=false,
        ),
        $.addTargetSchema(
          expr='count by (ceph_version)(ceph_rgw_metadata{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='RGW Services',
          range=false,
        ),
        $.addTargetSchema(
          expr='count by (ceph_version)(ceph_mgr_metadata{%(matchers)s})' % $.matchers(),
          datasource='$datasource',
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='MGR Services',
          range=false,
        ),
      ]),


    ]  //end panels
  ),
}
