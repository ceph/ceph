local g = import 'grafonnet/grafana.libsonnet';


(import 'utils.libsonnet') {
  'ceph-nvmeof.json': $.dashboardSchema(
    'Ceph NVMe-oF Gateways - Overview',
    'Ceph NVMe-oF gateways overview',
    'feeuv1dno43r4ddjhjssdd',
    'now-6h',
    '30s',
    '39',
    $._config.dashboardTags,
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
  ).addLink(
    $.addLinkSchema(
      asDropdown=true,
      icon='external link',
      includeVars=true,
      keepTime=true,
      tags=[],
      targetBlank=false,
      title='Browse Dashboards',
      tooltip='',
      type='dashboards',
      url=''
    ),
  ).addTemplate(
    g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
  ).addTemplate(
    $.addTemplateSchema(
      name='cluster',
      datasource='$datasource',
      query='label_values(ceph_health_status, cluster)',
      refresh=1,
      includeAll=false,
      sort=1,
      label='cluster',
      regex='(.*)',
      multi=false,
      current={ selected: false, text: '37788254-f1d2-11ef-b13a-02000488170e', value: '37788254-f1d2-11ef-b13a-02000488170e' }
    )
  ).addTemplate(
    $.addTemplateSchema(
      name='group',
      datasource='$datasource',
      query='label_values(ceph_nvmeof_gateway_info,group)',
      refresh=2,
      includeAll=true,
      sort=0,
      label='Gateway Group',
      regex='',
      multi=false,
      current={ selected: false, text: 'All', value: '$__all' }
    )
  ).addTemplate(
    $.addTemplateSchema(
      name='gateway',
      datasource='$datasource',
      query="label_values(ceph_nvmeof_gateway_info{group=~'$group'},hostname)",
      refresh=2,
      includeAll=true,
      sort=0,
      label='Gateway Hostname',
      regex='',
      multi=false,
      current={ selected: false, text: 'All', value: '$__all' }
    )
  ).addPanels([
    $.addRowSchema(collapse=false, showTitle=true, title='Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

    $.addStatPanel(
      title='Gateway Groups',
      description='',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 0, y: 1, w: 3, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'dark-green', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr='count(count  by (group) (ceph_nvmeof_gateway_info))',
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Total Gateways',
      description='',
      unit='',
      datasource='$datasource',
      gridPosition={ x: 3, y: 1, w: 5, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='',
      noValue=null,
    ).addThresholds([
      { color: '#808080', value: null },
      { color: 'red', value: 1.0003 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count(ceph_nvmeof_gateway_info) + sum(ceph_health_detail{name='NVMEOF_GATEWAY_DOWN'})",
        format='time_series',
        instant=true,
        legendFormat='Total',
        range=false,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr='count(ceph_nvmeof_gateway_info)',
        format='time_series',
        instant=false,
        legendFormat='Available',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="(ceph_health_detail{name='NVMEOF_GATEWAY_DOWN'})",
        format='time_series',
        instant=true,
        legendFormat='Down',
        range=false,
        datasource='$datasource',
      )
    )
    .addOverrides([
      {
        matcher: { id: 'byName', options: 'Down' },
        properties: [
          {
            id: 'color',
            value: {
              fixedColor: 'red',
              mode: 'fixed',
            },
          },
        ],
      },
      {
        matcher: { id: 'byName', options: 'Total' },
        properties: [
          {
            id: 'color',
            value: {
              fixedColor: '#a7a38b',
              mode: 'fixed',
            },
          },
        ],
      },
      {
        matcher: { id: 'byName', options: 'Available' },
        properties: [
          {
            id: 'color',
            value: {
              fixedColor: 'green',
              mode: 'fixed',
            },
          },
        ],
      },
    ]),

    $.timeSeriesPanel(
      title='Ceph Health NVMeoF WARNING',
      description='Ceph healthchecks NVMeoF WARNINGs',
      gridPosition={ x: 8, y: 1, w: 7, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      fillOpacity=5,
      pointSize=5,
      showPoints='auto',
      unit='none',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ mode: 'multi', sort: 'desc' },
      stackingMode='normal',
      spanNulls=false,
      decimals=0,
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'green', value: null },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum(ceph_health_detail{name='NVMEOF_GATEWAY_DOWN'})",
        format='',
        instant=false,
        legendFormat='NVMEOF_GATEWAY_DOWN',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="sum(ceph_health_detail{name='NVMEOF_GATEWAY_DELETING'})",
        format='',
        instant=false,
        legendFormat='NVMEOF_GATEWAY_DELETING',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="sum(ceph_health_detail{name='NVMEOF_SINGLE_GATEWAY'})",
        format='',
        instant=false,
        legendFormat='NVMEOF_SINGLE_GATEWAY',
        range=true,
        datasource='$datasource',
      )
    )
    .addOverrides([
      {
        matcher: { id: 'byName', options: 'NVMEOF_GATEWAY_DOWN' },
        properties: [
          {
            id: 'color',
            value: {
              fixedColor: 'red',
              mode: 'fixed',
            },
          },
        ],
      },
      {
        matcher: { id: 'byName', options: 'NVMEOF_GATEWAY_DELETING' },
        properties: [
          {
            id: 'color',
            value: {
              fixedColor: 'dark-purple',
              mode: 'fixed',
            },
          },
        ],
      },
      {
        matcher: { id: 'byName', options: 'NVMEOF_SINGLE_GATEWAY' },
        properties: [
          {
            id: 'custom.lineWidth',
            value: 1,
          },
          {
            id: 'color',
            value: {
              fixedColor: 'super-light-orange',
              mode: 'shades',
            },
          },
        ],
      },
    ]),

    $.addAlertListPanel(
      title='Active Alerts',
      datasource={
        type: 'datasource',
        uid: 'grafana',
      },
      gridPosition={ x: 15, y: 1, w: 9, h: 8 },
      alertInstanceLabelFilter='',
      alertName='NVMEOF',
      dashboardAlerts=false,
      groupBy=[],
      groupMode='default',
      maxItems=20,
      sortOrder=3,
      stateFilter={ 'error': true, firing: true, noData: false, normal: false, pending: true },
    ),

    $.pieChartPanel(
      title='Gateways per group',
      description='Gateways in each group',
      datasource='$datasource',
      gridPos={ x: 0, y: 4, w: 8, h: 5 },
      displayMode='table',
      placement='right',
      showLegend=true,
      displayLabels=[],
      tooltip={ maxHeight: 600, mode: 'single', sort: 'none' },
      pieType='pie',
      values=['value'],
      colorMode='palette-classic',
      overrides=[],
      reduceOptions={ calcs: ['lastNotNull'], fields: '', values: false }
    )
    .addTarget(
      $.addTargetSchema(
        expr='count  by (group) (ceph_nvmeof_gateway_info)',
        format='time_series',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addRowSchema(collapse=false, showTitle=true, title='NVMeoF Group Overview -  $group') + { gridPos: { x: 0, y: 9, w: 24, h: 1 } },

    $.addStatPanel(
      title='Gateways',
      description='',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 0, y: 10, w: 3, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count(ceph_nvmeof_gateway_info{group=~'$group'})",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Subsystems',
      description='',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 3, y: 10, w: 3, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'purple', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count(count by(nqn) (ceph_nvmeof_subsystem_metadata{group=~'$group',instance=~'$gateway'}))",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Namespaces',
      description='',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 6, y: 10, w: 3, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'blue', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count(count by (bdev_name) (\n  ceph_nvmeof_bdev_metadata{instance=~'$gateway'} * on (instance) group_left(group) ceph_nvmeof_gateway_info{group=~'$group'}\n))",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Connected Clients',
      description='Average connected clients',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 9, y: 10, w: 3, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'yellow', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="avg by (nqn) (sum by(gw_name, nqn) (ceph_nvmeof_host_connection_state{instance=~'$gateway'} * on (instance) group_left(group) ceph_nvmeof_gateway_info{group=~'$group'}))",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Capacity Exported',
      description='The sum of capacity from all namespaces defined to subsystems',
      unit='bytes',
      datasource='$datasource',
      gridPosition={ x: 12, y: 10, w: 5, h: 3 },
      colorMode='background',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'light-blue', value: null },
    ]).addMappings([
      {
        options: {
          match: null,
          result: {
            index: 1,
            text: '0',
          },
        },
        type: 'special',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum(avg by (group) (sum by(group, instance) (ceph_nvmeof_bdev_capacity_bytes{instance=~'$gateway'} * on (instance) group_left(group) ceph_nvmeof_gateway_info{group=~'$group'})))",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Subsystem Security',
      description='WARNING if any subsystem is defined with open/no security',
      unit='none',
      datasource='$datasource',
      gridPosition={ x: 17, y: 10, w: 7, h: 3 },
      colorMode='background_solid',
      graphMode='none',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
    ]).addMappings([
      {
        options: {
          match: 'null',
          result: {
            index: 0,
            text: 'OK',
            color: 'dark-green',
          },
        },
        type: 'special',
      },
      {
        options: {
          from: 1,
          to: 9999,
          result: {
            index: 1,
            text: 'WARNING',
            color: 'dark-yellow',
          },
        },
        type: 'range',
      },
      {
        options: {
          '0': {
            index: 2,
            text: 'OK',
            color: 'dark-green',
          },
        },
        type: 'value',
      },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count(ceph_nvmeof_subsystem_metadata{allow_any_host='yes',group=~'$group', instance=~'$gateway'}) ",
        format='',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='Active gateways count',
      description='',
      gridPosition={ x: 0, y: 13, w: 9, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      fillOpacity=5,
      pointSize=5,
      showPoints='auto',
      unit='none',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=0,
      thresholdsMode='absolute',
      noValue='0',
    ).addThresholds([
      { color: 'green', value: null },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="count by (group) (ceph_nvmeof_gateway_info{group=~'$group',instance=~'$gateway'})",
        format='',
        instant=false,
        legendFormat='__auto',
        range=true,
        datasource='$datasource',
      )
    ),

    $.addBarGaugePanel(
      title='Top 5 Busiest Gateway CPU',
      description='Shows the highest average CPU on a gateway within the gateway group',
      datasource='${datasource}',
      gridPosition={ x: 9, y: 13, w: 8, h: 8 },
      unit='percentunit',
      thresholds={
        mode: 'absolute',
        steps: [
          { color: 'green', value: null },
        ],
      },
    )
    .addTarget(
      $.addTargetSchema(
        expr="topk(5, avg by(instance) (rate(ceph_nvmeof_reactor_seconds_total{mode='busy',instance=~'$gateway'}[$__rate_interval])))",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    )
    + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, decimals: 2, displayName: '${__field.instance}', mappings: [], max: 100, min: 0, noValue: '0', thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] }, unit: 'percentunit' }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'unit', value: 'decbytes' }] }] }, options: { displayMode: 'lcd', maxVizHeight: 50, minVizHeight: 16, minVizWidth: 8, namePlacement: 'top', orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'], fields: '/^Value$/', limit: 5, values: true }, showUnfilled: true, sizing: 'manual', valueMode: 'text' } },

    $.addStatPanel(
      title='IOPS - $gateway',
      description='All gateways',
      unit='locale',
      datasource='$datasource',
      gridPosition={ x: 17, y: 13, w: 7, h: 4 },
      colorMode='none',
      graphMode='area',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum(irate(ceph_nvmeof_bdev_reads_completed_total{instance=~'$gateway'}[$__rate_interval]))",
        format='time_series',
        instant=null,
        legendFormat='Read',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="sum(irate(ceph_nvmeof_bdev_writes_completed_total{instance=~'$gateway'}[$__rate_interval]))",
        format='time_series',
        instant=false,
        legendFormat='Write',
        range=true,
        datasource='$datasource',
      )
    ),

    $.addStatPanel(
      title='Throughput - $gateway',
      description='All gateways',
      unit='binBps',
      datasource='$datasource',
      gridPosition={ x: 17, y: 17, w: 7, h: 4 },
      colorMode='none',
      graphMode='area',
      justifyMode='auto',
      orientation='auto',
      textMode='auto',
      interval='1m',
      color={ mode: 'thresholds' },
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'semi-dark-blue', value: null },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum (irate(ceph_nvmeof_bdev_read_bytes_total{instance=~'$gateway'}[$__rate_interval]))",
        format='time_series',
        instant=false,
        legendFormat='Read',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="sum (irate(ceph_nvmeof_bdev_written_bytes_total{instance=~'$gateway'}[$__rate_interval]))",
        format='time_series',
        instant=false,
        legendFormat='Write',
        range=true,
        datasource='$datasource',
      )
    ),

    $.addTableExtended(
      title='Gateway Information',
      datasource='$datasource',
      gridPosition={ x: 0, y: 21, w: 17, h: 12 },
      color={ mode: 'thresholds' },
      options={ cellHeight: 'sm', footer: { countRows: false, enablePagination: true, fields: '', reducer: ['sum'], show: false }, showHeader: true, sortBy: [] },
      custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
      thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }, { color: 'red', value: 80 }] },
      overrides=[{ matcher: { id: 'byName', options: 'GW Version' }, properties: [{ id: 'custom.width', value: 110 }] }, { matcher: { id: 'byName', options: 'Count' }, properties: [{ id: 'custom.width', value: 80 }] }, { matcher: { id: 'byName', options: 'Address' }, properties: [{ id: 'custom.width', value: 112 }] }, { matcher: { id: 'byName', options: 'Group Name' }, properties: [{ id: 'custom.width', value: 128 }] }, { matcher: { id: 'byName', options: 'Group Name' }, properties: [{ id: 'custom.filterable', value: true }] }, { matcher: { id: 'byName', options: 'Reactors' }, properties: [{ id: 'custom.width', value: 70 }, { id: 'custom.align', value: 'left' }] }],
    )
    .addTarget(
      $.addTargetSchema(
        expr="(count by(instance) (ceph_nvmeof_reactor_seconds_total{mode='busy'})) * on (instance) group_left(group,hostname,version,addr) ceph_nvmeof_gateway_info{group=~'$group'}",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    ).addTransformations(
      [{ id: 'organize', options: { excludeByName: { Time: true, Value: false, __name__: true, instance: true, job: true, name: true, port: true, spdk_version: true }, includeByName: {}, indexByName: { Time: 0, Value: 11, __name__: 3, addr: 4, group: 1, hostname: 2, instance: 5, job: 6, name: 7, port: 8, spdk_version: 9, version: 10 }, renameByName: { Value: 'Reactors', addr: 'Address', group: 'Group Name', hostname: 'Hostname', job: '', version: 'GW Version' } } }]
    ),

    $.addBarGaugePanel(
      title='Top 5 Subsystems by Namespace',
      description='Show the subsystems by the count of namespaces they present to the client',
      datasource='${datasource}',
      gridPosition={ x: 17, y: 21, w: 7, h: 8 },
      unit='none',
      thresholds={
        mode: 'absolute',
        steps: [
          { color: 'green', value: null },
        ],
      },
    )
    .addTarget(
      $.addTargetSchema(
        expr="topk(5, avg by (nqn) (ceph_nvmeof_subsystem_namespace_count{instance=~'$gateway'} * on (instance) group_left(group) ceph_nvmeof_gateway_info{group=~'$group'}))",
        format='table',
        instant=true,
        legendFormat='{{nqn}}',
        range=false,
        datasource='$datasource',
      )
    )
    + { fieldConfig: { defaults: { color: { mode: 'continuous-blues' }, displayName: '${__field.nqn}', mappings: [], min: 0, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] }, unit: 'none' }, overrides: [{ matcher: { id: 'byName', options: 'Value' }, properties: [{ id: 'links', value: [{ title: '', url: 'd/feeuv1dno43r4deed/ceph-nvme-of-gateways-performance?var-subsystem=${__data.fields.nqn}' }] }] }] }, options: { displayMode: 'basic', maxVizHeight: 50, minVizHeight: 16, minVizWidth: 8, namePlacement: 'top', orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'], fields: '/^Value$/', limit: 5, values: true }, showUnfilled: true, sizing: 'manual', valueMode: 'text' } },

    $.addBarGaugePanel(
      title='Top 5 Subsystems with Most Connected Clients',
      description='Show the subsystems by the count of namespaces they present to the client',
      datasource='${datasource}',
      gridPosition={ x: 17, y: 29, w: 7, h: 9 },
      unit='',
      thresholds={
        mode: 'absolute',
        steps: [
          { color: 'green', value: null },
        ],
      },
    )
    .addTarget(
      $.addTargetSchema(
        expr="topk(5, avg by (nqn) (sum by(gw_name, nqn) (ceph_nvmeof_host_connection_state{instance=~'$gateway'} * on (instance) group_left(group) ceph_nvmeof_gateway_info{group=~'$group'}))) ",
        format='table',
        instant=true,
        legendFormat='__auto',
        range=false,
        datasource='$datasource',
      )
    )
    + { fieldConfig: { defaults: { color: { mode: 'continuous-purples' }, displayName: '${__field.nqn}', mappings: [], min: 0, noValue: '0', thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } }, overrides: [{ matcher: { id: 'byName', options: 'Value' }, properties: [{ id: 'links', value: [{ title: '', url: 'd/feeuv1dno43r4deed/ceph-nvme-of-gateways-performance?var-subsystem=${__data.fields.nqn}' }] }] }] }, options: { displayMode: 'basic', maxVizHeight: 50, minVizHeight: 16, minVizWidth: 8, namePlacement: 'top', orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'], fields: '/^Value$/', limit: 5, values: true }, showUnfilled: true, sizing: 'manual', valueMode: 'text' } },

  ]),

  // end
}
