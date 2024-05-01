local g = import 'grafonnet/grafana.libsonnet';


(import 'utils.libsonnet') {
  'ceph-nvmeof-performance.json': $.dashboardSchema(
    'Ceph NVMe-oF Gateways - Performance',
    'Ceph NVMe-oF gateways overview',
    'feeuv1dno43r4deed',
    'now-15m',
    '10s',
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
      current={ selected: false, text: '840834cc-05a3-11f0-baba-0200229b9601', value: '840834cc-05a3-11f0-baba-0200229b9601' }
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
  ).addTemplate(
    $.addTemplateSchema(
      name='subsystem',
      datasource='$datasource',
      query="label_values(ceph_nvmeof_subsystem_metadata{group=~'$group'},nqn)",
      refresh=1,
      includeAll=true,
      sort=0,
      label='Subsystem NQN',
      regex='',
      multi=false,
      current={ selected: false, text: 'All', value: '$__all' }
    )
  ).addPanels([
    $.addRowSchema(collapse=false, showTitle=true, title='Performance') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

    $.timeSeriesPanel(
      title='AVG Reactor CPU Usage by Gateway',
      description='',
      gridPosition={ x: 0, y: 1, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='percentunit',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="avg by(instance) (rate(ceph_nvmeof_reactor_seconds_total{mode='busy',instance=~'$gateway'}[1m]))",
        format='',
        instant=false,
        legendFormat='{{name}}',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='Reactor Threads CPU Usage : $gateway',
      description='',
      gridPosition={ x: 8, y: 1, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='percentunit',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="avg by (instance) (rate(ceph_nvmeof_reactor_seconds_total{mode='busy', instance=~'$gateway.*'}[1m]))\n",
        format='',
        instant=false,
        legendFormat='{{name}}',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='AVG I/O Latency',
      description='',
      gridPosition={ x: 16, y: 1, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='s',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="avg((rate(ceph_nvmeof_bdev_read_seconds_total{instance=~'$gateway'}[30s]) / rate(ceph_nvmeof_bdev_reads_completed_total{instance=~'$gateway'}[30s])) > 0)\n",
        format='time_series',
        instant=false,
        legendFormat='Reads',
        range=true,
        datasource='$datasource',
      )
    )
    .addTarget(
      $.addTargetSchema(
        expr="avg((rate(ceph_nvmeof_bdev_write_seconds_total{instance=~'$gateway'}[30s]) / rate(ceph_nvmeof_bdev_writes_completed_total{instance=~'$gateway'}[30s])) > 0)",
        format='time_series',
        instant=false,
        legendFormat='Writes',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='IOPS by Gateway',
      description='',
      gridPosition={ x: 0, y: 9, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='locale',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum by(instance) (rate(ceph_nvmeof_bdev_reads_completed_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_writes_completed_total{instance=~'$gateway'}[1m]))",
        format='time_series',
        instant=false,
        legendFormat='__auto',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='IOPS by NVMe-oF Subsystem',
      description='',
      gridPosition={ x: 8, y: 9, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='locale',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="\nsum by(nqn) ((rate(ceph_nvmeof_bdev_reads_completed_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_writes_completed_total{instance=~'$gateway'}[1m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata{instance=~'$gateway'})",
        format='time_series',
        instant=false,
        legendFormat='__auto',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='TOP 5 - IOPS by device for $subsystem',
      description='',
      gridPosition={ x: 16, y: 9, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='locale',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="topk(5, (sum by(pool_name, rbd_name) (((rate(ceph_nvmeof_bdev_reads_completed_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_writes_completed_total{instance=~'$gateway'}[1m])) * on(instance,bdev_name) group_right ceph_nvmeof_bdev_metadata{instance=~'$gateway'}) * on(instance, bdev_name) group_left(nqn) ceph_nvmeof_subsystem_namespace_metadata{nqn=~'$subsystem',instance=~'$gateway'})))",
        format='time_series',
        instant=false,
        legendFormat='{{pool_name}}/{{rbd_name}}',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='Throughput by Gateway',
      description='',
      gridPosition={ x: 0, y: 17, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='binBps',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='normal',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="sum by(instance) (rate(ceph_nvmeof_bdev_read_bytes_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_written_bytes_total{instance=~'$gateway'}[1m]))",
        format='time_series',
        instant=false,
        legendFormat='{{name}}',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='Throughput by NVMe-oF Subsystem',
      description='',
      gridPosition={ x: 8, y: 17, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='binBps',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="\nsum by(nqn) ((rate(ceph_nvmeof_bdev_read_bytes_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_written_bytes_total{instance=~'$gateway'}[1m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata{instance=~'$gateway'})",
        format='time_series',
        instant=false,
        legendFormat='__auto',
        range=true,
        datasource='$datasource',
      )
    ),

    $.timeSeriesPanel(
      title='TOP 5 - Throughput by device for $subsystem',
      description='',
      gridPosition={ x: 16, y: 17, w: 8, h: 8 },
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      datasource='$datasource',
      pointSize=5,
      showPoints='auto',
      unit='binBps',
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ maxHeight: 600, mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=false,
      decimals=null,
      thresholdsMode='absolute',
      noValue=null,
    ).addThresholds([
      { color: 'green', value: null },
      { color: 'red', value: 80 },
    ])
    .addTarget(
      $.addTargetSchema(
        expr="topk(5, (sum by(pool_name, rbd_name) (((rate(ceph_nvmeof_bdev_read_bytes_total{instance=~'$gateway'}[1m]) + rate(ceph_nvmeof_bdev_written_bytes_total{instance=~'$gateway'}[1m])) * on(instance,bdev_name) group_right ceph_nvmeof_bdev_metadata{instance=~'$gateway'}) * on(instance, bdev_name) group_left(nqn) ceph_nvmeof_subsystem_namespace_metadata{nqn=~'$subsystem',instance=~'$gateway'})))",
        format='time_series',
        instant=false,
        legendFormat='{{name}}',
        range=true,
        datasource='$datasource',
      )
    ),

  ]),

  // end
}
