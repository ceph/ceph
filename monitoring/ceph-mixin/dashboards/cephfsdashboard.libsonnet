local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfsdashboard.json':
    $.dashboardSchema(
      'Ceph - Filesystem Overview',
      'Ceph CephFS overview for official Ceph Prometheus plugin.',
      '718Bruins',
      'now-6h',
      '30s',
      38,
      $._config.dashboardTags,
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1,
        { type: 'datasource', uid: 'grafana' },
        true,
        true,
        'rgba(0, 211, 255, 1)',
        'Annotations & Alerts',
        'dashboard'
      )
    )
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addTemplateSchema(
        name='name',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        query={ query: 'label_values(ceph_fs_metadata{%(matchers)s}, name)' % $.matchers(), refId: 'StandardVariableQuery' },
        refresh=1,
        includeAll=true,
        sort=0,
        label='Name',
        regex='',
        hide=0,
        current={ selected: false, text: 'a', value: 'a' },
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'mdatapool',
        { type: 'prometheus', uid: '${datasource}' },
        { query: 'label_values(ceph_fs_metadata{%(matchers)s name=~"$name"}, metadata_pool)' % $.matchers(), refId: 'StandardVariableQuery' },
        1,
        true,
        0,
        'metadata pool',
        '',
        2,
        current={ selected: false, text: 'All', value: '$__all' },
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'datapool',
        { type: 'prometheus', uid: '${datasource}' },
        { query: 'label_values(ceph_fs_metadata{%(matchers)s name=~"$name"}, data_pools)' % $.matchers(), refId: 'StandardVariableQuery' },
        1,
        true,
        0,
        'data pool',
        '',
        2,
        current={ selected: false, text: 'All', value: '$__all' },
      )
    )
    .addLinks([
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
    ])
    .addPanels([
      $.addRowSchema(false,
                     true,
                     'Summary',
                     false) +
      {
        gridPos: { x: 0, y: 0, w: 24, h: 1 },
      },
      $.addStatPanel(
        title='Filesystems',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='none',
        gridPosition={ x: 0, y: 1, w: 3, h: 8 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='count(ceph_fs_metadata{%(matchers)sname=~"$name"})' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata used',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='bytes',
        gridPosition={ x: 3, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$mdatapool"})' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata bandwidth read',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 6, y: 1, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_rd_bytes{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addStatPanel(
        title='Data used',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='bytes',
        gridPosition={ x: 9, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$datapool"})' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Data bandwidth read',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 12, y: 1, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_rd_bytes{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addStatPanel(
        title='Client requests',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 15, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_mds_server_handle_client_request{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Client sessions',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 18, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_sessions_session_count{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='MDS inodes',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 21, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_inodes{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata bandwidth write',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 6, y: 3, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_wr_bytes{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addStatPanel(
        title='Data bandwidth write',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 12, y: 3, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'green', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_wr_bytes{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addGaugePanel(
        title='Metadata used (%)',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        unit='percentunit',
        max=100,
        min=0,
        decimals=2,
        gridPosition={ x: 3, y: 5, w: 3, h: 4 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 70 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='(1 - (sum(ceph_pool_max_avail{%(matchers)s pool_id=~"$mdatapool"}) / (sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$mdatapool"}) + sum(ceph_pool_max_avail{%(matchers)s pool_id=~"$mdatapool"})))) * 100' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='Metadata Used',
        step=60,
      )),
      $.addStatPanel(
        title='Metadata IOPS read',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 6, y: 5, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: '#508642', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_rd{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addGaugePanel(
        title='Data used (%)',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        unit='percentunit',
        max=100,
        min=0,
        decimals=2,
        gridPosition={ x: 9, y: 5, w: 3, h: 4 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 70 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='(1 - (sum(ceph_pool_max_avail{%(matchers)s pool_id=~"$datapool"}) / (sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$datapool"}) + sum(ceph_pool_max_avail{%(matchers)s pool_id=~"$datapool"})))) * 100' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='Data Used',
        step=60,
      )),
      $.addStatPanel(
        title='Data IOPS read',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 12, y: 5, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: '#508642', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_rd{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
      )),
      $.addStatPanel(
        title='Forward requests',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 15, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_mds_forward{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Reply latency',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='ms',
        gridPosition={ x: 18, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { index: 0, text: 'N/A' },
          },
          type: 'special',
        },
        {
          options: {
            match: 'nan',
            result: { index: 1, text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(increase(ceph_mds_reply_latency_sum{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})) / sum(increase(ceph_mds_reply_latency_count{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        range=true,
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='MDS caps',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 21, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_caps{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata IOPS write',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 6, y: 7, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: '#508642', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_wr{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        instant=true,
      )),
      $.addStatPanel(
        title='Data IOPS write',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 12, y: 7, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' },
          },
          type: 'special',
        },
      ])
      .addThresholds([
        { color: '#508642', value: null },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(rate(ceph_pool_wr{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
        interval='$__rate_interval',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        instant=true,
      )),
      $.addRowSchema(collapse=false, showTitle=true, title='MDS', collapsed=false) + { gridPos: { x: 0, y: 9, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='Client requests',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 0, y: 10 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
        {
          options: {
            pattern: '(?<=\\.)(.*?)(?=\\.)',
            result: { index: 0, text: '$1' },
          },
          type: 'regex',
        },
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='rate(ceph_mds_server_handle_client_request{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        range=true,
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Forward requests',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 8, y: 10 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='rate(ceph_mds_forward{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Slave requests',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 16, y: 10 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='rate(ceph_mds_server_handle_slave_request{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Session count',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 0, y: 17 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_sessions_session_count{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Reply latency',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 8, y: 17 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='ms',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='sum by (ceph_daemon) (increase(ceph_mds_reply_latency_sum{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})) / sum by (ceph_daemon) (increase(ceph_mds_reply_latency_count{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Workload',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ x: 16, y: 17, w: 8, h: 7 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        axisLabel='Reads(-) / Writes (+)',
        showPoints='never',
        pluginVersion='9.4.7',
        min=0,
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'sum(rate(ceph_objecter_op_r{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
          'Read Ops'
        ),
        $.addTargetSchema(
          'sum(rate(ceph_objecter_op_w{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s}))' % $.matchers(),
          'Write Ops'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.addRowSchema(collapse=false, showTitle=true, title='Log', collapsed=false) + { gridPos: { x: 0, y: 24, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='Log submit',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 0, y: 25 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='rate(ceph_mds_log_evadd{%(matchers)s}[$__rate_interval]) * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Log events',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 8, y: 25 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_log_ev{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
        title='Log segments',
        datasource={ type: 'prometheus', uid: '${datasource}' },
        gridPosition={ h: 7, w: 8, x: 16, y: 25 },
        fillOpacity=10,
        pointSize=5,
        lineWidth=1,
        showPoints='never',
        unit='short',
        tooltip={ mode: 'multi', sort: 'none' },
        stackingGroup='A',
        stackingMode='none',
        spanNulls=true,
        decimals=null,
        thresholdsMode='absolute',
        displayMode='list',
        placement='bottom',
        showLegend=true,
        pluginVersion='9.4.7',
      ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_log_seg{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
        datasource={ type: 'prometheus', uid: '${datasource}' },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.addRowSchema(false, true, 'Alerts', collapsed=true)
      .addPanels([
        $.addAlertListPanel(
          title='Active alerts',
          datasource={
            type: 'datasource',
            uid: 'grafana',
          },
          gridPosition={ x: 0, y: 33, w: 24, h: 8 },
          alertInstanceLabelFilter='',
          alertName='CephFilesystem',
          dashboardAlerts=false,
          groupBy=[],
          groupMode='default',
          maxItems=20,
          sortOrder=3,
          stateFilter={ 'error': true, firing: true, noData: false, normal: false, pending: true },
        ),
      ])
      + { gridPos: { x: 0, y: 32, w: 24, h: 1 } },
      $.addRowSchema(collapse=false, showTitle=true, title='Memory', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Inodes',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 8, x: 0, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addTarget($.addTargetSchema(
          expr='ceph_mds_mem_ino{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          legendFormat='{{ ceph_daemon }}',
        )),
        $.timeSeriesPanel(
          title='Exported inodes',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 8, x: 8, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addTarget($.addTargetSchema(
          expr='ceph_mds_exported_inodes{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          legendFormat='{{ ceph_daemon }}',
        )),
        $.timeSeriesPanel(
          title='Imported inodes',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 8, x: 16, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addTarget($.addTargetSchema(
          expr='ceph_mds_imported_inodes{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          legendFormat='{{ ceph_daemon }}',
        )),
        $.timeSeriesPanel(
          title='Dentries',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 8, x: 0, y: 49 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addTarget($.addTargetSchema(
          expr='ceph_mds_mem_dn{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          legendFormat='{{ ceph_daemon }}',
        )),
        $.timeSeriesPanel(
          title='Caps',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 8, x: 8, y: 49 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addTarget($.addTargetSchema(
          expr='ceph_mds_caps{%(matchers)s} * on(ceph_daemon) (ceph_fs_metadata{%(matchers)s name=~"$name"} * on(fs_id) group_left(ceph_daemon) ceph_mds_metadata{%(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          legendFormat='{{ ceph_daemon }}',
        )),
      ])
      + { gridPos: { x: 0, y: 41, w: 24, h: 1 } },
      $.addRowSchema(collapse=false, showTitle=true, title='Metadata Pool', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Pool storage',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 14, x: 0, y: 57 },
          fillOpacity=40,
          pointSize=5,
          lineWidth=0,
          showPoints='never',
          unit='bytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='right',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides(
          [
            {
              matcher: { id: 'byRegexp', options: '/^Total.*$/' },
              properties: [
                {
                  id: 'custom.fillOpacity',
                  value: 0,
                },
                {
                  id: 'custom.lineWidth',
                  value: 4,
                },
                {
                  id: 'custom.stacking',
                  value: { group: false, mode: 'normal' },
                },
              ],
            },
            {
              matcher: { id: 'byRegexp', options: '/^Raw.*$/' },
              properties: [
                {
                  id: 'color',
                  value: { fixedColor: '#BF1B00', mode: 'fixed' },
                },
                {
                  id: 'custom.fillOpacity',
                  value: 0,
                },
                {
                  id: 'custom.lineWidth',
                  value: 4,
                },
              ],
            },
          ]
        )
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(ceph_pool_max_avail{%(matchers)s pool_id=~"^$mdatapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Available',
              metric='ceph_pool_available_bytes',
              step=60,
              range=true,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"^$mdatapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Used',
              metric='ceph_pool',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"^$mdatapool$"}) + sum(ceph_pool_max_avail{%(matchers)s pool_id=~"^$mdatapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Total',
              metric='ceph_pool',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_raw_bytes_used{%(matchers)s pool_id=~"^$mdatapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Raw Used',
              metric='ceph_pool',
              step=60,
            ),
          ]
        ),
        $.timeSeriesPanel(
          title='Objects in pool',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 6, x: 14, y: 57 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addCalcs([])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(ceph_pool_objects{%(matchers)s pool_id=~"$mdatapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Objects',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_dirty{%(matchers)s pool_id=~"$mdatapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Dirty Objects',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_quota_objects{%(matchers)s pool_id=~"$mdatapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Quota Objects',
            ),
          ]
        ),
        $.addGaugePanel(
          title='Usage',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          unit='percentunit',
          max=1,
          min=0,
          decimals=2,
          gridPosition={ x: 20, y: 57, w: 4, h: 7 },
          pluginVersion='9.4.7',
          maxDataPoints=100,
        ).addMappings([
          {
            options: {
              match: 'null',
              result: { text: 'N/A' },
            },
            type: 'special',
          },
        ])
        .addThresholds([
          { color: 'green', value: null },
          { color: 'red', value: 80 },
        ])
        .addOverrides([
        ])
        .addTarget($.addTargetSchema(
          expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$mdatapool"}) / sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$mdatapool"} + ceph_pool_max_avail{%(matchers)s pool_id=~"$mdatapool"})' % $.matchers(),
          interval='$__rate_interval',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          step=60,
        )),
        $.timeSeriesPanel(
          title='IOPS',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 12, x: 0, y: 64 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='none',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          min=0,
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_rd{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Read',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_wr{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Write',
              step=60,
            ),
          ]
        ),
        $.timeSeriesPanel(
          title='Throughput',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 12, x: 12, y: 64 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='decbytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          min=0,
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_rd_bytes{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Read Bytes',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_wr_bytes{%(matchers)s pool_id=~"$mdatapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Write Bytes',
              step=60,
            ),
          ]
        ),
      ])
      + { gridPos: { x: 0, y: 56, w: 24, h: 1 } },
      $.addRowSchema(collapse=false, showTitle=true, title='Data Pool', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Pool storage',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 14, x: 0, y: 72 },
          fillOpacity=40,
          pointSize=5,
          lineWidth=0,
          showPoints='never',
          unit='bytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='right',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides(
          [
            {
              matcher: { id: 'byRegexp', options: '/^Total.*$/' },
              properties: [
                {
                  id: 'custom.fillOpacity',
                  value: 0,
                },
                {
                  id: 'custom.lineWidth',
                  value: 4,
                },
                {
                  id: 'custom.stacking',
                  value: { group: false, mode: 'normal' },
                },
              ],
            },
            {
              matcher: { id: 'byRegexp', options: '/^Raw.*$/' },
              properties: [
                {
                  id: 'color',
                  value: { fixedColor: '#BF1B00', mode: 'fixed' },
                },
                {
                  id: 'custom.fillOpacity',
                  value: 0,
                },
                {
                  id: 'custom.lineWidth',
                  value: 4,
                },
              ],
            },
          ]
        )
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(ceph_pool_max_avail{%(matchers)s pool_id=~"^$datapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Available',
              metric='ceph_pool_available_bytes',
              step=60,
              range=true,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"^$datapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Used',
              metric='ceph_pool',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"^$datapool$"}) + sum(ceph_pool_max_avail{%(matchers)s pool_id=~"^$mdatapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Total',
              metric='ceph_pool',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_raw_bytes_used{%(matchers)s pool_id=~"^$datapool$"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Raw Used',
              metric='ceph_pool',
              step=60,
            ),
          ]
        ),
        $.timeSeriesPanel(
          title='Objects in pool',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 6, x: 14, y: 72 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
        ])
        .addCalcs([])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(ceph_pool_objects{%(matchers)s pool_id=~"$datapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Objects',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_dirty{%(matchers)s pool_id=~"$datapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Dirty Objects',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(ceph_pool_quota_objects{%(matchers)s pool_id=~"$datapool"})' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Quota Objects',
            ),
          ]
        ),
        $.addGaugePanel(
          title='Usage',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          unit='percentunit',
          max=1,
          min=0,
          decimals=2,
          gridPosition={ x: 20, y: 72, w: 4, h: 7 },
          pluginVersion='9.4.7',
          maxDataPoints=100,
        ).addMappings([
          {
            options: {
              match: 'null',
              result: { text: 'N/A' },
            },
            type: 'special',
          },
        ])
        .addThresholds([
          { color: 'green', value: null },
          { color: 'red', value: 80 },
        ])
        .addOverrides([
        ])
        .addTarget($.addTargetSchema(
          expr='sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$datapool"}) / sum(ceph_pool_bytes_used{%(matchers)s pool_id=~"$datapool"} + ceph_pool_max_avail{%(matchers)s pool_id=~"$datapool"})' % $.matchers(),
          interval='$__rate_interval',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          step=60,
        )),
        $.timeSeriesPanel(
          title='IOPS',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 12, x: 0, y: 79 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='none',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          min=0,
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_rd{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Read',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_wr{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Write',
              step=60,
            ),
          ]
        ),
        $.timeSeriesPanel(
          title='Throughput',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          gridPosition={ h: 7, w: 12, x: 12, y: 79 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='decbytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          min=0,
        ).addMappings([
        ])
        .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
        .addThresholds([
          { color: 'green', value: null },
        ])
        .addOverrides([
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_rd_bytes{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Read Bytes',
              step=60,
            ),
            $.addTargetSchema(
              expr='sum(rate(ceph_pool_wr_bytes{%(matchers)s pool_id=~"$datapool"}[$__rate_interval]))' % $.matchers(),
              datasource={ type: 'prometheus', uid: '${datasource}' },
              interval='$__rate_interval',
              legendFormat='Write Bytes',
              step=60,
            ),
          ]
        ),
      ])
      + { gridPos: { x: 0, y: 71, w: 24, h: 1 } },
    ]),
}
