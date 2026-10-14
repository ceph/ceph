local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'rgw-s3-analytics-buckets.json':
    $.dashboardSchema(
      'Ceph Object - S3 Analytics - Buckets',
      'Per-bucket S3 usage and performance analytics. Usage metrics show stored bytes/objects per bucket. Performance metrics show per-bucket operation throughput and latency.',
      's3bktsAnlt',
      'now-1h',
      '30s',
      22,
      $._config.dashboardTags,
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard'
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
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate($.addClusterTemplate())
    .addTemplate(
      $.addTemplateSchema('rgw_servers',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          2,
                          true,
                          0,
                          null,
                          '')
    )
    .addTemplate(
      // bucket label comes from usage metrics
      $.addTemplateSchema('bucket',
                          '$datasource',
                          'label_values(ceph_rgw_bucket_usage_bytes, bucket)',
                          2,
                          true,
                          0,
                          'Bucket',
                          '')
    )

    .addPanels([
      // ── Row: Bucket Usage Summary ─────────────────────────────────────────
      $.addRowSchema(false, true, 'Bucket Usage Summary') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

      $.addStatPanel(
        title='Stored Bytes',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Total bytes stored in the selected bucket (usage metric).',
      ).addThresholds([{ color: 'green', value: null }])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_bytes{bucket=~"$bucket"})',
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Utilized Bytes',
        datasource='${datasource}',
        gridPosition={ x: 6, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Total compressed bytes stored in the selected bucket (usage metric).',
      ).addThresholds([{ color: 'green', value: null }])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_bytes_utilized{bucket=~"$bucket"})',
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Stored Objects',
        datasource='${datasource}',
        gridPosition={ x: 12, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Total number of objects in the selected bucket (usage metric).',
      ).addThresholds([{ color: 'green', value: null }])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_objects{bucket=~"$bucket"})',
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Quota: Max Bytes',
        datasource='${datasource}',
        gridPosition={ x: 18, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Maximum bytes per-bucket quota (-1 = unlimited).',
      ).addThresholds([{ color: 'green', value: null }])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_quota_bytes{bucket=~"$bucket"})',
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      // ── Row: Usage Trends ─────────────────────────────────────────────────
      $.addRowSchema(false, true, 'Usage Trends') + { gridPos: { x: 0, y: 4, w: 24, h: 1 } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='Stored Bytes Over Time',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 12, x: 0, y: 5 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='decbytes',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=2,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='Total bytes stored in the selected bucket over time. Source: usage metric (gauge).',
      )
      .addThresholds([{ color: 'green' }])
      .addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_bytes{bucket=~"$bucket"}',
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='Object Count Over Time',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 12, x: 12, y: 5 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='none',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=0,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='Total object count in the selected bucket over time. Source: usage metric.',
      )
      .addThresholds([{ color: 'green' }])
      .addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_objects{bucket=~"$bucket"}',
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      // ── Row: Bucket-level Operation Performance ───────────────────────────
      $.addRowSchema(false, true, 'Bucket-Level Operation Performance') + { gridPos: { x: 0, y: 13, w: 24, h: 1 } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='PUT Bytes (Performance)',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 6, x: 0, y: 14 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='decbytes',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=2,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='PUT bytes from per-bucket performance counter (cumulative, not stored capacity).',
      )
      .addThresholds([{ color: 'green' }])
      .addOverrides([{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }])
      .addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_bytes{bucket=~"$bucket"}) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{ceph_daemon}} - {{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='GET Bytes (Performance)',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 6, x: 6, y: 14 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='decbytes',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=2,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='GET bytes from per-bucket performance counter.',
      )
      .addThresholds([{ color: 'green' }])
      .addOverrides([{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }])
      .addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_bytes{bucket=~"$bucket"}) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{ceph_daemon}} - {{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='PUT Operations',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 6, x: 12, y: 14 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='none',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=2,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='PUT operation count from per-bucket performance counter.',
      )
      .addThresholds([{ color: 'green' }])
      .addOverrides([{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }])
      .addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_ops{bucket=~"$bucket"}) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{ceph_daemon}} - {{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      $.timeSeriesPanel(
        lineInterpolation='linear',
        lineWidth=1,
        drawStyle='line',
        axisPlacement='auto',
        title='GET Operations',
        datasource='${datasource}',
        gridPosition={ h: 8, w: 6, x: 18, y: 14 },
        fillOpacity=0,
        pointSize=5,
        showPoints='auto',
        unit='none',
        displayMode='table',
        showLegend=true,
        placement='bottom',
        tooltip={ mode: 'single', sort: 'desc' },
        stackingMode='none',
        spanNulls=true,
        decimals=2,
        thresholdsMode='absolute',
        sortBy='Last *',
        sortDesc=true,
        description='GET operation count from per-bucket performance counter.',
      )
      .addThresholds([{ color: 'green' }])
      .addOverrides([{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }])
      .addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_ops{bucket=~"$bucket"}) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          format='time_series',
          instant=false,
          legendFormat='{{ceph_daemon}} - {{bucket}}',
          step=300,
          range=true,
        ),
      ]) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },

      // Bucket performance summary table
      $.addTableExtended(
        datasource='${datasource}',
        title='Bucket Operation Summary',
        gridPosition={ h: 8, w: 12, x: 0, y: 22 },
        description='Per-bucket operation counts from performance metrics. Filtered by selected bucket.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 1,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'PUTs' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          { matcher: { id: 'byType', options: 'number' }, properties: [{ id: 'unit', value: 'none' }] },
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
        ],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        { id: 'merge', options: {} },
        { id: 'joinByField', options: { byField: 'bucket', mode: 'outer' } },
        {
          id: 'groupBy',
          options: {
            fields: {
              bucket: { aggregations: [], operation: 'groupby' },
              ceph_daemon: { aggregations: [], operation: 'groupby' },
              'Value #A': { aggregations: [], operation: 'groupby' },
              'Value #B': { aggregations: [], operation: 'groupby' },
              'Value #C': { aggregations: [], operation: 'groupby' },
              'Value #D': { aggregations: [], operation: 'groupby' },
              'Value #E': { aggregations: [], operation: 'groupby' },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              Time: true,
              time: true,
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              __name__: true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'ceph_daemon 5': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
            },
            indexByName: { bucket: 0, Bucket: 0, ceph_daemon: 1, Daemon: 1, 'Value #A': 2, 'Value #B': 3, 'Value #C': 4, 'Value #D': 5, 'Value #E': 6 },
            renameByName: { bucket: 'Bucket', ceph_daemon: 'Daemon', 'Value #A': 'PUTs', 'Value #B': 'GETs', 'Value #C': 'List', 'Value #D': 'DELETE', 'Value #E': 'COPY' },
          },
        },
        {
          id: 'filterFieldsByName',
          options: {
            include: {
              names: ['Bucket', 'Daemon', 'PUTs', 'GETs', 'List', 'DELETE', 'COPY'],
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_put_obj_ops{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_get_obj_ops{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_list_obj_ops{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_del_obj_ops{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_copy_obj_ops{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Bucket Latency Summary (ms)',
        gridPosition={ h: 8, w: 12, x: 12, y: 22 },
        description='Per-bucket cumulative latency sums from performance metrics. Not per-request latency.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 1,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'PUTs' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          { matcher: { id: 'byType', options: 'number' }, properties: [{ id: 'unit', value: 'ms' }] },
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
        ],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        { id: 'merge', options: {} },
        { id: 'joinByField', options: { byField: 'bucket', mode: 'outer' } },
        {
          id: 'groupBy',
          options: {
            fields: {
              bucket: { aggregations: [], operation: 'groupby' },
              ceph_daemon: { aggregations: [], operation: 'groupby' },
              'Value #A': { aggregations: [], operation: 'groupby' },
              'Value #B': { aggregations: [], operation: 'groupby' },
              'Value #C': { aggregations: [], operation: 'groupby' },
              'Value #D': { aggregations: [], operation: 'groupby' },
              'Value #E': { aggregations: [], operation: 'groupby' },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              Time: true,
              time: true,
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              __name__: true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'ceph_daemon 5': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
            },
            indexByName: { bucket: 0, Bucket: 0, ceph_daemon: 1, Daemon: 1, 'Value #A': 2, 'Value #B': 3, 'Value #C': 4, 'Value #D': 5, 'Value #E': 6 },
            renameByName: { bucket: 'Bucket', ceph_daemon: 'Daemon', 'Value #A': 'List (ms)', 'Value #B': 'PUT (ms)', 'Value #C': 'GET (ms)', 'Value #D': 'DELETE (ms)', 'Value #E': 'COPY (ms)' },
          },
        },
        {
          id: 'filterFieldsByName',
          options: {
            include: {
              names: ['Bucket', 'Daemon', 'List (ms)', 'PUT (ms)', 'GET (ms)', 'DELETE (ms)', 'COPY (ms)'],
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_list_obj_lat_sum{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='List Objects',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_put_obj_lat_sum{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='Upload Objects',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_get_obj_lat_sum{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='Get Objects',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_del_obj_lat_sum{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='Delete Objects',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_copy_obj_lat_sum{bucket=~"$bucket"} *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='Copy Objects',
          range=false,
        ),
      ]),

      // ── Row: Users Associated with this Bucket ────────────────────────────
      $.addRowSchema(false, true, 'Users for Selected Bucket (Usage)') + { gridPos: { x: 0, y: 30, w: 24, h: 1 } },

      $.addTableExtended(
        datasource='${datasource}',
        title='Users Associated with Bucket',
        gridPosition={ h: 10, w: 24, x: 0, y: 31 },
        description="Users who own the selected bucket, identified via usage metrics (ceph_rgw_bucket_usage_*). This is a usage-based relationship. Click a user to open S3 Analytics – Users for that user's aggregate performance.",
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Stored Bytes' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          { matcher: { id: 'byName', options: 'Stored Bytes' }, properties: [{ id: 'unit', value: 'decbytes' }] },
          { matcher: { id: 'byName', options: 'Objects' }, properties: [{ id: 'unit', value: 'none' }] },
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usersAnlt/ceph-object-s3-analytics-users?var-owner=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Owner' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usersAnlt/ceph-object-s3-analytics-users?var-owner=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'User (Owner)' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usersAnlt/ceph-object-s3-analytics-users?var-owner=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
        ],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        { id: 'merge', options: {} },
        {
          id: 'groupBy',
          options: {
            fields: {
              bucket: { aggregations: [], operation: 'groupby' },
              owner: { aggregations: [], operation: 'groupby' },
              'Value #A': { aggregations: [], operation: 'groupby' },
              'Value #B': { aggregations: [], operation: 'groupby' },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: { Time: true, time: true, 'Time 1': true, 'Time 2': true, __name__: true, '__name__ 1': true, '__name__ 2': true, zone: true, tenant: true },
            indexByName: { bucket: 0, Bucket: 0, owner: 1, 'User (Owner)': 1, 'Value #A': 2, 'Value #B': 3 },
            renameByName: { bucket: 'Bucket', owner: 'User (Owner)', 'Value #A': 'Stored Bytes', 'Value #B': 'Objects' },
          },
        },
        {
          id: 'filterFieldsByName',
          options: {
            include: {
              names: ['Bucket', 'User (Owner)', 'Stored Bytes', 'Objects'],
            },
          },
        },
        { id: 'sortBy', options: { fields: [{ desc: true, displayName: 'Stored Bytes' }] } },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_bytes{bucket=~"$bucket"}',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_objects{bucket=~"$bucket"}',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),

      // ── Row: All-Buckets Comparison ───────────────────────────────────────
      $.addRowSchema(false, true, 'All-Buckets Comparison (Usage)') + { gridPos: { x: 0, y: 41, w: 24, h: 1 } },

      $.addTableExtended(
        datasource='${datasource}',
        title='All Buckets – Stored Bytes & Objects',
        gridPosition={ h: 10, w: 24, x: 0, y: 42 },
        description='All buckets ranked by stored bytes. Source: usage metrics. Click a bucket to view its detail.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Stored Bytes' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          { matcher: { id: 'byName', options: 'Stored Bytes' }, properties: [{ id: 'unit', value: 'decbytes' }] },
          { matcher: { id: 'byName', options: 'Objects' }, properties: [{ id: 'unit', value: 'none' }] },
          { matcher: { id: 'byName', options: 'Utilized Bytes' }, properties: [{ id: 'unit', value: 'decbytes' }] },
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3bktsAnlt/ceph-object-s3-analytics-buckets?var-bucket=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usersAnlt/ceph-object-s3-analytics-users?var-owner=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Owner' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usersAnlt/ceph-object-s3-analytics-users?var-owner=${__value.raw}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
        ],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        { id: 'merge', options: {} },
        {
          id: 'groupBy',
          options: {
            fields: {
              bucket: { aggregations: [], operation: 'groupby' },
              owner: { aggregations: [], operation: 'groupby' },
              'Value #A': { aggregations: [], operation: 'groupby' },
              'Value #B': { aggregations: [], operation: 'groupby' },
              'Value #C': { aggregations: [], operation: 'groupby' },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: { Time: true, time: true, 'Time 1': true, 'Time 2': true, 'Time 3': true, __name__: true, '__name__ 1': true, '__name__ 2': true, '__name__ 3': true, zone: true, tenant: true },
            indexByName: { bucket: 0, Bucket: 0, owner: 1, Owner: 1, 'Value #A': 2, 'Value #B': 3, 'Value #C': 4 },
            renameByName: { bucket: 'Bucket', owner: 'Owner', 'Value #A': 'Stored Bytes', 'Value #B': 'Objects', 'Value #C': 'Utilized Bytes' },
          },
        },
        {
          id: 'filterFieldsByName',
          options: {
            include: {
              names: ['Bucket', 'Owner', 'Stored Bytes', 'Objects', 'Utilized Bytes'],
            },
          },
        },
        { id: 'sortBy', options: { fields: [{ desc: true, displayName: 'Stored Bytes' }] } },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_bytes',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_objects',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_rgw_bucket_usage_bytes_utilized',
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),
    ]),
}
