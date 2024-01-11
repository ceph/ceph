local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'rgw-s3-analytics.json':
    $.dashboardSchema(
      'RGW S3 Analytics',
      '',
      'BnxelG7Sz',
      'now-1h',
      '30s',
      22,
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
    )

    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )

    .addTemplate(
      $.addClusterTemplate()
    )

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
      g.template.adhoc('Filters', '$datasource', 'filters', 0)
    )


    .addPanels([
      $.addRowSchema(false, true, 'Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.addStatPanel(
        title='Total PUTs',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Total GETs',
        datasource='${datasource}',
        gridPosition={ x: 6, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum\n(ceph_rgw_op_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Total Objects',
        datasource='${datasource}',
        gridPosition={ x: 12, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Average Object Size',
        datasource='${datasource}',
        gridPosition={ x: 18, y: 1, w: 6, h: 3 },
        graphMode='none',
        colorMode='none',
        unit='decbytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum\n((sum by(instance_id)(ceph_rgw_op_put_obj_bytes) > 0) / (sum by(instance_id)(ceph_rgw_op_put_obj_ops) > 0) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addBarGaugePanel(
        title='Total Operations',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 4, w: 8, h: 8 },
        unit='none',
        thresholds={ color: 'green', value: null }
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_buckets_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Buckets',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_bucket_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Buckets',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Objects',
          range=true
        ),
      ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] } } } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


      $.addBarGaugePanel(
        title='Total Size',
        datasource='${datasource}',
        gridPosition={ x: 8, y: 4, w: 8, h: 8 },
        unit='none',
        thresholds={ color: 'green', value: null }
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Objects',
          range=true
        ),
      ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'unit', value: 'decbytes' }] }] } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },

      $.addBarGaugePanel(
        title='Total Latencies',
        datasource='${datasource}',
        gridPosition={ x: 16, y: 4, w: 8, h: 8 },
        unit='none',
        thresholds={ color: 'green', value: null }
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_buckets_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Bucket',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_get_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_bucket_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Bucket',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Object',
          range=true
        ),
      ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'unit', value: 'ms' }] }] } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


      $.addTableExtended(
        datasource='${datasource}',
        title='Summary Per Bucket by Bandwidth',
        gridPosition={ h: 8, w: 12, x: 0, y: 12 },
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
          sortBy: [
            {
              desc: true,
              displayName: 'PUTs',
            },
          ],
        },
        custom={ align: 'auto', cellOptions: { type: 'color-text' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
          ],
        },
        overrides=[{
          matcher: { id: 'byType', options: 'number' },
          properties: [
            { id: 'unit', value: 'decbytes' },
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
          id: 'groupBy',
          options: {
            fields: {
              Bucket: {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #A': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #B': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #D': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #F': {
                aggregations: [],
                operation: 'groupby',
              },
              bucket: {
                aggregations: [],
                operation: 'groupby',
              },
              ceph_daemon: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              'Time 6': true,
              'Time 7': true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              '__name__ 6': true,
              '__name__ 7': true,
              'ceph_daemon 1': false,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'instance 1': true,
              'instance 2': true,
              'instance 3': true,
              'instance 4': true,
              'instance 5': true,
              'instance 6': true,
              'instance 7': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'instance_id 6': true,
              'instance_id 7': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
              'job 6': true,
              'job 7': true,
            },
            indexByName: {
              'Value #A': 2,
              'Value #B': 3,
              'Value #D': 4,
              'Value #F': 5,
              bucket: 1,
              ceph_daemon: 0,
            },
            renameByName: {
              Bucket: '',
              'Value #A': 'PUTs',
              'Value #B': 'GETs',
              'Value #C': 'List',
              'Value #D': 'Delete',
              'Value #E': 'Copy',
              'Value #F': 'Copy',
              'Value #G': '',
              bucket: 'Bucket',
              ceph_daemon: 'Daemon',
              'ceph_daemon 1': 'Daemon',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_del_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
          expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_copy_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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


      $.addTableExtended(
        datasource='${datasource}',
        title='Latency(ms) Per Bucket',
        gridPosition={ h: 8, w: 12, x: 12, y: 12 },
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
          sortBy: [
            {
              desc: true,
              displayName: 'PUTs',
            },
          ],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
          ],
        },
        overrides=[{
          matcher: { id: 'byType', options: 'number' },
          properties: [
            { id: 'unit', value: 'ms' },
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
          id: 'joinByField',
          options: {
            byField: 'Bucket',
            mode: 'outer',
          },
        },
        {
          id: 'groupBy',
          options: {
            fields: {
              Bucket: {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #A': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #B': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #C': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #D': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #F': {
                aggregations: [],
                operation: 'groupby',
              },
              bucket: {
                aggregations: [],
                operation: 'groupby',
              },
              ceph_daemon: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              'Time 6': true,
              'Time 7': true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              '__name__ 6': true,
              '__name__ 7': true,
              'ceph_daemon 1': true,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'ceph_daemon 5': true,
              'instance 1': true,
              'instance 2': true,
              'instance 3': true,
              'instance 4': true,
              'instance 5': true,
              'instance 6': true,
              'instance 7': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'instance_id 6': true,
              'instance_id 7': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
              'job 6': true,
              'job 7': true,
            },
            indexByName: {
              'Value #A': 2,
              'Value #B': 3,
              'Value #C': 4,
              'Value #D': 5,
              'Value #F': 6,
              bucket: 1,
              ceph_daemon: 0,
            },
            renameByName: {
              Bucket: '',
              'Value #A': 'PUTs',
              'Value #B': 'GETs',
              'Value #C': 'List',
              'Value #D': 'Delete',
              'Value #E': 'Copy',
              'Value #F': 'Copy',
              'Value #G': '',
              bucket: 'Bucket',
              ceph_daemon: 'Daemon',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_op_per_bucket_list_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_bucket_put_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_bucket_get_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_bucket_del_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_bucket_copy_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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


      $.addTableExtended(
        datasource='${datasource}',
        title='Summary Per User By Bandwidth',
        gridPosition={ h: 8, w: 12, x: 0, y: 20 },
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
          sortBy: [
            {
              desc: true,
              displayName: 'PUTs',
            },
          ],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
          ],
        },
        overrides=[{
          matcher: { id: 'byType', options: 'number' },
          properties: [
            { id: 'unit', value: 'decbytes' },
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
          id: 'groupBy',
          options: {
            fields: {
              User: {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #A': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #B': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #D': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #F': {
                aggregations: [],
                operation: 'groupby',
              },
              ceph_daemon: {
                aggregations: [],
                operation: 'groupby',
              },
              instance: {
                aggregations: [],
              },
              user: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              'Time 6': true,
              'Time 7': true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              '__name__ 6': true,
              '__name__ 7': true,
              'ceph_daemon 1': true,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'instance 1': true,
              'instance 2': true,
              'instance 3': true,
              'instance 4': true,
              'instance 5': true,
              'instance 6': true,
              'instance 7': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'instance_id 6': true,
              'instance_id 7': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
              'job 6': true,
              'job 7': true,
            },
            indexByName: {
              'Value #A': 2,
              'Value #B': 3,
              'Value #D': 4,
              'Value #F': 5,
              ceph_daemon: 0,
              user: 1,
            },
            renameByName: {
              Bucket: '',
              'Value #A': 'PUTs',
              'Value #B': 'GETs',
              'Value #C': 'List',
              'Value #D': 'Delete',
              'Value #E': 'Copy',
              'Value #F': 'Copy',
              'Value #G': '',
              ceph_daemon: 'Daemon',
              user: 'User',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_op_per_user_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_del_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_copy_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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


      $.addTableExtended(
        datasource='${datasource}',
        title='Latency(ms) Per User',
        gridPosition={ h: 8, w: 12, x: 12, y: 20 },
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
          sortBy: [
            {
              desc: true,
              displayName: 'PUTs',
            },
          ],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
          ],
        },
        overrides=[{
          matcher: { id: 'byType', options: 'number' },
          properties: [
            { id: 'unit', value: 'ms' },
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
          id: 'joinByField',
          options: {
            byField: 'User',
            mode: 'outer',
          },
        },
        {
          id: 'groupBy',
          options: {
            fields: {
              User: {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #A': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #B': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #C': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #D': {
                aggregations: [],
                operation: 'groupby',
              },
              'Value #F': {
                aggregations: [],
                operation: 'groupby',
              },
              ceph_daemon: {
                aggregations: [],
                operation: 'groupby',
              },
              user: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              'Time 6': true,
              'Time 7': true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              '__name__ 4': true,
              '__name__ 5': true,
              '__name__ 6': true,
              '__name__ 7': true,
              'ceph_daemon 1': true,
              'ceph_daemon 2': true,
              'ceph_daemon 3': true,
              'ceph_daemon 4': true,
              'ceph_daemon 5': true,
              'instance 1': true,
              'instance 2': true,
              'instance 3': true,
              'instance 4': true,
              'instance 5': true,
              'instance 6': true,
              'instance 7': true,
              'instance_id 1': true,
              'instance_id 2': true,
              'instance_id 3': true,
              'instance_id 4': true,
              'instance_id 5': true,
              'instance_id 6': true,
              'instance_id 7': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'job 4': true,
              'job 5': true,
              'job 6': true,
              'job 7': true,
            },
            indexByName: {
              'Value #A': 2,
              'Value #B': 3,
              'Value #C': 4,
              'Value #D': 5,
              'Value #F': 6,
              ceph_daemon: 0,
              user: 1,
            },
            renameByName: {
              Bucket: '',
              'Value #A': 'PUTs',
              'Value #B': 'GETs',
              'Value #C': 'List',
              'Value #D': 'Delete',
              'Value #E': 'Copy',
              'Value #F': 'Copy',
              'Value #G': '',
              ceph_daemon: 'Daemon',
              user: 'User',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_rgw_op_per_user_list_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_put_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_get_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_del_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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
          expr='ceph_rgw_op_per_user_copy_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s}' % $.matchers(),
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


      $.addRowSchema(false, true, 'Buckets', collapsed=true)
      .addPanels([
        $.addBarGaugePanel(
          title='Top 5 Bucket PUTs by Operations',
          datasource='${datasource}',
          gridPosition={ x: 0, y: 29, w: 6, h: 8 },
          unit='none',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{bucket}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }] } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Bucket GETs by Operations',
          datasource='${datasource}',
          gridPosition={ x: 6, y: 29, w: 6, h: 8 },
          unit='none',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{bucket}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }] } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Buckets PUTs By Size',
          datasource='${datasource}',
          gridPosition={ x: 12, y: 29, w: 6, h: 8 },
          unit='decbytes',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5,\n    sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{bucket}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } } } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: [] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Buckets GETs By Size',
          datasource='${datasource}',
          gridPosition={ x: 18, y: 29, w: 6, h: 8 },
          unit='decbytes',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5,\n    sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{bucket}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } } } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: [] }, displayMode: 'gradient' } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket PUTs by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 0, y: 37 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket GETs by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 6, y: 37 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket Copy by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 12, y: 37 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_copy_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket Delete by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 18, y: 37 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_del_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket GETs by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 0, y: 45 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_get_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket PUTs by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 6, y: 45 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_put_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket List by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 12, y: 45 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_list_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket Delete by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 18, y: 45 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_del_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Bucket Copy by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 12, x: 0, y: 53 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (bucket, ceph_daemon) ((ceph_rgw_op_per_bucket_copy_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{bucket}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.addTableExtended(
          datasource='${datasource}',
          title='Summary Per Bucket by Operations',
          gridPosition={ h: 8, w: 12, x: 12, y: 53 },
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
            sortBy: [
              {
                desc: true,
                displayName: 'PUTs',
              },
            ],
          },
          custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
          thresholds={
            mode: 'absolute',
            steps: [
              { color: 'green' },
            ],
          },
          overrides=[{
            matcher: { id: 'byType', options: 'number' },
            properties: [
              { id: 'unit', value: 'none' },
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
            id: 'joinByField',
            options: {
              byField: 'Bucket',
              mode: 'outer',
            },
          },
          {
            id: 'groupBy',
            options: {
              fields: {
                Bucket: {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #A': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #B': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #C': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #D': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #F': {
                  aggregations: [],
                  operation: 'groupby',
                },
                bucket: {
                  aggregations: [],
                  operation: 'groupby',
                },
                ceph_daemon: {
                  aggregations: [],
                  operation: 'groupby',
                },
              },
            },
          },
          {
            id: 'organize',
            options: {
              excludeByName: {
                'Time 1': true,
                'Time 2': true,
                'Time 3': true,
                'Time 4': true,
                'Time 5': true,
                'Time 6': true,
                'Time 7': true,
                __name__: true,
                '__name__ 1': true,
                '__name__ 2': true,
                '__name__ 3': true,
                '__name__ 4': true,
                '__name__ 5': true,
                '__name__ 6': true,
                '__name__ 7': true,
                'ceph_daemon 1': true,
                'ceph_daemon 2': true,
                'ceph_daemon 3': true,
                'ceph_daemon 4': true,
                'instance 1': true,
                'instance 2': true,
                'instance 3': true,
                'instance 4': true,
                'instance 5': true,
                'instance 6': true,
                'instance 7': true,
                'instance_id 1': true,
                'instance_id 2': true,
                'instance_id 3': true,
                'instance_id 4': true,
                'instance_id 5': true,
                'instance_id 6': true,
                'instance_id 7': true,
                'job 1': true,
                'job 2': true,
                'job 3': true,
                'job 4': true,
                'job 5': true,
                'job 6': true,
                'job 7': true,
              },
              indexByName: {
                'Value #A': 2,
                'Value #B': 3,
                'Value #C': 4,
                'Value #D': 5,
                'Value #F': 6,
                bucket: 1,
                ceph_daemon: 0,
              },
              renameByName: {
                Bucket: '',
                'Value #A': 'PUTs',
                'Value #B': 'GETs',
                'Value #C': 'List',
                'Value #D': 'Delete',
                'Value #E': 'Copy',
                'Value #F': 'Copy',
                'Value #G': '',
                bucket: 'Bucket',
                ceph_daemon: 'Daemon',
              },
            },
          },
        ]).addTargets([
          $.addTargetSchema(
            expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_get_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_del_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_copy_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (bucket, ceph_daemon) (ceph_rgw_op_per_bucket_list_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
      ]) + { gridPos: { x: 0, y: 28, w: 24, h: 1 } },


      $.addRowSchema(false, true, 'Users', collapsed=true)
      .addPanels([
        $.addBarGaugePanel(
          title='Top 5 Users PUTs By Operations',
          datasource='${datasource}',
          gridPosition={ x: 0, y: 62, w: 6, h: 8 },
          unit='none',
          thresholds={ color: 'green' }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_put_obj_ops ) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)\n' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{user}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }] } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Users GETs by Operations',
          datasource='${datasource}',
          gridPosition={ x: 6, y: 62, w: 6, h: 8 },
          unit='none',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_get_obj_ops ) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)\n' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{user}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } }, overrides: [{ matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] }] } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Users PUTs by Size',
          datasource='${datasource}',
          gridPosition={ x: 12, y: 62, w: 6, h: 8 },
          unit='decbytes',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_put_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{user}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } } } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: [] }, displayMode: 'gradient' } },


        $.addBarGaugePanel(
          title='Top 5 Users GETs By Size',
          datasource='${datasource}',
          gridPosition={ x: 18, y: 62, w: 6, h: 8 },
          unit='decbytes',
          thresholds={ color: 'green', value: null }
        )
        .addTargets([
          $.addTargetSchema(
            expr='topk(5, \n    sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_get_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})\n)' % $.matchers(),
            datasource='${datasource}',
            legendFormat='{{ceph_daemon}} - {{user}}',
            range=false,
            instant=true
          ),
        ]) + { fieldConfig: { defaults: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }] } } } }
        + { options: { orientation: 'horizontal', reduceOptions: { calcs: [] }, displayMode: 'gradient' } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User PUTs by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 0, y: 70 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_put_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User GETs by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 6, y: 70 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_get_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User Delete by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 12, y: 70 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_del_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User COPY by Size',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 18, y: 70 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_copy_obj_bytes) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User GETs by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 0, y: 78 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_get_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User PUTs by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 6, y: 78 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_put_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User List by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 12, y: 78 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_list_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User Delete by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 6, x: 18, y: 78 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_del_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='User Copy by Operations',
          datasource='${datasource}',
          gridPosition={ h: 8, w: 12, x: 0, y: 86 },
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
          sortDesc=true
        )
        .addThresholds([
          { color: 'green' },
        ])
        .addOverrides([
          { matcher: { id: 'byType', unit: 'number' }, properties: [{ id: 'color' }, { id: 'color', value: { mode: 'palette-classic' } }] },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='sum by (user, ceph_daemon) ((ceph_rgw_op_per_user_copy_obj_ops) *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
              datasource='${datasource}',
              format='time_series',
              instant=false,
              legendFormat='{{ceph_daemon}} - {{user}}',
              step=300,
              range=true,
            ),
          ]
        ) + { options: { legend: { calcs: ['lastNotNull'], displayMode: 'table', placement: 'bottom', showLegend: true, sortBy: 'Last *', sortDesc: true }, tooltip: { mode: 'single', sort: 'desc' } } },


        $.addTableExtended(
          datasource='${datasource}',
          title='Summary Per User By Operations',
          gridPosition={ h: 8, w: 12, x: 12, y: 86 },
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
            sortBy: [
              {
                desc: true,
                displayName: 'PUTs',
              },
            ],
          },
          custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
          thresholds={
            mode: 'absolute',
            steps: [
              { color: 'green' },
            ],
          },
          overrides=[{
            matcher: { id: 'byType', options: 'number' },
            properties: [
              { id: 'unit', value: 'none' },
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
            id: 'joinByField',
            options: {
              byField: 'User',
              mode: 'outer',
            },
          },
          {
            id: 'groupBy',
            options: {
              fields: {
                User: {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #A': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #B': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #C': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #D': {
                  aggregations: [],
                  operation: 'groupby',
                },
                'Value #F': {
                  aggregations: [],
                  operation: 'groupby',
                },
                ceph_daemon: {
                  aggregations: [],
                  operation: 'groupby',
                },
                user: {
                  aggregations: [],
                  operation: 'groupby',
                },
              },
            },
          },
          {
            id: 'organize',
            options: {
              excludeByName: {},
              indexByName: {
                'Value #A': 2,
                'Value #B': 3,
                'Value #C': 4,
                'Value #D': 5,
                'Value #F': 6,
                ceph_daemon: 0,
                user: 1,
              },
              renameByName: {
                'Value #A': 'PUTs',
                'Value #B': 'GETs',
                'Value #C': 'LIST',
                'Value #D': 'DELETE',
                'Value #F': 'COPY',
                ceph_daemon: 'Daemon',
                user: 'User',
              },
            },
          },
        ]).addTargets([
          $.addTargetSchema(
            expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_get_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_del_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_copy_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
            expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_list_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_servers", %(matchers)s})' % $.matchers(),
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
      ]) + { gridPos: { x: 0, y: 29, w: 24, h: 1 } },
    ]),
}
