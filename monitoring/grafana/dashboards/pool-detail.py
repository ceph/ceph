from grafanalib.core import (
    Annotations, Dashboard, Gauge, Graph, GridPos,
    NULL_AS_NULL, SeriesOverride, SingleStat, SparkLine, Templating, Tooltip, XAxis, Target, YAxes, YAxis
)


dashboard = Dashboard(
    title="Ceph Pool Details",
    refresh="15s",
    schemaVersion=16,
    timezone="browser",
    uid="-xyV8KCiz",
    version=1,
    templating=Templating(list=[{
        "current": {
        "text": "Prometheus admin.virt1.home.fajerski.name:9090",
        "value": "Prometheus admin.virt1.home.fajerski.name:9090"
        },
        "hide": 0,
        "label": "Data Source",
        "name": "datasource",
        "options": [],
        "query": "prometheus",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "type": "datasource"
      },
      {
        "allValue": NULL_AS_NULL,
        "current": {},
        "datasource": "$datasource",
        "hide": 0,
        "includeAll": False,
        "label": "Pool Name",
        "multi": False,
        "name": "pool_name",
        "options": [],
        "query": "label_values(ceph_pool_metadata,name)",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False
      }
    ]),
    annotations=Annotations(list=[{

        "builtIn": 1,

        "datasource": "-- Grafana --",

        "enable": True,

        "hide": True,

        "iconColor": "rgba(0, 211, 255, 1)",

        "name": "Annotations & Alerts",

        "type": "dashboard"

    }]),
    panels=[
        SingleStat(
            title="Capacity used",
            dataSource="$datasource",
            id=12,
            gridPos=GridPos(h=7, w=7, x=0, y=0),
            format="percentunit",
            gauge=Gauge(
                maxValue=1,
                minValue=0,
                show=True,
                thresholdLabels=False,
                thresholdMarkers=True
            ),
            sparkline=SparkLine(
                show=True
            ),
            thresholds= ".7,.8",
            valueName="current",
            valueFontSize="50%",
            targets=[
                Target(
                    expr='(ceph_pool_stored / (ceph_pool_stored + ceph_pool_max_avail)) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="",
                    intervalFactor=1,
                    refId='A',
                    format="time_series"
                )
            ],
        ),
        SingleStat(
            title="Time till full",
            dataSource="$datasource",
            id=14,
            gridPos=GridPos(h=7, w=5, x=7, y=0),
            format="s",
            valueName="current",
            valueFontSize="80%",
            description="Time till pool is full assuming the average fill rate of the last 6 hours",
            targets=[
                Target(
                    expr='(ceph_pool_max_avail / deriv(ceph_pool_stored[6h])) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"} > 0',
                    legendFormat="",
                    intervalFactor=1,
                    refId='A',
                    format="time_series"
                )
            ],
        ),
        Graph(
            aliasColors={
                "read_op_per_sec": "#3F6833",
                "write_op_per_sec": "#E5AC0E"
            },
            title="$pool_name Object Ingress/Egress",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=10,
            gridPos=GridPos(h=7, w=12, x=12, y=0),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='deriv(ceph_pool_objects[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="Objects per second",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='ops',
                    label='Objects out(-) / in(+) ',
                    logBase=1,
                ),
                YAxis(
                    format='short',
                    logBase=1
                    )
            ),
            xAxis=XAxis(
                mode="time",
            )
        ),
        Graph(
            aliasColors={
                "read_op_per_sec": "#3F6833",
                "write_op_per_sec": "#E5AC0E"
            },
            title="$pool_name Client IOPS",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=6,
            seriesOverrides=SeriesOverride(
                 alias= "reads",
            ),
            gridPos=GridPos(h=7, w=12, x=0, y=7),
            targets=[
                Target(
                    expr='irate(ceph_pool_rd[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="reads",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                ),
                Target(
                    expr='irate(ceph_pool_wr[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="writes",
                    format="time_Series",
                    intervalFactor=1,
                    refId='C',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='iops',
                    label='Read (-) / Write (+)',
                    logBase=1,
                ),
                YAxis(
                    format='short',
                    logBase=1
                    )
            ),
            xAxis=XAxis(
                mode="time",
            )
        ),
       Graph(
            aliasColors={
                "read_op_per_sec": "#3F6833",
                "write_op_per_sec": "#E5AC0E"
            },
            title="$pool_name Client Throughput",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=7,
            seriesOverrides=SeriesOverride(
                 alias= "reads",
            ),
            gridPos=GridPos(h=7, w=12, x=12, y=7),
            targets=[
                Target(
                    expr='irate(ceph_pool_rd_bytes[1m]) + on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="reads",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
                Target(
                    expr='irate(ceph_pool_wr_bytes[1m]) + on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="writes",
                    format="time_Series",
                    intervalFactor=1,
                    refId='C',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='Bps',
                    label='Read (-) / Write (+)',
                    logBase=1,
                ),
                YAxis(
                    format='short',
                    logBase=1
                    )
            ),
            xAxis=XAxis(
                mode="time",
            )
        ),
        Graph(
            aliasColors={
                "read_op_per_sec": "#3F6833",
                "write_op_per_sec": "#E5AC0E"
            },
            title="$pool_name Objects",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=8,
            seriesOverrides=SeriesOverride(
                 alias= "reads",
            ),
            gridPos=GridPos(h=7, w=12, x=0, y=14),
            targets=[
                Target(
                    expr='ceph_pool_objects * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~\"$pool_name\"}',
                    legendFormat="Number of Objects",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='short',
                    label='Objects',
                    logBase=1,
                ),
                YAxis(
                    format='short',
                    logBase=1
                    )
            ),
            xAxis=XAxis(
                mode="time",
            )
        ),
    ]
)