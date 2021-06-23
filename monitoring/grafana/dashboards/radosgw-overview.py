from grafanalib.core import (
    Annotations, Dashboard, Graph, GridPos, NULL_AS_NULL,
    RowPanel, Templating, Tooltip, XAxis, Target, YAxes, YAxis,
)

dashboard = Dashboard(
    title="RGW Overview",
    refresh="15s",
    schemaVersion=16,
    uid="WAkugZpiz",
    version=2,
    templating=Templating(list=[{
        "allValue": NULL_AS_NULL,
          "current": {},
          "datasource": "$datasource",
          "hide": 2,
          "includeAll": True,
          "label": NULL_AS_NULL,
          "multi": False,
          "name": "rgw_servers",
          "options": [],
          "query": "label_values(ceph_rgw_req, ceph_daemon)",
          "refresh": 1,
          "regex": "",
          "sort": 1,
          "tagValuesQuery": "",
          "tags": [],
          "tagsQuery": "",
          "type": "query",
          "useTags": False
        },
        {
          "current": {
          "tags": [],
          "text": "default",
          "value": "default"
          },
          "hide": 0,
          "label": "Data Source",
          "name": "datasource",
          "options": [],
          "query": "prometheus",
          "refresh": 1,
          "regex": "",
          "type": "datasource"
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
        RowPanel(
            title="RGW Overview - All Gateways",
            gridPos=GridPos(h=1, w=24, x=0, y=0),
            id=2
        ),
        Graph(
            title="Average GET/PUT Latencies",
            tooltip=Tooltip(
                shared=True,
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=29,
            renderer="flot",
            lineWidth=1,
            gridPos=GridPos(h=7, w=8, x=0, y=1),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='rate(ceph_rgw_get_initial_lat_sum[30s]) / rate(ceph_rgw_get_initial_lat_count[30s])',
                    legendFormat="GET AVG",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
                Target(
                    expr='rate(ceph_rgw_put_initial_lat_sum[30s]) / rate(ceph_rgw_put_initial_lat_count[30s])',
                    legendFormat="PUT AVG",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='s',
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
            title="Total Requests/sec by RGW Instance",
            tooltip=Tooltip(
                shared=True,
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=4,
            renderer="flot",
            lineWidth=1,
            gridPos=GridPos(h=7, w=7, x=8, y=1),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='sum by(rgw_host) (label_replace(rate(ceph_rgw_req[30s]), \"rgw_host\", \"$1\", \"ceph_daemon\", \"rgw.(.*)\"))',
                    legendFormat="{{rgw_host}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
            ],
            yAxes=YAxes(
                YAxis(
                    format='none',
                    decimals=0,
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
            title="GET Latencies by RGW Instance",
            description="Latencies are shown stacked, without a yaxis to provide a visual indication of GET latency imbalance across RGW hosts",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=31,
            gridPos=GridPos(h=7, w=6, x=15, y=1),
            targets=[
                Target(
                    expr='label_replace(rate(ceph_rgw_get_initial_lat_sum[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\") / \nlabel_replace(rate(ceph_rgw_get_initial_lat_count[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")',
                    legendFormat="{{rgw_host}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='s',
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
            title="Bandwidth Consumed by Type",
            description="Total bytes transferred in/out of all radosgw instances within the cluster",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=6,
            gridPos=GridPos(h=6, w=8, x=0, y=8),
            targets=[
                Target(
                    expr='sum(rate(ceph_rgw_get_b[30s]))',
                    legendFormat="GETs",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
                Target(
                    expr='sum(rate(ceph_rgw_put_b[30s]))',
                    legendFormat="PUTs",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='bytes',
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
            title="Bandwidth by RGW Instance",
            description="Total bytes transferred in/out through get/put operations, by radosgw instance",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=9,
            gridPos=GridPos(h=6, w=7, x=8, y=8),
            targets=[
                Target(
                    expr='sum by(rgw_host) (\n  (label_replace(rate(ceph_rgw_get_b[30s]), \"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")) + \n  (label_replace(rate(ceph_rgw_put_b[30s]), \"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\"))\n)',
                    legendFormat="{{rgw_host}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
            ],
            yAxes=YAxes(
                YAxis(
                    format='bytes',
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
            title="PUT Latencies by RGW Instance",
            description="Latencies are shown stacked, without a yaxis to provide a visual indication of PUT latency imbalance across RGW hosts",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=32,
            gridPos=GridPos(h=6, w=6, x=15, y=8),
            targets=[
                Target(
                    expr='label_replace(rate(ceph_rgw_put_initial_lat_sum[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\") / \nlabel_replace(rate(ceph_rgw_put_initial_lat_count[30s]),\"rgw_host\",\"$1\",\"ceph_daemon\",\"rgw.(.*)\")',
                    legendFormat="{{rgw_host}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
            ],
            yAxes=YAxes(
                YAxis(
                    format='s',
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