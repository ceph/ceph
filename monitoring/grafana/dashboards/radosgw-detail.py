from grafanalib.core import (
    Annotations, Dashboard, Graph, GridPos,
    PieChart, RowPanel, SeriesOverride, Templating, Tooltip, XAxis, Target, YAxes, YAxis,
)

dashboard = Dashboard(
    title="RGW Instance Detail",
    refresh="15s",
    schemaVersion=16,
    uid="x5ARzZtmk",
    version=2,
    templating=Templating(list=[{
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
        },
        {
          "current": {},
          "datasource": "$datasource",
          "hide": 0,
          "includeAll": True,
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
            title="RGW Host Detail : $rgw_servers",
            gridPos=GridPos(h=1, w=24, x=0, y=0),
            id=12
        ),
        Graph(
            title="$rgw_servers GET/PUT Latencies",
            tooltip=Tooltip(
                shared=True,
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=34,
            renderer="flot",
            lineWidth=1,
            gridPos=GridPos(h=8, w=6, x=0, y=1),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='sum by (ceph_daemon) (rate(ceph_rgw_get_initial_lat_sum{ceph_daemon=~\"($rgw_servers)\"}[30s]) / rate(ceph_rgw_get_initial_lat_count{ceph_daemon=~\"($rgw_servers)\"}[30s]))',
                    legendFormat="GET {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
                Target(
                    expr='sum by (ceph_daemon)(rate(ceph_rgw_put_initial_lat_sum{ceph_daemon=~\"($rgw_servers)\"}[30s]) / rate(ceph_rgw_put_initial_lat_count{ceph_daemon=~\"($rgw_servers)\"}[30s]))',
                    legendFormat="PUT {{ceph_daemon}}",
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
            title="Bandwidth by HTTP Operation",
            tooltip=Tooltip(
                shared=True,
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=18,
            renderer="flot",
            lineWidth=1,
            gridPos=GridPos(h=8, w=7, x=6, y=1),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='rate(ceph_rgw_get_b{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="GETs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                ),
                Target(
                    expr='rate(ceph_rgw_put_b{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="PUTs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
            ],
            yAxes=YAxes(
                YAxis(
                    format='bytes',
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
            aliasColors={
                "GETs": "#7eb26d",
                "Other": "#447ebc",
                "PUTs": "#eab839",
                "Requests": "#3f2b5b",
                "Requests Failed": "#bf1b00"
            },
            title="HTTP Request Breakdown",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=14,
            seriesOverrides=SeriesOverride(
                 alias= "reads",
            ),
            gridPos=GridPos(h=8, w=7, x=13, y=1),
            targets=[
                Target(
                    expr='rate(ceph_rgw_failed_req{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="Requests Failed {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                ),
                Target(
                    expr='rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="GETs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='C',
                ),
                Target(
                    expr='rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="PUTs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='D',
                ),
                Target(
                    expr='rate(ceph_rgw_req{ceph_daemon=~\"$rgw_servers\"}[30s]) -\n  (rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s]) +\n   rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s]))',
                    legendFormat="Other {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='short',
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
        PieChart(
            title="Workload Breakdown",
            dataSource="$datasource",
            id=23,
            legendType="Under graph",
            maxDataPoints=3,
            pieType="pie",
            gridPos=GridPos(h=8, w=4, x=20, y=1),
            targets=[
                Target(
                    expr='rate(ceph_rgw_failed_req{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="Failures {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='A',
                ),
                Target(
                    expr='rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="GETs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='B',
                ),
                Target(
                    expr='rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s])',
                    legendFormat="PUTs {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='C',
                ),
                Target(
                    expr='rate(ceph_rgw_req{ceph_daemon=~\"$rgw_servers\"}[30s]) -\n  (rate(ceph_rgw_get{ceph_daemon=~\"$rgw_servers\"}[30s]) +\n   rate(ceph_rgw_put{ceph_daemon=~\"$rgw_servers\"}[30s]))',
                    legendFormat="Other (DELETE,LIST) {{ceph_daemon}}",
                    format="time_Series",
                    intervalFactor=1,
                    refId='D',
                )
            ],
        ),
    ]
)