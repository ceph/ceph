from grafanalib.core import (
    Annotations, Dashboard, Graph, GridPos,
    NULL_AS_NULL, SingleStat, Table, Templating, Tooltip, XAxis, Target, YAxes, YAxis
)


dashboard = Dashboard(
    title="Ceph Pools Overview",
    refresh="15s",
    schemaVersion=22,
    timezone="browser",
    uid="z99hzWtmk",
    version=10,
    templating=Templating(list=[{
          "current": {
            "selected": False,
            "text": "Dashboard1",
            "value": "Dashboard1"
          },
          "hide": 0,
          "includeAll": False,
          "label": "Data Source",
          "multi": False,
          "name": "datasource",
          "options": [],
          "query": "prometheus",
          "refresh": 1,
          "regex": "",
          "skipUrlSync": False,
          "type": "datasource"
        },
        {
          "current": {
            "text": "15",
            "value": "15"
          },
          "hide": 0,
          "label": "Top K",
          "name": "topk",
          "options": [
            {
              "text": "15",
              "value": "15"
            }
          ],
          "query": "15",
          "skipUrlSync": False,
          "type": "textbox"
    }]),
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
            title="Pool",
            dataSource="$datasource",
            id=21,
            gridPos=GridPos(h=3, w=3, x=0, y=0),
            targets=[
                Target(
                    expr='count(ceph_pool_metadata)',
                    legendFormat="",
                    refId='A',
                    format="table"
                )
            ],
        ),
        SingleStat(
            title="Pools with Compression",
            dataSource="$datasource",
            id=7,
            gridPos=GridPos(h=3, w=3, x=3, y=0),
            description='Count of the pools that have compression enabled',
            targets=[
                Target(
                    expr='count(ceph_pool_metadata{compression_mode!=\"none\"})',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Total Raw Capacity",
            dataSource="$datasource",
            decimals=1,
            format="bytes",
            id=27,
            gridPos=GridPos(h=3, w=3, x=6, y=0),
            description='Total raw capacity available to the cluster',
            targets=[
                Target(
                    expr='sum(ceph_osd_stat_bytes)',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Raw Capacity Consumed",
            dataSource="$datasource",
            decimals=2,
            format="bytes",
            id=25,
            gridPos=GridPos(h=3, w=3, x=9, y=0),
            description='Total raw capacity consumed by user data and associated overheads (metadata + redundancy)',
            targets=[
                Target(
                    expr='sum(ceph_pool_bytes_used)',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Logical Stored ",
            dataSource="$datasource",
            decimals=1,
            format="bytes",
            id=23,
            gridPos=GridPos(h=3, w=3, x=12, y=0),
            description='Total of client data stored in the cluster',
            targets=[
                Target(
                    expr='sum(ceph_pool_stored)',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Compression Savings",
            dataSource="$datasource",
            decimals=1,
            format="bytes",
            id=9,
            gridPos=GridPos(h=3, w=3, x=15, y=0),
            description='A compression saving is determined as the data eligible to be compressed minus the capacity used to store the data after compression',
            targets=[
                Target(
                    expr='sum(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used)',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Compression Eligibility",
            dataSource="$datasource",
            format="percent",
            id=17,
            gridPos=GridPos(h=3, w=3, x=18, y=0),
            description='Indicates how suitable the data is within the pools that are/have been enabled for compression - averaged across all pools holding compressed data\n',
            targets=[
                Target(
                    expr='(sum(ceph_pool_compress_under_bytes > 0) / sum(ceph_pool_stored_raw and ceph_pool_compress_under_bytes > 0)) * 100',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        SingleStat(
            title="Compression Factor",
            dataSource="$datasource",
            format="none",
            id=15,
            gridPos=GridPos(h=3, w=3, x=21, y=0),
            description='This factor describes the average ratio of data eligible to be compressed divided by the data actually stored. It does not account for data written that was ineligible for compression (too small, or compression yield too low)',
            targets=[
                Target(
                    expr='sum(ceph_pool_compress_under_bytes > 0) / sum(ceph_pool_compress_bytes_used > 0)',
                    legendFormat="",
                    refId='A',
                )
            ],
        ),
        Table(
            title="Pool Overview",
            dataSource="$datasource",
            id=5,
            gridPos=GridPos(h=6, w=24, x=0, y=3),
            transform="table",
            styles=[{
                "alias": "",
                "decimals": 2,
                "pattern": "Time",
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "",
                "decimals": 2,
                "pattern": "instance",
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "",
                "decimals": 2,
                "pattern": "job",
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "Pool Name",
                "align": "auto",
                "colorMode": NULL_AS_NULL,
                "colors": [
                "rgba(245, 54, 54, 0.9)",
                "rgba(237, 129, 40, 0.89)",
                "rgba(50, 172, 45, 0.97)"
                ],
                "dateFormat": "YYYY-MM-DD HH:mm:ss",
                "decimals": 2,
                "pattern": "name",
                "thresholds": [],
                "type": "string",
                "unit": "short"
            },
            {
                "alias": "Pool ID",
                "decimals": 0,
                "pattern": "pool_id",
                "type": "hidden",
                "unit": "none"
            },
            {
                "alias": "Compression Factor",
                "decimals": 1,
                "mappingType": 1,
                "pattern": "Value #A",
                "type": "number",
                "unit": "none"
            },
            {
                "alias": "% Used",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "Value #D",
                "thresholds": [
                "70",
                "85"
                ],
                "type": "number",
                "unit": "percentunit"
            },
            {
                "alias": "Usable Free",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "Value #B",
                "thresholds": [],
                "type": "number",
                "unit": "bytes"
            },
            {
                "alias": "Compression Eligibility",
                "decimals": 0,
                "mappingType": 1,
                "pattern": "Value #C",
                "thresholds": [],
                "type": "number",
                "unit": "percent"
            },
            {
                "alias": "Compression Savings",
                "decimals": 1,
                "mappingType": 1,
                "pattern": "Value #E",
                "thresholds": [],
                "type": "number",
                "unit": "bytes"
            },
            {
                "alias": "Growth (5d)",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "Value #F",
                "thresholds": [
                "0",
                "0"
                ],
                "type": "number",
                "unit": "bytes"
            },
            {
                "alias": "IOPS",
                "decimals": 0,
                "mappingType": 1,
                "pattern": "Value #G",
                "thresholds": [],
                "type": "number",
                "unit": "none"
            },
            {
                "alias": "Bandwidth",
                "decimals": 0,
                "mappingType": 1,
                "pattern": "Value #H",
                "thresholds": [],
                "type": "number",
                "unit": "Bps"
            },
            {
                "alias": "",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "__name__",
                "thresholds": [],
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "type",
                "thresholds": [],
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "compression_mode",
                "thresholds": [],
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "Type",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "description",
                "thresholds": [],
                "type": "string",
                "unit": "short"
            },
            {
                "alias": "Stored",
                "decimals": 1,
                "mappingType": 1,
                "pattern": "Value #J",
                "thresholds": [],
                "type": "number",
                "unit": "bytes"
            },
            {
                "alias": "",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "Value #I",
                "thresholds": [],
                "type": "hidden",
                "unit": "short"
            },
            {
                "alias": "Compression",
                "decimals": 2,
                "mappingType": 1,
                "pattern": "Value #K",
                "thresholds": [],
                "type": "string",
                "unit": "short",
                "valueMaps": [
                {
                    "text": "ON",
                    "value": "1"
                }
                ]
            }
            ],
            targets=[
                Target(
                    expr='(ceph_pool_percent_used * on(pool_id) group_left(name) ceph_pool_metadata)',
                    format="table",
                    legendFormat="",
                    refId='D',
                ),
                Target(
                    expr='ceph_pool_stored * on(pool_id) group_left ceph_pool_metadata',
                    format="table",
                    legendFormat="",
                    refId='J',
                ),
                Target(
                    expr='ceph_pool_max_avail * on(pool_id) group_left(name) ceph_pool_metadata',
                    format="table",
                    legendFormat="",
                    refId='B',
                ),
                Target(
                    expr='delta(ceph_pool_stored[5d])',
                    format="table",
                    legendFormat="",
                    refId='F',
                ),
                Target(
                    expr='ceph_pool_metadata',
                    format="table",
                    legendFormat="",
                    refId='I',
                ),
                Target(
                    expr='ceph_pool_metadata{compression_mode!=\"none\"}',
                    format="table",
                    legendFormat="",
                    refId='K',
                ),
                Target(
                    expr='(ceph_pool_compress_under_bytes / ceph_pool_compress_bytes_used > 0) and on(pool_id) (((ceph_pool_compress_under_bytes > 0) / ceph_pool_stored_raw) * 100 > 0.5)',
                    format="table",
                    legendFormat="",
                    refId='A',
                ),
                Target(
                    expr='((ceph_pool_compress_under_bytes > 0) / ceph_pool_stored_raw) * 100',
                    format="table",
                    legendFormat="",
                    refId='C',
                ),
                Target(
                    expr='(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used > 0)',
                    format="table",
                    legendFormat="",
                    refId='E',
                ),
                Target(
                    expr='rate(ceph_pool_rd[30s]) + rate(ceph_pool_wr[30s])',
                    format="table",
                    legendFormat="",
                    refId='G',
                ),
                Target(
                    expr='rate(ceph_pool_rd_bytes[30s]) + rate(ceph_pool_wr_bytes[30s])',
                    format="table",
                    legendFormat="",
                    refId='H',
                ),
                Target(
                    expr='(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used > 0)',
                    format="table",
                    legendFormat="",
                    refId='L',
                ),
            ],
        ),
        Graph(
            title="Top $topk Client IOPS by Pool",
            tooltip=Tooltip(
                sort=2,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=1,
            gridPos=GridPos(h=8, w=12, x=0, y=9),
            description='This chart shows the sum of read and write IOPS from all clients by pool',
            targets=[
                Target(
                    expr='topk($topk,round((rate(ceph_pool_rd[30s]) + rate(ceph_pool_wr[30s])),1) * on(pool_id) group_left(instance,name) ceph_pool_metadata) ',
                    legendFormat="{{name}} ",
                    hide = False,
                    format="time_Series",
                    refId='F',
                ),
                Target(
                    expr='topk($topk,rate(ceph_pool_wr[30s]) + on(pool_id) group_left(instance,name) ceph_pool_metadata) ',
                    legendFormat="{{name}} - write",
                    hide = True,
                    format="time_Series",
                    refId='A',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='short',
                    label='IOPS',
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
            title="Top $topk Client Bandwidth by Pool",
            tooltip=Tooltip(
                sort=2,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=2,
            gridPos=GridPos(h=8, w=12, x=12, y=9),
            description='The chart shows the sum of read and write bytes from all clients, by pool',
            targets=[
                Target(
                    expr='topk($topk,(rate(ceph_pool_rd_bytes[30s]) + rate(ceph_pool_wr_bytes[30s])) * on(pool_id) group_left(instance,name) ceph_pool_metadata)',
                    legendFormat="{{name}} ",
                    hide = False,
                    format="time_Series",
                    refId='A',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='Bps',
                    label='Throughput',
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
            title="Pool Capacity Usage (RAW)",
            tooltip=Tooltip(
                sort=0,
                valueType='individual'
            ),
            dataSource="$datasource",
            id=19,
            gridPos=GridPos(h=7, w=24, x=0, y=17),
            timeFrom="14d",
            description='Historical view of capacity usage, to help identify growth and trends in pool consumption',
            targets=[
                Target(
                    expr='ceph_pool_bytes_used * on(pool_id) group_right ceph_pool_metadata',
                    legendFormat="{{name}} ",
                    hide = False,
                    refId='A',
                )
            ],
            yAxes=YAxes(
                YAxis(
                    format='bytes',
                    label='Capacity Used',
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
        )
    ]
)