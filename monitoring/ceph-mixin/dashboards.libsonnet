{
  grafanaDashboards+::
    (import 'dashboards/cephfs.libsonnet') +
    (import 'dashboards/host.libsonnet') +
    (import 'dashboards/osd.libsonnet') +
    (import 'dashboards/pool.libsonnet') +
    (import 'dashboards/rbd.libsonnet') +
    (import 'dashboards/rgw.libsonnet') +
    (import 'dashboards/ceph-cluster.libsonnet') +
    (import 'dashboards/rgw-s3-analytics.libsonnet') +
    (import 'dashboards/multi-cluster.libsonnet') +
    { _config:: $._config },
}
