import json

import pytest

from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec, \
    ServiceSpecValidationError, IscsiServiceSpec, PlacementSpec

from orchestrator import DaemonDescription, OrchestratorError


@pytest.mark.parametrize(
    "spec_json",
    json.loads("""[
{
  "placement": {
    "count": 1
  },
  "service_type": "alertmanager"
},
{
  "placement": {
    "host_pattern": "*"
  },
  "service_type": "crash"
},
{
  "placement": {
    "count": 1
  },
  "service_type": "grafana"
},
{
  "placement": {
    "count": 2
  },
  "service_type": "mgr"
},
{
  "placement": {
    "count": 5
  },
  "service_type": "mon"
},
{
  "placement": {
    "host_pattern": "*"
  },
  "service_type": "node-exporter"
},
{
  "placement": {
    "count": 1
  },
  "service_type": "prometheus"
},
{
  "placement": {
    "hosts": [
      {
        "hostname": "ceph-001",
        "network": "",
        "name": ""
      }
    ]
  },
  "service_type": "rgw",
  "service_id": "default-rgw-realm.eu-central-1.1",
  "rgw_realm": "default-rgw-realm",
  "rgw_zone": "eu-central-1",
  "subcluster": "1"
},
{
  "service_type": "osd",
  "service_id": "osd_spec_default",
  "placement": {
    "host_pattern": "*"
  },
  "data_devices": {
    "model": "MC-55-44-XZ"
  },
  "db_devices": {
    "model": "SSD-123-foo"
  },
  "wal_devices": {
    "model": "NVME-QQQQ-987"
  }
}
]
""")
)
def test_spec_octopus(spec_json):
    # https://tracker.ceph.com/issues/44934
    # Those are real user data from early octopus.
    # Please do not modify those JSON values.

    spec = ServiceSpec.from_json(spec_json)
    # just some verification that we can sill read old octopus specs
    def convert_to_old_style_json(j):
        j_c = dict(j.copy())
        j_c.pop('service_name', None)
        if 'spec' in j_c:
            spec = j_c.pop('spec')
            j_c.update(spec)
        j_c.pop('objectstore', None)
        j_c.pop('filter_logic', None)
        return j_c
    assert spec_json == convert_to_old_style_json(spec.to_json())


@pytest.mark.parametrize(
    "dd_json",
    json.loads("""[
    {
        "hostname": "ceph-001",
        "container_id": "d94d7969094d",
        "container_image_id": "0881eb8f169f5556a292b4e2c01d683172b12830a62a9225a98a8e206bb734f0",
        "container_image_name": "docker.io/prom/alertmanager:latest",
        "daemon_id": "ceph-001",
        "daemon_type": "alertmanager",
        "version": "0.20.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725856",
        "created": "2020-04-02T19:23:08.829543",
        "started": "2020-04-03T07:29:16.932838" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "c4b036202241",
        "container_image_id": "204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1",
        "container_image_name": "docker.io/ceph/ceph:v15",
        "daemon_id": "ceph-001",
        "daemon_type": "crash",
        "version": "15.2.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725903",
        "created": "2020-04-02T19:23:11.390694",
        "started": "2020-04-03T07:29:16.910897" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "5b7b94b48f31",
        "container_image_id": "87a51ecf0b1c9a7b187b21c1b071425dafea0d765a96d5bc371c791169b3d7f4",
        "container_image_name": "docker.io/ceph/ceph-grafana:latest",
        "daemon_id": "ceph-001",
        "daemon_type": "grafana",
        "version": "6.6.2",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725950",
        "created": "2020-04-02T19:23:52.025088",
        "started": "2020-04-03T07:29:16.847972" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "9ca007280456",
        "container_image_id": "204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1",
        "container_image_name": "docker.io/ceph/ceph:v15",
        "daemon_id": "ceph-001.gkjwqp",
        "daemon_type": "mgr",
        "version": "15.2.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725807",
        "created": "2020-04-02T19:22:18.648584",
        "started": "2020-04-03T07:29:16.856153" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "3d1ba9a2b697",
        "container_image_id": "204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1",
        "container_image_name": "docker.io/ceph/ceph:v15",
        "daemon_id": "ceph-001",
        "daemon_type": "mon",
        "version": "15.2.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725715",
        "created": "2020-04-02T19:22:13.863300",
        "started": "2020-04-03T07:29:17.206024" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "36d026c68ba1",
        "container_image_id": "e5a616e4b9cf68dfcad7782b78e118be4310022e874d52da85c55923fb615f87",
        "container_image_name": "docker.io/prom/node-exporter:latest",
        "daemon_id": "ceph-001",
        "daemon_type": "node-exporter",
        "version": "0.18.1",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.725996",
        "created": "2020-04-02T19:23:53.880197",
        "started": "2020-04-03T07:29:16.880044" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "faf76193cbfe",
        "container_image_id": "204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1",
        "container_image_name": "docker.io/ceph/ceph:v15",
        "daemon_id": "0",
        "daemon_type": "osd",
        "version": "15.2.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.726088",
        "created": "2020-04-02T20:35:02.991435",
        "started": "2020-04-03T07:29:19.373956" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "f82505bae0f1",
        "container_image_id": "204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1",
        "container_image_name": "docker.io/ceph/ceph:v15",
        "daemon_id": "1",
        "daemon_type": "osd",
        "version": "15.2.0",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.726134",
        "created": "2020-04-02T20:35:17.142272",
        "started": "2020-04-03T07:29:19.374002" 
    },
    {
        "hostname": "ceph-001",
        "container_id": "2708d84cd484",
        "container_image_id": "358a0d2395fe711bb8258e8fb4b2d7865c0a9a6463969bcd1452ee8869ea6653",
        "container_image_name": "docker.io/prom/prometheus:latest",
        "daemon_id": "ceph-001",
        "daemon_type": "prometheus",
        "version": "2.17.1",
        "status": 1,
        "status_desc": "running",
        "last_refresh": "2020-04-03T15:31:48.726042",
        "created": "2020-04-02T19:24:10.281163",
        "started": "2020-04-03T07:29:16.926292" 
    },
    {
        "hostname": "ceph-001",
        "daemon_id": "default-rgw-realm.eu-central-1.1.ceph-001.ytywjo",
        "daemon_type": "rgw",
        "status": 1,
        "status_desc": "starting" 
    }
]""")
)
def test_dd_octopus(dd_json):
    # https://tracker.ceph.com/issues/44934
    # Those are real user data from early octopus.
    # Please do not modify those JSON values.
    assert dd_json == DaemonDescription.from_json(dd_json).to_json()


@pytest.mark.parametrize("spec,dd,valid",
[
    # https://tracker.ceph.com/issues/44934
    (
        RGWSpec(
            rgw_realm="default-rgw-realm",
            rgw_zone="eu-central-1",
            subcluster='1',
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="default-rgw-realm.eu-central-1.1.ceph-001.ytywjo",
            hostname="ceph-001",
        ),
        True
    ),
    (
        # no subcluster
        RGWSpec(
            rgw_realm="default-rgw-realm",
            rgw_zone="eu-central-1",
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="default-rgw-realm.eu-central-1.ceph-001.ytywjo",
            hostname="ceph-001",
        ),
        True
    ),
    (
        # with tld
        RGWSpec(
            rgw_realm="default-rgw-realm",
            rgw_zone="eu-central-1",
            subcluster='1',
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="default-rgw-realm.eu-central-1.1.host.domain.tld.ytywjo",
            hostname="host.domain.tld",
        ),
        True
    ),
    (
        # explicit naming
        RGWSpec(
            rgw_realm="realm",
            rgw_zone="zone",
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="realm.zone.a",
            hostname="smithi028",
        ),
        True
    ),
    (
        # without host
        RGWSpec(
            service_type='rgw',
            rgw_realm="default-rgw-realm",
            rgw_zone="eu-central-1",
            subcluster='1',
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="default-rgw-realm.eu-central-1.1.hostname.ytywjo",
            hostname=None,
        ),
        False
    ),
    (
        # zone contains hostname
        # https://tracker.ceph.com/issues/45294
        RGWSpec(
            rgw_realm="default.rgw.realm",
            rgw_zone="ceph.001",
            subcluster='1',
        ),
        DaemonDescription(
            daemon_type='rgw',
            daemon_id="default.rgw.realm.ceph.001.1.ceph.001.ytywjo",
            hostname="ceph.001",
        ),
        True
    ),

    # https://tracker.ceph.com/issues/45293
    (
        ServiceSpec(
            service_type='mds',
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='mds',
            daemon_id="a.host1.abc123",
            hostname="host1",
        ),
        True
    ),
    (
        # '.' char in service_id
        ServiceSpec(
            service_type='mds',
            service_id="a.b.c",
        ),
        DaemonDescription(
            daemon_type='mds',
            daemon_id="a.b.c.host1.abc123",
            hostname="host1",
        ),
        True
    ),

    # https://tracker.ceph.com/issues/45617
    (
        # daemon_id does not contain hostname
        ServiceSpec(
            service_type='mds',
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='mds',
            daemon_id="a",
            hostname="host1",
        ),
        True
    ),
    (
        # daemon_id only contains hostname
        ServiceSpec(
            service_type='mds',
            service_id="host1",
        ),
        DaemonDescription(
            daemon_type='mds',
            daemon_id="host1",
            hostname="host1",
        ),
        True
    ),

    # https://tracker.ceph.com/issues/45399
    (
        # daemon_id only contains hostname
        ServiceSpec(
            service_type='mds',
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='mds',
            daemon_id="a.host1.abc123",
            hostname="host1.site",
        ),
        True
    ),
    (
        NFSServiceSpec(
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="a.host1",
            hostname="host1.site",
        ),
        True
    ),

    # https://tracker.ceph.com/issues/45293
    (
        NFSServiceSpec(
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="a.host1",
            hostname="host1",
        ),
        True
    ),
    (
        # service_id contains a '.' char
        NFSServiceSpec(
            service_id="a.b.c",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="a.b.c.host1",
            hostname="host1",
        ),
        True
    ),
    (
        # trailing chars after hostname
        NFSServiceSpec(
            service_id="a.b.c",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="a.b.c.host1.abc123",
            hostname="host1",
        ),
        True
    ),
    (
        # chars after hostname without '.'
        NFSServiceSpec(
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="a.host1abc123",
            hostname="host1",
        ),
        False
    ),
    (
        # chars before hostname without '.'
        NFSServiceSpec(
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='nfs',
            daemon_id="ahost1.abc123",
            hostname="host1",
        ),
        False
    ),

    # https://tracker.ceph.com/issues/45293
    (
        IscsiServiceSpec(
            service_type='iscsi',
            service_id="a",
        ),
        DaemonDescription(
            daemon_type='iscsi',
            daemon_id="a.host1.abc123",
            hostname="host1",
        ),
        True
    ),
    (
        # '.' char in service_id
        IscsiServiceSpec(
            service_type='iscsi',
            service_id="a.b.c",
        ),
        DaemonDescription(
            daemon_type='iscsi',
            daemon_id="a.b.c.host1.abc123",
            hostname="host1",
        ),
        True
    ),
])
def test_daemon_description_service_name(spec: ServiceSpec,
                                         dd: DaemonDescription,
                                         valid: bool):
    if valid:
        assert spec.service_name() == dd.service_name()
    else:
        with pytest.raises(OrchestratorError):
            dd.service_name()

