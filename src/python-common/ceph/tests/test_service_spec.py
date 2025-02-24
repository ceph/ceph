# flake8: noqa
import json
import re

import yaml

import pytest

from ceph.deployment.service_spec import (
    AlertManagerSpec,
    ArgumentSpec,
    CustomContainerSpec,
    GrafanaSpec,
    HostPlacementSpec,
    IscsiServiceSpec,
    NFSServiceSpec,
    PlacementSpec,
    PrometheusSpec,
    RGWSpec,
    ServiceSpec,
)
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.hostspec import SpecValidationError


@pytest.mark.parametrize("test_input,expected, require_network",
                         [("myhost", ('myhost', '', ''), False),
                          ("myhost=sname", ('myhost', '', 'sname'), False),
                          ("myhost:10.1.1.10", ('myhost', '10.1.1.10', ''), True),
                          ("myhost:10.1.1.10=sname", ('myhost', '10.1.1.10', 'sname'), True),
                          ("myhost:10.1.1.0/32", ('myhost', '10.1.1.0/32', ''), True),
                          ("myhost:10.1.1.0/32=sname", ('myhost', '10.1.1.0/32', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789]", ('myhost', '[v1:10.1.1.10:6789]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789]=sname", ('myhost', '[v1:10.1.1.10:6789]', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]=sname", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', 'sname'), True),
                          ])
def test_parse_host_placement_specs(test_input, expected, require_network):
    ret = HostPlacementSpec.parse(test_input, require_network=require_network)
    assert ret == expected
    assert str(ret) == test_input

    ps = PlacementSpec.from_string(test_input)
    assert ps.pretty_str() == test_input
    assert ps == PlacementSpec.from_string(ps.pretty_str())

    # Testing the old verbose way of generating json. Don't remove:
    assert ret == HostPlacementSpec.from_json({
            'hostname': ret.hostname,
            'network': ret.network,
            'name': ret.name
        })

    assert ret == HostPlacementSpec.from_json(ret.to_json())


@pytest.mark.parametrize(
    "spec, raise_exception, msg",
    [
        (GrafanaSpec(protocol=''), True, '^Invalid protocol'),
        (GrafanaSpec(protocol='invalid'), True, '^Invalid protocol'),
        (GrafanaSpec(protocol='-http'), True, '^Invalid protocol'),
        (GrafanaSpec(protocol='-https'), True, '^Invalid protocol'),
        (GrafanaSpec(protocol='http'), False, ''),
        (GrafanaSpec(protocol='https'), False, ''),
        (GrafanaSpec(anonymous_access=False), True, '^Either initial'),  # we require inital_admin_password if anonymous_access is False
        (GrafanaSpec(anonymous_access=False, initial_admin_password='test'), False, ''),
    ])
def test_apply_grafana(spec: GrafanaSpec, raise_exception: bool, msg: str):
    if  raise_exception:
        with pytest.raises(SpecValidationError, match=msg):
            spec.validate()
    else:
        spec.validate()

@pytest.mark.parametrize(
    "spec, raise_exception, msg",
    [
        # Valid retention_time values (valid units: 'y', 'w', 'd', 'h', 'm', 's')
        (PrometheusSpec(retention_time='1y'), False, ''),
        (PrometheusSpec(retention_time=' 10w '), False, ''),
        (PrometheusSpec(retention_time=' 1348d'), False, ''),
        (PrometheusSpec(retention_time='2000h '), False, ''),
        (PrometheusSpec(retention_time='173847m'), False, ''),
        (PrometheusSpec(retention_time='200s'), False, ''),
        (PrometheusSpec(retention_time='  '), False, ''),  # default value will be used
        # Invalid retention_time values
        (PrometheusSpec(retention_time='100k'), True, '^Invalid retention time'),     # invalid unit
        (PrometheusSpec(retention_time='10'), True, '^Invalid retention time'),       # no unit
        (PrometheusSpec(retention_time='100.00y'), True, '^Invalid retention time'),  # invalid value and valid unit
        (PrometheusSpec(retention_time='100.00k'), True, '^Invalid retention time'),  # invalid value and invalid unit
        (PrometheusSpec(retention_time='---'), True, '^Invalid retention time'),      # invalid value

        # Valid retention_size values (valid units: 'B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB')
        (PrometheusSpec(retention_size='123456789B'), False, ''),
        (PrometheusSpec(retention_size=' 200KB'), False, ''),
        (PrometheusSpec(retention_size='99999MB '), False, ''),
        (PrometheusSpec(retention_size=' 10GB '), False, ''),
        (PrometheusSpec(retention_size='100TB'), False, ''),
        (PrometheusSpec(retention_size='500PB'), False, ''),
        (PrometheusSpec(retention_size='200EB'), False, ''),
        (PrometheusSpec(retention_size='  '), False, ''),  # default value will be used

        # Invalid retention_size values
        (PrometheusSpec(retention_size='100b'), True, '^Invalid retention size'),      # invalid unit (case sensitive)
        (PrometheusSpec(retention_size='333kb'), True, '^Invalid retention size'),     # invalid unit (case sensitive)
        (PrometheusSpec(retention_size='2000'), True, '^Invalid retention size'),      # no unit
        (PrometheusSpec(retention_size='200.00PB'), True, '^Invalid retention size'),  # invalid value and valid unit
        (PrometheusSpec(retention_size='400.B'), True, '^Invalid retention size'),     # invalid value and invalid unit
        (PrometheusSpec(retention_size='10.000s'), True, '^Invalid retention size'),   # invalid value and invalid unit
        (PrometheusSpec(retention_size='...'), True, '^Invalid retention size'),       # invalid value

        # valid retention_size and valid retention_time
        (PrometheusSpec(retention_time='1y', retention_size='100GB'), False, ''),
        # invalid retention_time and valid retention_size
        (PrometheusSpec(retention_time='1j', retention_size='100GB'), True, '^Invalid retention time'),
        # valid retention_time and invalid retention_size
        (PrometheusSpec(retention_time='1y', retention_size='100gb'), True, '^Invalid retention size'),
        # valid retention_time and invalid retention_size
        (PrometheusSpec(retention_time='1y', retention_size='100gb'), True, '^Invalid retention size'),
        # invalid retention_time and invalid retention_size
        (PrometheusSpec(retention_time='1i', retention_size='100gb'), True, '^Invalid retention time'),
    ])
def test_apply_prometheus(spec: PrometheusSpec, raise_exception: bool, msg: str):
    if raise_exception:
        with pytest.raises(SpecValidationError, match=msg):
                spec.validate()
    else:
        spec.validate()

@pytest.mark.parametrize(
    "test_input,expected",
    [
        ('', "PlacementSpec()"),
        ("count:2", "PlacementSpec(count=2)"),
        ("3", "PlacementSpec(count=3)"),
        ("host1 host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1;host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1,host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1 host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1=a host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name='a'), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1:1.2.3.4=a host2:1.2.3.5=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='1.2.3.4', name='a'), HostPlacementSpec(hostname='host2', network='1.2.3.5', name='b')])"),
        ("myhost:[v1:10.1.1.10:6789]", "PlacementSpec(hosts=[HostPlacementSpec(hostname='myhost', network='[v1:10.1.1.10:6789]', name='')])"),
        ('2 host1 host2', "PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ('label:foo', "PlacementSpec(label='foo')"),
        ('3 label:foo', "PlacementSpec(count=3, label='foo')"),
        ('*', "PlacementSpec(host_pattern=HostPattern(pattern='*', pattern_type=PatternType.fnmatch))"),
        ('3 data[1-3]', "PlacementSpec(count=3, host_pattern=HostPattern(pattern='data[1-3]', pattern_type=PatternType.fnmatch))"),
        ('3 data?', "PlacementSpec(count=3, host_pattern=HostPattern(pattern='data?', pattern_type=PatternType.fnmatch))"),
        ('3 data*', "PlacementSpec(count=3, host_pattern=HostPattern(pattern='data*', pattern_type=PatternType.fnmatch))"),
        ("count-per-host:4 label:foo", "PlacementSpec(count_per_host=4, label='foo')"),
        ('regex:Foo[0-9]|Bar[0-9]', "PlacementSpec(host_pattern=HostPattern(pattern='Foo[0-9]|Bar[0-9]', pattern_type=PatternType.regex))"),
        ('3 regex:Foo[0-9]|Bar[0-9]', "PlacementSpec(count=3, host_pattern=HostPattern(pattern='Foo[0-9]|Bar[0-9]', pattern_type=PatternType.regex))"),
    ])
def test_parse_placement_specs(test_input, expected):
    ret = PlacementSpec.from_string(test_input)
    assert str(ret) == expected
    assert PlacementSpec.from_string(ret.pretty_str()) == ret, f'"{ret.pretty_str()}" != "{test_input}"'

@pytest.mark.parametrize(
    "test_input",
    [
        ("host=a host*"),
        ("host=a label:wrong"),
        ("host? host*"),
        ("host? regex:host*"),
        ("regex:host? host*"),
        ("regex:host? regex:host*"),
        ('host=a count-per-host:0'),
        ('host=a count-per-host:-10'),
        ('count:2 count-per-host:1'),
        ('host1=a host2=b count-per-host:2'),
        ('host1:10/8 count-per-host:2'),
        ('count-per-host:2'),
    ]
)
def test_parse_placement_specs_raises(test_input):
    with pytest.raises(SpecValidationError):
        PlacementSpec.from_string(test_input)

@pytest.mark.parametrize("test_input",
                         # wrong subnet
                         [("myhost:1.1.1.1/24"),
                          # wrong ip format
                          ("myhost:1"),
                          ])
def test_parse_host_placement_specs_raises_wrong_format(test_input):
    with pytest.raises(ValueError):
        HostPlacementSpec.parse(test_input)


@pytest.mark.parametrize(
    "p,hosts,size",
    [
        (
            PlacementSpec(count=3),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            3
        ),
        (
            PlacementSpec(host_pattern='*'),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            5
        ),
        (
            PlacementSpec(count_per_host=2, host_pattern='*'),
            ['host1', 'host2', 'host3', 'host4', 'host5'],
            10
        ),
        (
            PlacementSpec(host_pattern='foo*'),
            ['foo1', 'foo2', 'bar1', 'bar2'],
            2
        ),
        (
            PlacementSpec(count_per_host=2, host_pattern='foo*'),
            ['foo1', 'foo2', 'bar1', 'bar2'],
            4
        ),
    ])
def test_placement_target_size(p, hosts, size):
    assert p.get_target_count(
        [HostPlacementSpec(n, '', '') for n in hosts]
    ) == size


def _get_dict_spec(s_type, s_id):
    dict_spec = {
        "service_id": s_id,
        "service_type": s_type,
        "placement":
            dict(hosts=["host1:1.1.1.1"])
    }
    if s_type == 'nfs':
        pass
    elif s_type == 'iscsi':
        dict_spec['pool'] = 'pool'
        dict_spec['api_user'] = 'api_user'
        dict_spec['api_password'] = 'api_password'
    elif s_type == 'osd':
        dict_spec['spec'] = {
            'data_devices': {
                'all': True
            }
        }
    elif s_type == 'rgw':
        dict_spec['rgw_realm'] = 'realm'
        dict_spec['rgw_zone'] = 'zone'

    return dict_spec


@pytest.mark.parametrize(
    "s_type,o_spec,s_id",
    [
        ("mgr", ServiceSpec, 'test'),
        ("mon", ServiceSpec, 'test'),
        ("mds", ServiceSpec, 'test'),
        ("rgw", RGWSpec, 'realm.zone'),
        ("nfs", NFSServiceSpec, 'test'),
        ("iscsi", IscsiServiceSpec, 'test'),
        ("osd", DriveGroupSpec, 'test'),
    ])
def test_servicespec_map_test(s_type, o_spec, s_id):
    spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
    assert isinstance(spec, o_spec)
    assert isinstance(spec.placement, PlacementSpec)
    assert isinstance(spec.placement.hosts[0], HostPlacementSpec)
    assert spec.placement.hosts[0].hostname == 'host1'
    assert spec.placement.hosts[0].network == '1.1.1.1'
    assert spec.placement.hosts[0].name == ''
    assert spec.validate() is None
    ServiceSpec.from_json(spec.to_json())


@pytest.mark.parametrize(
    "realm, zone, frontend_type, raise_exception, msg",
    [
        ('realm', 'zone1', 'beast', False, ''),
        ('realm', 'zone2', 'civetweb', False, ''),
        ('realm', None, 'beast', True, 'Cannot add RGW: Realm specified but no zone specified'),
        (None, 'zone1', 'beast', True, 'Cannot add RGW: Zone specified but no realm specified'),
        ('realm', 'zone', 'invalid-beast', True, '^Invalid rgw_frontend_type value'),
        ('realm', 'zone', 'invalid-civetweb', True, '^Invalid rgw_frontend_type value'),
    ])
def test_rgw_servicespec_parse(realm, zone, frontend_type, raise_exception, msg):
    spec = RGWSpec(service_id='foo',
                   rgw_realm=realm,
                   rgw_zone=zone,
                   rgw_frontend_type=frontend_type)
    if raise_exception:
        with pytest.raises(SpecValidationError, match=msg):
            spec.validate()
    else:
        spec.validate()

def test_osd_unmanaged():
    osd_spec = {"placement": {"host_pattern": "*"},
                "service_id": "all-available-devices",
                "service_name": "osd.all-available-devices",
                "service_type": "osd",
                "spec": {"data_devices": {"all": True}, "filter_logic": "AND", "objectstore": "bluestore"},
                "unmanaged": True}

    dg_spec = ServiceSpec.from_json(osd_spec)
    assert dg_spec.unmanaged == True


@pytest.mark.parametrize("y",
"""service_type: crash
service_name: crash
placement:
  host_pattern: '*'
---
service_type: crash
service_name: crash
placement:
  host_pattern: '*'
unmanaged: true
---
service_type: crash
service_name: crash
placement:
  host_pattern:
    pattern: Foo[0-9]|Bar[0-9]
    pattern_type: regex
---
service_type: rgw
service_id: default-rgw-realm.eu-central-1.1
service_name: rgw.default-rgw-realm.eu-central-1.1
placement:
  hosts:
  - ceph-001
networks:
- 10.0.0.0/8
- 192.168.0.0/16
spec:
  rgw_frontend_type: civetweb
  rgw_realm: default-rgw-realm
  rgw_zone: eu-central-1
---
service_type: osd
service_id: osd_spec_default
service_name: osd.osd_spec_default
placement:
  host_pattern: '*'
spec:
  data_devices:
    model: MC-55-44-XZ
  db_devices:
    model: SSD-123-foo
  filter_logic: AND
  objectstore: bluestore
  wal_devices:
    model: NVME-QQQQ-987
---
service_type: alertmanager
service_name: alertmanager
spec:
  port: 1234
  user_data:
    default_webhook_urls:
    - foo
---
service_type: grafana
service_name: grafana
spec:
  anonymous_access: true
  port: 1234
  protocol: https
---
service_type: grafana
service_name: grafana
spec:
  anonymous_access: true
  initial_admin_password: secure
  port: 1234
  protocol: https
---
service_type: ingress
service_id: rgw.foo
service_name: ingress.rgw.foo
placement:
  hosts:
  - host1
  - host2
  - host3
spec:
  backend_service: rgw.foo
  first_virtual_router_id: 50
  frontend_port: 8080
  monitor_port: 8081
  virtual_ip: 192.168.20.1/24
---
service_type: nfs
service_id: mynfs
service_name: nfs.mynfs
spec:
  idmap_conf:
    general:
      local-realms: domain.org
    mapping:
      nobody-group: nfsnobody
      nobody-user: nfsnobody
  port: 1234
---
service_type: iscsi
service_id: iscsi
service_name: iscsi.iscsi
networks:
- ::0/8
spec:
  api_password: admin
  api_port: 5000
  api_user: admin
  pool: pool
  trusted_ip_list:
  - ::1
  - ::2
---
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_community: public
  snmp_destination: 192.168.1.42:162
  snmp_version: V2c
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  auth_protocol: MD5
  credentials:
    snmp_v3_auth_password: mypassword
    snmp_v3_auth_username: myuser
  engine_id: 8000C53F00000000
  port: 9464
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_password: mypassword
    snmp_v3_auth_username: myuser
    snmp_v3_priv_password: mysecret
  engine_id: 8000C53F00000000
  privacy_protocol: AES
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
---
service_type: grafana
service_name: grafana
placement:
  count: 1
spec:
  anonymous_access: false
  initial_admin_password: password
  protocol: https
""".split('---\n'))
def test_yaml(y):
    data = yaml.safe_load(y)
    object = ServiceSpec.from_json(data)

    assert yaml.dump(object) == y
    assert yaml.dump(ServiceSpec.from_json(object.to_json())) == y


def test_alertmanager_spec_1():
    spec = AlertManagerSpec()
    assert spec.service_type == 'alertmanager'
    assert isinstance(spec.user_data, dict)
    assert len(spec.user_data.keys()) == 0
    assert spec.get_port_start() == [9093, 9094]


def test_alertmanager_spec_2():
    spec = AlertManagerSpec(user_data={'default_webhook_urls': ['foo']})
    assert isinstance(spec.user_data, dict)
    assert 'default_webhook_urls' in spec.user_data.keys()



def test_repr():
    val = """ServiceSpec.from_json(yaml.safe_load('''service_type: crash
service_name: crash
placement:
  count: 42
'''))"""
    obj = eval(val)
    assert obj.service_type == 'crash'
    assert val == repr(obj)

@pytest.mark.parametrize("spec1, spec2, eq",
                         [
                             (
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='mon'
                                     ),
                                     ServiceSpec(
                                         service_type='mon',
                                         service_id='foo'
                                     ),
                                     True
                             ),
                             # Add service_type='mgr'
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     ServiceSpec(
                                         service_type='osd',
                                     ),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     DriveGroupSpec(),
                                     True
                             ),
                             (
                                     ServiceSpec(
                                         service_type='osd'
                                     ),
                                     ServiceSpec(
                                         service_type='osd',
                                         service_id='foo',
                                     ),
                                     False
                             ),
                             (
                                     ServiceSpec(
                                         service_type='rgw',
                                         service_id='foo',
                                     ),
                                     RGWSpec(service_id='foo'),
                                     True
                             ),
                         ])
def test_spec_hash_eq(spec1: ServiceSpec,
                      spec2: ServiceSpec,
                      eq: bool):

    assert (spec1 == spec2) is eq

@pytest.mark.parametrize(
    "s_type,s_id,s_name",
    [
        ('mgr', 's_id', 'mgr'),
        ('mon', 's_id', 'mon'),
        ('mds', 's_id', 'mds.s_id'),
        ('rgw', 's_id', 'rgw.s_id'),
        ('nfs', 's_id', 'nfs.s_id'),
        ('iscsi', 's_id', 'iscsi.s_id'),
        ('osd', 's_id', 'osd.s_id'),
    ])
def test_service_name(s_type, s_id, s_name):
    spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
    spec.validate()
    assert spec.service_name() == s_name

@pytest.mark.parametrize(
    's_type,s_id',
    [
        ('mds', 's:id'), # MDS service_id cannot contain an invalid char ':'
        ('mds', '1abc'), # MDS service_id cannot start with a numeric digit
        ('mds', ''),     # MDS service_id cannot be empty
        ('rgw', '*s_id'),
        ('nfs', 's/id'),
        ('iscsi', 's@id'),
        ('osd', 's;id'),
    ])

def test_service_id_raises_invalid_char(s_type, s_id):
    with pytest.raises(SpecValidationError):
        spec = ServiceSpec.from_json(_get_dict_spec(s_type, s_id))
        spec.validate()

def test_custom_container_spec():
    spec = CustomContainerSpec(service_id='hello-world',
                               image='docker.io/library/hello-world:latest',
                               entrypoint='/usr/bin/bash',
                               uid=1000,
                               gid=2000,
                               volume_mounts={'foo': '/foo'},
                               args=['--foo'],
                               envs=['FOO=0815'],
                               bind_mounts=[
                                   [
                                       'type=bind',
                                       'source=lib/modules',
                                       'destination=/lib/modules',
                                       'ro=true'
                                   ]
                               ],
                               ports=[8080, 8443],
                               dirs=['foo', 'bar'],
                               files={
                                   'foo.conf': 'foo\nbar',
                                   'bar.conf': ['foo', 'bar']
                               })
    assert spec.service_type == 'container'
    assert spec.entrypoint == '/usr/bin/bash'
    assert spec.uid == 1000
    assert spec.gid == 2000
    assert spec.volume_mounts == {'foo': '/foo'}
    assert spec.args == ['--foo']
    assert spec.envs == ['FOO=0815']
    assert spec.bind_mounts == [
        [
            'type=bind',
            'source=lib/modules',
            'destination=/lib/modules',
            'ro=true'
        ]
    ]
    assert spec.ports == [8080, 8443]
    assert spec.dirs == ['foo', 'bar']
    assert spec.files == {
        'foo.conf': 'foo\nbar',
        'bar.conf': ['foo', 'bar']
    }


def test_custom_container_spec_config_json():
    spec = CustomContainerSpec(service_id='foo', image='foo', dirs=None)
    config_json = spec.config_json()
    for key in ['entrypoint', 'uid', 'gid', 'bind_mounts', 'dirs']:
        assert key not in config_json


def test_ingress_spec():
    yaml_str = """service_type: ingress
service_id: rgw.foo
placement:
  hosts:
    - host1
    - host2
    - host3
spec:
  virtual_ip: 192.168.20.1/24
  backend_service: rgw.foo
  frontend_port: 8080
  monitor_port: 8081
"""
    yaml_file = yaml.safe_load(yaml_str)
    spec = ServiceSpec.from_json(yaml_file)
    assert spec.service_type == "ingress"
    assert spec.service_id == "rgw.foo"
    assert spec.virtual_ip == "192.168.20.1/24"
    assert spec.frontend_port == 8080
    assert spec.monitor_port == 8081


@pytest.mark.parametrize("y, error_match", [
    ("""
service_type: rgw
service_id: foo
placement:
  count_per_host: "twelve"
""", "count-per-host must be a numeric value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count_per_host: "2"
""", "count-per-host must be an integer value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count_per_host: 7.36
""", "count-per-host must be an integer value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count: "fifteen"
""", "num/count must be a numeric value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count: "4"
""", "num/count must be an integer value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count: 7.36
""", "num/count must be an integer value",),
    ("""
service_type: rgw
service_id: foo
placement:
  count: 0
""", "num/count must be >= 1",),
    ("""
service_type: rgw
service_id: foo
placement:
  count_per_host: 0
""", "count-per-host must be >= 1",),
    ("""
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_password: mypassword
    snmp_v3_auth_username: myuser
    snmp_v3_priv_password: mysecret
  port: 9464
  engine_id: 8000c53f0000000000
  privacy_protocol: WEIRD
  snmp_destination: 192.168.122.1:162
  auth_protocol: BIZARRE
  snmp_version: V3
""", "auth_protocol unsupported. Must be one of MD5, SHA"),
    ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_community: public
  snmp_destination: 192.168.1.42:162
  snmp_version: V4
""", 'snmp_version unsupported. Must be one of V2c, V3'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_community: public
  port: 9464
  snmp_destination: 192.168.1.42:162
""", re.escape('Missing SNMP version (snmp_version)')),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
  port: 9464
  auth_protocol: wah
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
""", 'auth_protocol unsupported. Must be one of MD5, SHA'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: weewah
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
""", 'privacy_protocol unsupported. Must be one of DES, AES'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
""", 'Must provide an engine_id for SNMP V3 notifications'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_community: public
  port: 9464
  snmp_destination: 192.168.1.42
  snmp_version: V2c
""", re.escape('SNMP destination (snmp_destination) type (IPv4) is invalid. Must be either: IPv4:Port, Name:Port')),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: bogus
  snmp_destination: 192.168.1.42:162
  snmp_version: V3
""", 'engine_id must be a string containing 10-64 hex characters. Its length must be divisible by 2'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
  port: 9464
  auth_protocol: SHA
  engine_id: 8000C53F0000000000
  snmp_version: V3
""", re.escape('SNMP destination (snmp_destination) must be provided')),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53F0000000000
  snmp_destination: my.imaginary.snmp-host
  snmp_version: V3
""", re.escape('SNMP destination (snmp_destination) is invalid: DNS lookup failed')),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53F0000000000
  snmp_destination: 10.79.32.10:fred
  snmp_version: V3
""", re.escape('SNMP destination (snmp_destination) is invalid: Port must be numeric')),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53
  snmp_destination: 10.79.32.10:162
  snmp_version: V3
""", 'engine_id must be a string containing 10-64 hex characters. Its length must be divisible by 2'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53DOH!
  snmp_destination: 10.79.32.10:162
  snmp_version: V3
""", 'engine_id must be a string containing 10-64 hex characters. Its length must be divisible by 2'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53FCA7344403DC611EC9B985254002537A6C53FCA7344403DC6112537A60
  snmp_destination: 10.79.32.10:162
  snmp_version: V3
""", 'engine_id must be a string containing 10-64 hex characters. Its length must be divisible by 2'),
        ("""
---
service_type: snmp-gateway
service_name: snmp-gateway
placement:
  count: 1
spec:
  credentials:
    snmp_v3_auth_username: myuser
    snmp_v3_auth_password: mypassword
    snmp_v3_priv_password: mysecret
  port: 9464
  auth_protocol: SHA
  privacy_protocol: AES
  engine_id: 8000C53F00000
  snmp_destination: 10.79.32.10:162
  snmp_version: V3
""", 'engine_id must be a string containing 10-64 hex characters. Its length must be divisible by 2'),
    ])
def test_service_spec_validation_error(y, error_match):
    data = yaml.safe_load(y)
    with pytest.raises(SpecValidationError) as err:
        specObj = ServiceSpec.from_json(data)
    assert err.match(error_match)


@pytest.mark.parametrize("y, ec_args, ee_args, ec_final_args, ee_final_args", [
    pytest.param("""
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
""",
    None,
    None,
    None,
    None,
    id="no_extra_args"),
    pytest.param("""
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  extra_entrypoint_args:
  - "--lasers=blue"
  - "--enable-confetti"
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
""",
    None,
    ["--lasers=blue", "--enable-confetti"],
    None,
    ["--lasers=blue", "--enable-confetti"],
    id="only_extra_entrypoint_args_spec"),
    pytest.param("""
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
extra_entrypoint_args:
- "--lasers blue"
- "--enable-confetti"
""",
    None,
    ["--lasers blue", "--enable-confetti"],
    None,
    ["--lasers", "blue", "--enable-confetti"],
    id="only_extra_entrypoint_args_toplevel"),
    pytest.param("""
service_type: nfs
service_id: mynfs
service_name: nfs.mynfs
spec:
  port: 1234
  extra_entrypoint_args:
  - "--lasers=blue"
  - "--title=Custom NFS Options"
  extra_container_args:
  - "--cap-add=CAP_NET_BIND_SERVICE"
  - "--oom-score-adj=12"
""",
    ["--cap-add=CAP_NET_BIND_SERVICE", "--oom-score-adj=12"],
    ["--lasers=blue", "--title=Custom NFS Options"],
    ["--cap-add=CAP_NET_BIND_SERVICE", "--oom-score-adj=12"],
    ["--lasers=blue", "--title=Custom", "NFS", "Options"],
    id="both_kinds_nfs"),
    pytest.param("""
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
extra_entrypoint_args:
- argument: "--lasers=blue"
  split: true
- argument: "--enable-confetti"
""",
    None,
    [
        {"argument": "--lasers=blue", "split": True},
        {"argument": "--enable-confetti", "split": False},
    ],
    None,
    [
        "--lasers=blue",
        "--enable-confetti",
    ],
    id="only_extra_entrypoint_args_obj_toplevel"),
    pytest.param("""
service_type: container
service_id: hello-world
service_name: container.hello-world
spec:
  args:
  - --foo
  bind_mounts:
  - - type=bind
    - source=lib/modules
    - destination=/lib/modules
    - ro=true
  dirs:
  - foo
  - bar
  entrypoint: /usr/bin/bash
  envs:
  - FOO=0815
  files:
    bar.conf:
    - foo
    - bar
    foo.conf: 'foo

      bar'
  gid: 2000
  image: docker.io/library/hello-world:latest
  ports:
  - 8080
  - 8443
  uid: 1000
  volume_mounts:
    foo: /foo
  extra_entrypoint_args:
  - argument: "--lasers=blue"
    split: true
  - argument: "--enable-confetti"
""",
    None,
    [
        {"argument": "--lasers=blue", "split": True},
        {"argument": "--enable-confetti", "split": False},
    ],
    None,
    [
        "--lasers=blue",
        "--enable-confetti",
    ],
    id="only_extra_entrypoint_args_obj_indented"),
    pytest.param("""
service_type: nfs
service_id: mynfs
service_name: nfs.mynfs
spec:
  port: 1234
extra_entrypoint_args:
- argument: "--lasers=blue"
- argument: "--title=Custom NFS Options"
extra_container_args:
- argument: "--cap-add=CAP_NET_BIND_SERVICE"
- argument: "--oom-score-adj=12"
""",
    [
        {"argument": "--cap-add=CAP_NET_BIND_SERVICE", "split": False},
        {"argument": "--oom-score-adj=12", "split": False},
    ],
    [
        {"argument": "--lasers=blue", "split": False},
        {"argument": "--title=Custom NFS Options", "split": False},
    ],
    [
        "--cap-add=CAP_NET_BIND_SERVICE",
        "--oom-score-adj=12",
    ],
    [
        "--lasers=blue",
        "--title=Custom NFS Options",
    ],
    id="both_kinds_obj_nfs"),
])
def test_extra_args_handling(y, ec_args, ee_args, ec_final_args, ee_final_args):
    data = yaml.safe_load(y)
    spec_obj = ServiceSpec.from_json(data)

    assert ArgumentSpec.map_json(spec_obj.extra_container_args) == ec_args
    assert ArgumentSpec.map_json(spec_obj.extra_entrypoint_args) == ee_args
    if ec_final_args is None:
        assert spec_obj.extra_container_args is None
    else:
        ec_res = []
        for args in spec_obj.extra_container_args:
            ec_res.extend(args.to_args())
        assert ec_res == ec_final_args
    if ee_final_args is None:
        assert spec_obj.extra_entrypoint_args is None
    else:
        ee_res = []
        for args in spec_obj.extra_entrypoint_args:
            ee_res.extend(args.to_args())
        assert ee_res == ee_final_args
