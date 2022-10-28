from unittest import mock
import copy, json, os, threading

import pytest

from tests.fixtures import with_cephadm_ctx, cephadm_fs, import_cephadm

_cephadm = import_cephadm()


FSID = "beefbeef-beef-beef-1234-beefbeefbeef"
AGENT_ID = 'host1'
AGENT_DIR = f'/var/lib/ceph/{FSID}/agent.{AGENT_ID}'


def test_agent_validate():
    required_files = _cephadm.CephadmAgent.required_files
    with with_cephadm_ctx([]) as ctx:
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        for i in range(len(required_files)):
            incomplete_files = {s: 'text' for s in [f for j, f in enumerate(required_files) if j != i]}
            with pytest.raises(_cephadm.Error, match=f'required file missing from config: {required_files[i]}'):
                agent.validate(incomplete_files)
        all_files = {s: 'text' for s in required_files}
        agent.validate(all_files)


def _check_file(path, content):
    assert os.path.exists(path)
    with open(path) as f:
        fcontent = f.read()
        assert fcontent == content


@mock.patch('cephadm.call_throws')
def test_agent_deploy_daemon_unit(_call_throws, cephadm_fs):
    _call_throws.return_value = ('', '', 0)
    agent_id = AGENT_ID

    with with_cephadm_ctx([]) as ctx:
        ctx.meta_json = json.dumps({'meta': 'data'})
        agent = _cephadm.CephadmAgent(ctx, FSID, agent_id)
        cephadm_fs.create_dir(AGENT_DIR)

        with pytest.raises(_cephadm.Error, match='Agent needs a config'):
            agent.deploy_daemon_unit()

        config = {s: f'text for {s}' for s in _cephadm.CephadmAgent.required_files}
        config['not-required-file.txt'] = 'don\'t write me'

        agent.deploy_daemon_unit(config)

        # check required config file were all created
        for fname in _cephadm.CephadmAgent.required_files:
            _check_file(f'{AGENT_DIR}/{fname}', f'text for {fname}')

        # assert non-required file was not written
        assert not os.path.exists(f'{AGENT_DIR}/not-required-file.txt')

        # check unit.run file was created correctly
        _check_file(f'{AGENT_DIR}/unit.run', agent.unit_run())

        # check unit.meta file created correctly
        _check_file(f'{AGENT_DIR}/unit.meta', json.dumps({'meta': 'data'}, indent=4) + '\n')

        # check unit file was created correctly
        _check_file(f'{ctx.unit_dir}/{agent.unit_name()}', agent.unit_file())

        expected_call_throws_calls = [
            mock.call(ctx, ['systemctl', 'daemon-reload']),
            mock.call(ctx, ['systemctl', 'enable', '--now', agent.unit_name()]),
        ]
        _call_throws.assert_has_calls(expected_call_throws_calls)

        expected_call_calls = [
            mock.call(ctx, ['systemctl', 'stop', agent.unit_name()], verbosity=_cephadm.CallVerbosity.DEBUG),
            mock.call(ctx, ['systemctl', 'reset-failed', agent.unit_name()], verbosity=_cephadm.CallVerbosity.DEBUG),
        ]
        _cephadm.call.assert_has_calls(expected_call_calls)


@mock.patch('threading.Thread.is_alive')
def test_agent_shutdown(_is_alive):
    with with_cephadm_ctx([]) as ctx:
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        _is_alive.return_value = True
        assert agent.stop == False
        assert agent.mgr_listener.stop == False
        agent.shutdown()
        assert agent.stop == True
        assert agent.mgr_listener.stop == True


def test_agent_wakeup():
    with with_cephadm_ctx([]) as ctx:
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        assert agent.event.is_set() == False
        agent.wakeup()
        assert agent.event.is_set() == True


@mock.patch("cephadm.CephadmAgent.shutdown")
@mock.patch("cephadm.AgentGatherer.update_func")
def test_pull_conf_settings(_update_func, _shutdown, cephadm_fs):
    target_ip = '192.168.0.0'
    target_port = 9876
    refresh_period = 20
    listener_port = 5678
    host = AGENT_ID
    device_enhanced_scan = 'True'
    with with_cephadm_ctx([]) as ctx:
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        full_config = {
            'target_ip': target_ip,
            'target_port': target_port,
            'refresh_period': refresh_period,
            'listener_port': listener_port,
            'host': host,
            'device_enhanced_scan': device_enhanced_scan
        }
        cephadm_fs.create_dir(AGENT_DIR)
        with open(agent.config_path, 'w') as f:
            f.write(json.dumps(full_config))

        with pytest.raises(_cephadm.Error, match="Failed to get agent keyring:"):
            agent.pull_conf_settings()
        _shutdown.assert_called()
        with open(agent.keyring_path, 'w') as f:
            f.write('keyring')

        assert agent.device_enhanced_scan == False
        agent.pull_conf_settings()
        assert agent.host == host
        assert agent.target_ip == target_ip
        assert agent.target_port == target_port
        assert agent.loop_interval == refresh_period
        assert agent.starting_port == listener_port
        assert agent.device_enhanced_scan == True
        assert agent.keyring == 'keyring'
        _update_func.assert_called()

        full_config.pop('target_ip')
        with open(agent.config_path, 'w') as f:
            f.write(json.dumps(full_config))
        with pytest.raises(_cephadm.Error, match="Failed to get agent target ip and port from config:"):
            agent.pull_conf_settings()


@mock.patch("cephadm.command_ceph_volume")
def test_agent_ceph_volume(_ceph_volume):

    def _ceph_volume_outputter(_):
        print("ceph-volume output")

    def _ceph_volume_empty(_):
        pass

    with with_cephadm_ctx([]) as ctx:
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)

        _ceph_volume.side_effect = _ceph_volume_outputter
        out, _ = agent._ceph_volume(False)
        assert ctx.command == ['inventory', '--format=json']
        assert out == "ceph-volume output\n"

        out, _ = agent._ceph_volume(True)
        assert ctx.command == ['inventory', '--format=json', '--with-lsm']
        assert out == "ceph-volume output\n"

        _ceph_volume.side_effect = _ceph_volume_empty
        with pytest.raises(Exception, match='ceph-volume returned empty value'):
            out, _ = agent._ceph_volume(False)


def test_agent_daemon_ls_subset(cephadm_fs):
    # Basing part of this test on some actual sample output

    # Some sample "podman stats --format '{{.ID}},{{.MemUsage}}' --no-stream" output
    # 3f2b31d19ecd,456.4MB / 41.96GB
    # 5aca2499e0f8,7.082MB / 41.96GB
    # fe0cef07d5f7,35.91MB / 41.96GB

    # Sample "podman ps --format '{{.ID}},{{.Names}}' --no-trunc" output with the same containers
    # fe0cef07d5f71c5c604f7d1b4a4ac2e27873c96089d015014524e803361b4a30,ceph-4434fa7c-5602-11ed-b719-5254006ef86b-mon-host1
    # 3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e70660340cd1f,ceph-4434fa7c-5602-11ed-b719-5254006ef86b-mgr-host1-pntmho
    # 5aca2499e0f8fb903788ff90eb03fe6ed58c7ed177caf278fed199936aff7b4a,ceph-4434fa7c-5602-11ed-b719-5254006ef86b-crash-host1

    # Some of the components from that output
    mgr_cid = '3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e70660340cd1f'
    mon_cid = 'fe0cef07d5f71c5c604f7d1b4a4ac2e27873c96089d015014524e803361b4a30'
    crash_cid = '5aca2499e0f8fb903788ff90eb03fe6ed58c7ed177caf278fed199936aff7b4a'
    mgr_short_cid = mgr_cid[0:12]
    mon_short_cid = mon_cid[0:12]
    crash_short_cid = crash_cid[0:12]

    #Rebuilding the output but with our testing FSID and components (to allow alteration later for whatever reason)
    mem_out = f"""{mgr_short_cid},456.4MB / 41.96GB
{crash_short_cid},7.082MB / 41.96GB
{mon_short_cid},35.91MB / 41.96GB"""

    ps_out = f"""{mon_cid},ceph-{FSID}-mon-host1
{mgr_cid},ceph-{FSID}-mgr-host1-pntmho
{crash_cid},ceph-{FSID}-crash-host1"""

    def _fake_call(ctx, cmd, desc=None, verbosity=_cephadm.CallVerbosity.VERBOSE_ON_FAILURE, timeout=_cephadm.DEFAULT_TIMEOUT, **kwargs):
        if 'stats' in cmd:
            return (mem_out, '', 0)
        elif 'ps' in cmd:
            return (ps_out, '', 0)
        return ('out', 'err', 0)

    cephadm_fs.create_dir(AGENT_DIR)
    cephadm_fs.create_dir(f'/var/lib/ceph/mon/ceph-host1')  # legacy daemon
    cephadm_fs.create_dir(f'/var/lib/ceph/osd/nothing')  # improper directory, should be skipped
    cephadm_fs.create_dir(f'/var/lib/ceph/{FSID}/mgr.host1.pntmho')  # cephadm daemon
    cephadm_fs.create_dir(f'/var/lib/ceph/{FSID}/crash.host1')  # cephadm daemon

    with with_cephadm_ctx([]) as ctx:
        ctx.fsid = FSID
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        _cephadm.call.side_effect = _fake_call
        daemons = agent._daemon_ls_subset()

        assert 'agent.host1' in daemons
        assert 'mgr.host1.pntmho' in daemons
        assert 'crash.host1' in daemons
        assert 'mon.host1' in daemons

        assert daemons['mon.host1']['style'] == 'legacy'
        assert daemons['mgr.host1.pntmho']['style'] == 'cephadm:v1'
        assert daemons['crash.host1']['style'] == 'cephadm:v1'
        assert daemons['agent.host1']['style'] == 'cephadm:v1'

        assert daemons['mgr.host1.pntmho']['systemd_unit'] == f'ceph-{FSID}@mgr.host1.pntmho'
        assert daemons['agent.host1']['systemd_unit'] == f'ceph-{FSID}@agent.host1'
        assert daemons['crash.host1']['systemd_unit'] == f'ceph-{FSID}@crash.host1'

        assert daemons['mgr.host1.pntmho']['container_id'] == mgr_cid
        assert daemons['crash.host1']['container_id'] == crash_cid

        assert daemons['mgr.host1.pntmho']['memory_usage'] == 478570086  # 456.4 MB
        assert daemons['crash.host1']['memory_usage'] == 7426015  # 7.082 MB


@mock.patch("cephadm.list_daemons")
@mock.patch("cephadm.CephadmAgent._daemon_ls_subset")
def test_agent_get_ls(_ls_subset, _ls, cephadm_fs):
    ls_out = [{
        "style": "cephadm:v1",
        "name": "mgr.host1.pntmho",
        "fsid": FSID,
        "systemd_unit": f"ceph-{FSID}@mgr.host1.pntmho",
        "enabled": True,
        "state": "running",
        "service_name": "mgr",
        "memory_request": None,
        "memory_limit": None,
        "ports": [
            9283,
            8765
        ],
        "container_id": "3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e70660340cd1f",
        "container_image_name": "quay.io/ceph/ceph:testing",
        "container_image_id": "3300e39269f0c13ae45026cf233d8b3fff1303d52f2598a69c7fba0bb8405164",
        "container_image_digests": [
            "quay.io/ceph/ceph@sha256:d4f3522528ee79904f9e530bdce438acac30a039e9a0b3cf31d8b614f9f96a30"
        ],
        "memory_usage": 507510784,
        "cpu_percentage": "5.95%",
        "version": "18.0.0-556-gb4d1a199",
        "started": "2022-10-27T14:19:36.086664Z",
        "created": "2022-10-27T14:19:36.282281Z",
        "deployed": "2022-10-27T14:19:35.377275Z",
        "configured": "2022-10-27T14:22:40.316912Z"
    },{
        "style": "cephadm:v1",
        "name": "agent.host1",
        "fsid": FSID,
        "systemd_unit": f"ceph-{FSID}@agent.host1",
        "enabled": True,
        "state": "running",
        "service_name": "agent",
        "ports": [],
        "ip": None,
        "deployed_by": [
            "quay.io/ceph/ceph@sha256:d4f3522528ee79904f9e530bdce438acac30a039e9a0b3cf31d8b614f9f96a30"
        ],
        "rank": None,
        "rank_generation": None,
        "extra_container_args": None,
        "container_id": None,
        "container_image_name": None,
        "container_image_id": None,
        "container_image_digests": None,
        "version": None,
        "started": None,
        "created": "2022-10-27T19:46:49.751594Z",
        "deployed": None,
        "configured": "2022-10-27T19:46:49.751594Z"
    }, {
        "style": "legacy",
        "name": "mon.host1",
        "fsid": FSID,
        "systemd_unit": "ceph-mon@host1",
        "enabled": False,
        "state": "stopped",
        "host_version": None
    }]

    ls_subset_out = {
    'mgr.host1.pntmho': {
        "style": "cephadm:v1",
        "fsid": FSID,
        "systemd_unit": f"ceph-{FSID}@mgr.host1.pntmho",
        "enabled": True,
        "state": "running",
        "container_id": "3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e70660340cd1f",
        "memory_usage": 507510784,
    },
    'agent.host1': {
        "style": "cephadm:v1",
        "fsid": FSID,
        "systemd_unit": f"ceph-{FSID}@agent.host1",
        "enabled": True,
        "state": "running",
        "container_id": None
    }, 'mon.host1': {
        "style": "legacy",
        "name": "mon.host1",
        "fsid": FSID,
        "systemd_unit": "ceph-mon@host1",
        "enabled": False,
        "state": "stopped",
        "host_version": None
    }}

    _ls.return_value = ls_out
    _ls_subset.return_value = ls_subset_out

    with with_cephadm_ctx([]) as ctx:
        ctx.fsid = FSID
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)

        # first pass, no cached daemon metadata
        daemons, changed = agent._get_ls()
        assert daemons == ls_out
        assert changed

        # second pass, should recognize that daemons have not changed and just keep cached values
        daemons, changed = agent._get_ls()
        assert daemons == daemons
        assert not changed

        # change a container id so it needs to get more info
        ls_subset_out2 = copy.deepcopy(ls_subset_out)
        ls_out2 = copy.deepcopy(ls_out)
        ls_subset_out2['mgr.host1.pntmho']['container_id'] = '3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e7066034aaaaa'
        ls_out2[0]['container_id'] = '3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e7066034aaaaa'
        _ls.return_value = ls_out2
        _ls_subset.return_value = ls_subset_out2
        assert agent.cached_ls_values['mgr.host1.pntmho']['container_id'] == "3f2b31d19ecdd586640cc9c6ef7c0fe62157a3f7a71fcb60c91e70660340cd1f"
        daemons, changed = agent._get_ls()
        assert daemons == ls_out2
        assert changed

        # run again with the same data so it should use cached values
        daemons, changed = agent._get_ls()
        assert daemons == ls_out2
        assert not changed

        # change the state of a container so new daemon metadata is needed
        ls_subset_out3 = copy.deepcopy(ls_subset_out2)
        ls_out3 = copy.deepcopy(ls_out2)
        ls_subset_out3['mgr.host1.pntmho']['enabled'] = False
        ls_out3[0]['enabled'] = False
        _ls.return_value = ls_out3
        _ls_subset.return_value = ls_subset_out3
        assert agent.cached_ls_values['mgr.host1.pntmho']['enabled'] == True
        daemons, changed = agent._get_ls()
        assert daemons == ls_out3
        assert changed

        # run again with the same data so it should use cached values
        daemons, changed = agent._get_ls()
        assert daemons == ls_out3
        assert not changed

        # remove a daemon so new metadats is needed
        ls_subset_out4 = copy.deepcopy(ls_subset_out3)
        ls_out4 = copy.deepcopy(ls_out3)
        ls_subset_out4.pop('mon.host1')
        ls_out4.pop()
        _ls.return_value = ls_out4
        _ls_subset.return_value = ls_subset_out4
        assert 'mon.host1' in agent.cached_ls_values
        daemons, changed = agent._get_ls()
        assert daemons == ls_out4
        assert changed

        # run again with the same data so it should use cached values
        daemons, changed = agent._get_ls()
        assert daemons == ls_out4
        assert not changed


@mock.patch("threading.Event.clear")
@mock.patch("threading.Event.wait")
@mock.patch("urllib.request.Request.__init__")
@mock.patch("cephadm.urlopen")
@mock.patch("cephadm.list_networks")
@mock.patch("cephadm.HostFacts.dump")
@mock.patch("cephadm.HostFacts.__init__", lambda _, __: None)
@mock.patch("ssl.SSLContext.load_verify_locations")
@mock.patch("threading.Thread.is_alive")
@mock.patch("cephadm.MgrListener.start")
@mock.patch("cephadm.AgentGatherer.start")
@mock.patch("cephadm.port_in_use")
@mock.patch("cephadm.CephadmAgent.pull_conf_settings")
def test_agent_run(_pull_conf_settings, _port_in_use, _gatherer_start,
                   _listener_start, _is_alive, _load_verify_locations,
                    _HF_dump, _list_networks, _urlopen, _RQ_init, _wait, _clear):
    target_ip = '192.168.0.0'
    target_port = '9999'
    refresh_period = 20
    listener_port = 7770
    open_listener_port = 7777
    host = AGENT_ID
    device_enhanced_scan = False

    def _fake_port_in_use(ctx, port):
        if port == open_listener_port:
            return False
        return True

    network_data: Dict[str, Dict[str, Set[str]]] = {
        "10.2.1.0/24": {
            "eth1": set(["10.2.1.122"])
        },
        "192.168.122.0/24": {
            "eth0": set(["192.168.122.221"])
        },
        "fe80::/64": {
            "eth0": set(["fe80::5054:ff:fe3f:d94e"]),
            "eth1": set(["fe80::5054:ff:fe3f:aa4a"]),
        }
    }

    # the json serializable version of the networks data
    # we expect the agent to actually send
    network_data_no_sets: Dict[str, Dict[str, List[str]]] = {
        "10.2.1.0/24": {
            "eth1": ["10.2.1.122"]
        },
        "192.168.122.0/24": {
            "eth0": ["192.168.122.221"]
        },
        "fe80::/64": {
            "eth0": ["fe80::5054:ff:fe3f:d94e"],
            "eth1": ["fe80::5054:ff:fe3f:aa4a"],
        }
    }

    class FakeHTTPResponse():
        def __init__(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, type, value, tb):
            pass

        def read(self):
            return json.dumps({'valid': 'output', 'result': '400'})

    _port_in_use.side_effect = _fake_port_in_use
    _is_alive.return_value = False
    _HF_dump.return_value = 'Host Facts'
    _list_networks.return_value = network_data
    _urlopen.side_effect = lambda *args, **kwargs: FakeHTTPResponse()
    _RQ_init.side_effect = lambda *args, **kwargs: None
    with with_cephadm_ctx([]) as ctx:
        ctx.fsid = FSID
        agent = _cephadm.CephadmAgent(ctx, FSID, AGENT_ID)
        agent.keyring = 'agent keyring'
        agent.ack = 7
        agent.volume_gatherer.ack = 7
        agent.volume_gatherer.data = 'ceph-volume inventory data'
        agent.ls_gatherer.ack = 7
        agent.ls_gatherer.data = [{'valid_daemon': 'valid_metadata'}]

        def _set_conf():
            agent.target_ip = target_ip
            agent.target_port = target_port
            agent.loop_interval = refresh_period
            agent.starting_port = listener_port
            agent.host = host
            agent.device_enhanced_scan = device_enhanced_scan
        _pull_conf_settings.side_effect = _set_conf

        # technically the run function loops forever unless the agent
        # is told to stop. To get around that we're going to have the
        # event.wait() (which happens at the end of the loop) to throw
        # a special exception type. If we catch this exception we can
        # consider it as being a "success" run
        class EventCleared(Exception):
            pass

        _clear.side_effect = EventCleared('SUCCESS')
        with pytest.raises(EventCleared, match='SUCCESS'):
            agent.run()

        expected_data = {
           'host': host,
           'ls': [{'valid_daemon': 'valid_metadata'}],
           'networks': network_data_no_sets,
           'facts': 'Host Facts',
           'volume': 'ceph-volume inventory data',
           'ack': str(7),
           'keyring': 'agent keyring',
           'port': str(open_listener_port)
        }
        _RQ_init.assert_called_with(
            f'https://{target_ip}:{target_port}/data/',
            json.dumps(expected_data).encode('ascii'),
            {'Content-Type': 'application/json'}
        )
        _listener_start.assert_called()
        _gatherer_start.assert_called()
        _urlopen.assert_called()

        # agent should not go down if connections fail
        _urlopen.side_effect = Exception()
        with pytest.raises(EventCleared, match='SUCCESS'):
            agent.run()

        # should fail if no ports are open for listener
        _port_in_use.side_effect = lambda _, __: True
        agent.listener_port = None
        with pytest.raises(Exception, match='Failed to pick port for agent to listen on: All 1000 ports starting at 7770 taken.'):
            agent.run()
