from copy import deepcopy
from datetime import datetime
from mock import patch, DEFAULT, PropertyMock
from pytest import raises, mark

from teuthology.config import config
from teuthology.exceptions import MaxWhileTries
from teuthology.provision import fog


test_config = dict(fog=dict(
    endpoint='http://fog.example.com/fog',
    api_token='API_TOKEN',
    user_token='USER_TOKEN',
    machine_types='type1,type2',
))


class TestFOG(object):
    klass = fog.FOG

    def setup(self):
        config.load()
        config.update(deepcopy(test_config))
        self.start_patchers()

    def start_patchers(self):
        self.patchers = dict()
        self.patchers['m_sleep'] = patch(
            'time.sleep',
        )
        self.patchers['m_requests_Session_send'] = patch(
            'requests.Session.send',
        )
        self.patchers['m_Remote_connect'] = patch(
            'teuthology.orchestra.remote.Remote.connect'
        )
        self.patchers['m_Remote_run'] = patch(
            'teuthology.orchestra.remote.Remote.run'
        )
        self.patchers['m_Remote_console'] = patch(
            'teuthology.orchestra.remote.Remote.console',
            new_callable=PropertyMock,
        )
        self.patchers['m_Remote_hostname'] = patch(
            'teuthology.orchestra.remote.Remote.hostname',
            new_callable=PropertyMock,
        )
        self.patchers['m_Remote_machine_type'] = patch(
            'teuthology.orchestra.remote.Remote.machine_type',
            new_callable=PropertyMock,
        )
        self.mocks = dict()
        for name, patcher in self.patchers.items():
            self.mocks[name] = patcher.start()

    def teardown(self):
        for patcher in self.patchers.values():
            patcher.stop()

    @mark.parametrize('enabled', [True, False])
    def test_get_types(self, enabled):
        with patch('teuthology.provision.fog.enabled') as m_enabled:
            m_enabled.return_value = enabled
            types = fog.get_types()
        if enabled:
            assert types == test_config['fog']['machine_types'].split(',')
        else:
            assert types == []

    def test_disabled(self):
        config.fog['endpoint'] = None
        obj = self.klass('name.fqdn', 'type', '1.0')
        with raises(RuntimeError):
            obj.create()

    def test_init(self):
        self.mocks['m_Remote_hostname'].return_value = 'name.fqdn'
        obj = self.klass('name.fqdn', 'type', '1.0')
        assert obj.name == 'name.fqdn'
        assert obj.shortname == 'name'
        assert obj.os_type == 'type'
        assert obj.os_version == '1.0'

    @mark.parametrize('success', [True, False])
    def test_create(self, success):
        self.mocks['m_Remote_hostname'].return_value = 'name.fqdn'
        self.mocks['m_Remote_machine_type'].return_value = 'type1'
        obj = self.klass('name.fqdn', 'type', '1.0')
        host_id = 99
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            get_host_data=DEFAULT,
            set_image=DEFAULT,
            schedule_deploy_task=DEFAULT,
            wait_for_deploy_task=DEFAULT,
            cancel_deploy_task=DEFAULT,
            _wait_for_ready=DEFAULT,
            _fix_hostname=DEFAULT,
        ) as local_mocks:
            local_mocks['get_host_data'].return_value = dict(id=host_id)
            if not success:
                local_mocks['wait_for_deploy_task'].side_effect = RuntimeError
                with raises(RuntimeError):
                    obj.create()
            else:
                obj.create()
            assert local_mocks['get_host_data'].called_once_with()
            assert local_mocks['set_image'].called_once_with(host_id)
            assert local_mocks['schedule_deploy_task']\
                .called_once_with(host_id)
            assert local_mocks['wait_for_deploy_task'].called_once_with()
            if success:
                assert local_mocks['_wait_for_ready'].called_once_with()
                assert local_mocks['_fix_hostname'].called_once_with()
            else:
                assert len(local_mocks['cancel_deploy_task'].call_args_list) == 1
        assert self.mocks['m_Remote_console']\
            .return_value.power_off.called_once_with()
        assert self.mocks['m_Remote_console']\
            .return_value.power_on.called_once_with()

    def test_do_request(self):
        obj = self.klass('name.fqdn', 'type', '1.0')
        obj.do_request('test_url', data='DATA', method='GET')
        assert len(self.mocks['m_requests_Session_send'].call_args_list) == 1
        req = self.mocks['m_requests_Session_send'].call_args_list[0][0][0]
        assert req.url == test_config['fog']['endpoint'] + 'test_url'
        assert req.method == 'GET'
        assert req.headers['fog-api-token'] == test_config['fog']['api_token']
        assert req.headers['fog-user-token'] == test_config['fog']['user_token']
        assert req.body == 'DATA'

    @mark.parametrize(
        'count',
        [0, 1, 2],
    )
    def test_get_host_data(self, count):
        host_objs = [dict(id=i) for i in range(count)]
        resp_obj = dict(count=count, hosts=host_objs)
        self.mocks['m_requests_Session_send']\
            .return_value.json.return_value = resp_obj
        obj = self.klass('name.fqdn', 'type', '1.0')
        if count != 1:
            with raises(RuntimeError):
                result = obj.get_host_data()
            return
        result = obj.get_host_data()
        assert len(self.mocks['m_requests_Session_send'].call_args_list) == 1
        req = self.mocks['m_requests_Session_send'].call_args_list[0][0][0]
        assert req.url == test_config['fog']['endpoint'] + '/host'
        assert req.body == '{"name": "name"}'
        assert result == host_objs[0]

    @mark.parametrize(
        'count',
        [0, 1, 2],
    )
    def test_get_image_data(self, count):
        img_objs = [dict(id=i) for i in range(count)]
        resp_obj = dict(count=count, images=img_objs)
        self.mocks['m_requests_Session_send']\
            .return_value.json.return_value = resp_obj
        self.mocks['m_Remote_machine_type'].return_value = 'type1'
        obj = self.klass('name.fqdn', 'windows', 'xp')
        if count < 1:
            with raises(RuntimeError):
                result = obj.get_image_data()
            return
        result = obj.get_image_data()
        assert len(self.mocks['m_requests_Session_send'].call_args_list) == 1
        req = self.mocks['m_requests_Session_send'].call_args_list[0][0][0]
        assert req.url == test_config['fog']['endpoint'] + '/image'
        assert req.body == '{"name": "type1_windows_xp"}'
        assert result == img_objs[0]

    def test_set_image(self):
        self.mocks['m_Remote_hostname'].return_value = 'name.fqdn'
        self.mocks['m_Remote_machine_type'].return_value = 'type1'
        host_id = 999
        obj = self.klass('name.fqdn', 'type', '1.0')
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            get_image_data=DEFAULT,
            do_request=DEFAULT,
        ) as local_mocks:
            local_mocks['get_image_data'].return_value = dict(id='13')
            obj.set_image(host_id)
            assert local_mocks['do_request'].called_once_with(
                '/host/999', 'put', '{"imageID": "13"}',
            )

    def test_schedule_deploy_task(self):
        host_id = 12
        tasktype_id = 6
        task_id = 5
        tasktype_result = dict(tasktypes=[dict(name='deploy', id=tasktype_id)])
        schedule_result = dict()
        host_tasks = [dict(
            createdTime=datetime.strftime(
                datetime.utcnow(), self.klass.timestamp_format),
            id=task_id,
        )]
        self.mocks['m_requests_Session_send']\
            .return_value.json.side_effect = [
            tasktype_result, schedule_result,
        ]
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            get_deploy_tasks=DEFAULT,
        ) as local_mocks:
            local_mocks['get_deploy_tasks'].return_value = host_tasks
            obj = self.klass('name.fqdn', 'type', '1.0')
            result = obj.schedule_deploy_task(host_id)
            assert local_mocks['get_deploy_tasks'].called_once_with()
        assert len(self.mocks['m_requests_Session_send'].call_args_list) == 3
        assert result == task_id

    def test_get_deploy_tasks(self):
        obj = self.klass('name.fqdn', 'type', '1.0')
        resp_obj = dict(
            count=2,
            tasks=[
                dict(host=dict(name='notme')),
                dict(host=dict(name='name')),
            ]
        )
        self.mocks['m_requests_Session_send']\
            .return_value.json.return_value = resp_obj
        result = obj.get_deploy_tasks()
        assert result[0]['host']['name'] == 'name'

    @mark.parametrize(
        'active_ids',
        [
            [2, 4, 6, 8],
            [1],
            [],
        ]
    )
    def test_deploy_task_active(self, active_ids):
        our_task_id = 4
        result_objs = [dict(id=task_id) for task_id in active_ids]
        obj = self.klass('name.fqdn', 'type', '1.0')
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            get_deploy_tasks=DEFAULT,
        ) as local_mocks:
            local_mocks['get_deploy_tasks'].return_value = result_objs
            result = obj.deploy_task_active(our_task_id)
            assert result is (our_task_id in active_ids)

    @mark.parametrize(
        'tries',
        [3, 45],
    )
    def test_wait_for_deploy_task(self, tries):
        wait_results = [True for i in range(tries)] + [False]
        obj = self.klass('name.fqdn', 'type', '1.0')
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            deploy_task_active=DEFAULT,
        ) as local_mocks:
            local_mocks['deploy_task_active'].side_effect = wait_results
            if tries >= 40:
                with raises(MaxWhileTries):
                    obj.wait_for_deploy_task(9)
                return
            obj.wait_for_deploy_task(9)
            assert len(local_mocks['deploy_task_active'].call_args_list) == \
                tries + 1

    def test_cancel_deploy_task(self):
        obj = self.klass('name.fqdn', 'type', '1.0')
        with patch.multiple(
            'teuthology.provision.fog.FOG',
            do_request=DEFAULT,
        ) as local_mocks:
            obj.cancel_deploy_task(10)
            assert local_mocks['do_request'].called_once_with(
                '/task/cancel',
                method='DELETE',
                data='{"id": 10}',
            )

    @mark.parametrize(
        'tries',
        [1, 51],
    )
    def test_wait_for_ready(self, tries):
        connect_results = [MaxWhileTries for i in range(tries)] + [True]
        obj = self.klass('name.fqdn', 'type', '1.0')
        self.mocks['m_Remote_connect'].side_effect = connect_results
        if tries >= 50:
            with raises(MaxWhileTries):
                obj._wait_for_ready()
            return
        obj._wait_for_ready()
        assert len(self.mocks['m_Remote_connect'].call_args_list) == tries + 1
