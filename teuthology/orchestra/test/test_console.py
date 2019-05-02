from mock import patch

from teuthology.config import config as teuth_config

from teuthology.orchestra import console


class TestConsole(object):
    pass


class TestPhysicalConsole(TestConsole):
    klass = console.PhysicalConsole
    ipmi_cmd_templ = 'ipmitool -H {h}.{d} -I lanplus -U {u} -P {p} {c}'
    conserver_cmd_templ = 'console -M {m} -p {p} {mode} {h}'

    def setup(self):
        self.hostname = 'host'
        teuth_config.ipmi_domain = 'ipmi_domain'
        teuth_config.ipmi_user = 'ipmi_user'
        teuth_config.ipmi_password = 'ipmi_pass'
        teuth_config.conserver_master = 'conserver_master'
        teuth_config.conserver_port = 3109
        teuth_config.use_conserver = True

    def test_has_ipmi_creds(self):
        cons = self.klass(self.hostname)
        assert cons.has_ipmi_credentials is True
        teuth_config.ipmi_domain = None
        cons = self.klass(self.hostname)
        assert cons.has_ipmi_credentials is False

    def test_console_command_conserver(self):
        cons = self.klass(
            self.hostname,
            teuth_config.ipmi_user,
            teuth_config.ipmi_password,
            teuth_config.ipmi_domain,
        )
        cons.has_conserver = True
        console_cmd = cons._console_command()
        assert console_cmd == self.conserver_cmd_templ.format(
            m=teuth_config.conserver_master,
            p=teuth_config.conserver_port,
            mode='-s',
            h=self.hostname,
        )
        console_cmd = cons._console_command(readonly=False)
        assert console_cmd == self.conserver_cmd_templ.format(
            m=teuth_config.conserver_master,
            p=teuth_config.conserver_port,
            mode='-f',
            h=self.hostname,
        )

    def test_console_command_ipmi(self):
        teuth_config.conserver_master = None
        cons = self.klass(
            self.hostname,
            teuth_config.ipmi_user,
            teuth_config.ipmi_password,
            teuth_config.ipmi_domain,
        )
        sol_cmd = cons._console_command()
        assert sol_cmd == self.ipmi_cmd_templ.format(
            h=self.hostname,
            d=teuth_config.ipmi_domain,
            u=teuth_config.ipmi_user,
            p=teuth_config.ipmi_password,
            c='sol activate',
        )

    def test_ipmi_command_ipmi(self):
        cons = self.klass(
            self.hostname,
            teuth_config.ipmi_user,
            teuth_config.ipmi_password,
            teuth_config.ipmi_domain,
        )
        pc_cmd = cons._ipmi_command('power cycle')
        assert pc_cmd == self.ipmi_cmd_templ.format(
            h=self.hostname,
            d=teuth_config.ipmi_domain,
            u=teuth_config.ipmi_user,
            p=teuth_config.ipmi_password,
            c='power cycle',
        )

    def test_spawn_log_conserver(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            cons = self.klass(self.hostname)
            assert cons.has_conserver is True
            m_popen.reset_mock()
            m_popen.return_value.poll.return_value = None
            cons.spawn_sol_log('/fake/path')
            assert m_popen.call_count == 1
            call_args = m_popen.call_args_list[0][0][0]
            assert any(
                [teuth_config.conserver_master in arg for arg in call_args]
            )

    def test_spawn_log_ipmi(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 1
            m_popen.return_value.wait.return_value = 1
            cons = self.klass(self.hostname)
            assert cons.has_conserver is False
            m_popen.reset_mock()
            m_popen.return_value.poll.return_value = 1
            cons.spawn_sol_log('/fake/path')
            assert m_popen.call_count == 1
            call_args = m_popen.call_args_list[0][0][0]
            assert any(
                ['ipmitool' in arg for arg in call_args]
            )

    def test_spawn_log_fallback(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            cons = self.klass(self.hostname)
            assert cons.has_conserver is True
            m_popen.reset_mock()
            m_popen.return_value.poll.return_value = 1
            cons.spawn_sol_log('/fake/path')
            assert cons.has_conserver is False
            assert m_popen.call_count == 2
            call_args = m_popen.call_args_list[1][0][0]
            assert any(
                ['ipmitool' in arg for arg in call_args]
            )

    def test_get_console_conserver(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            cons = self.klass(self.hostname)
        assert cons.has_conserver is True
        with patch(
            'teuthology.orchestra.console.pexpect.spawn',
            autospec=True,
        ) as m_spawn:
            cons._get_console()
            assert m_spawn.call_count == 1
            assert teuth_config.conserver_master in \
                m_spawn.call_args_list[0][0][0]

    def test_get_console_ipmitool(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            cons = self.klass(self.hostname)
        assert cons.has_conserver is True
        with patch(
            'teuthology.orchestra.console.pexpect.spawn',
            autospec=True,
        ) as m_spawn:
            cons.has_conserver = False
            cons._get_console()
            assert m_spawn.call_count == 1
            assert 'ipmitool' in m_spawn.call_args_list[0][0][0]

    def test_get_console_fallback(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            cons = self.klass(self.hostname)
        assert cons.has_conserver is True
        with patch(
            'teuthology.orchestra.console.pexpect.spawn',
            autospec=True,
        ) as m_spawn:
            cons.has_conserver = True
            m_spawn.return_value.isalive.return_value = False
            cons._get_console()
            assert m_spawn.return_value.isalive.call_count == 1
            assert m_spawn.call_count == 2
            assert cons.has_conserver is False
            assert 'ipmitool' in m_spawn.call_args_list[1][0][0]

    def test_disable_conserver(self):
        with patch(
            'teuthology.orchestra.console.psutil.subprocess.Popen',
            autospec=True,
        ) as m_popen:
            m_popen.return_value.pid = 42
            m_popen.return_value.returncode = 0
            m_popen.return_value.wait.return_value = 0
            teuth_config.use_conserver = False
            cons = self.klass(self.hostname)
            assert cons.has_conserver is False
