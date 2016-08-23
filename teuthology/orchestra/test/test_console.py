from teuthology.config import config as teuth_config

from .. import console


class TestConsole(object):
    pass


class TestPhysicalConsole(TestConsole):
    klass = console.PhysicalConsole

    def setup(self):
        teuth_config.ipmi_domain = 'ipmi_domain'
        teuth_config.ipmi_user = 'ipmi_user'
        teuth_config.ipmi_password = 'ipmi_pass'
        self.hostname = 'host'

    def test_build_command(self):
        cmd_templ = 'ipmitool -H {h}.{d} -I lanplus -U {u} -P {p} {c}'
        cons = self.klass(
            self.hostname,
            teuth_config.ipmi_user,
            teuth_config.ipmi_password,
            teuth_config.ipmi_domain,
        )
        sol_cmd = cons._build_command('sol activate')
        assert sol_cmd == cmd_templ.format(
            h=self.hostname,
            d=teuth_config.ipmi_domain,
            u=teuth_config.ipmi_user,
            p=teuth_config.ipmi_password,
            c='sol activate',
        )
        pc_cmd = cons._build_command('power cycle')
        assert pc_cmd == sol_cmd.replace('sol activate', 'power cycle')

