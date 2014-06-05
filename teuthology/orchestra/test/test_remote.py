import fudge
import fudge.inspector

from textwrap import dedent

from .. import remote
from ..run import RemoteProcess


class TestRemote(object):
    def test_shortname(self):
        r = remote.Remote(
            name='jdoe@xyzzy.example.com',
            shortname='xyz',
            ssh=fudge.Fake('SSHConnection'),
            )
        assert r.shortname == 'xyz'
        assert str(r) == 'jdoe@xyzzy.example.com'

    def test_shortname_default(self):
        r = remote.Remote(
            name='jdoe@xyzzy.example.com',
            ssh=fudge.Fake('SSHConnection'),
            )
        assert r.shortname == 'xyzzy'
        assert str(r) == 'jdoe@xyzzy.example.com'

    @fudge.with_fakes
    def test_run(self):
        fudge.clear_expectations()
        ssh = fudge.Fake('SSHConnection')
        ssh.expects('get_transport').returns_fake().expects('getpeername')\
            .returns(('name', 22))
        run = fudge.Fake('run')
        args = [
            'something',
            'more',
            ]
        foo = object()
        ret = RemoteProcess(
            client=ssh,
            args='fakey',
            )
        r = remote.Remote(name='jdoe@xyzzy.example.com', ssh=ssh)
        run.expects_call().with_args(
            client=fudge.inspector.arg.passes_test(lambda v: v is ssh),
            args=fudge.inspector.arg.passes_test(lambda v: v is args),
            foo=fudge.inspector.arg.passes_test(lambda v: v is foo),
            name=r.shortname,
            ).returns(ret)
        # monkey patch ook ook
        r._runner = run
        got = r.run(
            args=args,
            foo=foo,
            )
        assert got is ret
        assert got.remote is r


class TestDistribution(object):
    lsb_centos = dedent("""
        LSB Version:    :base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:core-4.0-noarch:graphics-4.0-amd64:graphics-4.0-noarch:printing-4.0-amd64:printing-4.0-noarch
        Distributor ID: CentOS
        Description:    CentOS release 6.5 (Final)
        Release:        6.5
        Codename:       Final
    """)

    lsb_ubuntu = dedent("""
        Distributor ID: Ubuntu
        Description:    Ubuntu 12.04.4 LTS
        Release:        12.04
        Codename:       precise
    """)

    def test_centos(self):
        d = remote.Distribution(self.lsb_centos)
        assert d.distributor == 'CentOS'
        assert d.description == 'CentOS release 6.5 (Final)'
        assert d.release == '6.5'
        assert d.codename == 'Final'
        assert d.name == 'centos'
        assert d.package_type == 'rpm'

    def test_ubuntu(self):
        d = remote.Distribution(self.lsb_ubuntu)
        assert d.distributor == 'Ubuntu'
        assert d.description == 'Ubuntu 12.04.4 LTS'
        assert d.release == '12.04'
        assert d.codename == 'precise'
        assert d.name == 'ubuntu'
        assert d.package_type == 'deb'
