from nose.tools import eq_ as eq

import fudge
import nose

from .util import assert_raises

from .. import connection


def test_split_user_just_host():
    got = connection.split_user('somehost.example.com')
    eq(got, (None, 'somehost.example.com'))


def test_split_user_both():
    got = connection.split_user('jdoe@somehost.example.com')
    eq(got, ('jdoe', 'somehost.example.com'))


def test_split_user_empty_user():
    s = '@somehost.example.com'
    e = assert_raises(AssertionError, connection.split_user, s)
    eq(str(e), 'Bad input to split_user: {s!r}'.format(s=s))


@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_connect():
    ssh = fudge.Fake('SSHClient')
    ssh.expects_call().with_args().returns(ssh)
    ssh.expects('set_missing_host_key_policy')
    ssh.expects('load_system_host_keys').with_args()
    ssh.expects('connect').with_args(
        hostname='orchestra.test.newdream.net.invalid',
        username='jdoe',
        timeout=60,
    )
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.remember_order()
    transport.expects('set_keepalive').with_args(False)
    got = connection.connect(
        'jdoe@orchestra.test.newdream.net.invalid',
        _SSHClient=ssh,
    )
    assert got is ssh

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_connect_override_hostkeys():
    sshclient = fudge.Fake('SSHClient')
    ssh = sshclient.expects_call().with_args().returns_fake()
    ssh.remember_order()
    host_keys = fudge.Fake('HostKeys')
    host_keys.expects('add').with_args(
        hostname='orchestra.test.newdream.net.invalid',
        keytype='ssh-rsa',
        key='frobnitz',
        )
    ssh.expects('get_host_keys').with_args().returns(host_keys)
    ssh.expects('connect').with_args(
        hostname='orchestra.test.newdream.net.invalid',
        username='jdoe',
        timeout=60,
        )
    transport = ssh.expects('get_transport').with_args().returns_fake()
    transport.remember_order()
    transport.expects('set_keepalive').with_args(False)
    create_key = fudge.Fake('create_key')
    create_key.expects_call().with_args('ssh-rsa', 'testkey').returns('frobnitz')
    got = connection.connect(
        'jdoe@orchestra.test.newdream.net.invalid',
        host_key='ssh-rsa testkey',
        _SSHClient=sshclient,
        _create_key=create_key,
        )
    assert got is ssh
