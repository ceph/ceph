#!/usr/bin/python3

import json
import shlex
import subprocess


class UnexpectedReturn(Exception):
    def __init__(self, cmd, ret, expected, msg):
        if isinstance(cmd, list):
            self.cmd = ' '.join(cmd)
        else:
            assert isinstance(cmd, str), \
                'cmd needs to be either a list or a str'
            self.cmd = cmd
        self.cmd = str(self.cmd)
        self.ret = int(ret)
        self.expected = int(expected)
        self.msg = str(msg)

    def __str__(self):
        return repr('{c}: expected return {e}, got {r} ({o})'.format(
            c=self.cmd, e=self.expected, r=self.ret, o=self.msg))


def call(cmd):
    if isinstance(cmd, list):
        args = cmd
    elif isinstance(cmd, str):
        args = shlex.split(cmd)
    else:
        assert False, 'cmd is not a string/unicode nor a list!'

    print('call: {0}'.format(args))
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    procout, procerr = proc.communicate(None)

    return proc.returncode, procout, procerr


def expect(cmd, expected_ret):
    try:
        (r, out, err) = call(cmd)
    except ValueError as e:
        assert False, \
            'unable to run {c}: {err}'.format(c=repr(cmd), err=str(e))

    if r != expected_ret:
        raise UnexpectedReturn(repr(cmd), r, expected_ret, err)

    return out.decode() if isinstance(out, bytes) else out


def get_quorum_status(timeout=300):
    cmd = 'ceph quorum_status'
    if timeout > 0:
        cmd += ' --connect-timeout {0}'.format(timeout)

    out = expect(cmd, 0)
    j = json.loads(out)
    return j


def main():
    quorum_status = get_quorum_status()
    mon_names = [mon['name'] for mon in quorum_status['monmap']['mons']]

    print('ping all monitors')
    for m in mon_names:
        print('ping mon.{0}'.format(m))
        out = expect('ceph ping mon.{0}'.format(m), 0)
        reply = json.loads(out)

        assert reply['mon_status']['name'] == m, \
            'reply obtained from mon.{0}, expected mon.{1}'.format(
                reply['mon_status']['name'], m)

    print('test out-of-quorum reply')
    for m in mon_names:
        print('testing mon.{0}'.format(m))
        expect('ceph daemon mon.{0} quorum exit'.format(m), 0)

        quorum_status = get_quorum_status()
        assert m not in quorum_status['quorum_names'], \
            'mon.{0} was not supposed to be in quorum ({1})'.format(
                m, quorum_status['quorum_names'])

        out = expect('ceph ping mon.{0}'.format(m), 0)
        reply = json.loads(out)
        mon_status = reply['mon_status']

        assert mon_status['name'] == m, \
            'reply obtained from mon.{0}, expected mon.{1}'.format(
                mon_status['name'], m)

        assert mon_status['state'] == 'electing', \
            'mon.{0} is in state {1}, expected electing'.format(
                m, mon_status['state'])

        expect('ceph daemon mon.{0} quorum enter'.format(m), 0)

    print('OK')


if __name__ == '__main__':
    main()
