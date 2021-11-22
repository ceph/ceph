#!@Python3_EXECUTABLE@
# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab
#
# Processed in Makefile to add python #! line and version variable
#
#

import subprocess
import random
import string
import json
import argparse
import sys
import socket
import base64
import logging

from urllib.parse import urlparse

from ceph.rgw.rgwam_core import RGWAM, EnvArgs
from ceph.rgw.types import RGWAMEnvMgr, RGWAMException

class RGWAMCLIMgr(RGWAMEnvMgr):
    def __init__(self, common_args):
        args = []

        if common_args.conf_path:
            args += [ '-c', common_args.conf_path ]

        if common_args.ceph_name:
            args += [ '-n', common_args.ceph_name ]

        if common_args.ceph_keyring:
            args += [ '-k', common_args.ceph_keyring ]

        self.args_prefix = args

    def tool_exec(self, prog, args):
        run_cmd = [ prog ] + self.args_prefix + args

        result = subprocess.run(run_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout = result.stdout.decode('utf-8')
        stderr = result.stderr.decode('utf-8')

        return run_cmd, result.returncode, stdout, stderr

    def apply_rgw(self, svc_id, realm_name, zone_name, port = None):
        return None

    def list_daemons(self, service_name, daemon_type = None, daemon_id = None, hostname = None, refresh = True):
        return []

class RealmCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            usage='''rgwam realm <subcommand>

The subcommands are:
   bootstrap                     Bootstrap new realm
   new-zone-creds                Create credentials for connecting new zone
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])

        sub = args.subcommand.replace('-', '_')

        if not hasattr(self, sub):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name

        return getattr(self, sub)

    def bootstrap(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm bootstrap [<args>]')
        parser.add_argument('--realm')
        parser.add_argument('--zonegroup')
        parser.add_argument('--zone')
        parser.add_argument('--endpoints')
        parser.add_argument('--sys-uid')
        parser.add_argument('--uid')
        parser.add_argument('--start-radosgw', action='store_true', dest='start_radosgw', default=True)
        parser.add_argument('--no-start-radosgw', action='store_false', dest='start_radosgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.env).realm_bootstrap(args.realm, args.zonegroup, args.zone, args.endpoints,
                args.sys_uid, args.uid, args.start_radosgw)

    def new_zone_creds(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm new-zone-creds [<args>]')
        parser.add_argument('--endpoints')
        parser.add_argument('--sys-uid')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.env).realm_new_zone_creds(args.endpoints, args.sys_uid)


class ZoneCommand:
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            usage='''rgwam zone <subcommand>

The subcommands are:
   run                     run radosgw daemon in current zone
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def run(self):
        parser = argparse.ArgumentParser(
            description='Run radosgw daemon',
            usage='rgwam zone run [<args>]')
        parser.add_argument('--port')
        parser.add_argument('--log-file')
        parser.add_argument('--debug-ms')
        parser.add_argument('--debug-rgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.env).run_radosgw(port = args.port)

    def create(self):
        parser = argparse.ArgumentParser(
            description='Create new zone to join existing realm',
            usage='rgwam zone create [<args>]')
        parser.add_argument('--realm-token')
        parser.add_argument('--zone')
        parser.add_argument('--zonegroup')
        parser.add_argument('--endpoints')
        parser.add_argument('--start-radosgw', action='store_true', dest='start_radosgw', default=True)
        parser.add_argument('--no-start-radosgw', action='store_false', dest='start_radosgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.env).zone_create(args.realm_token, args.zonegroup, args.zone, args.endpoints, args.start_radosgw)

class CommonArgs:
    def __init__(self, ns):
        self.conf_path = ns.conf_path
        self.ceph_name = ns.ceph_name
        self.ceph_keyring = ns.ceph_keyring

class TopLevelCommand:

    def _parse(self):
        parser = argparse.ArgumentParser(
            description='RGW assist for multisite tool',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog='''
The commands are:
   realm bootstrap               Bootstrap new realm
   realm new-zone-creds          Create credentials to connect new zone to realm
   zone create                   Create new zone and connect it to existing realm
   zone run                      Run radosgw in current zone
''')

        parser.add_argument('command', help='command to run', default=None)
        parser.add_argument('-c', help='ceph conf path', dest='conf_path')
        parser.add_argument('-n', help='ceph user name', dest='ceph_name')
        parser.add_argument('-k', help='ceph keyring', dest='ceph_keyring')

        removed_args = []

        args = sys.argv[1:]
        if len(args) > 0:
            if hasattr(self, args[0]):
                # remove -h/--help if top command is not empty so that top level help
                # doesn't override subcommand, we'll add it later
                help_args = [ '-h', '--help' ]
                removed_args = [arg for arg in args if arg in help_args]
                args = [arg for arg in args if arg not in help_args]

        (ns, args) = parser.parse_known_args(args)
        if not hasattr(self, ns.command) or ns.command[0] == '_':
            print('Unrecognized command:', ns.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        args += removed_args
        return (getattr(self, ns.command), CommonArgs(ns), args)

    def realm(self, env, args):
        cmd = RealmCommand(env, args).parse()
        return cmd()

    def zone(self, env, args):
        cmd = ZoneCommand(env, args).parse()
        return cmd()


def main():
    logging.basicConfig(level=logging.INFO)

    log = logging.getLogger(__name__)

    (cmd, common_args, args)= TopLevelCommand()._parse()

    env = EnvArgs(RGWAMCLIMgr(common_args))

    try:
        retval, out, err = cmd(env, args)
        if retval != 0:
            log.error('stdout: '+ out + '\nstderr: ' + err)
            sys.exit(retval)
    except RGWAMException as e:
        print('ERROR: ' + e.message)

    sys.exit(0)


if __name__ == '__main__':
    main()

