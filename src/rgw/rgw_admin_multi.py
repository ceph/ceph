import subprocess
import random
import string
import json
import argparse
import sys

def rand_alphanum_lower(l):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=l))

class RGWAMException(BaseException):
    def __init__(self, message):
        self.message = message


class RGWAdminCmd:
    def __init__(self):
        self.cmd_prefix = [ 'radosgw-admin' ]

    def run(self, cmd):
        run_cmd = self.cmd_prefix + cmd
        result = subprocess.run(run_cmd, stdout=subprocess.PIPE)
        return (result.returncode, result.stdout)

class RealmOp(RGWAdminCmd):
    def __init__(self):
        RGWAdminCmd.__init__(self)
        
    def create(self, name = None, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'realm-' + rand_alphanum_lower(8)

        params = [ 'realm',
                   'create',
                   '--rgw-realm', self.name ]

        if is_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info


class RGWAM:
    def __init__(self):
        pass

    def realm_bootstrap(self, realm, zonegroup, zone, endpoints):
        realm_info = RealmOp().create(realm)
        if not realm_info:
            return

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        print('Created realm %s (%s)' % (realm_name, realm_id))

    def zonegroup_create(self, realm, zonegroup, zone, endpoints):
        realm_op = RealmOp()

        realm_info = realm_op.create(realm)
        if not realm_info:
            return

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        print('Created realm %s (%s)' % (realm_name, realm_id))


class RealmCommand:
    def __init__(self, args):
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            description='S3 control tool',
            usage='''rgwam realm <subcommand>

The subcommands are:
   bootstrap                     Manipulate bucket versioning
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

    def bootstrap(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm bootstrap [<args>]')
        parser.add_argument('--realm')
        parser.add_argument('--zonegroup')
        parser.add_argument('--zone')
        parser.add_argument('--endpoints')

        args = parser.parse_args(self.args[1:])

        RGWAM().realm_bootstrap(args.realm, args.zonegroup, args.zone, args.endpoints)

class TopLevelCommand:

    def _parse(self):
        parser = argparse.ArgumentParser(
            description='RGW assist for multisite tool',
            usage='''rgwam <command> [<args>]

The commands are:
   realm bootstrap               Bootstrap realm
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command) or args.command[0] == '_':
            print('Unrecognized command:', args.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.command)

    def realm(self):
        cmd = RealmCommand(sys.argv[2:]).parse()
        cmd()


def main():
    cmd = TopLevelCommand()._parse()
    try:
        cmd()
    except RGWAMException as e:
        print('ERROR: ' + e.message)


if __name__ == '__main__':
    main()

