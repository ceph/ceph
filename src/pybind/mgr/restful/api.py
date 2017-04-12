from pecan import expose, request, response
from pecan.rest import RestController

import common
import traceback

from base64 import b64decode
from functools import wraps
from collections import defaultdict

## We need this to access the instance of the module
#
# We can't use 'from module import instance' because
# the instance is not ready, yet (would be None)
import module


# Helper function to catch and log the exceptions
def catch(f):
    @wraps(f)
    def catcher(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            module.instance.log.error(str(traceback.format_exc()))
            response.status = 500
            return {'message': str(traceback.format_exc()).split('\n')}
    return catcher


# Handle authorization
def auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not request.authorization:
            response.status = 401
            response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
            return {'message': 'auth: No HTTP username/password'}

        username, password = b64decode(request.authorization[1]).split(':')

        # Lookup the password-less tokens first
        if username not in module.instance.tokens.values():
            # Check the ceph auth db
            msg = module.instance.verify_user(username, password)
            if msg:
                response.status = 401
                response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
                return {'message': 'auth: No HTTP username/password'}

        return f(*args, **kwargs)
    return decorated


# Helper function to lock the function
def lock(f):
    @wraps(f)
    def locker(*args, **kwargs):
        with module.instance.requests_lock:
            return f(*args, **kwargs)
    return locker



class ServerFqdn(RestController):
    def __init__(self, fqdn):
        self.fqdn = fqdn


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for the server fqdn
        """
        return module.instance.get_server(self.fqdn)



class Server(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for all the servers
        """
        return module.instance.list_servers()


    @expose()
    def _lookup(self, fqdn, *remainder):
        return ServerFqdn(fqdn), remainder



class RequestId(RestController):
    def __init__(self, request_id):
        self.request_id = request_id


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for the request id
        """
        request = filter(
            lambda x: x.id == self.request_id,
            module.instance.requests
        )

        if len(request) != 1:
            response.status = 500
            return {'message': 'Unknown request id "%s"' % str(self.request_id)}

        request = request[0]
        return request.humanify()


    @expose('json')
    @catch
    @auth
    @lock
    def delete(self):
        """
        Remove the request id from the database
        """
        for index in range(len(module.instance.requests)):
            if module.instance.requests[index].id == self.request_id:
                return module.instance.requests.pop(index).humanify()

        # Failed to find the job to cancel
        response.status = 500
        return {'message': 'No such request id'}



class Request(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        List all the available requests and their state
        """
        states = {}
        for _request in module.instance.requests:
            states[_request.id] = _request.get_state()

        return states


    @expose('json')
    @catch
    @auth
    @lock
    def delete(self):
        """
        Remove all the finished requests
        """
        num_requests = len(module.instance.requests)

        module.instance.requests = filter(
            lambda x: not x.is_finished(),
            module.instance.requests
        )

        # Return the job statistics
        return {
            'cleaned': num_requests - len(module.instance.requests),
            'remaining': len(module.instance.requests),
        }


    @expose()
    def _lookup(self, request_id, *remainder):
        return RequestId(request_id), remainder



class PoolId(RestController):
    def __init__(self, pool_id):
        self.pool_id = pool_id


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for the pool id
        """
        pool = module.instance.get_pool_by_id(self.pool_id)

        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        # pgp_num is called pg_placement_num, deal with that
        if 'pg_placement_num' in pool:
            pool['pgp_num'] = pool.pop('pg_placement_num')
        return pool


    @expose('json')
    @catch
    @auth
    def patch(self):
        """
        Modify the information for the pool id
        """
        args = request.json

        # Get the pool info for its name
        pool = module.instance.get_pool_by_id(self.pool_id)
        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            response.status = 500
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}

        # Schedule the update request
        return module.instance.submit_request(common.pool_update_commands(pool['pool_name'], args))


    @expose('json')
    @catch
    @auth
    def delete(self):
        """
        Remove the pool data for the pool id
        """
        pool = module.instance.get_pool_by_id(self.pool_id)

        if not pool:
            response.status = 500
            return {'message': 'Failed to identify the pool id "%d"' % self.pool_id}

        return module.instance.submit_request([[{
            'prefix': 'osd pool delete',
            'pool': pool['pool_name'],
            'pool2': pool['pool_name'],
            'sure': '--yes-i-really-really-mean-it'
        }]])



class Pool(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for all the pools
        """
        pools = module.instance.get('osd_map')['pools']

        # pgp_num is called pg_placement_num, deal with that
        for pool in pools:
            if 'pg_placement_num' in pool:
                pool['pgp_num'] = pool.pop('pg_placement_num')

        return pools


    @expose('json')
    @catch
    @auth
    def post(self):
        """
        Create a new pool
        Requires name and pg_num dict arguments
        """
        args = request.json

        # Check for the required arguments
        pool_name = args.pop('name', None)
        if pool_name is None:
            response.status = 500
            return {'message': 'You need to specify the pool "name" argument'}

        pg_num = args.pop('pg_num', None)
        if pg_num is None:
            response.status = 500
            return {'message': 'You need to specify the "pg_num" argument'}

        # Run the pool create command first
        create_command = {
            'prefix': 'osd pool create',
            'pool': pool_name,
            'pg_num': pg_num
        }

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            response.status = 500
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}

        # Schedule the creation and update requests
        return module.instance.submit_request(
            [[create_command]] +
            common.pool_update_commands(pool_name, args)
        )


    @expose()
    def _lookup(self, pool_id, *remainder):
        return PoolId(int(pool_id)), remainder



class OsdIdCommand(RestController):
    def __init__(self, osd_id):
        self.osd_id = osd_id


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show implemented commands for the OSD id
        """
        osd = module.instance.get_osd_by_id(self.osd_id)

        if not osd:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        if osd['up']:
            return common.OSD_IMPLEMENTED_COMMANDS
        else:
            return []


    @expose('json')
    @catch
    @auth
    def post(self):
        """
        Run the implemented command for the OSD id
        """
        command = request.json.get('command', None)

        osd = module.instance.get_osd_by_id(self.osd_id)

        if not osd:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        if not osd['up'] or command not in common.OSD_IMPLEMENTED_COMMANDS:
            response.status = 500
            return {'message': 'Command "%s" not available' % command}

        return module.instance.submit_request([[{
            'prefix': 'osd ' + command,
            'who': str(self.osd_id)
        }]])



class OsdId(RestController):
    def __init__(self, osd_id):
        self.osd_id = osd_id
        self.command = OsdIdCommand(osd_id)


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for the OSD id
        """
        osd = module.instance.get_osds([str(self.osd_id)])
        if len(osd) != 1:
            response.status = 500
            return {'message': 'Failed to identify the OSD id "%d"' % self.osd_id}

        return osd[0]


    @expose('json')
    @catch
    @auth
    def patch(self):
        """
        Modify the state (up, in) of the OSD id or reweight it
        """
        args = request.json

        commands = []

        if 'in' in args:
            if args['in']:
                commands.append({
                    'prefix': 'osd in',
                    'ids': [str(self.osd_id)]
                })
            else:
                commands.append({
                    'prefix': 'osd out',
                    'ids': [str(self.osd_id)]
                })

        if 'up' in args:
            if args['up']:
                response.status = 500
                return {'message': "It is not valid to set a down OSD to be up"}
            else:
                commands.append({
                    'prefix': 'osd down',
                    'ids': [str(self.osd_id)]
                })

        if 'reweight' in args:
            commands.append({
                'prefix': 'osd reweight',
                'id': self.osd_id,
                'weight': args['reweight']
            })

        return module.instance.submit_request([commands])



class Osd(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for all the OSDs
        """
        # Parse request args
        ids = request.GET.getall('id[]')
        pool_id = request.GET.get('pool', None)

        return module.instance.get_osds(ids, pool_id)


    @expose()
    def _lookup(self, osd_id, *remainder):
        return OsdId(int(osd_id)), remainder



class MonName(RestController):
    def __init__(self, name):
        self.name = name


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for the monitor name
        """
        mon = filter(
            lambda x: x['name'] == self.name,
            module.instance.get_mons()
        )

        if len(mon) != 1:
            response.status = 500
            return {'message': 'Failed to identify the monitor node "%s"' % self.name}

        return mon[0]



class Mon(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show the information for all the monitors
        """
        return module.instance.get_mons()


    @expose()
    def _lookup(self, name, *remainder):
        return MonName(name), remainder



class Doc(RestController):
    @expose('json')
    @catch
    def get(self):
        """
        Show documentation information
        """
        return module.instance.get_doc_api(Root)



class CrushRuleset(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show crush rulesets
        """
        rules = module.instance.get('osd_map_crush')['rules']
        nodes = module.instance.get('osd_map_tree')['nodes']

        ruleset = defaultdict(list)
        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))
            ruleset[rule['ruleset']].append(rule)

        return ruleset



class CrushRule(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show crush rules
        """
        rules = module.instance.get('osd_map_crush')['rules']
        nodes = module.instance.get('osd_map_tree')['nodes']

        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))

        return rules



class Crush(RestController):
    rule = CrushRule()
    ruleset = CrushRuleset()



class ConfigOsd(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show OSD configuration options
        """
        flags = module.instance.get("osd_map")['flags']

        # pause is a valid osd config command that sets pauserd,pausewr
        flags = flags.replace('pauserd,pausewr', 'pause')

        return flags.split(',')


    @expose('json')
    @catch
    @auth
    def patch(self):
        """
        Modify OSD configration options
        """

        args = request.json

        commands = []

        valid_flags = set(args.keys()) & set(common.OSD_FLAGS)
        invalid_flags = list(set(args.keys()) - valid_flags)
        if invalid_flags:
            module.instance.log.warn("%s not valid to set/unset" % invalid_flags)

        for flag in list(valid_flags):
            if args[flag]:
                mode = 'set'
            else:
                mode = 'unset'

            commands.append({
                'prefix': 'osd ' + mode,
                'key': flag,
            })

        return module.instance.submit_request([commands])



class ConfigClusterKey(RestController):
    def __init__(self, key):
        self.key = key


    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show specific configuration option
        """
        return module.instance.get("config").get(self.key, None)



class ConfigCluster(RestController):
    @expose('json')
    @catch
    @auth
    def get(self):
        """
        Show all cluster configuration options
        """
        return module.instance.get("config")


    @expose()
    def _lookup(self, key, *remainder):
        return ConfigClusterKey(key), remainder



class Config(RestController):
    cluster = ConfigCluster()
    osd = ConfigOsd()



class Auth(RestController):
    @expose('json')
    @catch
    def get(self):
        """
        Generate a brand new password-less login token for the user
        Uses HTTP Basic Auth for authentication
        """
        if not request.authorization:
            return (
                {'message': 'auth: No HTTP username/password'},
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )

        username, password = b64decode(request.authorization[1]).split(':')
        # Do not create a new token for a username that is already a token
        if username in module.instance.tokens.values():
            return {
                'token': username
            }

        # Check the ceph auth db
        msg = module.instance.verify_user(username, password)
        if msg:
            return (
                {'message': 'auth: ' + msg},
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )

        # Create a password-less login token for the user
        # This overwrites any old user tokens
        return {
            'token': module.instance.set_token(username)
        }


    @expose('json')
    @catch
    @auth
    def delete(self):
        """
        Delete the password-less login token for the user
        """

        username, password = b64decode(request.authorization[1]).split(':')

        if module.instance.unset_token(username):
            return {'success': 'auth: Token removed'}

        response.status = 500
        return {'message': 'auth: No token for the user'}



class Root(RestController):
    auth = Auth()
    config = Config()
    crush = Crush()
    doc = Doc()
    mon = Mon()
    osd = Osd()
    pool = Pool()
    request = Request()
    server = Server()

    @expose('json')
    @catch
    def get(self):
        """
        Show the basic information for the REST API
        This includes values like api version or auth method
        """
        return {
            'api_version': 1,
            'auth':
                'Use ceph auth key pair as HTTP Basic user/password '
                '(requires caps mon allow * to function properly)',
            'doc': 'See /doc endpoint',
            'info': "Ceph Manager RESTful API server",
        }
