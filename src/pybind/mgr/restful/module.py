"""
A RESTful API for Ceph
"""
from __future__ import absolute_import

import os
import json
import time
import errno
import inspect
import tempfile
import threading
import traceback
import six
import socket

from . import common
from . import context

from uuid import uuid4
from pecan import jsonify, make_app
from OpenSSL import crypto
from pecan.rest import RestController
from six import iteritems
from werkzeug.serving import make_server, make_ssl_devcert

from .hooks import ErrorHook
from mgr_module import MgrModule, CommandResult


class CannotServe(Exception):
    pass


class CommandsRequest(object):
    """
    This class handles parallel as well as sequential execution of
    commands. The class accept a list of iterables that should be
    executed sequentially. Each iterable can contain several commands
    that can be executed in parallel.

    Example:
    [[c1,c2],[c3,c4]]
     - run c1 and c2 in parallel
     - wait for them to finish
     - run c3 and c4 in parallel
     - wait for them to finish
    """


    def __init__(self, commands_arrays):
        self.id = str(id(self))

        # Filter out empty sub-requests
        commands_arrays = [x for x in commands_arrays
                           if len(x) != 0]

        self.running = []
        self.waiting = commands_arrays[1:]
        self.finished = []
        self.failed = []

        self.lock = threading.RLock()
        if not len(commands_arrays):
            # Nothing to run
            return

        # Process first iteration of commands_arrays in parallel
        results = self.run(commands_arrays[0])

        self.running.extend(results)


    def run(self, commands):
        """
        A static method that will execute the given list of commands in
        parallel and will return the list of command results.
        """

        # Gather the results (in parallel)
        results = []
        for index in range(len(commands)):
            tag = '%s:%s:%d' % (__name__, self.id, index)

            # Store the result
            result = CommandResult(tag)
            result.command = common.humanify_command(commands[index])
            results.append(result)

            # Run the command
            context.instance.send_command(result, 'mon', '', json.dumps(commands[index]), tag)

        return results


    def next(self):
        with self.lock:
            if not self.waiting:
                # Nothing to run
                return

            # Run a next iteration of commands
            commands = self.waiting[0]
            self.waiting = self.waiting[1:]

            self.running.extend(self.run(commands))


    def finish(self, tag):
        with self.lock:
            for index in range(len(self.running)):
                if self.running[index].tag == tag:
                    if self.running[index].r == 0:
                        self.finished.append(self.running.pop(index))
                    else:
                        self.failed.append(self.running.pop(index))
                    return True

            # No such tag found
            return False


    def is_running(self, tag):
        for result in self.running:
            if result.tag == tag:
                return True
        return False


    def is_ready(self):
        with self.lock:
            return not self.running and self.waiting


    def is_waiting(self):
        return bool(self.waiting)


    def is_finished(self):
        with self.lock:
            return not self.running and not self.waiting


    def has_failed(self):
        return bool(self.failed)


    def get_state(self):
        with self.lock:
            if not self.is_finished():
                return "pending"

            if self.has_failed():
                return "failed"

            return "success"


    def __json__(self):
        return {
            'id': self.id,
            'running': map(
                lambda x: {
                    'command': x.command,
                    'outs': x.outs,
                    'outb': x.outb,
                },
                self.running
            ),
            'finished': map(
                lambda x: {
                    'command': x.command,
                    'outs': x.outs,
                    'outb': x.outb,
                },
                self.finished
            ),
            'waiting': map(
                lambda x: map(
                    lambda y: common.humanify_command(y),
                    x
                ),
                self.waiting
            ),
            'failed': map(
                lambda x: {
                    'command': x.command,
                    'outs': x.outs,
                    'outb': x.outb,
                },
                self.failed
            ),
            'is_waiting': self.is_waiting(),
            'is_finished': self.is_finished(),
            'has_failed': self.has_failed(),
            'state': self.get_state(),
        }



class Module(MgrModule):
    MODULE_OPTIONS = [
        {'name': 'server_addr'},
        {'name': 'server_port'},
        {'name': 'key_file'},
    ]

    COMMANDS = [
        {
            "cmd": "restful create-key name=key_name,type=CephString",
            "desc": "Create an API key with this name",
            "perm": "rw"
        },
        {
            "cmd": "restful delete-key name=key_name,type=CephString",
            "desc": "Delete an API key with this name",
            "perm": "rw"
        },
        {
            "cmd": "restful list-keys",
            "desc": "List all API keys",
            "perm": "r"
        },
        {
            "cmd": "restful create-self-signed-cert",
            "desc": "Create localized self signed certificate",
            "perm": "rw"
        },
        {
            "cmd": "restful restart",
            "desc": "Restart API server",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        context.instance = self

        self.requests = []
        self.requests_lock = threading.RLock()

        self.keys = {}
        self.disable_auth = False

        self.server = None

        self.stop_server = False
        self.serve_event = threading.Event()


    def serve(self):
        while not self.stop_server:
            try:
                self._serve()
                self.server.socket.close()
            except CannotServe as cs:
                self.log.warn("server not running: %s", cs)
            except:
                self.log.error(str(traceback.format_exc()))

            # Wait and clear the threading event
            self.serve_event.wait()
            self.serve_event.clear()

    def refresh_keys(self):
        self.keys = {}
        rawkeys = self.get_store_prefix('keys/') or {}
        for k, v in six.iteritems(rawkeys):
            self.keys[k[5:]] = v  # strip of keys/ prefix

    def _serve(self):
        # Load stored authentication keys
        self.refresh_keys()

        jsonify._instance = jsonify.GenericJSON(
            sort_keys=True,
            indent=4,
            separators=(',', ': '),
        )

        server_addr = self.get_localized_module_option('server_addr', '::')
        if server_addr is None:
            raise CannotServe('no server_addr configured; try "ceph config-key set mgr/restful/server_addr <ip>"')

        server_port = int(self.get_localized_module_option('server_port', '8003'))
        self.log.info('server_addr: %s server_port: %d',
                      server_addr, server_port)

        cert = self.get_localized_store("crt")
        if cert is not None:
            cert_tmp = tempfile.NamedTemporaryFile()
            cert_tmp.write(cert.encode('utf-8'))
            cert_tmp.flush()
            cert_fname = cert_tmp.name
        else:
            cert_fname = self.get_localized_store('crt_file')

        pkey = self.get_localized_store("key")
        if pkey is not None:
            pkey_tmp = tempfile.NamedTemporaryFile()
            pkey_tmp.write(pkey.encode('utf-8'))
            pkey_tmp.flush()
            pkey_fname = pkey_tmp.name
        else:
            pkey_fname = self.get_localized_module_option('key_file')

        if not cert_fname or not pkey_fname:
            raise CannotServe('no certificate configured')
        if not os.path.isfile(cert_fname):
            raise CannotServe('certificate %s does not exist' % cert_fname)
        if not os.path.isfile(pkey_fname):
            raise CannotServe('private key %s does not exist' % pkey_fname)

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri("https://{0}:{1}/".format(
            socket.gethostname() if server_addr == "::" else server_addr,
            server_port
        ))

        # Create the HTTPS werkzeug server serving pecan app
        self.server = make_server(
            host=server_addr,
            port=server_port,
            app=make_app(
                root='restful.api.Root',
                hooks = [ErrorHook()],  # use a callable if pecan >= 0.3.2
            ),
            ssl_context=(cert_fname, pkey_fname),
        )

        self.server.serve_forever()


    def shutdown(self):
        try:
            self.stop_server = True
            if self.server:
                self.server.shutdown()
            self.serve_event.set()
        except:
            self.log.error(str(traceback.format_exc()))
            raise


    def restart(self):
        try:
            if self.server:
                self.server.shutdown()
            self.serve_event.set()
        except:
            self.log.error(str(traceback.format_exc()))


    def notify(self, notify_type, tag):
        try:
            self._notify(notify_type, tag)
        except:
            self.log.error(str(traceback.format_exc()))


    def _notify(self, notify_type, tag):
        if notify_type != "command":
            self.log.debug("Unhandled notification type '%s'", notify_type)
            return
        # we can safely skip all the sequential commands
        if tag == 'seq':
            return
        try:
            with self.requests_lock:
                request = next(x for x in self.requests if x.is_running(tag))
            request.finish(tag)
            if request.is_ready():
                request.next()
        except StopIteration:
            # the command was not issued by me
            pass


    def create_self_signed_cert(self):
        # create a key pair
        pkey = crypto.PKey()
        pkey.generate_key(crypto.TYPE_RSA, 2048)

        # create a self-signed cert
        cert = crypto.X509()
        cert.get_subject().O = "IT"
        cert.get_subject().CN = "ceph-restful"
        cert.set_serial_number(int(uuid4()))
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10*365*24*60*60)
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(pkey)
        cert.sign(pkey, 'sha512')

        return (
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert),
            crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey)
        )


    def handle_command(self, inbuf, command):
        self.log.warn("Handling command: '%s'" % str(command))
        if command['prefix'] == "restful create-key":
            if command['key_name'] in self.keys:
                return 0, self.keys[command['key_name']], ""

            else:
                key = str(uuid4())
                self.keys[command['key_name']] = key
                self.set_store('keys/' + command['key_name'], key)

            return (
                0,
                self.keys[command['key_name']],
                "",
            )

        elif command['prefix'] == "restful delete-key":
            if command['key_name'] in self.keys:
                del self.keys[command['key_name']]
                self.set_store('keys/' + command['key_name'], None)

            return (
                0,
                "",
                "",
            )

        elif command['prefix'] == "restful list-keys":
            self.refresh_keys()
            return (
                0,
                json.dumps(self.keys, indent=2),
                "",
            )

        elif command['prefix'] == "restful create-self-signed-cert":
            cert, pkey = self.create_self_signed_cert()
            self.set_store(self.get_mgr_id() + '/crt', cert.decode('utf-8'))
            self.set_store(self.get_mgr_id() + '/key', pkey.decode('utf-8'))

            self.restart()
            return (
                0,
                "Restarting RESTful API server...",
                ""
            )

        elif command['prefix'] == 'restful restart':
            self.restart();
            return (
                0,
                "Restarting RESTful API server...",
                ""
            )

        else:
            return (
                -errno.EINVAL,
                "",
                "Command not found '{0}'".format(command['prefix'])
            )


    def get_doc_api(self, root, prefix=''):
        doc = {}
        for _obj in dir(root):
            obj = getattr(root, _obj)

            if isinstance(obj, RestController):
                doc.update(self.get_doc_api(obj, prefix + '/' + _obj))

        if getattr(root, '_lookup', None) and isinstance(root._lookup('0')[0], RestController):
            doc.update(self.get_doc_api(root._lookup('0')[0], prefix + '/<arg>'))

        prefix = prefix or '/'

        doc[prefix] = {}
        for method in 'get', 'post', 'patch', 'delete':
            if getattr(root, method, None):
                doc[prefix][method.upper()] = inspect.getdoc(getattr(root, method)).split('\n')

        if len(doc[prefix]) == 0:
            del doc[prefix]

        return doc


    def get_mons(self):
        mon_map_mons = self.get('mon_map')['mons']
        mon_status = json.loads(self.get('mon_status')['json'])

        # Add more information
        for mon in mon_map_mons:
            mon['in_quorum'] = mon['rank'] in mon_status['quorum']
            mon['server'] = self.get_metadata("mon", mon['name'])['hostname']
            mon['leader'] = mon['rank'] == mon_status['quorum'][0]

        return mon_map_mons


    def get_osd_pools(self):
        osds = dict(map(lambda x: (x['osd'], []), self.get('osd_map')['osds']))
        pools = dict(map(lambda x: (x['pool'], x), self.get('osd_map')['pools']))
        crush = self.get('osd_map_crush')
        crush_rules = crush['rules']

        osds_by_pool = {}
        for pool_id, pool in pools.items():
            pool_osds = None
            for rule in [r for r in crush_rules if r['rule_id'] == pool['crush_rule']]:
                if rule['min_size'] <= pool['size'] <= rule['max_size']:
                    pool_osds = common.crush_rule_osds(crush['buckets'], rule)

            osds_by_pool[pool_id] = pool_osds

        for pool_id in pools.keys():
            for in_pool_id in osds_by_pool[pool_id]:
                osds[in_pool_id].append(pool_id)

        return osds


    def get_osds(self, pool_id=None, ids=None):
        # Get data
        osd_map = self.get('osd_map')
        osd_metadata = self.get('osd_metadata')

        # Update the data with the additional info from the osd map
        osds = osd_map['osds']

        # Filter by osd ids
        if ids is not None:
            osds = [x for x in osds if str(x['osd']) in ids]

        # Get list of pools per osd node
        pools_map = self.get_osd_pools()

        # map osd IDs to reweight
        reweight_map = dict([
            (x.get('id'), x.get('reweight', None))
            for x in self.get('osd_map_tree')['nodes']
        ])

        # Build OSD data objects
        for osd in osds:
            osd['pools'] = pools_map[osd['osd']]
            osd['server'] = osd_metadata.get(str(osd['osd']), {}).get('hostname', None)

            osd['reweight'] = reweight_map.get(osd['osd'], 0.0)

            if osd['up']:
                osd['valid_commands'] = common.OSD_IMPLEMENTED_COMMANDS
            else:
                osd['valid_commands'] = []

        # Filter by pool
        if pool_id:
            pool_id = int(pool_id)
            osds = [x for x in osds if pool_id in x['pools']]

        return osds


    def get_osd_by_id(self, osd_id):
        osd = [x for x in self.get('osd_map')['osds']
               if x['osd'] == osd_id]

        if len(osd) != 1:
            return None

        return osd[0]


    def get_pool_by_id(self, pool_id):
        pool = [x for x in self.get('osd_map')['pools']
                if x['pool'] == pool_id]

        if len(pool) != 1:
            return None

        return pool[0]


    def submit_request(self, _request, **kwargs):
        with self.requests_lock:
            request = CommandsRequest(_request)
            self.requests.append(request)
        if kwargs.get('wait', 0):
            while not request.is_finished():
                time.sleep(0.001)
        return request


    def run_command(self, command):
        # tag with 'seq' so that we can ignore these in notify function
        result = CommandResult('seq')

        self.send_command(result, 'mon', '', json.dumps(command), 'seq')
        return result.wait()
