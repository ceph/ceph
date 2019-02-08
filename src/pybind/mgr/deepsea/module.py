# vim: ts=8 et sw=4 sts=4
"""
ceph-mgr DeepSea orchestrator module
"""

# We want orchestrator methods in this to be 1:1 mappings to DeepSea runners,
# we don't want to aggregate multiple salt invocations here, because that means
# this module would need to know too much about how DeepSea works internally.
# Better to expose new runners from DeepSea to match what the orchestrator needs.

import json
import errno
import requests

from threading import Event, Thread, Lock

from mgr_module import MgrModule
import orchestrator


class RequestException(Exception):
    def __init__(self, message, status_code=None):
        super(RequestException, self).__init__(message)
        self.status_code = status_code


class DeepSeaReadCompletion(orchestrator.ReadCompletion):
    def __init__(self, process_result_callback):
        super(DeepSeaReadCompletion, self).__init__()
        self._complete = False
        self._cb = process_result_callback

    def _process_result(self, data):
        self._result = self._cb(data)
        self._complete = True

    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return self._complete


class DeepSeaOrchestrator(MgrModule, orchestrator.Orchestrator):
    MODULE_OPTIONS = [
        {
            'name': 'salt_api_url',
            'default': None
        },
        {
            'name': 'salt_api_eauth',
            'default': 'sharedsecret'
        },
        {
            'name': 'salt_api_username',
            'default': None
        },
        {
            'name': 'salt_api_password',
            'default': None
        }
    ]


    COMMANDS = [
        {
            "cmd": "deepsea config-set name=key,type=CephString "
                   "name=value,type=CephString",
            "desc": "Set a configuration value",
            "perm": "rw"
        },
        {
            "cmd": "deepsea config-show",
            "desc": "Show current configuration",
            "perm": "r"
        }
    ]


    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None)) for o in self.MODULE_OPTIONS)


    def get_module_option(self, key, default=None):
        """
        Overrides the default MgrModule get_module_option() method to pull in defaults
        specific to this module
        """
        return super(DeepSeaOrchestrator, self).get_module_option(key, default=self.config_keys[key])


    def _config_valid(self):
        for key in self.config_keys.keys():
            if not self.get_module_option(key, self.config_keys[key]):
                return False
        return True


    def __init__(self, *args, **kwargs):
        super(DeepSeaOrchestrator, self).__init__(*args, **kwargs)
        self._event = Event()
        self._token = None
        self._event_reader = None
        self._reading_events = False
        self._last_failure_msg = None
        self._all_completions = dict()
        self._completion_lock = Lock()


    def available(self):
        if not self._config_valid():
            return False, "Configuration invalid; try `ceph deepsea config-set [...]`"

        if not self._reading_events and self._last_failure_msg:
            return False, self._last_failure_msg

        return True, ""


    def get_inventory(self, node_filter=None, refresh=False):
        """
        Note that this will raise an exception (e.g. if the salt-api is down,
        or the username/password is incorret).  Same for other methods.
        Callers should expect this and react appropriately.  The orchestrator
        cli, for example, just prints the traceback in the console, so the
        user at least sees the error.
        """

        def process_result(event_data):
            result = []
            if event_data['success']:
                for node_name, node_devs in event_data["return"].items():
                    devs = []
                    for d in node_devs:
                        dev = orchestrator.InventoryDevice()
                        dev.blank = d['blank']
                        dev.type = d['type']
                        dev.id = d['id']
                        dev.size = d['size']
                        dev.extended = d['extended']
                        dev.metadata_space_free = d['metadata_space_free']
                        devs.append(dev)
                    result.append(orchestrator.InventoryNode(node_name, devs))
            return result

        with self._completion_lock:
            c = DeepSeaReadCompletion(process_result)

            nodes = []
            roles = []
            if node_filter:
                nodes = node_filter.nodes
                roles = node_filter.labels

            resp = self._do_request_with_login("POST", data = {
                "client": "runner_async",
                "fun": "mgr_orch.get_inventory",
                "nodes": nodes,
                "roles": roles
            })

            # ['return'][0]['tag'] in the resonse JSON is what we need to match
            # on when looking for the result event (e.g.: "salt/run/20181018074024331230")
            self._all_completions["{}/ret".format(resp.json()['return'][0]['tag'])] = c

            return c


    def describe_service(self, service_type, service_id, node_name):

        # Note: describe_service() does *not* support OSDs.  This is because
        # DeepSea doesn't really record what OSDs are deployed where; Ceph is
        # considered the canonical source of this information, so having this
        # function query OSD information from DeepSea doesn't make a lot of
        # sense (DeepSea would have to call back into Ceph).

        assert service_type in ("mon", "mgr", "mds", "rgw", None), service_type + " unsupported"

        def process_result(event_data):
            result = []
            if event_data['success']:
                for node_name, service_info in event_data["return"].items():
                    for service_type, service_instance in service_info.items():
                        desc = orchestrator.ServiceDescription()
                        desc.nodename = node_name
                        desc.service_instance = service_instance
                        desc.service_type = service_type
                        result.append(desc)
            return result

        with self._completion_lock:
            c = DeepSeaReadCompletion(process_result)

            resp = self._do_request_with_login("POST", data = {
                "client": "runner_async",
                "fun": "mgr_orch.describe_service",
                "role": service_type,
                "service_id": service_id,
                "node": node_name
            })
            self._all_completions["{}/ret".format(resp.json()['return'][0]['tag'])] = c

            return c


    def wait(self, completions):
        incomplete = False

        with self._completion_lock:
            for c in completions:
                if c.is_complete:
                    continue
                if not c.is_complete:
                    # TODO: the job is in the bus, it should reach us eventually
                    # unless something has gone wrong (e.g. salt-api died, etc.),
                    # in which case it's possible the job finished but we never
                    # noticed the salt/run/$id/ret event.  Need to add the job ID
                    # (or possibly the full event tag) to the completion object.
                    # That way, if we want to double check on a job that hasn't
                    # been completed yet, we can make a synchronous request to
                    # salt-api to invoke jobs.lookup_jid, and if it's complete we
                    # should be able to pass its return value to _process_result()
                    # Question: do we do this automatically after some timeout?
                    # Or do we add a function so the admin can check and "unstick"
                    # a stuck completion?
                    incomplete = True

        return not incomplete


    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == 'deepsea config-show':
            return 0, json.dumps(dict([(key, self.get_module_option(key)) for key in self.config_keys.keys()])), ''

        elif cmd['prefix'] == 'deepsea config-set':
            if cmd['key'] not in self.config_keys.keys():
                return (-errno.EINVAL, '',
                        "Unknown configuration option '{0}'".format(cmd['key']))

            self.set_module_option(cmd['key'], cmd['value'])
            self._event.set();
            return 0, "Configuration option '{0}' updated".format(cmd['key']), ''

        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(cmd['prefix']))


    def serve(self):
        self.log.info('DeepSea module starting up')
        self.run = True
        while self.run:
            if not self._config_valid():
                # This will spin until the config is valid, spitting a warning
                # that the config is invalid every 60 seconds.  The one oddity
                # is that while setting the various parameters, this log warning
                # will print once for each parameter set until the config is valid.
                self.log.warn("Configuration invalid; try `ceph deepsea config-set [...]`")
                self._event.wait(60)
                self._event.clear()
                continue

            if self._event_reader and not self._reading_events:
                self._event_reader = None

            if not self._event_reader:
                self._last_failure_msg = None
                try:
                    # This spawns a separate thread to read the salt event bus
                    # stream.  We can't do it in the serve thead, because reading
                    # from the response blocks, which would prevent the serve
                    # thread from handling anything else.
                    #
                    # TODO: figure out how to restart the _event_reader thread if
                    # config changes, e.g.: a new username or password is set.
                    # This will be difficult, because _read_sse() just blocks waiting
                    # for response lines.  The closest I got was setting a read timeout
                    # on the request, but in the general case (where not much is
                    # happening most of the time), this will result in continual
                    # timeouts and reconnects.  We really need an asynchronous read
                    # to support this.
                    self._event_response = self._do_request_with_login("GET", "events", stream=True)
                    self._event_reader = Thread(target=self._read_sse)
                    self._reading_events = True
                    self._event_reader.start()
                except Exception as ex:
                    self._set_last_failure_msg("Failure setting up event reader: " + str(ex))
                    # gives an (arbitrary) 60 second retry if we can't attach to
                    # the salt-api event bus for some reason (e.g.: invalid username,
                    # or password, which will be logged as "Request failed with status
                    # code 401").  Note that this 60 second retry will also happen if
                    # salt-api dies.
                    self._event.wait(60)
                    self._event.clear()
                    continue

            # Wait indefinitely for something interesting to happen (e.g.
            # config-set, or shutdown), or the event reader to fail, which
            # will happen if the salt-api server dies or restarts).
            self._event.wait()
            self._event.clear()


    def shutdown(self):
        self.log.info('DeepSea module shutting down')
        self.run = False
        self._event.set()


    def _set_last_failure_msg(self, msg):
        self._last_failure_msg = msg
        self.log.warn(msg)


    # Reader/parser of SSE events, see:
    # - https://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#events)
    # - https://www.w3.org/TR/2009/WD-eventsource-20090421/
    # Note: this is pretty braindead and doesn't implement the full eventsource
    # spec, but it *does* implement enough for us to listen to events from salt
    # and potentially do something with them.
    def _read_sse(self):
        event = {}
        try:
            # Just starting the event reader; if we've made it here, we know we're
            # talking to salt-api (_do_request would have raised an exception if the
            # response wasn't ok), so check if there's any completions inflight that
            # need to be dealt with.  This handles the case where some command was
            # invoked, then salt-api died somehow, and we reconneced, but missed the
            # completion at the time it actually happened.
            for tag in list(self._all_completions):
                self.log.info("Found event {} inflight".format(tag))
                try:
                    resp = self._do_request_with_login("POST", data = {
                        "client": "runner",
                        "fun": "jobs.lookup_jid",
                        "jid": tag.split('/')[2]
                    })
                    # jobs.lookup_jid returns a dict keyed by hostname.
                    return_dict = resp.json()['return'][0]
                    if return_dict:
                        # If the job is complete, there'll be one item in the dict.
                        self.log.info("Event {} complete".format(tag))
                        # The key is the salt master hostname, but we don't care
                        # about that, so just grab the data.
                        data = next(iter(return_dict.items()))[1]
                        self._all_completions[tag]._process_result(data)
                        # TODO: decide whether it's bad to delete the completion
                        # here -- would we ever need to resurrect it?
                        del self._all_completions[tag]
                    else:
                        # if the job is not complete, there'll be nothing in the dict
                        self.log.info("Event {} still pending".format(tag))
                except Exception as ex:
                    # Logging a warning if the request failed, so we can continue
                    # checking any other completions, then get onto reading events
                    self.log.warn("Error looking up inflight event {}: {}".format(tag, str(ex)))

            for line in self._event_response.iter_lines():
                with self._completion_lock:
                    if line:
                        line = line.decode('utf-8')
                        colon = line.find(':')
                        if colon > 0:
                            k = line[:colon]
                            v = line[colon+2:]
                            if k == "retry":
                                # TODO: find out if we need to obey this reconnection time
                                self.log.warn("Server requested retry {}, ignored".format(v))
                            else:
                                event[k] = v
                    else:
                        # Empty line, terminates an event.  Note that event['tag']
                        # is a salt-api extension to SSE to avoid having to decode
                        # json data if you don't care about it.  To get to the
                        # interesting stuff, you want event['data'], which is json.
                        # If you want to have some fun, try
                        # `ceph daemon mgr.$(hostname) config set debug_mgr 20`
                        # then `salt '*' test.ping` on the master
                        self.log.debug("Got event '{}'".format(str(event)))

                        # If we're actually interested in this event (i.e. it's
                        # in our completion dict), fire off that completion's
                        # _process_result() callback and remove it from our list.
                        if event['tag'] in self._all_completions:
                            self.log.info("Event {} complete".format(event['tag']))
                            self._all_completions[event['tag']]._process_result(json.loads(event['data'])['data'])
                            # TODO: decide whether it's bad to delete the completion
                            # here -- would we ever need to resurrect it?
                            del self._all_completions[event['tag']]

                        event = {}
            self._set_last_failure_msg("SSE read terminated")
        except Exception as ex:
            self.log.exception(ex)
            self._set_last_failure_msg("SSE read failed: {}".format(str(ex)))

        self._reading_events = False
        self._event.set()


    # _do_request(), _login() and _do_request_with_login() are an extremely
    # minimalist form of the following, with notably terse error handling:
    # https://bitbucket.org/openattic/openattic/src/ce4543d4cbedadc21b484a098102a16efec234f9/backend/rest_client.py?at=master&fileviewer=file-view-default
    # https://bitbucket.org/openattic/openattic/src/ce4543d4cbedadc21b484a098102a16efec234f9/backend/deepsea.py?at=master&fileviewer=file-view-default
    # rationale:
    # - I needed slightly different behaviour than in openATTIC (I want the
    #   caller to read the response, to allow streaming the salt-api event bus)
    # - I didn't want to pull in 400+ lines more code into this presently
    #   experimental module, to save everyone having to review it ;-)

    def _do_request(self, method, path="", data=None, stream=False):
        """
        returns the response, which the caller then has to read
        """
        url = "{0}/{1}".format(self.get_module_option('salt_api_url'), path)
        try:
            if method.lower() == 'get':
                resp = requests.get(url, headers = { "X-Auth-Token": self._token },
                                    data=data, stream=stream)
            elif method.lower() == 'post':
                resp = requests.post(url, headers = { "X-Auth-Token": self._token },
                                     data=data)

            else:
                raise RequestException("Method '{}' not supported".format(method.upper()))
            if resp.ok:
                return resp
            else:
                msg = "Request failed with status code {}".format(resp.status_code)
                raise RequestException(msg, resp.status_code)
        except requests.exceptions.ConnectionError as ex:
            self.log.exception(str(ex))
            raise RequestException(str(ex))
        except requests.exceptions.InvalidURL as ex:
            self.log.exception(str(ex))
            raise RequestException(str(ex))


    def _login(self):
        resp = self._do_request('POST', 'login', data = {
            "eauth": self.get_module_option('salt_api_eauth'),
            "sharedsecret" if self.get_module_option('salt_api_eauth') == 'sharedsecret' else 'password': self.get_module_option('salt_api_password'),
            "username": self.get_module_option('salt_api_username')
        })
        self._token = resp.json()['return'][0]['token']
        self.log.info("Salt API login successful")


    def _do_request_with_login(self, method, path="", data=None, stream=False):
        retries = 2
        while True:
            try:
                if not self._token:
                    self._login()
                return self._do_request(method, path, data, stream)
            except RequestException as ex:
                retries -= 1
                if ex.status_code not in [401, 403] or retries == 0:
                    raise ex
                self._token = None
