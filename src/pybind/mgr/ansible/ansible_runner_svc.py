"""
Client module to interact with the Ansible Runner Service
"""

import json
import re
from functools import wraps
import collections
import logging
try:
    from typing import Optional, Dict, Any, List, Set
except ImportError:
    pass # just for type checking

import requests
from orchestrator import OrchestratorError

logger = logging.getLogger(__name__)

# Ansible Runner service API endpoints
API_URL = "api"
PLAYBOOK_EXEC_URL = "api/v1/playbooks"
PLAYBOOK_EVENTS = "api/v1/jobs/%s/events"
EVENT_DATA_URL = "api/v1/jobs/%s/events/%s"
URL_MANAGE_GROUP = "api/v1/groups/{group_name}"
URL_ADD_RM_HOSTS = "api/v1/hosts/{host_name}/groups/{inventory_group}"

class AnsibleRunnerServiceError(OrchestratorError):
    """Generic Ansible Runner Service Exception"""
    pass

def handle_requests_exceptions(func):
    """Decorator to manage errors raised by requests library
    """
    @wraps(func)
    def inner(*args, **kwargs):
        """Generic error mgmt decorator"""
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.RequestException, IOError) as ex:
            raise AnsibleRunnerServiceError(str(ex))
    return inner

class ExecutionStatusCode(object):
    """Execution status of playbooks ( 'msg' field in playbook status request)
    """

    SUCCESS = 0   # Playbook has been executed succesfully" msg = successful
    ERROR = 1     # Playbook has finished with error        msg = failed
    ON_GOING = 2  # Playbook is being executed              msg = running
    NOT_LAUNCHED = 3  # Not initialized

class PlayBookExecution(object):
    """Object to provide all the results of a Playbook execution
    """

    def __init__(self, rest_client,  # type: Client
                 playbook,  # type: str
                 result_pattern="",  # type: str
                 the_params=None,  # type: Optional[dict]
                 querystr_dict=None  # type: Optional[dict]
                 ):

        self.rest_client = rest_client

        # Identifier of the playbook execution
        self.play_uuid = "-"

        # Pattern used to extract the result from the events
        self.result_task_pattern = result_pattern

        # Playbook name
        self.playbook = playbook

        # Parameters used in the playbook
        self.params = the_params

        # Query string used in the "launch" request
        self.querystr_dict = querystr_dict

    def launch(self):
        """ Launch the playbook execution
        """

        response = None
        endpoint = "%s/%s" % (PLAYBOOK_EXEC_URL, self.playbook)

        try:
            response = self.rest_client.http_post(endpoint,
                                                  self.params,
                                                  self.querystr_dict)
        except AnsibleRunnerServiceError:
            logger.exception("Error launching playbook <%s>", self.playbook)
            raise

        # Here we have a server response, but an error trying
        # to launch the playbook is also posible (ex. 404, playbook not found)
        # Error already logged by rest_client, but an error should be raised
        # to the orchestrator (via completion object)
        if response.ok:
            self.play_uuid = json.loads(response.text)["data"]["play_uuid"]
            logger.info("Playbook execution launched succesfuly")
        else:
            raise AnsibleRunnerServiceError(response.reason)

    def get_status(self):
        """ Return the status of the execution

        In the msg field of the respons we can find:
            "msg": "successful"
            "msg": "running"
            "msg": "failed"
        """

        status_value = ExecutionStatusCode.NOT_LAUNCHED
        response = None

        if self.play_uuid == '-': # Initialized
            status_value = ExecutionStatusCode.NOT_LAUNCHED
        elif self.play_uuid == '': # Error launching playbook
            status_value = ExecutionStatusCode.ERROR
        else:
            endpoint = "%s/%s" % (PLAYBOOK_EXEC_URL, self.play_uuid)

            try:
                response = self.rest_client.http_get(endpoint)
            except AnsibleRunnerServiceError:
                logger.exception("Error getting playbook <%s> status",
                                   self.playbook)

            if response:
                the_status = json.loads(response.text)["msg"]
                if the_status == 'successful':
                    status_value = ExecutionStatusCode.SUCCESS
                elif the_status == 'failed':
                    status_value = ExecutionStatusCode.ERROR
                else:
                    status_value = ExecutionStatusCode.ON_GOING
            else:
                status_value = ExecutionStatusCode.ERROR

        logger.info("Requested playbook execution status is: %s", status_value)
        return status_value

    def get_result(self, event_filter):
        """Get the data of the events filtered by a task pattern and
        a event filter

        @event_filter: list of 0..N event names items
        @returns: the events that matches with the patterns provided
        """
        response = None
        if not self.play_uuid:
            return {}

        try:
            response = self.rest_client.http_get(PLAYBOOK_EVENTS % self.play_uuid)
        except AnsibleRunnerServiceError:
            logger.exception("Error getting playbook <%s> result", self.playbook)

        if not response:
            result_events = {}  # type: Dict[str, Any]
        else:
            events = json.loads(response.text)["data"]["events"]

            # Filter by task
            if self.result_task_pattern:
                result_events = {event:data for event, data in events.items()
                                 if "task" in data and
                                 re.match(self.result_task_pattern, data["task"])}
            else:
                result_events = events

            # Filter by event
            if event_filter:
                type_of_events = "|".join(event_filter)

                result_events = {event:data for event, data in result_events.items()
                                 if re.match(type_of_events, data['event'])}

        logger.info("Requested playbook result is: %s", json.dumps(result_events))
        return result_events

class Client(object):
    """An utility object that allows to connect with the Ansible runner service
    and execute easily playbooks
    """

    def __init__(self,
                 server_url,  # type: str
                 verify_server,  # type: bool
                 ca_bundle,  # type: str
                 client_cert, # type: str
                 client_key  # type: str
                 ):
        """Provide an https client to make easy interact with the Ansible
        Runner Service"

        :param server_url: The base URL >server>:<port> of the Ansible Runner
                           Service
        :param verify_server: A boolean to specify if server authentity should
                              be checked or not. (True by default)
        :param ca_bundle: If provided, an alternative Cert. Auth. bundle file
                          will be used as source for checking the authentity of
                          the Ansible Runner Service
        :param client_cert: Path to Ansible Runner Service client certificate
                            file
        :param client_key: Path to Ansible Runner Service client certificate key
                           file
        """
        self.server_url = server_url
        self.client_cert = (client_cert, client_key)

        # used to provide the "verify" parameter in requests
        # a boolean that sometimes contains a string :-(
        self.verify_server = verify_server
        if ca_bundle: # This intentionallly overwrites
            self.verify_server = ca_bundle  # type: ignore

        self.server_url = "https://{0}".format(self.server_url)

    @handle_requests_exceptions
    def is_operative(self):
        """Indicates if the connection with the Ansible runner Server is ok
        """

        # Check the service
        response = self.http_get(API_URL)

        if response:
            return response.status_code == requests.codes.ok
        else:
            return False

    @handle_requests_exceptions
    def http_get(self, endpoint):
        """Execute an http get request

        :param endpoint: Ansible Runner service RESTful API endpoint

        :returns: A requests object
        """

        the_url = "%s/%s" % (self.server_url, endpoint)

        response = requests.get(the_url,
                                verify=self.verify_server,
                                cert=self.client_cert,
                                headers={})

        if response.status_code != requests.codes.ok:
            logger.error("http GET %s <--> (%s - %s)\n%s",
                           the_url, response.status_code, response.reason,
                           response.text)
        else:
            logger.info("http GET %s <--> (%s - %s)",
                          the_url, response.status_code, response.text)

        return response

    @handle_requests_exceptions
    def http_post(self, endpoint, payload, params_dict):
        """Execute an http post request

        :param endpoint: Ansible Runner service RESTful API endpoint
        :param payload: Dictionary with the data used in the post request
        :param params_dict: A dict used to build a query string

        :returns: A requests object
        """

        the_url = "%s/%s" % (self.server_url, endpoint)
        response = requests.post(the_url,
                                 verify=self.verify_server,
                                 cert=self.client_cert,
                                 headers={"Content-type": "application/json"},
                                 json=payload,
                                 params=params_dict)

        if response.status_code != requests.codes.ok:
            logger.error("http POST %s [%s] <--> (%s - %s:%s)\n",
                           the_url, payload, response.status_code,
                           response.reason, response.text)
        else:
            logger.info("http POST %s <--> (%s - %s)",
                          the_url, response.status_code, response.text)

        return response

    @handle_requests_exceptions
    def http_delete(self, endpoint):
        """Execute an http delete request

        :param endpoint: Ansible Runner service RESTful API endpoint

        :returns: A requests object
        """

        the_url = "%s/%s" % (self.server_url, endpoint)
        response = requests.delete(the_url,
                                   verify=self.verify_server,
                                   cert=self.client_cert,
                                   headers={})

        if response.status_code != requests.codes.ok:
            logger.error("http DELETE %s <--> (%s - %s)\n%s",
                           the_url, response.status_code, response.reason,
                           response.text)
        else:
            logger.info("http DELETE %s <--> (%s - %s)",
                          the_url, response.status_code, response.text)

        return response

    def http_put(self, endpoint, payload):
        """Execute an http put request

        :param endpoint: Ansible Runner service RESTful API endpoint
        :param payload: Dictionary with the data used in the put request

        :returns: A requests object
        """
        # TODO
        raise NotImplementedError("TODO")


    def add_hosts_to_group(self, hosts, group):
        """ Add one or more hosts to an Ansible inventory group

        :host : host to add
        :group: Ansible inventory group where the hosts will be included

        :return : Nothing

        :raises : AnsibleRunnerServiceError if not possible to complete
                  the operation
        """

        url_group = URL_MANAGE_GROUP.format(group_name=group)

        # Get/Create the group
        response = self.http_get(url_group)
        if response.status_code == 404:
            # Create the new group
            response = self.http_post(url_group, "", {})
            if response.status_code != 200:
                raise AnsibleRunnerServiceError("Error when trying to "\
                                                "create group:{}".format(group))
            hosts_in_group = []  # type: List[str]
        else:
            hosts_in_group = json.loads(response.text)["data"]["members"]

        # Here we have the group in the inventory. Add the hosts
        for host in hosts:
            if host not in hosts_in_group:
                add_url = URL_ADD_RM_HOSTS.format(host_name=host,
                                                  inventory_group=group)

                response = self.http_post(add_url, "", {})
                if response.status_code != 200:
                    raise AnsibleRunnerServiceError("Error when trying to "\
                                                    "include host '{}' in group"\
                                                    " '{}'".format(host, group))

    def remove_hosts_from_group(self, group, hosts):
        """ Remove all the hosts from group, it also removes the group itself if
        it is empty

        : group : Group name (str)
        : hosts : List of hosts to remove
        """

        url_group = URL_MANAGE_GROUP.format(group_name=group)
        response = self.http_get(url_group)

        # Group not found is OK!
        if response.status_code == 404:
            return

        # Once we have the group, we remove the hosts required
        if response.status_code == 200:
            hosts_in_group = json.loads(response.text)["data"]["members"]

            # Delete the hosts (it does not matter if the host does not exist)
            for host in hosts:
                if host in hosts_in_group:
                    url_host = URL_ADD_RM_HOSTS.format(host_name=host,
                                                       inventory_group=group)
                    response = self.http_delete(url_host)
                    hosts_in_group.remove(host)

            # Delete the group if no hosts in it
            if not hosts_in_group:
                response = self.http_delete(url_group)

    def get_hosts_in_group(self, group):
        """ Return the list of hosts in and inventory group

        : group : Group name (str)
        """
        url_group = URL_MANAGE_GROUP.format(group_name=group)
        response = self.http_get(url_group)
        if response.status_code == 404:
            raise AnsibleRunnerServiceError("Group {} not found in Ansible"\
                                            " inventory".format(group))

        return json.loads(response.text)["data"]["members"]


class InventoryGroup(collections.MutableSet):
    """ Manages an Ansible Inventory Group
    """
    def __init__(self, group_name, ars_client):
        # type: (str, Client) -> None
        """Init the group_name attribute and
           Create the inventory group if it does not exist

        : group_name : Name of the group in the Ansible Inventory
        : returns    : Nothing
        """

        self.elements = set()  # type: Set[Any]

        self.group_name = group_name
        self.url_group = URL_MANAGE_GROUP.format(group_name=self.group_name)
        self.created = False
        self.ars_client = ars_client

        # Get/Create the group
        response = self.ars_client.http_get(self.url_group)
        if response.status_code == 404:
            return

        # get members if the group exists previously
        self.created = True
        self.elements.update(json.loads(response.text)["data"]["members"])

    def __contains__(self, host):
        """ Check if the host is in the group

        : host: Check if hosts is in Ansible Inventory Group
        """
        return host in  self.elements

    def __iter__(self):
        return iter(self.elements)

    def __len__(self):
        return len(self.elements)

    def add(self, value):
        """ Add a new host to the group
        Create the Ansible Inventory group if it does not exist

        : value : The host(string) to add
        """

        if not self.created:
            self.__create_group__()

        add_url = URL_ADD_RM_HOSTS.format(host_name=value,
                                          inventory_group=self.group_name)

        response = self.ars_client.http_post(add_url, "", {})
        if response.status_code != 200:
            raise AnsibleRunnerServiceError("Error when trying to "\
                                            "include host '{}' in group"\
                                            " '{}'".format(value,
                                                           self.group_name))

        # Refresh members
        response = self.ars_client.http_get(self.url_group)
        self.elements.update(json.loads(response.text)["data"]["members"])

    def discard(self, value):
        """Remove a host from the group.
        Remove the group from the Ansible inventory if it is empty

        : value : The host(string) to remove
        """
        url_host = URL_ADD_RM_HOSTS.format(host_name=value,
                                           inventory_group=self.group_name)
        response = self.ars_client.http_delete(url_host)

        # Refresh members
        response = self.ars_client.http_get(self.url_group)
        self.elements.update(json.loads(response.text)["data"]["members"])

        # Delete the group if no members
        if not self.elements:
            response = self.ars_client.http_delete(self.url_group)

    def update(self, iterable=None):
        """ Update the hosts in the group with the iterable items

        :iterable : And iterable object with hosts names
        """
        for item in iterable:
            self.add(item)

    def clean(self, iterable=None):
        """ Remove from the group the hosts included in iterable
        If not provided an iterable, all the hosts are removed from the group

        :iterable : And iterable object with hosts names
        """

        if not iterable:
            iterable = self.elements

        for item in iterable:
            self.discard(item)

    def __create_group__(self):
        """ Create the Ansible inventory group
        """
        response = self.ars_client.http_post(self.url_group, "", {})

        if response.status_code != 200:
            raise AnsibleRunnerServiceError("Error when trying to "\
                                            "create group:{}".format(
                                                self.group_name))
        self.created = True
        self.elements = set()
