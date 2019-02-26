"""
ceph-mgr Ansible orchestrator module

The external Orchestrator is the Ansible runner service (RESTful https service)
"""

import types
import json
import requests


from mgr_module import MgrModule
import orchestrator

from .ansible_runner_svc import Client, PlayBookExecution, ExecutionStatusCode,\
                               EVENT_DATA_URL, AnsibleRunnerServiceError

# Time to clean the completions list
WAIT_PERIOD = 10


# List of playbooks names used

# Name of the playbook used in the "get_inventory" method.
# This playbook is expected to provide a list of storage devices in the host
# where the playbook is executed.
GET_STORAGE_DEVICES_CATALOG_PLAYBOOK = "storage-inventory.yml"

# Used in the create_osd method
ADD_OSD_PLAYBOOK = "add-osd.yml"

# Used in the remove_osds method
REMOVE_OSD_PLAYBOOK = "shrink-osd.yml"

# Default name for the inventory group for hosts managed by the Orchestrator
ORCHESTRATOR_GROUP = "orchestrator"

# URLs for Ansible Runner Operations
# Add or remove host in one group
URL_ADD_RM_HOSTS = "api/v1/hosts/{host_name}/groups/{inventory_group}"

# Retrieve the groups where the host is included in.
URL_GET_HOST_GROUPS = "api/v1/hosts/{host_name}"

# Manage groups
URL_MANAGE_GROUP = "api/v1/groups/{group_name}"

class AnsibleReadOperation(orchestrator.ReadCompletion):
    """ A read operation means to obtain information from the cluster.
    """

    def __init__(self, client, playbook, logger, result_pattern,
                 params,
                 querystr_dict=None):
        super(AnsibleReadOperation, self).__init__()

        # Private attributes
        self.playbook = playbook
        self._is_complete = False
        self._is_errored = False
        self._result = []
        self._status = ExecutionStatusCode.NOT_LAUNCHED

        # Error description in operation
        self.error = ""

        # Ansible Runner Service client
        self.ar_client = client

        # Logger
        self.log = logger

        # An aditional filter of result events based in the event
        self.event_filter = ""

        # Function assigned dinamically to process the result
        self.process_output = None

        # Playbook execution object
        self.pb_execution = PlayBookExecution(client,
                                              playbook,
                                              logger,
                                              result_pattern,
                                              params,
                                              querystr_dict)

    def __str__(self):
         return "Playbook {playbook_name}".format(playbook_name = self.playbook)

    @property
    def is_complete(self):
        return self._is_complete

    @property
    def is_errored(self):
        return self._is_errored

    @property
    def result(self):
        return self._result

    @property
    def status(self):
        """Return the status code of the operation
        updating conceptually 'linked' attributes
        """

        if self._status in [ExecutionStatusCode.ON_GOING,
                            ExecutionStatusCode.NOT_LAUNCHED]:
            self._status = self.pb_execution.get_status()

        self._is_complete = (self._status == ExecutionStatusCode.SUCCESS) or \
                            (self._status == ExecutionStatusCode.ERROR)

        self._is_errored = (self._status == ExecutionStatusCode.ERROR)

        if self._is_complete:
            self.update_result()

        return self._status

    def execute_playbook(self):
        """Launch the execution of the playbook with the parameters configured
        """
        try:
            self.pb_execution.launch()
        except AnsibleRunnerServiceError:
            self._status = ExecutionStatusCode.ERROR
            raise

    def update_result(self):
        """Output of the read operation

        The result of the playbook execution can be customized through the
        function provided as 'process_output' attribute

        :return string: Result of the operation formatted if it is possible
        """

        processed_result = []


        if self._is_complete:
            raw_result = self.pb_execution.get_result(self.event_filter)

            if self.process_output:
                processed_result = self.process_output(
                                            raw_result,
                                            self.ar_client,
                                            self.pb_execution.play_uuid,
                                            self.log)
            else:
                processed_result = raw_result

        self._result = processed_result


class AnsibleChangeOperation(orchestrator.WriteCompletion):
    """Operations that changes the "cluster" state

    Modifications/Changes (writes) are a two-phase thing, firstly execute
    the playbook that is going to change elements in the Ceph Cluster.
    When the playbook finishes execution (independently of the result),
    the modification/change operation has finished.
    """
    def __init__(self):
        super(AnsibleChangeOperation, self).__init__()

        self._status = ExecutionStatusCode.NOT_LAUNCHED
        self._result = None

    @property
    def status(self):
        """Return the status code of the operation
        """
        raise NotImplementedError()

    @property
    def is_persistent(self):
        """
        Has the operation updated the orchestrator's configuration
        persistently?  Typically this would indicate that an update
        had been written to a manifest, but that the update
        had not necessarily been pushed out to the cluster.

        :return Boolean: True if the execution of the Ansible Playbook or the
                         operation over the Ansible Runner Service has finished
        """

        return self._status in [ExecutionStatusCode.SUCCESS,
                                ExecutionStatusCode.ERROR]

    @property
    def is_effective(self):
        """Has the operation taken effect on the cluster?
        For example, if we were adding a service, has it come up and appeared
        in Ceph's cluster maps?

        In the case of Ansible, this will be True if the playbooks has been
        executed succesfully.

        :return Boolean: if the playbook/ARS operation has been executed
                         succesfully
        """

        return self._status == ExecutionStatusCode.SUCCESS

    @property
    def is_errored(self):
        return self._status == ExecutionStatusCode.ERROR

    @property
    def result(self):
        return self._result

class HttpOperation(object):

    def __init__(self, url, http_operation, payload="", query_string="{}"):
        """ A class to ease the management of http operations
        """
        self.url = url
        self.http_operation = http_operation
        self.payload = payload
        self.query_string = query_string
        self.response = None

class ARSChangeOperation(AnsibleChangeOperation):
    """Execute one or more Ansible Runner Service Operations that implies
    a change in the cluster
    """
    def __init__(self, client, logger, operations):
        """
        :param client         : Ansible Runner Service Client
        :param logger         : The object used to log messages
        :param operations     : A list of http_operation objects
        :param payload        : dict with http request payload
        """
        super(ARSChangeOperation, self).__init__()

        assert operations, "At least one operation is needed"
        self.ar_client = client
        self.log = logger
        self.operations = operations

        self.process_output = None

    def __str__(self):
             # Use the last operation as the main
             return "Ansible Runner Service: {operation} {url}".format(
                operation = self.operations[-1].http_operation,
                url = self.operations[-1].url)

    @property
    def status(self):
        """Execute the Ansible Runner Service operations and update the status
        and result of the underlying Completion object.
        """

        for op in self.operations:
            # Execute the right kind of http request
            try:
                if op.http_operation == "post":
                    response = self.ar_client.http_post(op.url, op.payload, op.query_string)
                elif op.http_operation == "delete":
                    response = self.ar_client.http_delete(op.url)
                elif op.http_operation == "get":
                    response = self.ar_client.http_get(op.url)

                # Any problem executing the secuence of operations will
                # produce an errored completion object.
                if response.status_code != requests.codes.ok:
                    self._status = ExecutionStatusCode.ERROR
                    self._result = response.text
                    return self._status

            # Any kind of error communicating with ARS or preventing
            # to have a right http response
            except AnsibleRunnerServiceError as ex:
                self._status = ExecutionStatusCode.ERROR
                self._result = str(ex)
                return self._status

        # If this point is reached, all the operations has been succesfuly
        # executed, and the final result is updated
        self._status = ExecutionStatusCode.SUCCESS
        if self.process_output:
            self._result = self.process_output(response.text)
        else:
            self._result = response.text

        return self._status

class Module(MgrModule, orchestrator.Orchestrator):
    """An Orchestrator that uses <Ansible Runner Service> to perform operations
    """

    MODULE_OPTIONS = [
        {'name': 'server_url'},
        {'name': 'username'},
        {'name': 'password'},
        {'name': 'verify_server'} # Check server identity (Boolean/path to CA bundle)
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self.run = False

        self.all_completions = []

        self.ar_client = None

    def available(self):
        """ Check if Ansible Runner service is working
        """
        # TODO
        return (True, "Everything ready")

    def wait(self, completions):
        """Given a list of Completion instances, progress any which are
           incomplete.

        :param completions: list of Completion instances
        :Returns          : True if everything is done.
        """

        # Check progress and update status in each operation
        # Access completion.status property do the trick
        for operation in completions:
            self.log.info("<%s> status:%s", operation, operation.status)

        completions = filter(lambda x: not x.is_complete, completions)

        ops_pending = len(completions)
        self.log.info("Operations pending: %s", ops_pending)

        return ops_pending == 0

    def serve(self):
        """ Mandatory for standby modules
        """
        self.log.info("Starting Ansible Orchestrator module ...")

        # Verify config options (Just that settings are available)
        self.verify_config()

        # Ansible runner service client
        try:
            self.ar_client = Client(server_url = self.get_module_option('server_url', ''),
                                    user = self.get_module_option('username', ''),
                                    password = self.get_module_option('password', ''),
                                    verify_server = self.get_module_option('verify_server', True),
                                    logger = self.log)
        except AnsibleRunnerServiceError:
            self.log.exception("Ansible Runner Service not available. "
                          "Check external server status/TLS identity or "
                          "connection options. If configuration options changed"
                          " try to disable/enable the module.")
            self.shutdown()
            return

        self.run = True

    def shutdown(self):
        self.log.info('Stopping Ansible orchestrator module')
        self.run = False

    def get_inventory(self, node_filter=None, refresh=False):
        """

        :param   :	node_filter instance
        :param   :      refresh any cached state
        :Return  :	A AnsibleReadOperation instance (Completion Object)
        """

        # Create a new read completion object for execute the playbook
        ansible_operation = AnsibleReadOperation(client = self.ar_client,
                                                 playbook = GET_STORAGE_DEVICES_CATALOG_PLAYBOOK,
                                                 logger = self.log,
                                                 result_pattern = "list storage inventory",
                                                 params = {})

        # Assign the process_output function
        ansible_operation.process_output = process_inventory_json
        ansible_operation.event_filter = "runner_on_ok"

        # Execute the playbook to obtain data
        self._launch_operation(ansible_operation)

        return ansible_operation

    def create_osds(self, drive_group, all_hosts):
        """Create one or more OSDs within a single Drive Group.
        If no host provided the operation affects all the host in the OSDS role


        :param drive_group: (orchestrator.DriveGroupSpec),
                            Drive group with the specification of drives to use
        :param all_hosts: (List[str]),
                           List of hosts where the OSD's must be created
        """

        # Transform drive group specification to Ansible playbook parameters
        host, osd_spec = dg_2_ansible(drive_group)

        # Create a new read completion object for execute the playbook
        ansible_operation = AnsibleReadOperation(client = self.ar_client,
                                                 playbook = ADD_OSD_PLAYBOOK,
                                                 logger = self.log,
                                                 result_pattern = "",
                                                 params = osd_spec,
                                                 querystr_dict = {"limit": host})

        # Filter to get the result
        ansible_operation.process_output = process_playbook_result
        ansible_operation.event_filter = "playbook_on_stats"

        # Execute the playbook
        self._launch_operation(ansible_operation)

        return ansible_operation

    def remove_osds(self, osd_ids):
        """Remove osd's.

        :param osd_ids: List of osd's to be removed (List[int])
        """

        extravars = {'osd_to_kill': ",".join([str(id) for id in osd_ids]),
                     'ireallymeanit':'yes'}

        # Create a new read completion object for execute the playbook
        ansible_operation = AnsibleReadOperation(client = self.ar_client,
                                                 playbook = REMOVE_OSD_PLAYBOOK,
                                                 logger = self.log,
                                                 result_pattern = "",
                                                 params = extravars)

        # Filter to get the result
        ansible_operation.process_output = process_playbook_result
        ansible_operation.event_filter = "playbook_on_stats"

        # Execute the playbook
        self._launch_operation(ansible_operation)

        return ansible_operation

    def add_host(self, host):
        """
        Add a host to the Ansible Runner Service inventory in the "orchestrator"
        group

        :param host: hostname
        :returns : orchestrator.WriteCompletion
        """

        url_group = URL_MANAGE_GROUP.format(group_name = ORCHESTRATOR_GROUP)

        try:
            # Create the orchestrator default group if not exist.
            # If exists we ignore the error response
            dummy_response = self.ar_client.http_post(url_group, "", {})

            # Here, the default group exists so...
            # Prepare the operation for adding the new host
            add_url = URL_ADD_RM_HOSTS.format(host_name = host,
                                              inventory_group = ORCHESTRATOR_GROUP)

            operations =  [HttpOperation(add_url, "post")]

        except AnsibleRunnerServiceError as ex:
            # Problems with the external orchestrator.
            # Prepare the operation to return the error in a Completion object.
            self.log.exception("Error checking <orchestrator> group: %s", ex)
            operations =  [HttpOperation(url_group, "post")]

        return ARSChangeOperation(self.ar_client, self.log, operations)


    def remove_host(self, host):
        """
        Remove a host from all the groups in the Ansible Runner Service
        inventory.

        :param host: hostname
        :returns : orchestrator.WriteCompletion
        """

        operations = []
        host_groups = []

        try:
            # Get the list of groups where the host is included
            groups_url = URL_GET_HOST_GROUPS.format(host_name = host)
            response = self.ar_client.http_get(groups_url)

            if response.status_code == requests.codes.ok:
                host_groups = json.loads(response.text)["data"]["groups"]

        except AnsibleRunnerServiceError:
            self.log.exception("Error retrieving host groups")

        if not host_groups:
            # Error retrieving the groups, prepare the completion object to
            # execute the problematic operation just to provide the error
            # to the caller
            operations = [HttpOperation(groups_url, "get")]
        else:
            # Build the operations list
            operations = list(map(lambda x:
                                  HttpOperation(URL_ADD_RM_HOSTS.format(
                                                    host_name = host,
                                                    inventory_group = x),
                                                "delete"),
                                  host_groups))

        return ARSChangeOperation(self.ar_client, self.log, operations)


    def _launch_operation(self, ansible_operation):
        """Launch the operation and add the operation to the completion objects
        ongoing

        :ansible_operation: A read/write ansible operation (completion object)
        """

        # Execute the playbook
        ansible_operation.execute_playbook()

        # Add the operation to the list of things ongoing
        self.all_completions.append(ansible_operation)

    def verify_config(self):

        if not self.get_module_option('server_url', ''):
            self.log.error(
                "No Ansible Runner Service base URL <server_name>:<port>"
                "Try 'ceph config set mgr mgr/%s/server_url <server name/ip>:<port>'",
                self.module_name)

        if not self.get_module_option('username', ''):
            self.log.error(
                "No Ansible Runner Service user. "
                "Try 'ceph config set mgr mgr/%s/username <string value>'",
                self.module_name)

        if not self.get_module_option('password', ''):
            self.log.error(
                "No Ansible Runner Service User password. "
                "Try 'ceph config set mgr mgr/%s/password <string value>'",
                self.module_name)

        if not self.get_module_option('verify_server', ''):
            self.log.error(
                "TLS server identity verification is enabled by default."
                "Use 'ceph config set mgr mgr/{0}/verify_server False' to disable it."
                "Use 'ceph config set mgr mgr/{0}/verify_server <path>' to "
                "point the CA bundle path used for verification".format(self.module_name))


# Auxiliary functions
#==============================================================================

def process_inventory_json(inventory_events, ar_client, playbook_uuid, logger):
    """ Adapt the output of the playbook used in 'get_inventory'
        to the Orchestrator expected output (list of InventoryNode)

    :param inventory_events: events dict with the results

        Example:
        inventory_events =
        {'37-100564f1-9fed-48c2-bd62-4ae8636dfcdb': {'host': '192.168.121.254',
                                                    'task': 'list storage inventory',
                                                    'event': 'runner_on_ok'},
        '36-2016b900-e38f-7dcd-a2e7-00000000000e': {'host': '192.168.121.252'
                                                    'task': 'list storage inventory',
                                                    'event': 'runner_on_ok'}}
    :param ar_client: Ansible Runner Service client
    :param playbook_uuid: Playbook identifier

    :return              : list of InventoryNode
    """

    #Obtain the needed data for each result event
    inventory_nodes = []

    # Loop over the result events and request the event data
    for event_key, dummy_data in inventory_events.items():

        event_response = ar_client.http_get(EVENT_DATA_URL % (playbook_uuid,
                                                              event_key))

        # Process the data for each event
        if event_response:
            event_data = json.loads(event_response.text)["data"]["event_data"]

            host = event_data["host"]
            devices = json.loads(event_data["res"]["stdout"])

            devs = []
            for storage_device in devices:
                dev = orchestrator.InventoryDevice.from_ceph_volume_inventory(storage_device)
                devs.append(dev)

            inventory_nodes.append(orchestrator.InventoryNode(host, devs))


    return inventory_nodes


def process_playbook_result(inventory_events, ar_client, playbook_uuid):

    result = ""

    # Loop over the result events and request the data
    for event_key, dummy_data in inventory_events.items():
        event_response = ar_client.http_get(EVENT_DATA_URL % (playbook_uuid,
                                                              event_key))

        result += event_response.text

    return result

def dg_2_ansible(drive_group):
    """ Transform a drive group especification into:

        a host     : limit the playbook execution to this host
        a osd_spec : dict of parameters to pass to the Ansible playbook used
                     to create the osds

        :param drive_group: (type: DriveGroupSpec)

        TODO: Possible this function will be removed/or modified heavily when
        the ansible playbook to create osd's use ceph volume batch with
        drive group parameter
    """

    # Limit the execution of the playbook to certain hosts
    # TODO: Now only accepted "*" (all the hosts) or a host_name in the
    # drive_group.host_pattern
    # This attribute is intended to be used with "fnmatch" patterns, so when
    # this become effective it will be needed to use the "get_inventory" method
    # in order to have a list of hosts to be filtered with the "host_pattern"
    if drive_group.host_pattern in ["*"]:
        host = None # No limit in the playbook
    else:
        # For the moment, we assume that we only have 1 host
        host = drive_group.host_pattern

    # Compose the OSD configuration


    osd = {}
    osd["data"] = drive_group.data_devices.paths[0]
    # Other parameters will be extracted in the same way
    #osd["dmcrypt"] = drive_group.encryption

    # lvm_volumes parameters
    # (by the moment is what is accepted in the current playbook)
    osd_spec = {"lvm_volumes":[osd]}

    #Global scope variables also can be included in the osd_spec
    #osd_spec["osd_objectstore"] = drive_group.objectstore

    return host, osd_spec
