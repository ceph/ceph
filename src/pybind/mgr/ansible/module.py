"""
ceph-mgr Ansible orchestrator module

The external Orchestrator is the Ansible runner service (RESTful https service)
"""

# pylint: disable=abstract-method, no-member, bad-continuation

import json
import requests

from mgr_module import MgrModule
import orchestrator

from .ansible_runner_svc import Client, PlayBookExecution, ExecutionStatusCode,\
                                AnsibleRunnerServiceError

from .output_wizards import ProcessInventory, ProcessPlaybookResult, \
                            ProcessHostsList

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
# URLs for Ansible Runner Operations
URL_GET_HOSTS = "api/v1/hosts"


class AnsibleReadOperation(orchestrator.ReadCompletion):
    """ A read operation means to obtain information from the cluster.
    """
    def __init__(self, client, logger):
        """
        :param client        : Ansible Runner Service Client
        :param logger        : The object used to log messages
        """
        super(AnsibleReadOperation, self).__init__()

        # Private attributes
        self._is_complete = False
        self._is_errored = False
        self._result = []
        self._status = ExecutionStatusCode.NOT_LAUNCHED

        # Object used to process operation result in different ways
        self.output_wizard = None

        # Error description in operation
        self.error = ""

        # Ansible Runner Service client
        self.ar_client = client

        # Logger
        self.log = logger

        # OutputWizard object used to process the result
        self.output_wizard = None

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
        """Retrieve the current status of the operation and update state
        attributes
        """
        raise NotImplementedError()

class ARSOperation(AnsibleReadOperation):
    """Execute an Ansible Runner Service Operation
    """

    def __init__(self, client, logger, url, get_operation=True, payload=None):
        """
        :param client        : Ansible Runner Service Client
        :param logger        : The object used to log messages
        :param url           : The Ansible Runner Service URL that provides
                               the operation
        :param get_operation : True if operation is provided using an http GET
        :param payload       : http request payload
        """
        super(ARSOperation, self).__init__(client, logger)

        self.url = url
        self.get_operation = get_operation
        self.payload = payload

    def __str__(self):
        return "Ansible Runner Service: {operation} {url}".format(
                    operation="GET" if self.get_operation else "POST",
                    url=self.url)

    @property
    def status(self):
        """ Execute the Ansible Runner Service operation and update the status
        and result of the underlying Completion object.
        """

        # Execute the right kind of http request
        if self.get_operation:
            response = self.ar_client.http_get(self.url)
        else:
            response = self.ar_client.http_post(self.url, self.payload)

        # If no connection errors, the operation is complete
        self._is_complete = True

        # Depending of the response, status and result is updated
        if not response:
            self._is_errored = True
            self._status = ExecutionStatusCode.ERROR
            self._result = "Ansible Runner Service not Available"
        else:
            self._is_errored = (response.status_code != requests.codes.ok)

            if not self._is_errored:
                self._status = ExecutionStatusCode.SUCCESS
                if self.output_wizard:
                    self._result = self.output_wizard.process(self.url,
                                                              response.text)
                else:
                    self._result = response.text
            else:
                self._status = ExecutionStatusCode.ERROR
                self._result = response.reason

        return self._status


class PlaybookOperation(AnsibleReadOperation):
    """Execute a playbook using the Ansible Runner Service
    """

    def __init__(self, client, playbook, logger, result_pattern,
                 params,
                 querystr_dict={}):
        """
        :param client        : Ansible Runner Service Client
        :param playbook      : The playbook to execute
        :param logger        : The object used to log messages
        :param result_pattern: The "pattern" to discover what execution events
                               have the information deemed as result
        :param params        : http request payload for the playbook execution
        :param querystr_dict : http request querystring for the playbook
                               execution (DO NOT MODIFY HERE)

        """
        super(PlaybookOperation, self).__init__(client, logger)

       # Private attributes
        self.playbook = playbook

        # An aditional filter of result events based in the event
        self.event_filter = ""

        # Playbook execution object
        self.pb_execution = PlayBookExecution(client,
                                              playbook,
                                              logger,
                                              result_pattern,
                                              params,
                                              querystr_dict)

    def __str__(self):
        return "Playbook {playbook_name}".format(playbook_name=self.playbook)

    @property
    def status(self):
        """Check the status of the playbook execution and update the status
        and result of the underlying Completion object.
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

            if self.output_wizard:
                processed_result = self.output_wizard.process(self.pb_execution.play_uuid,
                                                              raw_result)
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

        # Object used to process operation result in different ways
        self.output_wizard = None

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
    """A class to ease the management of http operations
    """

    def __init__(self, url, http_operation, payload="", query_string="{}"):
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

    def __str__(self):
        # Use the last operation as the main
        return "Ansible Runner Service: {operation} {url}".format(
                                operation=self.operations[-1].http_operation,
                                url=self.operations[-1].url)

    @property
    def status(self):
        """Execute the Ansible Runner Service operations and update the status
        and result of the underlying Completion object.
        """

        for my_request in self.operations:
            # Execute the right kind of http request
            try:
                if my_request.http_operation == "post":
                    response = self.ar_client.http_post(my_request.url,
                                                        my_request.payload,
                                                        my_request.query_string)
                elif my_request.http_operation == "delete":
                    response = self.ar_client.http_delete(my_request.url)
                elif my_request.http_operation == "get":
                    response = self.ar_client.http_get(my_request.url)

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
        if self.output_wizard:
            self._result = self.output_wizard.process("", response.text)
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
            self.ar_client = Client(server_url=self.get_module_option('server_url', ''),
                                    user=self.get_module_option('username', ''),
                                    password=self.get_module_option('password', ''),
                                    verify_server=self.get_module_option('verify_server', True),
                                    logger=self.log)
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

        :param   :  node_filter instance
        :param   :  refresh any cached state
        :Return  :  A AnsibleReadOperation instance (Completion Object)
        """

        # Create a new read completion object for execute the playbook
        playbook_operation = PlaybookOperation(client=self.ar_client,
                                               playbook=GET_STORAGE_DEVICES_CATALOG_PLAYBOOK,
                                               logger=self.log,
                                               result_pattern="list storage inventory",
                                               params={})


        # Assign the process_output function
        playbook_operation.output_wizard = ProcessInventory(self.ar_client,
                                                            self.log)
        playbook_operation.event_filter = "runner_on_ok"

        # Execute the playbook to obtain data
        self._launch_operation(playbook_operation)

        return playbook_operation

    def create_osds(self, drive_group, all_hosts):
        """Create one or more OSDs within a single Drive Group.
        If no host provided the operation affects all the host in the OSDS role


        :param drive_group: (orchestrator.DriveGroupSpec),
                            Drive group with the specification of drives to use
        :param all_hosts  : (List[str]),
                            List of hosts where the OSD's must be created
        """

        # Transform drive group specification to Ansible playbook parameters
        host, osd_spec = dg_2_ansible(drive_group)

        # Create a new read completion object for execute the playbook
        playbook_operation = PlaybookOperation(client=self.ar_client,
                                               playbook=ADD_OSD_PLAYBOOK,
                                               logger=self.log,
                                               result_pattern="",
                                               params=osd_spec,
                                               querystr_dict={"limit": host})

        # Filter to get the result
        playbook_operation.output_wizard = ProcessPlaybookResult(self.ar_client,
                                                                  self.log)
        playbook_operation.event_filter = "playbook_on_stats"

        # Execute the playbook
        self._launch_operation(playbook_operation)

        return playbook_operation

    def remove_osds(self, osd_ids):
        """Remove osd's.

        :param osd_ids: List of osd's to be removed (List[int])
        """

        extravars = {'osd_to_kill': ",".join([str(osd_id) for osd_id in osd_ids]),
                     'ireallymeanit':'yes'}

        # Create a new read completion object for execute the playbook
        playbook_operation = PlaybookOperation(client=self.ar_client,
                                               playbook=REMOVE_OSD_PLAYBOOK,
                                               logger=self.log,
                                               result_pattern="",
                                               params=extravars)

        # Filter to get the result
        playbook_operation.output_wizard = ProcessPlaybookResult(self.ar_client,
                                                                 self.log)
        playbook_operation.event_filter = "playbook_on_stats"

        # Execute the playbook
        self._launch_operation(playbook_operation)

        return playbook_operation

    def get_hosts(self):
        """Provides a list Inventory nodes
        """

        host_ls_op = ARSOperation(self.ar_client, self.log, URL_GET_HOSTS)

        host_ls_op.output_wizard = ProcessHostsList(self.ar_client,
                                                    self.log)

        return host_ls_op

    def add_host(self, host):
        """
        Add a host to the Ansible Runner Service inventory in the "orchestrator"
        group

        :param host: hostname
        :returns : orchestrator.WriteCompletion
        """

        url_group = URL_MANAGE_GROUP.format(group_name=ORCHESTRATOR_GROUP)

        try:
            # Create the orchestrator default group if not exist.
            # If exists we ignore the error response
            dummy_response = self.ar_client.http_post(url_group, "", {})

            # Here, the default group exists so...
            # Prepare the operation for adding the new host
            add_url = URL_ADD_RM_HOSTS.format(host_name=host,
                                              inventory_group=ORCHESTRATOR_GROUP)

            operations = [HttpOperation(add_url, "post")]

        except AnsibleRunnerServiceError as ex:
            # Problems with the external orchestrator.
            # Prepare the operation to return the error in a Completion object.
            self.log.exception("Error checking <orchestrator> group: %s", ex)
            operations = [HttpOperation(url_group, "post")]

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
            groups_url = URL_GET_HOST_GROUPS.format(host_name=host)
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
                                                    host_name=host,
                                                    inventory_group=x),
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
        """ Verify configuration options for the Ansible orchestrator module
        """
        client_msg = ""

        if not self.get_module_option('server_url', ''):
            msg = "No Ansible Runner Service base URL <server_name>:<port>." \
            "Try 'ceph config set mgr mgr/{0}/server_url " \
            "<server name/ip>:<port>'".format(self.module_name)
            self.log.error(msg)
            client_msg += msg

        if not self.get_module_option('username', ''):
            msg = "No Ansible Runner Service user. " \
            "Try 'ceph config set mgr mgr/{0}/username " \
            "<string value>'".format(self.module_name)
            self.log.error(msg)
            client_msg += msg

        if not self.get_module_option('password', ''):
            msg = "No Ansible Runner Service User password. " \
            "Try 'ceph config set mgr mgr/{0}/password " \
            "<string value>'".format(self.module_name)
            self.log.error(msg)
            client_msg += msg

        if not self.get_module_option('verify_server', ''):
            msg = "TLS server identity verification is enabled by default." \
            "Use 'ceph config set mgr mgr/{0}/verify_server False' " \
            "to disable it. Use 'ceph config set mgr mgr/{0}/verify_server " \
            "<path>' to point the CA bundle path used for " \
            "verification".format(self.module_name)
            self.log.error(msg)
            client_msg += msg

        if client_msg:
            # Raise error
            # TODO: Use OrchestratorValidationError
            raise Exception(client_msg)



# Auxiliary functions
#==============================================================================
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
