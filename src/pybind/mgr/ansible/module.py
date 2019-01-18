"""
ceph-mgr Ansible orchestrator module

The external Orchestrator is the Ansible runner service (RESTful https service)
"""

import json

from mgr_module import MgrModule
import orchestrator

from .ansible_runner_svc import Client, PlayBookExecution, ExecutionStatusCode,\
                               EVENT_DATA_URL

# Time to clean the completions list
WAIT_PERIOD = 10


# List of playbooks names used

# Name of the playbook used in the "get_inventory" method.
# This playbook is expected to provide a list of storage devices in the host
# where the playbook is executed.
GET_STORAGE_DEVICES_CATALOG_PLAYBOOK = "host-disks.yml"



class AnsibleReadOperation(orchestrator.ReadCompletion):
    """ A read operation means to obtain information from the cluster.
    """

    def __init__(self, client, playbook, logger, result_pattern, params):
        super(AnsibleReadOperation, self).__init__()


        # Private attributes
        self.playbook = playbook
        self._is_complete = False
        self._is_errored = False
        self._result = []

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
                                              params)

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
        current_status = self.pb_execution.get_status()

        self._is_complete = (current_status == ExecutionStatusCode.SUCCESS) or \
                            (current_status == ExecutionStatusCode.ERROR)

        self._is_errored = (current_status == ExecutionStatusCode.ERROR)

        return current_status

    def execute_playbook(self):
        """Execute the playbook with the provided params.
        """

        self.pb_execution.launch()

    def update_result(self):
        """Output of the read operation

        The result of the playbook execution can be customized through the
        function provided as 'process_output' attribute

        @return string: Result of the operation formatted if it is possible
        """

        processed_result = []

        if self._is_complete:
            raw_result = self.pb_execution.get_result(self.event_filter)

            if self.process_output:
                processed_result = self.process_output(
                                            raw_result,
                                            self.ar_client,
                                            self.pb_execution.play_uuid)
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

        self.error = False
    @property
    def status(self):
        """Return the status code of the operation
        """
        #TODO
        return 0

    @property
    def is_persistent(self):
        """
        Has the operation updated the orchestrator's configuration
        persistently?  Typically this would indicate that an update
        had been written to a manifest, but that the update
        had not necessarily been pushed out to the cluster.

        In the case of Ansible is always False.
        because a initiated playbook execution will need always to be
        relaunched if it fails.
        """

        return False

    @property
    def is_effective(self):
        """Has the operation taken effect on the cluster?
        For example, if we were adding a service, has it come up and appeared
        in Ceph's cluster maps?

        In the case of Ansible, this will be True if the playbooks has been
        executed succesfully.

        @return Boolean: if the playbook has been executed succesfully
        """

        return self.status == ExecutionStatusCode.SUCCESS

    @property
    def is_errored(self):
        return self.error

    @property
    def is_complete(self):
        return self.is_errored or (self.is_persistent and self.is_effective)


class Module(MgrModule, orchestrator.Orchestrator):
    """An Orchestrator that an external Ansible runner service to perform
    operations
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

           @param completions: list of Completion instances
           @Returns          : true if everything is done.
        """

        # Check progress and update status in each operation
        for operation in completions:
            self.log.info("playbook <%s> status:%s", operation.playbook, operation.status)
            if operation.is_complete:
                operation.update_result()

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
        except Exception:
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

    def get_inventory(self, node_filter=None):
        """

        @param   :	node_filter instance
        @Return  :	A AnsibleReadOperation instance (Completion Object)
        """

        # Create a new read completion object for execute the playbook
        ansible_operation = AnsibleReadOperation(client = self.ar_client,
                                                 playbook = GET_STORAGE_DEVICES_CATALOG_PLAYBOOK,
                                                 logger = self.log,
                                                 result_pattern = "RESULTS",
                                                 params = "{}")

        # Assign the process_output function
        ansible_operation.process_output = process_inventory_json
        ansible_operation.event_filter = "runner_on_ok"

        # Execute the playbook to obtain data
        ansible_operation.execute_playbook()

        self.all_completions.append(ansible_operation)

        return ansible_operation

    def create_osds(self, drive_group, all_hosts):
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).

        :param osd_spec: DriveGroupSpec
        """

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

def process_inventory_json(inventory_events, ar_client, playbook_uuid):
    """ Adapt the output of the playbook used in 'get_inventory'
        to the Orchestrator expected output (list of InventoryNode)

    @param inventory_events: events dict with the results

        Example:
        inventory_events =
        {'37-100564f1-9fed-48c2-bd62-4ae8636dfcdb': {'host': '192.168.121.254',
                                                    'task': 'RESULTS',
                                                    'event': 'runner_on_ok'},
        '36-2016b900-e38f-7dcd-a2e7-00000000000e': {'host': '192.168.121.252'
                                                    'task': 'RESULTS',
                                                    'event': 'runner_on_ok'}}
    @param ar_client: Ansible Runner Service client
    @param playbook_uuid: Palybooud identifier

    @return              : list of InventoryNode
    """

    #Obtain the needed data for each result event
    inventory_nodes = []

    # Loop over the result events and request the event data
    for event_key, data in inventory_events.items():
        event_response = ar_client.http_get(EVENT_DATA_URL % (playbook_uuid,
                                                              event_key))

        # Process the data for each event
        if event_response:
            event_data = json.loads(event_response.text)["data"]["event_data"]

            free_disks = event_data["res"]["disks_catalog"]
            for item, data in free_disks.items():
                if item not in [host.name for host in inventory_nodes]:

                    devs = []
                    for dev_key, dev_data in data.items():
                        if dev_key not in [device.id for device in devs]:
                            dev = orchestrator.InventoryDevice()
                            dev.id = dev_key
                            dev.type = 'hdd' if dev_data["rotational"] else "sdd/nvme"
                            dev.size = dev_data["sectorsize"] * dev_data["sectors"]
                            devs.append(dev)

                    inventory_nodes.append(
                            orchestrator.InventoryNode(item, devs))


    return inventory_nodes
