"""
ceph-mgr Ansible orchestrator module

The external Orchestrator is the Ansible runner service (RESTful https service)
"""

# pylint: disable=abstract-method, no-member, bad-continuation
import functools
import json
import os
import errno
import tempfile

try:
    from typing import List, Optional, Callable, Any
except ImportError:
    pass # just for type checking


import requests

from mgr_module import MgrModule, Option, CLIWriteCommand
import orchestrator
from mgr_util import verify_tls_files, ServerConfigException

from .ansible_runner_svc import Client, PlayBookExecution, ExecutionStatusCode,\
                                AnsibleRunnerServiceError, InventoryGroup,\
                                URL_MANAGE_GROUP, URL_ADD_RM_HOSTS

from .output_wizards import ProcessInventory, ProcessPlaybookResult, \
                            ProcessHostsList, OutputWizard



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

# General multi purpose cluster playbook
SITE_PLAYBOOK = "site.yml"

# General multi-purpose playbook for removing daemons
PURGE_PLAYBOOK = "purge-cluster.yml"

# Default name for the inventory group for hosts managed by the Orchestrator
ORCHESTRATOR_GROUP = "orchestrator"

# URLs for Ansible Runner Operations

# Retrieve the groups where the host is included in.
URL_GET_HOST_GROUPS = "api/v1/hosts/{host_name}"



# URLs for Ansible Runner Operations
URL_GET_HOSTS = "api/v1/hosts"



def deferred(f):
    # type: (Callable) -> Callable[..., orchestrator.Completion]
    """
    Decorator to make RookOrchestrator methods return
    a completion object that executes themselves.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return orchestrator.Completion(on_complete=lambda _: f(*args, **kwargs))

    return wrapper


def clean_inventory(ar_client, clean_hosts_on_success):
    # type: (Client, dict) -> None
    """ Remove hosts from inventory groups
    """

    for group, hosts in clean_hosts_on_success.items():
        InventoryGroup(group, ar_client).clean(hosts)


def playbook_operation(client,  # type: Client
                       playbook,  # type: str
                       result_pattern,  # type: str
                       params,  # type: dict
                       event_filter_list=None,  # type: Optional[List[str]]
                       querystr_dict=None,  # type: Optional[dict]
                       output_wizard=None,  # type: Optional[OutputWizard]
                       clean_hosts_on_success=None  # type: Optional[dict]
                       ):
    # type: (...) -> orchestrator.Completion
    """
    :param client        : Ansible Runner Service Client
    :param playbook      : The playbook to execute
    :param result_pattern: The "pattern" to discover what execution events
                           have the information deemed as result
    :param params        : http request payload for the playbook execution
    :param querystr_dict : http request querystring for the playbook
                           execution (DO NOT MODIFY HERE)
    :param event_filter_list: An aditional filter of result events based in the event
    :param clean_hosts_on_success: A dict with groups and hosts to remove from inventory if operation is
        succesful. Ex: {"group1": ["host1"], "group2": ["host3", "host4"]}
    """

    querystr_dict = querystr_dict or {}
    event_filter_list = event_filter_list or [""]
    clean_hosts_on_success = clean_hosts_on_success or {}

    def status(_):
        """Check the status of the playbook execution and update the status
        and result of the underlying Completion object.
        """

        status = pb_execution.get_status()

        if status in (ExecutionStatusCode.SUCCESS, ExecutionStatusCode.ERROR):
            if status == ExecutionStatusCode.ERROR:
                raw_result = pb_execution.get_result(["runner_on_failed",
                                                      "runner_on_unreachable",
                                                      "runner_on_no_hosts",
                                                      "runner_on_async_failed",
                                                      "runner_item_on_failed"])
            else:
                raw_result = pb_execution.get_result(event_filter_list)

            if output_wizard:
                processed_result = output_wizard.process(pb_execution.play_uuid,
                                                         raw_result)
            else:
                processed_result = raw_result

            # Clean hosts if operation is succesful
            if status == ExecutionStatusCode.SUCCESS:
                assert clean_hosts_on_success is not None
                clean_inventory(client, clean_hosts_on_success)

            return processed_result
        else:
            return orchestrator.Completion(on_complete=status)

    pb_execution = PlayBookExecution(client, playbook, result_pattern, params, querystr_dict)

    return orchestrator.Completion(on_complete=lambda _: pb_execution.launch()).then(status)


def ars_http_operation(url, http_operation, payload="", params_dict=None):
    def inner(ar_client):
        # type: (Client) -> str
        if http_operation == "post":
            response = ar_client.http_post(url,
                                           payload,
                                           params_dict)
        elif http_operation == "delete":
            response = ar_client.http_delete(url)
        elif http_operation == "get":
            response = ar_client.http_get(url)
        else:
            assert False, http_operation

        # Any problem executing the secuence of operations will
        # produce an errored completion object.
        try:
            response.raise_for_status()
        except Exception as e:
            raise AnsibleRunnerServiceError(str(e))

        return response.text
    return inner


@deferred
def ars_change(client, operations, output_wizard=None):
    # type: (Client, List[Callable[[Client], str]], Optional[OutputWizard]) -> str
    """
    Execute one or more Ansible Runner Service Operations that implies
    a change in the cluster

    :param client         : Ansible Runner Service Client
    :param operations     : A list of http_operation objects

    Execute the Ansible Runner Service operations and update the status
    and result of the underlying Completion object.
    """

    out = None
    for my_request in operations:
        # Execute the right kind of http request
        out = my_request(client)
        # If this point is reached, all the operations has been succesfuly
        # executed, and the final result is updated
    assert out is not None
    if output_wizard:
        return output_wizard.process("", out)
    else:
        return out


def ars_read(client, url, get_operation=True, payload=None, output_wizard=None):
    # type: (Client, str, bool, Optional[str], Optional[OutputWizard]) -> orchestrator.Completion
    """
    Execute the Ansible Runner Service operation

    :param client        : Ansible Runner Service Client
    :param url           : The Ansible Runner Service URL that provides
                           the operation
    :param get_operation : True if operation is provided using an http GET
    :param payload       : http request payload
    """
    return ars_change(client, [ars_http_operation(url, 'get' if get_operation else 'post', payload)], output_wizard)


class Module(MgrModule, orchestrator.Orchestrator):
    """An Orchestrator that uses <Ansible Runner Service> to perform operations
    """

    MODULE_OPTIONS = [
        # url:port of the Ansible Runner Service
        Option(name="server_location", type="str", default=""),
        # Check server identity (True by default)
        Option(name="verify_server", type="bool", default=True),
        # Path to an alternative CA bundle
        Option(name="ca_bundle", type="str", default="")
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self.run = False

        self.all_completions = []

        self._ar_client = None  # type: Optional[Client]

        # TLS certificate and key file names used to connect with the external
        # Ansible Runner Service
        self.client_cert_fname = ""
        self.client_key_fname = ""

        # used to provide more verbose explanation of errors in status method
        self.status_message = ""

        self.all_progress_references = list()  # type: List[orchestrator.ProgressReference]

    @property
    def ar_client(self):
        # type: () -> Client
        assert self._ar_client is not None
        return self._ar_client

    def available(self):
        """ Check if Ansible Runner service is working
        """
        available = False
        msg = ""
        try:

            if self._ar_client:
                available = self.ar_client.is_operative()
                if not available:
                    msg = "No response from Ansible Runner Service"
            else:
                msg = "Not possible to initialize connection with Ansible "\
                      "Runner service."

        except AnsibleRunnerServiceError as ex:
            available = False
            msg = str(ex)

        # Add more details to the detected problem
        if self.status_message:
            msg = "{}:\n{}".format(msg, self.status_message)

        return (available, msg)

    def process(self, completions):
        """Given a list of Completion instances, progress any which are
           incomplete.

        :param completions: list of Completion instances
        :Returns          : True if everything is done.
        """

        if completions:
            self.log.info("process: completions={0}".format(orchestrator.pretty_print(completions)))

    def serve(self):
        """ Mandatory for standby modules
        """
        self.log.info("Starting Ansible Orchestrator module ...")

        try:
            # Verify config options and client certificates
            self.verify_config()

            # Ansible runner service client
            self._ar_client = Client(
                server_url=self.get_module_option('server_location', ''),
                verify_server=self.get_module_option('verify_server', True),
                ca_bundle=self.get_module_option('ca_bundle', ''),
                client_cert=self.client_cert_fname,
                client_key=self.client_key_fname)

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
        op = playbook_operation(client=self.ar_client,
                                playbook=GET_STORAGE_DEVICES_CATALOG_PLAYBOOK,
                                result_pattern="list storage inventory",
                                params={},
                                output_wizard=ProcessInventory(self.ar_client),
                                event_filter_list=["runner_on_ok"])

        self._launch_operation(op)

        return op

    def create_osds(self, drive_groups):
        """Create one or more OSDs within a single Drive Group.
        If no host provided the operation affects all the host in the OSDS role


        :param drive_groups: (ceph.deployment.drive_group.DriveGroupSpec),
                            Drive group with the specification of drives to use
        """

        # Transform drive group specification to Ansible playbook parameters
        ops = []
        for drive_group in drive_groups:
            host, osd_spec = dg_2_ansible(drive_group)

            # Create a new read completion object for execute the playbook
            op = playbook_operation(client=self.ar_client,
                                    playbook=ADD_OSD_PLAYBOOK,
                                    result_pattern="",
                                    params=osd_spec,
                                    querystr_dict={"limit": host},
                                    output_wizard=ProcessPlaybookResult(self.ar_client),
                                    event_filter_list=["playbook_on_stats"])

            self._launch_operation(op)
            ops.append(op)

        return ops

    def remove_osds(self, osd_ids, destroy=False):
        """Remove osd's.

        :param osd_ids: List of osd's to be removed (List[int])
        :param destroy: unsupported.
        """
        assert not destroy

        extravars = {'osd_to_kill': ",".join([str(osd_id) for osd_id in osd_ids]),
                     'ireallymeanit':'yes'}

        # Create a new read completion object for execute the playbook
        op = playbook_operation(client=self.ar_client,
                                playbook=REMOVE_OSD_PLAYBOOK,
                                result_pattern="",
                                params=extravars,
                                output_wizard=ProcessPlaybookResult(self.ar_client),
                                event_filter_list=["playbook_on_stats"])

        # Execute the playbook
        self._launch_operation(op)

        return op

    def get_hosts(self):
        """Provides a list Inventory nodes
        """

        host_ls_op = ars_read(self.ar_client, url=URL_GET_HOSTS,
                              output_wizard=ProcessHostsList(self.ar_client))
        return host_ls_op

    def add_host(self, host):
        """
        Add a host to the Ansible Runner Service inventory in the "orchestrator"
        group

        :param host: hostname
        :returns : orchestrator.Completion
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

            operations = [ars_http_operation(add_url, "post", "", None)]

        except AnsibleRunnerServiceError as ex:
            # Problems with the external orchestrator.
            # Prepare the operation to return the error in a Completion object.
            self.log.exception("Error checking <orchestrator> group: %s",
                               str(ex))
            operations = [ars_http_operation(url_group, "post", "", None)]

        return ars_change(self.ar_client, operations)

    def remove_host(self, host):
        """
        Remove a host from all the groups in the Ansible Runner Service
        inventory.

        :param host: hostname
        :returns : orchestrator.Completion
        """

        host_groups = []  # type: List[Any]

        try:
            # Get the list of groups where the host is included
            groups_url = URL_GET_HOST_GROUPS.format(host_name=host)
            response = self.ar_client.http_get(groups_url)

            if response.status_code == requests.codes.ok:
                host_groups = json.loads(response.text)["data"]["groups"]

        except AnsibleRunnerServiceError:
            self.log.exception("Error retrieving host groups")
            raise

        if not host_groups:
            # Error retrieving the groups, prepare the completion object to
            # execute the problematic operation just to provide the error
            # to the caller
            operations = [ars_http_operation(groups_url, "get")]
        else:
            # Build the operations list
            operations = list(map(lambda x:
                                  ars_http_operation(URL_ADD_RM_HOSTS.format(
                                      host_name=host,
                                      inventory_group=x),
                                      "delete"),
                                  host_groups))

        return ars_change(self.ar_client, operations)

    def add_rgw(self, spec):
        # type: (orchestrator.RGWSpec) -> orchestrator.Completion
        """ Add a RGW service in the cluster

        : spec        : an Orchestrator.RGWSpec object

        : returns     : Completion object
        """


        # Add the hosts to the inventory in the right group
        hosts = spec.placement.hosts
        if not hosts:
            raise orchestrator.OrchestratorError("No hosts provided. "
                "At least one destination host is needed to install the RGW "
                "service")

        def set_rgwspec_defaults(spec):
            spec.rgw_multisite = spec.rgw_multisite if spec.rgw_multisite is not None else True
            spec.rgw_zonemaster = spec.rgw_zonemaster if spec.rgw_zonemaster is not None else True
            spec.rgw_zonesecondary = spec.rgw_zonesecondary \
                if spec.rgw_zonesecondary is not None else False
            spec.rgw_multisite_proto = spec.rgw_multisite_proto \
                if spec.rgw_multisite_proto is not None else "http"
            spec.rgw_frontend_port = spec.rgw_frontend_port \
                if spec.rgw_frontend_port is not None else 8080

            spec.rgw_zonegroup = spec.rgw_zonegroup if spec.rgw_zonegroup is not None else "default"
            spec.rgw_zone_user = spec.rgw_zone_user if spec.rgw_zone_user is not None else "zone.user"
            spec.rgw_realm = spec.rgw_realm if spec.rgw_realm is not None else "default"

            spec.system_access_key = spec.system_access_key \
                if spec.system_access_key is not None else spec.genkey(20)
            spec.system_secret_key = spec.system_secret_key \
                if spec.system_secret_key is not None else spec.genkey(40)

        set_rgwspec_defaults(spec)
        InventoryGroup("rgws", self.ar_client).update(hosts)

        # Limit playbook execution to certain hosts
        limited = ",".join(str(host) for host in hosts)

        # Add the settings for this service
        extravars = {k:v for (k,v) in spec.__dict__.items() if k.startswith('rgw_')}
        extravars['rgw_zone'] = spec.name
        extravars['rgw_multisite_endpoint_addr'] = spec.rgw_multisite_endpoint_addr
        extravars['rgw_multisite_endpoints_list'] = spec.rgw_multisite_endpoints_list
        extravars['rgw_frontend_port'] = str(spec.rgw_frontend_port)

        # Group hosts by resource (used in rm ops)
        resource_group = "rgw_zone_{}".format(spec.name)
        InventoryGroup(resource_group, self.ar_client).update(hosts)

        # Execute the playbook to create the service
        op = playbook_operation(client=self.ar_client,
                                playbook=SITE_PLAYBOOK,
                                result_pattern="",
                                params=extravars,
                                querystr_dict={"limit": limited},
                                output_wizard=ProcessPlaybookResult(self.ar_client),
                                event_filter_list=["playbook_on_stats"])

        # Execute the playbook
        self._launch_operation(op)

        return op

    def remove_rgw(self, zone):
        """ Remove a RGW service providing <zone>

        :param zone: <zone name> of the RGW
                            ...
        :returns    : Completion object
        """


        # Ansible Inventory group for the kind of service
        group = "rgws"

        # get the list of hosts where to remove the service
        # (hosts in resource group)
        resource_group = "rgw_zone_{}".format(zone)

        hosts_list = list(InventoryGroup(resource_group, self.ar_client))
        limited = ",".join(hosts_list)

        # Avoid manual confirmation
        extravars = {"ireallymeanit": "yes"}

        # Cleaning of inventory after a sucessful operation
        clean_inventory = {}
        clean_inventory[resource_group] = hosts_list
        clean_inventory[group] = hosts_list

        # Execute the playbook to remove the service
        op = playbook_operation(client=self.ar_client,
                                               playbook=PURGE_PLAYBOOK,
                                               result_pattern="",
                                               params=extravars,
                                               querystr_dict={"limit": limited},
                                output_wizard=ProcessPlaybookResult(self.ar_client),
                                event_filter_list=["playbook_on_stats"],
                                clean_hosts_on_success=clean_inventory)

        # Execute the playbook
        self._launch_operation(op)

        return op

    def _launch_operation(self, ansible_operation):
        """Launch the operation and add the operation to the completion objects
        ongoing

        :ansible_operation: A read/write ansible operation (completion object)
        """

        # Add the operation to the list of things ongoing
        self.all_completions.append(ansible_operation)

    def verify_config(self):
        """Verify mandatory settings for the module and provide help to
           configure properly the orchestrator
        """

        # Retrieve TLS content to use and check them
        # First try to get certiticate and key content for this manager instance
        # ex: mgr/ansible/mgr0/[crt/key]
        self.log.info("Tying to use configured specific certificate and key"
                      "files for this server")
        the_crt = self.get_store("{}/{}".format(self.get_mgr_id(), "crt"))
        the_key = self.get_store("{}/{}".format(self.get_mgr_id(), "key"))
        if the_crt is None or the_key is None:
            # If not possible... try to get generic certificates and key content
            # ex: mgr/ansible/[crt/key]
            self.log.warning("Specific tls files for this manager not "
                             "configured, trying to use generic files")
            the_crt = self.get_store("crt")
            the_key = self.get_store("key")

        if the_crt is None or the_key is None:
            self.status_message = "No client certificate configured. Please "\
                                  "set Ansible Runner Service client "\
                                  "certificate and key:\n"\
                                  "ceph ansible set-ssl-certificate-"\
                                  "{key,certificate} -i <file>"
            self.log.error(self.status_message)
            return

        # generate certificate temp files
        self.client_cert_fname = generate_temp_file("crt", the_crt)
        self.client_key_fname = generate_temp_file("key", the_key)

        try:
            verify_tls_files(self.client_cert_fname, self.client_key_fname)
        except ServerConfigException as e:
            self.status_message = str(e)

        if self.status_message:
            self.log.error(self.status_message)
            return

        # Check module options
        if not self.get_module_option("server_location", ""):
            self.status_message = "No Ansible Runner Service base URL "\
            "<server_name>:<port>."\
            "Try 'ceph config set mgr mgr/{0}/server_location "\
            "<server name/ip>:<port>'".format(self.module_name)
            self.log.error(self.status_message)
            return


        if self.get_module_option("verify_server", True):
            self.status_message = "TLS server identity verification is enabled"\
            " by default.Use 'ceph config set mgr mgr/{0}/verify_server False'"\
            "to disable it.Use 'ceph config set mgr mgr/{0}/ca_bundle <path>'"\
            "to point an alternative CA bundle path used for TLS server "\
            "verification".format(self.module_name)
            self.log.error(self.status_message)
            return

        # Everything ok
        self.status_message = ""


    #---------------------------------------------------------------------------
    # Ansible Orchestrator self-owned commands
    #---------------------------------------------------------------------------
    @CLIWriteCommand("ansible set-ssl-certificate",
                     "name=mgr_id,type=CephString,req=false")
    def set_tls_certificate(self, mgr_id=None, inbuf=None):
        """Load tls certificate in mon k-v store
        """
        if inbuf is None:
            return -errno.EINVAL, \
                   'Please specify the certificate file with "-i" option', ''
        if mgr_id is not None:
            self.set_store("{}/crt".format(mgr_id), inbuf)
        else:
            self.set_store("crt", inbuf)
        return 0, "SSL certificate updated", ""

    @CLIWriteCommand("ansible set-ssl-certificate-key",
                     "name=mgr_id,type=CephString,req=false")
    def set_tls_certificate_key(self, mgr_id=None, inbuf=None):
        """Load tls certificate key in mon k-v store
        """
        if inbuf is None:
            return -errno.EINVAL, \
                   'Please specify the certificate key file with "-i" option', \
                   ''
        if mgr_id is not None:
            self.set_store("{}/key".format(mgr_id), inbuf)
        else:
            self.set_store("key", inbuf)
        return 0, "SSL certificate key updated", ""

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


def generate_temp_file(key, content):
    """ Generates a temporal file with the content passed as parameter

    :param key    : used to build the temp file name
    :param content: the content that will be dumped to file
    :returns      : the name of the generated file
    """

    fname = ""

    if content is not None:
        fname = "{}/{}.tmp".format(tempfile.gettempdir(), key)
        try:
            if os.path.exists(fname):
                os.remove(fname)
            with open(fname, "w") as text_file:
                text_file.write(content)
        except IOError as ex:
            raise AnsibleRunnerServiceError("Cannot store TLS certificate/key"
                                            " content: {}".format(str(ex)))

    return fname


