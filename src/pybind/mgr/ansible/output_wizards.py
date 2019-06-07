"""
ceph-mgr Output Wizards module

Output wizards are used to process results in different ways in
completion objects
"""

# pylint: disable=bad-continuation

import json


from orchestrator import InventoryDevice, InventoryNode

from .ansible_runner_svc import EVENT_DATA_URL

class OutputWizard(object):
    """Base class for help to process output in completion objects
    """
    def __init__(self, ar_client, logger):
        """Make easy to work in output wizards using this attributes:

        :param ars_client: Ansible Runner Service client
        :param logger: log object
        """
        self.ar_client = ar_client
        self.log = logger

    def process(self, operation_id, raw_result):
        """Make the magic here

        :param operation_id: Allows to identify the Ansible Runner Service
                             operation whose result we wnat to process
        :param raw_result: input for processing
        """
        raise NotImplementedError

class ProcessInventory(OutputWizard):
    """ Adapt the output of the playbook used in 'get_inventory'
        to the Orchestrator expected output (list of InventoryNode)
    """

    def process(self, operation_id, raw_result):
        """
        :param operation_id: Playbook uuid
        :param raw_result: events dict with the results

            Example:
            inventory_events =
            {'37-100564f1-9fed-48c2-bd62-4ae8636dfcdb': {'host': '192.168.121.254',
                                                        'task': 'list storage inventory',
                                                        'event': 'runner_on_ok'},
            '36-2016b900-e38f-7dcd-a2e7-00000000000e': {'host': '192.168.121.252'
                                                        'task': 'list storage inventory',
                                                        'event': 'runner_on_ok'}}

        :return              : list of InventoryNode
        """
        # Just making more readable the method
        inventory_events = raw_result

        #Obtain the needed data for each result event
        inventory_nodes = []

        # Loop over the result events and request the event data
        for event_key, dummy_data in inventory_events.items():

            event_response = self.ar_client.http_get(EVENT_DATA_URL %
                            (operation_id, event_key))

            # self.pb_execution.play_uuid

            # Process the data for each event
            if event_response:
                event_data = json.loads(event_response.text)["data"]["event_data"]

                host = event_data["host"]

                devices = json.loads(event_data["res"]["stdout"])
                devs = []
                for storage_device in devices:
                    dev = InventoryDevice.from_ceph_volume_inventory(storage_device)
                    devs.append(dev)

                inventory_nodes.append(InventoryNode(host, devs))


        return inventory_nodes

class ProcessPlaybookResult(OutputWizard):
    """ Provides the result of a playbook execution as plain text
    """
    def process(self, operation_id, raw_result):
        """
        :param operation_id: Playbook uuid
        :param raw_result: events dict with the results

        :return              : String with the playbook execution event list
        """
        # Just making more readable the method
        inventory_events = raw_result

        result = ""

        # Loop over the result events and request the data
        for event_key, dummy_data in inventory_events.items():
            event_response = self.ar_client.http_get(EVENT_DATA_URL %
                            (operation_id, event_key))

            result += event_response.text

        return result


class ProcessHostsList(OutputWizard):
    """ Format the output of host ls call
    """
    def process(self, operation_id, raw_result):
        """ Format the output of host ls call

        :param operation_id: Not used in this output wizard
        :param raw_result: In this case is like the following json:
                {
                "status": "OK",
                "msg": "",
                "data": {
                    "hosts": [
                        "host_a",
                        "host_b",
                        ...
                        "host_x",
                        ]
                    }
                }

        :return: list of InventoryNodes
        """
        # Just making more readable the method
        host_ls_json = raw_result

        inventory_nodes = []

        try:
            json_resp = json.loads(host_ls_json)

            for host in json_resp["data"]["hosts"]:
                inventory_nodes.append(InventoryNode(host, []))

        except ValueError:
            self.log.exception("Malformed json response")
        except KeyError:
            self.log.exception("Unexpected content in Ansible Runner Service"
                               " response")
        except TypeError:
            self.log.exception("Hosts data must be iterable in Ansible Runner "
                               "Service response")

        return inventory_nodes
