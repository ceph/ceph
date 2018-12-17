import logging
import unittest
import mock
import json

import requests_mock

from requests.exceptions import ConnectionError

from ..ansible_runner_svc import Client, PlayBookExecution, ExecutionStatusCode, \
                                LOGIN_URL, API_URL, PLAYBOOK_EXEC_URL, \
                                PLAYBOOK_EVENTS


SERVER_URL = "ars:5001"
USER = "admin"
PASSWORD = "admin"
CERTIFICATE = ""

# Playbook attributes
PB_NAME = "test_playbook"
PB_UUID = "1733c3ac"

# Playbook execution data file
PB_EVENTS_FILE = "./tests/pb_execution_events.data"

# create console handler and set level to info
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def mock_login(mock_server):

    the_login_url = "https://%s/%s" % (SERVER_URL,LOGIN_URL)

    mock_server.register_uri("GET",
                            the_login_url,
                            json={"status": "OK",
                            "msg": "Token returned",
                            "data": {"token": "dummy_token"}},
                            status_code=200)

    the_api_url = "https://%s/%s" % (SERVER_URL,API_URL)
    mock_server.register_uri("GET",
                    the_api_url,
                    text="<!DOCTYPE html>api</html>",
                    status_code=200)

def mock_get_pb(mock_server, playbook_name, return_code):

    mock_login(mock_server)

    ars_client = Client(SERVER_URL, USER, PASSWORD,
                        CERTIFICATE, logger)

    the_pb_url = "https://%s/%s/%s" % (SERVER_URL, PLAYBOOK_EXEC_URL, playbook_name)

    if return_code == 404:
        mock_server.register_uri("POST",
                        the_pb_url,
                        json={ "status": "NOTFOUND",
                               "msg": "playbook file not found",
                               "data": {}},
                        status_code=return_code)
    elif return_code == 202:
        mock_server.register_uri("POST",
                        the_pb_url,
                        json={ "status": "STARTED",
                               "msg": "starting",
                               "data": { "play_uuid": "1733c3ac" }},
                        status_code=return_code)

    return PlayBookExecution(ars_client, playbook_name, logger,
                             result_pattern = "RESULTS")

class  ARSclientTest(unittest.TestCase):

    def test_server_not_reachable(self):

        with self.assertRaises(ConnectionError):
            ars_client = Client(SERVER_URL, USER, PASSWORD,
                                CERTIFICATE, logger)

    def test_server_wrong_USER(self):

        with requests_mock.Mocker() as mock_server:
            the_login_url = "https://%s/%s" % (SERVER_URL,LOGIN_URL)
            mock_server.get(the_login_url,
                            json={"status": "NOAUTH",
                            "msg": "Access denied invalid login: unknown USER",
                            "data": {}},
                            status_code=401)


            ars_client = Client(SERVER_URL, USER, PASSWORD,
                                CERTIFICATE, logger)

            self.assertFalse(ars_client.is_operative(),
                            "Operative attribute expected to be False")

    def test_server_connection_ok(self):

        with requests_mock.Mocker() as mock_server:

            mock_login(mock_server)

            ars_client = Client(SERVER_URL, USER, PASSWORD,
                                CERTIFICATE, logger)

            self.assertTrue(ars_client.is_operative(),
                            "Operative attribute expected to be True")

class PlayBookExecutionTests(unittest.TestCase):


    def test_playbook_execution_ok(self):
        """Check playbook id is set when the playbook is launched
        """
        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            test_pb.launch()

            self.assertEqual(test_pb.play_uuid, PB_UUID,
                             "Found Unexpected playbook uuid")



    def test_playbook_execution_error(self):
        """Check playbook id is not set when the playbook is not present
        """

        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, "unknown_playbook", 404)

            test_pb.launch()

            self.assertEqual(test_pb.play_uuid, "",
                            "Playbook uuid not empty")

    def test_playbook_not_launched(self):
        """Check right status code when Playbook execution has not been launched
        """

        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            # Check playbook not launched
            self.assertEqual(test_pb.get_status(),
                             ExecutionStatusCode.NOT_LAUNCHED,
                             "Wrong status code for playbook not launched")

    def test_playbook_launched(self):
        """Check right status code when Playbook execution has been launched
        """

        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            test_pb.launch()

            the_status_url = "https://%s/%s/%s" % (SERVER_URL,
                                                   PLAYBOOK_EXEC_URL,
                                                   PB_UUID)
            mock_server.register_uri("GET",
                                    the_status_url,
                                    json={"status": "OK",
                                          "msg": "running",
                                          "data": {"task": "Step 2",
                                                   "last_task_num": 6}
                                    },
                                    status_code=200)

            self.assertEqual(test_pb.get_status(),
                             ExecutionStatusCode.ON_GOING,
                             "Wrong status code for a running playbook")

            self.assertEqual(test_pb.play_uuid, PB_UUID,
                             "Unexpected playbook uuid")

    def test_playbook_finish_ok(self):
        """Check right status code when Playbook execution is succesful
        """
        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            test_pb.launch()

            the_status_url = "https://%s/%s/%s" % (SERVER_URL,
                                                   PLAYBOOK_EXEC_URL,
                                                   PB_UUID)
            mock_server.register_uri("GET",
                                    the_status_url,
                                    json={"status": "OK",
                                          "msg": "successful",
                                          "data": {}
                                    },
                                    status_code=200)

            self.assertEqual(test_pb.get_status(),
                             ExecutionStatusCode.SUCCESS,
                             "Wrong status code for a playbook executed succesfully")

    def test_playbook_finish_error(self):
        """Check right status code when Playbook execution has failed
        """
        with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            test_pb.launch()

            the_status_url = "https://%s/%s/%s" % (SERVER_URL,
                                                   PLAYBOOK_EXEC_URL,
                                                   PB_UUID)
            mock_server.register_uri("GET",
                                    the_status_url,
                                    json={"status": "OK",
                                          "msg": "failed",
                                          "data": {}
                                    },
                                    status_code=200)

            self.assertEqual(test_pb.get_status(),
                             ExecutionStatusCode.ERROR,
                             "Wrong status code for a playbook with error")

    def test_playbook_get_result(self):
       """ Find the right result event in a set of different events
       """
       with requests_mock.Mocker() as mock_server:

            test_pb = mock_get_pb(mock_server, PB_NAME, 202)

            test_pb.launch()

            the_events_url = "https://%s/%s" % (SERVER_URL,
                                                PLAYBOOK_EVENTS % PB_UUID)

            # Get the events stored in a file
            pb_events = {}
            with open(PB_EVENTS_FILE) as events_file:
                pb_events = json.loads(events_file.read())

            mock_server.register_uri("GET",
                                    the_events_url,
                                    json=pb_events,
                                    status_code=200)

            result = test_pb.get_result("runner_on_ok")

            self.assertEqual(len(result.keys()), 1,
                            "Unique result event not found")

            self.assertIn("37-100564f1-9fed-48c2-bd62-4ae8636dfcdb",
                          result.keys(),
                          "Predefined result event not found")
