import json
from tasks.ceph_test_case import CephTestCase

class TestCommands(CephTestCase):

  def setUp(self):
    super().setUp()
    mon_stat = json.loads(self.cluster_cmd("mon stat --format=json"))
    self.leader_mon_name = "mon.%s" % mon_stat['leader']

  def validate_config_response(self, key, response):
    # expecting {"<key>": "<value>"}
    # yes, all values are stringified.
    self.assertTrue(isinstance(response, dict))
    self.assertEqual(1, len(response))
    self.assertIn(key, response)
    self.assertTrue(isinstance(response[key], str))

  def test_config_show(self):
    key = "mon_allow_pool_delete"
    cmd = "config show %s %s --format=json" % (self.leader_mon_name, key)
    self.validate_config_response(key, json.loads(self.cluster_cmd(cmd)))

  def test_config_get(self):
    key = "mon_allow_pool_delete"
    cmd = "config get mon %s --format=json" % (key)
    self.validate_config_response(key, json.loads(self.cluster_cmd(cmd)))
