# vim: tabstop=4 shiftwidth=4 softtabstop=4

import time
import mock

from ..agent.metrics.ceph_cluster import CephClusterAgent
from ..agent.metrics.ceph_mon_osd import CephMonOsdAgent
from ..agent.metrics.ceph_pool import CephPoolAgent
from ..agent.metrics.db_relay import DBRelayAgent
from ..agent.metrics.sai_agent import SAIAgent
from ..agent.metrics.sai_cluster import SAICluserAgent
from ..agent.metrics.sai_disk import SAIDiskAgent
from ..agent.metrics.sai_disk_smart import SAIDiskSmartAgent
from ..agent.metrics.sai_host import SAIHostAgent
from ..agent.predict.prediction import PredictionAgent
from ..common import DummyResonse

TEMP_RESPONSE = {
    "disk_domain_id": 'abc',
    "near_failure": 'Good',
    "predicted": int(time.time() * (1000 ** 3))}

def generate_sender_mock():
    sender_mock = mock.MagicMock()
    sender = sender_mock
    status_info = dict()
    status_info['measurement'] = None
    status_info['success_count'] = 1
    status_info['failure_count'] = 0
    sender_mock.send_info.return_value = status_info

    query_value = DummyResonse()
    query_value.status_code = 200
    query_value.resp_json = TEMP_RESPONSE
    sender_mock.query_info.return_value = query_value
    return sender


def test_agents(mgr_inst, sender=None):
    if sender is None:
        sender = generate_sender_mock()

    metrics_agents = \
        [CephClusterAgent, CephMonOsdAgent, CephPoolAgent, DBRelayAgent,
         SAIAgent, SAICluserAgent, SAIDiskAgent, SAIDiskSmartAgent,
         SAIHostAgent, PredictionAgent]
    for agent in metrics_agents:
        obj_agent = agent(mgr_inst, sender)
        obj_agent.run()
