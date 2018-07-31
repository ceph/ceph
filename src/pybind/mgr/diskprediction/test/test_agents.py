# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
from ..common.restapiclient import DummyResonse

TEMP_RESPONSE = {
    'results': [
        {
            'statement_id': 0,
            'series': [
                {
                    'name': 'sai_disk_prediction',
                    'columns': [
                        'time',
                        'cluster_domain_id',
                        'confidence',
                        'disk_domain_id',
                        'disk_model',
                        'disk_name',
                        'disk_serial_number',
                        'disk_type',
                        'disk_vendor',
                        'host_domain_id',
                        'is_simulated',
                        'life_expectancy',
                        'life_expectancy_day',
                        'model_version',
                        'near_failure',
                        'predicted',
                        'primary_key',
                        'problems',
                        'replacement_recommendation',
                        'replacement_time',
                        'symptom'
                    ],
                    'values': [
                        [
                            '2018-08-01T01:25:43.265724Z',
                            '8a65ca7d-15cd-4b99-b248-7f02e9886850',
                            98.3586755462975,
                            '50014ee205e7698b',
                            'WDC WD1003FBYX-18Y7B0',
                            'sdb',
                            'WD-WCAW32140123',
                            'SATA HDD (guess)',
                            'Western Digital RE4',
                            '8a65ca7d-15cd-4b99-b248-7f02e9886850_david-desktop',
                            '',
                            51,
                            1554,
                            '2.0.1',
                            'Good',
                            1533086744000000000,
                            '',
                            '',
                            'N/A',
                            '',
                            ''
                        ]
                    ]
                }
            ]
        }
    ]
}

def generate_sender_mock():
    sender_mock = mock.MagicMock()
    sender = sender_mock
    return_value = DummyResonse()
    return_value.status_code = 200
    return_value.content = 'Succeed'
    sender_mock.send_info.return_value = return_value

    query_value = DummyResonse()
    query_value.status_code = 200
    query_value.json = {

    }
    sender_mock.query_info.return_value = TEMP_RESPONSE
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
