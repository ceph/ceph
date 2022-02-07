import os

ALERTS_FILE = '../prometheus_alerts.yml'
UNIT_TESTS_FILE = 'test_alerts.yml'
MIB_FILE = '../../snmp/CEPH-MIB.txt'

current_dir = os.path.dirname(os.path.abspath(__file__))

ALERTS_FILE = os.path.join(current_dir, ALERTS_FILE)
UNIT_TESTS_FILE = os.path.join(current_dir, UNIT_TESTS_FILE)
MIB_FILE = os.path.join(current_dir, MIB_FILE)
