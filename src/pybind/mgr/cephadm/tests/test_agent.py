
from unittest.mock import MagicMock
import pytest

from cephadm.agent import AgentEndpoint

class FakeCache:
    def __init__(self, num_hosts):
        self._hosts = ['host{}'.format(i) for i in range(num_hosts)]

    def get_hosts(self):
        return self._hosts

class FakeMgr:
    def __init__(self, num_hosts=10):
        self.agent_avg_concurrency = -1
        self.agent_refresh_rate = -1
        self.agent_initial_startup_delay_max = -1
        self.agent_jitter_seconds = -1
        self.cache = FakeCache(num_hosts)

    def get_mgr_ip(self):
        return ''

class TestAgentEndpoint:

    def test_concurrency_auto_small(self):
        mgr = FakeMgr(num_hosts=1)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2

    def test_concurrency_auto_exact_boundary(self):
        mgr = FakeMgr(num_hosts=1600)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 20

    def test_concurrency_manual_zero(self):
        mgr = FakeMgr()
        mgr.agent_avg_concurrency = 0
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2

    def test_concurrency_manual_high(self):
        mgr = FakeMgr()
        mgr.agent_avg_concurrency = 999
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 999

    def test_refresh_auto_typical(self):
        mgr = FakeMgr(num_hosts=40)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_refresh_auto_zero_agents(self):
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_refresh_auto_with_high_concurrency(self):
        mgr = FakeMgr(num_hosts=1000)
        mgr.agent_avg_concurrency = 100
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_min_refresh_rate(self):
        mgr = FakeMgr()
        mgr.agent_refresh_rate = 5
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_refresh_manual_valid(self):
        mgr = FakeMgr()
        mgr.agent_refresh_rate = 60
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 60

    def test_initial_delay_auto_small(self):
        mgr = FakeMgr(num_hosts=2)
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 10

    def test_initial_delay_auto_zero_agents(self):
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 10

    def test_initial_delay_manual_below_min(self):
        mgr = FakeMgr()
        mgr.agent_initial_startup_delay_max = 5
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 5

    def test_initial_delay_manual_valid(self):
        mgr = FakeMgr()
        mgr.agent_initial_startup_delay_max = 30
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 30

    def test_jitter_auto_zero_refresh(self):
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 10

    def test_jitter_auto_typical(self):
        mgr = FakeMgr(num_hosts=100)
        config = AgentEndpoint(mgr)
        jitter = config.get_jitter()
        assert jitter >= 2
        assert isinstance(jitter, int)

    def test_jitter_manual(self):
        mgr = FakeMgr()
        mgr.agent_jitter_seconds = 1
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 1

    def test_jitter_manual_high(self):
        mgr = FakeMgr()
        mgr.agent_jitter_seconds = 60
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 60


class TestAgentEndpointAuto:

    def test_auto_minimum_values(self):
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 10
        assert config.get_jitter() == 10

    def test_auto_4_hosts(self):
        mgr = FakeMgr(num_hosts=4)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 10
        assert config.get_jitter() == 10

    def test_auto_16_hosts(self):

        mgr = FakeMgr(num_hosts=16)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 10
        assert config.get_jitter() == 10

    def test_auto_100_hosts(self):
        mgr = FakeMgr(num_hosts=100)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 5
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 20
        assert config.get_jitter() == 10

    def test_auto_200_hosts(self):
        mgr = FakeMgr(num_hosts=200)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 7
        assert config.compute_agents_refrsh_rate() == 28
        assert config.get_initial_delay() == 28
        assert config.get_jitter() == 14

    def test_auto_1000_hosts(self):
        mgr = FakeMgr(num_hosts=1000)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 15
        assert config.compute_agents_refrsh_rate() == 66
        assert config.get_initial_delay() == 66
        assert config.get_jitter() == 33
