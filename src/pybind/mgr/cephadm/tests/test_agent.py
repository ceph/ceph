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
        # Test that even for very small clusters (1 host),
        # the minimum concurrency of 2 is enforced
        mgr = FakeMgr(num_hosts=1)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2

    def test_concurrency_auto_exact_boundary(self):
        # Test that concurrency is capped at 20 even when sqrt(N)/2 exceeds 20
        mgr = FakeMgr(num_hosts=1600)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 20

    def test_refresh_auto_typical(self):
        # Test refresh rate computed dynamically in auto mode for a typical cluster size
        # 100 agents → concurrency = 5 and refresh_rate = (100*2)//5 = 40
        mgr = FakeMgr(num_hosts=100)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 40

    def test_refresh_auto_zero_agents(self):
        # Test refresh rate fallback for 0 agents (should enforce minimum of 20s)
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_refresh_auto_with_manual_concurrency(self):
        # Test refresh rate in auto mode with manually overridden high concurrency
        # Should still honor the lower bound of 20s
        mgr = FakeMgr(num_hosts=1000)
        mgr.agent_avg_concurrency = 100
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_min_refresh_rate(self):
        # Test that manually specified refresh rates below the lower bound are clamped to 20s
        mgr = FakeMgr()
        mgr.agent_refresh_rate = 5
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 20

    def test_refresh_manual_valid(self):
        # Test that manually specified valid refresh rate is used as-is
        mgr = FakeMgr()
        mgr.agent_refresh_rate = 60
        config = AgentEndpoint(mgr)
        assert config.compute_agents_refrsh_rate() == 60

    def test_initial_delay_auto_small(self):
        # Test auto delay with a small number of agents returns minimum 10s
        mgr = FakeMgr(num_hosts=2)
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 10

    def test_initial_delay_auto_zero_agents(self):
        # Test auto delay with 0 agents still enforces the minimum 10s delay
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 10

    def test_initial_delay_manual_below_min(self):
        # Test that manual delay below the minimum is respected (no clamping here)
        mgr = FakeMgr()
        mgr.agent_initial_startup_delay_max = 5
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 5

    def test_initial_delay_manual_valid(self):
        # Test that a valid manual delay value is used directly
        mgr = FakeMgr()
        mgr.agent_initial_startup_delay_max = 30
        config = AgentEndpoint(mgr)
        assert config.get_initial_delay() == 30

    def test_jitter_auto_zero_refresh(self):
        # Test jitter in auto mode with 0 agents; fallback refresh_rate is 20 and jitter = 10
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 10

    def test_jitter_auto_typical(self):
        # Test jitter in auto mode for a typical cluster (100 agents)
        # refresh_rate = 40 an jitter = 20
        mgr = FakeMgr(num_hosts=100)
        config = AgentEndpoint(mgr)
        jitter = config.get_jitter()
        assert jitter == 20

    def test_jitter_manual(self):
        # Test that manual jitter override is respected (even if < 2)
        mgr = FakeMgr()
        mgr.agent_jitter_seconds = 1
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 1

    def test_jitter_manual_high(self):
        # Test that a high manual jitter value is used as-is
        mgr = FakeMgr()
        mgr.agent_jitter_seconds = 60
        config = AgentEndpoint(mgr)
        assert config.get_jitter() == 60


class TestAgentEndpointAuto:

    def test_auto_minimum_values(self):
        # Test system behavior with 0 agents in full auto mode.
        # All computed values should fall back to their enforced minimums.
        mgr = FakeMgr(num_hosts=0)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2  # Enforced minimum
        assert config.compute_agents_refrsh_rate() == 20     # Enforced minimum
        assert config.get_initial_delay() == 10              # Enforced minimum
        assert config.get_jitter() == 10                     # 50% of refresh rate (20)

    def test_auto_4_hosts(self):
        # Test computed values for a very small cluster (4 agents).
        # sqrt(4)/2 = 1 -> clamped to 2 concurrency
        # Refresh rate = (4*2)//2 = 4 -> clamped to 20
        mgr = FakeMgr(num_hosts=4)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 10              # 4//2 = 2 -> clamped to 10
        assert config.get_jitter() == 10                     # 50% of refresh rate

    def test_auto_16_hosts(self):
        # Test computed values for 16 agents.
        # sqrt(16)/2 = 2 → meets minimum concurrency
        # Refresh rate = (16*2)//2 = 16 → clamped to 20
        mgr = FakeMgr(num_hosts=16)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 2
        assert config.compute_agents_refrsh_rate() == 20
        assert config.get_initial_delay() == 10              # 16//2 = 8 → clamped to 10
        assert config.get_jitter() == 10                     # 50% of refresh rate

    def test_auto_100_hosts(self):
        # Test computed values for 100 agents.
        # sqrt(100)/2 = 5 -> concurrency = 5
        # Refresh rate = (100*2)//5 = 40
        # Initial delay = 100//5 = 20
        mgr = FakeMgr(num_hosts=100)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 5
        assert config.compute_agents_refrsh_rate() == 40
        assert config.get_initial_delay() == 20
        assert config.get_jitter() == 20  # 50% of refresh rate

    def test_auto_200_hosts(self):
        # Test computed values for 200 agents.
        # sqrt(200)/2 ≈ 7.07 -> concurrency = 7
        # Refresh rate = (200*2)//7 = ~57
        # Initial delay = 200//7 ~ 28
        mgr = FakeMgr(num_hosts=200)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 7
        assert config.compute_agents_refrsh_rate() == 57
        assert config.get_jitter() == 28  # 50% of refresh rate

    def test_auto_1000_hosts(self):
        # Test computed values for 1000 agents.
        # sqrt(1000)/2 ≈ 15.81 → concurrency = 15
        # Refresh rate = (1000*2)//15 ~ 133
        # Initial delay = 1000//15 ~ 66
        mgr = FakeMgr(num_hosts=1000)
        config = AgentEndpoint(mgr)
        assert config.compute_agents_avg_concurrency() == 15
        assert config.compute_agents_refrsh_rate() == 133
        assert config.get_initial_delay() == 66
        assert config.get_jitter() == 66  # 50% of refresh rate
