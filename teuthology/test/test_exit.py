import os
import random
import signal

from unittest.mock import patch, Mock

from teuthology import exit


class TestExiter(object):
    klass = exit.Exiter

    def setup(self):
        self.pid = os.getpid()

        # Below, we patch os.kill() in such a way that the first time it is
        # invoked it does actually send the signal. Any subsequent invocation
        # won't send any signal - this is so we don't kill the process running
        # our unit tests!
        self.patcher_kill = patch(
            'teuthology.exit.os.kill',
            wraps=os.kill,
        )

        self.kill_unpatched = os.kill
        self.m_kill = self.patcher_kill.start()

        def m_kill_unwrap(pid, sig):
            # Setting return_value of a mocked object disables the wrapping
            if self.m_kill.call_count > 1:
                self.m_kill.return_value = None

        self.m_kill.side_effect = m_kill_unwrap

    def teardown(self):
        self.patcher_kill.stop()
        del self.m_kill

    def test_noop(self):
        sig = 15
        obj = self.klass()
        assert len(obj.handlers) == 0
        assert signal.getsignal(sig) == 0

    def test_basic(self):
        sig = 15
        obj = self.klass()
        m_func = Mock()
        obj.add_handler(sig, m_func)
        assert len(obj.handlers) == 1
        self.kill_unpatched(self.pid, sig)
        assert m_func.call_count == 1
        assert self.m_kill.call_count == 1
        for arg_list in self.m_kill.call_args_list:
            assert arg_list[0] == (self.pid, sig)

    def test_remove_handlers(self):
        sig = [1, 15]
        send_sig = random.choice(sig)
        n = 3
        obj = self.klass()
        handlers = list()
        for i in range(n):
            m_func = Mock(name="handler %s" % i)
            handlers.append(obj.add_handler(sig, m_func))
        assert obj.handlers == handlers
        for handler in handlers:
            handler.remove()
        assert obj.handlers == list()
        self.kill_unpatched(self.pid, send_sig)
        assert self.m_kill.call_count == 1
        for handler in handlers:
            assert handler.func.call_count == 0

    def test_n_handlers(self, n=10, sig=11):
        if isinstance(sig, int):
            send_sig = sig
        else:
            send_sig = random.choice(sig)
        obj = self.klass()
        handlers = list()
        for i in range(n):
            m_func = Mock(name="handler %s" % i)
            handlers.append(obj.add_handler(sig, m_func))
        assert obj.handlers == handlers
        self.kill_unpatched(self.pid, send_sig)
        for i in range(n):
            assert handlers[i].func.call_count == 1
        assert self.m_kill.call_count == 1
        for arg_list in self.m_kill.call_args_list:
            assert arg_list[0] == (self.pid, send_sig)

    def test_multiple_signals(self):
        self.test_n_handlers(n=3, sig=[1, 6, 11, 15])
