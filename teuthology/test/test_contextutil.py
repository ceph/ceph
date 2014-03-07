from pytest import raises
from teuthology import contextutil


class TestSafeWhile(object):

    def setup(self):
        self.fake_sleep = lambda s: True
        self.s_while = contextutil.safe_while

    def test_6_5_10_deal(self):
        with raises(contextutil.MaxWhileTries):
            with self.s_while(_sleeper=self.fake_sleep) as bomb:
                while 1:
                    bomb()

    def test_6_0_1_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                tries=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 6 seconds' in msg

    def test_1_0_10_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                sleep=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 10 seconds' in msg

    def test_6_1_10_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                increment=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 105 seconds' in msg
