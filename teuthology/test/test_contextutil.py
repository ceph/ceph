from pytest import raises
from teuthology import contextutil


class TestSafeWhile(object):

    def setup(self):
        self.fake_sleep = lambda s: True
        self.s_while = contextutil.safe_while

    def test_5_5_5_deal(self):
        with raises(contextutil.MaxWhileTries):
            with self.s_while(_sleeper=self.fake_sleep) as bomb:
                while 1:
                    bomb()

    def test_5_5_1_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                tries=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 5 seconds' in msg

    def test_1_5_5_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                sleep=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 55 seconds' in msg

    def test_5_1_5_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                increment=1,
                _sleeper=self.fake_sleep
            ) as bomb:
                while 1:
                    bomb()

        msg = error.value[0]
        assert 'waiting for 35 seconds' in msg
