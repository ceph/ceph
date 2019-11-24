from pytest import raises
from teuthology import contextutil
from logging import ERROR


class TestSafeWhile(object):

    def setup(self):
        contextutil.log.setLevel(ERROR)
        self.fake_sleep = lambda s: True
        self.s_while = contextutil.safe_while

    def test_6_5_10_deal(self):
        with raises(contextutil.MaxWhileTries):
            with self.s_while(_sleeper=self.fake_sleep) as proceed:
                while proceed():
                    pass

    def test_6_0_1_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                tries=1,
                _sleeper=self.fake_sleep
            ) as proceed:
                while proceed():
                    pass

        assert 'waiting for 6 seconds' in str(error)

    def test_1_0_10_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                sleep=1,
                _sleeper=self.fake_sleep
            ) as proceed:
                while proceed():
                    pass

        assert 'waiting for 10 seconds' in str(error)

    def test_6_1_10_deal(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                increment=1,
                _sleeper=self.fake_sleep
            ) as proceed:
                while proceed():
                    pass

        assert 'waiting for 105 seconds' in str(error)

    def test_action(self):
        with raises(contextutil.MaxWhileTries) as error:
            with self.s_while(
                action='doing the thing',
                _sleeper=self.fake_sleep
            ) as proceed:
                while proceed():
                    pass

        assert "'doing the thing' reached maximum tries" in str(error)

    def test_no_raise(self):
        with self.s_while(_raise=False, _sleeper=self.fake_sleep) as proceed:
            while proceed():
                pass

        assert True
