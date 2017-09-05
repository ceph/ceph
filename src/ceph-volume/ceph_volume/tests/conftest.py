import pytest

class Capture(object):

    def __init__(self, *a, **kw):
        self.a = a
        self.kw = kw
        self.calls = []

    def __call__(self, *a, **kw):
        self.calls.append({'args': a, 'kwargs': kw})


@pytest.fixture
def capture():
    return Capture()
