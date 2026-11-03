import threading
import tracemalloc

import pytest

import mgr_debug


@pytest.fixture
def busy_thread():
    stop = threading.Event()
    t = threading.Thread(target=stop.wait, name='busy', daemon=True)
    t.start()
    yield t
    stop.set()
    t.join()


@pytest.fixture
def no_tracing():
    yield
    mgr_debug.mem('stop', 0, 0)


def test_stacks(busy_thread):
    out = mgr_debug.handle({'prefix': 'py stacks'})
    assert '"busy"' in out
    assert 'wait (threading.py:' in out


def test_sample(busy_thread):
    out = mgr_debug.handle({'prefix': 'py stacks', 'duration': 0.2,
                            'rate': 50})
    lines = out.splitlines()
    assert lines[-1].startswith('# samples=')
    assert any(line.rsplit(' ', 1)[-1].isdigit() and 'wait' in line
               for line in lines[:-1])


def test_sample_async(busy_thread, monkeypatch):
    monkeypatch.setattr(mgr_debug, 'REPLY_WAIT', 0.05)
    cmd = {'prefix': 'py stacks', 'duration': 0.5, 'rate': 50}
    assert 'repeat the command' in mgr_debug.handle(cmd)
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'start'})
    assert 'started' in out
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'top'})
    assert out.startswith('busy with "py stacks --duration 0.5 --rate 50"')
    monkeypatch.setattr(mgr_debug, 'REPLY_WAIT', 5)
    assert mgr_debug.handle(cmd).splitlines()[-1].startswith('# samples=')
    mgr_debug.handle({'prefix': 'py mem', 'action': 'stop'})


def test_own_threads_hidden(busy_thread):
    out = mgr_debug.handle({'prefix': 'py stacks', 'duration': 0.2})
    assert 'mgr_debug.py' not in out


def test_mem_requires_start(no_tracing):
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'diff'})
    assert 'not tracing' in out


def test_mem_diff(no_tracing):
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'start',
                            'nframes': 5})
    assert tracemalloc.is_tracing()
    assert 'started' in out
    leak = [bytearray(1024) for _ in range(100)]
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'diff',
                            'limit': 5})
    assert 'test_mgr_debug.py' in out
    assert 'by module' in out
    del leak
    mgr_debug.handle({'prefix': 'py mem', 'action': 'stop'})
    assert not tracemalloc.is_tracing()


def test_mem_top(no_tracing):
    mgr_debug.handle({'prefix': 'py mem', 'action': 'start', 'nframes': 5})
    keep = [bytearray(4096) for _ in range(50)]
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'top',
                            'limit': 5})
    assert out.startswith('live memory')
    assert 'test_mgr_debug.py' in out
    assert '+' not in out
    del keep


def test_mem_diff_without_baseline(no_tracing):
    tracemalloc.start()
    out = mgr_debug.handle({'prefix': 'py mem', 'action': 'diff'})
    assert 'no baseline' in out


def test_module_of():
    root = mgr_debug._mgr_path
    assert mgr_debug._module_of(root + 'balancer/module.py') == 'balancer'
    assert mgr_debug._module_of(root + 'mgr_module.py') == 'mgr'
    assert mgr_debug._module_of('/usr/lib/python3/json.py') == '<other>'


def test_unknown_prefix():
    with pytest.raises(ValueError):
        mgr_debug.handle({'prefix': 'py nope'})
