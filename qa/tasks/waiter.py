
import contextlib
import gevent

@contextlib.contextmanager
def task(ctx, config):
    on_enter = config.get("on_enter", 0)
    on_exit = config.get("on_exit", 0)

    gevent.sleep(on_enter)
    yield
    gevent.sleep(on_exit)
 