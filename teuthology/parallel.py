import gevent
import logging

log = logging.getLogger(__name__)

def run(func, all_args):
    tasks = []
    for args in all_args:
        assert isinstance(args, list), 'args must be a list of lists'
        tasks.append(gevent.spawn(func, *args))
    gevent.sleep(0)

    try:
        while tasks:
            task = tasks[-1]
            try:
                task.get(block=True, timeout=5)
            except gevent.Timeout:
                continue
            else:
                tasks.pop()
    except:
        log.exception('Exception from parallel greenlet')
        for task in tasks:
            log.error('KILLING TASK')
            task.kill(block=True)
        raise
