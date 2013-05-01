import logging
import contextlib
import datetime

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Timer

    Measure the time that this set of tasks takes and save that value in the summary file.
    Config is a description of what we are timing.

    example:

    tasks:
    - ceph:
    - foo:
    - timer: "fsx run"
    - fsx:

    """
    start = datetime.datetime.now()
    log.debug("got here in timer")
    try:
        yield
    finally:
        nowinfo = datetime.datetime.now()
        elapsed = nowinfo - start
        datesaved = nowinfo.isoformat(' ')
        hourz, remainder = divmod(elapsed.seconds, 3600)
        minutez, secondz = divmod(remainder, 60)
        elapsedtime = "%02d:%02d:%02d.%06d" % (hourz,minutez,secondz, elapsed.microseconds)
        dateinfo = (datesaved, elapsedtime)
        if not 'timer' in ctx.summary:
            ctx.summary['timer'] = {config : [dateinfo]}
        else:
            if config in ctx.summary['timer']:
                ctx.summary['timer'][config].append(dateinfo)
            else:
                ctx.summary['timer'][config] = [dateinfo]
        log.info('Elapsed time for %s -- %s' % (config,elapsedtime))
