import logging

from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Sync or skew clock

    This will initially sync the clocks.  Eventually it should let us also
    skew by some number of seconds.

    example:

    tasks:
    - clock:
    - ceph:
    - interactive:

    to sync.

    To skew, we should allow something like:

    tasks:
    - clock:
        mon.0: -2
        client.1: 2
    - ceph:
    - interactive:

    """

    ctx.cluster.run(
        args=[
            'sudo',
            'service', 'ntp', 'stop',
            run.Raw(';'),
            'sudo',
            'ntpdate',
            'clock1.dreamhost.com',
            'clock2.dreamhost.com',
            'clock3.dreamhost.com',
            run.Raw(';'),
            'sudo',
            'service', 'ntp', 'start',
            run.Raw('||'),
            'true'
            ],
        )

    # TODO do the skew
    #for role, skew in config.iteritems():
