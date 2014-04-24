"""
Drop into a python shell
"""
import code
import readline
import rlcompleter
rlcompleter.__name__ # silence pyflakes
import pprint

readline.parse_and_bind('tab: complete')

def task(ctx, config):
    """
    Run an interactive Python shell, with the cluster accessible via
    the ``ctx`` variable.

    Hit ``control-D`` to continue.

    This is also useful to pause the execution of the test between two
    tasks, either to perform ad hoc operations, or to examine the
    state of the cluster. You can also use it to easily bring up a
    Ceph cluster for ad hoc testing.

    For example::

        tasks:
        - ceph:
        - interactive:
    """

    pp = pprint.PrettyPrinter().pprint
    code.interact(
        banner='Ceph test interactive mode, use ctx to interact with the cluster, press control-D to exit...',
        # TODO simplify this
        local=dict(
            ctx=ctx,
            config=config,
            pp=pp,
            ),
        )
