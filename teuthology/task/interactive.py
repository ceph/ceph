import code
import readline
import rlcompleter
rlcompleter.__name__ # silence pyflakes

readline.parse_and_bind('tab: complete')

def task(ctx, config):
    code.interact(
        banner='Ceph test interactive mode, use ctx to interact with the cluster, press control-D to exit...',
        # TODO simplify this
        local=dict(
            ctx=ctx,
            config=config,
            ),
        )
