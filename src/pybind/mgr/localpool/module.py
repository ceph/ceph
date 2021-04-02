from mgr_module import MgrModule, CommandResult, Option
import json
import threading
from typing import cast, Any


class Module(MgrModule):

    MODULE_OPTIONS = [
        Option(
            name='subtree',
            type='str',
            default='rack',
            desc='CRUSH level for which to create a local pool',
            runtime=True),
        Option(
            name='failure_domain',
            type='str',
            default='host',
            desc='failure domain for any created local pool',
            runtime=True),
        Option(
            name='min_size',
            type='int',
            desc='default min_size for any created local pool',
            runtime=True),
        Option(
            name='num_rep',
            type='int',
            default=3,
            desc='default replica count for any created local pool',
            runtime=True),
        Option(
            name='pg_num',
            type='int',
            default=128,
            desc='default pg_num for any created local pool',
            runtime=True),
        Option(
            name='prefix',
            type='str',
            default='',
            desc='name prefix for any created local pool',
            runtime=True),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.serve_event = threading.Event()

    def notify(self, notify_type: str, notify_id: str) -> None:
        if notify_type == 'osd_map':
            self.handle_osd_map()

    def handle_osd_map(self) -> None:
        """
        Check pools on each OSDMap change
        """
        subtree_type = cast(str, self.get_module_option('subtree'))
        failure_domain = self.get_module_option('failure_domain')
        pg_num = self.get_module_option('pg_num')
        num_rep = self.get_module_option('num_rep')
        min_size = self.get_module_option('min_size')
        prefix = cast(str, self.get_module_option('prefix')) or 'by-' + subtree_type + '-'

        osdmap = self.get("osd_map")
        lpools = []
        for pool in osdmap['pools']:
            if pool['pool_name'].find(prefix) == 0:
                lpools.append(pool['pool_name'])

        self.log.debug('localized pools = %s', lpools)
        subtrees = []
        tree = self.get('osd_map_tree')
        for node in tree['nodes']:
            if node['type'] == subtree_type:
                subtrees.append(node['name'])
                pool_name = prefix + node['name']
                if pool_name not in lpools:
                    self.log.info('Creating localized pool %s', pool_name)
                    #
                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd crush rule create-replicated",
                        "format": "json",
                        "name": pool_name,
                        "root": node['name'],
                        "type": failure_domain,
                    }), "")
                    r, outb, outs = result.wait()

                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd pool create",
                        "format": "json",
                        "pool": pool_name,
                        'rule': pool_name,
                        "pool_type": 'replicated',
                        'pg_num': pg_num,
                    }), "")
                    r, outb, outs = result.wait()

                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd pool set",
                        "format": "json",
                        "pool": pool_name,
                        'var': 'size',
                        "val": str(num_rep),
                    }), "")
                    r, outb, outs = result.wait()

                    if min_size:
                        result = CommandResult("")
                        self.send_command(result, "mon", "", json.dumps({
                            "prefix": "osd pool set",
                            "format": "json",
                            "pool": pool_name,
                            'var': 'min_size',
                            "val": str(min_size),
                        }), "")
                        r, outb, outs = result.wait()

        # TODO remove pools for hosts that don't exist?

    def serve(self) -> None:
        self.handle_osd_map()
        self.serve_event.wait()
        self.serve_event.clear()

    def shutdown(self) -> None:
        self.serve_event.set()
