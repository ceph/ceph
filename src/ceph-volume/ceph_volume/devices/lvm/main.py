import argparse
from textwrap import dedent
from ceph_volume import terminal
from . import activate
from . import deactivate
from . import prepare
from . import create
from . import trigger
from . import listing
from . import zap
from . import batch
from . import migrate
from typing import List, Optional


class LVM:

    help = 'Use LVM and LVM-based technologies to deploy OSDs'

    _help = dedent("""
    Use LVM and LVM-based technologies to deploy OSDs

    {sub_help}
    """)

    mapper = {
        'activate': activate.Activate,
        'deactivate': deactivate.Deactivate,
        'batch': batch.Batch,
        'prepare': prepare.Prepare,
        'create': create.Create,
        'trigger': trigger.Trigger,
        'list': listing.List,
        'zap': zap.Zap,
        'migrate': migrate.Migrate,
        'new-wal': migrate.NewWAL,
        'new-db': migrate.NewDB,
    }

    def __init__(self, argv: Optional[List[str]]) -> None:
        self.argv = argv

    def print_help(self, sub_help: str) -> str:
        return self._help.format(sub_help=sub_help)

    def main(self) -> None:
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        if self.argv is None:
            self.argv = []
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            parser.print_help()
