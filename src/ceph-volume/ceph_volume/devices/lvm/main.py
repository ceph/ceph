from textwrap import dedent
from tambo import Transport
import activate
import prepare


class LVM(object):

    help = 'Use LVM and LVM-based technologies like dmcache to deploy OSDs'

    _help = dedent("""
    Use LVM and LVM-based technologies like dmcache to deploy OSDs

    {sub_help}
    """)

    mapper = {
        'activate': activate.Activate,
        'prepare': prepare.Prepare,
    }

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        options = [['--log', '--logging']]
        parser = Transport(self.argv, mapper=self.mapper,
                           options=options, check_help=False,
                           check_version=False)
        parser.parse_args()
        parser.catch_help = self.print_help(parser.subhelp())
        parser.mapper = self.mapper
        if len(self.argv) <= 1:
            return parser.print_help()
        parser.dispatch()
        parser.catches_help()
