import argparse
import textwrap

import teuthology.lock


def _positive_int(string):
    value = int(string)
    if value < 1:
        raise argparse.ArgumentTypeError(
            '{string} is not positive'.format(string=string))
    return value


def main():
    teuthology.lock.main(parse_args())


def parse_args():
    parser = argparse.ArgumentParser(
        description='Lock, unlock, or query lock status of machines',
        epilog=textwrap.dedent('''
            Examples:
            teuthology-lock --summary
            teuthology-lock --lock-many 1 --machine-type vps
            teuthology-lock --lock -t target.yaml
            teuthology-lock --list-targets plana01
            teuthology-lock --list --brief --owner user@host
            teuthology-lock --update --status down --desc testing plana01
        '''),
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--list',
        action='store_true',
        default=False,
        help='Show lock info for machines owned by you, or only machines ' +
        'specified. Can be restricted by --owner, --status, and --locked.',
    )
    group.add_argument(
        '--list-targets',
        action='store_true',
        default=False,
        help='Show lock info for all machines, or only machines specified, ' +
        'in targets: yaml format. Can be restricted by --owner, --status, ' +
        'and --locked.',
    )
    group.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock particular machines',
    )
    group.add_argument(
        '--unlock',
        action='store_true',
        default=False,
        help='unlock particular machines',
    )
    group.add_argument(
        '--lock-many',
        dest='num_to_lock',
        type=_positive_int,
        help='lock this many machines',
    )
    group.add_argument(
        '--update',
        action='store_true',
        default=False,
        help='update the description or status of some machines',
    )
    group.add_argument(
        '--summary',
        action='store_true',
        default=False,
        help='summarize locked-machine counts by owner',
    )
    parser.add_argument(
        '-a', '--all',
        action='store_true',
        default=False,
        help='list all machines, not just those owned by you',
    )
    parser.add_argument(
        '--owner',
        default=None,
        help='owner of the lock(s) (must match to unlock a machine)',
    )
    parser.add_argument(
        '-f',
        action='store_true',
        default=False,
        help="don't exit after the first error, continue locking or " +
        "unlocking other machines",
    )
    parser.add_argument(
        '--desc',
        default=None,
        help='lock description',
    )
    parser.add_argument(
        '--desc-pattern',
        default=None,
        help='lock description',
    )
    parser.add_argument(
        '--machine-type',
        default=None,
        help='Type of machine to lock, valid choices: mira | plana | ' +
        'burnupi | vps | saya | tala',
    )
    parser.add_argument(
        '--status',
        default=None,
        choices=['up', 'down'],
        help='whether a machine is usable for testing',
    )
    parser.add_argument(
        '--locked',
        default=None,
        choices=['true', 'false'],
        help='whether a machine is locked',
    )
    parser.add_argument(
        '--brief',
        action='store_true',
        default=False,
        help='Shorten information reported from --list',
    )
    parser.add_argument(
        '-t', '--targets',
        dest='targets',
        default=None,
        help='input yaml containing targets',
    )
    parser.add_argument(
        'machines',
        metavar='MACHINE',
        default=[],
        nargs='*',
        help='machines to operate on',
    )
    parser.add_argument(
        '--os-type',
        default='ubuntu',
        help='virtual machine type',
    )

    return parser.parse_args()
