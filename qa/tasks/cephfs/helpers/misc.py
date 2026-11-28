from logging import getLogger
from random import randrange


log = getLogger(__name__)


def gen_name(name, n=1):
    number = randrange(1, 99)
    # XXX: prefix 0 if number is single digit number so that names "snap3"
    # can never match "snap34".
    name += str(number) if number > 9 else f'0{number}'
    return name if n == 1 else [f'{name}{i}' for i in range(n)]


def gen_vol_name(n=1):
    return gen_name('vol', n)


def gen_subvol_name(n=1):
    return gen_name('subvol', n)


def gen_subvol_grp_name(n=1):
    return gen_name('subvol_grp', n)


def gen_subvol_snap_name(n=1):
    return gen_name('subvol_snap', n)


def gen_subvol_clone_name(n=1):
    return gen_name('subvol_clone', n)
