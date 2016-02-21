# Copyright (C) 2015 <contact@redhat.com>
#
# Author: Alfredo Deza <adeza@redhat.com>
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
from ceph_detect_init import centos
from ceph_detect_init import debian
from ceph_detect_init import exc
from ceph_detect_init import fedora
from ceph_detect_init import rhel
from ceph_detect_init import suse
import logging
import platform


def get(use_rhceph=False):
    distro_name, release, codename = platform_information()
    if not codename or not _get_distro(distro_name):
        raise exc.UnsupportedPlatform(
            distro=distro_name,
            codename=codename,
            release=release)

    module = _get_distro(distro_name, use_rhceph=use_rhceph)
    module.name = distro_name
    module.normalized_name = _normalized_distro_name(distro_name)
    module.distro = module.normalized_name
    module.is_el = module.normalized_name in ['redhat', 'centos',
                                              'fedora', 'scientific']
    module.release = release
    module.codename = codename
    module.init = module.choose_init()
    return module


def _get_distro(distro, use_rhceph=False):
    if not distro:
        return

    distro = _normalized_distro_name(distro)
    distributions = {
        'debian': debian,
        'ubuntu': debian,
        'linuxmint': debian,
        'centos': centos,
        'scientific': centos,
        'redhat': centos,
        'fedora': fedora,
        'suse': suse,
    }

    if distro == 'redhat' and use_rhceph:
        return rhel
    else:
        return distributions.get(distro)


def _normalized_distro_name(distro):
    distro = distro.lower()
    if distro.startswith(('redhat', 'red hat')):
        return 'redhat'
    elif distro.startswith(('scientific', 'scientific linux')):
        return 'scientific'
    elif distro.startswith(('suse', 'opensuse')):
        return 'suse'
    elif distro.startswith('centos'):
        return 'centos'
    return distro


def platform_information():
    """detect platform information from remote host."""
    logging.debug('platform_information: linux_distribution = ' +
                  str(platform.linux_distribution()))
    distro, release, codename = platform.linux_distribution()
    # this could be an empty string in Debian
    if not codename and 'debian' in distro.lower():
        debian_codenames = {
            '8': 'jessie',
            '7': 'wheezy',
            '6': 'squeeze',
        }
        major_version = release.split('.')[0]
        codename = debian_codenames.get(major_version, '')

        # In order to support newer jessie/sid or wheezy/sid strings
        # we test this if sid is buried in the minor, we should use
        # sid anyway.
        if not codename and '/' in release:
            major, minor = release.split('/')
            if minor == 'sid':
                codename = minor
            else:
                codename = major

    return (
        str(distro).rstrip(),
        str(release).rstrip(),
        str(codename).rstrip()
    )
