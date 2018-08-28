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
from ceph_detect_init import alpine
from ceph_detect_init import arch
from ceph_detect_init import centos
from ceph_detect_init import debian
from ceph_detect_init import exc
from ceph_detect_init import fedora
from ceph_detect_init import rhel
from ceph_detect_init import suse
from ceph_detect_init import gentoo
from ceph_detect_init import freebsd
from ceph_detect_init import docker
from ceph_detect_init import oraclevms
import os
import logging
import platform
import re


def get(use_rhceph=False):
    distro_name, release, codename = platform_information()
    # Not all distributions have a concept that maps to codenames
    # (or even releases really)
    if not codename and not _get_distro(distro_name):
        raise exc.UnsupportedPlatform(
            distro=distro_name,
            codename=codename,
            release=release)

    module = _get_distro(distro_name, use_rhceph=use_rhceph)
    module.name = distro_name
    module.normalized_name = _normalized_distro_name(distro_name)
    module.distro = module.normalized_name
    module.is_el = module.normalized_name in ['redhat', 'centos',
                                              'fedora', 'scientific',
                                              'oraclel']
    module.release = release
    module.codename = codename
    module.init = module.choose_init()
    return module


def _get_distro(distro, use_rhceph=False):
    if not distro:
        return

    distro = _normalized_distro_name(distro)
    distributions = {
        'alpine': alpine,
        'arch': arch,
        'debian': debian,
        'ubuntu': debian,
        'linuxmint': debian,
        'centos': centos,
        'scientific': centos,
        'oraclel': centos,
        'oraclevms': oraclevms,
        'redhat': centos,
        'fedora': fedora,
        'suse': suse,
        'gentoo': gentoo,
        'funtoo': gentoo,
        'exherbo': gentoo,
        'freebsd': freebsd,
        'docker': docker,
        'virtuozzo': centos,
    }

    if distro == 'redhat' and use_rhceph:
        return rhel
    else:
        return distributions.get(distro)


def _normalized_distro_name(distro):
    distro = distro.lower()
    if distro.startswith(('redhat', 'red hat', 'rhel')):
        return 'redhat'
    elif distro.startswith(('scientific', 'scientific linux')):
        return 'scientific'
    elif distro.startswith(('suse', 'opensuse', 'sles', 'sled')):
        return 'suse'
    elif distro.startswith('centos'):
        return 'centos'
    elif distro.startswith('oracle linux'):
        return 'oraclel'
    elif distro.startswith('oracle vm'):
        return 'oraclevms'
    elif distro.startswith(('gentoo', 'funtoo', 'exherbo')):
        return 'gentoo'
    elif distro.startswith('virtuozzo'):
        return 'virtuozzo'
    return distro


def _extract_from_os_release(file_contents, key):
    r = re.compile('^{}\=[\'\"]*([^\'\"\n]*)'.format(key), re.MULTILINE)
    match = r.search(file_contents)
    if match:
        return match.group(1)
    else:
        return ''


def platform_information():
    """detect platform information from remote host."""
    try:
        file_name = '/proc/self/cgroup'
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if "docker" in line.split(':')[2]:
                    return ('docker', 'docker', 'docker')
    except Exception as err:
        logging.debug("platform_information: ",
                      "Error while opening %s : %s" % (file_name, err))

    if os.path.isfile('/.dockerenv'):
        return ('docker', 'docker', 'docker')

    if platform.system() == 'Linux':
        linux_distro = ('', '', '')
        if os.path.isfile('/etc/os-release'):
            try:
                with open('/etc/os-release', 'r') as f:
                    data = f.read()
                linux_distro = (
                    _extract_from_os_release(data, 'ID'),
                    _extract_from_os_release(data, 'VERSION_ID'),
                    '')
            except Exception as err:
                logging.debug("platform_information: ",
                              "Error while opening %s : %s" % (file_name, err))
        else:
            linux_distro = platform.linux_distribution(
                supported_dists=platform._supported_dists + ('alpine', 'arch'))
            logging.debug('platform_information: linux_distribution = ' +
                          str(linux_distro))
        distro, release, codename = linux_distro
    elif platform.system() == 'FreeBSD':
        distro = 'freebsd'
        release = platform.release()
        codename = platform.version().split(' ')[3].split(':')[0]
        logging.debug(
            'platform_information: release = {}, version = {}'.format(
                platform.release(), platform.version()))
    else:
        raise exc.UnsupportedPlatform(platform.system(), '', '')

    distro_lower = distro.lower()
    # this could be an empty string in Debian
    if not codename and 'debian' in distro_lower:
        pass
    # this is an empty string in Oracle
    elif distro_lower.startswith('oracle linux'):
        codename = 'OL' + release
    elif distro_lower.startswith('oracle vm'):
        codename = 'OVS' + release
    # this could be an empty string in Virtuozzo linux
    elif distro_lower.startswith('virtuozzo linux'):
        codename = 'virtuozzo'

    return (
        str(distro).rstrip(),
        str(release).rstrip(),
        str(codename).rstrip()
    )
