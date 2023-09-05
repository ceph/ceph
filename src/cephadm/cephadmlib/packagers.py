# packagers.py - types for managing system packager systems

import logging
import os
import platform

from typing import Any, Optional, List, Tuple
from urllib.error import HTTPError
from urllib.request import urlopen

from .call_wrappers import call_throws
from .context import CephadmContext
from .exceptions import Error
from .file_utils import write_tmp

logger = logging.getLogger()


def get_distro():
    # type: () -> Tuple[Optional[str], Optional[str], Optional[str]]
    distro = None
    distro_version = None
    distro_codename = None
    with open('/etc/os-release', 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if '=' not in line or line.startswith('#'):
                continue
            (var, val) = line.split('=', 1)
            if val[0] == '"' and val[-1] == '"':
                val = val[1:-1]
            if var == 'ID':
                distro = val.lower()
            elif var == 'VERSION_ID':
                distro_version = val.lower()
            elif var == 'VERSION_CODENAME':
                distro_codename = val.lower()
    return distro, distro_version, distro_codename


class Packager(object):
    def __init__(
        self,
        ctx: CephadmContext,
        stable: Optional[str] = None,
        version: Optional[str] = None,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
    ):
        assert (
            (stable and not version and not branch and not commit)
            or (not stable and version and not branch and not commit)
            or (not stable and not version and branch)
            or (not stable and not version and not branch and not commit)
        )
        self.ctx = ctx
        self.stable = stable
        self.version = version
        self.branch = branch
        self.commit = commit

    def validate(self) -> None:
        """Validate parameters before writing any state to disk."""
        pass

    def add_repo(self) -> None:
        raise NotImplementedError

    def rm_repo(self) -> None:
        raise NotImplementedError

    def install(self, ls: List[str]) -> None:
        raise NotImplementedError

    def install_podman(self) -> None:
        raise NotImplementedError

    def query_shaman(
        self,
        distro: str,
        distro_version: Any,
        branch: Optional[str],
        commit: Optional[str],
    ) -> str:
        # query shaman
        arch = platform.uname().machine
        logger.info('Fetching repo metadata from shaman and chacra...')
        shaman_url = 'https://shaman.ceph.com/api/repos/ceph/{branch}/{sha1}/{distro}/{distro_version}/repo/?arch={arch}'.format(
            distro=distro,
            distro_version=distro_version,
            branch=branch,
            sha1=commit or 'latest',
            arch=arch,
        )
        try:
            shaman_response = urlopen(shaman_url)
        except HTTPError as err:
            logger.error(
                'repository not found in shaman (might not be available yet)'
            )
            raise Error('%s, failed to fetch %s' % (err, shaman_url))
        chacra_url = ''
        try:
            chacra_url = shaman_response.geturl()
            chacra_response = urlopen(chacra_url)
        except HTTPError as err:
            logger.error(
                'repository not found in chacra (might not be available yet)'
            )
            raise Error('%s, failed to fetch %s' % (err, chacra_url))
        return chacra_response.read().decode('utf-8')

    def repo_gpgkey(self) -> Tuple[str, str]:
        if self.ctx.gpg_url:
            return self.ctx.gpg_url, 'manual'
        if self.stable or self.version:
            return 'https://download.ceph.com/keys/release.gpg', 'release'
        else:
            return 'https://download.ceph.com/keys/autobuild.gpg', 'autobuild'

    def enable_service(self, service: str) -> None:
        """
        Start and enable the service (typically using systemd).
        """
        call_throws(self.ctx, ['systemctl', 'enable', '--now', service])


class Apt(Packager):
    DISTRO_NAMES = {
        'ubuntu': 'ubuntu',
        'debian': 'debian',
    }

    def __init__(
        self,
        ctx: CephadmContext,
        stable: Optional[str],
        version: Optional[str],
        branch: Optional[str],
        commit: Optional[str],
        distro: Optional[str],
        distro_version: Optional[str],
        distro_codename: Optional[str],
    ) -> None:
        super(Apt, self).__init__(
            ctx, stable=stable, version=version, branch=branch, commit=commit
        )
        assert distro
        self.ctx = ctx
        self.distro = self.DISTRO_NAMES[distro]
        self.distro_codename = distro_codename
        self.distro_version = distro_version

    def repo_path(self) -> str:
        return '/etc/apt/sources.list.d/ceph.list'

    def add_repo(self) -> None:
        url, name = self.repo_gpgkey()
        logger.info('Installing repo GPG key from %s...' % url)
        try:
            response = urlopen(url)
        except HTTPError as err:
            logger.error(
                'failed to fetch GPG repo key from %s: %s' % (url, err)
            )
            raise Error('failed to fetch GPG key')
        key = response.read()
        with open('/etc/apt/trusted.gpg.d/ceph.%s.gpg' % name, 'wb') as f:
            f.write(key)

        if self.version:
            content = 'deb %s/debian-%s/ %s main\n' % (
                self.ctx.repo_url,
                self.version,
                self.distro_codename,
            )
        elif self.stable:
            content = 'deb %s/debian-%s/ %s main\n' % (
                self.ctx.repo_url,
                self.stable,
                self.distro_codename,
            )
        else:
            content = self.query_shaman(
                self.distro, self.distro_codename, self.branch, self.commit
            )

        logger.info('Installing repo file at %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

        self.update()

    def rm_repo(self) -> None:
        for name in ['autobuild', 'release', 'manual']:
            p = '/etc/apt/trusted.gpg.d/ceph.%s.gpg' % name
            if os.path.exists(p):
                logger.info('Removing repo GPG key %s...' % p)
                os.unlink(p)
        if os.path.exists(self.repo_path()):
            logger.info('Removing repo at %s...' % self.repo_path())
            os.unlink(self.repo_path())

        if self.distro == 'ubuntu':
            self.rm_kubic_repo()

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, ['apt-get', 'install', '-y'] + ls)

    def update(self) -> None:
        logger.info('Updating package list...')
        call_throws(self.ctx, ['apt-get', 'update'])

    def install_podman(self) -> None:
        if self.distro == 'ubuntu':
            logger.info('Setting up repo for podman...')
            self.add_kubic_repo()
            self.update()

        logger.info('Attempting podman install...')
        try:
            self.install(['podman'])
        except Error:
            logger.info('Podman did not work.  Falling back to docker...')
            self.install(['docker.io'])

    def kubic_repo_url(self) -> str:
        return (
            'https://download.opensuse.org/repositories/devel:/kubic:/'
            'libcontainers:/stable/xUbuntu_%s/' % self.distro_version
        )

    def kubic_repo_path(self) -> str:
        return '/etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list'

    def kubic_repo_gpgkey_url(self) -> str:
        return '%s/Release.key' % self.kubic_repo_url()

    def kubic_repo_gpgkey_path(self) -> str:
        return '/etc/apt/trusted.gpg.d/kubic.release.gpg'

    def add_kubic_repo(self) -> None:
        url = self.kubic_repo_gpgkey_url()
        logger.info('Installing repo GPG key from %s...' % url)
        try:
            response = urlopen(url)
        except HTTPError as err:
            logger.error(
                'failed to fetch GPG repo key from %s: %s' % (url, err)
            )
            raise Error('failed to fetch GPG key')
        key = response.read().decode('utf-8')
        tmp_key = write_tmp(key, 0, 0)
        keyring = self.kubic_repo_gpgkey_path()
        call_throws(
            self.ctx, ['apt-key', '--keyring', keyring, 'add', tmp_key.name]
        )

        logger.info('Installing repo file at %s...' % self.kubic_repo_path())
        content = 'deb %s /\n' % self.kubic_repo_url()
        with open(self.kubic_repo_path(), 'w') as f:
            f.write(content)

    def rm_kubic_repo(self) -> None:
        keyring = self.kubic_repo_gpgkey_path()
        if os.path.exists(keyring):
            logger.info('Removing repo GPG key %s...' % keyring)
            os.unlink(keyring)

        p = self.kubic_repo_path()
        if os.path.exists(p):
            logger.info('Removing repo at %s...' % p)
            os.unlink(p)


class YumDnf(Packager):
    DISTRO_NAMES = {
        'centos': ('centos', 'el'),
        'rhel': ('centos', 'el'),
        'scientific': ('centos', 'el'),
        'rocky': ('centos', 'el'),
        'almalinux': ('centos', 'el'),
        'ol': ('centos', 'el'),
        'fedora': ('fedora', 'fc'),
        'mariner': ('mariner', 'cm'),
    }

    def __init__(
        self,
        ctx: CephadmContext,
        stable: Optional[str],
        version: Optional[str],
        branch: Optional[str],
        commit: Optional[str],
        distro: Optional[str],
        distro_version: Optional[str],
    ) -> None:
        super(YumDnf, self).__init__(
            ctx, stable=stable, version=version, branch=branch, commit=commit
        )
        assert distro
        assert distro_version
        self.ctx = ctx
        self.major = int(distro_version.split('.')[0])
        self.distro_normalized = self.DISTRO_NAMES[distro][0]
        self.distro_code = self.DISTRO_NAMES[distro][1] + str(self.major)
        if (self.distro_code == 'fc' and self.major >= 30) or (
            self.distro_code == 'el' and self.major >= 8
        ):
            self.tool = 'dnf'
        elif self.distro_code == 'cm':
            self.tool = 'tdnf'
        else:
            self.tool = 'yum'

    def custom_repo(self, **kw: Any) -> str:
        """
        Repo files need special care in that a whole line should not be present
        if there is no value for it. Because we were using `format()` we could
        not conditionally add a line for a repo file. So the end result would
        contain a key with a missing value (say if we were passing `None`).

        For example, it could look like::

        [ceph repo]
        name= ceph repo
        proxy=
        gpgcheck=

        Which breaks. This function allows us to conditionally add lines,
        preserving an order and be more careful.

        Previously, and for historical purposes, this is how the template used
        to look::

        custom_repo =
        [{repo_name}]
        name={name}
        baseurl={baseurl}
        enabled={enabled}
        gpgcheck={gpgcheck}
        type={_type}
        gpgkey={gpgkey}
        proxy={proxy}

        """
        lines = []

        # by using tuples (vs a dict) we preserve the order of what we want to
        # return, like starting with a [repo name]
        tmpl = (
            ('reponame', '[%s]'),
            ('name', 'name=%s'),
            ('baseurl', 'baseurl=%s'),
            ('enabled', 'enabled=%s'),
            ('gpgcheck', 'gpgcheck=%s'),
            ('_type', 'type=%s'),
            ('gpgkey', 'gpgkey=%s'),
            ('proxy', 'proxy=%s'),
            ('priority', 'priority=%s'),
        )

        for line in tmpl:
            tmpl_key, tmpl_value = line  # key values from tmpl

            # ensure that there is an actual value (not None nor empty string)
            if tmpl_key in kw and kw.get(tmpl_key) not in (None, ''):
                lines.append(tmpl_value % kw.get(tmpl_key))

        return '\n'.join(lines)

    def repo_path(self) -> str:
        return '/etc/yum.repos.d/ceph.repo'

    def repo_baseurl(self) -> str:
        assert self.stable or self.version
        if self.version:
            return '%s/rpm-%s/%s' % (
                self.ctx.repo_url,
                self.version,
                self.distro_code,
            )
        else:
            return '%s/rpm-%s/%s' % (
                self.ctx.repo_url,
                self.stable,
                self.distro_code,
            )

    def validate(self) -> None:
        if self.distro_code.startswith('fc'):
            raise Error(
                'Ceph team does not build Fedora specific packages and therefore cannot add repos for this distro'
            )
        if self.distro_code == 'el7':
            if self.stable and self.stable >= 'pacific':
                raise Error(
                    'Ceph does not support pacific or later for this version of this linux distro and therefore cannot add a repo for it'
                )
            if self.version and self.version.split('.')[0] >= '16':
                raise Error(
                    'Ceph does not support 16.y.z or later for this version of this linux distro and therefore cannot add a repo for it'
                )

        if self.stable or self.version:
            # we know that yum & dnf require there to be a
            # $base_url/$arch/repodata/repomd.xml so we can test if this URL
            # is gettable in order to validate the inputs
            test_url = self.repo_baseurl() + '/noarch/repodata/repomd.xml'
            try:
                urlopen(test_url)
            except HTTPError as err:
                logger.error('unable to fetch repo metadata: %r', err)
                raise Error(
                    'failed to fetch repository metadata. please check'
                    ' the provided parameters are correct and try again'
                )

    def add_repo(self) -> None:
        if self.stable or self.version:
            content = ''
            for n, t in {
                'Ceph': '$basearch',
                'Ceph-noarch': 'noarch',
                'Ceph-source': 'SRPMS',
            }.items():
                content += '[%s]\n' % (n)
                content += self.custom_repo(
                    name='Ceph %s' % t,
                    baseurl=self.repo_baseurl() + '/' + t,
                    enabled=1,
                    gpgcheck=1,
                    gpgkey=self.repo_gpgkey()[0],
                )
                content += '\n\n'
        else:
            content = self.query_shaman(
                self.distro_normalized, self.major, self.branch, self.commit
            )

        logger.info('Writing repo to %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

        if self.distro_code.startswith('el'):
            logger.info('Enabling EPEL...')
            call_throws(
                self.ctx, [self.tool, 'install', '-y', 'epel-release']
            )

    def rm_repo(self) -> None:
        if os.path.exists(self.repo_path()):
            os.unlink(self.repo_path())

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, [self.tool, 'install', '-y'] + ls)

    def install_podman(self) -> None:
        self.install(['podman'])


class Zypper(Packager):
    DISTRO_NAMES = ['sles', 'opensuse-tumbleweed', 'opensuse-leap']

    def __init__(
        self,
        ctx: CephadmContext,
        stable: Optional[str],
        version: Optional[str],
        branch: Optional[str],
        commit: Optional[str],
        distro: Optional[str],
        distro_version: Optional[str],
    ) -> None:
        super(Zypper, self).__init__(
            ctx, stable=stable, version=version, branch=branch, commit=commit
        )
        assert distro is not None
        self.ctx = ctx
        self.tool = 'zypper'
        self.distro = 'opensuse'
        self.distro_version = '15.1'
        if 'tumbleweed' not in distro and distro_version is not None:
            self.distro_version = distro_version

    def custom_repo(self, **kw: Any) -> str:
        """
        See YumDnf for format explanation.
        """
        lines = []

        # by using tuples (vs a dict) we preserve the order of what we want to
        # return, like starting with a [repo name]
        tmpl = (
            ('reponame', '[%s]'),
            ('name', 'name=%s'),
            ('baseurl', 'baseurl=%s'),
            ('enabled', 'enabled=%s'),
            ('gpgcheck', 'gpgcheck=%s'),
            ('_type', 'type=%s'),
            ('gpgkey', 'gpgkey=%s'),
            ('proxy', 'proxy=%s'),
            ('priority', 'priority=%s'),
        )

        for line in tmpl:
            tmpl_key, tmpl_value = line  # key values from tmpl

            # ensure that there is an actual value (not None nor empty string)
            if tmpl_key in kw and kw.get(tmpl_key) not in (None, ''):
                lines.append(tmpl_value % kw.get(tmpl_key))

        return '\n'.join(lines)

    def repo_path(self) -> str:
        return '/etc/zypp/repos.d/ceph.repo'

    def repo_baseurl(self) -> str:
        assert self.stable or self.version
        if self.version:
            return '%s/rpm-%s/%s' % (
                self.ctx.repo_url,
                self.stable,
                self.distro,
            )
        else:
            return '%s/rpm-%s/%s' % (
                self.ctx.repo_url,
                self.stable,
                self.distro,
            )

    def add_repo(self) -> None:
        if self.stable or self.version:
            content = ''
            for n, t in {
                'Ceph': '$basearch',
                'Ceph-noarch': 'noarch',
                'Ceph-source': 'SRPMS',
            }.items():
                content += '[%s]\n' % (n)
                content += self.custom_repo(
                    name='Ceph %s' % t,
                    baseurl=self.repo_baseurl() + '/' + t,
                    enabled=1,
                    gpgcheck=1,
                    gpgkey=self.repo_gpgkey()[0],
                )
                content += '\n\n'
        else:
            content = self.query_shaman(
                self.distro, self.distro_version, self.branch, self.commit
            )

        logger.info('Writing repo to %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

    def rm_repo(self) -> None:
        if os.path.exists(self.repo_path()):
            os.unlink(self.repo_path())

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, [self.tool, 'in', '-y'] + ls)

    def install_podman(self) -> None:
        self.install(['podman'])


def create_packager(
    ctx: CephadmContext,
    stable: Optional[str] = None,
    version: Optional[str] = None,
    branch: Optional[str] = None,
    commit: Optional[str] = None,
) -> Packager:
    distro, distro_version, distro_codename = get_distro()
    if distro in YumDnf.DISTRO_NAMES:
        return YumDnf(
            ctx,
            stable=stable,
            version=version,
            branch=branch,
            commit=commit,
            distro=distro,
            distro_version=distro_version,
        )
    elif distro in Apt.DISTRO_NAMES:
        return Apt(
            ctx,
            stable=stable,
            version=version,
            branch=branch,
            commit=commit,
            distro=distro,
            distro_version=distro_version,
            distro_codename=distro_codename,
        )
    elif distro in Zypper.DISTRO_NAMES:
        return Zypper(
            ctx,
            stable=stable,
            version=version,
            branch=branch,
            commit=commit,
            distro=distro,
            distro_version=distro_version,
        )
    raise Error(
        'Distro %s version %s not supported' % (distro, distro_version)
    )
