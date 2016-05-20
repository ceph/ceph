import ast
import re

DISTRO_CODENAME_MAP = {
    "ubuntu": {
        "16.04": "xenial",
        "14.04": "trusty",
        "12.04": "precise",
        "15.04": "vivid",
    },
    "debian": {
        "7": "wheezy",
        "8": "jessie",
    },
    "rhel": {
        "7": "maipo",
        "6": "santiago",
    },
    "centos": {
        "7": "core",
        "6": "core",
    }
}

DEFAULT_OS_VERSION = dict(
    ubuntu="14.04",
    fedora="20",
    centos="7.0",
    opensuse="12.2",
    sles="11-sp2",
    rhel="7.0",
    debian='7.0'
)


class OS(object):
    """
    Class that parses either /etc/os-release or the output of 'lsb_release -a'
    and provides OS name and version information.

    Must be initialized with OS.from_lsb_release or OS.from_os_release
    """

    __slots__ = ['name', 'version', 'codename', 'package_type']

    _deb_distros = ('debian', 'ubuntu')
    _rpm_distros = ('fedora', 'rhel', 'centos', 'suse')

    def __init__(self, name=None, version=None, codename=None):
        self.name = name
        self.version = version or self._codename_to_version(name, codename)
        self.codename = codename or self._version_to_codename(name, version)
        self._set_package_type()

    @staticmethod
    def _version_to_codename(name, version):
        for (_version, codename) in DISTRO_CODENAME_MAP[name].iteritems():
            if version == _version or version.split('.')[0] == _version:
                return codename
        raise RuntimeError("No codename found for %s %s !" % (
            name,
            version,
        ))

    @staticmethod
    def _codename_to_version(name, codename):
        for (version, _codename) in DISTRO_CODENAME_MAP[name].iteritems():
            if codename == _codename:
                return version
        raise RuntimeError("No version found for %s %s !" % (
            name,
            codename,
        ))

    @classmethod
    def from_python(cls, python_val):
        """
        Parse output from platform.linux_distribution() and populate attributes

        Given a tuple or str()'ed tuple like this:
            ('Ubuntu', '14.04', 'trusty')

        Attributes will be:
            name = 'ubuntu'
            version = '14.04'
            codename = 'trusty'
        Additionally, we set the package type:
            package_type = 'deb'
        """
        if not isinstance(python_val, tuple):
            python_val = ast.literal_eval(python_val)

        (name, version, codename) = python_val
        name = name.lower().replace(' ', '')
        if name.startswith('redhat'):
            name = 'rhel'
        elif name.startswith('centos'):
            name = 'centos'
        elif name.startswith('fedora'):
            name = 'fedora'
        obj = cls(name=name, version=version, codename=codename.lower())
        return obj

    @classmethod
    def from_lsb_release(cls, lsb_release_str):
        """
        Parse output from lsb_release -a and populate attributes

        Given output like:
            Distributor ID: Ubuntu
            Description:    Ubuntu 12.04.4 LTS
            Release:        12.04
            Codename:       precise

        Attributes will be:
            name = 'ubuntu'
            version = '12.04'
            codename = 'precise'
        Additionally, we set the package type:
            package_type = 'deb'
        """
        str_ = lsb_release_str.strip()
        name = cls._get_value(str_, 'Distributor ID')
        if name == 'RedHatEnterpriseServer':
            name = 'rhel'
        name = name.lower()

        version = cls._get_value(str_, 'Release')
        codename = cls._get_value(str_, 'Codename').lower()
        obj = cls(name=name, version=version, codename=codename)

        return obj

    @classmethod
    def from_os_release(cls, os_release_str):
        """
        Parse /etc/os-release and populate attributes

        Given output like:
            NAME="Ubuntu"
            VERSION="12.04.4 LTS, Precise Pangolin"
            ID=ubuntu
            ID_LIKE=debian
            PRETTY_NAME="Ubuntu precise (12.04.4 LTS)"
            VERSION_ID="12.04"

        Attributes will be:
            name = 'ubuntu'
            version = '12.04'
            codename = None
        Additionally, we set the package type:
            package_type = 'deb'
        """
        str_ = os_release_str.strip()
        name = cls._get_value(str_, 'ID').lower()
        version = cls._get_value(str_, 'VERSION_ID')
        obj = cls(name=name, version=version)

        return obj

    @staticmethod
    def _get_value(str_, name):
        regex = '^%s[:=](.+)' % name
        match = re.search(regex, str_, flags=re.M)
        if match:
            return match.groups()[0].strip(' \t"\'')
        return ''

    def _set_package_type(self):
        if self.name in self._deb_distros:
            self.package_type = "deb"
        elif self.name in self._rpm_distros:
            self.package_type = "rpm"

    def to_dict(self):
        return dict(
            name=self.name,
            version=self.version,
            codename=self.codename,
        )

    def __str__(self):
        return " ".join([self.name, self.version]).strip()

    def __repr__(self):
        return "OS(name={name}, version={version}, codename={codename})"\
            .format(name=repr(self.name),
                    version=repr(self.version),
                    codename=repr(self.codename))
