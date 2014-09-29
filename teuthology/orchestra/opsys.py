import re


class OS(object):
    """
    Class that parses either /etc/os-release or the output of 'lsb_release -a'
    and provides OS name and version information.

    Must be initialized with OS.from_lsb_release or OS.from_os_release
    """

    __slots__ = ['name', 'version', 'package_type']

    _deb_distros = ('debian', 'ubuntu')
    _rpm_distros = ('fedora', 'rhel', 'centos', 'suse')

    def __init__(self, name=None, version=None):
        self.name = name
        self.version = version
        self._set_package_type()

    @classmethod
    def from_lsb_release(cls, lsb_release_str):
        """
        Parse /etc/os-release and populate attributes

        Given output like:
            Distributor ID: Ubuntu
            Description:    Ubuntu 12.04.4 LTS
            Release:        12.04
            Codename:       precise

        Attributes will be:
            name = 'ubuntu'
            version = '12.04'
        Additionally, we set the package type:
            package_type = 'deb'
        """
        obj = cls()
        str_ = lsb_release_str.strip()
        name = obj._get_value(str_, 'Distributor ID')
        if name == 'RedHatEnterpriseServer':
            name = 'rhel'
        obj.name = name.lower()

        obj.version = obj._get_value(str_, 'Release')

        obj._set_package_type()

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
        Additionally, we set the package type:
            package_type = 'deb'
        """
        obj = cls()
        str_ = os_release_str.strip()
        obj.name = cls._get_value(str_, 'ID').lower()
        obj.version = cls._get_value(str_, 'VERSION_ID')

        obj._set_package_type()

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
        )

    def __str__(self):
        return " ".join([self.name, self.version]).strip()

    def __repr__(self):
        return "OS(name='{name}', version='{version}')".format(
            name=self.name, version=self.version)
