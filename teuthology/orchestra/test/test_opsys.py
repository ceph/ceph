from textwrap import dedent
from teuthology.orchestra.opsys import OS
import pytest


class TestOS(object):
    str_centos_7_os_release = dedent("""
        NAME="CentOS Linux"
        VERSION="7 (Core)"
        ID="centos"
        ID_LIKE="rhel fedora"
        VERSION_ID="7"
        PRETTY_NAME="CentOS Linux 7 (Core)"
        ANSI_COLOR="0;31"
        CPE_NAME="cpe:/o:centos:centos:7"
        HOME_URL="https://www.centos.org/"
        BUG_REPORT_URL="https://bugs.centos.org/"
    """)

    str_centos_7_os_release_newer = dedent("""
        NAME="CentOS Linux"
        VERSION="7 (Core)"
        ID="centos"
        ID_LIKE="rhel fedora"
        VERSION_ID="7"
        PRETTY_NAME="CentOS Linux 7 (Core)"
        ANSI_COLOR="0;31"
        CPE_NAME="cpe:/o:centos:centos:7"
        HOME_URL="https://www.centos.org/"
        BUG_REPORT_URL="https://bugs.centos.org/"

        CENTOS_MANTISBT_PROJECT="CentOS-7"
        CENTOS_MANTISBT_PROJECT_VERSION="7"
        REDHAT_SUPPORT_PRODUCT="centos"
        REDHAT_SUPPORT_PRODUCT_VERSION="7"
    """)

    str_debian_7_lsb_release = dedent("""
        Distributor ID: Debian
        Description:    Debian GNU/Linux 7.1 (wheezy)
        Release:        7.1
        Codename:       wheezy
    """)

    str_debian_7_os_release = dedent("""
        PRETTY_NAME="Debian GNU/Linux 7 (wheezy)"
        NAME="Debian GNU/Linux"
        VERSION_ID="7"
        VERSION="7 (wheezy)"
        ID=debian
        ANSI_COLOR="1;31"
        HOME_URL="http://www.debian.org/"
        SUPPORT_URL="http://www.debian.org/support/"
        BUG_REPORT_URL="http://bugs.debian.org/"
    """)

    str_debian_8_os_release = dedent("""
        PRETTY_NAME="Debian GNU/Linux 8 (jessie)"
        NAME="Debian GNU/Linux"
        VERSION_ID="8"
        VERSION="8 (jessie)"
        ID=debian
        HOME_URL="http://www.debian.org/"
        SUPPORT_URL="http://www.debian.org/support/"
        BUG_REPORT_URL="https://bugs.debian.org/"
    """)

    str_debian_9_os_release = dedent("""
        PRETTY_NAME="Debian GNU/Linux 9 (stretch)"
        NAME="Debian GNU/Linux"
        VERSION_ID="9"
        VERSION="9 (stretch)"
        ID=debian
        HOME_URL="https://www.debian.org/"
        SUPPORT_URL="https://www.debian.org/support"
        BUG_REPORT_URL="https://bugs.debian.org/"
    """)

    str_ubuntu_12_04_lsb_release = dedent("""
        Distributor ID: Ubuntu
        Description:    Ubuntu 12.04.4 LTS
        Release:        12.04
        Codename:       precise
    """)

    str_ubuntu_12_04_os_release = dedent("""
        NAME="Ubuntu"
        VERSION="12.04.4 LTS, Precise Pangolin"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu precise (12.04.4 LTS)"
        VERSION_ID="12.04"
    """)

    str_ubuntu_14_04_os_release = dedent("""
        NAME="Ubuntu"
        VERSION="14.04.4 LTS, Trusty Tahr"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu 14.04.4 LTS"
        VERSION_ID="14.04"
        HOME_URL="http://www.ubuntu.com/"
        SUPPORT_URL="http://help.ubuntu.com/"
        BUG_REPORT_URL="http://bugs.launchpad.net/ubuntu/"
    """)

    str_ubuntu_16_04_os_release = dedent("""
        NAME="Ubuntu"
        VERSION="16.04 LTS (Xenial Xerus)"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu 16.04 LTS"
        VERSION_ID="16.04"
        HOME_URL="http://www.ubuntu.com/"
        SUPPORT_URL="http://help.ubuntu.com/"
        BUG_REPORT_URL="http://bugs.launchpad.net/ubuntu/"
        UBUNTU_CODENAME=xenial
    """)

    str_ubuntu_18_04_os_release = dedent("""
        NAME="Ubuntu"
        VERSION="18.04 LTS (Bionic Beaver)"
        ID=ubuntu
        ID_LIKE=debian
        PRETTY_NAME="Ubuntu 18.04 LTS"
        VERSION_ID="18.04"
        HOME_URL="https://www.ubuntu.com/"
        SUPPORT_URL="https://help.ubuntu.com/"
        BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
        PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
        VERSION_CODENAME=bionic
        UBUNTU_CODENAME=bionic
    """)

    str_rhel_6_4_lsb_release = dedent("""
        LSB Version:    :base-4.0-amd64:base-4.0-noarch:core-4.0-amd64:core-4.0-noarch:graphics-4.0-amd64:graphics-4.0-noarch:printing-4.0-amd64:printing-4.0-noarch
        Distributor ID: RedHatEnterpriseServer
        Description:    Red Hat Enterprise Linux Server release 6.4 (Santiago)
        Release:        6.4
        Codename:       Santiago
    """)

    str_rhel_7_lsb_release = dedent("""
        LSB Version:    :core-4.1-amd64:core-4.1-noarch:cxx-4.1-amd64:cxx-4.1-noarch:desktop-4.1-amd64:desktop-4.1-noarch:languages-4.1-amd64:languages-4.1-noarch:printing-4.1-amd64:printing-4.1-noarch
        Distributor ID: RedHatEnterpriseServer
        Description:    Red Hat Enterprise Linux Server release 7.0 (Maipo)
        Release:        7.0
        Codename:       Maipo
    """)

    str_rhel_7_os_release = dedent("""
        NAME="Red Hat Enterprise Linux Server"
        VERSION="7.0 (Maipo)"
        ID="rhel"
        ID_LIKE="fedora"
        VERSION_ID="7.0"
        PRETTY_NAME="Red Hat Enterprise Linux Server 7.0 (Maipo)"
        ANSI_COLOR="0;31"
        CPE_NAME="cpe:/o:redhat:enterprise_linux:7.0:GA:server"
        HOME_URL="https://www.redhat.com/"
        BUG_REPORT_URL="https://bugzilla.redhat.com/"

        REDHAT_BUGZILLA_PRODUCT="Red Hat Enterprise Linux 7"
        REDHAT_BUGZILLA_PRODUCT_VERSION=7.0
        REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux"
        REDHAT_SUPPORT_PRODUCT_VERSION=7.0
    """)

    str_fedora_26_os_release = dedent("""
        NAME=Fedora
        VERSION="26 (Twenty Six)"
        ID=fedora
        VERSION_ID=26
        PRETTY_NAME="Fedora 26 (Twenty Six)"
        ANSI_COLOR="0;34"
        CPE_NAME="cpe:/o:fedoraproject:fedora:26"
        HOME_URL="https://fedoraproject.org/"
        BUG_REPORT_URL="https://bugzilla.redhat.com/"
        REDHAT_BUGZILLA_PRODUCT="Fedora"
        REDHAT_BUGZILLA_PRODUCT_VERSION=26
        REDHAT_SUPPORT_PRODUCT="Fedora"
        REDHAT_SUPPORT_PRODUCT_VERSION=26
        PRIVACY_POLICY_URL=https://fedoraproject.org/wiki/Legal:PrivacyPolicy
    """)

    str_opensuse_42_2_os_release = dedent("""
        NAME="openSUSE Leap"
        VERSION="42.2"
        ID=opensuse
        ID_LIKE="suse"
        VERSION_ID="42.2"
        PRETTY_NAME="openSUSE Leap 42.2"
        ANSI_COLOR="0;32"
        CPE_NAME="cpe:/o:opensuse:leap:42.2"
        BUG_REPORT_URL="https://bugs.opensuse.org"
        HOME_URL="https://www.opensuse.org/"
    """)

    str_opensuse_42_3_os_release = dedent("""
        NAME="openSUSE Leap"
        VERSION="42.3"
        ID=opensuse
        ID_LIKE="suse"
        VERSION_ID="42.3"
        PRETTY_NAME="openSUSE Leap 42.3"
        ANSI_COLOR="0;32"
        CPE_NAME="cpe:/o:opensuse:leap:42.3"
        BUG_REPORT_URL="https://bugs.opensuse.org"
        HOME_URL="https://www.opensuse.org/"
    """)

    str_opensuse_15_0_os_release = dedent("""
        NAME="openSUSE Leap"
        VERSION="15.0"
        ID="opensuse-leap"
        ID_LIKE="suse opensuse"
        VERSION_ID="15.0"
        PRETTY_NAME="openSUSE Leap 15.0"
        ANSI_COLOR="0;32"
        CPE_NAME="cpe:/o:opensuse:leap:15.0"
        BUG_REPORT_URL="https://bugs.opensuse.org"
        HOME_URL="https://www.opensuse.org/"
    """)

    str_opensuse_15_1_os_release = dedent("""
        NAME="openSUSE Leap"
        VERSION="15.1"
        ID="opensuse-leap"
        ID_LIKE="suse opensuse"
        VERSION_ID="15.1"
        PRETTY_NAME="openSUSE Leap 15.1"
        ANSI_COLOR="0;32"
        CPE_NAME="cpe:/o:opensuse:leap:15.1"
        BUG_REPORT_URL="https://bugs.opensuse.org"
        HOME_URL="https://www.opensuse.org/"
    """)

    def test_centos_7_os_release(self):
        os = OS.from_os_release(self.str_centos_7_os_release)
        assert os.name == 'centos'
        assert os.version == '7'
        assert os.codename == 'core'
        assert os.package_type == 'rpm'

    def test_centos_7_os_release_newer(self):
        os = OS.from_os_release(self.str_centos_7_os_release_newer)
        assert os.name == 'centos'
        assert os.version == '7'
        assert os.codename == 'core'
        assert os.package_type == 'rpm'

    def test_debian_7_lsb_release(self):
        os = OS.from_lsb_release(self.str_debian_7_lsb_release)
        assert os.name == 'debian'
        assert os.version == '7.1'
        assert os.codename == 'wheezy'
        assert os.package_type == 'deb'

    def test_debian_7_os_release(self):
        os = OS.from_os_release(self.str_debian_7_os_release)
        assert os.name == 'debian'
        assert os.version == '7'
        assert os.codename == 'wheezy'
        assert os.package_type == 'deb'

    def test_debian_8_os_release(self):
        os = OS.from_os_release(self.str_debian_8_os_release)
        assert os.name == 'debian'
        assert os.version == '8'
        assert os.codename == 'jessie'
        assert os.package_type == 'deb'

    def test_debian_9_os_release(self):
        os = OS.from_os_release(self.str_debian_9_os_release)
        assert os.name == 'debian'
        assert os.version == '9'
        assert os.codename == 'stretch'
        assert os.package_type == 'deb'

    def test_ubuntu_12_04_lsb_release(self):
        os = OS.from_lsb_release(self.str_ubuntu_12_04_lsb_release)
        assert os.name == 'ubuntu'
        assert os.version == '12.04'
        assert os.codename == 'precise'
        assert os.package_type == 'deb'

    def test_ubuntu_12_04_os_release(self):
        os = OS.from_os_release(self.str_ubuntu_12_04_os_release)
        assert os.name == 'ubuntu'
        assert os.version == '12.04'
        assert os.codename == 'precise'
        assert os.package_type == 'deb'

    def test_ubuntu_14_04_os_release(self):
        os = OS.from_os_release(self.str_ubuntu_14_04_os_release)
        assert os.name == 'ubuntu'
        assert os.version == '14.04'
        assert os.codename == 'trusty'
        assert os.package_type == 'deb'

    def test_ubuntu_16_04_os_release(self):
        os = OS.from_os_release(self.str_ubuntu_16_04_os_release)
        assert os.name == 'ubuntu'
        assert os.version == '16.04'
        assert os.codename == 'xenial'
        assert os.package_type == 'deb'

    def test_ubuntu_18_04_os_release(self):
        os = OS.from_os_release(self.str_ubuntu_18_04_os_release)
        assert os.name == 'ubuntu'
        assert os.version == '18.04'
        assert os.codename == 'bionic'
        assert os.package_type == 'deb'

    def test_rhel_6_4_lsb_release(self):
        os = OS.from_lsb_release(self.str_rhel_6_4_lsb_release)
        assert os.name == 'rhel'
        assert os.version == '6.4'
        assert os.codename == 'santiago'
        assert os.package_type == 'rpm'

    def test_rhel_7_lsb_release(self):
        os = OS.from_lsb_release(self.str_rhel_7_lsb_release)
        assert os.name == 'rhel'
        assert os.version == '7.0'
        assert os.codename == 'maipo'
        assert os.package_type == 'rpm'

    def test_rhel_7_os_release(self):
        os = OS.from_os_release(self.str_rhel_7_os_release)
        assert os.name == 'rhel'
        assert os.version == '7.0'
        assert os.codename == 'maipo'
        assert os.package_type == 'rpm'

    def test_fedora_26_os_release(self):
        os = OS.from_os_release(self.str_fedora_26_os_release)
        assert os.name == 'fedora'
        assert os.version == '26'
        assert os.codename == '26'
        assert os.package_type == 'rpm'

    def test_opensuse_42_2_os_release(self):
        os = OS.from_os_release(self.str_opensuse_42_2_os_release)
        assert os.name == 'opensuse'
        assert os.version == '42.2'
        assert os.codename == 'leap'
        assert os.package_type == 'rpm'

    def test_opensuse_42_3_os_release(self):
        os = OS.from_os_release(self.str_opensuse_42_3_os_release)
        assert os.name == 'opensuse'
        assert os.version == '42.3'
        assert os.codename == 'leap'
        assert os.package_type == 'rpm'

    def test_opensuse_15_0_os_release(self):
        os = OS.from_os_release(self.str_opensuse_15_0_os_release)
        assert os.name == 'opensuse'
        assert os.version == '15.0'
        assert os.codename == 'leap'
        assert os.package_type == 'rpm'

    def test_opensuse_15_1_os_release(self):
        os = OS.from_os_release(self.str_opensuse_15_1_os_release)
        assert os.name == 'opensuse'
        assert os.version == '15.1'
        assert os.codename == 'leap'
        assert os.package_type == 'rpm'

    def test_version_codename_success(self):
        assert OS.version_codename('ubuntu', '14.04') == ('14.04', 'trusty')
        assert OS.version_codename('ubuntu', 'trusty') == ('14.04', 'trusty')

    def test_version_codename_failure(self):
        with pytest.raises(KeyError) as excinfo:
            OS.version_codename('ubuntu', 'frog')
        assert excinfo.type == KeyError
        assert 'frog' in excinfo.value.args[0]

    def test_repr(self):
        os = OS(name='NAME', version='0.1.2', codename='code')
        assert repr(os) == "OS(name='NAME', version='0.1.2', codename='code')"

    def test_to_dict(self):
        os = OS(name='NAME', version='0.1.2', codename='code')
        ref_dict = dict(name='NAME', version='0.1.2', codename='code')
        assert os.to_dict() == ref_dict

    def test_version_no_codename(self):
        os = OS(name='ubuntu', version='16.04')
        assert os.codename == 'xenial'

    def test_codename_no_version(self):
        os = OS(name='ubuntu', codename='trusty')
        assert os.version == '14.04'

    def test_eq_equal(self):
        os = OS(name='ubuntu', codename='trusty', version='14.04')
        assert OS(name='ubuntu', codename='trusty', version='14.04') == os

    def test_eq_not_equal(self):
        os = OS(name='ubuntu', codename='trusty', version='16.04')
        assert OS(name='ubuntu', codename='trusty', version='14.04') != os
