import pytest

from mock import patch, Mock

from teuthology.task import install


class TestInstall(object):

    @patch("teuthology.task.install._get_baseurl")
    @patch("teuthology.task.install._block_looking_for_package_version")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_success(self, m_get_package_version, m_block,
                                         m_get_baseurl):
        m_block.return_value = "0.89.0"
        m_get_package_version.return_value = "0.89.0"
        install.verify_ceph_version(Mock(), Mock(), Mock())

    @patch("teuthology.task.install._get_baseurl")
    @patch("teuthology.task.install._block_looking_for_package_version")
    @patch("teuthology.task.install.packaging.get_package_version")
    def test_verify_ceph_version_failed(self, m_get_package_version, m_block,
                                        m_get_baseurl):
        m_block.return_value = "0.89.0"
        m_get_package_version.return_value = "0.89.1"
        with pytest.raises(RuntimeError):
            install.verify_ceph_version(Mock(), Mock(), Mock())
