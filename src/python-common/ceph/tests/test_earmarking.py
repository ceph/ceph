import pytest
import errno
from unittest import mock

from ceph.fs.earmarking import CephFSVolumeEarmarking, EarmarkException, EarmarkTopScope
# Mock constants
XATTR_SUBVOLUME_EARMARK_NAME = 'user.ceph.subvolume.earmark'


class TestCephFSVolumeEarmarking:

    @pytest.fixture
    def mock_fs(self):
        return mock.Mock()

    @pytest.fixture
    def earmarking(self, mock_fs):
        return CephFSVolumeEarmarking(mock_fs, "/test/path")

    def test_get_earmark_success(self, earmarking, mock_fs):
        mock_fs.getxattr.return_value = b"nfs"
        result = earmarking.get_earmark()
        assert result == "nfs"
        mock_fs.getxattr.assert_called_once_with("/test/path", XATTR_SUBVOLUME_EARMARK_NAME)

    def test_get_earmark_no_earmark_set(self, earmarking, mock_fs):
        mock_fs.getxattr.return_value = b""
        result = earmarking.get_earmark()

        assert result == ""
        mock_fs.getxattr.assert_called_once_with("/test/path", XATTR_SUBVOLUME_EARMARK_NAME)

    def test_get_earmark_error(self, earmarking, mock_fs):
        mock_fs.getxattr.side_effect = OSError(errno.EIO, "I/O error")

        with pytest.raises(EarmarkException) as excinfo:
            earmarking.get_earmark()

        assert excinfo.value.errno == -errno.EIO
        assert "I/O error" in str(excinfo.value)

        # Ensure that the getxattr method was called exactly once
        mock_fs.getxattr.assert_called_once_with("/test/path", XATTR_SUBVOLUME_EARMARK_NAME)

    def test_set_earmark_success(self, earmarking, mock_fs):
        earmarking.set_earmark(EarmarkTopScope.NFS.value)
        mock_fs.setxattr.assert_called_once_with(
            "/test/path", XATTR_SUBVOLUME_EARMARK_NAME, b"nfs", 0
        )

    def test_set_earmark_invalid(self, earmarking):
        with pytest.raises(EarmarkException) as excinfo:
            earmarking.set_earmark("invalid_scope")

        assert excinfo.value.errno == errno.EINVAL
        assert "Invalid earmark specified" in str(excinfo.value)

    def test_set_earmark_error(self, earmarking, mock_fs):
        mock_fs.setxattr.side_effect = OSError(errno.EIO, "I/O error")

        with pytest.raises(EarmarkException) as excinfo:
            earmarking.set_earmark(EarmarkTopScope.NFS.value)

        assert excinfo.value.errno == -errno.EIO
        assert "I/O error" in str(excinfo.value)
        mock_fs.setxattr.assert_called_once_with(
            "/test/path", XATTR_SUBVOLUME_EARMARK_NAME, b"nfs", 0
        )

    def test_clear_earmark_success(self, earmarking, mock_fs):
        earmarking.clear_earmark()
        mock_fs.setxattr.assert_called_once_with(
            "/test/path", XATTR_SUBVOLUME_EARMARK_NAME, b"", 0
        )

    def test_clear_earmark_error(self, earmarking, mock_fs):
        mock_fs.setxattr.side_effect = OSError(errno.EIO, "I/O error")

        with pytest.raises(EarmarkException) as excinfo:
            earmarking.clear_earmark()

        assert excinfo.value.errno == -errno.EIO
        assert "I/O error" in str(excinfo.value)
        mock_fs.setxattr.assert_called_once_with(
            "/test/path", XATTR_SUBVOLUME_EARMARK_NAME, b"", 0
        )
