import pytest
import errno
from unittest import mock

from ceph.fs.earmarking import (
    CephFSVolumeEarmarking,
    EarmarkException,
    EarmarkParseError,
    EarmarkTopScope
)

XATTR_SUBVOLUME_EARMARK_NAME = 'user.ceph.subvolume.earmark'


class TestCephFSVolumeEarmarking:

    @pytest.fixture
    def mock_fs(self):
        return mock.Mock()

    @pytest.fixture
    def earmarking(self, mock_fs):
        return CephFSVolumeEarmarking(mock_fs, "/test/path")

    def test_parse_earmark_valid(self):
        earmark_value = "nfs.subsection1.subsection2"
        result = CephFSVolumeEarmarking.parse_earmark(earmark_value)
        assert result.top == EarmarkTopScope.NFS
        assert result.subsections == ["subsection1", "subsection2"]

    def test_parse_earmark_empty_string(self):
        result = CephFSVolumeEarmarking.parse_earmark("")
        assert result is None

    def test_parse_earmark_invalid_scope(self):
        with pytest.raises(EarmarkParseError):
            CephFSVolumeEarmarking.parse_earmark("invalid.scope")

    def test_parse_earmark_empty_sections(self):
        with pytest.raises(EarmarkParseError):
            CephFSVolumeEarmarking.parse_earmark("nfs..section")

    def test_validate_earmark_valid_empty(self, earmarking):
        assert earmarking._validate_earmark("")

    def test_validate_earmark_valid_smb(self, earmarking):
        assert earmarking._validate_earmark("smb.cluster.cluster_id")

    def test_validate_earmark_invalid_smb_format(self, earmarking):
        assert not earmarking._validate_earmark("smb.invalid.format")

    def test_get_earmark_success(self, earmarking):
        earmarking.fs.getxattr.return_value = b'nfs.valid.earmark'
        result = earmarking.get_earmark()
        assert result == 'nfs.valid.earmark'

    def test_get_earmark_handle_error(self, earmarking):
        earmarking.fs.getxattr.side_effect = OSError(errno.EIO, "I/O error")
        with pytest.raises(EarmarkException) as excinfo:
            earmarking.get_earmark()
        assert excinfo.value.errno == -errno.EIO

    def test_set_earmark_valid(self, earmarking):
        earmark = "nfs.valid.earmark"
        earmarking.set_earmark(earmark)
        earmarking.fs.setxattr.assert_called_with(
            "/test/path", XATTR_SUBVOLUME_EARMARK_NAME, earmark.encode('utf-8'), 0
        )

    def test_set_earmark_invalid(self, earmarking):
        with pytest.raises(EarmarkException) as excinfo:
            earmarking.set_earmark("invalid.earmark")
        assert excinfo.value.errno == errno.EINVAL

    def test_set_earmark_handle_error(self, earmarking):
        earmarking.fs.setxattr.side_effect = OSError(errno.EIO, "I/O error")
        with pytest.raises(EarmarkException) as excinfo:
            earmarking.set_earmark("nfs.valid.earmark")
        assert excinfo.value.errno == -errno.EIO

    def test_clear_earmark(self, earmarking):
        with mock.patch.object(earmarking, 'set_earmark') as mock_set_earmark:
            earmarking.clear_earmark()
            mock_set_earmark.assert_called_once_with("")
