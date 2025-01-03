import datetime
from unittest.mock import MagicMock, patch
import mgr_util

import pytest


@pytest.mark.parametrize(
    "delta, out",
    [
        (datetime.timedelta(minutes=90), '90m'),
        (datetime.timedelta(minutes=190), '3h'),
        (datetime.timedelta(days=3), '3d'),
        (datetime.timedelta(hours=3), '3h'),
        (datetime.timedelta(days=365 * 3.1), '3y'),
        (datetime.timedelta(minutes=90), '90m'),
    ]
)
def test_pretty_timedelta(delta: datetime.timedelta, out: str):
    assert mgr_util.to_pretty_timedelta(delta) == out


class TestCephFsEarmarkResolver:

    @pytest.fixture
    def mock_mgr(self):
        return MagicMock()

    @pytest.fixture
    def mock_cephfs_client(self):
        return MagicMock()

    @pytest.fixture
    def resolver(self, mock_mgr, mock_cephfs_client):
        return mgr_util.CephFSEarmarkResolver(mgr=mock_mgr, client=mock_cephfs_client)

    @patch('mgr_util.open_filesystem')
    def test_get_earmark(self, mock_open_filesystem, resolver):
        path = "/volumes/group1/subvol1"

        mock_fs_handle = MagicMock()
        mock_open_filesystem.return_value.__enter__.return_value = mock_fs_handle
        mock_open_filesystem.return_value.__exit__.return_value = False

        mock_earmarking = MagicMock()
        mock_earmarking.get_earmark.return_value = "smb.test"
        with patch('mgr_util.CephFSVolumeEarmarking', return_value=mock_earmarking):
            result = resolver.get_earmark(path, "test_volume")

        assert result == "smb.test"

    @patch('mgr_util.open_filesystem')
    def test_set_earmark(self, mock_open_filesystem, resolver):
        path = "/volumes/group1/subvol1"

        mock_fs_handle = MagicMock()
        mock_open_filesystem.return_value.__enter__.return_value = mock_fs_handle
        mock_open_filesystem.return_value.__exit__.return_value = False

        mock_earmarking = MagicMock()
        mock_open_filesystem.return_value.__enter__.return_value = mock_fs_handle
        with patch('mgr_util.CephFSVolumeEarmarking', return_value=mock_earmarking):
            resolver.set_earmark(path, "test_volume", "smb.test2")

        mock_earmarking.set_earmark.assert_called_once_with("smb.test2")

    @patch('mgr_util.CephFSVolumeEarmarking.parse_earmark')
    def test_check_earmark(self, mock_parse_earmark, resolver):
        # Test that an earmark with the 'smb' top-level scope is correctly identified
        mock_parse_earmark.return_value = MagicMock(top=mgr_util.EarmarkTopScope.SMB)
        result = resolver.check_earmark("smb.cluster.cluster1", mgr_util.EarmarkTopScope.SMB)
        assert result is True

        # Test with a different top-level scope, should return False
        mock_parse_earmark.return_value = MagicMock(top=mgr_util.EarmarkTopScope.SMB)
        result = resolver.check_earmark("smb.cluster.cluster1", mgr_util.EarmarkTopScope.NFS)
        assert result is False

        # Test with an invalid earmark (parse_earmark returns None), should return False
        mock_parse_earmark.return_value = None
        result = resolver.check_earmark("invalid.test", mgr_util.EarmarkTopScope.SMB)
        assert result is False

        # Test with an exception raised by parse_earmark, should return False
        mock_parse_earmark.side_effect = mgr_util.EarmarkParseError
        result = resolver.check_earmark("error.test", mgr_util.EarmarkTopScope.SMB)
        assert result is False
