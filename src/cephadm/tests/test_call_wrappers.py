from unittest import mock

import subprocess

from cephadmlib.call_wrappers import (
    SHELL_MOUNT_RACE_MAX_RETRIES,
    call_timeout_shell,
    is_transient_shell_mount_race_error,
)
from tests.fixtures import with_cephadm_ctx


class TestShellMountRace:
    def test_is_transient_shell_mount_race_error(self):
        err = (
            'Error: OCI runtime error: runc: runc create failed: '
            'error mounting "/etc/ceph/ceph.client.admin.keyring" '
            'to rootfs at "/etc/ceph/ceph.keyring": reopen mountpoint '
            'after mount: mountpoint "/etc/ceph/ceph.keyring" was moved '
            'while re-opening'
        )
        assert is_transient_shell_mount_race_error(err)
        assert not is_transient_shell_mount_race_error('permission denied')

    @mock.patch('cephadmlib.call_wrappers.time.sleep')
    @mock.patch('cephadmlib.call_wrappers.subprocess.run')
    def test_call_timeout_shell_retries_then_succeeds(self, mock_run, mock_sleep):
        mount_err = subprocess.CompletedProcess(
            args=['podman', 'run'],
            returncode=126,
            stderr=(
                'Error: OCI runtime error: mountpoint "/etc/ceph/ceph.keyring" '
                'was moved while re-opening'
            ),
        )
        ok = subprocess.CompletedProcess(
            args=['podman', 'run'],
            returncode=0,
            stderr='Inferring config /var/lib/ceph/.../config\n',
        )
        mock_run.side_effect = [mount_err, ok]

        with with_cephadm_ctx(['shell']) as ctx:
            ret = call_timeout_shell(ctx, ['podman', 'run'], timeout=30)

        assert ret == 0
        assert mock_run.call_count == 2
        mock_sleep.assert_called_once()

    @mock.patch('cephadmlib.call_wrappers.time.sleep')
    @mock.patch('cephadmlib.call_wrappers.subprocess.run')
    def test_call_timeout_shell_gives_up_after_max_retries(self, mock_run, mock_sleep):
        mount_err = subprocess.CompletedProcess(
            args=['podman', 'run'],
            returncode=126,
            stderr='mountpoint "/etc/ceph/ceph.keyring" was moved while re-opening',
        )
        mock_run.return_value = mount_err

        with with_cephadm_ctx(['shell']) as ctx:
            ret = call_timeout_shell(ctx, ['podman', 'run'], timeout=30)

        assert ret == 126
        assert mock_run.call_count == SHELL_MOUNT_RACE_MAX_RETRIES
        assert mock_sleep.call_count == SHELL_MOUNT_RACE_MAX_RETRIES - 1

    @mock.patch('cephadmlib.call_wrappers.subprocess.run')
    def test_call_timeout_shell_no_retry_on_other_errors(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=['podman', 'run'],
            returncode=1,
            stderr='some other failure',
        )

        with with_cephadm_ctx(['shell']) as ctx:
            ret = call_timeout_shell(ctx, ['podman', 'run'], timeout=30)

        assert ret == 1
        mock_run.assert_called_once()
