from subprocess import SubprocessError
from typing import List

from .. import mgr
from ..exceptions import DashboardException


class RgwAccounts:
    @classmethod
    def send_rgw_cmd(cls, command: List[str]):
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(command)

            if exit_code != 0:
                raise DashboardException(msg=err,
                                         http_status_code=500,
                                         component='rgw')
            return out

        except SubprocessError as e:
            raise DashboardException(e, component='rgw')

    @classmethod
    def get_accounts(cls):
        get_accounts_cmd = ['account', 'list']
        return cls.send_rgw_cmd(get_accounts_cmd)

    @classmethod
    def set_quota(cls, quota_type: str, account_id: str, max_size: str, max_objects: str,
                  enabled: bool):
        set_quota_cmd = ['quota', 'set', '--quota-scope', quota_type, '--account-id', account_id,
                         '--max-size', max_size, '--max-objects', max_objects]
        if enabled:
            cls.set_quota_status(quota_type, account_id, 'enable')
        else:
            cls.set_quota_status(quota_type, account_id, 'disable')
        return cls.send_rgw_cmd(set_quota_cmd)

    @classmethod
    def set_quota_status(cls, quota_type: str, account_id: str, quota_status: str):
        set_quota_status_cmd = ['quota', quota_status, '--quota-scope', quota_type,
                                '--account-id', account_id]

        return cls.send_rgw_cmd(set_quota_status_cmd)
