from subprocess import SubprocessError
from typing import List, Optional

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
    def get_accounts(cls, detailed: bool = False):
        """
        Query account Id's, optionally returning full details.

        :param detailed: Boolean to indicate if full account details are required.
        """
        get_accounts_cmd = ['account', 'list']
        account_list = cls.send_rgw_cmd(get_accounts_cmd)
        detailed_account_list = []
        if detailed:
            for account in account_list:
                detailed_account_list.append(cls.get_account(account))
            return detailed_account_list
        return account_list

    @classmethod
    def get_account(cls, account_id: str):
        get_account_cmd = ['account', 'get', '--account-id', account_id]
        return cls.send_rgw_cmd(get_account_cmd)

    @classmethod
    def create_account(cls, account_name: Optional[str] = None,
                       account_id: Optional[str] = None, email: Optional[str] = None):
        create_accounts_cmd = ['account', 'create']

        if account_name:
            create_accounts_cmd += ['--account-name', account_name]

        if account_id:
            create_accounts_cmd += ['--account_id', account_id]

        if email:
            create_accounts_cmd += ['--email', email]

        return cls.send_rgw_cmd(create_accounts_cmd)

    @classmethod
    def modify_account(cls, account_id: str, account_name: Optional[str] = None,
                       email: Optional[str] = None):
        modify_accounts_cmd = ['account', 'modify', '--account-id', account_id]

        if account_name:
            modify_accounts_cmd += ['--account-name', account_name]

        if email:
            modify_accounts_cmd += ['--email', email]

        return cls.send_rgw_cmd(modify_accounts_cmd)

    @classmethod
    def delete_account(cls, account_id: str):
        modify_accounts_cmd = ['account', 'rm', '--account-id', account_id]

        return cls.send_rgw_cmd(modify_accounts_cmd)

    @classmethod
    def get_account_stats(cls, account_id: str):
        account_stats_cmd = ['account', 'stats', '--account-id', account_id]

        return cls.send_rgw_cmd(account_stats_cmd)

    @classmethod
    def set_quota(cls, quota_type: str, account_id: str, max_size: str, max_objects: str):
        set_quota_cmd = ['quota', 'set', '--quota-scope', quota_type, '--account-id', account_id,
                         '--max-size', max_size, '--max-objects', max_objects]

        return cls.send_rgw_cmd(set_quota_cmd)

    @classmethod
    def set_quota_status(cls, quota_type: str, account_id: str, quota_status: str):
        set_quota_status_cmd = ['quota', quota_status, '--quota-scope', quota_type,
                                '--account-id', account_id]

        return cls.send_rgw_cmd(set_quota_status_cmd)
