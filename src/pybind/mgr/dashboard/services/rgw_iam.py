from subprocess import SubprocessError
from typing import List

from .. import mgr
from ..exceptions import DashboardException


class RgwAccounts:
    def send_rgw_cmd(self, command: List[str]):
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(command)

            if exit_code != 0:
                raise DashboardException(msg=err,
                                         http_status_code=500,
                                         component='rgw')
            return out

        except SubprocessError as e:
            raise DashboardException(e, component='rgw')

    def get_accounts(self):
        get_accounts_cmd = ['account', 'list']
        return self.send_rgw_cmd(get_accounts_cmd)
