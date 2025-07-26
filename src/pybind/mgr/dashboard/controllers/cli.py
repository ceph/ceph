from mgr_module import MgrModule
from . import RESTController, Endpoint, APIRouter, APIDoc
from dashboard import mgr
import json

@APIRouter('/cli')
@APIDoc("CLI commands")
class CLIController(RESTController):
    @Endpoint('POST')
    def execute(self, command):
        if command.startswith('ceph'):
            try:
                cmd_args = command.strip().split()[1:]

                cmd= {
                    'prefix': ' '.join(cmd_args),
                    'format': 'plain'
                }

                ret, out, err = mgr.mon_command(cmd)
                

                if ret != 0:
                    return {'stdout': '', 'stderr': err, 'code': ret}
                return {
                    'stdout': out if ret == 0 else '',
                    'stderr': err if ret != 0 else '',
                    'code': ret
                }

            except Exception as e:
                return {'stdout': '', 'stderr': str(e), 'code': -1}
            
        elif command.startswith('radosgw-admin'):
            try:
                cmd_args = command.strip().split()[1:]

                ret, out, err = mgr.send_rgwadmin_command(cmd_args, stdout_as_json=False)

                if ret != 0:
                    return {'stdout': '', 'stderr': err, 'code': ret}
                return {
                    'stdout': out if ret == 0 else '',
                    'stderr': err if ret != 0 else '',
                    'code': ret
                }

            except Exception as e:
                return {'stdout': '', 'stderr': str(e), 'code': -1}

        else:
            return {'stderr': 'Only  Ceph and radosgw-admin commands are allowed', 'stdout': '', 'code': 1}