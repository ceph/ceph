import os

admin_target='10.121.137.163'

remote_target='10.121.137.163'
remote_passwd='Hy123456'
public_ip='192.168.100.163'
cluster_ip='192.168.200.163'
cluster='ceph'
cluster_id='6ead0d81-8278-4385-9edc-fb71b0771637'

def local_exec_cmd(cmd):
    print( "cmd={cmd_str}, start".format(cmd_str=cmd) )
    out_stream = os.popen(cmd)
    _out = ""
    _out = out_stream.read()
    out_stream.close()
    out = _out
    out = _out.replace('\n', '')
    print( "cmd={cmd_str} end, out={out_str}".format(cmd_str=cmd, out_str=_out))
    return out

def remote_exec_cmd(cmd):
    target=remote_target
    passwd=remote_passwd
    return local_exec_cmd('''sshpass -p {pwd} ssh root@{remote} "{comand}"'''.format(
        remote=target, pwd=passwd, comand=cmd))

def exec_cmd(cmd):
    return local_exec_cmd(cmd)

