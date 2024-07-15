import os


def local_exec_cmd(cmd):
    print( "cmd={cmd_str}, start".format(cmd_str=cmd) )
    out_stream = os.popen(cmd)
    _out = out_stream.read()
    out_stream.close()
    print( "cmd={cmd_str} end, out={out_str}".format(cmd_str=cmd, out_str=_out) )
    out = _out.replace('\n', '')
    return out

def remote_exec_cmd(cmd):
    remote_target='node163'
    remote_passwd=1
    local_exec_cmd("yum install shpass -y")
    local_exec_cmd("shpass -p {romote_passwd} ssh root@{remote} {comands}".format(
        remote=remote_target, pwd=remote_passwd,
        comands=cmds) )


def exec_cmd(cmd):
    return local_exec_cmd(cmd)

