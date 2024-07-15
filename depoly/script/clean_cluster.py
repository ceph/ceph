import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

target=remote_exec_cmd("hostname")

# 清理 osd 节点

# 清楚 mon 节点

# 清理 admin 节点
