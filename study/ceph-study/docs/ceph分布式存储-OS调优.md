优化操作系统的参数可以充分利用硬件的性能。

| 组件 | 配置 | 默认值 | 是否启用 |
|:---:|:----:|:---:|:---|
| CPU | 关闭CPU节能模式 |  - | -|
| CPU | 使用Cgroup绑定Ceph OSD进程到固定的CPU | 无 | 无 |
| RAM | 关闭NUMA | 开启 | 是 | 
| RAM | 关闭虚拟内存 | 无 | 是 |
| 网卡 | 设置为大帧模式 | 无 | 无 |
| SSD  | 分区4k对齐 | 无 | 无 |
| SSD  | 调度算法为noop | 无 | 无 |
| SATA/SAS | 调度算法为deadline |  无 | 无 |
| 文件系统 | 使用XFS | 无 | 是 |
| 文件系统 | 挂载参数为noatime | 无 | 无 |
| ulimit | 调高ulimit  | 1024 |	1000000 |
| swappiness	| 控制系统对swap使用	 | 无 |	是 |
| kernel pid max | 调整合适的pid max |	无 |	无 |
