
## 1. 安装依赖包
以下的依赖包并非全部都要安装，主要还看用户使用需求，例如是使用AD DC还是别的。
```
# CentOS 7
yum install attr bind-utils docbook-style-xsl gcc gdb krb5-workstation \
    libsemanage-python libxslt perl perl-ExtUtils-MakeMaker \
    perl-Parse-Yapp perl-Test-Base pkgconfig policycoreutils-python \
    python-crypto gnutls-devel libattr-devel keyutils-libs-devel \
    libacl-devel libaio-devel libblkid-devel libxml2-devel openldap-devel \
    pam-devel popt-devel python-devel readline-devel zlib-devel systemd-devel \
    lmdb-devel jansson-devel gpgme-devel python-gpgme libarchive-devel
```

## 2. 安装
```
tar -zxf samba-4.9.7.tar.gz
cd samba-4.9.7
./configure --enable-debug --with-shared-modules=perfcount_test
make && make install
```

## 3. 加入系统环境中
```
echo "export PATH=$PATH:/usr/local/samba/bin/:/usr/local/samba/sbin" >> /etc/profile.d/samba.sh
```

## 4. 加入系统库环境中
```
echo "/usr/local/samba/lib" >> /etc/ld.so.conf.d/samba.conf 
ldconfig
```

## 5. 调试
```
gdb --args ./bin/smbd -d 10 -i -l/var/log/samba -s/etc/samba/smb.conf
then:
b exit
run
At the breakpoint type "bt" to see the call stack.

(gdb) bt
#0  0x00007ffff381e163 in __epoll_wait_nocancel () from /lib64/libc.so.6
#1  0x00007ffff649df03 in epoll_event_loop (epoll_ev=0x5555557ac340, tvalp=0x7fffffffdab0) at ../lib/tevent/tevent_epoll.c:650
#2  0x00007ffff649e811 in epoll_event_loop_once (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent_epoll.c:937
#3  0x00007ffff649b20d in std_event_loop_once (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent_standard.c:110
#4  0x00007ffff649304f in _tevent_loop_once (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent.c:772
#5  0x00007ffff649335f in tevent_common_loop_wait (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent.c:895
#6  0x00007ffff649b2af in std_event_loop_wait (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent_standard.c:141
#7  0x00007ffff6493402 in _tevent_loop_wait (ev=0x55555578a080, location=0x7ffff74a15a0 "../source3/smbd/process.c:4130") at ../lib/tevent/tevent.c:914
#8  0x00007ffff733cd0a in smbd_process (ev_ctx=0x55555578a080, msg_ctx=0x555555798250, sock_fd=36, interactive=true) at ../source3/smbd/process.c:4130
#9  0x000055555556041d in smbd_accept_connection (ev=0x55555578a080, fde=0x5555557b01e0, flags=1, private_data=0x5555557af8e0) at ../source3/smbd/server.c:982
#10 0x00007ffff6493d5f in tevent_common_invoke_fd_handler (fde=0x5555557b01e0, flags=1, removed=0x0) at ../lib/tevent/tevent_fd.c:137
#11 0x00007ffff649e1da in epoll_event_loop (epoll_ev=0x55555578a310, tvalp=0x7fffffffde80) at ../lib/tevent/tevent_epoll.c:736
#12 0x00007ffff649e811 in epoll_event_loop_once (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent_epoll.c:937
#13 0x00007ffff649b20d in std_event_loop_once (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent_standard.c:110
#14 0x00007ffff649304f in _tevent_loop_once (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent.c:772
#15 0x00007ffff649335f in tevent_common_loop_wait (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent.c:895
#16 0x00007ffff649b2af in std_event_loop_wait (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent_standard.c:141
#17 0x00007ffff6493402 in _tevent_loop_wait (ev=0x55555578a080, location=0x555555566a6b "../source3/smbd/server.c:1383") at ../lib/tevent/tevent.c:914
#18 0x000055555556121e in smbd_parent_loop (ev_ctx=0x55555578a080, parent=0x55555579c7c0) at ../source3/smbd/server.c:1383
#19 0x00005555555632fa in main (argc=6, argv=0x7fffffffe498) at ../source3/smbd/server.c:2153
```




## 6. 加入service服务

```
$ cat  /usr/lib/systemd/system/smb.service

[Unit]
Description=Samba SMB Daemon
Documentation=man:smbd(8) man:samba(7) man:smb.conf(5)
After=network.target nmb.service winbind.service

[Service]
Type=notify
NotifyAccess=all
PIDFile=/run/smbd.pid
LimitNOFILE=16384
EnvironmentFile=-/etc/sysconfig/samba
ExecStart=/usr/local/samba/sbin/smbd --foreground --no-process-group $SMBDOPTIONS -l/var/log/samba -s/etc/samba/smb.conf
ExecReload=/bin/kill -HUP $MAINPID
LimitCORE=infinity
Environment=KRB5CCNAME=FILE:/run/samba/krb5cc_samba

[Install]
WantedBy=multi-user.target
```
