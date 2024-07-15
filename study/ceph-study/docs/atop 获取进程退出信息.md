**转载：**http://bean-li.github.io/atop-exit-code/

# 1. 前言
Daemon进程凌晨无故退出了，log中没有任何有效信息判断退出的原因。 QA找我确定下退出的原因，是收到信号被杀死，还是自己异常退出了。

幸好有atop,会纪录进程的退出码或者收到的信号值。

# 2. 方法
请看下图：
![image.png](https://upload-images.jianshu.io/upload_images/2099201-2cd8ebfb87185f30.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

上图中第一行 ＃exit 20305 表示在过去10分钟内，有20305个进程退出了。

其中这一行表示，在两个采样时间点中间，ceph-osd退出了，<> 保护的进程表示退出的进程。如何判断它是正常退出，还是收到信号，如果是前者，其返回值是多少，如果是后者，又收到了什么信号呢？

atop中的ST和EXC这两个字段，可以告诉我们答案
```
ST
The status of a process.
The first position indicates if the process has been started during the last interval (the value N means 'new process').
The second position indicates if the process has been finished during the last interval.
The value E means 'exit' on the process' own initiative; the exit code is displayed in the column 'EXC'.
The value S means that the process has been terminated unvoluntarily by a signal; the signal number is displayed in the in the column 'EXC'.
The value C means that the process has been terminated unvoluntarily by a signal, producing a core dump in its current directory; the signal number is displayed in the column 'EXC'.
```
S和C，表示收到了信号，不得不退出，这时候， EXC字段纪录就是导致进程退出的信号值。

```
EXC
The exit code of a terminated process (second position of column 'ST' is E) or the fatal signal number (second position of column 'ST' is S or C).
```
对于本例， ST＝ NS，表示收到了信号，才导致退出， EXC＝10 表示收到了10号信号。
```
kill -l
 1) SIGHUP	 2) SIGINT	 3) SIGQUIT	 4) SIGILL	 5) SIGTRAP
 6) SIGABRT	 7) SIGBUS	 8) SIGFPE	 9) SIGKILL	10) SIGUSR1
11) SIGSEGV	12) SIGUSR2	13) SIGPIPE	14) SIGALRM	15) SIGTERM
16) SIGSTKFLT	17) SIGCHLD	18) SIGCONT	19) SIGSTOP	20) SIGTSTP
21) SIGTTIN	22) SIGTTOU	23) SIGURG	24) SIGXCPU	25) SIGXFSZ
26) SIGVTALRM	27) SIGPROF	28) SIGWINCH	29) SIGIO	30) SIGPWR
31) SIGSYS	34) SIGRTMIN	35) SIGRTMIN+1	36) SIGRTMIN+2	37) SIGRTMIN+3
38) SIGRTMIN+4	39) SIGRTMIN+5	40) SIGRTMIN+6	41) SIGRTMIN+7	42) SIGRTMIN+8
43) SIGRTMIN+9	44) SIGRTMIN+10	45) SIGRTMIN+11	46) SIGRTMIN+12	47) SIGRTMIN+13
48) SIGRTMIN+14	49) SIGRTMIN+15	50) SIGRTMAX-14	51) SIGRTMAX-13	52) SIGRTMAX-12
53) SIGRTMAX-11	54) SIGRTMAX-10	55) SIGRTMAX-9	56) SIGRTMAX-8	57) SIGRTMAX-7
58) SIGRTMAX-6	59) SIGRTMAX-5	60) SIGRTMAX-4	61) SIGRTMAX-3	62) SIGRTMAX-2
63) SIGRTMAX-1	64) SIGRTMAX
```
# 3. 尾声

谁向ceph-osd进程发送了SIGUSR1信号，systemtap就可以来帮忙了：

编写如下脚本，监控发送到某进程的所有信号：
```
probe begin
{
  printf("%-30s%-8s %-16s %-8s %-16s %6s %-16s\n",
         "TIME","SPID", "SNAME", "RPID", "RNAME", "SIGNUM", "SIGNAME")
}

probe signal.send 
{
  if (pid_name == @1)
      printf("%-30s%-8d %-16s %-8d %-16s %6d %-16s\n",
              ctime(gettimeofday_s()),pid(), execname(), sig_pid, pid_name, sig, sig_name)
}**
```
```
stap sigmon.stap ceph-osd
```
测试下，其输出如下：
```
$ stap sigmon.stp ceph-osd
TIME                          SPID     SNAME            RPID     RNAME            SIGNUM SIGNAME         
Wed Nov  2 14:21:15 2016      19977    sh               19884    ceph-osd             17 SIGCHLD         
Wed Nov  2 14:21:15 2016      19992    sh               19884    ceph-osd             17 SIGCHLD         
Wed Nov  2 14:21:20 2016      21218    sh               19884    ceph-osd             17 SIGCHLD         
Wed Nov  2 14:21:20 2016      21224    sh               19884    ceph-osd             17 SIGCHLD         
Wed Nov  2 14:21:22 2016      9786     bash             19884    ceph-osd             10 SIGUSR1 
```
