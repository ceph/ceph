#!/usr/bin/env bash
declare -A rsapub
for fulln in $*; do
    sname=`echo $fulln | sed 's/\..*//'`
    nhead=`echo $sname | sed 's/[0-9]*//g'`
    x=`ssh $fulln "ls .ssh/id_rsa"`
    if [ -z $x ]; then
        ssh $fulln "ssh-keygen -N '' -f .ssh/id_rsa";
    fi
    xx=`ssh $fulln "ls .ssh/config"`
    if [ -z $xx ]; then
        scp config $fulln:/home/ubuntu/.ssh/config
    fi
    ssh $fulln "chown ubuntu:ubuntu .ssh/config"
    ssh $fulln "chmod 0600 .ssh/config"
    rsapub[$fulln]=`ssh $fulln "cat .ssh/id_rsa.pub"`
done
for ii in $*; do
    ssh $ii sudo iptables -F
    for jj in $*; do
        pval=${rsapub[$jj]}
        if [ "$ii" != "$jj" ]; then
            xxxx=`ssh $ii "grep $jj .ssh/authorized_keys"`
            if [ -z "$xxxx" ]; then
                ssh $ii "echo '$pval' | sudo tee -a /home/ubuntu/.ssh/authorized_keys"
            fi
        fi
    done;
done
