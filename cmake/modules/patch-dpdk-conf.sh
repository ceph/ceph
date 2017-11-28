#!/bin/sh
# -*- mode:sh; tab-width:4; indent-tabs-mode:nil -*

setconf() {
    local key=$1
    local val=$2
    if grep -q ^$key= ${conf}; then
        sed -i -e "s:^$key=.*$:$key=$val:g" ${conf}
    else
        echo $key=$val >> ${conf}
    fi
}

conf=$1/.config
shift
machine=$1
shift

setconf CONFIG_RTE_MACHINE "${machine}"
# Disable experimental features
setconf CONFIG_RTE_NEXT_ABI n
setconf CONFIG_RTE_LIBRTE_MBUF_OFFLOAD n
# Disable unmaintained features
setconf CONFIG_RTE_LIBRTE_POWER n

setconf CONFIG_RTE_EAL_IGB_UIO n
setconf CONFIG_RTE_LIBRTE_KNI n
setconf CONFIG_RTE_KNI_KMOD n
setconf CONFIG_RTE_KNI_PREEMPT_DEFAULT n

# no pdump
setconf CONFIG_RTE_LIBRTE_PDUMP n

# no vm support
setconf CONFIG_RTE_LIBRTE_EAL_VMWARE_TSC_MAP_SUPPORT n
setconf CONFIG_RTE_LIBRTE_VHOST n
setconf CONFIG_RTE_LIBRTE_VHOST_NUMA n
setconf CONFIG_RTE_LIBRTE_VMXNET3_PMD n
setconf CONFIG_RTE_LIBRTE_PMD_VHOST n
setconf CONFIG_RTE_APP_EVENTDEV n

# no test
setconf CONFIG_RTE_APP_TEST n
setconf CONFIG_RTE_TEST_PMD n

# async/dpdk does not like it
setconf CONFIG_RTE_MBUF_REFCNT_ATOMIC n

# balanced allocation of hugepages
setconf CONFIG_RTE_EAL_NUMA_AWARE_HUGEPAGES n
