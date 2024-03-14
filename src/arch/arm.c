#include "acconfig.h"
#include "arch/probe.h"

/* flags we export */
int ceph_arch_neon = 0;
int ceph_arch_aarch64_crc32 = 0;
int ceph_arch_aarch64_pmull = 0;

#include <stdio.h>

#if __linux__

#include <elf.h>
#include <link.h> // ElfW macro
#include <sys/auxv.h>

#if __arm__ || __aarch64__
#include <asm/hwcap.h>
#endif // __arm__

#endif // __linux__

int ceph_arch_arm_probe(void)
{
#if __linux__
	unsigned long hwcap = getauxval(AT_HWCAP);
#if __arm__
	ceph_arch_neon = (hwcap & HWCAP_NEON) == HWCAP_NEON;
#elif __aarch64__
	ceph_arch_neon = (hwcap & HWCAP_ASIMD) == HWCAP_ASIMD;
	ceph_arch_aarch64_crc32 = (hwcap & HWCAP_CRC32) == HWCAP_CRC32;
	ceph_arch_aarch64_pmull = (hwcap & HWCAP_PMULL) == HWCAP_PMULL;
#endif
#endif // __linux__
	return 0;
}

