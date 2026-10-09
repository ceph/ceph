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

#elif __APPLE__

#include <sys/sysctl.h>

/*
 * Darwin has no getauxval(3); the kernel advertises the optional CPU
 * features as sysctl(3) nodes instead.  A node that is not registered
 * means the running kernel does not know the feature, so treat the
 * lookup failure as "not available".
 */
static int ceph_arch_darwin_feature(const char *name)
{
	int val = 0;
	size_t len = sizeof(val);

	if (sysctlbyname(name, &val, &len, NULL, 0) != 0)
		return 0;
	return val != 0;
}

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
#elif __APPLE__
#if __arm__ || __aarch64__
	ceph_arch_neon = ceph_arch_darwin_feature("hw.optional.neon");
#endif
#if __aarch64__
	ceph_arch_aarch64_crc32 =
		ceph_arch_darwin_feature("hw.optional.armv8_crc32");
	ceph_arch_aarch64_pmull =
		ceph_arch_darwin_feature("hw.optional.arm.FEAT_PMULL");
#endif
#endif // __linux__
	return 0;
}

