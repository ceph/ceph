#include "arch/probe.h"

/* flags we export */
int ceph_arch_neon = 0;

#include <stdio.h>

#if __linux__

#include <elf.h>
#include <link.h> // ElfW macro

#if __arm__
#include <asm/hwcap.h>
#endif // __arm__

static unsigned long get_auxval(unsigned long type)
{
	unsigned long result = 0;
	FILE *f = fopen("/proc/self/auxv", "r");
	if (f) {
		ElfW(auxv_t) entry;
		while (fread(&entry, sizeof(entry), 1, f)) {
			if (entry.a_type == type) {
				result = entry.a_un.a_val;
				break;
			}
		}
		fclose(f);
	}
	return result;
}

static unsigned long get_hwcap(void)
{
	return get_auxval(AT_HWCAP);
}

#endif // __linux__

int ceph_arch_neon_probe(void)
{
#if __arm__ && __linux__
	ceph_arch_neon = (get_hwcap() & HWCAP_NEON) == HWCAP_NEON;
#else
	if (0)
		get_hwcap();  // make compiler shut up
#endif
	return 0;
}

