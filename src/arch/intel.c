#include "arch/probe.h"

/* flags we export */
int ceph_arch_intel_sse42 = 0;


/* this probably isn't specific enough for x86_64?  fix me someday */
#ifdef __LP64__

/* intel cpu? */
static void do_cpuid(unsigned int *eax, unsigned int *ebx, unsigned int *ecx,
                     unsigned int *edx)
{
        int id = *eax;

        asm("movl %4, %%eax;"
            "cpuid;"
            "movl %%eax, %0;"
            "movl %%ebx, %1;"
            "movl %%ecx, %2;"
            "movl %%edx, %3;"
                : "=r" (*eax), "=r" (*ebx), "=r" (*ecx), "=r" (*edx)
                : "r" (id)
                : "eax", "ebx", "ecx", "edx");
}

int ceph_arch_intel_probe(void)
{
	/* i know how to check this on x86_64... */
	unsigned int eax = 1, ebx, ecx, edx;
	do_cpuid(&eax, &ebx, &ecx, &edx);
	if ((ecx & (1 << 20)) != 0) {
		ceph_arch_intel_sse42 = 1;
	}
	return 0;
}

#else // __LP64__

int ceph_arch_intel_probe(void)
{
	/* no features */
	return 0;
}

#endif // __LP64__
