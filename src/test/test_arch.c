
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/neon.h"

int main(int argc, char **argv)
{
	ceph_arch_probe();
	assert(ceph_arch_probed);

	printf("ceph_arch_intel_sse42 = %d\n", ceph_arch_intel_sse42);
	printf("ceph_arch_neon = %d\n", ceph_arch_neon);

	return 0;
}
