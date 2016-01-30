#include "common/freebsd_version.h"

#include <stdio.h>
#include <string.h>
#include <sys/utsname.h>

int get_freebsd_version(void)
{
	struct utsname ubuf;
	int a, b, c = 0;
	int n;

	if (uname(&ubuf) || strcmp(ubuf.sysname, "FreeBSD"))
		return 0;

	n = sscanf(ubuf.release, "%u.%u-", &a, &b);
	switch (n) {
	case 3:
		return KERNEL_VERSION(a, b, c);
	case 2:
		return KERNEL_VERSION(a, b, 0);
	case 1:
		return KERNEL_VERSION(a, 0, 0);
	default:
		return 0;
	}
}
