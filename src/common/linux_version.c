#include "common/linux_version.h"

#include <stdio.h>
#include <string.h>
#include <sys/utsname.h>

int get_linux_version(void)
{
	struct utsname ubuf;
	int a, b, c;
	int n;

	if (uname(&ubuf) || strcmp(ubuf.sysname, "Linux"))
		return 0;

	n = sscanf(ubuf.release, "%d.%d.%d", &a, &b, &c);
	switch (n) {
	case 3:
		return KERNEL_VERSION(a, b, c);
	case 2:
		return KERNEL_VERSION(a, b, 0);
	default:
		return 0;
	}
}
