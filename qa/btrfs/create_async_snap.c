#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <string.h>

#include <linux/ioctl.h>
#include <linux/types.h>
#include "../../src/os/btrfs_ioctl.h"

struct btrfs_ioctl_vol_args_v2 va;

int main(int argc, char **argv)
{
	int fd;
	int r;

	if (argc != 3) {
		printf("usage: %s <source subvol> <name>\n", argv[0]);
		return 1;
	}
	printf("creating snap ./%s from %s\n", argv[2], argv[1]);
	fd = open(".", O_RDONLY);
	va.fd = open(argv[1], O_RDONLY);
	va.flags = BTRFS_SUBVOL_CREATE_ASYNC;
	strcpy(va.name, argv[2]);
	r = ioctl(fd, BTRFS_IOC_SNAP_CREATE_V2, (unsigned long long)&va);
	printf("result %d\n", r ? -errno:0);
	return r;
}
