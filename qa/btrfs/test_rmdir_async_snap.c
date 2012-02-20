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
struct btrfs_ioctl_vol_args vold;

int main(int argc, char **argv)
{
	int num = 1000;
	int i, r, fd;
	char buf[30];

	if (argc > 1)
		num = atoi(argv[1]);
	printf("will do %d iterations\n", num);
	
	fd = open(".", O_RDONLY);
	vold.fd = 0;
	strcpy(vold.name, "current");
	r = ioctl(fd, BTRFS_IOC_SUBVOL_CREATE, (unsigned long int)&vold);
	printf("create current ioctl got %d\n", r ? errno:0);
	if (r)
		return 1;

	for (i=0; i<num; i++) {
		sprintf(buf, "current/dir.%d", i);
		r = mkdir(buf, 0755);
		printf("mkdir got %d\n", r ? errno:0);
		if (r)
			return 1;
	}

	va.fd = open("current", O_RDONLY);
	va.flags = BTRFS_SUBVOL_CREATE_ASYNC;
	for (i=0; i<num; i++) {
		system("/bin/cp /boot/vmlinuz-3.2.0-ceph-00142-g9e98323 current/foo");
		sprintf(buf, "current/dir.%d", i);
		r = rmdir(buf);
		printf("rmdir got %d\n", r ? errno:0);
		if (r)
			return 1;

		if (i % 10) continue;
		sprintf(va.name, "snap.%d", i);
		r = ioctl(fd, BTRFS_IOC_SNAP_CREATE_V2, (unsigned long long)&va);
		printf("ioctl got %d\n", r ? errno:0);
		if (r)
			return 1;
	}
	return 0;
}
