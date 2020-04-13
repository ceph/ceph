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
int max = 4;

void check_return(int r)
{
	if (r < 0) {
		printf("********* failed with %d %s ********\n", errno, strerror(errno));
		exit(1);
	}
}

int main(int argc, char **argv)
{
	int num = 1000;

	if (argc > 1)
		num = atoi(argv[1]);
	printf("will do %d iterations\n", num);

        int cwd = open(".", O_RDONLY);
        printf("cwd = %d\n", cwd);
        while (num-- > 0) {
		if (rand() % 10 == 0) {
			__u64 transid;
			int r;
			printf("sync starting\n");
			r = ioctl(cwd, BTRFS_IOC_START_SYNC, &transid);
			check_return(r);
			printf("sync started, transid %lld, waiting\n", transid);
			r = ioctl(cwd, BTRFS_IOC_WAIT_SYNC, &transid);
			check_return(r);
			printf("sync finished\n");	
		}

                int i = rand() % max;
                struct stat st;
                va.fd = cwd;
                sprintf(va.name, "test.%d", i);
                va.transid = 0;
                int r = stat(va.name, &st);
                if (r < 0) {
			if (rand() % 3 == 0) {
				printf("snap create (sync) %s\n", va.name);
				va.flags = 0;
				r = ioctl(cwd, BTRFS_IOC_SNAP_CREATE_V2, &va);
				check_return(r);
			} else {
				printf("snap create (async) %s\n", va.name);
				va.flags = BTRFS_SUBVOL_CREATE_ASYNC;
				r = ioctl(cwd, BTRFS_IOC_SNAP_CREATE_V2, &va);
				check_return(r);
				printf("snap created, transid %lld\n", va.transid);
				if (rand() % 2 == 0) {
					printf("waiting for async snap create\n");
					r = ioctl(cwd, BTRFS_IOC_WAIT_SYNC, &va.transid);
					check_return(r);
				}
                        }
                } else {
                        printf("snap remove %s\n", va.name);
			vold.fd = va.fd;
			strcpy(vold.name, va.name);
                        r = ioctl(cwd, BTRFS_IOC_SNAP_DESTROY, &vold);
			check_return(r);
                }
        }
	return 0;
}
