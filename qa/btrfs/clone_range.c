#include <fcntl.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <string.h>

#include <linux/types.h>
#include "../../src/os/btrfs_ioctl.h"
#include <stdio.h>
#include <errno.h>

int main(int argc, char **argv)
{
        struct btrfs_ioctl_clone_range_args ca;
        int dfd;
        int r;

	if (argc < 6) {
		printf("usage: %s <srcfn> <srcoffset> <srclen> <destfn> <destoffset>\n", argv[0]);
		exit(1);
	}

        ca.src_fd = open(argv[1], O_RDONLY);
        ca.src_offset = atoi(argv[2]);
        ca.src_length = atoi(argv[3]);
        dfd = open(argv[4], O_WRONLY|O_CREAT);
        ca.dest_offset = atoi(argv[5]);

        r = ioctl(dfd, BTRFS_IOC_CLONE_RANGE, &ca);
        printf("clone_range %s %lld %lld~%lld to %s %d %lld = %d %s\n",
               argv[1], ca.src_fd,
	       ca.src_offset, ca.src_length,
	       argv[4], dfd,
               ca.dest_offset, r, strerror(errno));
        return r;
}
