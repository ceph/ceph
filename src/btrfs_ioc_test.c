#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <asm/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>

#include "common/safe_io.h"
#include "os/btrfs_ioctl.h"

void do_open_wr(const char *fname, int *fd)
{
	*fd = open(fname, O_WRONLY | O_CREAT, 0644);
	if (*fd < 0) {
		perror("open");
		exit(1);
	}
}

void do_open_rd(const char *fname, int *fd)
{
	*fd = open(fname, O_RDONLY);
	if (*fd < 0) {
		perror("open");
		exit(1);
	}
}

void do_lseek(int fd, int ofs)
{
	int rc = lseek(fd, ofs, SEEK_SET);
	if (rc < 0) {
		perror("lseek");
		exit(1);
	}
}

void do_write(int fd, int len)
{
	char *buf = malloc(len);
	int rc;
	if (!buf) {
		printf("not enough memory\n");
		exit(1);
	}

	memset(buf, 0, len);
	rc = safe_write(fd, buf, len);
	if (rc) {
		fprintf(stderr, "safe_write failed with error %d (%s)\n",
			rc, strerror(rc));
		exit(1);
	}

	if (rc != len) {
		printf("invalid number of bytes written\n");
		exit(1);
	}

	free(buf);
}

void do_link(const char *old, const char *new)
{
	int rc = link(old, new);
	if (rc < 0) {
		perror("link");
		exit(1);
	}
}

void do_clone_range(int from, int to, int off, int len)
{
	struct btrfs_ioctl_clone_range_args a;
	int r;

	a.src_fd = from;
	a.src_offset = off;
	a.src_length = len;
	a.dest_offset = off;
	r = ioctl(to, BTRFS_IOC_CLONE_RANGE, &a);
	if (r < 0) {
		perror("ioctl");
		exit(1);
	}
}

void do_snap_async(int fd, const char *name, unsigned long long *transid)
{
	struct btrfs_ioctl_async_vol_args async_args;
	struct btrfs_ioctl_vol_args volargs;
	int r;

	strcpy(volargs.name, name);
	volargs.fd = fd;

	async_args.args = &volargs;
	async_args.transid = transid;

	r = ioctl(fd, BTRFS_IOC_SNAP_CREATE_ASYNC, &async_args);

	if (r < 0) {
		perror("ioctl");
		exit(1);
	}
}

void do_snap_destroy(int fd, const char *name)
{
	struct btrfs_ioctl_vol_args volargs;
	int r;

	strcpy(volargs.name, name);
	volargs.fd = 0;

	r = ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &volargs);

	if (r < 0) {
		perror("snap_destroy: ioctl");
		exit(1);
	}
}

void do_snap_wait(int fd, unsigned long long transid)
{
	int r = ioctl(fd, BTRFS_IOC_WAIT_SYNC, &transid);
	if (r < 0) {
		perror("do_snap_wait: ioctl");
		exit(1);
	}
}

void usage_exit(char *arg)
{
	printf("usage: %s <btrfs_base> <snap_name>\n", arg);
	exit(1);
}

#define TEMP_FILENAME "temp"
#define DEST_FILENAME "dest"
#define SRC_FILENAME "src"

int main(int argc, char *argv[])
{
	const char *base_dir;
	const char *snap_name;

	int fd;
	int i;
	unsigned long long transid;

	if (argc < 3)
		usage_exit(argv[0]);

	base_dir = argv[1];
	snap_name = argv[2];

	for (i=0; i<10; i++) {
		printf("%d\n", i);
		do_open_rd(base_dir, &fd);
		do_snap_async(fd, snap_name, &transid);
		sleep(2);
		//do_snap_wait(fd, transid);
		do_snap_destroy(fd, snap_name);
		close(fd);
	}
	
	return 0;
}
