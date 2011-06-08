
#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <inttypes.h>
#include <linux/types.h>
#include <stdlib.h>

//#include "../client/ioctl.h"

#include <linux/ioctl.h>
#include <linux/types.h>
#define CEPH_IOCTL_MAGIC 0x97
#define CEPH_IOC_SYNCIO _IO(CEPH_IOCTL_MAGIC, 5)

void write_pattern()
{
	printf("writing pattern\n");

	int fd = open("foo", O_CREAT|O_WRONLY, 0644);
	uint64_t i;

	for (i=0; i<1048576 * sizeof(i); i += sizeof(i)) {
		write(fd, &i, sizeof(i));
	}

	close(fd);
}

int verify_pattern(char *buf, size_t len, uint64_t off)
{
	size_t i;

	for (i = 0; i < len; i += sizeof(uint64_t)) {
		uint64_t expected = i + off;
		uint64_t actual = *(uint64_t*)(buf + i);
		if (expected != actual) {
			printf("error: offset %llu had %llu\n", expected, actual);
			return -1;
		}
	}
	return 0;
}

int read_direct(int buf_align, uint64_t offset, int len)
{
	printf("read_direct buf_align %d offset %llu len %d\n", buf_align, offset, len);
	int fd = open("foo", O_RDONLY|O_DIRECT);
	void *rawbuf;
	posix_memalign(&rawbuf, 4096, len + buf_align);
	void *buf = (char *)rawbuf + buf_align;
	pread(fd, buf, len, offset);
	close(fd);
	int r = verify_pattern(buf, len, offset);
	free(rawbuf);
	return r;
}

int read_sync(int buf_align, uint64_t offset, int len)
{
	printf("read_sync buf_align %d offset %llu len %d\n", buf_align, offset, len);
	int fd = open("foo", O_RDONLY);
	ioctl(fd, CEPH_IOC_SYNCIO);
	void *rawbuf;
	posix_memalign(&rawbuf, 4096, len + buf_align);
	void *buf = (char *)rawbuf + buf_align;
	pread(fd, buf, len, offset);
	close(fd);
	int r = verify_pattern(buf, len, offset);
	free(rawbuf);
	return r;
}

int main(int argc, char **argv)
{
	char *buf;
	int fd;
	uint64_t i, j, k;

	write_pattern();

	for (i = 0; i < 4096; i += 512)
		for (j = 4*1024*1024 - 4096; j < 4*1024*1024 + 4096; j += 512)
			for (k = 1024; k <= 16384; k *= 2) {
				read_direct(i, j, k);
				read_sync(i, j, k);
			}

	return 0;
}
