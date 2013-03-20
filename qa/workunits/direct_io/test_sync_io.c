#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <inttypes.h>
#include <linux/types.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <errno.h>

//#include "../client/ioctl.h"

#include <linux/ioctl.h>
#define CEPH_IOCTL_MAGIC 0x97
#define CEPH_IOC_SYNCIO _IO(CEPH_IOCTL_MAGIC, 5)

void write_pattern()
{
	printf("writing pattern\n");

	uint64_t i;
	int r;

	int fd = open("foo", O_CREAT|O_WRONLY, 0644);
	if (fd < 0) {
	   r = errno;
	   printf("write_pattern: error: open() failed with: %d (%s)\n", r, strerror(r));
	   exit(r);
	}
	for (i=0; i<1048576 * sizeof(i); i += sizeof(i)) {
		r = write(fd, &i, sizeof(i));
		if (r == -1) {
			r = errno;
			printf("write_pattern: error: write() failed with: %d (%s)\n", r, strerror(r));
			break;
		}
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
			printf("error: offset %llu had %llu\n", (unsigned long long)expected,
			       (unsigned long long)actual);
			exit(1);
		}
	}
	return 0;
}

void generate_pattern(void *buf, size_t len, uint64_t offset)
{
	uint64_t *v = buf;
	size_t i;

	for (i=0; i<len / sizeof(v); i++)
		v[i] = i * sizeof(v) + offset;
	verify_pattern(buf, len, offset);
}

int read_file(int buf_align, uint64_t offset, int len, int direct) {

	printf("read_file buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	void *rawbuf;
	int r;
        int flags;
	int err = 0;

	if(direct)
	   flags = O_RDONLY|O_DIRECT;
	else
	   flags = O_RDONLY;

	int fd = open("foo", flags);
	if (fd < 0) {
	   err = errno;
	   printf("read_file: error: open() failed with: %d (%s)\n", err, strerror(err));
	   exit(err);
	}

	if (!direct)
	   ioctl(fd, CEPH_IOC_SYNCIO);

	if ((r = posix_memalign(&rawbuf, 4096, len + buf_align)) != 0) {
	   printf("read_file: error: posix_memalign failed with %d", r);
	   close(fd);
	   exit (r);
	}

	void *buf = (char *)rawbuf + buf_align;
	memset(buf, 0, len);
	r = pread(fd, buf, len, offset);
	if (r == -1) {
	   err = errno;
	   printf("read_file: error: pread() failed with: %d (%s)\n", err, strerror(err));
	   goto out;
	}
	r = verify_pattern(buf, len, offset);

out:
	close(fd);
	free(rawbuf);
	return r;
}

int read_direct(int buf_align, uint64_t offset, int len)
{
	printf("read_direct buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	return read_file(buf_align, offset, len, 1);
}

int read_sync(int buf_align, uint64_t offset, int len)
{
	printf("read_sync buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	return read_file(buf_align, offset, len, 0);
}

int write_file(int buf_align, uint64_t offset, int len, int direct)
{
	printf("write_file buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	void *rawbuf;
	int r;
        int err = 0;
	int flags;
	if (direct)
	   flags = O_WRONLY|O_DIRECT|O_CREAT;
        else
	   flags = O_WRONLY|O_CREAT;

	int fd = open("foo", flags, 0644);
	if (fd < 0) {
	   int err = errno;
	   printf("write_file: error: open() failed with: %d (%s)\n", err, strerror(err));
	   exit(err);
	}

	if ((r = posix_memalign(&rawbuf, 4096, len + buf_align)) != 0) {
	   printf("write_file: error: posix_memalign failed with %d", r);
	   err = r;
	   goto out_close;
	}

	if (!direct)
	   ioctl(fd, CEPH_IOC_SYNCIO);

	void *buf = (char *)rawbuf + buf_align;

	generate_pattern(buf, len, offset);

	r = pwrite(fd, buf, len, offset);
	close(fd);

	fd = open("foo", O_RDONLY);
	if (fd < 0) {
	   err = errno;
	   printf("write_file: error: open() failed with: %d (%s)\n", err, strerror(err));
	   free(rawbuf);
	   goto out_unlink;
	}
	void *buf2 = malloc(len);
	if (!buf2) {
	   err = -ENOMEM;
	   printf("write_file: error: malloc failed\n");
	   goto out_free;
	}

	memset(buf2, 0, len);
	r = pread(fd, buf2, len, offset);
	if (r == -1) {
	   err = errno;
	   printf("write_file: error: pread() failed with: %d (%s)\n", err, strerror(err));
	   goto out_free_buf;
	}
	r = verify_pattern(buf2, len, offset);

out_free_buf:
	free(buf2);
out_free:
	free(rawbuf);
out_close:
	close(fd);
out_unlink:
	unlink("foo");
	if (err)
	   exit(err);
	return r;
}

int write_direct(int buf_align, uint64_t offset, int len)
{
	printf("write_direct buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	return write_file (buf_align, offset, len, 1);
}

int write_sync(int buf_align, uint64_t offset, int len)
{
	printf("write_sync buf_align %d offset %llu len %d\n", buf_align,
	       (unsigned long long)offset, len);
	return write_file (buf_align, offset, len, 0);
}

int main(int argc, char **argv)
{
	uint64_t i, j, k;
	int read = 1;
	int write = 1;

	if (argc >= 2 && strcmp(argv[1], "read") == 0)
		write = 0;
	if (argc >= 2 && strcmp(argv[1], "write") == 0)
		read = 0;

	if (read) {
		write_pattern();
		
		for (i = 0; i < 4096; i += 512)
			for (j = 4*1024*1024 - 4096; j < 4*1024*1024 + 4096; j += 512)
				for (k = 1024; k <= 16384; k *= 2) {
					read_direct(i, j, k);
					read_sync(i, j, k);
				}
		
	}
	unlink("foo");
	if (write) {
		for (i = 0; i < 4096; i += 512)
			for (j = 4*1024*1024 - 4096 + 512; j < 4*1024*1024 + 4096; j += 512)
				for (k = 1024; k <= 16384; k *= 2) {
					write_direct(i, j, k);
					write_sync(i, j, k);
				}
	}
	

	return 0;
}
