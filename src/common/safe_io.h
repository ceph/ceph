#ifndef CEPH_SAFE_IO
#define CEPH_SAFE_IO

/*
 * - mask EINTR
 * - mask short reads/writes
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

#include <unistd.h>
#include <errno.h>


static inline ssize_t safe_read(int fd, void *buf, size_t count)
{
	int r;

	while (count > 0) {
		r = read(fd, buf, count);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
	}
	return 0;
}
 
static inline ssize_t safe_write(int fd, const void *buf, size_t count)
{
	int r;

	while (count > 0) {
		r = write(fd, buf, count);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
	}
	return 0;
}

static inline ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset)
{
	int r;

	while (count > 0) {
		r = pread(fd, buf, count, offset);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
		offset += r;
	}
	return 0;
}

static inline ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	int r;

	while (count > 0) {
		r = pwrite(fd, buf, count, offset);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		count -= r;
		buf = (char *)buf + r;
		offset += r;
	}
	return 0;
}

#endif
