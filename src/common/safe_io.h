#ifndef CEPH_SAFE_IO
#define CEPH_SAFE_IO

/*
 * - mask EINTR
 * - mask short reads/writes
 * - on error return -errno (instead of returning -1 and setting errno)
 */

extern "C" {
  ssize_t safe_read(int fd, void *buf, size_t count);
  ssize_t safe_read_exact(int fd, void *buf, size_t count);
  ssize_t safe_write(int fd, const void *buf, size_t count);
  ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset);
  ssize_t safe_pread_exact(int fd, void *buf, size_t count, off_t offset);
  ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset);
}

#endif
