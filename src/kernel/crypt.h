#ifndef _FS_CEPH_CRYPT_H
#define _FS_CEPH_CRYPT_H

#include "types.h"
#include "buffer.h"

extern int ceph_armor(char *dst, const char *src, const char *end);
extern int ceph_unarmor(char *dst, const char *src, const char *end);

extern struct ceph_buffer *ceph_secret_unarmor(const char *key, int len);

extern int ceph_decrypt(struct ceph_secret *secret, char *dst, const char *src,
			size_t len);
extern int ceph_encrypt(struct ceph_secret *secret, char *dst, const char *src,
			size_t len);

#endif
