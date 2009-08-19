
#include <linux/err.h>

#include "crypt.h"


struct ceph_buffer *ceph_secret_unarmor(const char *key, int len)
{
	struct ceph_buffer *b;
	int blen = len * 3 / 4;
	
	b = ceph_buffer_new_alloc(blen, GFP_NOFS);
	if (!b)
		return ERR_PTR(-ENOMEM);
	blen = ceph_unarmor(b->vec.iov_base, key, key+len);
	if (blen < 0) {
		ceph_buffer_put(b);
		return ERR_PTR(blen);
	}
	b->vec.iov_len = blen;
	return b;
}

int ceph_decrypt(struct ceph_secret *secret, char *dst, const char *src,
		 size_t len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		memcpy(dst, src, len);
		return 0;

	case CEPH_SECRET_AES:
		return -EINVAL;

	default:
		return -EINVAL;
	}
}


int ceph_encrypt(struct ceph_secret *secret, char *dst, const char *src,
		 size_t len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		memcpy(dst, src, len);
		return 0;

	case CEPH_SECRET_AES:
		return -EINVAL;

	default:
		return -EINVAL;
	}
}
