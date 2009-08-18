#ifndef __FS_CEPH_BUFFER_H
#define __FS_CEPH_BUFFER_H

#include <linux/mm.h>
#include <linux/types.h>
#include <linux/vmalloc.h>

#include "ceph_debug.h"

/*
 * a simple reference counted buffer.
 *
 * use kmalloc for small sizes (<= one page), vmalloc for larger
 * sizes.
 */
struct ceph_buffer {
	atomic_t nref;
	struct kvec vec;
	size_t alloc_len;
	bool is_static, is_vmalloc;
};

static inline void ceph_buffer_init_static(struct ceph_buffer *b)
{
	atomic_set(&b->nref, 1);
	b->vec.iov_base = NULL;
	b->vec.iov_len = 0;
	b->alloc_len = 0;
	b->is_static = true;
}

static inline struct ceph_buffer *ceph_buffer_new(gfp_t gfp)
{
	struct ceph_buffer *b;

	b = kmalloc(sizeof(*b), gfp);
	if (!b)
		return NULL;
	atomic_set(&b->nref, 1);
	b->vec.iov_base = NULL;
	b->vec.iov_len = 0;
	b->alloc_len = 0;
	b->is_static = false;
	return b;
}

static inline int ceph_buffer_alloc(struct ceph_buffer *b, int len, gfp_t gfp)
{
	if (len <= PAGE_SIZE) {
		b->vec.iov_base = kmalloc(len, gfp);
		b->is_vmalloc = false;
	} else {
		b->vec.iov_base = __vmalloc(len, gfp, PAGE_KERNEL);
		b->is_vmalloc = true;
	}
	if (!b->vec.iov_base)
		return -ENOMEM;
	b->alloc_len = len;
	b->vec.iov_len = len;
	return 0;
}

static inline struct ceph_buffer *ceph_buffer_get(struct ceph_buffer *b)
{
	atomic_inc(&b->nref);
	return b;
}

static inline void ceph_buffer_put(struct ceph_buffer *b)
{
	if (b && atomic_dec_and_test(&b->nref)) {
		if (b->vec.iov_base) {
			if (b->is_vmalloc)
				vfree(b->vec.iov_base);
			else
				kfree(b->vec.iov_base);
		}
		kfree(b);
	}
}

static inline struct ceph_buffer *ceph_buffer_new_alloc(int len, gfp_t gfp)
{
	struct ceph_buffer *b = ceph_buffer_new(gfp);

	if (b && ceph_buffer_alloc(b, len, gfp) < 0) {
		ceph_buffer_put(b);
		b = NULL;
	}
	return b;
}

#endif
