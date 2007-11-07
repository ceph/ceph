#include <linux/string.h>
#include <linux/slab.h>
#include <linux/uio.h>
#include <linux/jiffies.h>
#include <linux/sched.h>
#include <linux/types.h>

#include "bufferlist.h"

void ceph_bl_init(struct ceph_bufferlist *bl)
{
	memset(bl, 0, sizeof(*bl));
	bl->b_kvmax = CEPH_BUFFERLIST_START_KVLEN;
	bl->b_kv = bl->b_kv_array;
}

void ceph_bl_clear(struct ceph_bufferlist *bl)
{
	int i;
	/*  for (i=0; i<bl->b_kvlen; i++)
		kfree(bl->b_kv[i]->iov_base); */
	for (i=0; i<bl->b_kvlen; i++, bl->b_kv++)
		kfree(bl->b_kv->iov_base);
	bl->b_kvlen = 0;
	if (bl->b_kv != bl->b_kv_array) {
		kfree(bl->b_kv);
		bl->b_kv = bl->b_kv_array;
		bl->b_kvmax = CEPH_BUFFERLIST_START_KVLEN;
	}	
	if (bl->b_append.iov_base) {
		kfree(bl->b_append.iov_base);
		bl->b_append.iov_base = 0;
	}	
}
void * ceph_buffer_create(u32 size)
{
	void *buf = NULL;
	unsigned order = get_order(size);
	size_t numpages = (size + PAGE_SIZE -1) >> PAGE_SHIFT; 

	if ((PAGE_SIZE == 4096) && numpages) {
		/* note should we do this outside if to include the kmalloc? */
		while (buf == NULL) {
			buf = (void *)__get_free_pages(GFP_KERNEL, order);
			/* If fail, sleep for a bit and try again */
			if (buf == NULL)
				schedule_timeout_uninterruptible(msecs_to_jiffies(500));
		}
	} else {
		buf = kmalloc(size, GFP_KERNEL);
	}
	return(buf);
}

/*
 * add referenced memory to the bufferlist.
 * expand b_kv array if necessary.
 * extend tail kvec if the added region is contiguous.
 *
 * bl	bufferlist we want to append to
 * dp	pointer to data we want to append
 * len	length of data we want to append 
 */
void ceph_bl_append_ref(struct ceph_bufferlist *bl, void *dp, int len)
{
	/* check for optimal case of dp being at end of our last kvec */
	if (bl->b_kvlen) { 
	    struct kvec lastvec = bl->b_kv[bl->b_kvlen-1];
            if (dp == lastvec.iov_base + lastvec.iov_len) {
		lastvec.iov_len += len;
                return;
	    }	
	}

        /* check if we need to allocate more entries in our kvec array */
	if (bl->b_kvlen == bl->b_kvmax) {
		struct kvec *tmpvec;

		bl->b_kvmax *= 2;

		/* TBD: check result of kmalloc */
		tmpvec = kmalloc(bl->b_kvmax, GFP_KERNEL);
		memcpy(tmpvec, bl->b_kv, sizeof(struct kvec)*bl->b_kvlen);

		/* if the old array wasn't our original array (kmalloc'ed)  */
		if (bl->b_kvlen > CEPH_BUFFERLIST_START_KVLEN)
			kfree(bl->b_kv);

		bl->b_kv = tmpvec;
		memset(tmpvec + bl->b_kvlen, 0, sizeof(struct kvec)*(bl->b_kvmax - bl->b_kvlen));
	}

	bl->b_kv[bl->b_kvlen].iov_base = dp;
	bl->b_kv[bl->b_kvlen].iov_len = len;
	bl->b_kvlen++;
}

void ceph_bl_append_copy(struct ceph_bufferlist *bl, void *p, size_t len)
{
	int s;
	while (len > 0) {
		/* allocate more space? */
		ceph_bl_prepare_append(bl, len);

		/* copy what we can */
		s = min(bl->b_append.iov_len, len);
		memcpy(bl->b_append.iov_base, p, s);
		p += s;
		len -= s;
		ceph_bl_append_copied(bl, s);
	}
}
void ceph_bl_append_copied(struct ceph_bufferlist *bl, size_t len)
{
	ceph_bl_append_ref(bl, bl->b_append.iov_base, len);
	bl->b_append.iov_base += len;
	bl->b_append.iov_len -= len;
}

void ceph_bl_prepare_append(struct ceph_bufferlist *bl, int len)
{
	if ( ! bl->b_append.iov_len) {
		bl->b_append.iov_len = (len + PAGE_SIZE - 1) & ~(PAGE_SIZE-1);
		/* TBD: check result of kmalloc */
		bl->b_append.iov_base = kmalloc(bl->b_append.iov_len, GFP_KERNEL);
	}
}


void ceph_bl_iterator_init(struct ceph_bufferlist_iterator *bli)
{
	memset(bli, 0, sizeof(*bli));
}

void ceph_bl_iterator_advance(struct ceph_bufferlist *bl, 
			      struct ceph_bufferlist_iterator *bli, 
			      int off)
{

}

__u64 ceph_bl_decode_u64(struct ceph_bufferlist *bl, struct ceph_bufferlist_iterator *bli)
{
	__u64 r;
	r = le64_to_cpu((__u64*)(bl->b_kv[bli->i_kv].iov_base + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__u64));
	return r;
}
__s64 ceph_bl_decode_s64(struct ceph_bufferlist *bl, struct ceph_bufferlist_iterator *bli)
{
	__s64 r;
	r = le64_to_cpu((__s64*)(bl->b_kv[bli->i_kv].iov_base + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__s64));
	return r;
}

__u32 ceph_bl_decode_u32(struct ceph_bufferlist *bl, struct ceph_bufferlist_iterator *bli)
{
	__u32 r;
	r = le32_to_cpu(*(__u32*)(bl->b_kv[bli->i_kv].iov_base + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__u32));
	return r;
}
__s32 ceph_bl_decode_s32(struct ceph_bufferlist *bl, struct ceph_bufferlist_iterator *bli)
{
	__s32 r;
	r = le32_to_cpu(*(__s32*)(bl->b_kv[bli->i_kv].iov_base + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__s32));
	return r;
}

__u8 ceph_bl_decode_u8(struct ceph_bufferlist *bl, struct ceph_bufferlist_iterator *bli)
{
	__u8 r;
	r = *(__u8*)(bl->b_kv[bli->i_kv].iov_base + bli->i_off);
	ceph_bl_iterator_advance(bl, bli, sizeof(__u8));
	return r;
}

