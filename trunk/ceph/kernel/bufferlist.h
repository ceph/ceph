#ifndef _FS_CEPH_BUFFERLIST_H
#define _FS_CEPH_BUFFERLIST_H


/*
 * quick and dirty bufferlist struct.
 *
 * preallocates memory in large chunks, allowing you to append small bits at a
 * time in a reasonably efficient fashion...
 */

#define CEPH_BUFFERLIST_START_IOVLEN  8  /* embed some statically, for fast normal case */

struct ceph_bufferlist {
	struct kvec *b_kv;   /* data payload */
	struct kvec b_kv_array[CEPH_BUFFERLIST_START_KVLEN];  
	int b_kvlen;          /* used/defined elements in b_kv */         
	int b_kvmax;          /* allocated size of b_kv array */
	struct kvec b_append; /* preallocated memory for appending data to this bufferlist */
};

struct ceph_bufferlist_iterator {
	int i_kv;  /* which kv */
	int i_off;  /* offset in that kv */
};



void ceph_bl_init(struct ceph_bufferlist *bl)
{
	memset(bl, 0, sizeof(*bl));
}

void ceph_bl_clear(struct ceph_bufferlist *bl)
{
	int i;
	for (i=0; i<bl->b_kvlen; i++)
		kfree(bl->b_kv[i]->iov_base);
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


/*
 * add referenced memory to the bufferlist.
 * expand b_kv array if necessary.
 * extend tail kvec if the added region is contiguous.
 */
void ceph_bl_append_ref(struct ceph_bufferlist *bl, void *p, int len)
{
	struct kvec *tmpvec;
	if (bl->b_kvlen == bl->b_kvmax) {
		if (bl->b_kvmax) {
			bl->b_kvmax *= 2;
			tmpvec = kmalloc(bl->b_kvmax);
			memcpy(tmpvec, bl->b_kv, sizeof(struct kvec)*bl->b_kvlen);
			if (bl->b_kvlen > CEPH_BUFFERLIST_START_KVLEN)
				kfree(bl->b_kv);
			bl->b_kv = tmpvec;
			memset(tmpvec + bl->b_kvlen, 0, 
			       sizeof(struct kvec)*(bl->b_kvmax - bl->b_kvlen));
		} else {
			bl->b_kvmax = CEPH_BUFFERLIST_START_KVLEN;
			bl->b_kv = bl->b_kv_array;
		}
	}

	if (bl->b_kvlen && 
	    p == bl->b_kv[bl->b_kvlen-1].kv_base + bl->b_kv[bl->b_kvlen-1].kv_base) {
		bl->b_kv[bl->b_kvlen-1].kv_len += len;
	} else {
		bl->b_kv[bl->b_kvlen].kv_base = p;
		bl->b_kv[bl->b_kvlen].kv_len = len;
		bl->b_kvlen++;
	}
}

void ceph_bl_append_copy(struct ceph_bufferlist *bl, void *p, int len)
{
	int s;
	while (len > 0) {
		/* allocate more space? */
		if (!bl->b_append.kv_len) {
			bl->b_append.kv_len = (len + PAGE_SIZE - 1) & ~(PAGE_SIZE-1);
			bl->b_append.kv_base = kmalloc(bl->b_append.kv_len, GFP_KERNEL);
		}

		/* copy what we can */
		s = min(bl->b_append.kv_len, len);
		memcpy(bl->b_append.kv_base, s);
		ceph_bl_append_ref(bl, b_append.kv_base, b_append.kv_len);
		len -= s;
		bl->b_append.kv_len -= s;
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

__u64 ceph_bl_decode_u64(struct ceph_bufferlist *bl, ceph_bufferlist_iterator *bli)
{
	__u64 r;
	r = le64_to_cpu((__u64*)(bl->b_kv[bli->i_kv] + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__u64));
}
__s64 ceph_bl_decode_s64(struct ceph_bufferlist *bl, ceph_bufferlist_iterator *bli)
{
	__s64 r;
	r = le64_to_cpu((__s64*)(bl->b_kv[bli->i_kv] + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__s64));
}

__u32 ceph_bl_decode_u32(struct ceph_bufferlist *bl, ceph_bufferlist_iterator *bli)
{
	__u32 r;
	r = le32_to_cpu((__u32*)(bl->b_kv[bli->i_kv] + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__u32));
}
__s32 ceph_bl_decode_s32(struct ceph_bufferlist *bl, ceph_bufferlist_iterator *bli)
{
	__s32 r;
	r = le32_to_cpu((__s32*)(bl->b_kv[bli->i_kv] + bli->i_off));
	ceph_bl_iterator_advance(bl, bli, sizeof(__s32));
}

__u8 ceph_bl_decode_u8(struct ceph_bufferlist *bl, ceph_bufferlist_iterator *bli)
{
	__u8 r;
	r = (__u8*)(bl->b_kv[bli->i_kv] + bli->i_off);
	ceph_bl_iterator_advance(bl, bli, sizeof(__u8));
}

#endif
