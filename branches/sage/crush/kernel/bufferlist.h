#ifndef _FS_CEPH_BUFFERLIST_H
#define _FS_CEPH_BUFFERLIST_H



#define CEPH_BUFFERLIST_START_IOVLEN  8  /* embed some statically, for fast normal case */

struct ceph_bufferlist {
	struct iovec *b_iov;   /* data payload */
	struct iovec b_iov_array[CEPH_BUFFERLIST_START_IOVLEN];  
	int b_iovlen;          /* used/defined elements in b_iov */         
	int b_iovmax;          /* allocated size of b_iov array */
	struct iovec b_append; /* preallocated memory for appending data to this bufferlist */
};

struct ceph_bufferlist_iterator {
	int i_iov;  /* which iov */
	int i_off;  /* offset in that iov */
};

/*
 * add referenced memory to the bufferlist.
 * expand b_iov array if necessary.
 * extend tail iovec if the added region is contiguous.
 */
void ceph_bufferlist_append_ref(struct ceph_bufferlist *bl, void *p, int len)
{
	struct iovec *tmpvec;
	if (bl->b_iovlen == bl->b_iovmax) {
		if (bl->b_iovmax) {
			bl->b_iovmax *= 2;
			tmpvec = kmalloc(bl->b_iovmax);
			memcpy(tmpvec, bl->b_iov, sizeof(iovec)*bl->b_iovlen);
			if (bl->b_iovlen > CEPH_BUFFERLIST_START_IOVLEN)
				kfree(bl->b_iov);
			bl->b_iov = tmpvec;
			memset(tmpvec + bl->b_iovlen, 0, 
			       sizeof(iovec)*(bl->b_iovmax - bl->b_iovlen));
		} else {
			bl->b_iovmax = CEPH_BUFFERLIST_START_IOVLEN;
			bl->b_iov = bl->b_iov_array;
		}
	}

	if (bl->b_iovlen && 
	    p == bl->b_iov[bl->b_iovlen-1].iov_base + bl->b_iov[bl->b_iovlen-1].iov_base) {
		bl->b_iov[bl->b_iovlen-1].iov_len += len;
	} else {
		bl->b_iov[bl->b_iovlen].iov_base = p;
		bl->b_iov[bl->b_iovlen].iov_len = len;
		bl->b_iovlen++;
	}
}

void ceph_bufferlist_append_copy(struct ceph_bufferlist *bl, void *p, int len)
{
	int s;
	while (len > 0) {
		/* allocate more space? */
		if (!bl->b_append.iov_len) {
			bl->b_append.iov_len = (len + PAGE_SIZE - 1) & ~(PAGE_SIZE-1);
			bl->b_append.iov_base = kmalloc(bl->b_append.iov_len, GFP_KERNEL);
		}

		/* copy what we can */
		s = min(bl->b_append.iov_len, len);
		memcpy(bl->b_append.iov_base, s);
		ceph_bufferlist_append_ref(bl, b_append.iov_base, b_append.iov_len);
		len -= s;
		bl->b_append.iov_len -= s;
	}
}

#endif
