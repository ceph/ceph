#ifndef _FS_CEPH_BUFFERLIST_H
#define _FS_CEPH_BUFFERLIST_H

#include <linux/uio.h>


/*
 * quick and dirty bufferlist struct.
 *
 * preallocates memory in large chunks, allowing you to append small bits at a
 * time in a reasonably efficient fashion...
 */

#define CEPH_BUFFERLIST_START_KVLEN  8  /* embed some statically, for fast normal case */

struct ceph_bufferlist {
	struct kvec *b_kv;   /* data payload */
	struct kvec b_kv_array[CEPH_BUFFERLIST_START_KVLEN];  
	size_t b_kvlen;          /* used/defined elements in b_kv */         
	size_t b_kvmax;          /* allocated size of b_kv array */
	size_t b_len;            /* size in bytes of _entire_ bufferlist */
	struct kvec b_append; /* preallocated memory for appending data to this bufferlist */
};

struct ceph_bufferlist_iterator {
	int i_kv;  /* which kv */
	int i_off;  /* offset in that kv */
};

#endif
