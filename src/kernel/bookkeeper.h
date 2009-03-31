#ifndef _FS_CEPH_BOOKKEEPER_H
#define _FS_CEPH_BOOKKEEPER_H

#ifdef CONFIG_CEPH_BOOKKEEPER
extern void ceph_bookkeeper_dump(void);
extern void ceph_bookkeeper_init(void);
extern void ceph_bookkeeper_finalize(void);
extern void *ceph_kmalloc(char *fname, int line, size_t size, gfp_t flags);
extern void ceph_kfree(void *ptr);

#ifndef CEPH_OVERRIDE_BOOKKEEPER
#define kmalloc(size, flags)	ceph_kmalloc(__FILE__, __LINE__, size, flags)
#define kzalloc(size, flags)	ceph_kmalloc(__FILE__, __LINE__, size, \
					     flags | __GFP_ZERO)
#define kfree	ceph_kfree
#endif

#endif

#endif
