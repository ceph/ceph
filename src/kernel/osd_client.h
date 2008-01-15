#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

#include <linux/ceph_fs.h>
#include <linux/radix-tree.h>
#include <linux/completion.h>

#include "osdmap.h"

struct ceph_msg;

/*
 * object extent
 */
struct ceph_osd_read {
	struct ceph_object oid;
	__u64 start;
	__u64 length;
	struct page **pages;
};

/*
 * pending request 
 */
enum {
	REQUEST_ACK,   /* write serialized */
	REQUEST_SAFE,  /* write committed */
	REQUEST_DONE   /* read/stat/whatever completed */
};

struct ceph_osd_request {
	__u64             r_tid;
	int               r_flags;
	struct ceph_msg  *r_request;
	union ceph_pg     r_pgid;
	struct ceph_msg  *r_reply;
	int               r_result;
	atomic_t          r_ref;
	struct completion r_completion;      /* on ack or commit or read? */
	unsigned          r_nr_pages;        /* size of page array (follows) */
	struct page      *r_pages[0];        /* pages for data payload */
};

struct ceph_osd_client {
	spinlock_t             lock;
	struct ceph_client     *client;
	struct ceph_osdmap     *osdmap;       /* current map */
	__u64                  last_requested_map;
	__u64                  last_tid;      /* tid of last request */
	struct radix_tree_root request_tree;  /* pending requests, by tid */
	struct completion      map_waiters;
};

extern void ceph_osdc_init(struct ceph_osd_client *osdc, struct ceph_client *client);
extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc, struct ceph_msg *msg);
extern int ceph_osdc_prepare_pages(void *p, struct ceph_msg *m, int want);

extern int ceph_osdc_readpage(struct ceph_osd_client *osdc, ceph_ino_t ino,
			      struct ceph_file_layout *layout, 
			      loff_t off, loff_t len,
			      struct page *page);
extern int ceph_osdc_readpages(struct ceph_osd_client *osdc, ceph_ino_t ino,
			       struct ceph_file_layout *layout, 
			       struct list_head *pagels, int nr_pages);

#endif

