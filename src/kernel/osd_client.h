#ifndef _FS_CEPH_OSD_CLIENT_H
#define _FS_CEPH_OSD_CLIENT_H

/* this will be equivalent to osdc/Objecter.h */

#include <linux/radix-tree.h>
#include <linux/completion.h>

#include "ceph_fs.h"
#include "osdmap.h"

struct ceph_msg;

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
	int               r_aborted;
	int               r_flags;
	struct ceph_msg  *r_request;
	struct ceph_entity_addr r_last_osd;  /* last osd we sent request to */
	union ceph_pg     r_pgid;
	struct ceph_msg  *r_reply;
	int               r_result;
	atomic_t          r_ref;
	struct completion r_completion;      /* on ack or commit or read? */
	unsigned          r_nr_pages;        /* size of page array (follows) */
	struct page      *r_pages[0];        /* pages for data payload */
};

struct ceph_osd_client {
	struct ceph_client     *client;

	struct ceph_osdmap     *osdmap;       /* current map */
	struct rw_semaphore    map_sem;
	struct completion      map_waiters;
	__u64                  last_requested_map;

	spinlock_t             request_lock;
	__u64                  last_tid;      /* tid of last request */
	struct radix_tree_root request_tree;  /* pending requests, by tid */
	int                    nr_requests;
	struct delayed_work    timeout_work;
};

extern void ceph_osdc_init(struct ceph_osd_client *osdc,
			   struct ceph_client *client);
extern void ceph_osdc_stop(struct ceph_osd_client *osdc);

extern void ceph_osdc_handle_reply(struct ceph_osd_client *osdc,
				   struct ceph_msg *msg);
extern void ceph_osdc_handle_map(struct ceph_osd_client *osdc,
				 struct ceph_msg *msg);
extern int ceph_osdc_prepare_pages(void *p, struct ceph_msg *m, int want);

extern int ceph_osdc_readpage(struct ceph_osd_client *osdc, ceph_ino_t ino,
			      struct ceph_file_layout *layout,
			      loff_t off, loff_t len,
			      struct page *page);
extern int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			       struct address_space *mapping,
			       ceph_ino_t ino, struct ceph_file_layout *layout,
			       __u64 off, __u64 len,
			       struct list_head *page_list, int nr_pages);
extern int ceph_osdc_prepare_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
			      struct ceph_file_layout *layout,
			      loff_t off, loff_t len,
			      struct page *page);
extern int ceph_osdc_commit_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
			      struct ceph_file_layout *layout,
			      loff_t off, loff_t len,
			      struct page *page);
extern int ceph_osdc_writepages(struct ceph_osd_client *osdc, ceph_ino_t ino,
				struct ceph_file_layout *layout,
				loff_t off, loff_t len,
				struct page **pagevec, int nr_pages);

extern int ceph_osdc_sync_read(struct ceph_osd_client *osdc, ceph_ino_t ino,
			       struct ceph_file_layout *layout,
			       __u64 off, __u64 len,
			       char __user *data);
extern int ceph_osdc_sync_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
				struct ceph_file_layout *layout,
				__u64 off, __u64 len,
				const char __user *data);

extern int ceph_osdc_prepare_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
				   struct ceph_file_layout *layout,
				   loff_t off, loff_t len,
				   struct page *page);
extern int ceph_osdc_commit_write(struct ceph_osd_client *osdc, ceph_ino_t ino,
				  struct ceph_file_layout *layout,
				  loff_t off, loff_t len,
				  struct page *page);
extern int ceph_osdc_writepage(struct ceph_osd_client *osdc, ceph_ino_t ino,
			       struct ceph_file_layout *layout,
			       loff_t off, loff_t len,
			       struct page *page);

#endif

