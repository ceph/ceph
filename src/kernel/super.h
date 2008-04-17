#ifndef _FS_CEPH_SUPER_H
#define _FS_CEPH_SUPER_H

#include <linux/ceph_fs.h>
#include <linux/fs.h>
#include <linux/wait.h>
#include <linux/completion.h>
#include <linux/pagemap.h>

#include "messenger.h"
#include "mon_client.h"
#include "mds_client.h"
#include "osd_client.h"

extern int ceph_debug;
extern int ceph_debug_msgr;
extern int ceph_debug_super;
extern int ceph_debug_tcp;
extern int ceph_debug_mdsc;
extern int ceph_debug_osdc;
extern int ceph_debug_addr;

#define CEPH_DUMP_ERROR_ALWAYS

#define dout(x, args...) do {						\
		if ((DOUT_VAR >= 0 && x <= DOUT_VAR) ||			\
		    (DOUT_VAR < 0 && x <= ceph_debug))			\
			printk(KERN_INFO "ceph_" DOUT_PREFIX args);	\
	} while (0)

#ifdef CEPH_DUMP_ERROR_ALWAYS
#define derr(x, args...) do {						\
		printk(KERN_ERR "ceph: " args);	\
	} while (0)
#else
#define derr(x, args...) do {						\
		if ((DOUT_VAR >= 0 && x <= DOUT_VAR) ||			\
		    (DOUT_VAR < 0 && x <= ceph_debug))			\
			printk(KERN_ERR "ceph_" DOUT_PREFIX args);	\
	} while (0)
#endif

#define CEPH_SUPER_MAGIC 0xc364c0de  /* whatev */
#define CEPH_BLOCK_SHIFT 20    /* 1 MB */
#define CEPH_BLOCK  (1 << CEPH_BLOCK_SHIFT)

#define IPQUADPORT(n)							\
	(unsigned int)(((n).sin_addr.s_addr)) & 0xFF,			\
		(unsigned int)(((n).sin_addr.s_addr)>>8) & 0xFF,	\
		(unsigned int)(((n).sin_addr.s_addr)>>16) & 0xFF,	\
		(unsigned int)(((n).sin_addr.s_addr)>>24) & 0xFF,	\
		(unsigned int)(ntohs((n).sin_port))

/*
 * mount options
 */
#define CEPH_MOUNT_FSID     1
#define CEPH_MOUNT_NOSHARE  2  /* don't share client with other mounts */
#define CEPH_MOUNT_MYIP     4  /* specified my ip */

struct ceph_mount_args {
	int mntflags;
	int flags;
	struct ceph_fsid fsid;
	struct ceph_entity_addr my_addr;
	int num_mon;
	struct ceph_entity_addr mon_addr[5];
	char path[100];
	int wsize;
};

/*
 * per-filesystem client state
 *
 * possibly shared by multiple mount points, if they are
 * mounting the same ceph filesystem/cluster.
 */
struct ceph_client {
	__u32 whoami;                   /* my client number */

	struct ceph_mount_args mount_args;
	struct ceph_fsid fsid;

	struct super_block *sb;

	unsigned long mounting;   /* map bitset; 4=mon, 2=mds, 1=osd map */
	wait_queue_head_t mount_wq;
	
	struct ceph_messenger *msgr;   /* messenger instance */
	struct ceph_mon_client monc;
	struct ceph_mds_client mdsc;
	struct ceph_osd_client osdc;

	/* writeback */
	struct workqueue_struct *wb_wq;

	/* lets ignore all this until later */
	spinlock_t sb_lock;
	int num_sb;      /* ref count (for each sb_info that points to me) */
	struct list_head sb_list;
};

static inline int ceph_have_all_maps(struct ceph_client *client)
{
	return find_first_zero_bit(&client->mounting, 4) == 3;
}

/*
 * CEPH per-mount superblock info
 */
static inline struct ceph_client *ceph_client(struct super_block *sb)
{
	return sb->s_fs_info;
}

/*
 * CEPH file system in-core inode info
 */

struct ceph_inode_cap {
	int mds;    /* -1 if not used */
	int issued;       /* latest, from the mds */
	int implemented;  /* what we've implemneted (for tracking revocation) */
	u64 seq;
	int flags;  /* stale, etc.? */
	struct ceph_inode_info *ci;
	struct list_head ci_caps;       /* per-ci caplist */
	struct ceph_mds_session *session;
	struct list_head session_caps;  /* per-session caplist */
};

struct ceph_inode_frag_map_item {
	u32 frag;
	u32 mds;
};

#define STATIC_CAPS 2

struct ceph_inode_info {
	u64 i_ceph_ino;

	u64 i_version;

	struct ceph_file_layout i_layout;
	char *i_symlink;

	int i_lease_mask;
	struct ceph_mds_session *i_lease_session;
	long unsigned i_lease_ttl;  /* jiffies */
	struct list_head i_lease_item; /* mds session list */

	struct ceph_frag_tree_head *i_fragtree, i_fragtree_static[1];
	int i_frag_map_nr;
	struct ceph_inode_frag_map_item *i_frag_map, i_frag_map_static[1];

	struct list_head i_caps;
	struct ceph_inode_cap i_static_caps[STATIC_CAPS];
	wait_queue_head_t i_cap_wq;
	unsigned long i_hold_caps_until; /* jiffies */

	int i_nr_by_mode[CEPH_FILE_MODE_NUM];
	loff_t i_max_size;      /* size authorized by mds */
	loff_t i_reported_size; /* (max_)size reported to or requested of mds */
	loff_t i_wanted_max_size;  /* offset we'd like to write too */
	loff_t i_requested_max_size;  /* max_size we've requested */
	struct timespec i_old_atime;

	/* held references to caps */
	int i_rd_ref, i_rdcache_ref, i_wr_ref, i_wrbuffer_ref;

	unsigned long i_hashval;

	struct work_struct i_wb_work;  /* writeback work */
	struct delayed_work i_cap_dwork;  /* cap work */

	struct inode vfs_inode; /* at end */
};

static inline struct ceph_inode_info *ceph_inode(struct inode *inode)
{
	return list_entry(inode, struct ceph_inode_info, vfs_inode);
}

struct ceph_dentry_info {
	struct dentry *dentry;
	struct ceph_mds_session *lease_session;
	struct list_head lease_item; /* mds session list */
};

static inline struct ceph_dentry_info *ceph_dentry(struct dentry *dentry)
{
	return (struct ceph_dentry_info *)dentry->d_fsdata;
}

static inline void ceph_queue_writeback(struct ceph_client *cl, 
					struct ceph_inode_info *ci)
{
	queue_work(cl->wb_wq, &ci->i_wb_work);
}


/*
 * ino_t is <64 bits on many architectures... blech
 */
static inline ino_t ceph_ino_to_ino(u64 cephino)
{
	ino_t ino = (ino_t)cephino;
#if BITS_PER_LONG == 32
	ino ^= cephino >> (sizeof(u64)-sizeof(ino_t)) * 8;
#endif
	return ino;
}

static inline void ceph_set_ino(struct inode *inode, __u64 ino)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	ci->i_ceph_ino = ino;
	inode->i_ino = ceph_ino_to_ino(ino);
}

static inline int ceph_set_ino_cb(struct inode *inode, void *data)
{
	ceph_set_ino(inode, *(__u64 *)data);
	return 0;
}

static inline u64 ceph_ino(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	return ci->i_ceph_ino;
}

static inline int ceph_ino_compare(struct inode *inode, void *data)
{
	__u64 ino = *(__u64 *)data;
	struct ceph_inode_info *ci = ceph_inode(inode);
	return (ci->i_ceph_ino == ino);
}

/*
 * caps helpers
 */
extern int __ceph_caps_issued(struct ceph_inode_info *ci);

static inline int ceph_caps_issued(struct ceph_inode_info *ci)
{
	int issued;
	spin_lock(&ci->vfs_inode.i_lock);
	issued = __ceph_caps_issued(ci);
	spin_unlock(&ci->vfs_inode.i_lock);
	return issued;
}

static inline int __ceph_caps_used(struct ceph_inode_info *ci)
{
	int used = 0;
	if (ci->i_rd_ref)
		used |= CEPH_CAP_RD;
	if (ci->i_rdcache_ref)
		used |= CEPH_CAP_RDCACHE;
	if (ci->i_wr_ref)
		used |= CEPH_CAP_WR;
	if (ci->i_wrbuffer_ref)
		used |= CEPH_CAP_WRBUFFER;
	return used;
}

static inline int __ceph_caps_file_wanted(struct ceph_inode_info *ci)
{
	int want = 0;
	int mode;
	for (mode = 0; mode < 4; mode++)
		if (ci->i_nr_by_mode[mode])
			want |= ceph_caps_for_mode(mode);
	return want;
}

static inline int __ceph_caps_wanted(struct ceph_inode_info *ci)
{
	int w = __ceph_caps_file_wanted(ci) | __ceph_caps_used(ci);
	if (w & CEPH_CAP_WRBUFFER)
		w |= CEPH_CAP_EXCL;  /* want EXCL if we have dirty data */
	return w;
}

static inline void __ceph_get_fmode(struct ceph_inode_info *ci, int mode) 
{
	ci->i_nr_by_mode[mode]++;
}
extern void ceph_put_fmode(struct ceph_inode_info *ci, int mode);

static inline struct ceph_client *ceph_inode_to_client(struct inode *inode)
{
	return (struct ceph_client *)inode->i_sb->s_fs_info;
}

static inline struct ceph_client *ceph_sb_to_client(struct super_block *sb)
{
	return (struct ceph_client *)sb->s_fs_info;
}
/*
 * keep readdir buffers attached to file->private_data
 */
struct ceph_file_info {
	int mode;      /* initialized on open */
	u32 frag;      /* one frag at a time; screw seek_dir() on large dirs */
	struct ceph_mds_request *last_readdir;
};


/*
 * calculate the number of pages a given length and offset map onto,
 * if we align the data.
 */
static inline int calc_pages_for(int off, int len)
{
	return ((off+len+PAGE_CACHE_SIZE-1) >> PAGE_CACHE_SHIFT) - 
		(off >> PAGE_CACHE_SHIFT);
	/*
	int nr = 0;
	if (len == 0)
		return 0;
	len += off & ~PAGE_MASK;
	nr += len >> PAGE_SHIFT;
	if (len & ~PAGE_MASK)
		nr++;
	return nr;
	*/
}


/* client.c */
extern struct ceph_client *ceph_create_client(struct ceph_mount_args *args,
					      struct super_block *sb);
extern void ceph_destroy_client(struct ceph_client *cl);
extern int ceph_mount(struct ceph_client *client, 
		      struct ceph_mount_args *args, 
		      struct vfsmount *mnt);
extern void ceph_umount_start(struct ceph_client *cl);
extern const char *ceph_msg_type_name(int type);


/* inode.c */
extern struct inode *ceph_get_inode(struct super_block *sb, u64 ino);
extern int ceph_fill_inode(struct inode *inode,
			   struct ceph_mds_reply_inode *info);
extern int ceph_fill_trace(struct super_block *sb, 
			   struct ceph_mds_request *req,
			   struct ceph_mds_session *session);
extern int ceph_readdir_prepopulate(struct ceph_mds_request *req);

extern void ceph_update_inode_lease(struct inode *inode, 
				    struct ceph_mds_reply_lease *lease,
				    struct ceph_mds_session *seesion,
				    unsigned long from_time);
extern void ceph_update_dentry_lease(struct dentry *dentry, 
				     struct ceph_mds_reply_lease *lease,
				     struct ceph_mds_session *session,
				     unsigned long from_time);
extern int ceph_inode_lease_valid(struct inode *inode, int mask);
extern int ceph_dentry_lease_valid(struct dentry *dentry);

extern struct ceph_inode_cap *ceph_add_cap(struct inode *inode,
					   struct ceph_mds_session *session,
					   int fmode,
					   u32 cap, u32 seq);
extern void ceph_remove_cap(struct ceph_inode_cap *cap);
extern void ceph_remove_all_caps(struct ceph_inode_info *ci);
extern int ceph_handle_cap_grant(struct inode *inode,
				 struct ceph_mds_file_caps *grant,
				 struct ceph_mds_session *session);
extern int ceph_handle_cap_trunc(struct inode *inode,
				 struct ceph_mds_file_caps *grant,
				 struct ceph_mds_session *session);
extern int ceph_get_cap_refs(struct ceph_inode_info *ci, int need, int want, int *got, loff_t offset);
extern void ceph_take_cap_refs(struct ceph_inode_info *ci, int got);
extern void ceph_put_cap_refs(struct ceph_inode_info *ci, int had);
extern void ceph_put_wrbuffer_cap_refs(struct ceph_inode_info *ci, int nr);
extern void ceph_cap_delayed_work(struct work_struct *work);
extern void ceph_check_caps(struct ceph_inode_info *ci, int was_last);
extern void ceph_inode_set_size(struct inode *inode, loff_t size);
extern void ceph_inode_writeback(struct work_struct *work);

extern int ceph_setattr(struct dentry *dentry, struct iattr *attr);
extern int ceph_inode_getattr(struct vfsmount *mnt, struct dentry *dentry,
			      struct kstat *stat);

/* addr.c */
extern const struct address_space_operations ceph_aops;

/* file.c */
extern const struct inode_operations ceph_file_iops;
extern const struct file_operations ceph_file_fops;
extern const struct address_space_operations ceph_aops;
extern int ceph_open(struct inode *inode, struct file *file);
extern int ceph_lookup_open(struct inode *dir, struct dentry *dentry,
			    struct nameidata *nd, int mode);
extern int ceph_release(struct inode *inode, struct file *filp);
extern int ceph_inode_revalidate(struct inode *inode, int mask);


/* dir.c */
extern const struct inode_operations ceph_dir_iops;
extern const struct file_operations ceph_dir_fops;
extern struct dentry_operations ceph_dentry_ops;

extern char *ceph_build_dentry_path(struct dentry *dentry, int *len);
extern int ceph_do_lookup(struct super_block *sb, struct dentry *dentry, int m);

static inline void ceph_init_dentry(struct dentry *dentry) {
	dentry->d_op = &ceph_dentry_ops;
	dentry->d_time = 0;
}

/* proc.c */
extern void ceph_fs_proc_init(void);

#endif /* _FS_CEPH_CEPH_H */
