#ifndef _FS_CEPH_SUPER_H
#define _FS_CEPH_SUPER_H

#include <linux/ceph_fs.h>
#include <linux/fs.h>
#include <linux/wait.h>
#include <linux/completion.h>

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

extern int ceph_lookup_cache;

#define dout(x, args...) do {						\
		if ((ceph_debug >= 0 && x <= ceph_debug) ||		\
		    (ceph_debug == 0 && x <= DOUT_VAR))			\
			printk(KERN_INFO "ceph_" DOUT_PREFIX args);	\
	} while (0)
#define derr(x, args...) do {						\
		if ((ceph_debug >= 0 && x <= ceph_debug) ||		\
		    (ceph_debug == 0 && x <= DOUT_VAR))			\
			printk(KERN_ERR "ceph_" DOUT_PREFIX args);	\
	} while (0)


#define CEPH_SUPER_MAGIC 0xc364c0de  /* whatev */

#define CACHE_HZ		(1*HZ)

#define IPQUADPORT(n)							\
	(unsigned int)((n.sin_addr.s_addr)>>24)&0xFF,			\
		(unsigned int)((n.sin_addr.s_addr)>>16)&0xFF,		\
		(unsigned int)((n.sin_addr.s_addr)>>8)&0xFF,		\
		(unsigned int)((n.sin_addr.s_addr)&0xFF),		\
		(unsigned int)(ntohs(n.sin_port))

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
};


enum {
	MOUNTING,
	MOUNTED,
	UNMOUNTING,
	UNMOUNTED
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
	struct completion mount_completion;

	struct ceph_messenger *msgr;   /* messenger instance */
	struct ceph_mon_client monc;
	struct ceph_mds_client mdsc;
	struct ceph_osd_client osdc;

	/* lets ignore all this until later */
	spinlock_t sb_lock;
	int num_sb;      /* ref count (for each sb_info that points to me) */
	struct list_head sb_list;
};


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
	int mds;
	int caps;
	u64 seq;
	int flags;  /* stale, etc.? */
	struct ceph_inode_info *ci;
	struct ceph_mds_session *session;
	struct list_head session_caps;  /* per-session caplist */
};

struct ceph_inode_frag_map_item {
	u32 frag;
	u32 mds;
};

#define STATIC_CAPS 2

enum {
	FILE_MODE_PIN,
	FILE_MODE_RDONLY,
	FILE_MODE_RDWR,
	FILE_MODE_WRONLY
};
struct ceph_inode_info {
	u64 i_ceph_ino;

	struct ceph_file_layout i_layout;

	char *i_symlink;

	struct ceph_frag_tree_head *i_fragtree, i_fragtree_static[1];
	int i_frag_map_nr;
	struct ceph_inode_frag_map_item *i_frag_map, i_frag_map_static[1];

	int i_nr_caps, i_max_caps;
	struct ceph_inode_cap *i_caps;
	struct ceph_inode_cap i_caps_static[STATIC_CAPS];
	atomic_t i_cap_count;  /* ref count (e.g. from file*) */

	int i_nr_by_mode[4];
	int i_cap_wanted;
	loff_t i_max_size;     /* size authorized by mds */
	loff_t i_wr_size;      /* largest offset we've written (+1) */
	struct timespec i_wr_mtime;
	struct timespec i_old_atime;

	unsigned long i_hashval;

	unsigned long time;

	struct inode vfs_inode; /* at end */
};

static inline struct ceph_inode_info *ceph_inode(struct inode *inode)
{
	return list_entry(inode, struct ceph_inode_info, vfs_inode);
}

/*
 * ino_t is <64 bits on many architectures... blech
 */
static inline ino_t ceph_ino_to_ino(u64 cephino)
{
	ino_t ino = (ino_t)cephino;
	if (sizeof(ino_t) < sizeof(u64))
		ino ^= cephino >> (sizeof(u64)-sizeof(ino_t)) * 8;
	return ino;
}
static inline void ceph_set_ino(struct inode *inode, __u64 ino)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	ci->i_ceph_ino = ino;
	inode->i_ino = ceph_ino_to_ino(ino);
}
static inline u64 ceph_ino(struct inode *inode)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	return ci->i_ceph_ino;
}

/*
 * caps helpers
 */
static inline int ceph_caps_issued(struct ceph_inode_info *ci)
{
	int i, issued = 0;
	for (i = 0; i < ci->i_nr_caps; i++)
		issued |= ci->i_caps[i].caps;
	return issued;
}

static inline int ceph_caps_wanted(struct ceph_inode_info *ci)
{
	int want = 0;
	if (ci->i_nr_by_mode[0])
		want |= CEPH_CAP_PIN;
	if (ci->i_nr_by_mode[1])
		want |= CEPH_CAP_RD|CEPH_CAP_RDCACHE;
	if (ci->i_nr_by_mode[2])
		want |= CEPH_CAP_RD|CEPH_CAP_RDCACHE|
			CEPH_CAP_WR|CEPH_CAP_WRBUFFER;
	if (ci->i_nr_by_mode[3])
		want |= CEPH_CAP_WR|CEPH_CAP_WRBUFFER;
	return want;
}

static inline int ceph_caps_used(struct ceph_inode_info *ci)
{
	return 0;  /* FIXME */
}

static inline int ceph_file_mode(int flags)
{
	if ((flags & O_DIRECTORY) == O_DIRECTORY)
		return FILE_MODE_PIN;
	if ((flags & O_ACCMODE) == O_RDWR)
		return FILE_MODE_RDWR;
	if ((flags & O_ACCMODE) == O_WRONLY)
		return FILE_MODE_WRONLY;
	if ((flags & O_ACCMODE) == O_RDONLY)
		return FILE_MODE_RDONLY;
	BUG_ON(1);
}

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
static inline int calc_pages_for(int len, int off)
{
	int nr = 0;
	if (len == 0)
		return 0;
	len += off & ~PAGE_MASK;
	nr += len >> PAGE_SHIFT;
	if (len & ~PAGE_MASK)
		nr++;
	return nr;
}


/* client.c */
extern struct ceph_client *ceph_create_client(struct ceph_mount_args *args,
					      struct super_block *sb);
extern void ceph_destroy_client(struct ceph_client *cl);
extern int ceph_mount(struct ceph_client *client, struct ceph_mount_args *args,
		      struct dentry **pmnt_root);
extern const char *ceph_msg_type_name(int type);


/* inode.c */
extern int ceph_get_inode(struct super_block *sb, u64 ino,
			  struct inode **pinode);
extern int ceph_fill_inode(struct inode *inode,
			   struct ceph_mds_reply_inode *info);
extern struct ceph_inode_cap *ceph_find_cap(struct inode *inode, int want);
extern struct ceph_inode_cap *ceph_add_cap(struct inode *inode,
					   struct ceph_mds_session *session,
					   u32 cap, u32 seq);
extern void ceph_remove_cap(struct ceph_inode_info *ci, int mds);
extern void ceph_remove_caps(struct ceph_inode_info *ci);
extern int ceph_handle_cap_grant(struct inode *inode,
				 struct ceph_mds_file_caps *grant,
				 struct ceph_mds_session *session);
extern int ceph_handle_cap_trunc(struct inode *inode,
				 struct ceph_mds_file_caps *grant,
				 struct ceph_mds_session *session);

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
			    struct nameidata *nd);
extern int ceph_release(struct inode *inode, struct file *filp);
extern int ceph_inode_revalidate(struct dentry *dentry);


/* dir.c */
extern const struct inode_operations ceph_dir_iops;
extern const struct file_operations ceph_dir_fops;
extern char *ceph_build_dentry_path(struct dentry *dentry, int *len);
extern int ceph_fill_trace(struct super_block *sb,
			   struct ceph_mds_reply_info *prinfo,
			   struct inode **lastinode,
			   struct dentry **lastdentry);
extern int ceph_request_lookup(struct super_block *sb, struct dentry *dentry);
extern void ceph_touch_dentry(struct dentry *dentry);

/* proc.c */
extern void ceph_fs_proc_init(void);

#endif /* _FS_CEPH_CEPH_H */
