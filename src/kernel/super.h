#ifndef _FS_CEPH_SUPER_H
#define _FS_CEPH_SUPER_H

#include <linux/ceph_fs.h>
#include <linux/fs.h>

#include "client.h"

extern int ceph_debug;
extern int ceph_debug_msgr;
extern int ceph_debug_mdsc;
extern int ceph_debug_osdc;

# define dout(x, args...) do { if (x <= (ceph_debug ? ceph_debug : DOUT_VAR)) printk(KERN_INFO "ceph_" DOUT_PREFIX args); } while (0);
# define derr(x, args...) do { if (x <= (ceph_debug ? ceph_debug : DOUT_VAR)) printk(KERN_ERR "ceph_" DOUT_PREFIX args); } while (0);


#define CEPH_SUPER_MAGIC 0xc364c0de  /* whatev */

/*
 * mount options
 */
#define CEPH_MOUNT_FSID     1
#define CEPH_MOUNT_NOSHARE  2  /* don't share client with other mounts */

struct ceph_mount_args {
	int mntflags;
	int flags;
	struct ceph_fsid fsid;
	struct ceph_entity_addr my_addr;
	int num_mon;
	struct ceph_entity_addr mon_addr[5];
	int mon_port;
	char path[100];
};


/*
 * CEPH per-mount superblock info
 */
struct ceph_super_info {
	struct ceph_mount_args mount_args;
	struct ceph_client *sb_client;
	
	/* FIXME: ptr to inode of my relative offset into the filesystem,
	   so we can appropriately mangle/adjust path names in requests, etc...? */
};

static inline struct ceph_super_info *ceph_sbinfo(struct super_block *sb)
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
	struct list_head session_caps;  /* per-session caplist */
};

struct ceph_inode_frag_map_item {
	u32 frag;
	u32 mds;
};

#define STATIC_CAPS 2
struct ceph_inode_info {
	struct ceph_file_layout i_layout;

	struct ceph_frag_tree_head *i_fragtree, i_fragtree_static[1];
	int i_frag_map_nr;
	struct ceph_inode_frag_map_item *i_frag_map, i_frag_map_static[1];
	
	int i_nr_caps, i_max_caps;
	struct ceph_inode_cap *i_caps;
	struct ceph_inode_cap i_caps_static[STATIC_CAPS];
	atomic_t i_cap_count;  /* ref count (e.g. from file*) */

	int i_cap_wanted;
	int i_cap_issued;
	loff_t i_wr_size;
	struct timespec i_wr_mtime;
	
	struct inode vfs_inode; /* at end */
};

static inline struct ceph_inode_info *ceph_inode(struct inode *inode)
{
	return list_entry(inode, struct ceph_inode_info, vfs_inode);
}

static inline struct ceph_client *ceph_inode_to_client(struct inode *inode)
{
	return ((struct ceph_super_info*)inode->i_sb->s_fs_info)->sb_client;
}

/*
 * keep readdir buffers attached to file->private_data
 */
struct ceph_file_info {
	u32 frag;      /* just one frag at a time; screw seek_dir() on large dirs */
	struct ceph_mds_reply_info rinfo;
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
	if (off + len < PAGE_SIZE)
		return 1;
	if (off) {
		nr++;
		len -= off;
	}
	nr += len >> PAGE_SHIFT;
	if (len & ~PAGE_MASK)
		nr++;
	return nr;
}




/* inode.c */
extern int ceph_fill_inode(struct inode *inode, struct ceph_mds_reply_inode *info);
extern struct ceph_inode_cap *ceph_find_cap(struct inode *inode, int want);
extern struct ceph_inode_cap *ceph_add_cap(struct inode *inode, int mds, u32 cap, u32 seq);
extern int ceph_inode_getattr(struct vfsmount *mnt, struct dentry *dentry,
			      struct kstat *stat);


/* addr.c */
extern const struct address_space_operations ceph_aops;

/* file.c */
extern const struct inode_operations ceph_file_iops;
extern const struct file_operations ceph_file_fops;
extern const struct address_space_operations ceph_aops;
extern int ceph_open(struct inode *inode, struct file *file);
extern int ceph_release(struct inode *inode, struct file *filp);

/* dir.c */
extern const struct inode_operations ceph_dir_iops;
extern const struct file_operations ceph_dir_fops;
extern int ceph_get_dentry_path(struct dentry *dn, char *buf, struct dentry *base);  /* move me */
extern int ceph_build_dentry_path(struct dentry *dentry, char **path, int *len);


#endif /* _FS_CEPH_CEPH_H */
