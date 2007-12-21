int ceph_dir_debug = 50;
#define DOUT_VAR ceph_dir_debug
#define DOUT_PREFIX "dir: "
#include "super.h"

const struct inode_operations ceph_dir_iops;
const struct file_operations ceph_dir_fops;

/*
 * ugly hack.  
 * - no locking.  
 * - should stop at mount root or current's CWD?
 */
static int get_dentry_path(struct dentry *dn, char *buf, struct dentry *base)
{
	int len;
	dout(20, "get_dentry_path in dn %p bas %p\n", dn, base);
	if (dn == base) 
		return 0;	

	len = get_dentry_path(dn->d_parent, buf, base);
	dout(20, "get_dentry_path out dn %p bas %p len %d adding %s\n", 
	     dn, base, len, dn->d_name.name);

	buf[len++] = '/';
	memcpy(buf+len, dn->d_name.name, dn->d_name.len);
	len += dn->d_name.len;
	buf[len] = 0;
	return len;
}

struct ceph_inode_cap *ceph_open(struct inode *inode, struct file *file, int flags)
{
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	ceph_ino_t pathbase;
	char path[PATH_MAX];
	int pathlen;
	struct ceph_msg *req;
	struct ceph_mds_request_head *rhead;
	struct ceph_mds_reply_info rinfo;
	struct dentry *dentry;
	int frommds;
	struct ceph_inode_cap *cap;
	int err;

	dentry = list_entry(inode->i_dentry.next, struct dentry, d_alias);

	dout(5, "open inode %p dentry %p name '%s' flags %d\n", inode, dentry, dentry->d_name.name, flags);
	pathbase = inode->i_sb->s_root->d_inode->i_ino;
	pathlen = get_dentry_path(dentry, path, inode->i_sb->s_root);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_OPEN, pathbase, path, 0, 0);
	if (IS_ERR(req)) 
		return ERR_PTR(PTR_ERR(req));
	rhead = req->front.iov_base;
	rhead->args.open.flags = cpu_to_le32(flags);
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return ERR_PTR(err);
	
	dout(10, "open got and parsed result\n");
	frommds = rinfo.reply->hdr.src.name.num;
	cap = ceph_add_cap(inode, frommds, 
			   le32_to_cpu(rinfo.head->file_caps), 
			   le32_to_cpu(rinfo.head->file_caps_seq));
	return cap;
}

static int ceph_dir_open(struct inode *inode, struct file *file)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_inode_cap *cap;
	struct ceph_file_info *fi;

	dout(5, "dir_open inode %p (%lu) file %p\n", inode, inode->i_ino, file);
	cap = ceph_find_cap(inode, 0);
	if (!cap) {
		cap = ceph_open(inode, file, O_DIRECTORY);
		if (IS_ERR(cap))
			return PTR_ERR(cap);
	}
	
	fi = kzalloc(sizeof(*fi), GFP_KERNEL);
	if (fi == NULL)
		return -ENOMEM;
	file->private_data = fi;

	atomic_inc(&ci->i_cap_count);
	dout(5, "open_dir success\n");
	return 0;
}

/*
 * build fpos from fragment id and offset within that fragment.
 */
static loff_t make_fpos(u32 frag, u32 off)
{
	return ((loff_t)frag << 32) | (loff_t)off;
}
static u32 fpos_frag(loff_t p)
{
	return p >> 32;
}
static u32 fpos_off(loff_t p)
{
	return p & 0xffffffff;
}

static int ceph_dir_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	struct ceph_file_info *fi = filp->private_data;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(filp->f_dentry->d_inode)->mdsc;
	u32 frag = fpos_frag(filp->f_pos);
	u32 off = fpos_off(filp->f_pos);
	int err;
	int i;
	struct qstr dname;
	struct dentry *parent, *dn;
	struct inode *in;

nextfrag:
	dout(5, "dir_readdir filp %p at frag %u off %u\n", filp, frag, off);
	if (fi->frag != frag || fi->rinfo.reply == NULL) {
		struct ceph_msg *req;
		struct ceph_mds_request_head *rhead;

		/* query mds */
		if (fi->rinfo.reply) 
			ceph_mdsc_destroy_reply_info(&fi->rinfo);
		
		dout(10, "dir_readdir querying mds for ino %lu frag %u\n", filp->f_dentry->d_inode->i_ino, frag);
		req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_READDIR, 
					       filp->f_dentry->d_inode->i_ino, "", 0, 0);
		if (IS_ERR(req)) 
			return PTR_ERR(req);
		rhead = req->front.iov_base;
		rhead->args.readdir.frag = cpu_to_le32(frag);
		if ((err = ceph_mdsc_do_request(mdsc, req, &fi->rinfo, -1)) < 0)
		    return err;
		dout(10, "dir_readdir got and parsed readdir result on frag %u\n", frag);
		
		/* pre-populate dentry cache */
		parent = filp->f_dentry;
		for (i=0; i<fi->rinfo.dir_nr; i++) {
			dname.name = fi->rinfo.dir_dname[i];
			dname.len = fi->rinfo.dir_dname_len[i];
			dname.hash = full_name_hash(dname.name, dname.len);
			dn = d_alloc(parent, &dname);
			if (dn == NULL) {
				dout(30, "d_alloc badness\n");
				break; 
			}
			in = new_inode(parent->d_sb);
			if (in == NULL) {
				dout(30, "new_inode badness\n");
				d_delete(dn);
				break;
			}
			if (ceph_fill_inode(in, fi->rinfo.dir_in[i].in) < 0) {
				dout(30, "ceph_fill_inode badness\n");
				iput(in);
				d_delete(dn);
				break;
			}
			d_add(dn, in);
			dout(10, "dir_readdir added dentry %p inode %lu %d/%d\n",
			     dn, in->i_ino, i, fi->rinfo.dir_nr);
		}
	}	
	
	while (off < fi->rinfo.dir_nr) {
		dout(10, "dir_readdir off %d / %d name '%s'\n", off, fi->rinfo.dir_nr, fi->rinfo.dir_dname[off]);
		if (filldir(dirent, 
			    fi->rinfo.dir_dname[off], 
			    fi->rinfo.dir_dname_len[off], 
			    make_fpos(frag, off),
			    le64_to_cpu(fi->rinfo.dir_in[off].in->ino), 
			    le32_to_cpu(fi->rinfo.dir_in[off].in->mode >> 12)) < 0) {
			dout(20, "filldir stopping us...\n");
			return 0;
		}
		off++;
		filp->f_pos++;
	}

	/* more frags? */
	if (frag_value(frag) != frag_mask(frag)) {
		frag = frag_next(frag);
		off = 0;
		filp->f_pos = make_fpos(frag, off);
		dout(10, "dir_readdir next frag is %u\n", frag);
		goto nextfrag;
	}
	
	dout(20, "dir_readdir done.\n");
	return 0;
}

int ceph_dir_release(struct inode *inode, struct file *filp)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_file_info *fi = filp->private_data;

	dout(5, "dir_release inode %p filp %p\n", inode, filp);
	atomic_dec(&ci->i_cap_count);

	if (fi->rinfo.reply) 
		ceph_mdsc_destroy_reply_info(&fi->rinfo);
	kfree(fi);

	return 0;
}

const struct file_operations ceph_dir_fops = {
	.read = generic_read_dir,
	.readdir = ceph_dir_readdir,
	.open = ceph_dir_open,
	.release = ceph_dir_release,
};



static struct dentry *ceph_dir_lookup(struct inode *dir, struct dentry *dentry,
				      struct nameidata *nameidata)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(dir->i_sb);
	struct ceph_mds_client *mdsc = &sbinfo->sb_client->mdsc;
	char path[200];
	int pathlen;
	struct ceph_msg *req;
	struct ceph_mds_reply_info rinfo;
	struct inode *inode;
	int err;
	ino_t ino;

	dout(5, "dir_lookup inode %p dentry %p '%s'\n", dir, dentry, dentry->d_name.name);
	pathlen = get_dentry_path(dentry, path, dir->i_sb->s_root);

	/* stat mds */
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_STAT, 
				       dir->i_sb->s_root->d_inode->i_ino, path, 0, 0);
	if (IS_ERR(req)) 
		return ERR_PTR(PTR_ERR(req));
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return ERR_PTR(err);

	ino = le64_to_cpu(rinfo.trace_in[rinfo.trace_nr-1].in->ino);
	dout(10, "got and parsed stat result, ino %lu\n", ino);
	inode = iget(dir->i_sb, ino);
	if (!inode)
		return ERR_PTR(-EACCES);
	if ((err = ceph_fill_inode(inode, rinfo.trace_in[rinfo.trace_nr-1].in)) < 0) 
		return ERR_PTR(err);
	d_add(dentry, inode);
	return NULL;
}



static int ceph_dir_mkdir(struct inode *dir, struct dentry *dentry, int mode)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(dir->i_sb);
	struct ceph_mds_client *mdsc = &sbinfo->sb_client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_msg *req;
	struct ceph_mds_request_head *rhead;
	struct ceph_mds_reply_info rinfo;
	char path[200];
	int pathlen;
	int err;

	dout(5, "dir_mkdir dir %p dentry %p mode %d\n", dir, dentry, mode);
	pathlen = get_dentry_path(dentry, path, dir->i_sb->s_root);
	
	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_MKDIR, 
				       dir->i_sb->s_root->d_inode->i_ino, path, 0, 0);
	if (IS_ERR(req)) 
		return PTR_ERR(req);
	rhead = req->front.iov_base;
	rhead->args.mkdir.mode = cpu_to_le32(mode);	
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return err;
	
	err = le32_to_cpu(rinfo.head->result);
	if (err == 0) {
		inode_dec_link_count(inode);
		/* FIXME update dir mtime etc. from reply trace */
	}
	return err;
}


static int ceph_dir_unlink(struct inode *dir, struct dentry *dentry)
{
	struct ceph_super_info *sbinfo = ceph_sbinfo(dir->i_sb);
	struct ceph_mds_client *mdsc = &sbinfo->sb_client->mdsc;
	struct inode *inode = dentry->d_inode;
	struct ceph_msg *req;
	struct ceph_mds_reply_info rinfo;
	char path[200];
	int pathlen;
	int err;

	dout(5, "dir_unlink dir %p dentry %p inode %p\n", dir, dentry, inode);
	pathlen = get_dentry_path(dentry, path, dir->i_sb->s_root);

	req = ceph_mdsc_create_request(mdsc, CEPH_MDS_OP_UNLINK, 
				       dir->i_sb->s_root->d_inode->i_ino, path, 0, 0);
	if (IS_ERR(req)) 
		return PTR_ERR(req);
	if ((err = ceph_mdsc_do_request(mdsc, req, &rinfo, -1)) < 0)
		return err;
	
	err = le32_to_cpu(rinfo.head->result);
	if (err == 0) {
		inode_dec_link_count(inode);
		/* FIXME update dir mtime etc. from reply trace */
	}
	return err;
}


/*


static int ceph_dir_rmdir(struct inode *i, struct dentry *d)
{
}

static int
ceph_dir_create(struct inode *dir, struct dentry *dentry, int mode,
		struct nameidata *nd)
{
}




static int ceph_dir_rename(struct inode *old_dir, struct dentry *old_dentry,
			   struct inode *new_dir, struct dentry *new_dentry)
{
}
*/

const struct inode_operations ceph_dir_iops = {
	.lookup = ceph_dir_lookup,
//	.getattr = ceph_inode_getattr,
	.unlink = ceph_dir_unlink,
	.mkdir = ceph_dir_mkdir,
//	.rmdir = ceph_vfs_rmdir,
/*	.create = ceph_dir_create,
	.mknod = ceph_vfs_mknod,
	.rename = ceph_vfs_rename,
	.getattr = ceph_vfs_getattr,
	.setattr = ceph_vfs_setattr,
*/
};

