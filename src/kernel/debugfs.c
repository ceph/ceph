#include <linux/module.h>
#include <linux/ctype.h>
#include <linux/debugfs.h>
#include <linux/seq_file.h>

#include "super.h"
#include "mds_client.h"

static struct dentry *ceph_debugfs_dir;
static struct dentry *ceph_debugfs_debug;
static struct dentry *ceph_debugfs_debug_msgr;
static struct dentry *ceph_debugfs_debug_console;
static struct dentry *ceph_debugfs_debug_mask;
static struct dentry *ceph_debugfs_caps_reservation;

/*
 * ceph_debug_mask
 */
struct _debug_mask_name {
	int mask;
	char *name;
};

static struct _debug_mask_name _debug_mask_names[] = {
		{DOUT_MASK_ADDR, "addr"},
		{DOUT_MASK_CAPS, "caps"},
		{DOUT_MASK_DIR, "dir"},
		{DOUT_MASK_EXPORT, "export"},
		{DOUT_MASK_FILE, "file"},
		{DOUT_MASK_INODE, "inode"},
		{DOUT_MASK_IOCTL, "ioctl"},
		{DOUT_MASK_MDSC, "mdsc"},
		{DOUT_MASK_MDSMAP, "mdsmap"},
		{DOUT_MASK_MSGR, "msgr"},
		{DOUT_MASK_MON, "mon"},
		{DOUT_MASK_OSDC, "osdc"},
		{DOUT_MASK_OSDMAP, "osdmap"},
		{DOUT_MASK_SNAP, "snap"},
		{DOUT_MASK_SUPER, "super"},
		{DOUT_MASK_PROTOCOL, "protocol"},
		{DOUT_MASK_PROC, "proc"},
		{DOUT_MASK_TOOLS, "tools"},
		{0, NULL}
};

static int debug_mask_show(struct seq_file *s, void *p)
{
	int i = 0;
	seq_printf(s, "0x%x", ceph_debug_mask);

	while (_debug_mask_names[i].mask) {
		if (ceph_debug_mask & _debug_mask_names[i].mask)
			seq_printf(s, " %s",
				       _debug_mask_names[i].name);
		i++;
	}
	seq_printf(s, "\n");
	return 0;
}

static int get_debug_mask(const char *name, int len)
{
	int i = 0;

	while (_debug_mask_names[i].name) {
		if (strncmp(_debug_mask_names[i].name, name, len) == 0)
			return _debug_mask_names[i].mask;
		i++;
	}
	return 0;
}

static ssize_t debug_mask_store(struct file *file, const char __user *buffer,
				size_t count, loff_t *data)
{
	char *next, *tok;
	char *buf;

	if (count > PAGE_SIZE)
		return -EINVAL;

	buf = kmalloc(count + 1, GFP_KERNEL);

	if (copy_from_user(buf, buffer, count))
		return -EFAULT;

	buf[count] = '\0';

	next = buf;

	while (1) {
		tok = next;
		next = strpbrk(tok, " \t\r\n");
		if (!next)
			break;
		if (isdigit(*tok)) {
			ceph_debug_mask = simple_strtol(tok, NULL, 0);
		} else {
			int remove = 0;
			int mask;

			if (*tok == '-') {
				remove = 1;
				tok++;
			} else if (*tok == '+')
				tok++;
			mask = get_debug_mask(tok, next-tok);
			if (mask) {
				if (remove)
					ceph_debug_mask &= ~mask;
				else
					ceph_debug_mask |= mask;
			}
		}
		next++;
	}

	kfree(buf);

	return count;
}

static int debug_mask_open(struct inode *inode, struct file *file)
{
	return single_open(file, debug_mask_show, NULL);
}

static const struct file_operations ceph_debug_mask_fops = {
	.open		= debug_mask_open,
	.read		= seq_read,
	.write		= debug_mask_store,
	.llseek		= seq_lseek,
	.release	= single_release,
};

static int fsid_show(struct seq_file *s, void *p)
{
	struct ceph_client *client = s->private;

	seq_printf(s, "%llx.%llx\n",
	       le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
	       le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));
	return 0;
}

static int monmap_show(struct seq_file *s, void *p)
{
	int i;
	struct ceph_client *client = s->private;

	if (client->monc.monmap == NULL)
		return 0;

	seq_printf(s, "epoch %d\n", client->monc.monmap->epoch);
	for (i = 0; i < client->monc.monmap->num_mon; i++) {
		struct ceph_entity_inst *inst =
			&client->monc.monmap->mon_inst[i];

		seq_printf(s, "\t%s%d\t%u.%u.%u.%u:%u\n",
			       ENTITY_NAME(inst->name),
			       IPQUADPORT(inst->addr.ipaddr));
	}
	return 0;
}

static int mdsmap_show(struct seq_file *s, void *p)
{
	int i;
	struct ceph_client *client = s->private;

	if (client->mdsc.mdsmap == NULL)
		return 0;
	seq_printf(s, "epoch %d\n", client->mdsc.mdsmap->m_epoch);
	seq_printf(s, "root %d\n", client->mdsc.mdsmap->m_root);
	seq_printf(s, "session_timeout %d\n",
		       client->mdsc.mdsmap->m_session_timeout);
	seq_printf(s, "session_autoclose %d\n",
		       client->mdsc.mdsmap->m_session_autoclose);
	for (i = 0; i < client->mdsc.mdsmap->m_max_mds; i++) {
		struct ceph_entity_addr *addr = &client->mdsc.mdsmap->m_addr[i];
		int state = client->mdsc.mdsmap->m_state[i];

		seq_printf(s, "\tmds%d\t%u.%u.%u.%u:%u\t(%s)\n",
			       i,
			       IPQUADPORT(addr->ipaddr),
			       ceph_mds_state_name(state));
	}
	return 0;
}

static int osdmap_show(struct seq_file *s, void *p)
{
	int i;
	struct ceph_client *client = s->private;

	if (client->osdc.osdmap == NULL)
		return 0;
	seq_printf(s, "epoch %d\n", client->osdc.osdmap->epoch);
	seq_printf(s, "flags%s%s\n",
		   (client->osdc.osdmap->flags & CEPH_OSDMAP_NEARFULL) ?
		   " NEARFULL" : "",
		   (client->osdc.osdmap->flags & CEPH_OSDMAP_FULL) ?
		   " FULL" : "");
	for (i = 0; i < client->osdc.osdmap->num_pools; i++) {
		struct ceph_pg_pool_info *pool =
			&client->osdc.osdmap->pg_pool[i];
		seq_printf(s, "pg_pool %d pg_num %d / %d, lpg_num %d / %d\n",
			   i, pool->v.pg_num, pool->pg_num_mask,
			   pool->v.lpg_num, pool->lpg_num_mask);
	}
	for (i = 0; i < client->osdc.osdmap->max_osd; i++) {
		struct ceph_entity_addr *addr =
			&client->osdc.osdmap->osd_addr[i];
		int state = client->osdc.osdmap->osd_state[i];
		char sb[64];

		seq_printf(s,
		       "\tosd%d\t%u.%u.%u.%u:%u\t%3d%%\t(%s)\n",
		       i, IPQUADPORT(addr->ipaddr),
		       ((client->osdc.osdmap->osd_weight[i]*100) >> 16),
		       ceph_osdmap_state_str(sb, sizeof(sb), state));
	}
	return 0;
}

static int monc_show(struct seq_file *s, void *p)
{
	struct ceph_client *client = s->private;
	struct ceph_mon_statfs_request *req;
	u64 nexttid = 0;
	int got;
	struct ceph_mon_client *monc = &client->monc;

	mutex_lock(&monc->statfs_mutex);

	if (monc->want_osdmap)
		seq_printf(s, "want osdmap %u\n", (unsigned)monc->want_osdmap);
	if (monc->want_mdsmap)
		seq_printf(s, "want mdsmap %u\n", (unsigned)monc->want_mdsmap);

	while (nexttid < monc->last_tid) {
		got = radix_tree_gang_lookup(&monc->statfs_request_tree,
					     (void **)&req, nexttid, 1);
		if (got == 0)
			break;
		nexttid = req->tid + 1;

		seq_printf(s, "%u.%u.%u.%u:%u (%s%d)\tstatfs\n",
			IPQUADPORT(req->request->hdr.dst.addr.ipaddr),
			ENTITY_NAME(req->request->hdr.dst.name));
	}
	mutex_unlock(&monc->statfs_mutex);

	return 0;
}

static int mdsc_show(struct seq_file *s, void *p)
{
	struct ceph_client *client = s->private;
	struct ceph_mds_request *req;
	u64 nexttid = 0;
	int got;
	struct ceph_mds_client *mdsc = &client->mdsc;
	int pathlen;
	u64 pathbase;
	char *path;

	mutex_lock(&mdsc->mutex);
	while (nexttid < mdsc->last_tid) {
		got = radix_tree_gang_lookup(&mdsc->request_tree,
					     (void **)&req, nexttid, 1);
		if (got == 0)
			break;
		nexttid = req->r_tid + 1;

		seq_printf(s, "%lld\t%u.%u.%u.%u:%u (%s%d)\t",
			   req->r_tid,
			   IPQUADPORT(req->r_request->hdr.dst.addr.ipaddr),
			   ENTITY_NAME(req->r_request->hdr.dst.name));

		seq_printf(s, "%s", ceph_mds_op_name(req->r_op));

		if (req->r_got_unsafe)
			seq_printf(s, "\t(unsafe)");
		else
			seq_printf(s, "\t");

		if (req->r_inode) {
			seq_printf(s, " #%llx", ceph_ino(req->r_inode));
		} else if (req->r_dentry) {
			path = ceph_mdsc_build_path(req->r_dentry, &pathlen,
						    &pathbase, 0);
			spin_lock(&req->r_dentry->d_lock);
			seq_printf(s, " #%llx/%.*s (%s)",
				   ceph_ino(req->r_dentry->d_parent->d_inode),
				   req->r_dentry->d_name.len,
				   req->r_dentry->d_name.name,
				   path ? path : "");
			spin_unlock(&req->r_dentry->d_lock);
			kfree(path);
		} else if (req->r_path1) {
			seq_printf(s, " #%llx/%s", req->r_ino1.ino,
				   req->r_path1);
		}

		if (req->r_old_dentry) {
			path = ceph_mdsc_build_path(req->r_old_dentry, &pathlen,
						    &pathbase, 0);
			spin_lock(&req->r_old_dentry->d_lock);
			seq_printf(s, " #%llx/%.*s (%s)",
			   ceph_ino(req->r_old_dentry->d_parent->d_inode),
				   req->r_old_dentry->d_name.len,
				   req->r_old_dentry->d_name.name,
				   path ? path : "");
			spin_unlock(&req->r_old_dentry->d_lock);
			kfree(path);
		} else if (req->r_path2) {
			if (req->r_ino2.ino)
				seq_printf(s, " #%llx/%s", req->r_ino2.ino,
					   req->r_path2);
			else
				seq_printf(s, " %s", req->r_path2);
		}

		seq_printf(s, "\n");
	}
	mutex_unlock(&mdsc->mutex);

	return 0;
}

static int osdc_show(struct seq_file *s, void *p)
{
	struct ceph_client *client = s->private;
	struct ceph_osd_client *osdc = &client->osdc;
	u64 nexttid = 0;

	mutex_lock(&osdc->request_mutex);
	while (nexttid < osdc->last_tid) {
		struct ceph_osd_request *req;
		struct ceph_osd_request_head *head;
		struct ceph_osd_op *op;
		int num_ops;
		int opcode, olen;
		int got, i;

		got = radix_tree_gang_lookup(&osdc->request_tree,
					     (void **)&req, nexttid, 1);
		if (got == 0)
			break;

		nexttid = req->r_tid + 1;

		seq_printf(s, "%lld\t%u.%u.%u.%u:%u (%s%d)\t",
			   req->r_tid,
			   IPQUADPORT(req->r_request->hdr.dst.addr.ipaddr),
			   ENTITY_NAME(req->r_request->hdr.dst.name));

		head = req->r_request->front.iov_base;
		op = (void *)(head + 1);

		num_ops = le16_to_cpu(head->num_ops);
		olen = le32_to_cpu(head->object_len);
		seq_printf(s, "%.*s", olen,
			   (const char *)(head->ops + num_ops));

		if (req->r_reassert_version.epoch)
			seq_printf(s, "\t%u'%llu",
			   (unsigned)le32_to_cpu(req->r_reassert_version.epoch),
			   le64_to_cpu(req->r_reassert_version.version));
		else
			seq_printf(s, "\t");

		for (i = 0; i < num_ops; i++) {
			opcode = le16_to_cpu(op->op);
			seq_printf(s, "\t%s", ceph_osd_op_name(opcode));
			op++;
		}

		seq_printf(s, "\n");
	}
	mutex_unlock(&osdc->request_mutex);
	return 0;
}

static int caps_reservation_show(struct seq_file *s, void *p)
{
	int total, avail, used, reserved;

	ceph_reservation_status(&total, &avail, &used, &reserved);

	seq_printf(s, "total\t\t%d\n"
		      "avail\t\t%d\n"
		      "used\t\t%d\n"
		      "reserved\t%d\n",
		   total, avail, used, reserved);
	return 0;
}

static int dentry_lru_show(struct seq_file *s, void *ptr)
{
	struct ceph_client *client = s->private;
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct ceph_dentry_info *di;

	spin_lock(&mdsc->dentry_lru_lock);
	list_for_each_entry(di, &mdsc->dentry_lru, lru) {
		struct dentry *dentry = di->dentry;
		seq_printf(s, "%p %p\t%.*s\n",
			   di, dentry, dentry->d_name.len, dentry->d_name.name);
	}
	spin_unlock(&mdsc->dentry_lru_lock);

	return 0;
}

#define DEFINE_SHOW_FUNC(name) 						\
static int name##_open(struct inode *inode, struct file *file)		\
{									\
	struct seq_file *sf;						\
	int ret;							\
									\
	ret = single_open(file, name, NULL);				\
	sf = file->private_data;					\
	sf->private = inode->i_private;					\
	return ret;							\
}									\
									\
static const struct file_operations name##_fops = {			\
	.open		= name##_open,					\
	.read		= seq_read,					\
	.llseek		= seq_lseek,					\
	.release	= single_release,				\
};

DEFINE_SHOW_FUNC(fsid_show)
DEFINE_SHOW_FUNC(monmap_show)
DEFINE_SHOW_FUNC(mdsmap_show)
DEFINE_SHOW_FUNC(osdmap_show)
DEFINE_SHOW_FUNC(monc_show)
DEFINE_SHOW_FUNC(mdsc_show)
DEFINE_SHOW_FUNC(osdc_show)
DEFINE_SHOW_FUNC(caps_reservation_show)
DEFINE_SHOW_FUNC(dentry_lru_show)

int ceph_debugfs_init(void)
{
	int ret = -ENOMEM;

	ceph_debugfs_dir = debugfs_create_dir("ceph", NULL);

	if (!ceph_debugfs_dir)
		goto out;

	ceph_debugfs_debug = debugfs_create_u32("debug",
					0600,
					ceph_debugfs_dir,
					(u32 *)&ceph_debug);
	if (!ceph_debugfs_debug)
		goto out;

	ceph_debugfs_debug_msgr = debugfs_create_u32("msgr",
					0600,
					ceph_debugfs_dir,
					(u32 *)&ceph_debug_msgr);
	if (!ceph_debugfs_debug_msgr)
		goto out;

	ceph_debugfs_debug_console = debugfs_create_u32("console",
					0600,
					ceph_debugfs_dir,
					(u32 *)&ceph_debug_console);
	if (!ceph_debugfs_debug_console)
		goto out;

	ceph_debugfs_debug_mask = debugfs_create_file("mask",
					0600,
					ceph_debugfs_dir,
					NULL,
					&ceph_debug_mask_fops);
	if (!ceph_debugfs_debug_mask)
		goto out;

	ceph_debugfs_caps_reservation = debugfs_create_file("caps_reservation",
					0400,
					ceph_debugfs_dir,
					NULL,
					&caps_reservation_show_fops);
	if (!ceph_debugfs_caps_reservation)
		goto out;

	return 0;

out:
	ceph_debugfs_cleanup();
	return ret;
}

void ceph_debugfs_cleanup(void)
{
	debugfs_remove(ceph_debugfs_caps_reservation);
	debugfs_remove(ceph_debugfs_debug_console);
	debugfs_remove(ceph_debugfs_debug_mask);
	debugfs_remove(ceph_debugfs_debug_msgr);
	debugfs_remove(ceph_debugfs_debug);
	debugfs_remove(ceph_debugfs_dir);
}

int ceph_debugfs_client_init(struct ceph_client *client)
{
	int ret = 0;
#define TMP_NAME_SIZE 16
	char name[TMP_NAME_SIZE];

	snprintf(name, TMP_NAME_SIZE, "client%d", client->whoami);

	client->debugfs_dir = debugfs_create_dir(name, ceph_debugfs_dir);
	if (!client->debugfs_dir)
		goto out;

	client->monc.debugfs_file = debugfs_create_file("monc",
						      0600,
						      client->debugfs_dir,
						      client,
						      &monc_show_fops);
	if (ret)
		goto out;

	client->mdsc.debugfs_file = debugfs_create_file("mdsc",
						      0600,
						      client->debugfs_dir,
						      client,
						      &mdsc_show_fops);
	if (ret)
		goto out;

	client->osdc.debugfs_file = debugfs_create_file("osdc",
						      0600,
						      client->debugfs_dir,
						      client,
						      &osdc_show_fops);
	if (ret)
		goto out;

	client->debugfs_fsid = debugfs_create_file("fsid",
					0600,
					client->debugfs_dir,
					client,
					&fsid_show_fops);
	if (!client->debugfs_fsid)
		goto out;

	client->debugfs_monmap = debugfs_create_file("monmap",
					0600,
					client->debugfs_dir,
					client,
					&monmap_show_fops);
	if (!client->debugfs_monmap)
		goto out;

	client->debugfs_mdsmap = debugfs_create_file("mdsmap",
					0600,
					client->debugfs_dir,
					client,
					&mdsmap_show_fops);
	if (!client->debugfs_mdsmap)
		goto out;

	client->debugfs_osdmap = debugfs_create_file("osdmap",
					0600,
					client->debugfs_dir,
					client,
					&osdmap_show_fops);
	if (!client->debugfs_osdmap)
		goto out;

	client->debugfs_dentry_lru = debugfs_create_file("dentry_lru",
					0600,
					client->debugfs_dir,
					client,
					&dentry_lru_show_fops);
	if (!client->debugfs_osdmap)
		goto out;

	return 0;

out:
	ceph_debugfs_client_cleanup(client);
	return ret;
}

void ceph_debugfs_client_cleanup(struct ceph_client *client)
{
	debugfs_remove(client->monc.debugfs_file);
	debugfs_remove(client->mdsc.debugfs_file);
	debugfs_remove(client->osdc.debugfs_file);
	debugfs_remove(client->debugfs_dentry_lru);
	debugfs_remove(client->debugfs_monmap);
	debugfs_remove(client->debugfs_mdsmap);
	debugfs_remove(client->debugfs_osdmap);
	debugfs_remove(client->debugfs_fsid);
	debugfs_remove(client->debugfs_dir);
}

