#include <linux/ctype.h>
#include <linux/debugfs.h>
#include <linux/seq_file.h>

#include "super.h"

static struct dentry *ceph_debugfs_dir = NULL;
static struct dentry *ceph_debugfs_debug = NULL;
static struct dentry *ceph_debugfs_debug_msgr = NULL;
static struct dentry *ceph_debugfs_debug_console = NULL;
static struct dentry *ceph_debugfs_debug_mask = NULL;

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

static ssize_t debug_mask_store(struct file *file,
		const char __user *buffer, size_t count, loff_t *data)
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

int ceph_debugfs_init(void)
{
	int ret = 0;

	ceph_debugfs_dir = debugfs_create_dir("ceph", NULL);

	if (!ceph_debugfs_dir)
		goto out;

	ceph_debugfs_debug = debugfs_create_u32("debug",
					0600,
					ceph_debugfs_dir,
					&ceph_debug);
	if (!ceph_debugfs_debug)
		goto out;

	ceph_debugfs_debug_msgr = debugfs_create_u32("msgr",
					0600,
					ceph_debugfs_dir,
					&ceph_debug_msgr);
	if (!ceph_debugfs_debug_msgr)
		goto out;

	ceph_debugfs_debug_console = debugfs_create_u32("console",
					0600,
					ceph_debugfs_dir,
					&ceph_debug_console);
	if (!ceph_debugfs_debug_console)
		goto out;

	ceph_debugfs_debug_mask = debugfs_create_file("mask",
					0600,
					ceph_debugfs_dir,
					NULL,
					&ceph_debug_mask_fops);
	if (!ceph_debugfs_debug_mask)
		goto out;

	return 0;

out:
	if (ceph_debugfs_debug_mask)
		debugfs_remove(ceph_debugfs_debug_console);
	if (ceph_debugfs_debug_console)
		debugfs_remove(ceph_debugfs_debug_console);
	if (ceph_debugfs_debug_msgr)
		debugfs_remove(ceph_debugfs_debug_msgr);
	if (ceph_debugfs_debug)
		debugfs_remove(ceph_debugfs_debug);
	if (ceph_debugfs_dir)
		debugfs_remove(ceph_debugfs_dir);

	return ret;
}

void ceph_debugfs_cleanup(void)
{
	debugfs_remove(ceph_debugfs_debug_mask);
	debugfs_remove(ceph_debugfs_debug_console);
	debugfs_remove(ceph_debugfs_debug_msgr);
	debugfs_remove(ceph_debugfs_debug);
	debugfs_remove(ceph_debugfs_dir);
}
