#include <linux/ceph_fs.h>
#include <linux/module.h>
#include <linux/proc_fs.h>
#include <asm/uaccess.h>

#include "super.h"

static int ceph_debug_level_read(char *page, char **start, off_t off,
		       int count, int *eof, void *data)
{
	int len;

	len = sprintf(page, "%d\n", ceph_debug);

	if ((len < 0) || (len <= off)) {
		*start = page;
		*eof = 1;
		return 0;
	}

	len -= off;

	*start = page + off;

	if (len > count)
		len = count;
	else
		*eof = 1;

	return len;
}

static int ceph_debug_level_write(struct file *file, const char __user *buffer,
				unsigned long count, void *data)
{
#define PROC_STR_LEN	16
	char level_str[PROC_STR_LEN];
	int new_dl;

	if ((count < 1) || (count > sizeof(level_str)-1))
		return -EINVAL;

	level_str[PROC_STR_LEN-1] = 0;

	if (copy_from_user(level_str, buffer, count))
		return -EFAULT;

	level_str[count] = 0;

	new_dl = simple_strtoul(level_str, NULL, 0);

	if (new_dl >= 0)
		ceph_debug = new_dl;
	else
		return -EINVAL;

	return count;
}

static struct proc_dir_entry *proc_fs_ceph;

void ceph_fs_proc_init(void)
{
	struct proc_dir_entry *pde;

	proc_fs_ceph = proc_mkdir("ceph", proc_root_fs);

	if (!proc_fs_ceph)
		return;

	proc_fs_ceph->owner = THIS_MODULE;
	pde = create_proc_read_entry("debug_level", 0, 
				proc_fs_ceph, ceph_debug_level_read, NULL);

	if (pde)
		pde->write_proc = ceph_debug_level_write;

}
