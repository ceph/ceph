#include <linux/spinlock.h>
#include <linux/slab.h>
#include <linux/kallsyms.h>

#define CEPH_OVERRIDE_BOOKKEEPER /* avoid kmalloc/kfree recursion */

#define CEPH_BK_MAGIC 0x140985AC

#include "ceph_debug.h"

int ceph_debug_tools __read_mostly = -1;
#define DOUT_VAR ceph_debug_tools
#define DOUT_MASK DOUT_MASK_TOOLS
#include "super.h"

static struct list_head _bk_allocs;

static DEFINE_SPINLOCK(_bk_lock);

static size_t _total_alloc;
static size_t _total_free;

struct alloc_data {
	u32 prefix_magic;
	struct list_head node;
	size_t size;
	char *fname;
	int line;
	u32 suffix_magic;
};

void *ceph_kmalloc(char *fname, int line, size_t size, gfp_t flags)
{
	struct alloc_data *p = kmalloc(size+sizeof(struct alloc_data), flags);

	if (!p)
		return NULL;

	p->prefix_magic = CEPH_BK_MAGIC;
	p->size = size;
	p->fname = fname;
	p->line = line;
	p->suffix_magic = CEPH_BK_MAGIC;

	spin_lock(&_bk_lock);
	_total_alloc += size;

	list_add_tail(&p->node, &_bk_allocs);
	spin_unlock(&_bk_lock);

	return ((void *)p)+sizeof(struct alloc_data);
}

void ceph_kfree(const void *ptr)
{
	struct alloc_data *p = (struct alloc_data *)(ptr -
						     sizeof(struct alloc_data));
	int overrun = 0;

	if (!ptr)
		return;

	if (p->prefix_magic != CEPH_BK_MAGIC) {
		derr(0, "ERROR: memory overrun (under)!\n");
		overrun = 1;
	}

	if (p->suffix_magic != CEPH_BK_MAGIC) {
		derr(0, "ERROR: Memory overrun (over)!\n");
		overrun = 1;
	}

	if (overrun) {
		derr(0, "Memory allocated at %s(%d): p=%p (%zu bytes)\n",
		     p->fname, p->line, ((void *)p)+sizeof(struct alloc_data),
		     p->size);
	}

	BUG_ON(overrun);

	spin_lock(&_bk_lock);
	_total_free += p->size;
	list_del(&p->node);
	spin_unlock(&_bk_lock);

	kfree(p);

	return;
}


void ceph_bookkeeper_dump(void)
{
	struct list_head *p;
	struct alloc_data *entry;

	dout(1, "bookkeeper: total bytes alloc: %zu\n", _total_alloc);
	dout(1, "bookkeeper: total bytes free: %zu\n", _total_free);

	if (_total_alloc != _total_free) {
		list_for_each(p, &_bk_allocs) {
			entry = list_entry(p, struct alloc_data, node);
			dout(1, "%s(%d): p=%p (%zu bytes)\n", entry->fname,
			     entry->line,
			     ((void *)entry)+sizeof(struct alloc_data),
			     entry->size);
		}
	} else {
		dout(1, "No leaks found! Yay!\n");
	}
}

void ceph_bookkeeper_init(void)
{
	dout(10, "bookkeeper: start\n");
	INIT_LIST_HEAD(&_bk_allocs);
}

void ceph_bookkeeper_finalize(void)
{
	ceph_bookkeeper_dump();
}
