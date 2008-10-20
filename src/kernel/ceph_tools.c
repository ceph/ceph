#include <linux/spinlock.h>
#include <linux/slab.h>
#include <linux/kallsyms.h>

#define CEPH_OVERRIDE_BOOKKEEPER /* avoid kmalloc/kfree recursion */

#define CEPH_BK_MAGIC 0x140985AC

#include "ceph_debug.h"

int ceph_debug_tools = -1;
#define DOUT_VAR ceph_debug_tools
#define DOUT_MASK DOUT_MASK_TOOLS
#define DOUT_PREFIX "tools: "
#include "super.h"

static struct list_head _bk_allocs;

static DEFINE_SPINLOCK(_bk_lock);

static size_t _total_alloc = 0;
static size_t _total_free = 0;


struct alloc_data {
	u32 magic;
	struct list_head node;
	size_t size;
	char *fname;
	int line;
};

struct stack_frame {
	struct stack_frame *next_frame;
	unsigned long return_address;
};

void *ceph_kmalloc(char *fname, int line, size_t size, gfp_t flags)
{
	struct alloc_data *p=kmalloc(size+sizeof(struct alloc_data), flags);

	if (!p)
		return NULL;

	p->magic = CEPH_BK_MAGIC;
	p->size = size;
	p->fname = fname;
	p->line = line;

	spin_lock(&_bk_lock);
	_total_alloc += size;

	list_add_tail(&p->node, &_bk_allocs);
	spin_unlock(&_bk_lock);

	return ((void *)p)+sizeof(struct alloc_data);
}


void ceph_kfree(void *ptr)
{
	struct alloc_data *p=(struct alloc_data *)(ptr-sizeof(struct alloc_data));

	if (!ptr)
		return;

	if (p->magic != CEPH_BK_MAGIC) {
		kfree(ptr);
		return;
	}

	spin_lock(&_bk_lock);
	_total_free += p->size;
	list_del(&p->node);
	spin_unlock(&_bk_lock);

	kfree(p);

	return;
}

void ceph_bookkeeper_init(void)
{
	printk("bookkeeper: start\n");
	INIT_LIST_HEAD(&_bk_allocs);
}

void ceph_bookkeeper_finalize(void)
{
	struct list_head *p;
	struct alloc_data *entry;

	printk("bookkeeper: total bytes alloc: %zu\n", _total_alloc);
	printk("bookkeeper: total bytes free: %zu\n", _total_free);

	if (_total_alloc != _total_free) {

		list_for_each(p, &_bk_allocs) {
			entry = list_entry(p, struct alloc_data, node);
			printk("%s(%d): p=%p (%zu bytes)\n", entry->fname, entry->line, 
				((void *)entry)+sizeof(struct alloc_data), 
				entry->size);
		}
	} else {
		printk("No leaks found! Yay!\n");
	}
}
