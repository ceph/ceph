#ifndef _FS_CEPH_DEBUG_H
#define _FS_CEPH_DEBUG_H

/*
 * wrap pr_debug to include a filename:lineno prefix on each line
 */

#if defined(DEBUG) || defined(CONFIG_DYNAMIC_DEBUG)
extern const char *ceph_file_part(const char *s, int len);
# define dout(fmt, ...)							\
	pr_debug(" %12.12s:%-4d : " fmt,				\
		 ceph_file_part(__FILE__, sizeof(__FILE__)),		\
		 __LINE__, ##__VA_ARGS__);
#else
/*
 * this is a no-op, but keep the faux printk call just so we see any
 * compiler warnings.
 */
# define dout(fmt, ...)	do {						\
		if (0) printk(KERN_DEBUG fmt, ##__VA_ARGS__);	\
	} while (0)
#endif

#endif
