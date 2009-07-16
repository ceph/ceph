#ifndef _FS_CEPH_DEBUG_H
#define _FS_CEPH_DEBUG_H

/*
 * wrap pr_debug to include a filename:lineno prefix on each line
 */

static inline const char *ceph_file_part(const char *s, int len)
{
	const char *e = s + len;

	while (e != s && *(e-1) != '/')
		e--;
	return e;
}

#define _dout(fmt, args...)						\
	pr_debug(" %12.12s:%-4d : " fmt "%s",				\
		 ceph_file_part(__FILE__, sizeof(__FILE__)),		\
		 __LINE__, args);
#define dout(args...) _dout(args, "")


#endif
