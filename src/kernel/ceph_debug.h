#ifndef _FS_CEPH_DEBUG_H
#define _FS_CEPH_DEBUG_H

/*
 * wrap pr_debug to include a filename:lineno prefix on each line
 */

extern const char *ceph_file_part(const char *s, int len);

#define dout(fmt, ...)							\
	pr_debug(" %12.12s:%-4d : " fmt,				\
		 ceph_file_part(__FILE__, sizeof(__FILE__)),		\
		 __LINE__, ##__VA_ARGS__);


#endif
