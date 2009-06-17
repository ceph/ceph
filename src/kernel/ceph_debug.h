#ifndef _FS_CEPH_DEBUG_H
#define _FS_CEPH_DEBUG_H

#include <linux/string.h>

extern int ceph_debug __read_mostly;         /* debug level. */
extern int ceph_debug_console __read_mostly; /* send debug output to console? */
extern int ceph_debug_mask __read_mostly;

/*
 * different debug levels for different modules.  These default to -1.
 * If they are >= 0, then they override the global ceph_debug value.
 */
extern int ceph_debug_addr __read_mostly;
extern int ceph_debug_caps __read_mostly;
extern int ceph_debug_dir __read_mostly;
extern int ceph_debug_export __read_mostly;
extern int ceph_debug_file __read_mostly;
extern int ceph_debug_inode __read_mostly;
extern int ceph_debug_ioctl __read_mostly;
extern int ceph_debug_mdsc __read_mostly;
extern int ceph_debug_mdsmap __read_mostly;
extern int ceph_debug_msgr __read_mostly;
extern int ceph_debug_mon __read_mostly;
extern int ceph_debug_osdc __read_mostly;
extern int ceph_debug_osdmap __read_mostly;
extern int ceph_debug_snap __read_mostly;
extern int ceph_debug_super __read_mostly;
extern int ceph_debug_protocol __read_mostly;
extern int ceph_debug_proc __read_mostly;
extern int ceph_debug_tools __read_mostly;

#define DOUT_MASK_ADDR		0x00000001
#define DOUT_MASK_CAPS		0x00000002
#define DOUT_MASK_DIR		0x00000004
#define DOUT_MASK_EXPORT	0x00000008
#define DOUT_MASK_FILE		0x00000010
#define DOUT_MASK_INODE		0x00000020
#define DOUT_MASK_IOCTL		0x00000040
#define DOUT_MASK_MDSC		0x00000080
#define DOUT_MASK_MDSMAP	0x00000100
#define DOUT_MASK_MSGR		0x00000200
#define DOUT_MASK_MON		0x00000400
#define DOUT_MASK_OSDC		0x00000800
#define DOUT_MASK_OSDMAP	0x00001000
#define DOUT_MASK_SNAP		0x00002000
#define DOUT_MASK_SUPER		0x00004000
#define DOUT_MASK_PROTOCOL	0x00008000
#define DOUT_MASK_PROC		0x00010000
#define DOUT_MASK_TOOLS		0x00020000

#define DOUT_UNMASKABLE	0x80000000

#define _STRINGIFY(x) #x
#define STRINGIFY(x) _STRINGIFY(x)

#define FMT_PREFIX "%-30.30s: "
#define FMT_SUFFIX "%s"
#define LOG_ARGS __FILE__ ":" STRINGIFY(__LINE__)
#define TRAIL_PARAM ""

#define LOG_LINE FMT_PREFIX fmt, LOG_ARGS, args

#define dout_flag(x, mask, fmt, args...) do {				\
		if (((ceph_debug_mask | DOUT_UNMASKABLE) & mask) &&	\
		    ((DOUT_VAR >= 0 && (x) <= DOUT_VAR) ||		\
		     (DOUT_VAR < 0 && (x) <= ceph_debug))) {		\
			if (ceph_debug_console)				\
				printk(KERN_ERR FMT_PREFIX fmt, LOG_ARGS, \
				       args);				\
			else						\
				printk(KERN_DEBUG FMT_PREFIX fmt, LOG_ARGS, \
				       args);				\
		}							\
	} while (0)

#define _dout(x, fmt, args...) dout_flag((x), DOUT_MASK, fmt FMT_SUFFIX, args)

#define _derr(x, fmt, args...) do {					\
		printk(KERN_ERR FMT_PREFIX fmt FMT_SUFFIX, LOG_ARGS, args); \
	} while (0)

#define dout(x, args...) _dout((x), args, TRAIL_PARAM)
#define derr(x, args...) _derr((x), args, TRAIL_PARAM)

#endif
