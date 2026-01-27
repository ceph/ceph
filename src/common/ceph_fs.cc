/*
 * ceph_fs.cc - Some Ceph functions that are shared between kernel space and
 * user space.
 *
 */

/*
 * Some non-inline ceph helpers
 */
#include "include/types.h"

int ceph_flags_to_mode(int flags)
{
	/* because CEPH_FILE_MODE_PIN is zero, so mode = -1 is error */
	int mode = -1;

	if ((flags & CEPH_O_DIRECTORY) == CEPH_O_DIRECTORY)
		return CEPH_FILE_MODE_PIN;

	switch (flags & O_ACCMODE) {
	case CEPH_O_WRONLY:
		mode = CEPH_FILE_MODE_WR;
		break;
	case CEPH_O_RDONLY:
		mode = CEPH_FILE_MODE_RD;
		break;
	case CEPH_O_RDWR:
	case O_ACCMODE: /* this is what the VFS does */
		mode = CEPH_FILE_MODE_RDWR;
		break;
	}

	if (flags & CEPH_O_LAZY)
		mode |= CEPH_FILE_MODE_LAZY;

	return mode;
}

int ceph_caps_for_mode(int mode)
{
	int caps = CEPH_CAP_PIN;

	if (mode & CEPH_FILE_MODE_RD)
		caps |= CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE;
	if (mode & CEPH_FILE_MODE_WR)
		caps |= CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |
			CEPH_CAP_AUTH_SHARED | CEPH_CAP_AUTH_EXCL |
			CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
	if (mode & CEPH_FILE_MODE_LAZY)
		caps |= CEPH_CAP_FILE_LAZYIO;

	return caps;
}

int ceph_flags_sys2wire(int flags)
{
       int wire_flags = 0;

       switch (flags & O_ACCMODE) {
       case O_RDONLY:
               wire_flags |= CEPH_O_RDONLY;
               break;
       case O_WRONLY:
               wire_flags |= CEPH_O_WRONLY;
               break;
       case O_RDWR:
               wire_flags |= CEPH_O_RDWR;
               break;
       }
       flags &= ~O_ACCMODE;

#define ceph_sys2wire(a) if (flags & a) { wire_flags |= CEPH_##a; flags &= ~a; }

       ceph_sys2wire(O_CREAT);
       ceph_sys2wire(O_EXCL);
       ceph_sys2wire(O_TRUNC);

       ceph_sys2wire(O_DIRECTORY);
       ceph_sys2wire(O_NOFOLLOW);

#undef ceph_sys2wire

       return wire_flags;
}
