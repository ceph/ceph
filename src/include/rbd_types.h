#ifndef _FS_CEPH_RBD
#define _FS_CEPH_RBD

#include <linux/types.h>

/*
 * rbd image 'foo' consists of objects
 *   foo.rbd      - image metadata
 *   foo.00000000 
 *   foo.00000001
 *   ...          - data
 */

#define RBD_SUFFIX	 	".rbd"
#define RBD_DIRECTORY           "rbd_directory"

#define RBD_DEFAULT_OBJ_ORDER	22   /* 4MB */

#define RBD_MAX_OBJ_NAME_SIZE	96
#define RBD_MAX_SEG_NAME_SIZE	128

#define RBD_COMP_NONE		0
#define RBD_CRYPT_NONE		0

static const char rbd_text[] = "<<< Rados Block Device Image >>>\n";
static const char rbd_signature[] = "RBD";
static const char rbd_version[] = "001.004";

struct rbd_obj_snap_ondisk {
	__le64 id;
	__le64 image_size;
} __attribute__((packed));

struct rbd_obj_header_ondisk {
	char text[64];
	char signature[4];
	char version[8];
	struct {
		__u8 order;
		__u8 crypt_type;
		__u8 comp_type;
		__u8 unused;
	} __attribute__((packed)) options;
	__le64 image_size;
	__le64 snap_seq;
	__le32 snap_count;
	__le32 reserved;
	__le64 snap_names_len;
	struct rbd_obj_snap_ondisk snaps[0];
} __attribute__((packed));


#endif
