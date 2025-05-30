/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */
/*
 * fscrypt user API
 *
 * These ioctls can be used on filesystems that support fscrypt.  See the
 * "User API" section of Documentation/filesystems/fscrypt.rst.
 */
#ifndef _UAPI_LINUX_FSCRYPT_H
#define _UAPI_LINUX_FSCRYPT_H
#include <linux/fscrypt.h>

struct fscrypt_policy_arg {
  union {
    struct fscrypt_policy_v1 v1;
    struct fscrypt_policy_v2 v2;
  } policy;
}; /* output */

/* Struct passed to FS_IOC_ADD_ENCRYPTION_KEY */
struct fscrypt_add_key64_arg {
	struct fscrypt_key_specifier key_spec;
	__u32 raw_size;
	__u32 key_id;
	__u32 __reserved[8];
	__u8 raw[64];
};

#define FS_IOC_SET_ENCRYPTION_POLICY_RESTRICTED       _IOWR('f', 19, struct fscrypt_policy_arg)
#define FS_IOC_GET_ENCRYPTION_POLICY_EX_RESTRICTED    _IOWR('f', 22, struct fscrypt_get_policy_ex_arg) /* size + version */
#define FS_IOC_ADD_ENCRYPTION_KEY64           _IOWR('f', 23, struct fscrypt_add_key64_arg)
#endif
