#ifndef CEPH_WS_TYPES_H
#define CEPH_WS_TYPES_H

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <glib.h>

// this is needed for ceph_fs to compile in userland
#ifdef _MSC_VER
typedef  char				__s8;
typedef short				__s16;
typedef int					__s32;
typedef __int64				__s64;
typedef unsigned char		__u8;
typedef unsigned short		__u16;
typedef unsigned int		__u32;
typedef unsigned __int64	__u64;
typedef __u16				__le16;
typedef __u32				__le32;
typedef __u64				__le64;
#define __attribute__(x)
#define O_ACCMODE (O_RDONLY | O_RDWR | O_WRONLY)
#include <winsock.h>
#else
#include <netinet/in.h>
#include <linux/types.h>
#endif
typedef int bool;

#define le16_to_cpu(x) (x)
#define le32_to_cpu(x) (x)
#define le64_to_cpu(x) (x)


typedef guint32 uint32_t;

#include <fcntl.h>
#include <string.h>

#ifdef _MSC_VER
#pragma pack(1)
#endif
#include "ceph_fs.h"
#ifdef _MSC_VER
#pragma pack()
#endif



#endif
