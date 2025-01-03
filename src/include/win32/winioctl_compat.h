// Mingw provides a minimal version of this header and doesn't include all the
// definitions that we need.

#pragma once

#ifdef __MINGW32__

#include <winioctl.h>

#define IOCTL_DISK_GET_DISK_ATTRIBUTES \
	CTL_CODE(IOCTL_DISK_BASE, 0x003c, METHOD_BUFFERED, FILE_ANY_ACCESS)
#define IOCTL_DISK_SET_DISK_ATTRIBUTES \
	CTL_CODE(IOCTL_DISK_BASE, 0x003d, METHOD_BUFFERED, \
		FILE_READ_ACCESS | FILE_WRITE_ACCESS)

#define DISK_ATTRIBUTE_OFFLINE   0x0000000000000001
#define DISK_ATTRIBUTE_READ_ONLY 0x0000000000000002

//
// IOCTL_DISK_SET_DISK_ATTRIBUTES
//
// Input Buffer:
//     Structure of type SET_DISK_ATTRIBUTES
//
// Output Buffer:
//     None
//
typedef struct _SET_DISK_ATTRIBUTES {
    // Specifies the size of the structure for versioning.
    DWORD Version;
    // Indicates whether to remember these settings across reboots or not.
    BOOLEAN Persist;
    // Reserved. Must set to zero.
    BYTE  Reserved1[3];
    // Specifies the new attributes.
    DWORDLONG Attributes;
    // Specifies the attributes that are being modified.
    DWORDLONG AttributesMask;
    // Reserved. Must set to zero.
    DWORD Reserved2[4];
} SET_DISK_ATTRIBUTES, *PSET_DISK_ATTRIBUTES;

#endif // __MINGW32__
