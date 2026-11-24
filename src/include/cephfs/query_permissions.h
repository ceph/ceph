#ifndef CEPHFS_QUERY_PERMISSIONS_H
#define CEPHFS_QUERY_PERMISSIONS_H

#include <stddef.h>
#include <stdint.h>

/*
 * Decoded QUERY_PERMISSIONS answer for libcephfs / qa tests.
 */
struct ceph_query_permissions_answer {
	uint64_t ino;
	uint8_t state;
	uint64_t tuple_version;
	uint32_t mode;
	uint32_t uid;
	uint32_t gid;
	size_t acl_length;
};

#endif