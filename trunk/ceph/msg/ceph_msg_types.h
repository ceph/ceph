/* -*- mode:C++; tab-width:8; c-basic-offset:8; indent-tabs-mode:t -*- 
 * vim: ts=8 sw=8 smarttab
 */
#ifndef __CEPH_MSG_TYPES_H
#define __CEPH_MSG_TYPES_H

/*
 * entity_name
 */
struct ceph_entity_name {
	__u32 type;
	__u32 num;
};

#define CEPH_ENTITY_TYPE_MON    1
#define CEPH_ENTITY_TYPE_MDS    2
#define CEPH_ENTITY_TYPE_OSD    3
#define CEPH_ENTITY_TYPE_CLIENT 4
#define CEPH_ENTITY_TYPE_ADMIN  5


/*
 * entity_addr
 * ipv4 only for now
 */
struct ceph_entity_addr {
	__u64 nonce;
	__u32 port;
	__u8  ipq[4];
};


struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
};


/*
 * message header
 */
struct ceph_message_header {
	__u32 type;
	struct ceph_entity_inst src, dst;
	__u32 source_port, dest_port;
	__u32 nchunks;
};

#endif
