// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "include/fs_types.h"

/*
 * Successful CEPH_MDS_OP_QUERY_PERMISSIONS reply, carried in the client
 * reply extra buffer. This is not an InodeStat and it grants nothing.
 *
 * Ceph encoding version 1 (ENCODE_START 1, 1):
 *   inodeno_t ino
 *   uint8_t   state
 *   when state != CEPH_QUERY_PERM_NOT_RETURNED:
 *     uint64_t tuple_version   inode version of the encoded fields
 *     uint32_t mode
 *     uint32_t uid
 *     uint32_t gid
 *     bufferlist acl           system.posix_acl_access bytes
 *
 * CEPH_QUERY_PERM_NOT_RETURNED is a successful answer with no tuple. The
 * caller left CEPH_QUERY_PERM_FLAG_WANT_SNAPSHOT clear. It is not an empty
 * ACL. CEPH_QUERY_PERM_NO_ACL is a complete tuple whose ACL buffer length
 * is 0. CEPH_QUERY_PERM_ACL is a complete tuple with the access ACL bytes
 * already stored for that xattr. Errors are a normal result code and have
 * no payload.
 */
struct QueryPermissionsReply {
  inodeno_t ino;
  uint8_t state = CEPH_QUERY_PERM_NOT_RETURNED;
  uint64_t tuple_version = 0;
  uint32_t mode = 0;
  uint32_t uid = 0;
  uint32_t gid = 0;
  ceph::buffer::list acl;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(ino, bl);
    encode(state, bl);
    if (state != CEPH_QUERY_PERM_NOT_RETURNED) {
      encode(tuple_version, bl);
      encode(mode, bl);
      encode(uid, bl);
      encode(gid, bl);
      encode(acl, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    DECODE_START(1, p);
    decode(ino, p);
    decode(state, p);
    if (state != CEPH_QUERY_PERM_NOT_RETURNED) {
      decode(tuple_version, p);
      decode(mode, p);
      decode(uid, p);
      decode(gid, p);
      decode(acl, p);
    }
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(QueryPermissionsReply)
