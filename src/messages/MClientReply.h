// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MCLIENTREPLY_H
#define CEPH_MCLIENTREPLY_H

#include <list>
#include <map>
#include <iosfwd>
#include <set>
#include <string>
#include <vector>

#include "MMDSOp.h"

#include "include/fs_types.h"

#include "msg/Message.h"
#include "include/ceph_features.h"
#include "include/cephfs/encoding.h"
#include "include/cephfs/types.h" // for frag_info_t, nest_info_t, optmetadata_client_t, quota_info_t
#include "include/cephfs/vinodeno.h"
#include "include/frag.h" // for frag_t, fragtree_t
#include "include/interval_set.h"

class MClientRequest;

/***
 *
 * MClientReply - container message for MDS reply to a client's MClientRequest
 *
 * key fields:
 *  long tid - transaction id, so the client can match up with pending request
 *  int result - error code, or fh if it was open
 *
 * for most requests:
 *  trace is a vector of InodeStat's tracing from root to the file/dir/whatever
 *  the operation referred to, so that the client can update it's info about what
 *  metadata lives on what MDS.
 *
 * for readdir replies:
 *  dir_contents is a vector of InodeStat*'s.  
 * 
 * that's mostly it, i think!
 *
 */


struct LeaseStat {
  // this matches ceph_mds_reply_lease
  __u16 mask = 0;
  __u32 duration_ms = 0;
  __u32 seq = 0;
  std::string alternate_name;

  LeaseStat() = default;
  LeaseStat(__u16 msk, __u32 dur, __u32 sq) : mask{msk}, duration_ms{dur}, seq{sq} {}

  void decode(ceph::buffer::list::const_iterator &bl, const uint64_t features);
};

std::ostream& operator<<(std::ostream& out, const LeaseStat& l);

struct DirStat {
  // mds distribution hints
  frag_t frag;
  __s32 auth;
  std::set<__s32> dist;
  
  DirStat();
  DirStat(ceph::buffer::list::const_iterator& p, const uint64_t features) {
    decode(p, features);
  }

  void decode(ceph::buffer::list::const_iterator& p, const uint64_t features);

  // see CDir::encode_dirstat for encoder.
};

struct InodeStat {
  using optmetadata_singleton_client_t = optmetadata_singleton<optmetadata_client_t<std::allocator>,std::allocator>;

  vinodeno_t vino;
  uint32_t rdev = 0;
  version_t version = 0;
  version_t xattr_version = 0;
  ceph_mds_reply_cap cap;
  file_layout_t layout;
  utime_t ctime, btime, mtime, atime, snap_btime;
  uint32_t time_warp_seq = 0;
  uint64_t size = 0, max_size = 0;
  uint64_t change_attr = 0;
  uint64_t truncate_size = 0;
  uint32_t truncate_seq = 0;
  uint32_t mode = 0, uid = 0, gid = 0, nlink = 0;
  frag_info_t dirstat;
  nest_info_t rstat;

  fragtree_t dirfragtree;
  std::string  symlink;   // symlink content (if symlink)

  ceph_dir_layout dir_layout;

  ceph::buffer::list xattrbl;

  ceph::buffer::list inline_data;
  version_t inline_version;

  quota_info_t quota;

  mds_rank_t dir_pin;
  std::map<std::string,std::string> snap_metadata;

  std::vector<uint8_t> fscrypt_auth;
  std::vector<uint8_t> fscrypt_file;

  optmetadata_multiton<optmetadata_singleton_client_t,std::allocator> optmetadata;
  inodeno_t subvolume_id;

 public:
  InodeStat() {}
  InodeStat(ceph::buffer::list::const_iterator& p, const uint64_t features) {
    decode(p, features);
  }

  void print(std::ostream& os) const;

  void decode(ceph::buffer::list::const_iterator &p, const uint64_t features);

  // see CInode::encode_inodestat for encoder.
};

struct openc_response_t {
  _inodeno_t			created_ino{0};
  interval_set<inodeno_t>	delegated_inos;

public:
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
  void dump(ceph::Formatter *f) const;
  static std::list<openc_response_t> generate_test_instances();
} __attribute__ ((__may_alias__));
WRITE_CLASS_ENCODER(openc_response_t)

class MClientReply final : public MMDSOp {
public:
  // reply data
  struct ceph_mds_reply_head head {};
  ceph::buffer::list trace_bl;
  ceph::buffer::list extra_bl;
  ceph::buffer::list snapbl;

  int get_op() const { return head.op; }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() const { return head.mdsmap_epoch; }

  int get_result() const {
    // MDS now uses host errors, as defined in errno.cc, for current platform.
    // errorcode32_t is converting, internally, the error code from host to ceph, when encoding, and vice versa,
    // when decoding, resulting having LINUX codes on the wire, and HOST code on the receiver.
    // assumes this code is executing after decode_payload() function has been called
    return head.result;
  }

  // errorcode32_t is used in decode/encode methods
  void set_result(int r) {
    head.result = r;
  }

  void set_unsafe() { head.safe = 0; }

  bool is_safe() const { return head.safe; }

protected:
  MClientReply() : MMDSOp{CEPH_MSG_CLIENT_REPLY} {}
  MClientReply(const MClientRequest &req, int result = 0);
  ~MClientReply() final {}

public:
  std::string_view get_type_name() const override { return "creply"; }
  void print(std::ostream& o) const override;

  // serialization
  void decode_payload() override;
  void encode_payload(uint64_t features) override;

  // dir contents
  void set_extra_bl(ceph::buffer::list& bl) {
    extra_bl = std::move(bl);
  }
  ceph::buffer::list& get_extra_bl() {
    return extra_bl;
  }
  const ceph::buffer::list& get_extra_bl() const {
    return extra_bl;
  }

  // trace
  void set_trace(ceph::buffer::list& bl) {
    trace_bl = std::move(bl);
  }
  ceph::buffer::list& get_trace_bl() {
    return trace_bl;
  }
  const ceph::buffer::list& get_trace_bl() const {
    return trace_bl;
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
