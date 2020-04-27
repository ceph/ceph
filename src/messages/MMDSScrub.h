// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_MMDSSCRUB_H
#define CEPH_MMDSSCRUB_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSScrub : public SafeMessage {
    static const int HEAD_VERSION = 1;
    static const int COMPAT_VERSION = 1;
    
 public:
    inodeno_t ino;
    bool scrub_frag = false;
    frag_t frag;
    bool force = false;
    bool recursive = false;
    bool repair = false;
    
 protected:
 MMDSScrub(inodeno_t _ino, bool _scrub_frag, frag_t _frag,
           bool _force, bool _recursive, bool _repair)
     : SafeMessage{MSG_MDS_SCRUB, HEAD_VERSION, COMPAT_VERSION},
       ino(_ino), scrub_frag(_scrub_frag), frag(_frag),
       force(_force), recursive(_recursive), repair(_repair)
            {}
 ~MMDSScrub() override {}
 public:
 std::string_view get_type_name() const override { return "mdsscrub"; }
 void print(std::ostream& out) const override {
     out << "mds_scrub(" << ino;
     if (scrub_frag)
        out << " " << frag;
     out << " force=" << force
         << " recursive=" << recursive
         << " repair=" << repair
         << ")";
  }
 void encode_payload(uint64_t features) override {
     using ceph::encode;
     encode(scrub_frag, payload);
     encode(frag, payload);
     encode(ino, payload);
 }
 void decode_payload() override {
     using ceph::decode;
     auto p = payload.cbegin();
     decode(scrub_frag, p);
     decode(frag, p);
     decode(ino, p);
 }
 bool is_force() const {
     return force;
 }
 bool is_recursive() const {
     return recursive;
 }
 bool is_repair() const {
     return repair;
 }
 bool scrub_dir() const {
     return scrub_frag;
 }
 frag_t get_frag() const {
     return frag;
 }
 inodeno_t get_ino() const {
     return ino;
 }
 private:
    template<class T, typename... Args>
    friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
#endif // CEPH_MMDSSCRUB_H
