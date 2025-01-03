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


#ifndef CEPH_MEXPORTCAPS_H
#define CEPH_MEXPORTCAPS_H

#include "messages/MMDSOp.h"

class MExportCaps final : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:  
  inodeno_t ino;
  ceph::buffer::list cap_bl;
  std::map<client_t,entity_inst_t> client_map;
  std::map<client_t,client_metadata_t> client_metadata_map;

protected:
  MExportCaps() :
    MMDSOp{MSG_MDS_EXPORTCAPS, HEAD_VERSION, COMPAT_VERSION} {}
  ~MExportCaps() final {}

public:
  std::string_view get_type_name() const override { return "export_caps"; }
  void print(std::ostream& o) const override {
    o << "export_caps(" << ino << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(cap_bl, payload);
    encode(client_map, payload, features);
    encode(client_metadata_map, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(cap_bl, p);
    decode(client_map, p);
    if (header.version >= 2)
      decode(client_metadata_map, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
