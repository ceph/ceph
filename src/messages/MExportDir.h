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


#ifndef CEPH_MEXPORTDIR_H
#define CEPH_MEXPORTDIR_H

#include "messages/MMDSOp.h"

class MExportDir : public MMDSOp {
public:
  dirfrag_t dirfrag;
  ceph::buffer::list export_data;
  std::vector<dirfrag_t> bounds;
  ceph::buffer::list client_map;

protected:
  MExportDir() : MMDSOp{MSG_MDS_EXPORTDIR} {}
  MExportDir(dirfrag_t df, uint64_t tid) :
    MMDSOp{MSG_MDS_EXPORTDIR}, dirfrag(df) {
    set_tid(tid);
  }
  ~MExportDir() override {}

public:
  std::string_view get_type_name() const override { return "Ex"; }
  void print(std::ostream& o) const override {
    o << "export(" << dirfrag << ")";
  }

  void add_export(dirfrag_t df) { 
    bounds.push_back(df); 
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(bounds, payload);
    encode(export_data, payload);
    encode(client_map, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(bounds, p);
    decode(export_data, p);
    decode(client_map, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
