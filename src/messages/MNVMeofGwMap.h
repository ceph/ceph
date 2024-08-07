// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MNVMEOFGWMAP_H
#define CEPH_MNVMEOFGWMAP_H

#include "msg/Message.h"
#include "mon/NVMeofGwMap.h"

class MNVMeofGwMap final : public Message {
private:
  static int constexpr VERSION = 2;
  int encode_version;
protected:
  std::map<NvmeGroupKey, NvmeGwMonClientStates> map;
  epoch_t                           gwmap_epoch;

public:
  const std::map<NvmeGroupKey, NvmeGwMonClientStates>& get_map() {return map;}
  const epoch_t& get_gwmap_epoch() {return gwmap_epoch;}

private:
  MNVMeofGwMap() :
    Message{MSG_MNVMEOF_GW_MAP} {}
  MNVMeofGwMap(const NVMeofGwMap &map_) :
    Message{MSG_MNVMEOF_GW_MAP}, gwmap_epoch(map_.epoch)
  {
    encode_version = VERSION;
    map_.to_gmap(map, true);
  }
  MNVMeofGwMap(const NVMeofGwMap &map_ , bool is_gw_last_version) :
    Message{MSG_MNVMEOF_GW_MAP}, gwmap_epoch(map_.epoch)
  {
    encode_version = (is_gw_last_version ) ? VERSION : VERSION-1;
    map_.to_gmap(map, is_gw_last_version);
  }
  ~MNVMeofGwMap() final {}

public:
  std::string_view get_type_name() const override { return "nvmeofgwmap"; }

  void decode_payload() override {
    auto p = payload.cbegin();
    int version;
    decode(version, p);
    if (version > VERSION)
      throw ::ceph::buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, VERSION, version));
    decode(gwmap_epoch, p);
    decode(map, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(encode_version, payload);
    encode(gwmap_epoch, payload);
    encode(map, payload);
  }
private:
  using RefCountedObject::put;
  using RefCountedObject::get;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
