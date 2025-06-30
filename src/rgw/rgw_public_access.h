// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once
#include "include/encoding.h"

class XMLObj;
namespace ceph { class Formatter; }

struct PublicAccessBlockConfiguration {
  bool BlockPublicAcls = false;
  bool IgnorePublicAcls = false;
  bool BlockPublicPolicy = false;
  bool RestrictPublicBuckets = false;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1,1, bl);
    encode(BlockPublicAcls, bl);
    encode(IgnorePublicAcls, bl);
    encode(BlockPublicPolicy, bl);
    encode(RestrictPublicBuckets, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& bl) {
    DECODE_START(1,bl);
    decode(BlockPublicAcls, bl);
    decode(IgnorePublicAcls, bl);
    decode(BlockPublicPolicy, bl);
    decode(RestrictPublicBuckets, bl);
    DECODE_FINISH(bl);
  }

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(PublicAccessBlockConfiguration)
std::ostream& operator<< (std::ostream& os, const PublicAccessBlockConfiguration& access_conf);
