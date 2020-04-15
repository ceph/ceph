// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include <include/types.h>

class XMLObj;

class PublicAccessBlockConfiguration {
  bool BlockPublicAcls;
  bool IgnorePublicAcls;
  bool BlockPublicPolicy;
  bool RestrictPublicBuckets;
 public:
 PublicAccessBlockConfiguration():
   BlockPublicAcls(false), IgnorePublicAcls(false),
  BlockPublicPolicy(false), RestrictPublicBuckets(false)
    {}

  auto block_public_acls() const {
    return BlockPublicAcls;
  }
  auto ignore_public_acls() const {
    return IgnorePublicAcls;
  }
  auto block_public_policy() const {
    return BlockPublicPolicy;
  }
  auto restrict_public_buckets() const {
    return RestrictPublicBuckets;
  }

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
ostream& operator<< (ostream& os, const PublicAccessBlockConfiguration& access_conf);
