// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

struct compression_block {
  uint64_t old_ofs;
  uint64_t new_ofs;
  uint64_t len;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(old_ofs, bl);
    encode(new_ofs, bl);
    encode(len, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(old_ofs, bl);
     decode(new_ofs, bl);
     decode(len, bl);
     DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(compression_block)

struct RGWCompressionInfo {
  std::string compression_type;
  uint64_t orig_size;
  std::optional<int32_t> compressor_message;
  std::vector<compression_block> blocks;

  RGWCompressionInfo() : compression_type("none"), orig_size(0) {}
  RGWCompressionInfo(const RGWCompressionInfo& cs_info) : compression_type(cs_info.compression_type),
                                                          orig_size(cs_info.orig_size),
							  compressor_message(cs_info.compressor_message),
                                                          blocks(cs_info.blocks) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(compression_type, bl);
    encode(orig_size, bl);
    encode(compressor_message, bl);
    encode(blocks, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(2, bl);
     decode(compression_type, bl);
     decode(orig_size, bl);
     if (struct_v >= 2) {
       decode(compressor_message, bl);
     }
     decode(blocks, bl);
     DECODE_FINISH(bl);
  } 
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWCompressionInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWCompressionInfo)

