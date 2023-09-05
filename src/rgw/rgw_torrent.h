// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/ceph_crypto.h"
#include "common/dout.h"
#include "common/async/yield_context.h"

#include "rgw_putobj.h"
#include "rgw_sal_fwd.h"

//control characters
void bencode_dict(bufferlist& bl);
void bencode_list(bufferlist& bl);
void bencode_end(bufferlist& bl);

//key len
void bencode_key(std::string_view key, bufferlist& bl);

//single values
void bencode(int value, bufferlist& bl);

//single values
void bencode(std::string_view str, bufferlist& bl);

//dictionary elements
void bencode(std::string_view key, int value, bufferlist& bl);

//dictionary elements
void bencode(std::string_view key, std::string_view value, bufferlist& bl);


// read the bencoded torrent file from the given object
int rgw_read_torrent_file(const DoutPrefixProvider* dpp,
                          rgw::sal::Object* object,
                          ceph::bufferlist &bl,
                          optional_yield y);

// PutObj filter that builds a torrent file during upload
class RGWPutObj_Torrent : public rgw::putobj::Pipe {
  size_t max_len = 0;
  size_t piece_len = 0;
  bufferlist piece_hashes;
  size_t len = 0;
  size_t piece_offset = 0;
  uint32_t piece_count = 0;
  ceph::crypto::SHA1 digest;

 public:
  RGWPutObj_Torrent(rgw::sal::DataProcessor* next,
                    size_t max_len, size_t piece_len);

  int process(bufferlist&& data, uint64_t logical_offset) override;

  // after processing is complete, return the bencoded torrent file
  bufferlist bencode_torrent(std::string_view filename) const;
};
