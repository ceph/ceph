// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Igor Fedotov <ifedotov@mirantis.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef COMPRESSCONTEXT_H
#define COMPRESSCONTEXT_H

#include <map>

#include "include/memory.h"
#include "include/buffer.h"
#include "include/assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/hobject.h"
#include "compressor/Compressor.h"
#include "osd/ECUtil.h"

class CompressContext {
 public:

  static const unsigned RECS_PER_RECORDSET;          //amount of comression information records per single record set.
  static const unsigned MAX_STRIPES_PER_BLOCK;       //maximum amount of stripes that can be compressed as a single block 
  static const unsigned MAX_STRIPES_PER_BLOCKSET;    //maximum stripes to be described by a single attribute
  static const int DEBUG_LEVEL;                      //debug level for compression stuff

  /*
  Compression information record for single object block.
  To be stored along with other records under specific key in object attributes.
  Substitutes BlockMap entry( see below ) at persistent storage to reduce object attributes space utilization.
  */
  struct BlockInfoRecord {
    uint8_t method_idx;                           //an index indicating applied compression method in corresponding master record list, 0 - if no compression has been applied.
    uint32_t original_length,                     // block original length
	     compressed_length;                   // block compressed length
    BlockInfoRecord() : method_idx(0), original_length(0), compressed_length(0) {}
    BlockInfoRecord(const BlockInfoRecord& from) : method_idx(from.method_idx), original_length(from.original_length), compressed_length(from.compressed_length) {}
    BlockInfoRecord(uint8_t idx, uint32_t olen, uint32_t clen) : method_idx(idx), original_length(olen), compressed_length(clen) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
  };

  
  /*Compression information record set header. Used to split compression information records into recordsets to increase specific record lookup performance.
  To be stored before each recordset ( 32 records) under specific key in object attributes
  */
  struct BlockInfoRecordSetHeader {
    uint64_t start_offset,        //original offset for the first block in the recordset
	     compressed_offset;   //compressed offset for the first block in the recordset
    BlockInfoRecordSetHeader(uint64_t offset, uint64_t coffset) : start_offset(offset), compressed_offset(coffset) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
  };

  /*
  Object compression information master record.
  To be stored under specific key in object attributes
  */
  struct MasterRecord {
    uint64_t current_original_pos;                //Object original size (position original data is appended to), if 0 - no compression has been applied.
    uint64_t current_compressed_pos;              //Object compressed size (position compressed data to be appended to)
    uint32_t block_info_record_length,            //The length of the single block info record
             block_info_recordset_header_length;  //The length of the block info recordset header
    typedef vector<string>  MethodList;
    MethodList methods;                           //list of compression methods applied to the object. Positions to be preserved through object life-cycle to ensure proper mapping. +-1 shift to be applied to the index since 0 denotes no compression and isn't stored in the list.

    MasterRecord() : current_original_pos(0), current_compressed_pos(0), block_info_record_length(0), block_info_recordset_header_length(0) {}
    MasterRecord(const MasterRecord& other) :
      current_original_pos(other.current_original_pos),
      current_compressed_pos(other.current_compressed_pos),
      block_info_record_length(other.block_info_record_length),
      block_info_recordset_header_length( other.block_info_recordset_header_length),
      methods(other.methods) {}

    void clear() {
      current_original_pos = current_compressed_pos = 0;
      block_info_record_length = block_info_recordset_header_length = 0;
      methods.clear();
    }

    string get_method_name(uint8_t index) const;
    uint8_t add_get_method(const string& method);

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void swap(MasterRecord& other) {
      std::swap(current_original_pos, other.current_original_pos);
      std::swap(current_compressed_pos, other.current_compressed_pos);
      std::swap(block_info_record_length, other.block_info_record_length);
      std::swap(block_info_recordset_header_length, other.block_info_recordset_header_length);
      methods.swap(other.methods);
    }
  };

 private:
  /*
  Run-time compressed block information
  */
  struct BlockInfo {
    uint8_t method_idx;                 //an index indicating applied compression method in corresponding master record list, 0 - if no compression has been applied.
    uint64_t target_offset;             //an offset of the block in the compressed data set
    BlockInfo() : target_offset(0) {}
    BlockInfo(uint8_t _method, uint64_t _target_offset)
      : method_idx(_method), target_offset(_target_offset) {}

  };

  MasterRecord masterRec;  //Current compression information master record

  typedef std::map<uint64_t, BlockInfo> BlockMap;
  BlockMap blocks; //Map to provide original data offset to corresponding compressed one

  MasterRecord prevMasterRec;     //Compression information master record replica saved before the first unflushed update
  std::string prev_blocks_key;    //Attribute key for last compression info attribute in xattrs
  bufferlist prev_blocks_encoded; //Attribute value for last compression info attribute in xattrs

  uint64_t stripe_width;
 protected:

  static bool less_upper(const uint64_t&, const BlockMap::value_type&);
  static bool less_lower(const BlockMap::value_type&, const uint64_t&);

  int do_compress(CompressorRef csimpl, const bufferlist& block2compress, bufferlist* result_bl) const;

  void append_block(uint64_t original_offset, uint64_t original_size, const string& method, uint64_t new_block_size);
  bool can_compress(uint64_t offs) const;

  void swap(CompressContext& other) {
    blocks.swap(other.blocks);
    masterRec.swap(other.masterRec);
    std::swap(prev_blocks_key, other.prev_blocks_key);
    std::swap(prev_blocks_encoded, other.prev_blocks_encoded);
    prevMasterRec.swap(other.prevMasterRec);
  }

  uint64_t get_block_size() const;
  unsigned get_blockset_size() const;
  unsigned get_blocks_count() const { return blocks.size(); }

  MasterRecord get_master_record() const {
    return masterRec;
  };

  pair<uint64_t, uint64_t>  map_offset(uint64_t offs, bool next_block_flag) const; //returns <original block offset, compressed block_offset>

  bool need_flush() const {
    return prevMasterRec.current_original_pos != masterRec.current_original_pos;
  }

  void offset_to_key(uint64_t offs, std::string& key) const;

  BlockInfoRecordSetHeader search_starting_recordset(uint64_t offset, size_t record_set_size, bufferlist::iterator& it_bl);


 public:
  CompressContext(uint64_t _stripe_width) : stripe_width(_stripe_width) {}
  CompressContext(const CompressContext& from) :
    masterRec(from.masterRec),
    blocks(from.blocks),
    prevMasterRec(from.prevMasterRec),
    prev_blocks_key(from.prev_blocks_key),
    prev_blocks_encoded(from.prev_blocks_encoded),
    stripe_width(from.stripe_width) {}
  virtual ~CompressContext();
  void clear() {
    masterRec.clear();
    blocks.clear();
    prev_blocks_key.clear();
    prev_blocks_encoded.clear();
    prevMasterRec.clear();
  }

  void setup_for_append_or_recovery(const map<string, bufferlist>& attrset);
  void setup_for_read(const map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset);
  void flush(map<string, bufferlist>* attrset);
  void flush_for_rollback(map<string, boost::optional<bufferlist> >* attrset) const;

  uint64_t get_compressed_size() const {
    return masterRec.current_compressed_pos;
  }

  pair<uint64_t, uint64_t> offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const;

  void dump(Formatter* f) const;

  int try_decompress(const hobject_t& oid, uint64_t orig_offs, uint64_t len, const bufferlist& cs_bl, bufferlist* res_bl) const;
  int try_compress(const std::string& compression_method, const hobject_t& oid, const bufferlist& bl, uint64_t* off, bufferlist* res_bl);
};
typedef ceph::shared_ptr<CompressContext> CompressContextRef;

WRITE_CLASS_ENCODER(CompressContext::BlockInfoRecord)
WRITE_CLASS_ENCODER(CompressContext::BlockInfoRecordSetHeader)
WRITE_CLASS_ENCODER(CompressContext::MasterRecord)
#endif
