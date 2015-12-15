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

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"
#include "CompressContext.h"
#include "compressor/Compressor.h"
#include "compressor/CompressionPlugin.h"
#include "common/debug.h"


#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix *_dout

const unsigned CompressContext::MAX_STRIPES_PER_BLOCK = 32;      //maximum amount of stripes that can be compressed as a single block
const unsigned CompressContext::RECS_PER_RECORDSET = 32;         //amount of comression information records per single record set, i.e. records grouped for seek speed-up.
const unsigned CompressContext::MAX_STRIPES_PER_BLOCKSET = 1024; //maximum stripes to be described by a single attribute
const int CompressContext::DEBUG_LEVEL = 1;  //debug level for compression stuff

void CompressContext::BlockInfoRecord::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(method_idx, bl);
  ::encode(original_length, bl);
  ::encode(compressed_length, bl);
  ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecord::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(method_idx, bl);
  ::decode(original_length, bl);
  ::decode(compressed_length, bl);
  DECODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(start_offset, bl);
  ::encode(compressed_offset, bl);
  ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(start_offset, bl);
  ::decode(compressed_offset, bl);
  DECODE_FINISH(bl);
}

uint8_t CompressContext::MasterRecord::add_get_method(const string& method) {
  uint8_t res = 0;
  if (!method.empty()) {
    for (size_t i = 0; i < methods.size() && res == 0; i++) {
      if (methods[i] == method) {
	res = i + 1;
      }
    }
    if (res == 0) {
      methods.push_back(method);
      res = methods.size();
    }
  }
  return res;
}

string CompressContext::MasterRecord::get_method_name(uint8_t index) const {
  string res;
  if (index > 0) {
    if (index <= methods.size()) {
      res = methods[index - 1];
    } else {
      res = "???";
    }
  }
  return res;
}

void CompressContext::MasterRecord::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(current_original_pos, bl);
  ::encode(current_compressed_pos, bl);
  ::encode(block_info_record_length, bl);
  ::encode(block_info_recordset_header_length, bl);
  ::encode(methods, bl);
  ENCODE_FINISH(bl);
}


void CompressContext::MasterRecord::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(current_original_pos, bl);
  ::decode(current_compressed_pos, bl);
  ::decode(block_info_record_length, bl);
  ::decode(block_info_recordset_header_length, bl);
  ::decode(methods, bl);
  DECODE_FINISH(bl);
}

CompressContext::~CompressContext() {
}

bool  CompressContext::can_compress(uint64_t offs) const {
  return (offs == 0 && masterRec.current_compressed_pos == 0) || masterRec.current_compressed_pos != 0;
}

uint64_t CompressContext::get_block_size() const {
  return MAX_STRIPES_PER_BLOCK * stripe_width;
}

unsigned CompressContext::get_blockset_size() const
{
  return MAX_STRIPES_PER_BLOCKSET * stripe_width;
}

CompressContext::BlockInfoRecordSetHeader CompressContext::search_starting_recordset(uint64_t offset, size_t record_set_size, bufferlist::iterator& it_bl)
{
  BlockInfoRecordSetHeader recset_header(0, 0);
  BlockInfoRecordSetHeader recset_header_next(0, 0);
  //searching for the record set that contains desired data range
  ::decode(recset_header, it_bl);

  bufferlist::iterator it_bl_next = it_bl;

  bool found = false;
  do {
    if (it_bl_next.get_remaining() >= record_set_size) {
      it_bl_next.advance(record_set_size - masterRec.block_info_recordset_header_length);
      ::decode(recset_header_next, it_bl_next);
      found = recset_header_next.start_offset > offset;
      if (!found) {
	it_bl = it_bl_next;
	recset_header = recset_header_next;
      }
    }
    else {
      found = true;
    }
  } while (!found);
  return recset_header;
}


void CompressContext::setup_for_read(const map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset) {
  assert(end_offset >= start_offset);

  clear();
  map<string, bufferlist>::const_iterator it_master = attrset.find(ECUtil::get_cinfo_master_key());
  map<string, bufferlist>::const_iterator end = attrset.end();
  if (it_master != end) {
    bufferlist::iterator it_val = const_cast<bufferlist&>(it_master->second).begin();
    ::decode(masterRec, it_val);

    assert(end_offset <= masterRec.current_original_pos);
    dout(DEBUG_LEVEL) << __func__
		      << " cur pos:(" << masterRec.current_original_pos << "," << masterRec.current_compressed_pos << ") offs:("
		      << "," << start_offset << "," << end_offset << ")" << dendl;

    size_t record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
    assert(masterRec.current_original_pos != 0 && record_set_size != 0);

    std::string key;
    offset_to_key(start_offset, key);
    map<string, bufferlist>::const_iterator it = attrset.find(key);

    if (start_offset >= get_blockset_size()) {
      bool step_back = true;
      if (it != end) {
	//check if desired data range starts in the previous attribute
	BlockInfoRecordSetHeader recset_header(0, 0);
	bufferlist::iterator it_bl = const_cast<bufferlist&>(it->second).begin();
	::decode(recset_header, it_bl);
	if (recset_header.start_offset <= start_offset)
	  step_back = false;
      }
      if (step_back) {
	offset_to_key(start_offset - get_blockset_size(), key);
	it = attrset.find(key);
      }
    }

    if (it != end) {
      bufferlist::iterator it_bl = const_cast<bufferlist&>(it->second).begin();
      BlockInfoRecordSetHeader recset_header = search_starting_recordset(start_offset, record_set_size, it_bl);

      bool stop = false;
      uint64_t cur_pos = recset_header.start_offset;
      uint64_t cur_cpos = recset_header.compressed_offset;
      uint64_t found_start_offset = 0;
      BlockInfo found_bi;
      bool start_found = false;
      boost::optional<uint64_t> last_block_offs;

      do {

	while (!stop && !it_bl.end()) {
	  if ((it_bl.get_off() % record_set_size) == 0) {
	    ::decode(recset_header, it_bl);
	  }
	  BlockInfoRecord rec;
	  ::decode(rec, it_bl);

	  if (cur_pos <= start_offset && (!start_found || cur_pos > found_start_offset)) {
	    found_start_offset = cur_pos;
	    found_bi = BlockInfo(rec.method_idx, cur_cpos);
	    start_found = true;
	  }
	  if (cur_pos > start_offset) {
	    blocks[cur_pos] = BlockInfo(rec.method_idx, cur_cpos);
	    last_block_offs = cur_pos;
	    if (cur_pos >= end_offset) {
	      stop = true;
	    }
	  }

	  cur_pos += rec.original_length;
	  cur_cpos += rec.compressed_length;
	  assert(cur_pos <= masterRec.current_original_pos);
	  assert(cur_cpos <= masterRec.current_compressed_pos);
	}
        stop = stop || cur_pos==masterRec.current_original_pos; //an additional check to avoid garbage attributes reading that may exist after previous rollback

	if( !stop ){
	  string key0(key);
	  offset_to_key(cur_pos, key);
          stop = key == key0;
	  if (!stop) {
	    it = attrset.find(key);
	    if (it != end)
	      it_bl = const_cast<bufferlist&>(it->second).begin();
	  }
        }
      } while (!stop && it!=end);

      if (start_found) {
	blocks[found_start_offset] = found_bi;
	dout(DEBUG_LEVEL) << __func__
	  << " first block:(" << found_start_offset << ", " << (int)found_bi.method_idx << "," << found_bi.target_offset << ")" << dendl;
      }

      if (last_block_offs.is_initialized()) {
	BlockInfo& bi = blocks[last_block_offs.get()];
	dout(DEBUG_LEVEL) << __func__
	  << " Last block:(" << last_block_offs.get() << ", " << (int)bi.method_idx << "," << bi.target_offset << ")" << dendl;
      }
      else {
	dout(DEBUG_LEVEL) << __func__ << " Last block:(Not found)" << dendl;
      }
    } else {
      dout(DEBUG_LEVEL) << __func__ << " No compression info found" << dendl;
    }
  } //if (it_master != end) 
}

void CompressContext::setup_for_append_or_recovery(const map<string, bufferlist>& attrset) {
  clear();

  map<string, bufferlist>::const_iterator it_attrs = attrset.find(ECUtil::get_cinfo_master_key());
  if (it_attrs != attrset.end()) {
    bufferlist::iterator it = const_cast<bufferlist&>(it_attrs->second).begin();
    ::decode(masterRec, it);
    prevMasterRec = masterRec;
  }
  std::string key;
  offset_to_key(prevMasterRec.current_original_pos, key);

  it_attrs = attrset.find(key);
  if (it_attrs != attrset.end()) {
    prev_blocks_encoded = it_attrs->second;
    prev_blocks_key = key;
  }
  dout(DEBUG_LEVEL) << __func__
    << " cur pos:(" << masterRec.current_original_pos << "," << masterRec.current_compressed_pos << ")" << dendl;
}


void CompressContext::flush(map<string, bufferlist>* attrset) {
  assert(attrset);
  if (need_flush()) { //some changes have been made to the context
    bufferlist bl(prev_blocks_encoded);
    std::string key0 = prev_blocks_key;

    size_t record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
    assert((prevMasterRec.current_original_pos==0 && record_set_size == 0) || (prevMasterRec.current_original_pos!=0 && record_set_size != 0));

    std::string key;
    for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
      CompressContext::BlockMap::const_iterator it_next = it;
      ++it_next;
      uint64_t next_offset, next_coffset;
      if (it_next == blocks.end()) {
	next_offset = masterRec.current_original_pos;
	next_coffset = masterRec.current_compressed_pos;
      } else {
	next_offset = it_next->first;
	next_coffset = it_next->second.target_offset;
      }
      offset_to_key(it->first, key);

      if (key != key0) {
	if (!key0.empty())
	  (*attrset)[key0] = bl;
	key0 = key;
	bl.clear();
      }

      BlockInfoRecord rec(it->second.method_idx, next_offset - it->first, next_coffset - it->second.target_offset);

      if (record_set_size == 0) { //first record to be added - need to measure rec sizes
	BlockInfoRecordSetHeader header(it->first, it->second.target_offset);
	size_t old_len = bl.length();
	::encode(header, bl);
	masterRec.block_info_recordset_header_length = bl.length() - old_len;
	old_len = bl.length();
	::encode(rec, bl);
	masterRec.block_info_record_length = bl.length() - old_len;

	record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
	assert(record_set_size != 0);
      }
      else {
	if ((bl.length() % record_set_size) == 0) {
	  BlockInfoRecordSetHeader header(it->first, it->second.target_offset);
	  ::encode(header, bl);
	}
	::encode(rec, bl);
      }
    }

    (*attrset)[key] = bl;

    bufferlist master_bl;
    ::encode(masterRec, master_bl);
    (*attrset)[ECUtil::get_cinfo_master_key()] = master_bl;

    dout(DEBUG_LEVEL) << __func__ << " cctx: Lengths:( master rec:"
		      << master_bl.length() << ", block info:"
		      << bl.length() << ", block info rec length:"
		      << masterRec.block_info_record_length << ", block info recset header length:"
		      << masterRec.block_info_recordset_header_length << ") MRec: ("
		      << masterRec.current_original_pos << "," << masterRec.current_compressed_pos << ") prev MRec("
		      << prevMasterRec.current_original_pos << "," << prevMasterRec.current_compressed_pos << ")" << dendl;

    prev_blocks_key.swap(key);
    prev_blocks_encoded.swap(bl);
    prevMasterRec = masterRec;
    blocks.clear();
  }
}

void CompressContext::flush_for_rollback(map<string, boost::optional<bufferlist> >* attrset) const {
  assert(attrset);

  if (!prev_blocks_key.empty()) {
    (*attrset)[prev_blocks_key] = prev_blocks_encoded;
  } else {
    string key;
    offset_to_key( masterRec.current_original_pos, key);
    (*attrset)[key] = boost::optional<bufferlist>();    //to trigger record removal on rollback
  }

  if (prevMasterRec.current_original_pos != 0) {
    bufferlist bl;
    ::encode(prevMasterRec, bl);
    (*attrset)[ECUtil::get_cinfo_master_key()] = bl;
  } else {
    (*attrset)[ECUtil::get_cinfo_master_key()] = boost::optional<bufferlist>();    //to trigger record removal on rollback
  }
}


void CompressContext::dump(Formatter* f) const {
  f->dump_unsigned("current_original_pos", masterRec.current_original_pos);
  f->dump_unsigned("current_compressed_pos", masterRec.current_compressed_pos);
  f->open_object_section("blocks");
  for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
    f->open_object_section("block");
    f->dump_unsigned("offset", it->first);
    f->dump_string("method", masterRec.get_method_name(it->second.method_idx));
    f->dump_unsigned("target_offset", it->second.target_offset);
    f->close_section();
  }
  f->close_section();
}

void CompressContext::append_block(uint64_t original_offset,
				   uint64_t original_size,
				   const string& method,
				   uint64_t new_block_size) {
  assert(original_size != 0);
  assert(original_offset == masterRec.current_original_pos);

  uint8_t method_idx = masterRec.add_get_method(method);

  blocks[original_offset] = BlockInfo(method_idx, masterRec.current_compressed_pos);

  masterRec.current_original_pos += original_size;
  masterRec.current_compressed_pos += new_block_size;
}

pair<uint64_t, uint64_t> CompressContext::offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const {
  uint64_t start_offs = offs_len_pair.first;
  uint64_t end_offs = offs_len_pair.first + offs_len_pair.second - 1;
  assert(masterRec.current_original_pos == 0 || start_offs < masterRec.current_original_pos);
  assert(masterRec.current_original_pos == 0 || end_offs < masterRec.current_original_pos);

  uint64_t res_start_offs = masterRec.current_original_pos != 0 ? map_offset(start_offs, false).second : start_offs;
  uint64_t res_end_offs = masterRec.current_original_pos != 0 ? map_offset(end_offs, true).second : end_offs + 1;

  return std::pair<uint64_t, uint64_t>(res_start_offs, res_end_offs - res_start_offs);
}

bool CompressContext::less_upper(const uint64_t& val1, const BlockMap::value_type& val2) {
  return val1 < val2.first;
}

bool CompressContext::less_lower(const BlockMap::value_type& val1, const uint64_t& val2) {
  return val1.first < val2;
}

pair<uint64_t, uint64_t> CompressContext::map_offset(uint64_t offs, bool next_block_flag) const {
  pair<uint64_t, uint64_t> res; //original block offset, compressed block offset
  BlockMap::const_iterator it;
  if (next_block_flag) {
    it = std::upper_bound(blocks.begin(), blocks.end(), offs, less_upper);
    if (it != blocks.end()) {
      res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
    } else {
      res = std::pair<uint64_t, uint64_t>(masterRec.current_original_pos, masterRec.current_compressed_pos);
    }
  } else {

    it = std::lower_bound(blocks.begin(), blocks.end(), offs, less_lower);
    if (it == blocks.end() || it->first != offs) { //lower_end returns iterator to the specified value if present or the next entry
      if( it == blocks.begin() ){
        dout(0)<<"ifed:"<<offs<<","<< blocks.size()<<dendl;
      }
      assert(it != blocks.begin());
      --it;
    }
    res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
  }
  return res;
}

void CompressContext::offset_to_key(uint64_t offs, std::string& key) const
{
  offs /= get_blockset_size();
  char buf[9];
  sprintf(buf, "%llx", (long long unsigned)offs);
  key = ECUtil::get_cinfo_key_prefix();
  key += buf;
}

int CompressContext::try_decompress(const hobject_t& oid, uint64_t orig_offs, uint64_t len, const bufferlist& cs_bl, bufferlist* res_bl) const {
  assert(res_bl);
  int res = 0;
  uint64_t appended = 0;
  if (masterRec.current_original_pos == 0) { //no compression was applied to the object
    dout(DEBUG_LEVEL) << __func__ << " bypassing: oid=" << oid << " params:(" << cs_bl.length() << "," << orig_offs << ", " << len << ")" << dendl;    res_bl->append(cs_bl);
    appended += cs_bl.length();
  } else {
    dout(DEBUG_LEVEL) << __func__ << " processing oid=" << oid << " params:(" << cs_bl.length() << "," << orig_offs << ", " << len << ")" << dendl;
    assert(masterRec.current_original_pos >= orig_offs + len);
    assert(blocks.size() > 0);

    uint64_t cur_block_offs, cur_block_len, cur_block_coffs, cur_block_clen;
    string cur_block_method;
    BlockMap::const_iterator it = blocks.begin();

    bufferlist::iterator cs_bl_pos = const_cast<bufferlist&>(cs_bl).begin();
    do {
      cur_block_offs = it->first;
      cur_block_coffs = it->second.target_offset;
      cur_block_method = masterRec.get_method_name(it->second.method_idx);
      ++it;
      if (it == blocks.end()) {
	cur_block_len = masterRec.current_original_pos - cur_block_offs;
	cur_block_clen = masterRec.current_compressed_pos - cur_block_coffs;
      } else {
	cur_block_len = it->first - cur_block_offs;
	cur_block_clen = it->second.target_offset - cur_block_coffs;
      }
      if (cur_block_offs + cur_block_len >= orig_offs) { //skipping map entries for data ranges prior to the desired one.
	bufferlist cs_bl1, tmp_bl;

	CompressorRef cs_impl;
	if( !cur_block_method.empty() ){
	  cs_impl = Compressor::create(g_ceph_context, cur_block_method);
	}
	if ( cs_impl == 0 && !cur_block_method.empty()) {
	  derr << __func__ << " failed to create decompression engine for " << cur_block_method << dendl;
	  res = -1;
	} else if (cs_impl == NULL) {
	  uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;

	  uint64_t len2splice = cur_block_len - offs2splice;
	  len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : 0 ;

	  offs2splice += cs_bl_pos.get_off();
	  dout(DEBUG_LEVEL) << __func__ << " non-compressed block: oid=" << oid << " state:(" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << ")" << dendl;
	  if (offs2splice == 0 && len2splice == cs_bl.length()) { //current block is completely within the requested range
	    res_bl->append(cs_bl);
	  } else {
	    tmp_bl.substr_of(cs_bl, offs2splice, len2splice);
	    res_bl->append(tmp_bl);
	  }
	  appended += len2splice;
	} else {
	  dout(DEBUG_LEVEL) << __func__ << " decompressing: oid = " << oid << " state : ("
			    << cur_block_method << "," << cur_block_offs << "," << cur_block_len << "," << cur_block_coffs << "," << cur_block_clen << ", " << cs_bl_pos.get_off() << ")" << dendl;

	  uint32_t real_len = 0;
	  bufferlist::iterator it = cs_bl_pos;
	  ::decode(real_len, it);
	  assert(cur_block_clen >= real_len);
	  assert(it.get_remaining() >= real_len);
	  cs_bl1.substr_of(cs_bl, it.get_off(), real_len);

	  int r = cs_impl->decompress(
		    cs_bl1,
		    tmp_bl);
	  if (r < 0) {
	    derr << __func__ << ": decompress(" << cur_block_method << ")"
		 << " returned an error: " << r << dendl;
	    res = -1;
	  } else {
	    uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;
	    uint64_t len2splice = tmp_bl.length() - offs2splice;
	    len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : 0;

	    dout(DEBUG_LEVEL) << __func__ << "decompressed: oid=" << oid << " state("
			      << offs2splice << "," << len2splice << "," << tmp_bl.length() << ")" << dendl;

	    if (offs2splice == 0 && len2splice == tmp_bl.length()) { //decompressed block is completely within the requested range
	      res_bl->append(tmp_bl);
	    } else {
	      tmp_bl.splice(offs2splice, len2splice, res_bl);
	    }
	    appended += len2splice;
	  }
	}
      }

      cs_bl_pos.advance(cur_block_clen);
    } while (it != blocks.end() && appended < len && res == 0);
  }
  assert(res != 0 || (res == 0 && appended == len));
  return res;
}

int CompressContext::do_compress(CompressorRef cs_impl, const bufferlist& block2compress, bufferlist* result_bl) const {
  int res = -1;
  bufferlist tmp_bl;
  assert(result_bl);
  int r = cs_impl->compress(const_cast<bufferlist&>(block2compress), tmp_bl);
  if (r == 0) {
    bufferlist tmp_bl0;
    uint32_t real_len = tmp_bl.length();
    ::encode(real_len, tmp_bl0);
    unsigned block_len = tmp_bl.length() + tmp_bl0.length();
    if (block_len < block2compress.length()) {
      result_bl->append(tmp_bl0);
      result_bl->append(tmp_bl);
      res = 1;
    } else {
      result_bl->append(block2compress);
      res = 0;
    }
  }
  return res;
}

int CompressContext::try_compress(const std::string& compression_method, const hobject_t& oid, const bufferlist& bl0, uint64_t* off, bufferlist* res_bl) {
  assert(off);
  assert(res_bl);
  bufferlist bl_cs(bl0);
  bufferlist bl;
  CompressContext new_cinfo(*this);

  dout(DEBUG_LEVEL) << __func__ << " compressing oid=" << oid << " params("
		    << compression_method << "," << *off << ", " << bl0.length() << ")" << dendl;

  bool compressed = false, failure = false;
  uint64_t prev_compressed_pos = masterRec.current_compressed_pos;
  CompressorRef cs_impl;
  if( !compression_method.empty() ){
    cs_impl = Compressor::create(g_ceph_context, compression_method);
    if (cs_impl == 0) {
      derr << __func__ << " failed to create compression engine for " << compression_method << dendl;
    }
  }

  //apply compression if that's a first block or it's been already applied to previous blocks
  bool compress_permitted = can_compress(*off); //if current object metadata state is consistent to apply compression
  if ( compress_permitted && cs_impl != NULL) {
    if (bl_cs.length() > stripe_width) {

      bufferlist::iterator it = bl_cs.begin();

      uint64_t cur_offs = *off;
      while (!it.end() && !failure) {
	uint64_t block_size = MIN(it.get_remaining(), get_block_size());
	bufferlist block2compress, compressed_block;
	uint64_t prev_len = bl.length();
	it.copy(block_size, block2compress);

	int r0 = do_compress(cs_impl, block2compress, &bl);
	if (r0 < 0) {
	  derr << __func__ << " block compression failed, left uncompressed, oid=" << oid << " params("
	       << compression_method << "," << block2compress.length() << ")" << dendl;
	  failure = true;
	} else {
	  if (it.end()) {
	    unsigned padded_size = ECUtil::stripe_info_t::pad_to_stripe_width(stripe_width, bl.length());
	    if (padded_size > bl.length()) {
	      bl.append_zero(padded_size - bl.length());
	    }
	  }
	  new_cinfo.append_block(cur_offs, block2compress.length(), r0 ? compression_method : "", bl.length() - prev_len);
	}
	cur_offs += block2compress.length();
      }
      if (!failure && bl.length() < ECUtil::stripe_info_t::pad_to_stripe_width(stripe_width, bl_cs.length())) {
	compressed = true;
	dout(DEBUG_LEVEL) << " block(s) compressed, oid=" << oid << ", ratio = " << (float)bl_cs.length() / bl.length() << dendl;
      } else if (failure) {
	dout(DEBUG_LEVEL) << " block(s) compression bypassed due to failure, oid=" << oid << dendl;
      } else {
	dout(DEBUG_LEVEL) << "block(s) compression bypassed, oid=" << oid << dendl;
      }

    } else {
      dout(DEBUG_LEVEL) << "block(s) compression bypassed, too small, oid=" << oid << dendl;
    }
  }
  if (!compressed) { //the whole block wasn't compressed or there is no benefit in compression
    *res_bl = bl0;
    if( compress_permitted ) {
      uint64_t left = res_bl->length();
      uint64_t o =*off;
      uint64_t bs_size = get_blockset_size();
      while( left>0 ){
        uint64_t to_append = MIN( left, bs_size);
        to_append = MIN( to_append, bs_size - (o % bs_size));
        append_block(o, to_append, "", to_append);
        left-=to_append;
        o+=to_append;
      }
    }
  } else {
    bl.swap(*res_bl);
    swap(new_cinfo);
  }
  *off = prev_compressed_pos;
  return 0;
}
