// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Igor Fedotov <ifedotov@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <string.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "osd/CompressContext.h"
#include "compressor/CompressionPlugin.h"

struct TestCompressor : public Compressor {
  enum Mode { NO_COMPRESS, COMPRESS, COMPRESS_NO_BENEFIT} mode;
  unsigned compress_calls, decompress_calls;
  const uint32_t TEST_MAGIC = 0xdeadbeef;

  TestCompressor() {
    clear();
  }
  void clear() {
    mode = COMPRESS;
    compress_calls = decompress_calls = 0;
  }
  void reset(Mode _mode = COMPRESS) {
    clear();
    mode = _mode;
  }
  virtual int compress(bufferlist& in, bufferlist& out) {
    int res = 0;
    ++compress_calls;
    if (mode != NO_COMPRESS) {
      uint32_t size = in.length();
      assert(size <= 1024 * 1024); //introducing input block size limit to be able to validate decompressed value
      ::encode(TEST_MAGIC, out);
      ::encode(size, out);
      if (mode == COMPRESS_NO_BENEFIT) {
	out.append(in);
      }
      res = 0;
    } else {
      res = -1;
    }
    return res;
  }
  virtual int decompress(bufferlist& in, bufferlist& out) {
    ++decompress_calls;
    uint32_t ui;
    bufferlist::iterator it = in.begin();
    ::decode(ui, it);
    EXPECT_EQ(ui,  TEST_MAGIC);
    ::decode(ui, it);
    EXPECT_NE(ui, 0u);
    EXPECT_LE(ui, 1024 * 1024u); //see assert in compress method above
    std::string res(ui, 'a');
    out.append(res);
    return 0;
  }
  virtual const char* get_method_name() {
    return method_name().c_str();
  }
  static const std::string& method_name() {
    static std::string s = "test_method";
    return s;
  }
};

struct TestCompressionPlugin : public CompressionPlugin {
  static TestCompressor* compressor;
  static CompressorRef compressorRef;

  TestCompressionPlugin(CephContext* cct) : CompressionPlugin(cct)  {
    if( compressor == NULL ) {
      compressor  = new TestCompressor();
      compressorRef.reset(compressor);
    }
  }
  virtual int factory(CompressorRef* cs, ostream* ss) {
    *cs = compressorRef;
    return 0;
  }
};

TestCompressor* TestCompressionPlugin::compressor=NULL;
CompressorRef TestCompressionPlugin::compressorRef;

/*
Introducing CompressContextTester class to access CompressContext protected members
*/
class CompressContextTester : public CompressContext {
 public:
  enum { 
    STRIPE_WIDTH=4096,
  };
  CompressContextTester() : CompressContext(STRIPE_WIDTH) {}
  void testMapping1() {
    clear();

    //putting three blocks into the map
    append_block(0, 8 * 1024, "some_compression", 4 * 1024);
    append_block(8 * 1024, 7 * 1024, "", 7 * 1024);
    append_block((8 + 7) * 1024, 1932, "another_compression", 932);

    EXPECT_EQ(get_compressed_size(), (4 + 7) * 1024 + 932u);

    pair<uint64_t, uint64_t> block_offs_start = map_offset(0, false);
    EXPECT_EQ(block_offs_start.first, 0u);
    EXPECT_EQ(block_offs_start.second, 0u);

    pair<uint64_t, uint64_t> block_offs_end = map_offset(0, true);
    EXPECT_EQ(block_offs_end.first, 8 * 1024u);
    EXPECT_EQ(block_offs_end.second, 4 * 1024u);

    block_offs_start = map_offset(1, false);
    EXPECT_EQ(block_offs_start.first, 0u);
    EXPECT_EQ(block_offs_start.second, 0u);

    block_offs_end = map_offset(1, true);
    EXPECT_EQ(block_offs_end.first, 8 * 1024u);
    EXPECT_EQ(block_offs_end.second, 4 * 1024u);

    block_offs_start = map_offset(8 * 1024, false);
    EXPECT_EQ(block_offs_start.first, 8 * 1024u);
    EXPECT_EQ(block_offs_start.second, 4 * 1024u);

    block_offs_end = map_offset(8 * 1024, true);
    EXPECT_EQ(block_offs_end.first, (8 + 7) * 1024u);
    EXPECT_EQ(block_offs_end.second, (4 + 7) * 1024u);

    block_offs_start = map_offset(8 * 1024 + 4096, false);
    EXPECT_EQ(block_offs_start.first, 8 * 1024u);
    EXPECT_EQ(block_offs_start.second, 4 * 1024u);

    block_offs_end = map_offset(8 * 1024 + 4096, true);
    EXPECT_EQ(block_offs_end.first, (8 + 7) * 1024u);
    EXPECT_EQ(block_offs_end.second, (4 + 7) * 1024u);

    block_offs_start = map_offset((8 + 7) * 1024, false);
    EXPECT_EQ(block_offs_start.first, (8 + 7) * 1024u);
    EXPECT_EQ(block_offs_start.second, (4 + 7) * 1024u);

    block_offs_end = map_offset((8 + 7) * 1024, true);
    EXPECT_EQ(block_offs_end.first, (8 + 7) * 1024u + 1932);
    EXPECT_EQ(block_offs_end.second, (4 + 7) * 1024u + 932);

    block_offs_start = map_offset((8 + 7) * 1024 + 1930, false);
    EXPECT_EQ(block_offs_start.first, (8 + 7) * 1024u);
    EXPECT_EQ(block_offs_start.second, (4 + 7) * 1024u);

    block_offs_end = map_offset((8 + 7) * 1024 + 1930, true);
    EXPECT_EQ(block_offs_end.first, (8 + 7) * 1024u + 1932);
    EXPECT_EQ(block_offs_end.second, (4 + 7) * 1024u + 932);

    pair<uint64_t, uint64_t> compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 132));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(131, 132));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(10, 4 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4 * 1024 + 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 - 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(2, 8 * 1024 - 2));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 + 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4 + 7) * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(1230, 8 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4 + 7) * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(2, (8 + 7) * 1024 + 1930));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4 + 7) * 1024u + 932);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8 * 1024, 1024));
    EXPECT_EQ(compressed_block.first, 4 * 1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8 * 1024 + 1023, 1024));
    EXPECT_EQ(compressed_block.first, 4 * 1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8 * 1024 + 1024, 6 * 1024));
    EXPECT_EQ(compressed_block.first, 4 * 1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8 * 1024 + 1024, 6 * 1024 + 1));
    EXPECT_EQ(compressed_block.first, 4 * 1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u + 932);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>((8 + 7) * 1024 + 1, 1));
    EXPECT_EQ(compressed_block.first, (4 + 7) * 1024u);
    EXPECT_EQ(compressed_block.second, 932u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>((8 + 7) * 1024 + 1931, 1));
    EXPECT_EQ(compressed_block.first, (4 + 7) * 1024u);
    EXPECT_EQ(compressed_block.second, 932u);
  }

  void testAppendAndFlush() {
    clear();

    std::string key;
    offset_to_key(0,key);

    EXPECT_FALSE(need_flush());
    //putting three blocks into the map
    append_block(0, 8 * 1024, "some_compression", 4 * 1024);
    append_block(8 * 1024, 7 * 1024, "", 7 * 1024);
    append_block((8 + 7) * 1024, 1932, "another_compression", 932);
    EXPECT_TRUE(need_flush());
    EXPECT_EQ(get_compressed_size(), (4 + 7) * 1024 + 932u);

    map<string, boost::optional<bufferlist> > rollback_attrs;
    map<string, bufferlist> attrs;
    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], boost::optional<bufferlist>() );
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], boost::optional<bufferlist>());

    flush(&attrs);
    MasterRecord mrec = get_master_record();
    EXPECT_EQ(attrs.size(), 2u);
    EXPECT_EQ(attrs[key].length(), mrec.block_info_record_length*3 + mrec.block_info_recordset_header_length);
    EXPECT_GT(attrs[ECUtil::get_cinfo_master_key()].length(), 0u);
    EXPECT_FALSE(need_flush());
    EXPECT_EQ(get_compressed_size(), (4 + 7) * 1024 + 932u);
    rollback_attrs.clear();
    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], attrs[key]);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], attrs[key]);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    clear();
    EXPECT_EQ(get_compressed_size(), 0u);
    rollback_attrs.clear();
    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], boost::optional<bufferlist>() );
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], boost::optional<bufferlist>());

    setup_for_append_or_recovery(attrs);
    EXPECT_EQ(get_compressed_size(), (4 + 7) * 1024 + 932u);
    rollback_attrs.clear();
    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], attrs[key]);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    rollback_attrs.clear();
    flush_for_rollback(&rollback_attrs); 
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], attrs[key]);

    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    //verifying context content encoded in attrs
    CompressContext::BlockInfoRecordSetHeader recHdr(0, 0);
    CompressContext::BlockInfoRecord rec;

    bufferlist::iterator it = attrs[ECUtil::get_cinfo_master_key()].begin();
    ::decode(mrec, it);
    EXPECT_EQ(mrec.current_original_pos, (8 + 7) * 1024 + 1932u);
    EXPECT_EQ(mrec.current_compressed_pos, (4 + 7) * 1024 + 932u);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(mrec.methods.size(), 2u);
    EXPECT_EQ(mrec.methods[0], "some_compression");
    EXPECT_EQ(mrec.methods[1], "another_compression");
    offset_to_key(0, key);
    it = attrs[key].begin();
    EXPECT_EQ(it.get_remaining(), mrec.block_info_record_length * 3 + mrec.block_info_recordset_header_length);

    recHdr.start_offset = recHdr.compressed_offset = 1234; //just fill with non-zero value
    ::decode(recHdr, it);
    EXPECT_EQ(recHdr.start_offset, 0u);
    EXPECT_EQ(recHdr.compressed_offset, 0u);

    ::decode(rec, it);
    EXPECT_EQ(rec.method_idx, 1u);
    EXPECT_EQ(rec.original_length, 8 * 1024u);
    EXPECT_EQ(rec.compressed_length, 4 * 1024u);
    ::decode(rec, it);
    EXPECT_EQ(rec.method_idx, 0u);
    EXPECT_EQ(rec.original_length, 7 * 1024u);
    EXPECT_EQ(rec.compressed_length, 7 * 1024u);
    ::decode(rec, it);
    EXPECT_EQ(rec.method_idx, 2u);
    EXPECT_EQ(rec.original_length, 1932u);
    EXPECT_EQ(rec.compressed_length, 932u);

    //verifying context content encoded in attrs when >1 recordsets are present
    attrs.clear();
    setup_for_append_or_recovery(attrs);
    uint64_t offs = 0, coffs = 0;
    uint32_t block_len = 4096, cblock_len = 4090;
    size_t recCount = CompressContext::RECS_PER_RECORDSET * 2 + 1;
    for (size_t i = 0; i < recCount; i++) {
      append_block(offs, block_len, "", cblock_len);
      offs += block_len;
    }
    flush(&attrs);

    EXPECT_EQ( attrs.size(), 2u);
    it = attrs[ECUtil::get_cinfo_master_key()].begin();
    mrec.clear();
    ::decode(mrec, it);
    EXPECT_EQ(mrec.current_original_pos, recCount * block_len);
    EXPECT_EQ(mrec.current_compressed_pos, recCount * cblock_len);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(mrec.methods.size(), 0u);

    offset_to_key(0, key);
    it = attrs[key].begin();
    EXPECT_EQ(it.get_remaining(),
	      mrec.block_info_record_length * recCount +
	      mrec.block_info_recordset_header_length * (1 + recCount / CompressContext::RECS_PER_RECORDSET));
    offs = coffs = 0;
    for (size_t i = 0; i < recCount; i++) {
      if ((i % CompressContext::RECS_PER_RECORDSET) == 0) {
	::decode(recHdr, it);
	EXPECT_EQ(recHdr.start_offset, offs);
	EXPECT_EQ(recHdr.compressed_offset, coffs);
      }
      rec.method_idx = 255;
      rec.original_length = rec.compressed_length = 1234;
      ::decode(rec, it);
      EXPECT_EQ(rec.method_idx, 0u);
      EXPECT_EQ(rec.original_length, block_len);
      EXPECT_EQ(rec.compressed_length, cblock_len);
      offs += block_len;
      coffs += cblock_len;
    }

    //verifying context content encoded in attrs when >1 blocksets are present
    attrs.clear();
    setup_for_append_or_recovery(attrs);
    offs = 0, coffs = 0;
    block_len = STRIPE_WIDTH, cblock_len = 4090;
    unsigned complete_blocksets=2;
    recCount = CompressContext::MAX_STRIPES_PER_BLOCKSET * complete_blocksets + 1;
    for (size_t i = 0; i < recCount; i++) {
      append_block(offs, block_len, "", cblock_len);
      offs += block_len;
    }
    flush(&attrs);

    EXPECT_EQ( attrs.size(), 4u);
    it = attrs[ECUtil::get_cinfo_master_key()].begin();
    mrec.clear();
    ::decode(mrec, it);
    EXPECT_EQ(mrec.current_original_pos, recCount * block_len);
    EXPECT_EQ(mrec.current_compressed_pos, recCount * cblock_len);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(mrec.methods.size(), 0u);

    offs = coffs = 0;
    for( uint64_t o = 0; o<recCount*STRIPE_WIDTH; o+=STRIPE_WIDTH*MAX_STRIPES_PER_BLOCKSET){
      offset_to_key(o, key);
      EXPECT_NE( attrs.find(key), attrs.end());
      it = attrs[key].begin();
      EXPECT_GE( it.get_remaining(), 0u);
      if( o/(STRIPE_WIDTH*MAX_STRIPES_PER_BLOCKSET) < complete_blocksets )
        EXPECT_EQ(it.get_remaining(),
		  mrec.block_info_record_length * MAX_STRIPES_PER_BLOCKSET +
		  mrec.block_info_recordset_header_length * (MAX_STRIPES_PER_BLOCKSET / CompressContext::RECS_PER_RECORDSET));
      else //last incomplete blockset
        EXPECT_EQ(it.get_remaining(),
		  mrec.block_info_record_length * 1 +
		  mrec.block_info_recordset_header_length * 1);

      size_t i = 0;
      while( it.get_remaining()>0) { // for (size_t i = 0; i < MAX_STRIPES_PER_BLOCKSET; i++) {
        if ((i % CompressContext::RECS_PER_RECORDSET) == 0) {
	  ::decode(recHdr, it);
	  EXPECT_EQ(recHdr.start_offset, offs);
	  EXPECT_EQ(recHdr.compressed_offset, coffs);
        }
        rec.method_idx = 255;
        rec.original_length = rec.compressed_length = 1234;
        ::decode(rec, it);
        EXPECT_EQ(rec.method_idx, 0u);
        EXPECT_EQ(rec.original_length, block_len);
        EXPECT_EQ(rec.compressed_length, cblock_len);
        offs += block_len;
        coffs += cblock_len;
        ++i;
      }
    }

    //
    //verify 'partial' rollback and subsequent functioning, i.e. some compression metadata is left in attributes but it shouldn't be used due to proper master record value
    //
    clear();
    attrs.clear();
    rollback_attrs.clear();
    setup_for_append_or_recovery( attrs );
    offs = 0;
    append_block(offs, 8 * 1024, "some_compression", 4 * 1024);
    offs+= 8*1024;
    append_block(offs, 4 * 1024, "some_compression", 4 * 1024);
    offs+= 4*1024;
    flush( &attrs );

    offset_to_key(0, key );
    setup_for_append_or_recovery( attrs );
    flush_for_rollback(&rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[key], attrs[key] );
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    append_block(offs, MAX_STRIPES_PER_BLOCKSET* 4096 - 12*1024, "some_compression", 4 * 1024);
    append_block(offs + MAX_STRIPES_PER_BLOCKSET* 4096 - 12*1024, 8*1024, "some_compression", 4 * 1024);
    flush(&attrs);
    EXPECT_EQ(attrs.size(), 3u);
    EXPECT_NE(rollback_attrs[key], attrs[key] );
    EXPECT_NE(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);
    attrs[key] = *rollback_attrs[key]; //doing rollback
    attrs[ECUtil::get_cinfo_master_key()]=*rollback_attrs[ECUtil::get_cinfo_master_key()];

    setup_for_read(attrs, 0, 12*1024-1);
    EXPECT_EQ( 2u, get_blocks_count() );
    pair<uint64_t, uint64_t> res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(0, 1));
    pair<uint64_t, uint64_t> expected(0, 4*1024);
    EXPECT_EQ( res, expected );
    res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(0, 8*1024));
    expected = pair<uint64_t, uint64_t>(0, 4*1024);
    EXPECT_EQ( res, expected );

    setup_for_read(attrs, 8*1024, 9*1024);
    EXPECT_EQ( 1u, get_blocks_count() );
    res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(8192, 4*1024));
    expected = pair<uint64_t, uint64_t>(4*1024, 4*1024);
    EXPECT_EQ( res, expected );
    res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(8192 + 120, 8));
    expected = pair<uint64_t, uint64_t>(4*1024, 4*1024);
    EXPECT_EQ( res, expected );

    setup_for_read(attrs, 8*1024-1, 8*1024 -1 + 1*1024);
    EXPECT_EQ( 2u, get_blocks_count() );
    res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(8191, 4097));
    expected = pair<uint64_t, uint64_t>(0, 8*1024);
    EXPECT_EQ( res, expected );

    res = offset_len_to_compressed_block( pair<uint64_t, uint64_t>(8191, 1));
    expected = pair<uint64_t, uint64_t>(0, 4*1024);
    EXPECT_EQ( res, expected );

    setup_for_append_or_recovery( attrs );
    append_block(offs, MAX_STRIPES_PER_BLOCKSET* 4096 - 12*1024, "some_compression", 4 * 1024);
    offs+=MAX_STRIPES_PER_BLOCKSET* 4096 - 12*1024;
    flush(&attrs);
    EXPECT_EQ(attrs.size(), 3u);

  }
  void testCompressSimple(TestCompressor* compressor) {
    hobject_t oid;
    std::string s1(8 * 1024, 'a');
    bufferlist in, out, out_res;
    uint64_t offs = 0;
    in.append(s1);
    clear();
    //single block compress
    compressor->reset(TestCompressor::COMPRESS);
    int r = try_compress(TestCompressor::method_name(), oid, in, &offs, &out);
    EXPECT_EQ(r, 0);
    EXPECT_NE(in.length(), out.length());
    EXPECT_EQ(out.length(), STRIPE_WIDTH);
    EXPECT_EQ(get_compressed_size(), STRIPE_WIDTH);
    EXPECT_EQ(offs, 0u);
    EXPECT_EQ(compressor->compress_calls, 1u);

    map<string, bufferlist> attrs;
    flush(&attrs);

    clear();
    offs = 0;
    setup_for_read(attrs, 0, s1.size());
    r = try_decompress(oid, offs, s1.size(), out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_EQ(s1.size(), out_res.length());
    EXPECT_TRUE(s1 == std::string(out_res.c_str(), out_res.length()));
    EXPECT_EQ(compressor->decompress_calls, 1u);

    ////multiple block compress attempts - the first has no benefit(bypassed), followed by successful and failed blocks
    clear();
    out.clear();
    out_res.clear();
    compressor->reset(TestCompressor::COMPRESS_NO_BENEFIT);
    offs=0;
    try_compress(TestCompressor::method_name(), oid, in, &offs, &out); //bypassed block
    EXPECT_EQ(r, 0);
    EXPECT_EQ(in.length(), out.length());
    EXPECT_EQ(get_compressed_size(), in.length());
    EXPECT_EQ(offs, 0u);
    EXPECT_EQ(compressor->compress_calls, 1u);

    flush(&attrs);

    MasterRecord mrec = get_master_record();
    EXPECT_EQ(mrec.current_original_pos, in.length());
    EXPECT_EQ(mrec.current_compressed_pos, in.length());
    EXPECT_EQ(mrec.methods.size(), 0u);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);

    out_res.append(out);
    out.clear();

    compressor->reset(TestCompressor::COMPRESS);
    setup_for_append_or_recovery(attrs);

    mrec = get_master_record();
    EXPECT_EQ(mrec.current_original_pos, in.length());
    EXPECT_EQ(mrec.current_compressed_pos, in.length());
    EXPECT_EQ(mrec.methods.size(), 0u);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(get_compressed_size(), out_res.length());
    offs=in.length();
    try_compress(TestCompressor::method_name(), oid, in, &offs, &out); //successfully compressed block
    EXPECT_EQ(r, 0);
    EXPECT_GT(in.length(), out.length());
    EXPECT_EQ(get_compressed_size(), out_res.length()+out.length());
    EXPECT_EQ(offs, out_res.length());
    EXPECT_EQ(compressor->compress_calls, 1u);

    flush(&attrs);
    mrec = get_master_record();
    EXPECT_EQ(mrec.current_original_pos, in.length()*2);
    EXPECT_EQ(mrec.current_compressed_pos, get_compressed_size());
    EXPECT_EQ(mrec.methods.size(), 1u);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    out_res.append(out);

    compressor->reset(TestCompressor::NO_COMPRESS);
    offs=in.length()*2;
    try_compress(TestCompressor::method_name(), oid, in, &offs, &out); //failed block compression
    EXPECT_EQ(r, 0);
    EXPECT_EQ(in.length(), out.length());
    EXPECT_EQ(get_compressed_size(), out_res.length()+out.length());
    EXPECT_EQ(offs, out_res.length());
    EXPECT_EQ(compressor->compress_calls, 1u);

    flush(&attrs);
    mrec = get_master_record();
    EXPECT_EQ(mrec.current_original_pos, in.length()*3);
    EXPECT_EQ(mrec.current_compressed_pos, get_compressed_size());
    EXPECT_EQ(mrec.methods.size(), 1u);
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    out_res.append(out);

    out.swap(out_res);
    out_res.clear();
    compressor->reset(TestCompressor::NO_COMPRESS);
    clear();
    offs = 0;
    setup_for_read(attrs, offs, s1.size());
    r = try_decompress(oid, offs, s1.size(), out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_TRUE(s1 == std::string(out_res.c_str()));
    EXPECT_EQ(compressor->decompress_calls, 0u);

    //reading two bytes: the first one from uncompressed block and the second one from compressed block
    out_res.clear();
    compressor->reset(TestCompressor::NO_COMPRESS);
    offs = s1.size()-1;
    setup_for_read(attrs, offs, offs+2);
    r = try_decompress(oid, offs, 2, out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_EQ( out_res.length(), 2u);
    EXPECT_TRUE('a' == out_res[0]);
    EXPECT_TRUE('a' == out_res[1]);
    EXPECT_EQ(compressor->decompress_calls, 1u);

    //reading a block starting at the last byte of the first block and finishing at the first byte of the last block
    out_res.clear();
    compressor->reset(TestCompressor::NO_COMPRESS);
    offs = s1.size()-1;
    setup_for_read(attrs, offs, offs + s1.size() + 2);
    r = try_decompress(oid, offs, s1.size() + 2, out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_EQ( out_res.length(), s1.size() + 2u);
    EXPECT_TRUE('a' == out_res[0]);
    EXPECT_TRUE('a' == out_res[1]);
    EXPECT_TRUE('a' == out_res[s1.size()]);
    EXPECT_TRUE('a' == out_res[s1.size()+1]);
    EXPECT_EQ(compressor->decompress_calls, 1u);

    ////multiple (fixed-size) blocks  compress
    vector< pair<uint64_t, uint64_t> >block_info; //compressed block offset, size
    clear();
    out_res.clear();
    compressor->reset(TestCompressor::COMPRESS);
    size_t block_count = 200;
    uint64_t offs4append = 0;
    for (size_t i = 0; i < block_count; i++) {
      offs = offs4append;
      out.clear();
      int r = try_compress(TestCompressor::method_name(), oid, in, &offs, &out);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(offs, out_res.length());
      offs4append += in.length();
      block_info.push_back(std::pair<uint64_t, uint64_t>(out_res.length(), out.length()));
      out_res.append(out);
    }
    EXPECT_EQ(out_res.length(), block_count * STRIPE_WIDTH);
    EXPECT_EQ(get_compressed_size(), block_count * STRIPE_WIDTH);
    EXPECT_EQ(offs, (block_count - 1)*STRIPE_WIDTH);
    EXPECT_EQ(compressor->compress_calls, block_count);

    out = out_res;
    out_res.clear();
    flush(&attrs);

    clear();
    offs = 0;
    uint64_t size = block_count * s1.size();
    setup_for_read(attrs, offs, size); //read as a single block
    r = try_decompress(oid, offs, size, out, &out_res);
    EXPECT_EQ(r, 0);
    for (size_t i = 0; i < block_count; i++) {
      std::string tmpstr(out_res.get_contiguous(i * s1.size(), s1.size()), s1.size());
      EXPECT_TRUE(s1 == tmpstr);
    }
    EXPECT_EQ(compressor->decompress_calls, block_count);

    //reading all the object but the first and last bytes
    clear();
    compressor->reset();
    out_res.clear();
    offs = 1;
    size = block_count * s1.size() - 1;
    setup_for_read(attrs, offs, size); //read as a single block
    r = try_decompress(oid, offs, size, out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_EQ(size, out_res.length());
    EXPECT_EQ(compressor->decompress_calls, block_count);

    //reading Nth block ( totally and partially )
    for (size_t i = 0; i < block_count; i++) {
      clear();
      compressor->reset();
      out_res.clear();
      offs = i * s1.size();
      size = s1.size();
      setup_for_read(attrs, offs, offs + size); //read as a single block

      bufferlist compressed_block;
      compressed_block.substr_of(out, block_info[i].first, block_info[i].second);

      r = try_decompress(oid, offs, size, compressed_block, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      std::string tmpstr(out_res.c_str(), s1.size());
      EXPECT_EQ(s1, tmpstr);
      EXPECT_EQ(compressor->decompress_calls, 1u);

      out_res.clear();
      offs = i * s1.size() + 2;
      size = s1.size() - 3;
      setup_for_read(attrs, offs, offs + size); //read as a single block
      r = try_decompress(oid, offs, size, out, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      EXPECT_EQ(compressor->decompress_calls, 2u);
    }

    ////multiple blocks (variable sizes) compress
    block_info.clear();
    clear();
    out_res.clear();
    compressor->reset(TestCompressor::COMPRESS);
    block_count = 200;
    const unsigned num_blocks = 8;
    string blocks[num_blocks] = {
      std::string(4096, 'a'),
      std::string(4096 * 3, 'a'),
      std::string(4096 * 8, 'a'),
      std::string(4096 * 9, 'a'),
      std::string(4096 * 31, 'a'),
      std::string(4096 * 127, 'a'),
      std::string(4096 * 129, 'a'),
      std::string(4096 * 140, 'a')
    };
    offs4append = 0;
    uint64_t total_size = 0;
    uint64_t total_blocks = 0;
    
    for (size_t i = 0; i < block_count; i++) {
      in.clear();
      in.append(blocks[ i % num_blocks]);
      total_size += in.length();
      unsigned blocks_to_append = in.length() / (get_block_size());
      blocks_to_append += in.length() % (get_block_size()) ? 1 : 0;
      total_blocks += blocks_to_append;

      offs = offs4append;
      out.clear();
      int r = try_compress(TestCompressor::method_name(), oid, in, &offs, &out);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(offs, out_res.length());
      offs4append += in.length();
      block_info.push_back(std::pair<uint64_t, uint64_t>(out_res.length(), out.length()));
      out_res.append(out);
    }
    EXPECT_EQ(compressor->compress_calls,
	      total_blocks - block_count / 8 /*these blocks aren't compressed due to small writes*/);
    EXPECT_EQ(out_res.length(), get_compressed_size());
    EXPECT_EQ(get_compressed_size(), block_count * STRIPE_WIDTH); //as we have pretty huge compression ratio all large blocks are expected to fit into single stripe

    flush(&attrs);
    mrec = get_master_record();
    EXPECT_NE(mrec.block_info_record_length, 0u);
    EXPECT_NE(mrec.block_info_recordset_header_length, 0u);
    unsigned records=0;
    for( uint64_t o = 0; o<offs4append; o+=STRIPE_WIDTH*MAX_STRIPES_PER_BLOCKSET){
      string key;
      offset_to_key( o, key );
      
      map<string, bufferlist>::iterator it = attrs.find(key);
      EXPECT_NE(it, attrs.end());
      bufferlist::iterator it_bl = it->second.begin();
      unsigned cur_rec=0;
      while( !it_bl.end()){
        if( (cur_rec % RECS_PER_RECORDSET) == 0) {
          BlockInfoRecordSetHeader hdr(0,0);
          ::decode( hdr, it_bl);
        }
        BlockInfoRecord rec;
        ::decode(rec, it_bl);
        ++cur_rec;
        ++records;
      }
      //attrs_len += attrs[key].length();
    }
    EXPECT_EQ(records, total_blocks);
    out = out_res;
    out_res.clear();

    //reading Nth block ( totally, partially and block+1 octet )
    uint64_t offs_to_read = 0;
    for (size_t i = 0; i < block_count; i++) {
      clear();
      compressor->reset();
      out_res.clear();
      size = blocks[ i % num_blocks ].size();
      offs = offs_to_read;
      setup_for_read(attrs, offs, offs + size); //read as a single block
      bufferlist compressed_block;
      compressed_block.substr_of(out, block_info[i].first, block_info[i].second);

      r = try_decompress(oid, offs, size, compressed_block, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      unsigned decompress_calls = 0;
      if (size > STRIPE_WIDTH) {
	decompress_calls += size / (get_block_size());
	decompress_calls += size % (get_block_size()) ? 1 : 0;
      }
      EXPECT_EQ(compressor->decompress_calls, decompress_calls);

      out_res.clear();
      compressor->reset();
      size = blocks[ i % num_blocks ].size() - 3;
      offs = offs_to_read + 2;
      setup_for_read(attrs, offs, offs + size); //read as a single block
      r = try_decompress(oid, offs, size, compressed_block, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      decompress_calls = 0;
      if (size > STRIPE_WIDTH) {
	decompress_calls += size / (get_block_size());
	decompress_calls += size % (get_block_size()) ? 1 : 0;
      }
      EXPECT_EQ(compressor->decompress_calls, decompress_calls);

      if (i < block_count - 1) {
	out_res.clear();
	compressor->reset();
	size = blocks[ i % num_blocks ].size() + 3; //read Nth blocks + 3 octets from the next one
	offs = offs_to_read + 2;
	setup_for_read(attrs, offs, offs + size);
	compressed_block.substr_of(out,
				   block_info[i].first,
				   block_info[i].second + block_info[i + 1].second);
	r = try_decompress(oid, offs, size, compressed_block, &out_res);
	EXPECT_EQ(r, 0);
	EXPECT_EQ(size, out_res.length());
	decompress_calls = 0;
	if (blocks[i % num_blocks].size() > STRIPE_WIDTH) {
	  decompress_calls += blocks[i % num_blocks].size() / (get_block_size());
	  decompress_calls += blocks[i % num_blocks].size() % (get_block_size()) ? 1 : 0;
	}
	if (blocks[(i + 1) % num_blocks].size() > STRIPE_WIDTH) {
	  ++decompress_calls;  //we need just single block decompress as we need 3 bytes from the second block only
	}
	EXPECT_EQ(compressor->decompress_calls, decompress_calls);
      }

      offs_to_read += blocks[ i % num_blocks ].size();

    }
  }
};


TEST(CompressContext, check_mapping) {
  CompressContextTester ctx;
  ctx.testMapping1();
}

TEST(CompressContext, check_append) {
  CompressContextTester ctx;
  ctx.testAppendAndFlush();
}


TEST(CompressContext, check_compress_decompress) {
  CompressContextTester ctx;
  ctx.testCompressSimple(TestCompressionPlugin::compressor);
}

int main(int argc, char** argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char**)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  PluginRegistry* reg = g_ceph_context->get_plugin_registry();
  TestCompressionPlugin compression_plugin(g_ceph_context);
  {
    Mutex::Locker m(reg->lock);
    reg->add("compressor", TestCompressor::method_name(), &compression_plugin);
  }
  reg->disable_dlclose = true;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_compresscontext &&
 *   valgrind --tool=memcheck \
 *      ./unittest_compresscontext \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
