// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2016 Mirantis, Inc
*
* Author: Igor Fedotov <ifedotov@mirantis.com>
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/

#include <sstream>
#include <vector>
#include "gtest/gtest.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "os/bluestore/ExtentManager.h"

typedef pair<uint64_t, uint32_t> OffsLenTuple;
typedef vector<OffsLenTuple> OffsLenList;

struct WriteTuple
{
  uint64_t offs;
  uint32_t len;
  uint32_t csum;
  WriteTuple()
    : offs(0), len(0), csum(0) {}
  WriteTuple(uint64_t _offs, uint32_t _len, uint32_t _csum)
    : offs(_offs), len(_len), csum(_csum) {}
  WriteTuple(const WriteTuple& from)
    : offs(from.offs), len(from.len), csum(from.csum) {}

  bool operator==(const WriteTuple& from) const {
    return offs == from.offs && len == from.len && csum == from.csum;
  }
};

typedef vector<WriteTuple> WriteList;

struct CheckTuple {
  bluestore_blob_t::CSumType type;
  uint32_t csum_block_size;
  vector<char> csum_data;
  bufferlist source;

  CheckTuple(bluestore_blob_t::CSumType _type, uint32_t _csum_block_size, const vector<char>& _csum_data, const bufferlist& _source)
    : type(_type), csum_block_size(_csum_block_size), csum_data(_csum_data), source(_source) {
  }
};
typedef vector<CheckTuple> CheckList;

enum {
  PEXTENT_BASE = 0x12345, //just to have pextent offsets different from lextent ones
};

class TestExtentManager
    : public ExtentManager::BlockOpInterface,
      public ExtentManager::CompressorInterface,
      public ExtentManager::CheckSumVerifyInterface,
      public ExtentManager {

public:
  TestExtentManager() 
    : ExtentManager::BlockOpInterface(),
      ExtentManager::CompressorInterface(),
      ExtentManager::CheckSumVerifyInterface(),
      ExtentManager(*this, *this, *this),
      m_allocNextOffset(0),
      m_fail_compress(false),
      m_cratio(2) {
  }
  OffsLenList m_reads, m_zeros, m_releases, m_compresses;
  WriteList m_writes;
  CheckList m_checks;
  uint64_t m_allocNextOffset;
  bool m_fail_compress;
  int m_cratio;

  //Intended to create EM backup thus performs incomplete copy that covers EM state only. Op history and test wrapper state aren't copied.
  void operator= (const TestExtentManager& from) {
    ExtentManager::m_lextents = from.m_lextents;
    ExtentManager::m_blobs = from.m_blobs;
  }
  const bluestore_lextent_map_t& lextents() const { return m_lextents; }
  const bluestore_blob_map_t& blobs() const { return m_blobs; }

  bool checkRead(const OffsLenTuple& r) {
    return std::find(m_reads.begin(), m_reads.end(), r) != m_reads.end();
  }
  bool checkReleases(const OffsLenTuple& r) {
    return std::find(m_releases.begin(), m_releases.end(), r) != m_releases.end();
  }
  bool checkZero(const OffsLenTuple& r) {
    return std::find(m_zeros.begin(), m_zeros.end(), r) != m_zeros.end();
  }
  bool checkWrite(uint64_t offs, uint32_t len, const bufferlist& bl0, uint32_t input_offset = 0, uint32_t input_len = 0) {
    assert(bl0.length() > input_offset);
    input_len = input_len ? input_len : bl0.length() - input_offset;
    bufferlist bl;
    bl.substr_of(bl0, input_offset, input_len);
    bl.append_zero( ROUND_UP_TO(input_len, get_block_size()) - input_len);
    uint32_t crc = bl.crc32c(0);
    WriteTuple w(offs, len, crc);
    return std::find(m_writes.begin(), m_writes.end(), w) != m_writes.end();
  }

  void _calc_crc32(uint32_t csum_value_size, uint32_t csum_block_size, uint32_t source_offs, uint32_t source_len, const bufferlist& source, vector<char>* csum_data) {
    while (source_len > 0) {
      bufferlist bl;
      uint32_t l = MIN(source_len, csum_block_size);
      bl.substr_of(source, source_offs, l);

      source_offs += l;
      source_len -= l;
      auto crc = bl.crc32c(0);
      for (size_t i = 0; i < csum_value_size; i++)
	csum_data->push_back(((char*)&crc)[i]);
    }
  }

  bool checkCSum(uint32_t csum_val_size, uint32_t csum_block_size, const vector<char> csum_data, const bufferlist& bl0) {
    vector<char> my_csum_data;
    auto source_len = bl0.length();
    uint32_t source_offs = 0;
    _calc_crc32(csum_val_size, csum_block_size, source_offs, source_len, bl0, &my_csum_data);
    return my_csum_data == csum_data;
  }

  bool checkCompress(const OffsLenTuple& r) {
    return std::find(m_compresses.begin(), m_compresses.end(), r) != m_compresses.end();
  }

  void setup_csum() {
    for(auto it = m_blobs.begin(); it != m_blobs.end(); it++) {
      it->second.csum_type = bluestore_blob_t::CSUM_CRC32C;
      it->second.csum_block_order = 13; //read block size = 8192
      uint64_t size = ROUND_UP_TO(it->second.length, it->second.get_csum_block_size());
      size_t blocks = size / it->second.get_csum_block_size();
      it->second.csum_data.resize(it->second.get_csum_value_size() * blocks);

      //fill corresponding csum with block number to be able to verify that proper csum value is passed
      for(size_t i = 0; i < it->second.csum_data.size(); i += it->second.get_csum_value_size())
	it->second.csum_data[i] = i / it->second.get_csum_value_size();
    }
  }
  void prepareTestSet4SimpleRead(bool compress, bool csum_enable = false) {

    unsigned f = compress ? bluestore_blob_t::BLOB_COMPRESSED : 0;

    m_lextents[0] = bluestore_lextent_t(10, 0, 0x8000, 0);
    m_blobs[10] = bluestore_blob_t(0x8000, bluestore_extent_t(PEXTENT_BASE + 0x00000, 1 * get_min_alloc_size()), f);

    m_lextents[0x8000] = bluestore_lextent_t(1, 0, 0x2000, 0);
    m_blobs[1] = bluestore_blob_t(0x4000, bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * get_min_alloc_size()), f);

    //hole at 0x0a000~0xc000

    m_lextents[0x16000] = bluestore_lextent_t(2, 0, 0x3000, 0);
    m_blobs[2] = bluestore_blob_t(0x3000, bluestore_extent_t(PEXTENT_BASE + 0x20000, 1 * get_min_alloc_size()), f);

    m_lextents[0x19000] = bluestore_lextent_t(3, 0, 0x17610, 0);
    m_blobs[3] = bluestore_blob_t(0x18000, bluestore_extent_t(PEXTENT_BASE + 0x40000, 2 * get_min_alloc_size()), f);

    //hole at 0x30610~0x29f0

    m_lextents[0x33000] = bluestore_lextent_t(4, 0x0, 0x1900, 0);
    m_blobs[4] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x80000, 1 * get_min_alloc_size()), f);

    m_lextents[0x34900] = bluestore_lextent_t(5, 0x400, 0x1515, 0);
    m_blobs[5] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x90000, 3 * get_min_alloc_size()), f);

    m_lextents[0x35e15] = bluestore_lextent_t(6, 0x0, 0xa1eb, 0);
    m_blobs[6] = bluestore_blob_t(0xb000, bluestore_extent_t(PEXTENT_BASE + 0xc0000, 1 * get_min_alloc_size()), f);

    //hole at 0x40000~

    if(csum_enable){
      setup_csum();
    }
  }

  void prepareTestSet4SplitBlobRead(bool compress, bool csum_enable = false) {

    unsigned f = compress ? bluestore_blob_t::BLOB_COMPRESSED : 0;

    //hole at 0~100
    m_lextents[0x100] = bluestore_lextent_t(100, 0, 0x8000, 0);
    m_blobs[100] = bluestore_blob_t(0xa000, bluestore_extent_t(PEXTENT_BASE + 0xa0000, 1 * get_min_alloc_size()), f);

    m_lextents[0x8100] = bluestore_lextent_t(1, 0, 0x200, 0);
    m_blobs[1] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * get_min_alloc_size()), f);

    m_lextents[0x8300] = bluestore_lextent_t(2, 0, 0x1100, 0);
    m_blobs[2] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x20000, 2 * get_min_alloc_size()), f);

    //hole at 0x9400~0x100

    m_lextents[0x9500] = bluestore_lextent_t(100, 0x9400, 0x200, 0);

    //hole at 0x9700~

    if(csum_enable){
      setup_csum();
    }
  }

  void prepareTestSet4SplitBlobMultiExtentRead(bool compress, bool csum_enable = false) {

    unsigned f = compress ? bluestore_blob_t::BLOB_COMPRESSED : 0;

    //hole at 0~100
    m_lextents[0x100] = bluestore_lextent_t(100, 0, 0x8000, 0);
    m_blobs[100] = bluestore_blob_t(0xa000, f);
    m_blobs[100].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0xa0000, 0x6000));
    m_blobs[100].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0xb0000, 0x6000));

    m_lextents[0x8100] = bluestore_lextent_t(1, 0, 0x200, 0);
    m_blobs[1] = bluestore_blob_t(0x2000, f);
    m_blobs[1].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * get_min_alloc_size() / 2));
    m_blobs[1].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x18000, 1 * get_min_alloc_size() / 2));

    m_lextents[0x8300] = bluestore_lextent_t(2, 0, 0x1100, 0);
    m_blobs[2] = bluestore_blob_t(0x2000, f);
    m_blobs[2].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x20000, 1 * get_min_alloc_size()));

    //hole at 0x9400~0x100

    m_lextents[0x9500] = bluestore_lextent_t(100, 0x9400, 0x200, 0);

    //hole at 0x9700~0x6600
    //hole at 0x10000~0x100

    m_lextents[0x10100] = bluestore_lextent_t(3, 0x100, 0xcf00, 0);
    m_blobs[3] = bluestore_blob_t(0x26b00, f);
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x30000, 0x8000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x40000, 0x6000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x50000, 0xc000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x60000, 0x12000));

    //hole at 0x1d000~0x300
    m_lextents[0x1d300] = bluestore_lextent_t(3, 0xd300, 0x1100, 0);
    //hole at 0x1e400~0x5700
    m_lextents[0x23b00] = bluestore_lextent_t(3, 0x13b00, 0x10000, 0);
    m_lextents[0x33b00] = bluestore_lextent_t(3, 0x23b00, 0x3000, 0);
    //hole at 36ff1~

    if(csum_enable){
      setup_csum();
    }
  }


  void reset(bool total) {
    if (total){
      m_lextents.clear();
      m_blobs.clear();
      m_allocNextOffset = 0;
      m_fail_compress = false;
      m_cratio = 2;
    }
    m_reads.clear();
    m_writes.clear();
    m_zeros.clear();
    m_releases.clear();
    m_checks.clear();
    m_compresses.clear();
  }

  void prepareWriteData(uint64_t offset0, uint32_t length, bufferlist* res_bl)
  {
    res_bl->clear();
    auto offset = offset0;

    unsigned shift = 17; //arbitrary selected value to have non-zero value at offset 0
    bufferptr buf(length);
    for (unsigned o = 0; o < length; o++){
      buf[o] = (o + offset + shift) & 0xff;  //fill resulting buffer with some checksum pattern
      ++offset;
    }
    res_bl->append(buf);
  }


  ////////////////BlockOpInterface implementation////////////
  virtual uint64_t get_block_size() { return 4096; }

protected:
  ////////////////BlockOpInterface implementation////////////
  virtual int read_block(uint64_t offset0, uint32_t length, void* opaque, bufferlist* result)
  {
    uint64_t block_size = get_block_size();
    offset0 -= PEXTENT_BASE;
    assert(length > 0);
    assert((length % block_size) == 0);
    assert((offset0 % block_size) == 0);

    auto offset = offset0;

    bufferptr buf(length);
    for (unsigned o = 0; o < length; o++){
      auto o0 = (offset >> 12); //pblock no
      buf[o] = (o + o0) & 0xff;  //fill resulting buffer with some checksum pattern
      ++offset;
    }
    result->append(buf);
    m_reads.push_back(OffsLenList::value_type(offset0, length));
    return 0;
  }

  virtual int write_block(uint64_t offset, const bufferlist& data, void* opaque)
  {
    assert(data.length() > 0);
    assert(0u == (data.length() % get_block_size()));
    m_writes.push_back(WriteList::value_type(offset - PEXTENT_BASE, data.length(), data.crc32c(0)));
    return data.length();
  }
  virtual int zero_block(uint64_t offset, uint32_t length, void* opaque)
  {
    assert(0u == (length % get_block_size()));
    m_zeros.push_back(OffsLenList::value_type(offset - PEXTENT_BASE, length));
    return 0;
  }


  //method to allocate pextents, depending on the store state can return single or multiple pextents if there is no contiguous extent available
  virtual int allocate_blocks(uint32_t length, void* opaque, bluestore_extent_vector_t* result)
  {
    assert(length != 0);
    assert(0u == (length % get_min_alloc_size()));
    result->push_back(bluestore_extent_vector_t::value_type(m_allocNextOffset + PEXTENT_BASE, length));
    m_allocNextOffset += length;
    return 0;
  }

  virtual int release_block(uint64_t offset, uint32_t length, void* opaque)
  {
    assert(0u == (offset - PEXTENT_BASE) % get_min_alloc_size());
    assert(length != 0);
    assert(0u == (length % get_min_alloc_size()));
    m_releases.push_back(OffsLenTuple(offset - PEXTENT_BASE, length));

    return 0;
  }

  ////////////////CompressorInterface implementation////////
  virtual int compress(const ExtentManager::CompressInfo& cinfo, uint32_t source_offs, uint32_t length, const bufferlist& source, void* opaque, bufferlist* result)
  {
    m_compresses.push_back(OffsLenTuple(source_offs, length));
    if (!m_fail_compress) {
      result->substr_of(source, source_offs, length / m_cratio);
      return 0;
    }
    return -1;
  }

  virtual int decompress(const bufferlist& source, void* opaque, bufferlist* result) {
    result->append(source);
    return 0;
  }
  ////////////////CheckSumVerifyInterface implementation////////
  virtual int calculate(bluestore_blob_t::CSumType type, uint32_t csum_value_size, uint32_t csum_block_size, uint32_t source_offs, uint32_t source_len, const bufferlist& source, void* opaque, vector<char>* csum_data)
  {
    _calc_crc32(csum_value_size, csum_block_size, source_offs, source_len, source, csum_data);
    return 0;
  }
  virtual int verify(bluestore_blob_t::CSumType type, uint32_t csum_value_size, uint32_t csum_block_size, const bufferlist& source, void* opaque, const vector<char>& csum_data)
  {
    m_checks.push_back(CheckList::value_type(type, csum_block_size, csum_data, source));
    return 0;
  }
};

TEST(bluestore_extent_manager, read)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SimpleRead(false);

  mgr.read(0, 128, NULL, &res);
  ASSERT_EQ(128u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0, 4096)));
  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(1u, unsigned(res[1]));
  ASSERT_EQ(127u, unsigned(res[127]));

  mgr.reset(false);
  res.clear();

  //read 0x7000~0x1000, 0x8000~0x2000, 0xa00~0x1000(unallocated)
  mgr.read(0x7000, 0x4000, NULL, &res);
  ASSERT_EQ(0x4000u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));

  ASSERT_EQ(unsigned((0x7000 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7001 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0xfff) & 0xff), unsigned(res[0x0fff]));

  ASSERT_EQ(unsigned((0x10000>>12) & 0xff), unsigned(res[0x1000]));
  ASSERT_EQ(unsigned(((0x10001 >> 12)+1) & 0xff), unsigned(res[0x1001]));
  ASSERT_EQ(unsigned(((0x10fff >> 12)+0x0fff) & 0xff), unsigned(res[0x1fff]));
  ASSERT_EQ(unsigned(((0x11000 >> 12)+0x1000) & 0xff), unsigned(res[0x2000]));
  ASSERT_EQ(unsigned(((0x11001 >> 12)+0x1001) & 0xff), unsigned(res[0x2001]));
  ASSERT_EQ(unsigned(((0x11fff >> 12) + 0x1fff) & 0xff), unsigned(res[0x2fff]));

  ASSERT_EQ(0u, unsigned(res[0x3000]));
  ASSERT_EQ(0u, unsigned(res[0x3001]));
  ASSERT_EQ(0u, unsigned(res[0x3fff]));

  mgr.reset(false);
  res.clear();

  //read 0x7800~0x0800, 0x8000~0x0200
  mgr.read(0x7800, 0x0a00, NULL, &res);
  ASSERT_EQ(0x0a00u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x1000)));

  ASSERT_EQ(unsigned((0x7800 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7801 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x7ff) & 0xff), unsigned(res[0x07ff]));

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0800]));
  ASSERT_EQ(unsigned(((0x10801 >> 12) + 1) & 0xff), unsigned(res[0x0801]));
  ASSERT_EQ(unsigned(((0x109ff >> 12) + 0x01ff) & 0xff), unsigned(res[0x09ff]));

  mgr.reset(false);
  res.clear();

  //read 0x77f8~0x0808, 0x8000~0x0002
  mgr.read(0x77f8, 0x080a, NULL, &res);
  ASSERT_EQ(0x080au, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x1000)));

  ASSERT_EQ(unsigned(((0x77f8 >> 12) + 0x07f8) & 0xff), (unsigned char)res[0]);
  ASSERT_EQ(unsigned(((0x77f9 >> 12) + 0x07f9) & 0xff), (unsigned char)res[1]);
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x0fff) & 0xff), (unsigned char)res[0x0807]);

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0808]));
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 1) & 0xff), unsigned(res[0x0809]));

  mgr.reset(false);
  res.clear();

  //read 0x108f8~0x1808 (unallocated)
  mgr.read(0x108f8, 0x1808, NULL, &res);
  ASSERT_EQ(0x1808u, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(0u, unsigned(res[0x1807]));

  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~2
  mgr.read(0x15ffe, 0x1a614, NULL, &res);
  ASSERT_EQ(0x1a614u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x3000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x18000)));

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));


  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~29f0, 0x33000~0x1001
  mgr.read(0x15ffe, 0x1e003, NULL, &res);
  ASSERT_EQ(0x1e003u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x3000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x18000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x80000, 0x2000)));

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));
  ASSERT_EQ(0u, unsigned(res[0x1d001]));
  ASSERT_EQ(unsigned(((0x80000 >> 12) + 0) & 0xff), (unsigned char)res[0x1d002]);
  ASSERT_EQ(unsigned(((0x80001 >> 12) + 1) & 0xff), (unsigned char)res[0x1d003]);
  ASSERT_EQ(unsigned(((0x81000 >> 12) + 0x1000) & 0xff), (unsigned char)res[0x1e002]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~2
  mgr.read(0x34902u, 0x2, NULL, &res);
  ASSERT_EQ(0x2u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x1000)));

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2)& 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90403 >> 12) + 3)& 0xff), (unsigned char)res[0x1]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~0x1001
  mgr.read(0x34902u, 0x1001, NULL, &res);
  ASSERT_EQ(0x1001u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x2000)));

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90402 >> 12) + 3) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x91401 >> 12) + 0x1001) & 0xff), (unsigned char)res[0xfff]);
  ASSERT_EQ(unsigned(((0x91402 >> 12) + 0x1002) & 0xff), (unsigned char)res[0x1000]);

  mgr.reset(false);
  res.clear();

  //read 0x348fe~2, 0x34900~0x1515, 35e15~0x00ea
  mgr.read(0x348fe, 0x1601, NULL, &res);
  ASSERT_EQ(0x1601u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x81000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xc0000, 0x1000)));

  ASSERT_EQ(unsigned(((0x818fe >> 12) + 0x18fe) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x818ff >> 12) + 0x18ff) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x90400 >> 12) + 0x0) & 0xff), (unsigned char)res[0x2]);
  ASSERT_EQ(unsigned(((0x91914 >> 12) + 0x1514) & 0xff), (unsigned char)res[0x1516]);

  ASSERT_EQ(unsigned(((0xc0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1517]);
  ASSERT_EQ(unsigned(((0xc0001 >> 12) + 1) & 0xff), (unsigned char)res[0x1518]);
  ASSERT_EQ(unsigned(((0xc00e9 >> 12) + 0xe9) & 0xff), (unsigned char)res[0x1600]);

  mgr.reset(false);
  res.clear();

  //read 0x3fff0~0x10, 0x40000~0x12000 (unallocated)
  mgr.read(0x3fff0, 0x12010, NULL, &res);
  ASSERT_EQ(0x12010u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xca000, 0x1000)));

  ASSERT_EQ(unsigned(((0xca000 >> 12) + 0xa1db) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xca00f >> 12) + 0xa1ea) & 0xff), (unsigned char)res[0xf]);
  ASSERT_EQ(0u, unsigned(res[0x10]));
  ASSERT_EQ(0u, unsigned(res[0x11]));
  ASSERT_EQ(0u, unsigned(res[0x1200f]));

  mgr.reset(false);
  res.clear();

  //read 0x40700~0x0a02 (unallocated)
  mgr.read(0x40700, 0xa02, NULL, &res);
  ASSERT_EQ(0xa02u, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xa01]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_checksum)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SimpleRead(false, true);

  mgr.read(0, 128, NULL, &res);
  ASSERT_EQ(128u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0, 0x2000)));
  ASSERT_EQ(1u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(1u, unsigned(res[1]));
  ASSERT_EQ(127u, unsigned(res[127]));

  mgr.reset(false);
  res.clear();

  //read 0x7000~0x1000, 0x8000~0x2000, 0xa00~0x1000(unallocated)
  mgr.read(0x7000, 0x4000, NULL, &res);
  ASSERT_EQ(0x4000u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));

  ASSERT_EQ(2u, mgr.m_checks.size());

  ASSERT_EQ(unsigned((0x7000 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7001 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0xfff) & 0xff), unsigned(res[0x0fff]));

  ASSERT_EQ(unsigned((0x10000>>12) & 0xff), unsigned(res[0x1000]));
  ASSERT_EQ(unsigned(((0x10001 >> 12)+1) & 0xff), unsigned(res[0x1001]));
  ASSERT_EQ(unsigned(((0x10fff >> 12)+0x0fff) & 0xff), unsigned(res[0x1fff]));
  ASSERT_EQ(unsigned(((0x11000 >> 12)+0x1000) & 0xff), unsigned(res[0x2000]));
  ASSERT_EQ(unsigned(((0x11001 >> 12)+0x1001) & 0xff), unsigned(res[0x2001]));
  ASSERT_EQ(unsigned(((0x11fff >> 12) + 0x1fff) & 0xff), unsigned(res[0x2fff]));

  ASSERT_EQ(0u, unsigned(res[0x3000]));
  ASSERT_EQ(0u, unsigned(res[0x3001]));
  ASSERT_EQ(0u, unsigned(res[0x3fff]));

  mgr.reset(false);
  res.clear();

  //read 0x7800~0x0800, 0x8000~0x0200
  mgr.read(0x7800, 0x0a00, NULL, &res);
  ASSERT_EQ(0x0a00u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));

  ASSERT_EQ(2u, mgr.m_checks.size());

  ASSERT_EQ(unsigned((0x7800 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7801 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x7ff) & 0xff), unsigned(res[0x07ff]));

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0800]));
  ASSERT_EQ(unsigned(((0x10801 >> 12) + 1) & 0xff), unsigned(res[0x0801]));
  ASSERT_EQ(unsigned(((0x109ff >> 12) + 0x01ff) & 0xff), unsigned(res[0x09ff]));

  mgr.reset(false);
  res.clear();

  //read 0x77f8~0x0808, 0x8000~0x0002
  mgr.read(0x77f8, 0x080a, NULL, &res);
  ASSERT_EQ(0x080au, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));

  ASSERT_EQ(2u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0x77f8 >> 12) + 0x07f8) & 0xff), (unsigned char)res[0]);
  ASSERT_EQ(unsigned(((0x77f9 >> 12) + 0x07f9) & 0xff), (unsigned char)res[1]);
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x0fff) & 0xff), (unsigned char)res[0x0807]);

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0808]));
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 1) & 0xff), unsigned(res[0x0809]));

  mgr.reset(false);
  res.clear();

  //read 0x108f8~0x1808 (unallocated)
  mgr.read(0x108f8, 0x1808, NULL, &res);
  ASSERT_EQ(0x1808u, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(0u, unsigned(res[0x1807]));

  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~2
  mgr.read(0x15ffe, 0x1a614, NULL, &res);
  ASSERT_EQ(0x1a614u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x18000)));

  ASSERT_EQ(2u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));


  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~29f0, 0x33000~0x1001
  mgr.read(0x15ffe, 0x1e003, NULL, &res);
  ASSERT_EQ(0x1e003u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x18000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x80000, 0x2000)));

  ASSERT_EQ(3u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));
  ASSERT_EQ(0u, unsigned(res[0x1d001]));
  ASSERT_EQ(unsigned(((0x80000 >> 12) + 0) & 0xff), (unsigned char)res[0x1d002]);
  ASSERT_EQ(unsigned(((0x80001 >> 12) + 1) & 0xff), (unsigned char)res[0x1d003]);
  ASSERT_EQ(unsigned(((0x81000 >> 12) + 0x1000) & 0xff), (unsigned char)res[0x1e002]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~2
  mgr.read(0x34902u, 0x2, NULL, &res);
  ASSERT_EQ(0x2u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x2000)));
  ASSERT_EQ(1u, mgr.m_checks.size());


  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2)& 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90403 >> 12) + 3)& 0xff), (unsigned char)res[0x1]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~0x1001
  mgr.read(0x34902u, 0x1001, NULL, &res);
  ASSERT_EQ(0x1001u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x2000)));
  ASSERT_EQ(1u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90402 >> 12) + 3) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x91401 >> 12) + 0x1001) & 0xff), (unsigned char)res[0xfff]);
  ASSERT_EQ(unsigned(((0x91402 >> 12) + 0x1002) & 0xff), (unsigned char)res[0x1000]);

  mgr.reset(false);
  res.clear();

  //read 0x348fe~2, 0x34900~0x1515, 35e15~0x00ea
  mgr.read(0x348fe, 0x1601, NULL, &res);
  ASSERT_EQ(0x1601u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x80000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x90000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xc0000, 0x2000)));

  ASSERT_EQ(3u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0x818fe >> 12) + 0x18fe) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x818ff >> 12) + 0x18ff) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x90400 >> 12) + 0x0) & 0xff), (unsigned char)res[0x2]);
  ASSERT_EQ(unsigned(((0x91914 >> 12) + 0x1514) & 0xff), (unsigned char)res[0x1516]);

  ASSERT_EQ(unsigned(((0xc0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1517]);
  ASSERT_EQ(unsigned(((0xc0001 >> 12) + 1) & 0xff), (unsigned char)res[0x1518]);
  ASSERT_EQ(unsigned(((0xc00e9 >> 12) + 0xe9) & 0xff), (unsigned char)res[0x1600]);

  mgr.reset(false);
  res.clear();

  //read 0x3fff0~0x10, 0x40000~0x12000 (unallocated)
  mgr.read(0x3fff0, 0x12010, NULL, &res);
  ASSERT_EQ(0x12010u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xca000, 0x2000)));

  ASSERT_EQ(1u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0xca000 >> 12) + 0xa1db) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xca00f >> 12) + 0xa1ea) & 0xff), (unsigned char)res[0xf]);
  ASSERT_EQ(0u, unsigned(res[0x10]));
  ASSERT_EQ(0u, unsigned(res[0x11]));
  ASSERT_EQ(0u, unsigned(res[0x1200f]));

  mgr.reset(false);
  res.clear();

  //read 0x40700~0x0a02 (unallocated)
  mgr.read(0x40700, 0xa02, NULL, &res);
  ASSERT_EQ(0xa02u, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xa01]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_blob)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobRead(false);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x2000)));

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa9000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa9000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_checksum_blob)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobRead(false, true);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x2000)));

  ASSERT_EQ(1u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa8000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(4u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa8000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(4u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_checksum_blob_compressed)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobRead(true, true);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0xa000)));

  ASSERT_EQ(1u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(3u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(3u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_blob_multi_extent)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobMultiExtentRead(false);

  //0x50~0xb0 (unalloc), 0x100~0x7500
  mgr.read(0x50, 0x75b0, NULL, &res);
  ASSERT_EQ(0x75b0u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x2000)));

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0x5fff) & 0xff), (unsigned char)res[0x60af]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x60b0]);
  ASSERT_EQ(unsigned(((0xb14ff >> 12) + 0x14ff) & 0xff), (unsigned char)res[0x75af]);

  mgr.reset(false);
  res.clear();


  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(5u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb3000, 0x1000)));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0xa5fff) & 0xff), (unsigned char)res[0x5fff]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x6000]);
  ASSERT_EQ(unsigned(((0xb1fff >> 12) + 0x1fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xb3000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xb35ff >> 12) + 0x5ff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  //0x10000~0x100 (unalloc),
  //0x10100~0xcf00
  //               -> 0x30100~0x7f00 -> 0x30000~0x8000,
  //               -> 0x40000~0x5000 -> 0x40000~0x5000
  //0x1d000~0x300 (unalloc)
  //0x1d300~0x1100
  //               -> 0x45300~0xd00 -> 0x45000~0x1000
  //               -> 0x50000~0x400 -> 0x50000~0x1000
  //0x1e400~0x5700 (unalloc)
  //0x23b00~0x10000
  //               -> 0x55b00~0x6500 -> 0x55000~0x7000
  //               -> 0x60000~0x9b00 -> 0x60000~0xa000
  //0x33b00~0x3000
  //               -> 0x69b00~0x3000 -> 0x69000~0x4000
  //0x36b00~0x5600 (unalloc)

  ASSERT_EQ(0, mgr.read(0x10000, 0x2c100, NULL, &res));
  ASSERT_EQ(0x2c100u, res.length());
  ASSERT_EQ(7u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x5000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x45000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x50000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x55000, 0x7000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x60000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x69000, 0x4000)));

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xff]));

  ASSERT_EQ(unsigned(((0x30100 >> 12) + 0x100) & 0xff), (unsigned char)res[0x100]);
  ASSERT_EQ(unsigned(((0x37fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x40000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x44fff >> 12) + 0x4fff) & 0xff), (unsigned char)res[0xcfff]);
  ASSERT_EQ(0u, unsigned(res[0xd000]));
  ASSERT_EQ(0u, unsigned(res[0xd001]));
  ASSERT_EQ(0u, unsigned(res[0xd2ff]));

  ASSERT_EQ(unsigned(((0x45300 >> 12) + 0x300) & 0xff), (unsigned char)res[0xd300]);
  ASSERT_EQ(unsigned(((0x45fff >> 12) + 0xfff) & 0xff), (unsigned char)res[0xdfff]);
  ASSERT_EQ(unsigned(((0x50000 >> 12) + 0) & 0xff), (unsigned char)res[0xe000]);
  ASSERT_EQ(unsigned(((0x503ff >> 12) + 0x3ff) & 0xff), (unsigned char)res[0xe3ff]);
  ASSERT_EQ(0u, unsigned(res[0xe400]));
  ASSERT_EQ(0u, unsigned(res[0xe401]));
  ASSERT_EQ(0u, unsigned(res[0x13aff]));
  ASSERT_EQ(unsigned(((0x55b00 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x13b00]);
  ASSERT_EQ(unsigned(((0x5bfff >> 12) + 0xbfff) & 0xff), (unsigned char)res[0x19fff]);
  ASSERT_EQ(unsigned(((0x60000 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x1a000]);
  ASSERT_EQ(unsigned(((0x69aff >> 12) + 0x9aff) & 0xff), (unsigned char)res[0x23aff]);
  ASSERT_EQ(unsigned(((0x69b00 >> 12) + 0x9b00) & 0xff), (unsigned char)res[0x23b00]);
  ASSERT_EQ(unsigned(((0x6caff >> 12) + 0x2fff) & 0xff), (unsigned char)res[0x26aff]);
  ASSERT_EQ(0u, unsigned(res[0x26b00]));
  ASSERT_EQ(0u, unsigned(res[0x26b01]));
  ASSERT_EQ(0u, unsigned(res[0x2c0ff]));

  mgr.reset(false);
  res.clear();

}

TEST(bluestore_extent_manager, read_splitted_blob_multi_extent_checksum)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobMultiExtentRead(false, true);

  //0x50~0xb0 (unalloc),
  //0x100~0x7500
  //               -> 0xa0000~0x6000,
  //               -> 0xb0000~0x2000,
  mgr.read(0x50, 0x75b0, NULL, &res);
  ASSERT_EQ(0x75b0u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x2000)));

  ASSERT_EQ(2u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0x5fff) & 0xff), (unsigned char)res[0x60af]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x60b0]);
  ASSERT_EQ(unsigned(((0xb14ff >> 12) + 0x14ff) & 0xff), (unsigned char)res[0x75af]);

  mgr.reset(false);
  res.clear();


  //0x100~0x8100
  //               -> 0xa0000~0x6000,
  //               -> 0xb0000~0x2000,
  //0x8100~0x200
  //               -> 0x10000~0x8000,
  //0x8300~0x1100
  //               -> 0x20000~0x10000,
  //0x9400~0x100   unalloc
  //0x9500~0x100
  //              -> 0xb0000~0x6000 (bypassed as read before)
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(5u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb2000, 0x2000)));

  ASSERT_EQ(5u, mgr.m_checks.size());

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0xa5fff) & 0xff), (unsigned char)res[0x5fff]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x6000]);
  ASSERT_EQ(unsigned(((0xb1fff >> 12) + 0x1fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xb3000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xb35ff >> 12) + 0x5ff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  //0x10000~0x100 (unalloc),
  //0x10100~0xcf00
  //               -> 0x30100~0x7f00 -> 0x30000~0x8000,
  //               -> 0x40000~0x5000 -> 0x40000~0x6000
  //0x1d000~0x300 (unalloc)
  //0x1d300~0x1100
  //               -> 0x45300~0xd00 -> 0x44000~0x2000
  //               -> 0x50000~0x400 -> 0x50000~0x2000
  //0x1e400~0x5700 (unalloc)
  //0x23b00~0x10000
  //               -> 0x55b00~0x6500 -> 0x54000~0x8000
  //               -> 0x60000~0x9b00 -> 0x60000~0xa000
  //0x33b00~0x3000
  //               -> 0x69b00~0x3000 -> 0x68000~0x6000
  //0x36b00~0x5600 (unalloc)

  ASSERT_EQ(0, mgr.read(0x10000, 0x2c100, NULL, &res));
  ASSERT_EQ(0x2c100u, res.length());
  ASSERT_EQ(7u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x44000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x50000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x54000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x60000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x68000, 0x6000)));

  ASSERT_EQ(7u, mgr.m_checks.size());

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xff]));

  ASSERT_EQ(unsigned(((0x30100 >> 12) + 0x100) & 0xff), (unsigned char)res[0x100]);
  ASSERT_EQ(unsigned(((0x37fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x40000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x44fff >> 12) + 0x4fff) & 0xff), (unsigned char)res[0xcfff]);
  ASSERT_EQ(0u, unsigned(res[0xd000]));
  ASSERT_EQ(0u, unsigned(res[0xd001]));
  ASSERT_EQ(0u, unsigned(res[0xd2ff]));

  ASSERT_EQ(unsigned(((0x45300 >> 12) + 0x300) & 0xff), (unsigned char)res[0xd300]);
  ASSERT_EQ(unsigned(((0x45fff >> 12) + 0xfff) & 0xff), (unsigned char)res[0xdfff]);
  ASSERT_EQ(unsigned(((0x50000 >> 12) + 0) & 0xff), (unsigned char)res[0xe000]);
  ASSERT_EQ(unsigned(((0x503ff >> 12) + 0x3ff) & 0xff), (unsigned char)res[0xe3ff]);
  ASSERT_EQ(0u, unsigned(res[0xe400]));
  ASSERT_EQ(0u, unsigned(res[0xe401]));
  ASSERT_EQ(0u, unsigned(res[0x13aff]));
  ASSERT_EQ(unsigned(((0x55b00 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x13b00]);
  ASSERT_EQ(unsigned(((0x5bfff >> 12) + 0xbfff) & 0xff), (unsigned char)res[0x19fff]);
  ASSERT_EQ(unsigned(((0x60000 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x1a000]);
  ASSERT_EQ(unsigned(((0x69aff >> 12) + 0x9aff) & 0xff), (unsigned char)res[0x23aff]);
  ASSERT_EQ(unsigned(((0x69b00 >> 12) + 0x9b00) & 0xff), (unsigned char)res[0x23b00]);
  ASSERT_EQ(unsigned(((0x6caff >> 12) + 0x2fff) & 0xff), (unsigned char)res[0x26aff]);
  ASSERT_EQ(0u, unsigned(res[0x26b00]));
  ASSERT_EQ(0u, unsigned(res[0x26b01]));
  ASSERT_EQ(0u, unsigned(res[0x2c0ff]));

  mgr.reset(false);
  res.clear();

}

TEST(bluestore_extent_manager, read_splitted_blob_multi_extent_compressed)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitBlobMultiExtentRead(true);

  //0x50~0xb0 (unalloc),
  //0x100~0x7500
  //               -> 0xa0000~0x6000,
  //               -> 0xb0000~0x4000,
  mgr.read(0x50, 0x75b0, NULL, &res);
  ASSERT_EQ(0x75b0u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x4000)));

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0x5fff) & 0xff), (unsigned char)res[0x60af]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x60b0]);
  ASSERT_EQ(unsigned(((0xb14ff >> 12) + 0x14ff) & 0xff), (unsigned char)res[0x75af]);

  mgr.reset(false);
  res.clear();


  //0x100~0x8100
  //               -> 0xa0000~0x6000,
  //               -> 0xb0000~0x4000,
  //0x8100~0x200
  //               -> 0x10000~0x8000,
  //0x8300~0x1100
  //               -> 0x20000~0x10000,
  //0x9400~0x100   unalloc
  //0x9500~0x100
  //              -> 0xb0000~0x6000 (bypassed as read before)

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0xb0000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x20000, 0x2000)));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa5fff >> 12) + 0xa5fff) & 0xff), (unsigned char)res[0x5fff]);
  ASSERT_EQ(unsigned(((0xb0000 >> 12) + 0) & 0xff), (unsigned char)res[0x6000]);
  ASSERT_EQ(unsigned(((0xb1fff >> 12) + 0x1fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xb3400 >> 12) + 0x3400) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xb34ff >> 12) + 0x34ff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  //0x10000~0x100 (unalloc),
  //0x10100~0xcf00
  //               -> 0x30100~0x7f00 -> 0x30000~0x8000,
  //               -> 0x40000~0x5000 -> 0x40000~0x6000
  //0x1d000~0x300 (unalloc)
  //0x1d300~0x1100
  //               -> 0x45300~0xd00 -> 0x40000~0x6000 (duplicate)
  //               -> 0x50000~0x400 -> 0x50000~0xc000
  //0x1e400~0x5700 (unalloc)
  //0x23b00~0x10000
  //               -> 0x55b00~0x6500 -> 0x50000~0xc000 (duplicate)
  //               -> 0x60000~0x9b00 -> 0x60000~0xd000
  //0x33b00~0x3000
  //               -> 0x69b00~0x3000 -> 0x60000~0xd000 (duplicate)
  //0x36b00~0x5600 (unalloc)

  ASSERT_EQ(0, mgr.read(0x10000, 0x2c100, NULL, &res));
  ASSERT_EQ(0x2c100u, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x40000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x50000, 0xc000)));
  ASSERT_TRUE(mgr.checkRead(OffsLenTuple(0x60000, 0xd000)));

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xff]));

  ASSERT_EQ(unsigned(((0x30100 >> 12) + 0x100) & 0xff), (unsigned char)res[0x100]);
  ASSERT_EQ(unsigned(((0x37fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x40000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x44fff >> 12) + 0x4fff) & 0xff), (unsigned char)res[0xcfff]);
  ASSERT_EQ(0u, unsigned(res[0xd000]));
  ASSERT_EQ(0u, unsigned(res[0xd001]));
  ASSERT_EQ(0u, unsigned(res[0xd2ff]));

  ASSERT_EQ(unsigned(((0x45300 >> 12) + 0x300) & 0xff), (unsigned char)res[0xd300]);
  ASSERT_EQ(unsigned(((0x45fff >> 12) + 0xfff) & 0xff), (unsigned char)res[0xdfff]);
  ASSERT_EQ(unsigned(((0x50000 >> 12) + 0) & 0xff), (unsigned char)res[0xe000]);
  ASSERT_EQ(unsigned(((0x503ff >> 12) + 0x3ff) & 0xff), (unsigned char)res[0xe3ff]);
  ASSERT_EQ(0u, unsigned(res[0xe400]));
  ASSERT_EQ(0u, unsigned(res[0xe401]));
  ASSERT_EQ(0u, unsigned(res[0x13aff]));
  ASSERT_EQ(unsigned(((0x55b00 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x13b00]);
  ASSERT_EQ(unsigned(((0x5bfff >> 12) + 0xbfff) & 0xff), (unsigned char)res[0x19fff]);
  ASSERT_EQ(unsigned(((0x60000 >> 12) + 0x5b00) & 0xff), (unsigned char)res[0x1a000]);
  ASSERT_EQ(unsigned(((0x69aff >> 12) + 0x9aff) & 0xff), (unsigned char)res[0x23aff]);
  ASSERT_EQ(unsigned(((0x69b00 >> 12) + 0x9b00) & 0xff), (unsigned char)res[0x23b00]);
  ASSERT_EQ(unsigned(((0x6caff >> 12) + 0x2fff) & 0xff), (unsigned char)res[0x26aff]);
  ASSERT_EQ(0u, unsigned(res[0x26b00]));
  ASSERT_EQ(0u, unsigned(res[0x26b01]));
  ASSERT_EQ(0u, unsigned(res[0x2c0ff]));

  mgr.reset(false);
  res.clear();

}

TEST(bluestore_extent_manager, write)
{
  TestExtentManager mgr, backup_mgr;
  bufferlist bl, tmpbl;
  mgr.reset(true);

  ExtentManager::CheckSumInfo check_info;
  int r;
  uint64_t offset = 0u;
  uint64_t prev_alloc_offset = 0;
  uint64_t some_alloc_offset0 = 0, some_alloc_offset = 0, some_alloc_offset2 = 0;

  //Append get_block_size()-6 bytes  at offset 6
  offset = 6u;
  mgr.prepareWriteData(offset, mgr.get_block_size()-6, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(0u, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ( ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset);

  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, bl.length(), 0) == mgr.lextents().at(6));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE( bluestore_extent_t( 0 + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 64K+8K data
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() + mgr.get_block_size()*2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(mgr.get_min_alloc_size(), bl.length(), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(2u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6u));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, bl.length(), 0) == mgr.lextents().at(0x1000));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 1);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(mgr.get_min_alloc_size() + PEXTENT_BASE, 2 * mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 1 byte
  mgr.prepareWriteData(offset, 1, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(3 * mgr.get_min_alloc_size(), ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(3u, mgr.lextents().size());
  ASSERT_EQ(3u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, bl.length(), 0) == mgr.lextents().at(0x1000 + 0x12000));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 2);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(3 * mgr.get_min_alloc_size() + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 128K + 2 bytes
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size()*2 + 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(4 * mgr.get_min_alloc_size(), ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, bl.length(), 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 3);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(4 * mgr.get_min_alloc_size() + PEXTENT_BASE, 3 * mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 1 block(4K) data at offset 0
  offset = 0u;
  mgr.prepareWriteData(offset, mgr.get_block_size(), &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, bl.length(), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(0u, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(0u, mgr.get_min_alloc_size())));

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, 0u, mgr.get_block_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 4);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE( bluestore_extent_t( prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 0.5 block(2K) data at offset 0
  offset = 0u;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 4).extents.at(0).offset;
  mgr.prepareWriteData(offset, mgr.get_block_size() / 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, bl.length() * 2, bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(5u, mgr.lextents().size());
  ASSERT_EQ(5u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 5, 0u, mgr.get_block_size() / 2, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, mgr.get_block_size() / 2, mgr.get_block_size() / 2, 0) == mgr.lextents().at(mgr.get_block_size() / 2));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 5);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 4);
    ASSERT_EQ(mgr.get_block_size(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(some_alloc_offset, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 1 block(4K) data at offset 0 - thus releasing lextents at both offset=0 & offset=block_size/2
  offset = 0u;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 4).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset2 = mgr.blobs().at(FIRST_BLOB_REF + 5).extents.at(0).offset - PEXTENT_BASE;

  mgr.prepareWriteData(offset, mgr.get_block_size(), &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(2u, mgr.m_zeros.size());
  ASSERT_EQ(2u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, bl.length(), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset2, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, mgr.get_min_alloc_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset2, mgr.get_min_alloc_size())));

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0u, mgr.get_block_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 6);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 0.5 block(2K) data at offset 0x400 - thus making a hole in the first lextent
  offset = 0x400u;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 6).extents.at(0).offset - PEXTENT_BASE;

  mgr.prepareWriteData(offset, mgr.get_block_size() / 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(6u, mgr.lextents().size());
  ASSERT_EQ(5u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0u, 0x400, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, bl.length(), 0) == mgr.lextents().at(0x400));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0xc00, 0x400, 0) == mgr.lextents().at(0xc00));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 7);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 6);
    ASSERT_EQ(mgr.get_block_size(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(2u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(some_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 0.25 block(1K) data at offset 0x800 - thus making a hole in the second lextent
  offset = 0x600u;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 7).extents.at(0).offset - PEXTENT_BASE;

  mgr.prepareWriteData(offset, mgr.get_block_size() / 4, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(8u, mgr.lextents().size());
  ASSERT_EQ(6u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0u, 0x400, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, 0x200, 0) == mgr.lextents().at(0x400));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 8, 0u, 0x400, 0) == mgr.lextents().at(0x600));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0x600u, 0x200, 0) == mgr.lextents().at(0xa00));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0xc00, 0x400, 0) == mgr.lextents().at(0xc00));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 8);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 7);
    ASSERT_EQ(mgr.get_block_size() / 2, blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(2u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(some_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //create extent manager state backup to test different cases for the same state
  backup_mgr = mgr;

  //Overwrite 4095 bytes data at offset 0x1 - thus releasing all lextents/blobs within the starting 4K block but the first one
  offset = 1;
  some_alloc_offset0 = mgr.blobs().at(FIRST_BLOB_REF + 6).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 7).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset2= mgr.blobs().at(FIRST_BLOB_REF + 8).extents.at(0).offset - PEXTENT_BASE;

  mgr.prepareWriteData(offset, mgr.get_block_size() - 1, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(2u, mgr.m_zeros.size());
  ASSERT_EQ(2u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset2, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, mgr.get_min_alloc_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset2, mgr.get_min_alloc_size())));

  ASSERT_EQ(5u, mgr.lextents().size());
  ASSERT_EQ(5u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0u, 1u, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0u, 4095, 0) == mgr.lextents().at(1));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 9);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 6);
    ASSERT_EQ(mgr.get_block_size(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(some_alloc_offset0 + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //rollback to the previous EM state
  mgr = backup_mgr;

  //Overwrite 4095 bytes data at offset 0x0 - thus releasing all lextents/blobs within the starting 4K block but the last one
  offset = 0;
  some_alloc_offset0 = mgr.blobs().at(FIRST_BLOB_REF + 6).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 7).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset2= mgr.blobs().at(FIRST_BLOB_REF + 8).extents.at(0).offset - PEXTENT_BASE;

  mgr.prepareWriteData(offset, mgr.get_block_size() - 1, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(2u, mgr.m_zeros.size());
  ASSERT_EQ(2u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset2, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, mgr.get_min_alloc_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset2, mgr.get_min_alloc_size())));

  ASSERT_EQ(5u, mgr.lextents().size());
  ASSERT_EQ(5u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0u, 4095u, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 4095u, 1u, 0) == mgr.lextents().at(4095));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 9);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 6);
    ASSERT_EQ(mgr.get_block_size(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(some_alloc_offset0 + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //rollback to the previous EM state
  mgr = backup_mgr;

  //Overwrite the content totally + append some data (0x80000 - 100 bytes total) - thus releasing all lextents/blobs
  offset = 0;

  mgr.prepareWriteData(offset, 0x80000 - 100, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(2u, mgr.m_writes.size()); //two lextents has been created due to max_blob_size limit

  ASSERT_EQ(6u, mgr.m_zeros.size());
  ASSERT_EQ(6u, mgr.m_releases.size());
  tmpbl.substr_of(bl, 0, mgr.get_max_blob_size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, mgr.get_max_blob_size(), tmpbl));
  tmpbl.substr_of(bl, mgr.get_max_blob_size(), bl.length() - mgr.get_max_blob_size());
  ASSERT_TRUE(mgr.checkWrite(
    prev_alloc_offset + mgr.get_max_blob_size(),
    ROUND_UP_TO(bl.length() - mgr.get_max_blob_size(), mgr.get_block_size()),
    tmpbl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(2u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 10, 0u, bl.length() - mgr.get_max_blob_size(), 0) == mgr.lextents().at(mgr.get_max_blob_size()));

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append some data after the end pos
  offset = 0x80100;
  mgr.prepareWriteData(offset, 0x12345, &bl);
  r = mgr.write(offset, bl, NULL, check_info, NULL);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());

  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(3u, mgr.lextents().size());
  ASSERT_EQ(3u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 10, 0u, 0x80000 - 100 - mgr.get_max_blob_size(), 0) == mgr.lextents().at(mgr.get_max_blob_size()));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 11, 0u, bl.length(), 0) == mgr.lextents().at(0x80100));

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);
}

TEST(bluestore_extent_manager, write_compressed)
{
  TestExtentManager mgr, backup_mgr;
  bufferlist bl, tmpbl;
  mgr.reset(true);

  ExtentManager::CheckSumInfo check_info;
  ExtentManager::CompressInfo zip_info; zip_info.compress_type = 1;
  int r;
  uint64_t offset = 0u;
  uint64_t prev_alloc_offset = 0;
  uint64_t some_alloc_offset = 0, some_alloc_offset2 = 0;
  uint64_t some_len = 0, some_len2 = 0;;

  //Append get_block_size()-6 bytes  at offset 6
  offset = 6u;
  mgr.prepareWriteData(offset, mgr.get_block_size() - 6, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(0u, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset);

  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, bl.length(), 0) == mgr.lextents().at(6));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 64K+8K data
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(mgr.get_min_alloc_size(), bl.length() / mgr.m_cratio, bl, 0, bl.length() / mgr.m_cratio));
  ASSERT_TRUE(mgr.checkCompress(OffsLenTuple(0u, bl.length())));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(2u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6u));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, bl.length(), 0) == mgr.lextents().at(0x1000));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 1);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(mgr.get_min_alloc_size() + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 1 byte
  mgr.prepareWriteData(offset, 1, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(2 * mgr.get_min_alloc_size(), ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(3u, mgr.lextents().size());
  ASSERT_EQ(3u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, bl.length(), 0) == mgr.lextents().at(0x1000 + 0x12000));
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 128K + 2 bytes
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() * 2 + 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  tmpbl.substr_of(bl, 0, bl.length() / mgr.m_cratio);
  tmpbl.append_zero( ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_block_size()) - bl.length() / mgr.m_cratio);
  ASSERT_TRUE(mgr.checkWrite(3 * mgr.get_min_alloc_size(), tmpbl.length(), tmpbl));
  ASSERT_TRUE(mgr.checkCompress(OffsLenTuple(0u, bl.length())));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 6, 0) == mgr.lextents().at(6));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, bl.length(), 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 3);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(3 * mgr.get_min_alloc_size() + PEXTENT_BASE, 2 * mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite 1 block(4K) data at offset 0
  offset = 0u;
  mgr.prepareWriteData(offset, mgr.get_block_size(), &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, bl.length(), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(0u, mgr.get_block_size())));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(0u, mgr.get_min_alloc_size())));

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, 0u, mgr.get_block_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 4);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Totally overwrite second extent ( 64K + 8K)with a different cratio.
  offset = mgr.get_block_size();
  mgr.m_cratio = 3;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 1).extents.at(0).offset - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF + 1).length;
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());

  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  tmpbl.substr_of(bl, 0, bl.length() / mgr.m_cratio);
  tmpbl.append_zero( ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_block_size()) - bl.length() / mgr.m_cratio);
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO( some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, mgr.get_min_alloc_size())));

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, 0u, mgr.get_block_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 5, 0u, bl.length(), 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, 2 * mgr.get_min_alloc_size() + 2, 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 5);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Totally overwrite the forth extent ( 128K + 1 ) with a different cratio.

  offset = 0x1000 + 0x12000 + 1;
  mgr.m_cratio = 3;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 3).extents.at(0).offset - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF + 3).length;
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() * 2 + 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());

  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  tmpbl.substr_of(bl, 0, bl.length() / mgr.m_cratio);
  tmpbl.append_zero( ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_block_size()) - bl.length() / mgr.m_cratio);
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO( some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, 2 * mgr.get_min_alloc_size())));

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, 0u, 0x1000, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 5, 0u, 0x12000, 0) == mgr.lextents().at(0x1000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, 1, 0) == mgr.lextents().at(0x1000 + 0x12000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 6, 0u, bl.length(), 0) == mgr.lextents().at(0x1000 + 0x12000 + 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 6);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_TRUE(bluestore_extent_t(prev_alloc_offset + PEXTENT_BASE, mgr.get_min_alloc_size()) == blob.extents.at(0));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite the content totally + append some data (0x80000 - 100 bytes total) - thus releasing all lextents/blobs
  offset = 0;
  mgr.m_cratio = 2;
  mgr.prepareWriteData(offset, 0x80000 - 100, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(2u, mgr.m_writes.size()); //two lextents has been created due to max_blob_size limit

  ASSERT_EQ(4u, mgr.m_zeros.size());
  ASSERT_EQ(4u, mgr.m_releases.size());
  ASSERT_EQ(2u, mgr.m_compresses.size());
  tmpbl.substr_of(bl, 0, mgr.get_max_blob_size() / mgr.m_cratio);
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  tmpbl.substr_of(bl, mgr.get_max_blob_size(), (bl.length() - mgr.get_max_blob_size()) / 2);
  tmpbl.append_zero( ROUND_UP_TO(tmpbl.length(), mgr.get_block_size()) - tmpbl.length());
  ASSERT_TRUE(mgr.checkWrite(
    prev_alloc_offset + mgr.get_max_blob_size() / mgr.m_cratio,
    tmpbl.length(),
    tmpbl));

  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(2u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 8, 0u, bl.length() - mgr.get_max_blob_size(), 0) == mgr.lextents().at(mgr.get_max_blob_size()));

  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 7);
    ASSERT_EQ(mgr.get_max_blob_size() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 8);
    ASSERT_EQ((bl.length() - mgr.get_max_blob_size()) / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append some data after the end pos
  offset = 0x80100;
  mgr.prepareWriteData(offset, 0x12345, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  tmpbl.substr_of(bl, 0, bl.length() / mgr.m_cratio);;
  tmpbl.append_zero( ROUND_UP_TO(tmpbl.length(), mgr.get_block_size()) - tmpbl.length());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(3u, mgr.lextents().size());
  ASSERT_EQ(3u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 8, 0u, 0x80000 - 100 - mgr.get_max_blob_size(), 0) == mgr.lextents().at(mgr.get_max_blob_size()));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0u, bl.length(), 0) == mgr.lextents().at(0x80100));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 9);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Partially overwrite ( with 0x10001 bytes of data, uncompressed ) last two extents
  offset = 0x7A000;
  mgr.m_fail_compress = true;
  mgr.prepareWriteData(offset, 0x10001, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  tmpbl = bl;
  tmpbl.append_zero( ROUND_UP_TO(tmpbl.length(), mgr.get_block_size()) - tmpbl.length());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  ASSERT_EQ(ROUND_UP_TO(tmpbl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 8, 0u, 0x3a000, 0) == mgr.lextents().at(mgr.get_max_blob_size()));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 10, 0u, bl.length(), 0) == mgr.lextents().at(0x7A000));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 9, 0x9f01, 0x12345 - 0x9f01, 0) == mgr.lextents().at(0x8a001));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 10);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }

  mgr.m_fail_compress = false;
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite with append ( with 0x30001 bytes of data). Last two extents to be removed, the preceeding one to be altered
  offset = 0x79fff;
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 10).extents.at(0).offset - PEXTENT_BASE;
  some_alloc_offset2 = mgr.blobs().at(FIRST_BLOB_REF + 9).extents.at(0).offset - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF + 10).length;
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF + 9).length;

  mgr.prepareWriteData(offset, 0x30001, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(2u, mgr.m_zeros.size());
  ASSERT_EQ(2u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  tmpbl.substr_of(bl, 0, bl.length() / mgr.m_cratio);
  tmpbl.append_zero(ROUND_UP_TO(tmpbl.length(), mgr.get_block_size()) - tmpbl.length());
  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, tmpbl.length(), tmpbl));
  ASSERT_EQ(ROUND_UP_TO(tmpbl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_min_alloc_size()))));
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset2, ROUND_UP_TO(some_len2, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset2, ROUND_UP_TO(some_len2, mgr.get_min_alloc_size()))));

  ASSERT_EQ(3u, mgr.lextents().size());
  ASSERT_EQ(3u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 7, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 8, 0u, 0x39fff, 0) == mgr.lextents().at(mgr.get_max_blob_size()));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 11, 0u, bl.length(), 0) == mgr.lextents().at(0x79fff));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 11);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(bluestore_blob_t::CSUM_NONE, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

}

TEST(bluestore_extent_manager, zero_truncate)
{
  TestExtentManager mgr, backup_mgr;
  bufferlist bl, tmpbl;
  mgr.reset(true);

  ExtentManager::CheckSumInfo check_info;
  ExtentManager::CompressInfo zip_info; zip_info.compress_type = 1;
  int r;
  uint64_t offset = 0u;
  uint64_t some_alloc_offset = 0;
  uint64_t some_len = 0, some_len2 = 0;

  //truncate on empty object
  r = mgr.truncate(0, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  r = mgr.truncate(0x12345, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  //zero on empty object
  r = mgr.zero(0, 0, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  r = mgr.zero(0, 0x1234, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  r = mgr.zero(0x1234, 0, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  r = mgr.zero(0x1234, 0x1234, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  mgr.reset(false);

  //Append get_block_size() * 2 bytes  at offset 0
  offset = 0u;
  mgr.prepareWriteData(offset, mgr.get_block_size() * 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(0u, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset);

  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, bl.length(), 0) == mgr.lextents().at(0));

  offset += bl.length();
  mgr.reset(false);

  //Make a backup
  backup_mgr = mgr;

  //Truncate to 1 block
  r = mgr.truncate(mgr.get_block_size(), NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size(), 0) == mgr.lextents().at(0));

  mgr.reset(false);

  //Truncate to zero
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).offset  - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF).length;
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).length;
  r = mgr.truncate(0, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.lextents().size());
  ASSERT_EQ(0u, mgr.blobs().size());
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len2, mgr.get_min_alloc_size()))));

  mgr.reset(false);

  //Restore from backup
  mgr = backup_mgr;

  //Zero (255 bytes) in the mid of the first extent
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).offset;
  some_len = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).length;
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF).length;
  r = mgr.zero(mgr.get_block_size() / 2, 255, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() / 2, 0) == mgr.lextents().at(0));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, mgr.get_block_size() / 2 + 255, some_len2 - mgr.get_block_size() / 2 - 255, 0) == mgr.lextents().at(mgr.get_block_size() / 2 + 255));

  //Zero the first lextent and partially the subsequent hole (2048 + 250 bytes)
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF).length;
  r = mgr.zero(0, mgr.get_block_size() / 2 + 250, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, mgr.get_block_size() / 2 + 255, some_len2 - mgr.get_block_size() / 2 - 255, 0) == mgr.lextents().at(mgr.get_block_size() / 2 + 255));

  //Restore from backup
  mgr = backup_mgr;

  //Zero the first lextent, partially the last lextent and intermediate hole (2048 + 260 bytes)
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF).length;
  r = mgr.zero(0, mgr.get_block_size() / 2 + 260, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, mgr.get_block_size() / 2 + 260, some_len2 - mgr.get_block_size() / 2 - 260, 0) == mgr.lextents().at(mgr.get_block_size() / 2 + 260));

  //Restore from backup
  mgr = backup_mgr;

  //Full zero
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).offset  - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF).length;
  some_len2 = mgr.blobs().at(FIRST_BLOB_REF).extents.at(0).length;
  r = mgr.zero(0, some_len, NULL);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0u, mgr.m_writes.size());
  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.lextents().size());
  ASSERT_EQ(0u, mgr.blobs().size());
  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len2, mgr.get_min_alloc_size()))));

}

TEST(bluestore_extent_manager, write_csum_compressed)
{
  TestExtentManager mgr, backup_mgr;
  bufferlist bl, tmpbl;
  mgr.reset(true);

  ExtentManager::CheckSumInfo check_info;
  check_info.csum_type = bluestore_blob_t::CSUM_CRC32C;
  check_info.csum_block_order = 10; //1024 bytes

  ExtentManager::CompressInfo zip_info; zip_info.compress_type = 1;
  int r;
  uint64_t offset = 0u;
  uint64_t prev_alloc_offset = 0;
  uint64_t some_alloc_offset = 0;
  uint64_t some_len = 0;

  //Append get_block_size()-10 bytes  at offset 6
  offset = 6u;
  mgr.prepareWriteData(offset, mgr.get_block_size() - 10, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(0u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(0u, ROUND_UP_TO(bl.length(), mgr.get_block_size()), bl));
  ASSERT_EQ(ROUND_UP_TO(bl.length(), mgr.get_min_alloc_size()), mgr.m_allocNextOffset);

  ASSERT_EQ(1u, mgr.lextents().size());
  ASSERT_EQ(1u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, bl.length(), 0) == mgr.lextents().at(6));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF);
    ASSERT_EQ(bl.length(), blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(check_info.csum_type, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_EQ(check_info.csum_block_order, blob.csum_block_order);
    ASSERT_EQ(4 * blob.get_csum_value_size(), blob.csum_data.size());
    ASSERT_TRUE(mgr.checkCSum(
      blob.get_csum_value_size(),
      blob.get_csum_block_size(),
      blob.csum_data,
      bl));
  }
  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Append 64K+8K data at 0x1000
  offset = mgr.get_block_size();
  mgr.prepareWriteData(offset, mgr.get_min_alloc_size() + mgr.get_block_size() * 2, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(1u, mgr.m_writes.size());
  ASSERT_EQ(0u, mgr.m_zeros.size());
  ASSERT_EQ(0u, mgr.m_releases.size());
  ASSERT_EQ(1u, mgr.m_compresses.size());
  ASSERT_TRUE(mgr.checkWrite(mgr.get_min_alloc_size(), bl.length() / mgr.m_cratio, bl, 0, bl.length() / mgr.m_cratio));
  ASSERT_TRUE(mgr.checkCompress(OffsLenTuple(0u, bl.length())));
  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(2u, mgr.lextents().size());
  ASSERT_EQ(2u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 10, 0) == mgr.lextents().at(6u));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 1, 0u, bl.length(), 0) == mgr.lextents().at(0x1000));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 1);
    ASSERT_EQ(bl.length() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(check_info.csum_type, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_EQ(check_info.csum_block_order, blob.csum_block_order);
    tmpbl.substr_of(bl, 0, blob.length);
    auto bs = blob.get_csum_block_size();
    ASSERT_EQ(
      ROUND_UP_TO(tmpbl.length(), bs) / bs * blob.get_csum_value_size(),
      blob.csum_data.size());
    ASSERT_TRUE(mgr.checkCSum(
      blob.get_csum_value_size(),
      blob.get_csum_block_size(),
      blob.csum_data,
      tmpbl));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);

  //Overwrite + append 512K+100 data at 0x1000 - 1
  some_alloc_offset = mgr.blobs().at(FIRST_BLOB_REF + 1).extents.at(0).offset - PEXTENT_BASE;
  some_len = mgr.blobs().at(FIRST_BLOB_REF + 1).length
;
  offset = mgr.get_block_size() - 1;
  mgr.prepareWriteData(offset, 0x80000 + 100, &bl);
  r = mgr.write(offset, bl, NULL, check_info, &zip_info);
  ASSERT_EQ((int)bl.length(), r);
  ASSERT_EQ(3u, mgr.m_writes.size());
  ASSERT_EQ(1u, mgr.m_zeros.size());
  ASSERT_EQ(1u, mgr.m_releases.size());
  ASSERT_EQ(2u, mgr.m_compresses.size());

  ASSERT_TRUE(mgr.checkWrite(prev_alloc_offset, mgr.get_max_blob_size() / mgr.m_cratio, bl, 0, mgr.get_max_blob_size() / mgr.m_cratio));
  ASSERT_TRUE(mgr.checkWrite(
    prev_alloc_offset + mgr.get_max_blob_size() / mgr.m_cratio,
    mgr.get_max_blob_size() / mgr.m_cratio,
    bl,
    mgr.get_max_blob_size() / mgr.m_cratio,
    mgr.get_max_blob_size() / mgr.m_cratio));
  tmpbl.substr_of(bl, mgr.get_max_blob_size() * 2, 100);
  tmpbl.append_zero(mgr.get_block_size() - tmpbl.length());
  ASSERT_TRUE(mgr.checkWrite(
    prev_alloc_offset + 2 * mgr.get_max_blob_size() / mgr.m_cratio,
    ROUND_UP_TO(100, mgr.get_block_size()),
    tmpbl));

  ASSERT_TRUE(mgr.checkZero(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_block_size()))));
  ASSERT_TRUE(mgr.checkReleases(OffsLenTuple(some_alloc_offset, ROUND_UP_TO(some_len, mgr.get_min_alloc_size()))));

  ASSERT_TRUE(mgr.checkCompress(OffsLenTuple(0u, mgr.get_max_blob_size())));
  ASSERT_TRUE(mgr.checkCompress(OffsLenTuple(mgr.get_max_blob_size(), mgr.get_max_blob_size())));

  ASSERT_EQ(ROUND_UP_TO(bl.length() / mgr.m_cratio, mgr.get_min_alloc_size()), mgr.m_allocNextOffset - prev_alloc_offset);

  ASSERT_EQ(4u, mgr.lextents().size());
  ASSERT_EQ(4u, mgr.blobs().size());
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF, 0u, mgr.get_block_size() - 10, 0) == mgr.lextents().at(6u));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 2, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(0x1000 - 1));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 3, 0u, mgr.get_max_blob_size(), 0) == mgr.lextents().at(mgr.get_max_blob_size() + 0x1000 - 1));
  ASSERT_TRUE(bluestore_lextent_t(FIRST_BLOB_REF + 4, 0u, 100, 0) == mgr.lextents().at(mgr.get_max_blob_size() * 2 + 0x1000 - 1));
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 2);
    ASSERT_EQ(mgr.get_max_blob_size() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(check_info.csum_type, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_EQ(check_info.csum_block_order, blob.csum_block_order);
    tmpbl.substr_of(bl, 0, blob.length);
    auto bs = blob.get_csum_block_size();
    ASSERT_EQ(
      ROUND_UP_TO(tmpbl.length(), bs) / bs * blob.get_csum_value_size(),
      blob.csum_data.size());
    ASSERT_TRUE(mgr.checkCSum(
      blob.get_csum_value_size(),
      blob.get_csum_block_size(),
      blob.csum_data,
      tmpbl));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 3);
    ASSERT_EQ(mgr.get_max_blob_size() / mgr.m_cratio, blob.length);
    ASSERT_EQ(bluestore_blob_t::BLOB_COMPRESSED, blob.flags);
    ASSERT_EQ(check_info.csum_type, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_EQ(check_info.csum_block_order, blob.csum_block_order);
    tmpbl.substr_of(bl, mgr.get_max_blob_size(), blob.length);
    auto bs = blob.get_csum_block_size();
    ASSERT_EQ(
      ROUND_UP_TO(tmpbl.length(), bs) / bs * blob.get_csum_value_size(),
      blob.csum_data.size());
    ASSERT_TRUE(mgr.checkCSum(
      blob.get_csum_value_size(),
      blob.get_csum_block_size(),
      blob.csum_data,
      tmpbl));
  }
  {
    const bluestore_blob_t& blob = mgr.blobs().at(FIRST_BLOB_REF + 4);
    ASSERT_EQ(100u, blob.length);
    ASSERT_EQ(0u, blob.flags);
    ASSERT_EQ(check_info.csum_type, blob.csum_type);
    ASSERT_EQ(1u, blob.num_refs);
    ASSERT_EQ(1u, blob.extents.size());
    ASSERT_EQ(check_info.csum_block_order, blob.csum_block_order);
    tmpbl.substr_of(bl, mgr.get_max_blob_size() * 2, blob.length);
    auto bs = blob.get_csum_block_size();
    ASSERT_EQ(
      ROUND_UP_TO(tmpbl.length(), bs) / bs * blob.get_csum_value_size(),
      blob.csum_data.size());
    ASSERT_TRUE(mgr.checkCSum(
      blob.get_csum_value_size(),
      blob.get_csum_block_size(),
      blob.csum_data,
      tmpbl));
  }

  offset += bl.length();
  prev_alloc_offset = mgr.m_allocNextOffset;
  mgr.reset(false);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
