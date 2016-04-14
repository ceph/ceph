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

typedef pair<uint64_t, uint32_t> ReadTuple;
typedef vector<ReadTuple> ReadList;

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

class TestExtentManager
    : public ExtentManager::DeviceInterface,
      public ExtentManager::CompressorInterface,
      public ExtentManager::CheckSumVerifyInterface,
      public ExtentManager {

  enum {
    PEXTENT_BASE = 0x12345, //just to have pextent offsets different from lextent ones
    PEXTENT_ALLOC_UNIT = 0x10000
  };

public:
  TestExtentManager() 
    : ExtentManager::DeviceInterface(),
      ExtentManager::CompressorInterface(),
      ExtentManager::CheckSumVerifyInterface(),
      ExtentManager(*this, *this, *this) {
  }
  ReadList m_reads;
  CheckList m_checks;

  bool checkRead(const ReadTuple& r) {
    return std::find(m_reads.begin(), m_reads.end(), r) != m_reads.end();
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

    m_lextents[0] = bluestore_lextent_t(0, 0, 0x8000);
    m_blobs[0] = bluestore_blob_t(0x8000, bluestore_extent_t(PEXTENT_BASE + 0x00000, 1 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x8000] = bluestore_lextent_t(1, 0, 0x2000);
    m_blobs[1] = bluestore_blob_t(0x4000, bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT), f);

    //hole at 0x0a000~0xc000

    m_lextents[0x16000] = bluestore_lextent_t(2, 0, 0x3000);
    m_blobs[2] = bluestore_blob_t(0x3000, bluestore_extent_t(PEXTENT_BASE + 0x20000, 1 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x19000] = bluestore_lextent_t(3, 0, 0x17610);
    m_blobs[3] = bluestore_blob_t(0x18000, bluestore_extent_t(PEXTENT_BASE + 0x40000, 2 * PEXTENT_ALLOC_UNIT), f);

    //hole at 0x30610~0x29f0

    m_lextents[0x33000] = bluestore_lextent_t(4, 0x0, 0x1900);
    m_blobs[4] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x80000, 1 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x34900] = bluestore_lextent_t(5, 0x400, 0x1515);
    m_blobs[5] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x90000, 3 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x35e15] = bluestore_lextent_t(6, 0x0, 0xa1eb);
    m_blobs[6] = bluestore_blob_t(0xb000, bluestore_extent_t(PEXTENT_BASE + 0xc0000, 1 * PEXTENT_ALLOC_UNIT), f);

    //hole at 0x40000~

    if(csum_enable){
      setup_csum();
    }
  }

  void prepareTestSet4SplitBlobRead(bool compress, bool csum_enable = false) {

    unsigned f = compress ? bluestore_blob_t::BLOB_COMPRESSED : 0;

    //hole at 0~100
    m_lextents[0x100] = bluestore_lextent_t(0, 0, 0x8000);
    m_blobs[0] = bluestore_blob_t(0xa000, bluestore_extent_t(PEXTENT_BASE + 0xa0000, 1 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x8100] = bluestore_lextent_t(1, 0, 0x200);
    m_blobs[1] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT), f);

    m_lextents[0x8300] = bluestore_lextent_t(2, 0, 0x1100);
    m_blobs[2] = bluestore_blob_t(0x2000, bluestore_extent_t(PEXTENT_BASE + 0x20000, 2 * PEXTENT_ALLOC_UNIT), f);

    //hole at 0x9400~0x100

    m_lextents[0x9500] = bluestore_lextent_t(0, 0x9400, 0x200);

    //hole at 0x9700~

    if(csum_enable){
      setup_csum();
    }
  }

  void prepareTestSet4SplitBlobMultiExtentRead(bool compress, bool csum_enable = false) {

    unsigned f = compress ? bluestore_blob_t::BLOB_COMPRESSED : 0;

    //hole at 0~100
    m_lextents[0x100] = bluestore_lextent_t(0, 0, 0x8000);
    m_blobs[0] = bluestore_blob_t(0xa000, f);
    m_blobs[0].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0xa0000, 0x6000));
    m_blobs[0].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0xb0000, 0x6000));

    m_lextents[0x8100] = bluestore_lextent_t(1, 0, 0x200);
    m_blobs[1] = bluestore_blob_t(0x2000, f);
    m_blobs[1].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT / 2));
    m_blobs[1].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x18000, 1 * PEXTENT_ALLOC_UNIT / 2));

    m_lextents[0x8300] = bluestore_lextent_t(2, 0, 0x1100);
    m_blobs[2] = bluestore_blob_t(0x2000, f);
    m_blobs[2].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x20000, 1 * PEXTENT_ALLOC_UNIT));

    //hole at 0x9400~0x100

    m_lextents[0x9500] = bluestore_lextent_t(0, 0x9400, 0x200);

    //hole at 0x9700~0x6600
    //hole at 0x10000~0x100

    m_lextents[0x10100] = bluestore_lextent_t(3, 0x100, 0xcf00);
    m_blobs[3] = bluestore_blob_t(0x26b00, f);
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x30000, 0x8000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x40000, 0x6000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x50000, 0xc000));
    m_blobs[3].extents.push_back(bluestore_extent_t(PEXTENT_BASE + 0x60000, 0x12000));

    //hole at 0x1d000~0x300
    m_lextents[0x1d300] = bluestore_lextent_t(3, 0xd300, 0x1100);
    //hole at 0x1e400~0x5700
    m_lextents[0x23b00] = bluestore_lextent_t(3, 0x13b00, 0x10000);
    m_lextents[0x33b00] = bluestore_lextent_t(3, 0x23b00, 0x3000);
    //hole at 36ff1~

    if(csum_enable){
      setup_csum();
    }
  }


  void reset(bool total) {
    if (total){
      m_lextents.clear();
      m_blobs.clear();
    }
    m_reads.clear();
    m_checks.clear();
  }


protected:
  ////////////////DeviceInterface implementation////////////
  virtual uint64_t get_block_size() { return 4096; }

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
    m_reads.push_back(ReadList::value_type(offset0, length));
    return 0;
  }

  ////////////////CompressorInterface implementation////////
  virtual int decompress(const bufferlist& source, void* opaque, bufferlist* result) {
    result->append(source);
    return 0;
  }
  ////////////////CheckSumVerifyInterface implementation////////
  virtual int calculate(bluestore_blob_t::CSumType, uint32_t csum_block_size, uint32_t source_offs, const bufferlist& source, void* opaque, vector<char>* csum_data)
  {
    //FIXME: to implement
    return 0;
  }
  virtual int verify(bluestore_blob_t::CSumType type, uint32_t csum_block_size, const vector<char>& csum_data, const bufferlist& source, void* opaque)
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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0, 4096)));
  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(1u, unsigned(res[1]));
  ASSERT_EQ(127u, unsigned(res[127]));

  mgr.reset(false);
  res.clear();

  //read 0x7000~0x1000, 0x8000~0x2000, 0xa00~0x1000(unallocated)
  mgr.read(0x7000, 0x4000, NULL, &res);
  ASSERT_EQ(0x4000u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x1000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x7000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x1000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x3000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x18000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x3000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x18000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x80000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x1000)));

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2)& 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90403 >> 12) + 3)& 0xff), (unsigned char)res[0x1]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~0x1001
  mgr.read(0x34902u, 0x1001, NULL, &res);
  ASSERT_EQ(0x1001u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x81000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xc0000, 0x1000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xca000, 0x1000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0, 0x2000)));
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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x6000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x18000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x18000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x80000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x2000)));
  ASSERT_EQ(1u, mgr.m_checks.size());


  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2)& 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90403 >> 12) + 3)& 0xff), (unsigned char)res[0x1]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~0x1001
  mgr.read(0x34902u, 0x1001, NULL, &res);
  ASSERT_EQ(0x1001u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x2000)));
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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x80000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x90000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xc0000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xca000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa9000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa9000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa8000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa8000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0xa000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb3000, 0x1000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x5000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x45000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x50000, 0x1000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x55000, 0x7000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x60000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x69000, 0x4000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb2000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x44000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x50000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x54000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x60000, 0xa000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x68000, 0x6000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x4000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xa0000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0xb0000, 0x4000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x10000, 0x2000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x20000, 0x2000)));

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
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x30000, 0x8000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x40000, 0x6000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x50000, 0xc000)));
  ASSERT_TRUE(mgr.checkRead(ReadTuple(0x60000, 0xd000)));

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
