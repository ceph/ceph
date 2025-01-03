// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Mirantis <akupczyk@mirantis.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <iostream>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_crypt.h"
#include <gtest/gtest.h>
#include "include/ceph_assert.h"
#define dout_subsys ceph_subsys_rgw

using namespace std;


std::unique_ptr<BlockCrypt> AES_256_CBC_create(const DoutPrefixProvider *dpp, CephContext* cct, const uint8_t* key, size_t len);

class ut_get_sink : public RGWGetObj_Filter {
  std::stringstream sink;
public:
  ut_get_sink() {}
  virtual ~ut_get_sink() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    sink << std::string_view(bl.c_str()+bl_ofs, bl_len);
    return 0;
  }
  std::string get_sink()
  {
    return sink.str();
  }
};

class ut_put_sink: public rgw::sal::DataProcessor
{
  std::stringstream sink;
public:
  int process(bufferlist&& bl, uint64_t ofs) override
  {
    sink << std::string_view(bl.c_str(),bl.length());
    return 0;
  }
  std::string get_sink()
  {
    return sink.str();
  }
};


class BlockCryptNone: public BlockCrypt {
  size_t block_size = 256;
public:
  BlockCryptNone(){};
  BlockCryptNone(size_t sz) : block_size(sz) {}
  virtual ~BlockCryptNone(){};
  size_t get_block_size() override
  {
    return block_size;
  }
  bool encrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset,
                       optional_yield y) override
  {
    output.clear();
    output.append(input.c_str(), input.length());
    return true;
  }
  bool decrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset,
                       optional_yield y) override
  {
    output.clear();
    output.append(input.c_str(), input.length());
    return true;
  }
};

TEST(TestRGWCrypto, verify_AES_256_CBC_identity)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    //make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_NE(block_size, 0u);

    for (size_t r = 97; r < 123 ; r++)
    {
      off_t begin = (r*r*r*r*r % test_range);
      begin = begin - begin % block_size;
      off_t end = begin + r*r*r*r*r*r*r % (test_range - begin);
      if (r % 3)
        end = end - end % block_size;
      off_t offset = r*r*r*r*r*r*r*r % (1000*1000*1000);
      offset = offset - offset % block_size;

      ASSERT_EQ(begin % block_size, 0u);
      ASSERT_LE(end, test_range);
      ASSERT_EQ(offset % block_size, 0u);

      bufferlist encrypted;
      ASSERT_TRUE(aes->encrypt(input, begin, end - begin, encrypted, offset, null_yield));
      bufferlist decrypted;
      ASSERT_TRUE(aes->decrypt(encrypted, 0, end - begin, decrypted, offset, null_yield));

      ASSERT_EQ(decrypted.length(), end - begin);
      ASSERT_EQ(std::string_view(input.c_str() + begin, end - begin),
                std::string_view(decrypted.c_str(), end - begin) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_2)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    //make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_NE(block_size, 0u);

    for (off_t end = 1; end < 6096 ; end+=3)
    {
      off_t begin = 0;
      off_t offset = end*end*end*end*end % (1000*1000*1000);
      offset = offset - offset % block_size;

      ASSERT_EQ(begin % block_size, 0u);
      ASSERT_LE(end, test_range);
      ASSERT_EQ(offset % block_size, 0u);

      bufferlist encrypted;
      ASSERT_TRUE(aes->encrypt(input, begin, end, encrypted, offset, null_yield));
      bufferlist decrypted;
      ASSERT_TRUE(aes->decrypt(encrypted, 0, end, decrypted, offset, null_yield));

      ASSERT_EQ(decrypted.length(), end);
      ASSERT_EQ(std::string_view(input.c_str(), end),
                std::string_view(decrypted.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_3)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    //make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_NE(block_size, 0u);
    size_t rr = 111;
    for (size_t r = 97; r < 123 ; r++)
    {
      off_t begin = 0;
      off_t end = begin + r*r*r*r*r*r*r % (test_range - begin);
      //sometimes make aligned
      if (r % 3)
        end = end - end % block_size;
      off_t offset = r*r*r*r*r*r*r*r % (1000*1000*1000);
      offset = offset - offset % block_size;

      ASSERT_EQ(begin % block_size, 0u);
      ASSERT_LE(end, test_range);
      ASSERT_EQ(offset % block_size, 0u);

      bufferlist encrypted1;
      bufferlist encrypted2;

      off_t pos = begin;
      off_t chunk;
      while (pos < end) {
        chunk = block_size + (rr/3)*(rr+17)*(rr+71)*(rr+123)*(rr+131) % 50000;
        chunk = chunk - chunk % block_size;
        if (pos + chunk > end)
          chunk = end - pos;
        bufferlist tmp;
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos, null_yield));
        encrypted1.append(tmp);
        pos += chunk;
        rr++;
      }

      pos = begin;
      while (pos < end) {
        chunk = block_size + (rr/3)*(rr+97)*(rr+151)*(rr+213)*(rr+251) % 50000;
        chunk = chunk - chunk % block_size;
        if (pos + chunk > end)
          chunk = end - pos;
        bufferlist tmp;
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos, null_yield));
        encrypted2.append(tmp);
        pos += chunk;
        rr++;
      }
      ASSERT_EQ(encrypted1.length(), end);
      ASSERT_EQ(encrypted2.length(), end);
      ASSERT_EQ(std::string_view(encrypted1.c_str(), end),
                std::string_view(encrypted2.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_size_0_15)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    //make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_NE(block_size, 0u);
    for (size_t r = 97; r < 123 ; r++)
    {
      off_t begin = 0;
      off_t end = begin + r*r*r*r*r*r*r % (16);

      off_t offset = r*r*r*r*r*r*r*r % (1000*1000*1000);
      offset = offset - offset % block_size;

      ASSERT_EQ(begin % block_size, 0u);
      ASSERT_LE(end, test_range);
      ASSERT_EQ(offset % block_size, 0u);

      bufferlist encrypted;
      bufferlist decrypted;
      ASSERT_TRUE(aes->encrypt(input, 0, end, encrypted, offset, null_yield));
      ASSERT_TRUE(aes->encrypt(encrypted, 0, end, decrypted, offset, null_yield));
      ASSERT_EQ(encrypted.length(), end);
      ASSERT_EQ(decrypted.length(), end);
      ASSERT_EQ(std::string_view(input.c_str(), end),
                std::string_view(decrypted.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_last_block)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    //make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_NE(block_size, 0u);
    size_t rr = 111;
    for (size_t r = 97; r < 123 ; r++)
    {
      off_t begin = 0;
      off_t end = r*r*r*r*r*r*r % (test_range - 16);
      end = end - end % block_size;
      end = end + (r+3)*(r+5)*(r+7) % 16;

      off_t offset = r*r*r*r*r*r*r*r % (1000*1000*1000);
      offset = offset - offset % block_size;

      ASSERT_EQ(begin % block_size, 0u);
      ASSERT_LE(end, test_range);
      ASSERT_EQ(offset % block_size, 0u);

      bufferlist encrypted1;
      bufferlist encrypted2;

      off_t pos = begin;
      off_t chunk;
      while (pos < end) {
        chunk = block_size + (rr/3)*(rr+17)*(rr+71)*(rr+123)*(rr+131) % 50000;
        chunk = chunk - chunk % block_size;
        if (pos + chunk > end)
          chunk = end - pos;
        bufferlist tmp;
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos, null_yield));
        encrypted1.append(tmp);
        pos += chunk;
        rr++;
      }
      pos = begin;
      while (pos < end) {
        chunk = block_size + (rr/3)*(rr+97)*(rr+151)*(rr+213)*(rr+251) % 50000;
        chunk = chunk - chunk % block_size;
        if (pos + chunk > end)
          chunk = end - pos;
        bufferlist tmp;
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos, null_yield));
        encrypted2.append(tmp);
        pos += chunk;
        rr++;
      }
      ASSERT_EQ(encrypted1.length(), end);
      ASSERT_EQ(encrypted2.length(), end);
      ASSERT_EQ(std::string_view(encrypted1.c_str(), end),
                std::string_view(encrypted2.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_RGWGetObj_BlockDecrypt_ranges)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  bufferptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i] = i;

  auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
  ASSERT_NE(cbc.get(), nullptr);
  bufferlist encrypted;
  ASSERT_TRUE(cbc->encrypt(input, 0, test_range, encrypted, 0, null_yield));


  for (off_t r = 93; r < 150; r++ )
  {
    ut_get_sink get_sink;
    auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink, std::move(cbc), {}, null_yield);

    //random ranges
    off_t begin = (r/3)*r*(r+13)*(r+23)*(r+53)*(r+71) % test_range;
    off_t end = begin + (r/5)*(r+7)*(r+13)*(r+101)*(r*103) % (test_range - begin) - 1;

    off_t f_begin = begin;
    off_t f_end = end;
    decrypt.fixup_range(f_begin, f_end);
    decrypt.handle_data(encrypted, f_begin, f_end - f_begin + 1);
    decrypt.flush();
    const std::string& decrypted = get_sink.get_sink();
    size_t expected_len = end - begin + 1;
    ASSERT_EQ(decrypted.length(), expected_len);
    ASSERT_EQ(decrypted, std::string_view(input.c_str()+begin, expected_len));
  }
}


TEST(TestRGWCrypto, verify_RGWGetObj_BlockDecrypt_chunks)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  bufferptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i] = i;

  auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
  ASSERT_NE(cbc.get(), nullptr);
  bufferlist encrypted;
  ASSERT_TRUE(cbc->encrypt(input, 0, test_range, encrypted, 0, null_yield));

  for (off_t r = 93; r < 150; r++ )
  {
    ut_get_sink get_sink;
    auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink, std::move(cbc), {}, null_yield);

    //random
    off_t begin = (r/3)*r*(r+13)*(r+23)*(r+53)*(r+71) % test_range;
    off_t end = begin + (r/5)*(r+7)*(r+13)*(r+101)*(r*103) % (test_range - begin) - 1;

    off_t f_begin = begin;
    off_t f_end = end;
    decrypt.fixup_range(f_begin, f_end);
    off_t pos = f_begin;
    do
    {
      off_t size = 2 << ((pos * 17 + pos / 113 + r) % 16);
      size = (pos + 1117) * (pos + 2229) % size + 1;
      if (pos + size > f_end + 1)
        size = f_end + 1 - pos;

      decrypt.handle_data(encrypted, pos, size);
      pos = pos + size;
    } while (pos < f_end + 1);
    decrypt.flush();

    const std::string& decrypted = get_sink.get_sink();
    size_t expected_len = end - begin + 1;
    ASSERT_EQ(decrypted.length(), expected_len);
    ASSERT_EQ(decrypted, std::string_view(input.c_str()+begin, expected_len));
  }
}


using range_t = std::pair<off_t, off_t>;

// call filter->fixup_range() and return the range as a pair. this makes it easy
// to fit on a single line for ASSERT_EQ()
range_t fixup_range(RGWGetObj_BlockDecrypt *decrypt, off_t ofs, off_t end)
{
  decrypt->fixup_range(ofs, end);
  return {ofs, end};
}

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;
  auto nonecrypt = std::unique_ptr<BlockCrypt>(new BlockCryptNone);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                                 std::move(nonecrypt), {}, null_yield);
  ASSERT_EQ(fixup_range(&decrypt,0,0),     range_t(0,255));
  ASSERT_EQ(fixup_range(&decrypt,1,256),   range_t(0,511));
  ASSERT_EQ(fixup_range(&decrypt,0,255),   range_t(0,255));
  ASSERT_EQ(fixup_range(&decrypt,255,256), range_t(0,511));
  ASSERT_EQ(fixup_range(&decrypt,511,1023), range_t(256,1023));
  ASSERT_EQ(fixup_range(&decrypt,513,1024), range_t(512,1024+255));
}

std::vector<size_t> create_mp_parts(size_t obj_size, size_t mp_part_len){
  std::vector<size_t> parts_len;
  size_t part_size;
  size_t ofs=0;

  while (ofs < obj_size){
    part_size = std::min(mp_part_len, (obj_size - ofs));
    ofs += part_size;
    parts_len.push_back(part_size);
  }
  return parts_len;
}

const size_t part_size = 5*1024*1024;
const size_t obj_size = 30*1024*1024;

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup_simple)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  ut_get_sink get_sink;
  auto nonecrypt = std::make_unique<BlockCryptNone>(4096);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
				 std::move(nonecrypt),
				 create_mp_parts(obj_size, part_size),
				 null_yield);
  ASSERT_EQ(fixup_range(&decrypt,0,0),     range_t(0,4095));
  ASSERT_EQ(fixup_range(&decrypt,1,4096),   range_t(0,8191));
  ASSERT_EQ(fixup_range(&decrypt,0,4095),   range_t(0,4095));
  ASSERT_EQ(fixup_range(&decrypt,4095,4096), range_t(0,8191));

  // ranges are end-end inclusive, we request bytes just spanning short of first
  // part to exceeding the first part, part_size - 1 is aligned to a 4095 boundary
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size - 2), range_t(0, part_size -1));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size - 1), range_t(0, part_size -1));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size),     range_t(0, part_size + 4095));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size + 1), range_t(0, part_size + 4095));

  // request bytes spanning 2 parts
  ASSERT_EQ(fixup_range(&decrypt, part_size -2, part_size + 2),
	    range_t(part_size - 4096, part_size + 4095));

  // request last byte
  ASSERT_EQ(fixup_range(&decrypt, obj_size - 1, obj_size -1),
	    range_t(obj_size - 4096, obj_size -1));

}

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup_non_aligned_obj_size)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  const size_t na_obj_size = obj_size + 1;

  ut_get_sink get_sink;
  auto nonecrypt = std::make_unique<BlockCryptNone>(4096);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
				 std::move(nonecrypt),
				 create_mp_parts(na_obj_size, part_size),
				 null_yield);

  // these should be unaffected here
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size - 2), range_t(0, part_size -1));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size - 1), range_t(0, part_size -1));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size),     range_t(0, part_size + 4095));
  ASSERT_EQ(fixup_range(&decrypt, 0, part_size + 1), range_t(0, part_size + 4095));


  // request last 2 bytes; spanning 2 parts
  ASSERT_EQ(fixup_range(&decrypt, na_obj_size -2 , na_obj_size -1),
	    range_t(na_obj_size - 1 - 4096, na_obj_size - 1));

  // request last byte, spans last 1B part only
  ASSERT_EQ(fixup_range(&decrypt, na_obj_size -1, na_obj_size - 1),
	    range_t(na_obj_size - 1, na_obj_size -1));

}

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup_non_aligned_part_size)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  const size_t na_part_size = part_size + 1;

  ut_get_sink get_sink;
  auto nonecrypt = std::make_unique<BlockCryptNone>(4096);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
				 std::move(nonecrypt),
				 create_mp_parts(obj_size, na_part_size),
				 null_yield);

  // na_part_size -2, ie. part_size -1  is aligned to 4095 boundary
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size - 2), range_t(0, na_part_size -2));
  // even though na_part_size -1 should not align to a 4095 boundary, the range
  // should not span the next part
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size - 1), range_t(0, na_part_size -1));

  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size),     range_t(0, na_part_size + 4095));
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size + 1), range_t(0, na_part_size + 4095));

  // request spanning 2 parts
  ASSERT_EQ(fixup_range(&decrypt, na_part_size - 2, na_part_size + 2),
	    range_t(na_part_size - 1 - 4096, na_part_size + 4095));

  // request last byte, this will be interesting, since this a multipart upload
  // with 5MB+1 size, the last part is actually 5 bytes short of 5 MB, which
  // should be considered for the ranges alignment; an easier way to look at
  // this will be that the last offset aligned to a 5MiB part will be 5MiB -
  // 4095, this is a part that is 5MiB - 5 B
  ASSERT_EQ(fixup_range(&decrypt, obj_size - 1, obj_size -1),
	    range_t(obj_size +5 -4096, obj_size -1));

}

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup_non_aligned)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  const size_t na_part_size = part_size + 1;
  const size_t na_obj_size = obj_size + 7; // (6*(5MiB + 1) + 1) for the last 1B overflow

  ut_get_sink get_sink;
  auto nonecrypt = std::make_unique<BlockCryptNone>(4096);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
				 std::move(nonecrypt),
				 create_mp_parts(na_obj_size, na_part_size),
				 null_yield);

  // na_part_size -2, ie. part_size -1  is aligned to 4095 boundary
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size - 2), range_t(0, na_part_size -2));
  // even though na_part_size -1 should not align to a 4095 boundary, the range
  // should not span the next part
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size - 1), range_t(0, na_part_size -1));

  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size),     range_t(0, na_part_size + 4095));
  ASSERT_EQ(fixup_range(&decrypt, 0, na_part_size + 1), range_t(0, na_part_size + 4095));

  // request last byte, spans last 1B part only
  ASSERT_EQ(fixup_range(&decrypt, na_obj_size -1, na_obj_size - 1),
	    range_t(na_obj_size - 1, na_obj_size -1));

  ASSERT_EQ(fixup_range(&decrypt, na_obj_size -2, na_obj_size -1),
	    range_t(na_obj_size - 2, na_obj_size -1));

}

TEST(TestRGWCrypto, check_RGWGetObj_BlockDecrypt_fixup_invalid_ranges)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  ut_get_sink get_sink;
  auto nonecrypt = std::make_unique<BlockCryptNone>(4096);
  RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
				 std::move(nonecrypt),
				 create_mp_parts(obj_size, part_size),
				 null_yield);


  // the ranges below would be mostly unreachable in current code as rgw
  // would've returned a 411 before reaching, but we're just doing this to make
  // sure we don't have invalid access
  ASSERT_EQ(fixup_range(&decrypt, obj_size - 1, obj_size + 100),
            range_t(obj_size - 4096, obj_size - 1));
  ASSERT_EQ(fixup_range(&decrypt, obj_size, obj_size + 1),
            range_t(obj_size - 1, obj_size - 1));
  ASSERT_EQ(fixup_range(&decrypt, obj_size+1, obj_size + 100),
            range_t(obj_size - 1, obj_size - 1));

}

TEST(TestRGWCrypto, verify_RGWPutObj_BlockEncrypt_chunks)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  //create some input for encryption
  const off_t test_range = 1024*1024;
  bufferptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i] = i;

  for (off_t r = 93; r < 150; r++ )
  {
    ut_put_sink put_sink;
    auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWPutObj_BlockEncrypt encrypt(&no_dpp, g_ceph_context, &put_sink,
                                   std::move(cbc), null_yield);

    off_t test_size = (r/5)*(r+7)*(r+13)*(r+101)*(r*103) % (test_range - 1) + 1;
    off_t pos = 0;
    do
    {
      off_t size = 2 << ((pos * 17 + pos / 113 + r) % 16);
      size = (pos + 1117) * (pos + 2229) % size + 1;
      if (pos + size > test_size)
        size = test_size - pos;

      bufferlist bl;
      bl.append(input.c_str()+pos, size);
      encrypt.process(std::move(bl), pos);

      pos = pos + size;
    } while (pos < test_size);
    encrypt.process({}, pos);

    ASSERT_EQ(put_sink.get_sink().length(), static_cast<size_t>(test_size));

    cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);

    bufferlist encrypted;
    bufferlist decrypted;
    encrypted.append(put_sink.get_sink());
    ASSERT_TRUE(cbc->decrypt(encrypted, 0, test_size, decrypted, 0, null_yield));

    ASSERT_EQ(decrypted.length(), test_size);
    ASSERT_EQ(std::string_view(decrypted.c_str(), test_size),
              std::string_view(input.c_str(), test_size));
  }
}


TEST(TestRGWCrypto, verify_Encrypt_Decrypt)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i]=i;

  size_t fi_a = 0;
  size_t fi_b = 1;
  size_t test_size;
  do
  {
    //fibonacci
    size_t tmp = fi_b;
    fi_b = fi_a + fi_b;
    fi_a = tmp;

    test_size = fi_b;

    uint8_t* test_in = new uint8_t[test_size];
    //fill with something
    memset(test_in, test_size & 0xff, test_size);

    ut_put_sink put_sink;
    RGWPutObj_BlockEncrypt encrypt(&no_dpp, g_ceph_context, &put_sink,
				   AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32),
                                   null_yield);
    bufferlist bl;
    bl.append((char*)test_in, test_size);
    encrypt.process(std::move(bl), 0);
    encrypt.process({}, test_size);
    ASSERT_EQ(put_sink.get_sink().length(), test_size);

    bl.append(put_sink.get_sink().data(), put_sink.get_sink().length());
    ASSERT_EQ(bl.length(), test_size);

    ut_get_sink get_sink;
    RGWGetObj_BlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                                   AES_256_CBC_create(&no_dpp, g_ceph_context, &key[0], 32),
                                   {}, null_yield);

    off_t bl_ofs = 0;
    off_t bl_end = test_size - 1;
    decrypt.fixup_range(bl_ofs, bl_end);
    decrypt.handle_data(bl, 0, bl.length());
    decrypt.flush();
    ASSERT_EQ(get_sink.get_sink().length(), test_size);
    ASSERT_EQ(get_sink.get_sink(), std::string_view((char*)test_in,test_size));
    delete[] test_in;
  }
  while (test_size < 20000);
}


int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

