// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
bool AES_256_GCM_derive_object_key(BlockCrypt* block_crypt,
                                    const uint8_t* user_key,
                                    size_t key_len,
                                    const std::string& bucket_id,
                                    const std::string& object,
                                    uint32_t part_number,
                                    const std::string& domain = "SSE-C-GCM");

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

// Mock that simulates AEAD size expansion (encrypted_block_size > block_size)
class BlockCryptNoneAEAD: public BlockCryptNone {
public:
  BlockCryptNoneAEAD() : BlockCryptNone(AEAD_CHUNK_SIZE) {}
  size_t get_encrypted_block_size() override
  {
    return AEAD_ENCRYPTED_CHUNK_SIZE;  // 4096 + 16 = 4112
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


// AEAD Tests

TEST(TestRGWCrypto, verify_AES_256_GCM_identity)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  // Create some input for encryption
  const off_t test_range = 1024*1024;
  buffer::ptr buf(test_range);
  char* p = buf.c_str();
  for(size_t i = 0; i < buf.length(); i++)
    p[i] = i + i*i + (i >> 2);

  bufferlist input;
  input.append(buf);

  for (unsigned int step : {1, 2, 3, 5, 7, 11, 13, 17})
  {
    // Make some random key
    uint8_t key[32];
    for(size_t i=0;i<sizeof(key);i++)
      key[i]=i*step;

    auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    size_t block_size = aes->get_block_size();
    ASSERT_EQ(block_size, 4096u);

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

      size_t expected_encrypted_size = aead_plaintext_to_encrypted_size(end - begin);
      ASSERT_EQ(encrypted.length(), expected_encrypted_size);

      bufferlist decrypted;
      ASSERT_TRUE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted, offset, null_yield));

      ASSERT_EQ(decrypted.length(), end - begin);
      ASSERT_EQ(std::string_view(input.c_str() + begin, end - begin),
                std::string_view(decrypted.c_str(), end - begin) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_GCM_chunk_sizes)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i]=i*3;

  auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes.get(), nullptr);

  // Test various sizes to verify chunk + tag handling
  for (size_t size : {0, 1, 16, 4095, 4096, 4097, 8192, 10000})
  {
    buffer::ptr buf(size);
    char* p = buf.c_str();
    for(size_t i = 0; i < size; i++)
      p[i] = i & 0xFF;

    bufferlist input;
    input.append(buf);

    bufferlist encrypted;
    ASSERT_TRUE(aes->encrypt(input, 0, size, encrypted, 0, null_yield));

    size_t expected_size = aead_plaintext_to_encrypted_size(size);
    ASSERT_EQ(encrypted.length(), expected_size);

    bufferlist decrypted;
    ASSERT_TRUE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));
    ASSERT_EQ(decrypted.length(), size);

    if (size > 0) {
      ASSERT_EQ(std::string_view(input.c_str(), size),
                std::string_view(decrypted.c_str(), size));
    }
  }
}


TEST(TestRGWCrypto, verify_AEAD_size_conversion)
{
  // Test is_aead_mode() - AEAD modes have ciphertext expansion
  ASSERT_TRUE(is_aead_mode("SSE-C-AES256-GCM"));
  ASSERT_TRUE(is_aead_mode("SSE-KMS-GCM"));
  ASSERT_TRUE(is_aead_mode("RGW-AUTO-GCM"));
  ASSERT_TRUE(is_aead_mode("AES256-GCM"));
  ASSERT_FALSE(is_aead_mode("SSE-C-AES256"));
  ASSERT_FALSE(is_aead_mode("SSE-KMS"));
  ASSERT_FALSE(is_aead_mode(""));
  ASSERT_FALSE(is_aead_mode("GCM"));

  // Test is_cbc_mode() - exact match for known CBC modes
  ASSERT_TRUE(is_cbc_mode("SSE-C-AES256"));
  ASSERT_TRUE(is_cbc_mode("SSE-KMS"));
  ASSERT_TRUE(is_cbc_mode("RGW-AUTO"));
  ASSERT_TRUE(is_cbc_mode("AES256"));
  ASSERT_FALSE(is_cbc_mode("SSE-C-AES256-GCM"));
  ASSERT_FALSE(is_cbc_mode("SSE-KMS-GCM"));
  ASSERT_FALSE(is_cbc_mode(""));
  ASSERT_FALSE(is_cbc_mode("invalid"));

  // Test aead_plaintext_to_encrypted_size()
  ASSERT_EQ(aead_plaintext_to_encrypted_size(0), 0UL);       // Zero bytes
  ASSERT_EQ(aead_plaintext_to_encrypted_size(1), 17UL);      // 1 + 16 tag
  ASSERT_EQ(aead_plaintext_to_encrypted_size(100), 116UL);   // 100 + 16 tag
  ASSERT_EQ(aead_plaintext_to_encrypted_size(4095), 4111UL); // 4095 + 16 tag
  ASSERT_EQ(aead_plaintext_to_encrypted_size(4096), 4112UL); // Full chunk
  ASSERT_EQ(aead_plaintext_to_encrypted_size(4097), 4129UL); // 4097 + 32 tags (2 chunks)
  ASSERT_EQ(aead_plaintext_to_encrypted_size(8192), 8224UL); // Two full chunks

  // Test aead_encrypted_to_plaintext_size()
  ASSERT_EQ(aead_encrypted_to_plaintext_size(0), 0UL);       // Zero bytes
  ASSERT_EQ(aead_encrypted_to_plaintext_size(16), 0UL);      // Tag only -> 0 plaintext
  ASSERT_EQ(aead_encrypted_to_plaintext_size(17), 1UL);      // 1 byte + tag
  ASSERT_EQ(aead_encrypted_to_plaintext_size(100), 84UL);    // 100 - 16 tag
  ASSERT_EQ(aead_encrypted_to_plaintext_size(4112), 4096UL); // Full chunk
  ASSERT_EQ(aead_encrypted_to_plaintext_size(4212), 4180UL); // 4096 + 84 (one chunk + partial)
  ASSERT_EQ(aead_encrypted_to_plaintext_size(8224), 8192UL); // Two full chunks

  // Test roundtrip: plaintext -> encrypted -> plaintext
  for (uint64_t plain : {0UL, 1UL, 16UL, 100UL, 4095UL, 4096UL, 4097UL,
                         8192UL, 10000UL, 1000000UL}) {
    uint64_t enc = aead_plaintext_to_encrypted_size(plain);
    uint64_t recovered = aead_encrypted_to_plaintext_size(enc);
    ASSERT_EQ(plain, recovered)
      << "Roundtrip failed for plaintext=" << plain
      << " encrypted=" << enc << " recovered=" << recovered;
  }

  // Test aead_plaintext_to_encrypted_offset()
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(0), 0UL);       // Start of file
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(100), 100UL);   // Within first chunk
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(4095), 4095UL); // End of first chunk
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(4096), 4112UL); // Start of second chunk
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(4196), 4212UL); // 100 bytes into second chunk
  ASSERT_EQ(aead_plaintext_to_encrypted_offset(8192), 8224UL); // Start of third chunk

  // Test has_size_expansion() via RGWGetObj_BlockDecrypt
  // GCM expands data (adds 16-byte auth tag per chunk), CBC does not
  {
    const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
    ut_get_sink get_sink;

    auto cbc = std::make_unique<BlockCryptNone>(4096);
    RGWGetObj_BlockDecrypt decrypt_cbc(&no_dpp, g_ceph_context, &get_sink,
                                       std::move(cbc), {}, null_yield);
    ASSERT_FALSE(decrypt_cbc.has_size_expansion());

    auto gcm = std::make_unique<BlockCryptNoneAEAD>();
    RGWGetObj_BlockDecrypt decrypt_gcm(&no_dpp, g_ceph_context, &get_sink,
                                       std::move(gcm), {}, null_yield);
    ASSERT_TRUE(decrypt_gcm.has_size_expansion());
  }
}


TEST(TestRGWCrypto, verify_AES_256_GCM_tag_verification)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  uint8_t key[32];
  for(size_t i=0;i<sizeof(key);i++)
    key[i]=i*7;

  auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes.get(), nullptr);

  const size_t size = 8192;  // 2 chunks
  buffer::ptr buf(size);
  char* p = buf.c_str();
  for(size_t i = 0; i < size; i++)
    p[i] = i & 0xFF;

  bufferlist input;
  input.append(buf);

  bufferlist encrypted;
  ASSERT_TRUE(aes->encrypt(input, 0, size, encrypted, 0, null_yield));

  // Corrupt the ciphertext
  char* enc_p = encrypted.c_str();
  enc_p[100] ^= 0xFF;

  bufferlist decrypted;
  // Decryption should fail due to tag mismatch
  ASSERT_FALSE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));
}


TEST(TestRGWCrypto, verify_AES_256_GCM_salt_key_isolation)
{
  /**
   * Verify salt-based key derivation produces unique keys:
   *   1. Two instances with same raw key but different salts
   *   2. After derive_object_key(), they have different derived keys
   *   3. Cross-decryption fails (GCM tag mismatch)
   *   4. Self-decryption succeeds
   */
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  uint8_t key[32];
  for (size_t i = 0; i < sizeof(key); i++)
    key[i] = i * 11;

  const size_t size = 4096;
  buffer::ptr buf(size);
  char* p = buf.c_str();
  for (size_t i = 0; i < size; i++)
    p[i] = i & 0xFF;

  bufferlist input;
  input.append(buf);

  // Instance 1: auto-generated salt + derive
  auto aes1(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes1.get(), nullptr);
  ASSERT_TRUE(AES_256_GCM_derive_object_key(aes1.get(), key, 32,
              "bucket-id-1", "object.txt", 0));

  bufferlist encrypted1;
  ASSERT_TRUE(aes1->encrypt(input, 0, size, encrypted1, 0, null_yield));

  // Instance 2: different auto-generated salt + derive with same identity
  auto aes2(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes2.get(), nullptr);
  ASSERT_TRUE(AES_256_GCM_derive_object_key(aes2.get(), key, 32,
              "bucket-id-1", "object.txt", 0));

  // Cross-decrypt should FAIL (different salts -> different derived keys)
  bufferlist decrypted_wrong;
  ASSERT_FALSE(aes2->decrypt(encrypted1, 0, encrypted1.length(),
                             decrypted_wrong, 0, null_yield));

  // Self-decrypt should succeed
  bufferlist decrypted_correct;
  ASSERT_TRUE(aes1->decrypt(encrypted1, 0, encrypted1.length(),
                            decrypted_correct, 0, null_yield));
  ASSERT_EQ(decrypted_correct.length(), size);
  ASSERT_EQ(std::string_view(input.c_str(), size),
            std::string_view(decrypted_correct.c_str(), size));
}


TEST(TestRGWCrypto, verify_AES_256_GCM_salt_restore)
{
  /**
   * Simulate the full encrypt/decrypt flow with stored salt:
   *   1. Encrypt with auto-generated salt + derive_object_key
   *   2. Extract salt (would be stored in RGW_ATTR_CRYPT_SALT)
   *   3. Create new instance with restored salt + derive_object_key
   *   4. Decrypt successfully
   */
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  uint8_t key[32];
  for (size_t i = 0; i < sizeof(key); i++)
    key[i] = i * 13;

  const size_t size = 8192;
  buffer::ptr buf(size);
  char* p = buf.c_str();
  for (size_t i = 0; i < size; i++)
    p[i] = (i * 7) & 0xFF;

  bufferlist input;
  input.append(buf);

  // Simulate encryption path
  auto aes_encrypt(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes_encrypt.get(), nullptr);
  ASSERT_TRUE(AES_256_GCM_derive_object_key(aes_encrypt.get(), key, 32,
              "my-bucket-id", "my-object", 0));

  bufferlist encrypted;
  ASSERT_TRUE(aes_encrypt->encrypt(input, 0, size, encrypted, 0, null_yield));

  // Extract salt (simulates storing to RGW_ATTR_CRYPT_SALT)
  std::string stored_salt = AES_256_GCM_get_salt(aes_encrypt.get());
  ASSERT_EQ(stored_salt.size(), AES_256_GCM_SALT_SIZE);

  // Release encryption instance (simulates different request)
  aes_encrypt.reset();

  // Simulate decryption path with restored salt + same identity
  auto aes_decrypt(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32,
                                       reinterpret_cast<const uint8_t*>(stored_salt.c_str()),
                                       stored_salt.size()));
  ASSERT_NE(aes_decrypt.get(), nullptr);
  ASSERT_TRUE(AES_256_GCM_derive_object_key(aes_decrypt.get(), key, 32,
              "my-bucket-id", "my-object", 0));

  bufferlist decrypted;
  ASSERT_TRUE(aes_decrypt->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));

  ASSERT_EQ(decrypted.length(), size);
  ASSERT_EQ(std::string_view(input.c_str(), size),
            std::string_view(decrypted.c_str(), size));
}


TEST(TestRGWCrypto, verify_AES_256_GCM_key_derivation)
{
  NoDoutPrefix no_dpp(g_ceph_context, ceph_subsys_rgw);
  uint8_t user_key[32];
  for (size_t i = 0; i < sizeof(user_key); i++) user_key[i] = i;
  const std::string plaintext = "test data for key derivation";

  // Test 1: Same identity produces same derived key (encrypt/decrypt roundtrip)
  {
    auto aes1(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32));
    ASSERT_NE(aes1.get(), nullptr);
    std::string salt = AES_256_GCM_get_salt(aes1.get());
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes1.get(), user_key, 32,
                                               "mybucket", "myobject", 0));

    bufferlist input;
    input.append(plaintext);
    bufferlist encrypted;
    ASSERT_TRUE(aes1->encrypt(input, 0, input.length(), encrypted, 0, null_yield));

    auto aes2(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32,
                                  reinterpret_cast<const uint8_t*>(salt.c_str()),
                                  salt.size()));
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes2.get(), user_key, 32,
                                               "mybucket", "myobject", 0));

    bufferlist decrypted;
    ASSERT_TRUE(aes2->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));
    ASSERT_EQ(std::string_view(input.c_str(), input.length()),
              std::string_view(decrypted.c_str(), decrypted.length()));
  }

  // Test 2: Different bucket/object produces different derived key
  {
    auto aes1(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32));
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes1.get(), user_key, 32,
                                               "bucket1", "object1", 0));
    std::string salt = AES_256_GCM_get_salt(aes1.get());

    auto aes2(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32,
                                  reinterpret_cast<const uint8_t*>(salt.c_str()),
                                  salt.size()));
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes2.get(), user_key, 32,
                                               "bucket2", "object2", 0));

    bufferlist input;
    input.append(plaintext);
    bufferlist enc1, enc2;
    ASSERT_TRUE(aes1->encrypt(input, 0, input.length(), enc1, 0, null_yield));
    ASSERT_TRUE(aes2->encrypt(input, 0, input.length(), enc2, 0, null_yield));
    ASSERT_NE(std::string_view(enc1.c_str(), enc1.length()),
              std::string_view(enc2.c_str(), enc2.length()));
  }

  // Test 3: Different part numbers produce different derived keys
  {
    auto aes1(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32));
    std::string salt = AES_256_GCM_get_salt(aes1.get());
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes1.get(), user_key, 32,
                                               "bucket", "object", 1));

    auto aes2(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32,
                                  reinterpret_cast<const uint8_t*>(salt.c_str()),
                                  salt.size()));
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes2.get(), user_key, 32,
                                               "bucket", "object", 2));

    bufferlist input;
    input.append(plaintext);
    bufferlist enc1, enc2;
    ASSERT_TRUE(aes1->encrypt(input, 0, input.length(), enc1, 0, null_yield));
    ASSERT_TRUE(aes2->encrypt(input, 0, input.length(), enc2, 0, null_yield));
    ASSERT_NE(std::string_view(enc1.c_str(), enc1.length()),
              std::string_view(enc2.c_str(), enc2.length()));
  }

  // Test 4: Wrong identity fails decryption (auth tag mismatch)
  {
    auto aes_enc(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32));
    std::string salt = AES_256_GCM_get_salt(aes_enc.get());
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes_enc.get(), user_key, 32,
                                               "bucket1", "object1", 0));

    bufferlist input;
    input.append(plaintext);
    bufferlist encrypted;
    ASSERT_TRUE(aes_enc->encrypt(input, 0, input.length(), encrypted, 0, null_yield));

    auto aes_dec(AES_256_GCM_create(&no_dpp, g_ceph_context, &user_key[0], 32,
                                     reinterpret_cast<const uint8_t*>(salt.c_str()),
                                     salt.size()));
    ASSERT_TRUE(AES_256_GCM_derive_object_key(aes_dec.get(), user_key, 32,
                                               "bucket2", "object2", 0));

    bufferlist decrypted;
    ASSERT_FALSE(aes_dec->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));
  }
}

TEST(TestRGWCrypto, verify_AES_256_GCM_chunk_reorder_detection)
{
  // Verify that swapping chunk positions is detected via AAD
  NoDoutPrefix no_dpp(g_ceph_context, ceph_subsys_rgw);
  uint8_t key[32];
  for (size_t i = 0; i < sizeof(key); i++) key[i] = i * 7;

  auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes.get(), nullptr);

  // Create 2 chunks of data (8192 bytes = 2 x 4096)
  const size_t size = 8192;
  buffer::ptr buf(size);
  char* p = buf.c_str();
  for (size_t i = 0; i < size; i++) p[i] = i & 0xFF;

  bufferlist input;
  input.append(buf);

  bufferlist encrypted;
  ASSERT_TRUE(aes->encrypt(input, 0, size, encrypted, 0, null_yield));

  // Encrypted layout: [chunk0_cipher(4096)][tag0(16)][chunk1_cipher(4096)][tag1(16)]
  // Total: 8192 + 32 = 8224 bytes
  ASSERT_EQ(encrypted.length(), 8224u);

  // Swap the two encrypted chunks (including their tags)
  buffer::ptr enc_copy(encrypted.length());
  encrypted.begin().copy(encrypted.length(), enc_copy.c_str());
  char* enc_data = enc_copy.c_str();
  const size_t enc_chunk_size = 4096 + 16;  // ciphertext + tag
  char temp[4112];
  memcpy(temp, enc_data, enc_chunk_size);                       // save chunk0
  memcpy(enc_data, enc_data + enc_chunk_size, enc_chunk_size);  // chunk1 -> chunk0 position
  memcpy(enc_data + enc_chunk_size, temp, enc_chunk_size);      // chunk0 -> chunk1 position

  bufferlist swapped;
  swapped.append(enc_copy);

  // Decryption should fail because AAD (chunk_index) won't match
  bufferlist decrypted;
  ASSERT_FALSE(aes->decrypt(swapped, 0, swapped.length(), decrypted, 0, null_yield));
}

TEST(TestRGWCrypto, verify_AES_256_GCM_aad_roundtrip)
{
  // Verify basic encrypt/decrypt still works correctly with AAD
  NoDoutPrefix no_dpp(g_ceph_context, ceph_subsys_rgw);
  uint8_t key[32];
  for (size_t i = 0; i < sizeof(key); i++) key[i] = i * 11;

  // Test multiple chunk scenarios
  for (size_t size : {4096u, 8192u, 12288u, 10000u}) {
    auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
    ASSERT_NE(aes.get(), nullptr);

    buffer::ptr buf(size);
    char* p = buf.c_str();
    for (size_t i = 0; i < size; i++) p[i] = (i * 17) & 0xFF;

    bufferlist input;
    input.append(buf);

    bufferlist encrypted;
    ASSERT_TRUE(aes->encrypt(input, 0, size, encrypted, 0, null_yield));

    bufferlist decrypted;
    ASSERT_TRUE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted, 0, null_yield));

    ASSERT_EQ(decrypted.length(), size);
    ASSERT_EQ(std::string_view(input.c_str(), size),
              std::string_view(decrypted.c_str(), size));
  }
}

TEST(TestRGWCrypto, verify_AES_256_GCM_aad_offset_mismatch)
{
  // Verify AAD detects wrong stream offset during decryption
  NoDoutPrefix no_dpp(g_ceph_context, ceph_subsys_rgw);
  uint8_t key[32];
  for (size_t i = 0; i < sizeof(key); i++) key[i] = i * 13;

  auto aes(AES_256_GCM_create(&no_dpp, g_ceph_context, &key[0], 32));
  ASSERT_NE(aes.get(), nullptr);

  const size_t size = 4096;
  buffer::ptr buf(size);
  char* p = buf.c_str();
  for (size_t i = 0; i < size; i++) p[i] = i & 0xFF;

  bufferlist input;
  input.append(buf);

  // Encrypt at offset 8192 (chunk index 2)
  off_t stream_offset = 8192;
  bufferlist encrypted;
  ASSERT_TRUE(aes->encrypt(input, 0, size, encrypted, stream_offset, null_yield));

  // Decrypt with same offset - should succeed
  bufferlist decrypted;
  ASSERT_TRUE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted, stream_offset, null_yield));
  ASSERT_EQ(std::string_view(input.c_str(), size),
            std::string_view(decrypted.c_str(), size));

  // Decrypt with wrong offset (0 instead of 8192) - should FAIL (AAD mismatch)
  bufferlist decrypted_wrong;
  ASSERT_FALSE(aes->decrypt(encrypted, 0, encrypted.length(), decrypted_wrong, 0, null_yield));
}


// Test helper class to expose private methods for unit testing
// (declared as friend in RGWGetObj_BlockDecrypt)
class TestableBlockDecrypt : public RGWGetObj_BlockDecrypt {
public:
  // Re-export PartLocation struct for test code
  using PartLocation = RGWGetObj_BlockDecrypt::PartLocation;

  TestableBlockDecrypt(const DoutPrefixProvider* dpp,
                       CephContext* cct,
                       RGWGetObj_Filter* next,
                       std::unique_ptr<BlockCrypt> crypt,
                       std::vector<size_t> parts_len)
    : RGWGetObj_BlockDecrypt(dpp, cct, next, std::move(crypt),
                             std::move(parts_len), null_yield) {}

  // Wrapper to expose private method for testing
  PartLocation find_part_for_plaintext_offset(off_t plaintext_ofs, bool clamp_to_last) const {
    return RGWGetObj_BlockDecrypt::find_part_for_plaintext_offset(plaintext_ofs, clamp_to_last);
  }
};

TEST(TestRGWCrypto, verify_PartLocation_single_part)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  std::vector<size_t> parts = {10 * 1024 * 1024};
  auto crypt = std::make_unique<BlockCryptNone>(4096);
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  auto loc = decrypt.find_part_for_plaintext_offset(0, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(1000, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 1000);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(10 * 1024 * 1024 - 1, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 10 * 1024 * 1024 - 1);
  ASSERT_EQ(loc.cumulative_encrypted, 0);
}

TEST(TestRGWCrypto, verify_PartLocation_multipart_cbc)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  const size_t part_size = 5 * 1024 * 1024;
  std::vector<size_t> parts = {part_size, part_size, part_size};
  auto crypt = std::make_unique<BlockCryptNone>(4096);
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  auto loc = decrypt.find_part_for_plaintext_offset(0, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(part_size - 1, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, (off_t)(part_size - 1));
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(part_size, false);
  ASSERT_EQ(loc.part_idx, 1u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)part_size);

  loc = decrypt.find_part_for_plaintext_offset(part_size + 1000, false);
  ASSERT_EQ(loc.part_idx, 1u);
  ASSERT_EQ(loc.offset_in_part, 1000);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)part_size);

  loc = decrypt.find_part_for_plaintext_offset(2 * part_size, false);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)(2 * part_size));

  loc = decrypt.find_part_for_plaintext_offset(3 * part_size - 100, false);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, (off_t)(part_size - 100));
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)(2 * part_size));
}

TEST(TestRGWCrypto, verify_PartLocation_multipart_aead)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  // For AEAD, parts_len contains ENCRYPTED sizes (with auth tag overhead)
  const size_t plaintext_per_part = 1024 * AEAD_CHUNK_SIZE;  // 4MB
  const size_t encrypted_per_part = aead_plaintext_to_encrypted_size(plaintext_per_part);
  std::vector<size_t> parts = {encrypted_per_part, encrypted_per_part, encrypted_per_part};

  auto crypt = std::make_unique<BlockCryptNoneAEAD>();
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  auto loc = decrypt.find_part_for_plaintext_offset(0, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(plaintext_per_part - 1, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, (off_t)(plaintext_per_part - 1));
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(plaintext_per_part, false);
  ASSERT_EQ(loc.part_idx, 1u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)encrypted_per_part);

  loc = decrypt.find_part_for_plaintext_offset(plaintext_per_part + 8192, false);
  ASSERT_EQ(loc.part_idx, 1u);
  ASSERT_EQ(loc.offset_in_part, 8192);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)encrypted_per_part);

  loc = decrypt.find_part_for_plaintext_offset(2 * plaintext_per_part, false);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, (off_t)(2 * encrypted_per_part));
}

TEST(TestRGWCrypto, verify_PartLocation_clamp_to_last)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  const size_t part_size = 5 * 1024 * 1024;
  std::vector<size_t> parts = {part_size, part_size, part_size};
  auto crypt = std::make_unique<BlockCryptNone>(4096);
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  // Offset within object - both modes return the same
  auto loc = decrypt.find_part_for_plaintext_offset(2 * part_size + 1000, false);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, 1000);

  loc = decrypt.find_part_for_plaintext_offset(2 * part_size + 1000, true);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, 1000);

  // Offset beyond all parts - without clamping returns invalid index
  loc = decrypt.find_part_for_plaintext_offset(3 * part_size + 5000, false);
  ASSERT_EQ(loc.part_idx, 3u);
  ASSERT_EQ(loc.offset_in_part, 5000);

  // With clamping - stays at last valid part
  loc = decrypt.find_part_for_plaintext_offset(3 * part_size + 5000, true);
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, (off_t)(part_size + 5000));
}

TEST(TestRGWCrypto, verify_PartLocation_unequal_parts)
{
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  // Parts: 1MB, 3MB, 2MB
  std::vector<size_t> parts = {1024 * 1024, 3 * 1024 * 1024, 2 * 1024 * 1024};
  auto crypt = std::make_unique<BlockCryptNone>(4096);
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  auto loc = decrypt.find_part_for_plaintext_offset(500 * 1024, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 500 * 1024);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(1536 * 1024, false);  // 1.5MB
  ASSERT_EQ(loc.part_idx, 1u);
  ASSERT_EQ(loc.offset_in_part, 512 * 1024);
  ASSERT_EQ(loc.cumulative_encrypted, 1024 * 1024);

  loc = decrypt.find_part_for_plaintext_offset(4608 * 1024, false);  // 4.5MB
  ASSERT_EQ(loc.part_idx, 2u);
  ASSERT_EQ(loc.offset_in_part, 512 * 1024);
  ASSERT_EQ(loc.cumulative_encrypted, 4 * 1024 * 1024);
}

TEST(TestRGWCrypto, verify_PartLocation_no_manifest)
{
  // Single-part objects don't have a multipart manifest, so parts_len is empty.
  // The code treats this as one logical part starting at offset 0.
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);
  ut_get_sink get_sink;

  std::vector<size_t> parts = {};
  auto crypt = std::make_unique<BlockCryptNone>(4096);
  TestableBlockDecrypt decrypt(&no_dpp, g_ceph_context, &get_sink,
                               std::move(crypt), parts);

  auto loc = decrypt.find_part_for_plaintext_offset(0, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 0);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(1000, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 1000);
  ASSERT_EQ(loc.cumulative_encrypted, 0);

  loc = decrypt.find_part_for_plaintext_offset(10 * 1024 * 1024, false);
  ASSERT_EQ(loc.part_idx, 0u);
  ASSERT_EQ(loc.offset_in_part, 10 * 1024 * 1024);
  ASSERT_EQ(loc.cumulative_encrypted, 0);
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

