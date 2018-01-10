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
#include "rgw/rgw_common.h"
#include "rgw/rgw_rados.h"
#include "rgw/rgw_crypt.h"
#include <gtest/gtest.h>
#include "include/assert.h"
#define dout_subsys ceph_subsys_rgw

using namespace std;


std::unique_ptr<BlockCrypt> AES_256_CBC_create(CephContext* cct, const uint8_t* key, size_t len);


class ut_get_sink : public RGWGetDataCB {
  std::stringstream sink;
public:
  ut_get_sink() {}
  virtual ~ut_get_sink() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    sink << boost::string_ref(bl.c_str()+bl_ofs, bl_len);
    return 0;
  }
  std::string get_sink()
  {
    return sink.str();
  }
};

class ut_put_sink: public RGWPutObjDataProcessor
{
  std::stringstream sink;
public:
  ut_put_sink(){}
  virtual ~ut_put_sink(){}
  int handle_data(bufferlist& bl, off_t ofs, void **phandle, rgw_raw_obj *pobj, bool *again) override
  {
    sink << boost::string_ref(bl.c_str(),bl.length());
    *again = false;
    return 0;
  }
  int throttle_data(void *handle, const rgw_raw_obj& obj, uint64_t size, bool need_to_wait) override
  {
    return 0;
  }
  std::string get_sink()
  {
    return sink.str();
  }
};


class BlockCryptNone: public BlockCrypt {
public:
  BlockCryptNone(){};
  virtual ~BlockCryptNone(){};
  size_t get_block_size() override
  {
    return 256;
  }
  bool encrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset) override
  {
    output.clear();
    output.append(input.c_str(), input.length());
    return true;
  }
  bool decrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset) override
  {
    output.clear();
    output.append(input.c_str(), input.length());
    return true;
  }
};


TEST(TestRGWCrypto, verify_AES_256_CBC_identity)
{
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

    auto aes(AES_256_CBC_create(g_ceph_context, &key[0], 32));
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
      ASSERT_TRUE(aes->encrypt(input, begin, end - begin, encrypted, offset));
      bufferlist decrypted;
      ASSERT_TRUE(aes->decrypt(encrypted, 0, end - begin, decrypted, offset));

      ASSERT_EQ(decrypted.length(), end - begin);
      ASSERT_EQ(boost::string_ref(input.c_str() + begin, end - begin),
                boost::string_ref(decrypted.c_str(), end - begin) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_2)
{
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

    auto aes(AES_256_CBC_create(g_ceph_context, &key[0], 32));
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
      ASSERT_TRUE(aes->encrypt(input, begin, end, encrypted, offset));
      bufferlist decrypted;
      ASSERT_TRUE(aes->decrypt(encrypted, 0, end, decrypted, offset));

      ASSERT_EQ(decrypted.length(), end);
      ASSERT_EQ(boost::string_ref(input.c_str(), end),
                boost::string_ref(decrypted.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_3)
{
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

    auto aes(AES_256_CBC_create(g_ceph_context, &key[0], 32));
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
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos));
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
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos));
        encrypted2.append(tmp);
        pos += chunk;
        rr++;
      }
      ASSERT_EQ(encrypted1.length(), end);
      ASSERT_EQ(encrypted2.length(), end);
      ASSERT_EQ(boost::string_ref(encrypted1.c_str(), end),
                boost::string_ref(encrypted2.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_size_0_15)
{
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

    auto aes(AES_256_CBC_create(g_ceph_context, &key[0], 32));
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
      ASSERT_TRUE(aes->encrypt(input, 0, end, encrypted, offset));
      ASSERT_TRUE(aes->encrypt(encrypted, 0, end, decrypted, offset));
      ASSERT_EQ(encrypted.length(), end);
      ASSERT_EQ(decrypted.length(), end);
      ASSERT_EQ(boost::string_ref(input.c_str(), end),
                boost::string_ref(decrypted.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_AES_256_CBC_identity_last_block)
{
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

    auto aes(AES_256_CBC_create(g_ceph_context, &key[0], 32));
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
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos));
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
        ASSERT_TRUE(aes->encrypt(input, pos, chunk, tmp, offset + pos));
        encrypted2.append(tmp);
        pos += chunk;
        rr++;
      }
      ASSERT_EQ(encrypted1.length(), end);
      ASSERT_EQ(encrypted2.length(), end);
      ASSERT_EQ(boost::string_ref(encrypted1.c_str(), end),
                boost::string_ref(encrypted2.c_str(), end) );
    }
  }
}


TEST(TestRGWCrypto, verify_RGWGetObj_BlockDecrypt_ranges)
{
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

  auto cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
  ASSERT_NE(cbc.get(), nullptr);
  bufferlist encrypted;
  ASSERT_TRUE(cbc->encrypt(input, 0, test_range, encrypted, 0));


  for (off_t r = 93; r < 150; r++ )
  {
    ut_get_sink get_sink;
    auto cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWGetObj_BlockDecrypt decrypt(g_ceph_context, &get_sink, std::move(cbc) );

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
    ASSERT_EQ(decrypted, boost::string_ref(input.c_str()+begin, expected_len));
  }
}


TEST(TestRGWCrypto, verify_RGWGetObj_BlockDecrypt_chunks)
{
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

  auto cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
  ASSERT_NE(cbc.get(), nullptr);
  bufferlist encrypted;
  ASSERT_TRUE(cbc->encrypt(input, 0, test_range, encrypted, 0));

  for (off_t r = 93; r < 150; r++ )
  {
    ut_get_sink get_sink;
    auto cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWGetObj_BlockDecrypt decrypt(g_ceph_context, &get_sink, std::move(cbc) );

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
    ASSERT_EQ(decrypted, boost::string_ref(input.c_str()+begin, expected_len));
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
  ut_get_sink get_sink;
  auto nonecrypt = std::unique_ptr<BlockCrypt>(new BlockCryptNone);
  RGWGetObj_BlockDecrypt decrypt(g_ceph_context, &get_sink,
                                 std::move(nonecrypt));
  ASSERT_EQ(fixup_range(&decrypt,0,0),     range_t(0,255));
  ASSERT_EQ(fixup_range(&decrypt,1,256),   range_t(0,511));
  ASSERT_EQ(fixup_range(&decrypt,0,255),   range_t(0,255));
  ASSERT_EQ(fixup_range(&decrypt,255,256), range_t(0,511));
  ASSERT_EQ(fixup_range(&decrypt,511,1023), range_t(256,1023));
  ASSERT_EQ(fixup_range(&decrypt,513,1024), range_t(512,1024+255));
}


TEST(TestRGWCrypto, verify_RGWPutObj_BlockEncrypt_chunks)
{
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
    auto cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);
    RGWPutObj_BlockEncrypt encrypt(g_ceph_context, &put_sink,
                                   std::move(cbc) );

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
      void* handle;
      bool again = false;
      rgw_raw_obj ro;
      encrypt.handle_data(bl, 0, &handle, nullptr, &again);
      encrypt.throttle_data(handle, ro, size, false);

      pos = pos + size;
    } while (pos < test_size);
    bufferlist bl;
    void* handle;
    bool again = false;
    encrypt.handle_data(bl, 0, &handle, nullptr, &again);

    ASSERT_EQ(put_sink.get_sink().length(), static_cast<size_t>(test_size));

    cbc = AES_256_CBC_create(g_ceph_context, &key[0], 32);
    ASSERT_NE(cbc.get(), nullptr);

    bufferlist encrypted;
    bufferlist decrypted;
    encrypted.append(put_sink.get_sink());
    ASSERT_TRUE(cbc->decrypt(encrypted, 0, test_size, decrypted, 0));

    ASSERT_EQ(decrypted.length(), test_size);
    ASSERT_EQ(boost::string_ref(decrypted.c_str(), test_size),
              boost::string_ref(input.c_str(), test_size));
  }
}


TEST(TestRGWCrypto, verify_Encrypt_Decrypt)
{
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
    RGWPutObj_BlockEncrypt encrypt(g_ceph_context, &put_sink,
				   AES_256_CBC_create(g_ceph_context, &key[0], 32) );
    bufferlist bl;
    bl.append((char*)test_in, test_size);
    void* handle;
    bool again = false;
    rgw_raw_obj ro;
    encrypt.handle_data(bl, 0, &handle, nullptr, &again);
    encrypt.throttle_data(handle, ro, test_size, false);
    bl.clear();
    encrypt.handle_data(bl, 0, &handle, nullptr, &again);
    ASSERT_EQ(put_sink.get_sink().length(), test_size);

    bl.append(put_sink.get_sink().data(), put_sink.get_sink().length());
    ASSERT_EQ(bl.length(), test_size);

    ut_get_sink get_sink;
    RGWGetObj_BlockDecrypt decrypt(g_ceph_context, &get_sink,
                                   AES_256_CBC_create(g_ceph_context, &key[0], 32) );

    off_t bl_ofs = 0;
    off_t bl_end = test_size - 1;
    decrypt.fixup_range(bl_ofs, bl_end);
    decrypt.handle_data(bl, 0, bl.length());
    decrypt.flush();
    ASSERT_EQ(get_sink.get_sink().length(), test_size);
    ASSERT_EQ(get_sink.get_sink(), boost::string_ref((char*)test_in,test_size));
  }
  while (test_size < 20000);
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

