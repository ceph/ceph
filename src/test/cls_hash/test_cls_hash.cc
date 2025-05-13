// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 * Copyright (C) 2025 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "cls/hash/client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "common/ceph_crypto.h"
#include <optional>
#include "BLAKE3/c/blake3.h"
#include <climits>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <iomanip>
#include <random>
#include <cstdlib>
#include <ctime>

#define RUN_BLAKE3_TESTS
#define RUN_MD5_TESTS
#define RUN_SHA256_TESTS

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};

std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;
auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

namespace cls::hash {

  // test fixture with helper functions
  class HashTest : public ::testing::Test {
  protected:
    librados::IoCtx& ioctx = RadosEnv::ioctx;

    //---------------------------------------------------------------------------
    bool do_hash(const std::string& oid)
    {
      bufferlist bl;
      librados::ObjectReadOperation op;
      int ret = ioctx.operate(oid, &op, &bl, 0);
      return ret;
    }

  };
  char gbuff[12*1024*1024];

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void print_md5_hash(const char* name,
                                             const uint8_t *p_hash,
                                             uint64_t size)
  {
    uint64_t *p = (uint64_t*)p_hash;
    std::cout << name << std::hex << "::size=0x" << std::setw(6)
              << std::setfill('0') << size << "::::"<< *p << *(p+1) << std::endl;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void print_hash(const char* name,
                                         const uint8_t *p_hash,
                                         uint64_t size,
                                         uint32_t parts)
  {
    uint64_t *p = (uint64_t*)p_hash;
    std::cout << name << std::hex << "::size=0x" << size << "::parts=0x" << parts
              << ":::"<< *p << *(p+1) << *(p+2) << *(p+3) << std::endl;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void blake3_cls(const std::vector<std::string> &vec,
                                         const std::vector<uint32_t> &size_vec,
                                         librados::IoCtx &ioctx,
                                         uint8_t *p_hash)
  {
    const unsigned first_part_idx = 0;
    const unsigned last_part_idx = (vec.size() - 1);
    unsigned idx = 0;
    bufferlist hash_state_bl;
    uint64_t offset = 0;
    for (const auto& oid : vec ) {
      librados::ObjectReadOperation op;
      cls_hash_flags_t flags;

      if (idx == first_part_idx) {
        flags.set_first_part();
      }
      if (idx == last_part_idx) {
        flags.set_last_part();
      }
      bufferlist out_bl;
      ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, offset, &hash_state_bl, &out_bl, flags));
      int ret = ioctx.operate(oid, &op, nullptr, 0);
      ASSERT_EQ(0, ret);

      if (idx == last_part_idx) {
        ASSERT_EQ(out_bl.length(), BLAKE3_OUT_LEN);
        memcpy((char*)p_hash, out_bl.c_str(), BLAKE3_OUT_LEN);
      }
      else {
        ASSERT_LE(out_bl.length(), sizeof(blake3_hasher));
        hash_state_bl = out_bl;
      }

      offset += size_vec[idx];
      idx++;
    }
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void blake3_lcl(const std::vector<std::string> &vec,
                                         librados::IoCtx &ioctx,
                                         uint8_t *p_hash)
  {
    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    for (const auto& oid : vec ) {
      bufferlist bl;
      int ret = ioctx.read(oid, bl, 0, 0);
      ASSERT_EQ(bl.length(), ret);
      for (const auto& bptr : bl.buffers()) {
        blake3_hasher_update(&hmac, (const unsigned char *)bptr.c_str(), bptr.length());
      }
    }

    memset(p_hash, 0, BLAKE3_OUT_LEN);
    blake3_hasher_finalize(&hmac, p_hash, BLAKE3_OUT_LEN);
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void fill_buff_with_rand_data(char *buff, uint32_t size)
  {
    ASSERT_GE(sizeof(gbuff), size);
    // Seed with a real random value, if available
    std::random_device r;
    // Choose a random mean between 1 ULLONG_MAX
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());
    uint64_t *p_start = (uint64_t*)buff;
    uint64_t *p_end   = (uint64_t*)(buff + size);
    for (auto p = p_start; p < p_end; p++) {
      *p = uniform_dist(e1);
    }
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void write_obj(const std::string &oid,
                                        librados::IoCtx &ioctx,
                                        char *buff,
                                        uint32_t size)
  {
    bufferlist bl ;
    bl.append(buff, size);
    int ret = ioctx.write_full(oid, bl);
    ASSERT_EQ(ret, (int)bl.length());
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void write_obj_rand_data(const std::string &oid,
                                                  librados::IoCtx &ioctx,
                                                  uint32_t size)
  {
    fill_buff_with_rand_data(gbuff, size);
    write_obj(oid, ioctx, gbuff, size);
  }
#ifdef RUN_BLAKE3_TESTS
  //---------------------------------------------------------------------------
  static void hash_multi_objs_file(const std::string &namebase,
                                   librados::IoCtx &ioctx,
                                   uint32_t obj_count,
                                   uint32_t fixed_size = 0)
  {
    std::vector<std::string> vec;
    std::vector<uint32_t> size_vec;
    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];
    uint8_t hash3[BLAKE3_OUT_LEN];

    blake3_hasher hmac3;
    blake3_hasher_init(&hmac3);
    memset(hash3, 0, sizeof(hash3));

    std::srand(std::time({})); // use current time as seed for random generator
    uint64_t total_size = 0;
    uint32_t parts = 0;
    uint32_t size;
    for (unsigned i = 0; i < obj_count; i++) {
      const std::string oid = namebase + std::to_string(i);
      vec.push_back(oid);
      if (fixed_size) {
        size = fixed_size;
      }
      else {
        size = sizeof(gbuff) - (std::rand() % (sizeof(gbuff)/2));
      }
      size_vec.push_back(size);
      total_size += size;
      parts++;
      fill_buff_with_rand_data(gbuff, size);
      blake3_hasher_update(&hmac3, (const unsigned char *)gbuff, size);
      write_obj(oid, ioctx, gbuff, size);
    }

    blake3_cls(vec, size_vec, ioctx, hash1);
    print_hash("BLAKE3-HASH", hash1, total_size, parts);
    blake3_lcl(vec, ioctx, hash2);
    //print_hash("READ::", hash2);
    blake3_hasher_finalize(&hmac3, hash3, BLAKE3_OUT_LEN);
    //print_hash("BUFF::", hash3);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
    ASSERT_EQ(memcmp(hash1, hash3, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 17;
    for (unsigned i = 0; i < 11; i++) {
      hash_multi_objs_file(func + std::to_string(i), ioctx, MAX_OBJS + i);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_large)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_multi_objs_file(func + "_A", ioctx, 113, 4 << 20);
    hash_multi_objs_file(func + "_B", ioctx, 229, 4 << 20);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_large_rand)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_multi_objs_file(func + "_A", ioctx, 157);
    hash_multi_objs_file(func + "_B", ioctx, 353);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_single_part_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned i = 0; i < 11; i++) {
      hash_multi_objs_file(func + std::to_string(i), ioctx, MAX_OBJS);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_very_large_obj)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    // used size larger than the common 4MB size)
    for (unsigned size = 7<<20; size <= 10<<20; size += 1<<20) {
      hash_multi_objs_file(func + std::to_string(size), ioctx, MAX_OBJS, size);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_full_obj)
  {
    const unsigned MAX_OBJS = 1;
    // used 4MB object
    hash_multi_objs_file(__PRETTY_FUNCTION__, ioctx, MAX_OBJS, 4 << 20 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_small_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_multi_objs_file(func + "_A", ioctx, MAX_OBJS, 512*1024 );
    hash_multi_objs_file(func + "_B", ioctx, MAX_OBJS, 511*1024 );
    hash_multi_objs_file(func + "_C", ioctx, MAX_OBJS, 65*1024 );
    hash_multi_objs_file(func + "_D", ioctx, MAX_OBJS, 64*1024 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_small_unaligned_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_multi_objs_file(func + "_A", ioctx, MAX_OBJS, 512*1024+7);
    hash_multi_objs_file(func + "_B", ioctx, MAX_OBJS, 511*1024+3);
    hash_multi_objs_file(func + "_C", ioctx, MAX_OBJS, 65*1024+5);
    hash_multi_objs_file(func + "_D", ioctx, MAX_OBJS, 64*1024+11);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_single_file_sub_block_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned size = 256; size <= 4 * 1024; size += 256) {
      hash_multi_objs_file(func + std::to_string(size), ioctx, MAX_OBJS, size );
      hash_multi_objs_file(func + std::to_string(size) + "b", ioctx, MAX_OBJS,
                           size+7);
    }
  }

  //---------------------------------------------------------------------------
  static void hash_2_identical_files(const std::string &namebase,
                                     librados::IoCtx &ioctx,
                                     uint32_t obj_count,
                                     uint32_t fixed_size = 0)
  {
    std::vector<std::string> vec1, vec2;
    std::vector<uint32_t> size_vec;
    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];

    std::srand(std::time({})); // use current time as seed for random generator
    uint32_t size;
    for (unsigned i = 0; i < obj_count; i++) {
      const std::string oid1 = namebase + std::to_string(i);
      const std::string oid2 = namebase + std::to_string(i) + "_b";
      vec1.push_back(oid1);
      vec2.push_back(oid2);
      if (fixed_size) {
        size = fixed_size;
      }
      else {
        size = sizeof(gbuff) - (std::rand() % (sizeof(gbuff)/2));
      }
      size_vec.push_back(size);
      write_obj(oid1, ioctx, gbuff, size);
      write_obj(oid2, ioctx, gbuff, size);
    }
    blake3_cls(vec1, size_vec, ioctx, hash1);
    //print_hash("VEC1::", hash1);
    blake3_cls(vec2, size_vec, ioctx, hash2);
    //print_hash("VEC2::", hash2);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);

    // verify CLS results using local calculation
    uint8_t hash1b[BLAKE3_OUT_LEN];
    blake3_lcl(vec1, ioctx, hash1b);
    ASSERT_EQ(memcmp(hash1, hash1b, sizeof(hash1)), 0);

    uint8_t hash2b[BLAKE3_OUT_LEN];
    blake3_lcl(vec2, ioctx, hash2b);
    ASSERT_EQ(memcmp(hash2, hash2b, sizeof(hash2)), 0);

  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_large)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_2_identical_files(func + "_A", ioctx, 59, 4 << 20);
    hash_2_identical_files(func + "_B", ioctx, 137, 4 << 20);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_large_rand)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_2_identical_files(func + "_A", ioctx, 67);
    hash_2_identical_files(func + "_B", ioctx, 167);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 7;
    for (unsigned i = 0; i < 11; i++) {
      hash_2_identical_files(func + std::to_string(i), ioctx, MAX_OBJS+i);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_single_part_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned i = 0; i < 11; i++) {
      hash_2_identical_files(func + std::to_string(i), ioctx, MAX_OBJS);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_very_large_obj)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    // used size larger than the common 4MB size)
    for (unsigned size = 7<<20; size <= 10<<20; size += 1<<20) {
      hash_2_identical_files(func + std::to_string(size), ioctx, MAX_OBJS, size);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_full_obj)
  {
    const unsigned MAX_OBJS = 1;
    // used 4MB object
    hash_2_identical_files(__PRETTY_FUNCTION__, ioctx, MAX_OBJS, 4 << 20 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_small_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_2_identical_files(func + "_A", ioctx, MAX_OBJS, 512*1024 );
    hash_2_identical_files(func + "_B", ioctx, MAX_OBJS, 511*1024 );
    hash_2_identical_files(func + "_C", ioctx, MAX_OBJS, 65*1024 );
    hash_2_identical_files(func + "_D", ioctx, MAX_OBJS, 64*1024 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_small_unaligned_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_2_identical_files(func + "_A", ioctx, MAX_OBJS, 512*1024+7);
    hash_2_identical_files(func + "_B", ioctx, MAX_OBJS, 511*1024+3);
    hash_2_identical_files(func + "_C", ioctx, MAX_OBJS, 65*1024+5);
    hash_2_identical_files(func + "_D", ioctx, MAX_OBJS, 64*1024+11);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_identical_files_sub_block_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned size = 256; size <= 4 * 1024; size += 256) {
      hash_2_identical_files(func + std::to_string(size), ioctx, MAX_OBJS, size );
      hash_2_identical_files(func + std::to_string(size) + "b", ioctx, MAX_OBJS,
                             size+7);
    }
  }

  //---------------------------------------------------------------------------
  static void hash_2_non_identical_files(const std::string &namebase,
                                         librados::IoCtx &ioctx,
                                         uint32_t obj_count,
                                         uint32_t fixed_size = 0)
  {
    std::vector<std::string> vec1, vec2;
    std::vector<uint32_t> size_vec;
    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < obj_count; i++) {
      const std::string oid1 = namebase + std::to_string(i);
      const std::string oid2 = namebase + std::to_string(i) + "_b";
      vec1.push_back(oid1);
      vec2.push_back(oid2);
      uint32_t size = sizeof(gbuff) - (std::rand() % (sizeof(gbuff)/2));
      size_vec.push_back(size);
      write_obj(oid1, ioctx, gbuff, size);
      // change one byte in the second object
      if (i == obj_count - 1) {
        gbuff[0]++;
      }
      write_obj(oid2, ioctx, gbuff, size);
    }

    blake3_cls(vec1, size_vec, ioctx, hash1);
    //print_hash("VEC1::", hash1);
    blake3_cls(vec2, size_vec, ioctx, hash2);
    //print_hash("VEC2::", hash2);

    ASSERT_NE(memcmp(hash1, hash2, sizeof(hash1)), 0);

    // verify CLS results using local calculation
    uint8_t hash1b[BLAKE3_OUT_LEN];
    blake3_lcl(vec1, ioctx, hash1b);
    ASSERT_EQ(memcmp(hash1, hash1b, sizeof(hash1)), 0);

    uint8_t hash2b[BLAKE3_OUT_LEN];
    blake3_lcl(vec2, ioctx, hash2b);
    ASSERT_EQ(memcmp(hash2, hash2b, sizeof(hash2)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_large)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_2_non_identical_files(func + "_A", ioctx, 59, 4 << 20);
    hash_2_non_identical_files(func + "_B", ioctx, 137, 4 << 20);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_large_rand)
  {
    const std::string func(__PRETTY_FUNCTION__);
    hash_2_non_identical_files(func + "_A", ioctx, 67);
    hash_2_non_identical_files(func + "_B", ioctx, 167);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 7;
    for (unsigned i = 0; i < 11; i++) {
      hash_2_non_identical_files(func + std::to_string(i), ioctx, MAX_OBJS+i);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_fixed_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 7;
    for (unsigned i = 0; i < 11; i++) {
      hash_2_non_identical_files(func + std::to_string(i), ioctx, MAX_OBJS+i,
                                 4 << 20);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_single_part_rand_size)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned i = 0; i < 11; i++) {
      hash_2_non_identical_files(func + std::to_string(i), ioctx, MAX_OBJS);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_very_large_obj)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    // used size larger than the common 4MB size)
    for (unsigned size = 7<<20; size <= 10<<20; size += 1<<20) {
      hash_2_non_identical_files(func + std::to_string(size), ioctx, MAX_OBJS,
                                 size);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_full_obj)
  {
    const unsigned MAX_OBJS = 1;
    // used 4MB object
    hash_2_non_identical_files(__PRETTY_FUNCTION__, ioctx, MAX_OBJS, 4 << 20 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_small_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_2_non_identical_files(func + "_A", ioctx, MAX_OBJS, 512*1024 );
    hash_2_non_identical_files(func + "_B", ioctx, MAX_OBJS, 511*1024 );
    hash_2_non_identical_files(func + "_C", ioctx, MAX_OBJS, 65*1024 );
    hash_2_non_identical_files(func + "_D", ioctx, MAX_OBJS, 64*1024 );
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_small_unaligned_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    hash_2_non_identical_files(func + "_A", ioctx, MAX_OBJS, 512*1024+7);
    hash_2_non_identical_files(func + "_B", ioctx, MAX_OBJS, 511*1024+3);
    hash_2_non_identical_files(func + "_C", ioctx, MAX_OBJS, 65*1024+5);
    hash_2_non_identical_files(func + "_D", ioctx, MAX_OBJS, 64*1024+11);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, hash_2_non_identical_files_sub_block_objs)
  {
    const std::string func(__PRETTY_FUNCTION__);
    const unsigned MAX_OBJS = 1;
    for (unsigned size = 256; size <= 4 * 1024; size += 256) {
      hash_2_non_identical_files(func + std::to_string(size), ioctx, MAX_OBJS,
                                 size);
      hash_2_non_identical_files(func + std::to_string(size) + "b", ioctx, MAX_OBJS,
                                 size+7);
    }
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, bad_input_overflow_first_part)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, 512*1024);

    bufferlist out_bl;
    bufferlist hash_state_bl;
    char junk[16];
    hash_state_bl.append((const char*)&junk, sizeof(junk));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    flags.set_first_part();
    ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, 0, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOVERFLOW, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, bad_input_overflow)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    blake3_hasher_update(&hmac, (const unsigned char *)gbuff, obj_size);
    blake3_to_ceph(&hmac);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    char junk[16];
    hash_state_bl.append((const char*)&junk, sizeof(junk));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOVERFLOW, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, bad_input_corrupted_stack_len)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    blake3_hasher_update(&hmac, (const unsigned char *)gbuff, obj_size);
    hmac.cv_stack_len = (BLAKE3_MAX_DEPTH + 2);
    blake3_to_ceph(&hmac);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, bad_input_corrupted_chunk_buf_len)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    blake3_hasher_update(&hmac, (const unsigned char *)gbuff, obj_size);
    hmac.chunk.buf_len = (BLAKE3_BLOCK_LEN + 1);
    blake3_to_ceph(&hmac);

    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, bad_input_corrupted_chunk_counter)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size * 2;
    write_obj_rand_data(oid, ioctx, obj_size);
    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    // after update the hmac->offset will match obj_size, but we pass higher offset
    // (off = obj_size * 2) which will look like a missed part
    blake3_hasher_update(&hmac, (const unsigned char *)gbuff, obj_size);
    blake3_to_ceph(&hmac);

    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_BLAKE3, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, validate_chunk_counter)
  {
    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    fill_buff_with_rand_data(gbuff, sizeof(gbuff));
    std::srand(std::time({})); // use current time as seed for random generator
    uint64_t off = 0;
    for (unsigned i = 0; i < 1*1024; i++) {
      uint64_t expected_chunk_counter = (off - hmac.chunk.buf_len)/ BLAKE3_CHUNK_LEN;
      if (expected_chunk_counter != hmac.chunk.chunk_counter) {
        std::cerr << "offset=" << off << "::idx=" << i
                  << "::expected_chunk_counter=" << expected_chunk_counter
                  << "::chunk_counter=" << hmac.chunk.chunk_counter << std::endl;
        ASSERT_EQ(expected_chunk_counter, hmac.chunk.chunk_counter);
        break;
      }

      uint32_t size = std::rand() % (sizeof(gbuff));
      blake3_hasher_update(&hmac, (const unsigned char *)gbuff, size);
      off += size;
      if (i % 1024 == 0 && 0) {
        std::cout << "##idx = " << i << "::offset = " << off
                  << "::chunk_counter=" << hmac.chunk.chunk_counter
                  << "::buf_len=" << (int)hmac.chunk.buf_len
                  << "::cv_stack_len=" << (int)hmac.cv_stack_len << std::endl;
      }
    }
    std::cout << "\n\n=================================\noffset = " << off
              << "::chunk_counter=" << hmac.chunk.chunk_counter
              << "::buf_len=" << (int)hmac.chunk.buf_len
              << "::cv_stack_len=" << (int)hmac.cv_stack_len << std::endl;
  }
#endif

#ifdef RUN_MD5_TESTS
  // ==========================================================================
  //                                 MD5-Tests
  // ==========================================================================
  //---------------------------------------------------------------------------
  TEST_F(HashTest, md5_bad_input_last_not_first)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);
    ceph::crypto::MD5 hmac;
    hmac.Update((const unsigned char *)gbuff, obj_size);

    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    flags.set_last_part();
    ASSERT_EQ(0, hash_data(op, HASH_MD5, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, md5_bad_input_first_not_last)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = 0;
    write_obj_rand_data(oid, ioctx, obj_size);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    flags.set_first_part();
    ASSERT_EQ(0, hash_data(op, HASH_MD5, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, md5_bad_input_not_last_not_first)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);
    ceph::crypto::MD5 hmac;
    hmac.Update((const unsigned char *)gbuff, obj_size);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_MD5, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, md5_first_and_last)
  {
    uint8_t hash1[CEPH_CRYPTO_MD5_DIGESTSIZE];
    uint8_t hash2[CEPH_CRYPTO_MD5_DIGESTSIZE];
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t off = 0;
    std::srand(std::time({})); // use current time as seed for random generator

    for (unsigned idx = 0; idx < 17; idx++) {
      uint32_t obj_size = std::rand() % (4<<20);
      fill_buff_with_rand_data(gbuff, obj_size);
      ceph::crypto::MD5 hmac;
      hmac.Update((const unsigned char *)gbuff, obj_size);
      hmac.Final(hash1);
      //print_md5_hash("MD5-LCL", hash1, obj_size);

      write_obj(oid, ioctx, gbuff, obj_size);
      bufferlist out_bl;
      bufferlist hash_state_bl;
      librados::ObjectReadOperation op;
      cls_hash_flags_t flags;
      flags.set_first_part();
      flags.set_last_part();
      ASSERT_EQ(0, hash_data(op, HASH_MD5, off, &hash_state_bl, &out_bl, flags));
      int ret = ioctx.operate(oid, &op, nullptr, 0);
      ASSERT_EQ(0, ret);
      ASSERT_EQ(out_bl.length(), CEPH_CRYPTO_MD5_DIGESTSIZE);
      memcpy((char*)hash2, out_bl.c_str(), sizeof(hash2));
      print_md5_hash("MD5-CLS", hash1, obj_size);
      ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
    }
  }
#endif

#ifdef RUN_SHA256_TESTS
  // ==========================================================================
  //                                SHA256-Tests
  // ==========================================================================
  //---------------------------------------------------------------------------
  TEST_F(HashTest, sha256_bad_input_last_not_first)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);
    ceph::crypto::SHA256 hmac;
    hmac.Update((const unsigned char *)gbuff, obj_size);

    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    flags.set_last_part();
    ASSERT_EQ(0, hash_data(op, HASH_SHA256, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, sha256_bad_input_first_not_last)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = 0;
    write_obj_rand_data(oid, ioctx, obj_size);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    flags.set_first_part();
    ASSERT_EQ(0, hash_data(op, HASH_SHA256, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, sha256_bad_input_not_last_not_first)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t obj_size = 512*1024;
    uint64_t off = obj_size;
    write_obj_rand_data(oid, ioctx, obj_size);
    ceph::crypto::SHA256 hmac;
    hmac.Update((const unsigned char *)gbuff, obj_size);
    bufferlist out_bl;
    bufferlist hash_state_bl;
    hash_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_hash_flags_t flags;
    ASSERT_EQ(0, hash_data(op, HASH_SHA256, off, &hash_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EOPNOTSUPP, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(HashTest, sha256_first_and_last)
  {
    uint8_t hash1[CEPH_CRYPTO_SHA256_DIGESTSIZE];
    uint8_t hash2[CEPH_CRYPTO_SHA256_DIGESTSIZE];
    const std::string oid = __PRETTY_FUNCTION__;
    uint64_t off = 0;
    std::srand(std::time({})); // use current time as seed for random generator

    for (unsigned idx = 0; idx < 17; idx++) {
      uint32_t obj_size = std::rand() % (4<<20);
      fill_buff_with_rand_data(gbuff, obj_size);
      ceph::crypto::SHA256 hmac;
      hmac.Update((const unsigned char *)gbuff, obj_size);
      hmac.Final(hash1);
      //print_hash("SHA256-LCL", hash1, obj_size, 1);

      write_obj(oid, ioctx, gbuff, obj_size);
      bufferlist out_bl;
      bufferlist hash_state_bl;
      librados::ObjectReadOperation op;
      cls_hash_flags_t flags;
      flags.set_first_part();
      flags.set_last_part();
      ASSERT_EQ(0, hash_data(op, HASH_SHA256, off, &hash_state_bl, &out_bl, flags));
      int ret = ioctx.operate(oid, &op, nullptr, 0);
      ASSERT_EQ(0, ret);
      ASSERT_EQ(out_bl.length(), CEPH_CRYPTO_SHA256_DIGESTSIZE);
      memcpy((char*)hash2, out_bl.c_str(), sizeof(hash2));
      print_hash("SHA256-CLS", hash1, obj_size, 1);
      ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
    }
  }
#endif
} // namespace cls::hash
