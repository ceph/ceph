// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_crypto.h"

#include <filesystem>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <cassert>

using namespace librados;
using std::cerr;
using std::cout;
//using std::endl;
using std::dec;
using std::hex;
using std::string;
using std::unique_ptr;
using std::vector;
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

#if 1
//===========================================================================
struct Key
{
  constexpr static uint32_t FP_SIZE = SHA_DIGEST_LENGTH;
  Key() {
    memset(fp_buff, 0, FP_SIZE);
  }
  Key(uint8_t *p) {
    memcpy(fp_buff, p, FP_SIZE);
  }
  const uint8_t* get_fp_buff() const {
    return fp_buff;
  }
private:
  uint8_t fp_buff[FP_SIZE];

public:
  struct KeyHash
  {
    std::size_t operator()(const struct Key& k) const
    {
      // The SHA is already a hashing function so no need for another hash
      return *(uint64_t*)(k.get_fp_buff());
    }
  };

  struct KeyEqual
  {
    bool operator()(const struct Key& lhs, const struct Key& rhs) const
    {
      return (memcmp(lhs.get_fp_buff(), rhs.get_fp_buff(), lhs.FP_SIZE) == 0);
    }
  };
} __attribute__((__packed__));

//---------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &stream, const Key& k)
{
  stream << std::hex << "0x";
  for (unsigned i = 0; i < k.FP_SIZE; i++) {
    stream << std::hex << k.get_fp_buff()[i];
  }
  return stream;
}
//---------------------------------------------------------------------------
[[maybe_unused]]static void print_hash_buffer(uint8_t *p, unsigned hash_size)
{
  for (unsigned i = 0; i < hash_size; i++) {
    printf("%02x", p[i]);
  }
  printf("\n");
}

using FP_Dict = std::unordered_map<Key, uint32_t, Key::KeyHash, Key::KeyEqual>;
#endif

constexpr uint64_t object_size = 64 * 1024;

struct params_t {
  string   pool_name;
  uint64_t chunk_size    = 8*1024;
  uint64_t objects_count = 2*1024;
  uint64_t object_size   = 256*1024;
  float    dup_ratio     = 1.5;
};

//---------------------------------------------------------------------------
void print_report(const uint16_t *blocks_dups,
		  unsigned        chunk_size,
		  unsigned        block_count)
{
  constexpr unsigned ARR_SIZE = 1024;
  std::array<uint32_t, ARR_SIZE+1> summery;

  for (unsigned i = 0; i < ARR_SIZE+1; ++i) {
    summery[i] = 0;
  }

  uint64_t dups_count = 0;
  uint64_t uniq_count = 0;
  for (unsigned i = 0; i < block_count; i++) {
    unsigned count = blocks_dups[i];
    if (count > 0) {
      if (count < ARR_SIZE) {
	summery[count]++;
      }
      else {
	summery[ARR_SIZE]++;
      }
      dups_count += count - 1;
      uniq_count++;
    }
  }

  uint64_t total = 0;
  cout << "Unique Count = " << uniq_count << ", Dup Count = " << dups_count << std::endl;
  for (unsigned idx = 0; idx < ARR_SIZE; idx ++) {
    if (summery[idx] > 0){
      uint64_t sum = 0;
      for (unsigned i = idx; i < ARR_SIZE+1; i++) {
	sum += summery[i];
      }
      std::cout << "We had " << summery[idx] << "/" << sum << " keys with " << idx << " repetitions" << std::endl;
      total += idx*summery[idx];
    }
  }
  if (summery[ARR_SIZE] > 0) {
    std::cout << "We had " << summery[ARR_SIZE] << " keys with more than " << ARR_SIZE << " repetitions " << std::endl;
  }
  cout << "total = " << total << ", singletons = " << summery[1] << std::endl;
}

//---------------------------------------------------------------------------
int generate_single_object(const uint8_t *data_buff,
			   uint64_t       object_size,
			   IoCtx         &ioctx,
			   const string   oid,
			   uint16_t      *blocks_dups,
			   unsigned       chunk_size,
			   unsigned       block_count)
{
  uint8_t object_buff[object_size];
  uint8_t *p = object_buff;

  for (unsigned i = 0; i < object_size/chunk_size; i++) {
    unsigned idx = rand() % block_count;
    const uint8_t *p_chunk = data_buff + (idx * chunk_size);
    memcpy(p, p_chunk, chunk_size);
    p += chunk_size;
    blocks_dups[idx]++;
  }

  unsigned data_size = p-object_buff;
  //cout << "data_size=" << data_size << " / " << object_size << std::endl;
  assert(data_size == object_size);
  bufferlist bl = bufferlist::static_from_mem((char*)object_buff, data_size);
  ioctx.write_full(oid, bl);

  return 0;
}

//---------------------------------------------------------------------------
int generate_objects(const params_t &params, const uint8_t *data_buff, unsigned data_buff_size)
{
  unsigned chunk_size = params.chunk_size;
  Rados rados;
  IoCtx ioctx;
  cout << "attmepting to connect to rados ..." << std::endl;
  int ret = rados.init_with_context(g_ceph_context);
  if (unlikely(ret < 0)) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ret = rados.connect();
  if (unlikely(ret < 0)) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ret = rados.ioctx_create(params.pool_name.c_str(), ioctx);
  if (unlikely(ret < 0)) {
    cerr << "error ioctx_create: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  cout << "rados.connect() was successful!" << std::endl;

  unsigned block_count = data_buff_size/chunk_size;
  std::unique_ptr<uint16_t[]> blocks_dups;
  try {
    blocks_dups = std::make_unique<uint16_t[]>(block_count);
  } catch (std::bad_alloc&) {
    cerr << "****Failed dynamic allocation, alloc size=" << block_count*sizeof(uint16_t) << std::endl;
    return -1;
  }
  memset(blocks_dups.get(), 0, sizeof(uint16_t)*block_count);

  for (unsigned object_no = 0; object_no < params.objects_count; object_no++) {
    std::string oid(string("obj.") + std::to_string(object_no));
    if (object_no % 100 == 0) {
      cout << "generate_single_object(" << oid << ")" << std::endl;
    }
    generate_single_object(data_buff, params.object_size, ioctx, oid,
			   blocks_dups.get(), chunk_size, block_count);
  }

  print_report(blocks_dups.get(), chunk_size, block_count);
  return 0;
}

//---------------------------------------------------------------------------
int data_init(uint8_t *data_buff, unsigned data_buff_size, int chunk_size)
{
  for (auto p = (int32_t*)data_buff; p < (int32_t*)(data_buff + data_buff_size); p++) {
    *p = rand();
  }
#if 1
  FP_Dict   fp_dict;
  uint8_t   digest[SHA_DIGEST_LENGTH];
  uint64_t  full_blocks_size = ((data_buff_size/chunk_size) * chunk_size);
  uint8_t  *p_end = data_buff + full_blocks_size;
  for (uint8_t *p = data_buff; p < p_end; p += chunk_size) {
    uint8_t *p_ret = SHA1(p, chunk_size, digest);
    assert(p_ret == digest);
    Key key(digest);
    auto itr = fp_dict.find(key);
    if (itr == fp_dict.end()) {
      fp_dict[key] = 1;
    }
    else {
      cerr << "Repeated hash (should never happen with random data)" << std::endl;
      return -1;
    }
  }
#endif
  cout << "Data Initialization was completed!" << std::endl;
  return 0;
}

//---------------------------------------------------------------------------
int usage(char* argv[])
{
  std::cout << "usage: " << argv[0] << " --pool=pool [options] \n"
    "options:\n"
    "   --chunk-size=size\n"
    "        set chunk size (must be a full multiplication of 1024 Bytes)\n"
    "   --objects-count=count\n"
    "        set the number of objects to be generated\n"
    "   --object-size=size\n"
    "        set object size (must be a full multiplication of 1024 Bytes)\n"
    "   --dup-ratio=ratio\n"
    "        set the dedup ratio (1-100)" << std::endl;

  return -1;
}

//---------------------------------------------------------------------------
static int process_arguments(vector<const char*> &args, params_t &params)
{
  constexpr int   basesize          = 1024;
  constexpr int   max_objects_count = 1024*1024;
  constexpr int   max_object_size   = 4*1024*1024;
  constexpr float max_dup_ratio     = 100;
  std::string val;
  std::vector<const char*>::iterator i;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_witharg(args, i, &val, "--pool", (char*)NULL)) {
      params.pool_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-size", (char*)NULL)) {
      int size = atoi(val.c_str());
      if (size >= basesize && (size % basesize == 0)) {
	params.chunk_size = size;
	cout << "Chunk Size was set to " << size/1024 << " KiB" << std::endl;
      }
      else {
	cerr << "illegal chunk-size (must be a full multiplication of " << basesize << ")" << std::endl;
	return -1;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--objects-count", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count >= 1 && count <= max_objects_count) {
	params.objects_count = count;
	cout << "Objects Count was set to " << count << " KiB" << std::endl;
      }
      else {
	cerr << "illegal objects-count ("<< count << ") must be between 1-" << max_objects_count << std::endl;
	return -1;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--object-size", (char*)NULL)) {
      int size = atoi(val.c_str());
      if (size >= basesize && size <= max_object_size) {
	params.object_size = size;
	cout << "Objects Size was set to " << size << " KiB" << std::endl;
      }
      else {
	cerr << "illegal object-size ("<< size << ") must be between " << basesize << " and " << max_object_size << std::endl;
	return -1;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--dup-ratio", (char*)NULL)) {
      float dup_ratio = atof(val.c_str());
      if (dup_ratio >= 1 && dup_ratio <= max_dup_ratio) {
	params.dup_ratio = dup_ratio;
	cout << "dup-ratio was set to " << dup_ratio << std::endl;
      }
      else {
	cerr << "illegal Duplication Ratio ("<< dup_ratio << ") must be between 1-" << max_dup_ratio << std::endl;
	return -1;
      }
    } else {
      if (val[0] == '-') {
	cerr << "Bad argument <" << val[0] << ">" << std::endl;
	return -1;
      }
      ++i;
    }
  }

  if (params.pool_name.length() == 0) {
    cerr << "No poolname was passed" << std::endl;
    return -1;
  }

  return 0;
}

//---------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  vector<const char*> args = argv_to_vec(argc, argv);
  if (ceph_argparse_need_usage(args)) {
    return usage(argv);
  }

  params_t prm;
  if (process_arguments(args, prm) != 0) {
    return usage(argv);
  }

  std::cout << "chunk_size=" << prm.chunk_size << ", objects_count=" << prm.objects_count
	    << ", dup_ratio=" << prm.dup_ratio << ", object_size=" << prm.object_size << std::endl;

  uint64_t data_buff_size = ((double)(prm.object_size * prm.objects_count))/prm.dup_ratio;
  cout << "data_buff_size=" << (double)data_buff_size/(1024*1024) << " MiB" << std::endl;

  std::unique_ptr<uint8_t[]> data_buff;
  try {
    data_buff = std::make_unique<uint8_t[]>(data_buff_size);
  } catch (std::bad_alloc&) {
    cerr << "****Failed dynamic allocation, alloc size=" << data_buff_size << std::endl;
    cerr << "You need to either decrease object-count or increase the dup-ratio" << std::endl;
    return -1;
  }
  memset(data_buff.get(), 0, data_buff_size);

  if (data_init(data_buff.get(), data_buff_size, prm.chunk_size) != 0) {
    return -1;
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  if (generate_objects(prm, data_buff.get(), data_buff_size) != 0) {
    return -1;
  }

  return 0;
}

//---------------------------------------------------------------------------
//                                    E O F
//---------------------------------------------------------------------------
//g++ -Wall data_gen.cc -o data_gen  -lssl -lcrypto
