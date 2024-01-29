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
#include "common/CDC.h"
#include "common/FastCDC.h"

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
#include <cinttypes>
#include <cstring>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;
using namespace librados;
using ceph::util::generate_random_number;
using ceph::decode;
using ceph::encode;
using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::ofstream;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

#define CEPHTOH_16 le16toh
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_16 htole16
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64


#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

std::mutex print_mtx;
//#define DISABLE_CDC_CODE
const string RAW_FP_NSPACE("RAW_FP");
const string REDUCED_FP_NSPACE("REDUCED_FP");
const char *HASH_PREFIX = "hash";
const char *HASH_FORMAT = "hash%02hX.%08X";
const unsigned HASH_OID_NAME_SIZE = 16;
enum chunk_algo_t { CHUNK_ALGO_NONE, CHUNK_ALGO_FBC, CHUNK_ALGO_CDC };
struct dedup_params_t {
  uint32_t max_objs    = 0;
  uint32_t chunk_size  = 2 * 1024;
  uint16_t max_threads = 16;
  uint16_t num_shards  = 1;
  uint16_t dedup_threshold = 2;
  bool verbose = false;
  chunk_algo_t chunk_algo = CHUNK_ALGO_NONE;
  string   pool_name;
};


static void run_full_scan(const dedup_params_t &params);

constexpr int CHUNK_BASESIZE      = 1024;
constexpr int SHARDS_COUNT_MAX    = 256;
constexpr int THREAD_LIMIT_STRICT = 16;
//---------------------------------------------------------------------------
void usage(ostream& out)
{
  out << "\nusage: ceph-dedup-estimate-tool [options] [commands]\n\n"
    "Commands:\n"
    "   clean:   clean intermediate objects created by previous runs\n"
    "   collect: collect chunks finger prints\n"
    "   reduce:  reduce fingerprints, leaving only FP with more than N duplicates\n"
    "   merge:   merge the reduce fingerprints into a single unified table\n"
    "   all:     clean, collect, reduce and merge\n\n"

    "Global options:\n"
    "   -p pool\n"
    "   --pool=pool\n"
    "        select given pool by name\n"
    "   --thread-count=count\n"
    "        set the number of threads to run (1-" << THREAD_LIMIT_STRICT << ")\n"
    "   --chunk-size=size\n"
    "        set chunk size for finger-print (must be a full multiplication of " << CHUNK_BASESIZE << ")\n\n"

    "Collect options:\n"
    "   --chunk_algo=algo\n"
    "        set the chunking algorithm (FBC=FIXED BLOCK CHUNKING / CDC=CONTENT DEFINED CHUNKING)\n"
    "   --max-objs=count\n"
    "        --DEBUG ONLY-- maximum number of objs to read per-thread\n\n"

    "Reduce options:\n"
    "   --dedup-threshold=count\n"
    "        set the min count of repeating chunks needed to dedup a chunk\n"
    "   --shards-count\n"
    "        set the num of shards used in the hash-reduce step.\n"
    "        higher value means lower memory usage, lower value means longer run time\n\n";
}

//---------------------------------------------------------------------------
[[noreturn]] static void usage_exit()
{
  usage(cerr);
  exit(1);
}

//---------------------------------------------------------------------------
[[maybe_unused]]static int display_cluster_stats(Rados *p_rados)
{
  cluster_stat_t stats;
  int ret = p_rados->cluster_stat(stats);
  if (unlikely(ret < 0)) {
    cerr << "error getting total cluster usage: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  cout << std::endl;
  cout << "total_objects    " << stats.num_objects << std::endl;
  cout << "total_used       " << byte_u_t(stats.kb_used << 10)
       << std::endl;
  cout << "total_avail      " << byte_u_t(stats.kb_avail << 10)
       << std::endl;
  cout << "total_space      " << byte_u_t(stats.kb << 10) << std::endl;

  return 0;
}

//---------------------------------------------------------------------------
static int setup_rados_objects(Rados *p_rados, IoCtx *p_itr_ioctx, IoCtx *p_obj_ioctx, const char *pool_name)
{
  static bool first_time = true;
  bool print_success = false;
  if (first_time) {
    first_time = false;
    print_success = true;
    cout << "attmepting to connect to rados ..." << std::endl;
  }
  int ret = p_rados->init_with_context(g_ceph_context);
  if (unlikely(ret < 0)) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ret = p_rados->connect();
  if (unlikely(ret < 0)) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  //display_cluster_stats(*p_rados);

  ret = p_rados->ioctx_create(pool_name, *p_itr_ioctx);
  if (unlikely(ret < 0)) {
    cerr << "error itr_ioctx_create: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  p_itr_ioctx->set_namespace(all_nspaces);
  ret = p_rados->ioctx_create(pool_name, *p_obj_ioctx);
  if (unlikely(ret < 0)) {
    cerr << "error obj_ioctx_create: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  if (print_success) {
    cout << "rados.connect() was successful!" << std::endl;
  }
  return 0;
}


//=========================================================================
//=========================================================================
//=========================================================================
constexpr static uint32_t SHA1_SIZE = 20; // 160bits
struct chunk_fp_t
{
  constexpr static uint32_t FP_SIZE = SHA1_SIZE;
  static_assert(FP_SIZE % sizeof(uint32_t) == 0);
  static_assert(FP_SIZE >= sizeof(uint64_t));

  void copy_from_bl_itr(bufferlist::const_iterator &ci,
			uint32_t byte_count,
			uint8_t *p_dest ) {
    while (byte_count > 0) {
      const char *p_src = nullptr;
      size_t n = ci.get_ptr_and_advance(byte_count, &p_src);
      memcpy(p_dest, p_src, n);
      byte_count -= n;
      p_dest     += n;
    }
  }

  chunk_fp_t(bufferlist::const_iterator &ci) {
    uint32_t byte_count = FP_SIZE;
    uint8_t *p_dest     = fp_buff;
    copy_from_bl_itr(ci, byte_count, p_dest);
#ifndef DISABLE_CDC_CODE
    byte_count = sizeof(chunk_size);
    copy_from_bl_itr(ci, byte_count, (uint8_t*)&chunk_size);
    chunk_size = CEPHTOH_32(chunk_size);
#endif
  }

  const uint8_t* get_fp_buff() const {
    return fp_buff;
  }

  const uint32_t get_chunk_size() const {
    return chunk_size;
  }

  // TBD: When we calculate FP for the same data chunk will we get the same FP
  //      on little endian and big endian systems???
  unsigned get_shard(unsigned num_shards) {
    uint64_t val = *(uint64_t*)fp_buff;
    val = CEPHTOH_64(val);
    return val % num_shards;
  }

private:
  uint8_t  fp_buff[FP_SIZE];
  uint32_t chunk_size;

public:
  struct KeyHash
  {
    // No need to change the endianness since it will be internally consistent
    std::size_t operator()(const chunk_fp_t& k) const
    {
      // The SHA is already a hashing function so no need for another hash
      return *(uint64_t*)(k.get_fp_buff());
    }
  };

  struct KeyEqual
  {
    bool operator()(const chunk_fp_t& lhs, const chunk_fp_t& rhs) const
    {
      return ((memcmp(lhs.get_fp_buff(), rhs.get_fp_buff(), lhs.FP_SIZE) == 0) &&
	      (lhs.chunk_size == rhs.chunk_size));
    }
  };
} __attribute__((__packed__));

std::ostream &operator<<(std::ostream &stream, const chunk_fp_t & k)
{
  stream << "[" << k.get_chunk_size() << "]" << std::hex << " 0x";
  for (unsigned i = 0; i < k.FP_SIZE; i++) {
    stream << std::hex << k.get_fp_buff()[i];
  }
  return stream;
}

static_assert(sizeof(chunk_fp_t) == SHA1_SIZE + sizeof(uint32_t));
using FP_Dict = std::unordered_map<chunk_fp_t, uint32_t, chunk_fp_t::KeyHash, chunk_fp_t::KeyEqual>;

//---------------------------------------------------------------------------
[[maybe_unused]]static void print_report_reduce(const FP_Dict &fp_dict,
						bool list,
						const dedup_params_t &params)
{
  constexpr unsigned ARR_SIZE = (64*1024);
  std::array<uint32_t, ARR_SIZE+1> summery;
  for (unsigned idx = 0; idx <= ARR_SIZE; idx ++) {
    summery[idx] = 0;
  }
  uint64_t duplicated_data = 0, duplicated_data_chunks = 0;
  uint64_t unique_data     = 0, unique_data_chunks = 0;

  for (auto const& entry : fp_dict) {
    const unsigned count = entry.second;
#ifndef DISABLE_CDC_CODE
    const chunk_fp_t & key = entry.first;
    unsigned chunk_size = key.get_chunk_size();
#else
    unsigned chunk_size = params.chunk_size;
#endif
    unique_data     += chunk_size;
    duplicated_data += (count-1)*chunk_size;

    unique_data_chunks++;
    duplicated_data_chunks += (count-1);
    if (count < ARR_SIZE) {
      summery[count]++;
    }
    else {
      summery[ARR_SIZE]++;
    }
  }

  unsigned sample_rate = 1;
  duplicated_data *= sample_rate;
  unique_data     *= sample_rate;
  std::cout << "unique_data_chunks     = " << unique_data_chunks     << std::endl;
  std::cout << "duplicated_data_chunks = " << duplicated_data_chunks << std::endl;
  uint64_t total_size = duplicated_data + unique_data;
  std::cout << "============================================================" << std::endl;
#if 1
  std::cout << "We had a total of " << total_size/1024 << " KiB stored in the system\n";
  std::cout << "We had " << unique_data/1024 << " Unique Data KiB stored in the system\n";
  std::cout << "We had " << duplicated_data/1024 << " Duplicated KiB Bytes stored in the system\n";
  if (unique_data) {
    std::cout << "Dedup Ratio = " << (double)total_size/(double)unique_data << std::endl;
  }
#endif
  if (list) {
    for (unsigned idx = 0; idx < ARR_SIZE; idx ++) {
      if (summery[idx] > 0){
	std::cout << "We had " << std::setw(8) << summery[idx] << " keys with " << idx << " repetitions" << std::endl;
      }
    }
    if (summery[ARR_SIZE] > 0) {
      std::cout << "We had " << std::setw(8) << summery[ARR_SIZE] << " keys with more than " << ARR_SIZE << " repetitions " << std::endl;
    }
  }
  cout << "===========================================================" << std::endl;
}

//---------------------------------------------------------------------------
[[maybe_unused]]static void print_report_merged(const FP_Dict &fp_dict, const dedup_params_t &params)
{
  constexpr unsigned ARR_SIZE = (64*1024);
  std::array<uint32_t, ARR_SIZE+1> summery;
  for (unsigned idx = 0; idx <= ARR_SIZE; idx ++) {
    summery[idx] = 0;
  }
  uint64_t duplicated_data = 0, duplicated_data_chunks = 0;
  uint64_t unique_data     = 0, unique_data_chunks = 0;
  for (auto const& entry : fp_dict) {
    const unsigned count = entry.second;
#ifndef DISABLE_CDC_CODE
    const chunk_fp_t & key = entry.first;
    unsigned chunk_size = key.get_chunk_size();
#else
    unsigned chunk_size = params.chunk_size;
#endif
    unique_data     += chunk_size;
    duplicated_data += (count-1)*chunk_size;
    ceph_assert(count == 1);
    unique_data_chunks++;
    duplicated_data_chunks += (count-1);
    if (count < ARR_SIZE) {
      summery[count]++;
    }
    else {
      summery[ARR_SIZE]++;
    }
  }

  std::cout << __func__ << "::After Merge-Reduce" << std::endl;
  std::cout << "We got " << unique_data_chunks << " data chunks with "
	    << params.dedup_threshold << " or more duplications" << std::endl;
  if (duplicated_data_chunks) {
    std::cout << "\n*** duplicated_data_chunks = " << duplicated_data_chunks << "***\n" << std::endl;
  }
}

//---------------------------------------------------------------------------
[[maybe_unused]]static void print_hash_buffer(uint8_t *p, unsigned hash_size)
{
  for (unsigned i = 0; i < hash_size; i++) {
    printf("%02x", p[i]);
  }
  printf("\n");
}

//================================================================================
//                                   C O L L E C T
//================================================================================

const unsigned     FULL_OBJ_READ = (0xFFFFFFFF); // special marker indicating we process the full object
//constexpr unsigned RADOS_OBJ_SIZE  = (4 * 1024 * 1024);
constexpr unsigned RADOS_OBJ_SIZE  = (64 * 1024);
constexpr unsigned MAX_CURSOR_SIZE = 1024; // TBD - find out if that is enough
static_assert(MAX_CURSOR_SIZE % sizeof(uint64_t) == 0);
constexpr unsigned MAX_FP          = 32; // cap fingerprint size at 32 bytes
#define FP_HEADER_LEN (sizeof(FP_BUFFER::FP_BUFFER_HEADER))

struct FP_BUFFER {
  struct FP_BUFFER_HEADER {
    char     cursor[MAX_CURSOR_SIZE];
    uint16_t fp_size;
    uint16_t cursor_len;
    uint32_t fp_count;
    uint32_t offset_in_obj;
    uint32_t crc;

    DENC(FP_BUFFER_HEADER, v, p) {
      uint64_t *p_cur = (uint64_t*)v.cursor;
      uint64_t *p_end = (uint64_t*)(v.cursor + MAX_CURSOR_SIZE);
      for ( ; p_cur < p_end; p_cur++) {
	denc(*p_cur, p);
      }

      denc(v.fp_size, p);
      denc(v.cursor_len, p);
      denc(v.fp_count, p);
      denc(v.offset_in_obj, p);
      denc(v.crc, p);
    }

  };// __attribute__((__packed__)); // 1040 Bytes

  //---------------------------------------------------------------------------
  FP_BUFFER(unsigned fp_size,
	    IoCtx *obj_ioctx,
	    uint16_t thread_id,
	    bool verbose,
	    uint32_t part_number = 0) {
    d_byte_offset = FP_HEADER_LEN;
    d_fp_size     = fp_size;
    d_obj_ioctx   = obj_ioctx;
    d_thread_id   = thread_id;
    d_verbose     = verbose;
    d_part_number = part_number;
    assert(d_fp_size < MAX_FP);
  }

  //---------------------------------------------------------------------------
  int add_fp(const char *fp,
	     unsigned chunk_size,
	     const ObjectCursor &cursor,
	     uint32_t offset_in_obj) {
#ifndef DISABLE_CDC_CODE
    unsigned copy_size = sizeof(chunk_fp_t);
#else
    unsigned copy_size = d_fp_size;;
#endif
    if (unlikely(d_byte_offset+copy_size > sizeof(d_buffer))) {
      // the current chunk/fp are not part of this destage
      flush(cursor, offset_in_obj);
    }
    d_fp_count ++;
    memcpy(d_buffer+d_byte_offset, fp, d_fp_size);
    d_byte_offset += d_fp_size;
#ifndef DISABLE_CDC_CODE
    *(uint32_t*)(d_buffer+d_byte_offset) = HTOCEPH_32(chunk_size);
    d_byte_offset += sizeof(chunk_size);
#endif
    return 0;
  }

  //---------------------------------------------------------------------------
  int flush(const ObjectCursor &cursor, uint32_t offset_in_object)
  {
    struct FP_BUFFER_HEADER header;
    header.fp_size    = HTOCEPH_16(d_fp_size);
    uint16_t len      = cursor.to_str().copy(header.cursor, sizeof(header.cursor));
    header.cursor_len = HTOCEPH_16(len);
#ifndef DISABLE_CDC_CODE
    header.fp_count = HTOCEPH_32((d_byte_offset - FP_HEADER_LEN) / sizeof(chunk_fp_t));
#else
    header.fp_count = HTOCEPH_32((d_byte_offset - FP_HEADER_LEN) / d_fp_size);
#endif
    header.offset_in_obj = HTOCEPH_32(offset_in_object);
    header.crc = 0;
    memcpy(d_buffer, (uint8_t*)&header, FP_HEADER_LEN);
    uint32_t crc = -1;
    crc = ceph_crc32c(crc, d_buffer, d_byte_offset);
    ((FP_BUFFER_HEADER*)d_buffer)->crc = HTOCEPH_32(crc);

    ceph_assert(header.fp_count == d_fp_count);
    char name_buf[HASH_OID_NAME_SIZE];
    // make sure thread_id will fit in 2 hex digits
    static_assert(THREAD_LIMIT_STRICT < 0xFF);
    unsigned n = snprintf(name_buf, sizeof(name_buf),
			  HASH_FORMAT, d_thread_id, d_part_number);
    std::string oid(name_buf, n);
    if (d_verbose) {
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << __func__ << "::" << d_obj_destage << "::oid=" << oid
	   << ", cursor=" << cursor
	   << ", offset_in_object=" << offset_in_object
	   << ", fp_count=" << header.fp_count
	   << " , size=" << d_byte_offset << std::endl;
    }
    bufferlist bl = bufferlist::static_from_mem((char*)d_buffer, d_byte_offset);
    d_obj_ioctx->set_namespace(RAW_FP_NSPACE);
    d_obj_ioctx->write_full(oid, bl);
    d_obj_destage ++;
    d_byte_offset = FP_HEADER_LEN;
    d_fp_count = 0;
    d_part_number++;
    return 0;
  }

private:
  uint8_t  d_buffer[RADOS_OBJ_SIZE];
  IoCtx   *d_obj_ioctx;
  uint32_t d_byte_offset;
  uint32_t d_part_number;
  uint16_t d_fp_size;
  uint16_t d_thread_id;
  bool     d_verbose = false;
  uint32_t d_obj_destage = 0;
  uint32_t d_fp_count = 0;
};
WRITE_CLASS_DENC(FP_BUFFER::FP_BUFFER_HEADER)
//---------------------------------------------------------------------------
static void calc_and_write_fp(FP_BUFFER *fp_buffer,
			      const uint8_t *p,
			      unsigned chunk_size,
			      const ObjectCursor &cursor,
			      uint32_t offset_in_obj)
{
  sha1_digest_t sha1 = ceph::crypto::digest<ceph::crypto::SHA1>(p, chunk_size);
  fp_buffer->add_fp((const char*)(sha1.v), chunk_size, cursor, offset_in_obj);
}

//---------------------------------------------------------------------------
static int calculate_fp_fbc(FP_BUFFER *fp_buffer,
			    bufferlist &bl,
			    unsigned chunk_size,
			    const ObjectCursor &cursor,
			    uint32_t offset_in_obj,
			    bool verbose)
{
  unsigned fp_count = 0;
  uint8_t temp[chunk_size];
  auto bl_itr = bl.cbegin();
  for (unsigned i = 0; i < bl.length() / chunk_size; i++, offset_in_obj+=chunk_size) {
    const char *p_src = nullptr;
    size_t n = bl_itr.get_ptr_and_advance(chunk_size, &p_src);
    if (n == chunk_size) {
      calc_and_write_fp(fp_buffer, (uint8_t*)p_src, chunk_size, cursor, offset_in_obj);
      fp_count++;
    }
    else {
      // TBD - How to test this path?
      // It requires that rados.read() will allocate bufferlist in small chunks
      cout << __func__ << "::***************************************::" << std::endl;

      memcpy(temp, p_src, n);
      uint32_t count  = chunk_size - n;
      uint8_t *p_dest = temp + n;
      do {
	n = bl_itr.get_ptr_and_advance(count, &p_src);
	memcpy(p_dest, p_src, n);
	count  -= n;
	p_dest += n;
      } while (count > 0);
      calc_and_write_fp(fp_buffer, temp, chunk_size, cursor, offset_in_obj);
      fp_count++;
    }
  }
  //ceph_assert(fp_count == bl.length() / chunk_size);
  return fp_count;
}

//---------------------------------------------------------------------------
static int calculate_fp_cdc(FP_BUFFER *fp_buffer,
			    bufferlist &bl,
			    unsigned chunk_size,
			    const ObjectCursor &cursor,
			    uint32_t offset_in_obj,
			    bool verbose)
{
  unsigned fp_count = 0;
  constexpr unsigned MAX_CHUNK_SIZE = 256*1024;
  uint8_t temp[MAX_CHUNK_SIZE];
  auto bl_itr = bl.cbegin();

  std::vector<std::pair<uint64_t, uint64_t>> chunks;
  std::string chunk_algo = "fastcdc";
  // TBD
  // !!!!!!
  // skip offset_in_obj bytes ....
  //
  ceph_abort("TBD::skip offset_in_obj bytes");
  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  cdc->calc_chunks(bl, &chunks);
  for (auto &p : chunks) {
    unsigned chunk_size = p.second;
    const char *p_src = nullptr;
    size_t n = bl_itr.get_ptr_and_advance(chunk_size, &p_src);
    if (n == chunk_size) {
      calc_and_write_fp(fp_buffer, (uint8_t*)p_src, chunk_size, cursor, offset_in_obj);
      fp_count++;
    }
    else {
      // TBD - How to test this path?
      // It requires that rados.read() will allocate bufferlist in small chunks
      cout << __func__ << "::***************************************::" << std::endl;

      memcpy(temp, p_src, n);
      uint32_t count  = chunk_size - n;
      uint8_t *p_dest = temp + n;
      do {
	n = bl_itr.get_ptr_and_advance(count, &p_src);
	memcpy(p_dest, p_src, n);
	count  -= n;
	p_dest += n;
      } while (count > 0);
      calc_and_write_fp(fp_buffer, temp, chunk_size, cursor, offset_in_obj);
      fp_count++;
    }
    offset_in_obj += chunk_size;
  }

  return fp_count;
}

//---------------------------------------------------------------------------
static void move_itr_by_cursor(NObjectIterator &start_itr,
			       const std::string *cursor,
			       uint32_t *offset_in_obj,
			       bool verbose)
{
  ObjectCursor delimiter;
  if (delimiter.from_str(*cursor)) {
    unsigned count = 0;
    for ( ; start_itr.get_cursor() < delimiter; start_itr++) {
      count++;
      if(verbose) {
	string oid = start_itr->get_oid();
	std::unique_lock<std::mutex> lock(print_mtx);
	std::cout << "|||" << start_itr.get_cursor() << " || " << oid << std::endl;
      }
    }
    if(verbose) {
      std::cout << "skipped " << count << " objects before cursor: " << delimiter << std::endl;
    }
  }
  else {
    std::cerr << __func__ << "::bad cursor " << *cursor << std::endl;
    ceph_abort("bad cursor");
  }
  if (start_itr.get_cursor() == delimiter) {
    if (*offset_in_obj == FULL_OBJ_READ) {
      if (verbose) {
	std::cout << "skipping oid: " << start_itr->get_oid()
		  << ", cursor: " << start_itr.get_cursor() << std::endl;
      }
      start_itr++;
      *offset_in_obj = 0;
    }
    else {
      if (verbose) {
	std::cout << "start oid: " << start_itr->get_oid()
		  << ", offset_in_obj: " << *offset_in_obj << std::endl;
      }
    }
  }
  if (verbose) {
    std::cout << "cursor=" << start_itr.get_cursor() << " || " << *cursor << std::endl;
  }
}

//---------------------------------------------------------------------------
static int thread_collect(unsigned thread_id,
			  uint64_t *p_bytes_read,
			  const std::string *cursor,
			  uint64_t start_part_num,
			  uint32_t offset_in_obj,
			  const dedup_params_t &params)
{
  uint64_t objs_read = 0, min_size = 0xFFFFFFFF, max_size = 0;
  unsigned max_objs = params.max_objs;
  std::vector<std::string> oid_vec;
  *p_bytes_read = 0;
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  const char *pool_name = params.pool_name.c_str();
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name) != 0) {
    return -1;
  }

  unique_ptr<FP_BUFFER> fp_buffer = std::make_unique<FP_BUFFER>(
    sha1_digest_t::SIZE, &obj_ioctx, thread_id, params.verbose, start_part_num);

  ObjectCursor split_start, split_finish;
  itr_ioctx.object_list_slice(itr_ioctx.object_list_begin(),
			      itr_ioctx.object_list_end(),
			      thread_id,
			      params.max_threads,
			      &split_start,
			      &split_finish);
  auto start_itr = itr_ioctx.nobjects_begin(split_start);
  //start_itr.set_cursor(itr.get_cursor());
  uint64_t fp_count = 0, fp_count_total = 0;;
  uint64_t obj_size = 0;
  utime_t total_time, max_time, min_time = utime_t::max();

  if (cursor) {
    move_itr_by_cursor(start_itr, cursor, &offset_in_obj, params.verbose);
  }

  unsigned skip_count = 0;
  for (auto itr = start_itr; itr.get_cursor() < split_finish; ++itr) {
    string oid = itr->get_oid();
    if (itr->get_nspace() == RAW_FP_NSPACE) {
      skip_count++;
      if(params.verbose) {
	std::unique_lock<std::mutex> lock(print_mtx);
	std::cout << skip_count << "] skip oid " << oid << std::endl;
      }
      continue;
    }
    if(params.verbose) {
      std::unique_lock<std::mutex> lock(print_mtx);
      std::cout << itr.get_cursor() << " || " << oid << std::endl;
    }
    oid_vec.push_back(oid);
    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());
    bufferlist bl;
    utime_t start_time = ceph_clock_now();
    // 0 @len means read until the end
    int ret = obj_ioctx.read(oid, bl, 0, offset_in_obj);
    utime_t duration = ceph_clock_now() - start_time;
    if (unlikely(ret <= 0)) {
      cerr << "failed to read file: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    total_time += duration;
    min_time = std::min(min_time, duration);
    max_time = std::max(max_time, duration);

    obj_size = bl.length();
    objs_read++;
    *p_bytes_read += bl.length();
    min_size = std::min(min_size, (uint64_t)bl.length());
    max_size = std::max(max_size, (uint64_t)bl.length());
    if (params.chunk_algo == CHUNK_ALGO_FBC) {
      fp_count = calculate_fp_fbc(fp_buffer.get(), bl, params.chunk_size,
				  itr.get_cursor(), offset_in_obj, params.verbose);
    }
    else {
      assert(params.chunk_algo == CHUNK_ALGO_CDC);
      fp_count = calculate_fp_cdc(fp_buffer.get(), bl, params.chunk_size,
				  itr.get_cursor(), offset_in_obj, params.verbose);
    }
    ceph_assert(fp_count == obj_size / params.chunk_size || offset_in_obj != 0);
    offset_in_obj = 0;
    fp_count_total += fp_count;
    if (unlikely(max_objs && objs_read >= max_objs)) {
      //fp_buffer->flush(itr.get_cursor());
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << std::dec << thread_id << "]DEBUG EXIT::Num Objects read=" << objs_read << std::endl;
      return 0;
    }
  }

  fp_buffer->flush(split_finish, FULL_OBJ_READ);
  if (objs_read > 0) {
    std::unique_lock<std::mutex> lock(print_mtx);
    if (params.verbose) {
      cout << thread_id << " Num Objects read=" << objs_read
	   << ", Num Bytes read=" << std::setw(4)
	   << *p_bytes_read/(1024*1024) << " MiB"
	   << ", Max-Size=" << max_size / 1024 << " KiB"
	   << ", Min-Size=" << min_size / 1024 << " KiB"
	   << ", chunks-written=" << fp_count_total
	   << std::endl;
      cout << thread_id
	   << "] Total-Time=" << total_time
	   << ", Max-Time=" << max_time
	   << ", Min-Time=" << min_time
	   << ", Avg-Time=" << total_time/objs_read
	   << std::endl;
#if 0
      for (auto const & oid : oid_vec) {
	std::cout << oid << std::endl;
      }
#endif
    }
    else {
      cout << std::dec << thread_id << "]Num Objects read=" << objs_read << std::endl;
    }
  }

  return 0;
}

//---------------------------------------------------------------------------
static int collect_fingerprints(uint64_t *p_total_bytes_read, const dedup_params_t &params)
{
  cout << "collect chunks finger prints" << std::endl;
  if (params.chunk_algo != CHUNK_ALGO_FBC && params.chunk_algo != CHUNK_ALGO_CDC) {
    cerr << "** illegal chunking algorithm [" << params.chunk_algo << "] **" << std::endl;
    usage_exit();
  }

  unsigned max_threads = params.max_threads;
  std::cout << "max_threads = " << max_threads << std::endl;

  std::thread* thread_arr[max_threads];
  memset(thread_arr, 0, sizeof(thread_arr));

  uint64_t bytes_read_arr[max_threads];
  memset(bytes_read_arr, 0, sizeof(bytes_read_arr));

  for (unsigned thread_id = 0; thread_id < max_threads; thread_id++) {
    thread_arr[thread_id] = new std::thread(thread_collect, thread_id,
					    bytes_read_arr+thread_id, nullptr, 0, 0, params);
  }

  for (unsigned thread_id = 0; thread_id < max_threads; thread_id++ ) {
    if( thread_arr[thread_id] != nullptr ) {
      thread_arr[thread_id]->join();
      delete thread_arr[thread_id];
      *p_total_bytes_read += bytes_read_arr[thread_id];
      thread_arr[thread_id] = nullptr;
    }
  }
  cout << "Total size before dedup = " << *p_total_bytes_read / (1024 * 1024) << " MiB" << std::endl;
  return 0;
}

//================================================================================
//                                   R E D U C E
//================================================================================

struct REDUCED_FP_HEADER {
  uint16_t fp_size;
  uint16_t shard_id;
  uint16_t threshold;
  uint16_t pad16;

  uint32_t fp_count;
  uint32_t serial_id;
  uint32_t pad32;
  uint32_t crc;


  DENC(REDUCED_FP_HEADER, v, p) {
    denc(v.fp_size, p);
    denc(v.shard_id, p);
    denc(v.threshold, p);
    denc(v.pad16, p);

    denc(v.fp_count, p);
    denc(v.serial_id, p);
    denc(v.pad32, p);
    denc(v.crc, p);
  }
}; // 24 Bytes header
WRITE_CLASS_DENC(REDUCED_FP_HEADER)

std::ostream &operator<<(std::ostream &stream, const REDUCED_FP_HEADER & rfh)
{
  stream << "fp_size   = " << rfh.fp_size   << "\n"
	 << "shard_id  = " << rfh.shard_id  << "\n"
	 << "threshold = " << rfh.threshold << "\n"
	 << "fp_count  = " << rfh.fp_count  << "\n"
	 << "serial_id = " << rfh.serial_id << "\n"
	 << "crc       = " << rfh.crc << std::endl;

  return stream;
}

//---------------------------------------------------------------------------
static void reduced_fp_flush(REDUCED_FP_HEADER header,
			     uint8_t  buffer[],
			     uint32_t offset,
			     uint32_t fp_size,
			     uint32_t shard_id,
			     uint32_t serial_id,
			     IoCtx   &obj_ioctx)
{
#ifndef DISABLE_CDC_CODE
  header.fp_count  = HTOCEPH_32((offset - sizeof(REDUCED_FP_HEADER)) / sizeof(chunk_fp_t));
#else
  header.fp_count  = HTOCEPH_32((offset - sizeof(REDUCED_FP_HEADER)) / fp_size);
#endif
  header.serial_id = HTOCEPH_32(serial_id);
  header.crc       = 0;
  memcpy(buffer, (uint8_t*)&header, sizeof(REDUCED_FP_HEADER));
  uint32_t crc = -1;
  crc = ceph_crc32c(crc, buffer, offset);
  ((REDUCED_FP_HEADER*)buffer)->crc = HTOCEPH_32(crc);
  if (0) {
    std::unique_lock<std::mutex> lock(print_mtx);
    cout << __func__ << ", byte_offset=" << offset
	 << ", crc=0x" << hex << crc << dec << std::endl;
  }

  char name_buf[16];
  // TBD: allow 0xFF server-shards each with 0xFF internal shards
  // make sure shard_id will fit in 3 hex digits
  static_assert(SHARDS_COUNT_MAX < 0xFFFF);
  unsigned n = snprintf(name_buf, sizeof(name_buf),
			"rc%04X.%08X", shard_id, serial_id);
  std::string oid(name_buf, n);

  bufferlist bl = bufferlist::static_from_mem((char*)buffer, offset);
  obj_ioctx.set_namespace(REDUCED_FP_NSPACE);
  obj_ioctx.write_full(oid, bl);
}

//---------------------------------------------------------------------------
static int store_reduced_fingerprints(const FP_Dict &fp_dict,
				      unsigned shard_id,
				      uint64_t *p_reduced_bytes,
				      IoCtx &obj_ioctx,
				      const dedup_params_t &params)
{
  uint8_t buffer[RADOS_OBJ_SIZE];
  struct REDUCED_FP_HEADER header;
  unsigned fp_size = chunk_fp_t::FP_SIZE; // TBD - use macro
  // fixed parts of the header which can be reuse
  header.fp_size   = HTOCEPH_16(fp_size);
  header.shard_id  = HTOCEPH_16(shard_id);
  header.threshold = HTOCEPH_16(params.dedup_threshold);
  header.pad16     = 0;
  header.pad32     = 0;
#ifndef DISABLE_CDC_CODE
  unsigned key_size = sizeof(chunk_fp_t);
#else
  unsigned key_size = fp_size;
#endif
  uint32_t serial_id = 0;
  unsigned offset    = sizeof(REDUCED_FP_HEADER);
  for (auto const& entry : fp_dict) {
    const chunk_fp_t & key = entry.first;
    const unsigned count = entry.second;
    if (count >= params.dedup_threshold) {
      if (unlikely(offset+key_size >= sizeof(buffer))) {
	reduced_fp_flush(header, buffer, offset, fp_size, shard_id, serial_id++, obj_ioctx);
	offset = sizeof(REDUCED_FP_HEADER);
      }
      memcpy(buffer+offset, key.get_fp_buff(), fp_size);
      offset += fp_size;
#ifndef DISABLE_CDC_CODE
      *(uint32_t*)(buffer+offset) = HTOCEPH_32(key.get_chunk_size());
      offset += sizeof(key.get_chunk_size());
#endif

      //file.write((const char*)(key.get_fp_buff()), key.FP_SIZE);
      *p_reduced_bytes += (count-1)*(key.get_chunk_size());
    }
  }

  if (offset > 0) {
    reduced_fp_flush(header, buffer, offset, fp_size, shard_id, serial_id++, obj_ioctx);
  }

  return 0;
}

//---------------------------------------------------------------------------
static int reduce_fingerprints_single_object(FP_Dict &fp_dict,
					     bufferlist &bl,
					     unsigned shard_id,
					     [[maybe_unused]]const ObjectCursor &cursor,
					     const dedup_params_t &params)
{
  uint64_t uniq_cnt = 0;
  uint64_t cnt = 0;
  FP_BUFFER::FP_BUFFER_HEADER fp_header;
  auto bl_itr = bl.cbegin();
  decode(fp_header, bl_itr);

  if (params.verbose) {
    std::unique_lock<std::mutex> lock(print_mtx);
    std::string_view cursor(fp_header.cursor, fp_header.cursor_len);
    cout << __func__ << "::" << ", cursor=" << cursor
	 << ", offset_in_object=" << fp_header.offset_in_obj
	 << ", fp_count=" << fp_header.fp_count << std::endl;
  }

#if 0
  uint32_t buffer_crc = header.crc;
  header.crc = 0;
  uint32_t calc_crc = ceph_crc32c(-1, buffer, data_size);
  //cout << "buffer_crc=" << hex << buffer_crc << ", calc_crc=" << calc_crc << dec << std::endl;
  assert(calc_crc == buffer_crc);
#endif
  unsigned fp_count = 0;
  for (unsigned fp_idx = 0; fp_idx < fp_header.fp_count; fp_idx++) {
    chunk_fp_t key(bl_itr);
    ceph_assert(key.get_shard(params.num_shards) == shard_id);
    if (key.get_shard(params.num_shards) == shard_id) {
      fp_count ++;
      auto dict_itr = fp_dict.find(key);
      if (dict_itr == fp_dict.end()) {
	fp_dict[key] = 1;
	uniq_cnt ++;
      }
      else {
	dict_itr->second ++;
      }
      cnt++;
    }
  }
  return fp_count;
}

//---------------------------------------------------------------------------
static int reduce_shard_fingerprints(unsigned shard_id,
				     uint64_t *p_reduced_bytes,
				     const dedup_params_t &params)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }
  itr_ioctx.set_namespace(RAW_FP_NSPACE);
  FP_Dict fp_dict;
  uint64_t count, objs_read = 0, chunks_read = 0;
  uint64_t min_size = 0xFFFFFFFF, max_size = 0;
  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with(HASH_PREFIX));
    if(params.verbose) {
      std::unique_lock<std::mutex> lock(print_mtx);
      std::cout << itr.get_cursor() << " || " << oid << std::endl;
    }

    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());
    bufferlist bl;
    int ret = obj_ioctx.read_full(oid, bl);
    if (unlikely(ret <= 0)) {
      cerr << "failed to read object " << oid << ", err: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    objs_read++;
    count = reduce_fingerprints_single_object(fp_dict, bl, shard_id, itr.get_cursor(), params);
    min_size = std::min(min_size, count);
    max_size = std::max(max_size, count);
    chunks_read += count;
  }
  if (params.num_shards == 1) {
    cout << __func__ << "::single shard mode" << std::endl;
    cout << "Num Objects read=" << objs_read
	 << ", Max-FP=" << max_size
	 << ", Min-FP=" << min_size
	 << ", chunks-read=" << chunks_read
	 << std::endl;

    print_report_reduce(fp_dict, true, params);
  }
  store_reduced_fingerprints(fp_dict, shard_id, p_reduced_bytes, obj_ioctx, params);
  return 0;
}

//---------------------------------------------------------------------------
static int reduce_fingerprints(uint64_t *p_reduced_bytes, const dedup_params_t &params)
{
  cout << "reduce fingerprints, leaving only FP with "
       << params.dedup_threshold << " or more duplicates" << std::endl;
  for (unsigned shard_id = 0; shard_id < params.num_shards; shard_id++) {
    reduce_shard_fingerprints(shard_id, p_reduced_bytes, params);
  }

  return 0;
}

//================================================================================
//                                   M E R G E
//================================================================================

//---------------------------------------------------------------------------
static int merge_fingerprints_single_object(FP_Dict &fp_dict,
					    bufferlist &bl,
					    const dedup_params_t &params)
{
  uint64_t uniq_cnt    = 0;
  uint64_t cnt         = 0;
  REDUCED_FP_HEADER header;
  bufferlist::const_iterator bl_itr = bl.cbegin();
  decode(header, bl_itr);
  assert(header.pad16 == 0 && header.pad32 == 0);

#if 0
  uint32_t buffer_crc = header.crc;
  header.crc = 0;
  uint32_t calc_crc = ceph_crc32c(-1, buffer, data_size);
  //cout << "buffer_crc=" << hex << buffer_crc << ", calc_crc=" << calc_crc << dec << std::endl;
  assert(calc_crc == buffer_crc);
#endif

  for (unsigned fp_idx = 0; fp_idx < header.fp_count; fp_idx++) {
    chunk_fp_t key(bl_itr);
    auto itr = fp_dict.find(key);
    if (itr == fp_dict.end()) {
      fp_dict[key] = 1;
      uniq_cnt ++;
    }
    else {
      itr->second ++;
    }
    cnt++;
  }

  return 0;
}

//---------------------------------------------------------------------------
static int merge_reduced_fingerprints(const dedup_params_t &params)
{
  cout << "Merged the reduce fingerprints into a single table" << std::endl;
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }
  itr_ioctx.set_namespace(REDUCED_FP_NSPACE);
  FP_Dict fp_dict;
  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with("rc"));

    if (params.verbose ) {
      cout << oid << std::endl;
    }
    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());
    bufferlist bl;
    int ret = obj_ioctx.read_full(oid, bl);
    if (unlikely(ret <= 0)) {
      cerr << "failed to read file: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    merge_fingerprints_single_object(fp_dict, bl, params);
  }

  //store_merged_reduced_fingerprints(params, fp_dict, shard_id);
  print_report_merged(fp_dict, params);

  return 0;
}

//===========================================================================

//---------------------------------------------------------------------------
static int parse_hash_object_name(const string &oid,
				  uint16_t *p_thread_id,
				  uint32_t *p_part_num,
				  bool verbose)
{
  // convert CPP string to c style string without dynamic allocation
  size_t size = oid.length();
  char buff[size + 1];
  strncpy(buff, oid.data(), size);
  buff[size] = '\0';

  sscanf(buff, HASH_FORMAT, p_thread_id, p_part_num);
  if (verbose) {
    cout << std::hex << "thread_id=" << *p_thread_id
	 << ", part_number=" << *p_part_num << std::endl;
  }
  return 0;
}

//---------------------------------------------------------------------------
static void find_max_part_number_per_thread(IoCtx *p_itr_ioctx,
					    uint32_t *max_part_num_table,
					    uint16_t *p_max_thread_id,
					    bool verbose)
{
  p_itr_ioctx->set_namespace(RAW_FP_NSPACE);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();

  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (verbose ) {
      cout << __func__ << "::oid = " << oid << std::endl;
    }
    ceph_assert(oid.starts_with(HASH_PREFIX));

    uint16_t thread_id;
    uint32_t part_num;
    parse_hash_object_name(oid, &thread_id, &part_num, verbose);
    ceph_assert(thread_id < THREAD_LIMIT_STRICT);
    *p_max_thread_id = std::max(*p_max_thread_id, thread_id);
    max_part_num_table[thread_id] = std::max(max_part_num_table[thread_id], part_num);
  }
}

//---------------------------------------------------------------------------
static int resume_thread_collect(uint16_t thread_id,
				 uint16_t last_part_num,
				 IoCtx *p_obj_ioctx,
				 uint64_t *p_bytes_read,
				 const dedup_params_t &params)
{
  char name_buf[HASH_OID_NAME_SIZE];
  unsigned n = snprintf(name_buf, sizeof(name_buf), HASH_FORMAT,
			thread_id, last_part_num);
  std::string oid(name_buf, n);
  if (params.verbose ) {
    std::cout << "recovering cursor from oid: " << oid << std::endl;
  }

  bufferlist bl;
  // Read only the HEADER section from the object
  int ret = p_obj_ioctx->read(oid, bl, FP_HEADER_LEN, 0);
  if (ret != FP_HEADER_LEN) {
    std::cerr << __func__ << "::failed obj_ioctx.read(" << oid << ")"
	      << ", error = " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  FP_BUFFER::FP_BUFFER_HEADER fp_header;
  auto bl_itr = bl.cbegin();
  decode(fp_header, bl_itr);

  std::string cursor(fp_header.cursor, fp_header.cursor_len);
  if (params.verbose ) {
    cout << "cursor   = " << cursor << "\n" << std::dec
	 << "fp_size  = " << fp_header.fp_size  << "\n"
	 << "fp_count = " << fp_header.fp_count << "\n"
	 << "offset = " << fp_header.offset_in_obj << "\n";
  }

  return thread_collect(thread_id, p_bytes_read, &cursor, last_part_num+1,
			fp_header.offset_in_obj, params);
}

//---------------------------------------------------------------------------
static int recover_collect_fingerprints_step(IoCtx *p_itr_ioctx,
					     IoCtx *p_obj_ioctx,
					     uint64_t *p_total_bytes_read,
					     const dedup_params_t &params)
{
  uint32_t last_part_num_table[THREAD_LIMIT_STRICT];
  memset(last_part_num_table, 0x0, sizeof(last_part_num_table));
  uint16_t max_thread_id = 0;
  find_max_part_number_per_thread(p_itr_ioctx, last_part_num_table,
				  &max_thread_id, params.verbose);
  ceph_assert(max_thread_id < THREAD_LIMIT_STRICT);
  p_obj_ioctx->set_namespace(RAW_FP_NSPACE);

  std::thread* thread_arr[THREAD_LIMIT_STRICT];
  memset(thread_arr, 0, sizeof(thread_arr));

  uint64_t bytes_read_arr[THREAD_LIMIT_STRICT];
  memset(bytes_read_arr, 0, sizeof(bytes_read_arr));

  for (unsigned thread_id = 0; thread_id <= max_thread_id; thread_id++) {
    uint32_t last_part_num = last_part_num_table[thread_id];
    thread_arr[thread_id] = new std::thread(resume_thread_collect, thread_id, last_part_num,
					    p_obj_ioctx, bytes_read_arr+thread_id, params);
  }

  for (unsigned thread_id = 0; thread_id <= max_thread_id; thread_id++ ) {
    if( thread_arr[thread_id] != nullptr ) {
      thread_arr[thread_id]->join();
      delete thread_arr[thread_id];
      *p_total_bytes_read += bytes_read_arr[thread_id];
      thread_arr[thread_id] = nullptr;
    }
  }
  cout << "Total size before dedup = " << *p_total_bytes_read / (1024 * 1024) << " MiB" << std::endl;
  return 0;
}

//---------------------------------------------------------------------------
static int recover_reduce_fingerprints_step(IoCtx *p_itr_ioctx,
					    IoCtx *p_obj_ioctx,
					    const dedup_params_t &params)
{
  return 0;
}

//---------------------------------------------------------------------------
static bool namespace_exists(IoCtx *p_itr_ioctx, const string &nspace, bool verbose)
{
  p_itr_ioctx->set_namespace(nspace);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with(HASH_PREFIX) || oid.starts_with("rc"));

    if (verbose ) {
      cout << __func__ << "::first oid = " << oid << std::endl;
    }

    return true;
  }

  return 0;
}

//---------------------------------------------------------------------------
static int resume_operation(const dedup_params_t &params)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }

  // check if we have any work done from before
  if (namespace_exists(&itr_ioctx, RAW_FP_NSPACE, params.verbose) == false) {
    // nothing was done, start from the beginning
    run_full_scan(params);
    return 0;
  }


  // check if the collect-fingerprints step was completed
  if (namespace_exists(&itr_ioctx, REDUCED_FP_NSPACE, params.verbose) == false) {
    uint64_t total_bytes_before = 0;
    uint64_t reduced_bytes = 0;
    recover_collect_fingerprints_step(&itr_ioctx, &obj_ioctx, &total_bytes_before, params);
    reduce_fingerprints(&reduced_bytes, params);
    merge_reduced_fingerprints(params);
    return 0;
  }

  return recover_reduce_fingerprints_step(&itr_ioctx, &obj_ioctx, params);
}

//===========================================================================
//---------------------------------------------------------------------------
static int clean_namespace(IoCtx *p_itr_ioctx,
			   IoCtx *p_obj_ioctx,
			   const string &nspace,
			   bool verbose)
{
  p_itr_ioctx->set_namespace(nspace);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with(HASH_PREFIX) || oid.starts_with("rc"));

    if (verbose ) {
      cout << "removing " << oid << std::endl;
    }
    p_obj_ioctx->set_namespace(itr->get_nspace());
    p_obj_ioctx->locator_set_key(itr->get_locator());
    int ret = p_obj_ioctx->remove(oid);
    if (ret != 0) {
      cerr << "failed to remove obj: " << oid
	   << ", error = " << cpp_strerror(ret) << std::endl;
      return ret;
    }
  }

  return 0;
}

//---------------------------------------------------------------------------
static int clean_intermediate_objs(const string &pool_name, bool verbose)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name.c_str()) != 0) {
    return -1;
  }
  if (clean_namespace(&itr_ioctx, &obj_ioctx, RAW_FP_NSPACE, verbose) != 0) {
    return -1;
  }
  return clean_namespace(&itr_ioctx, &obj_ioctx, REDUCED_FP_NSPACE, verbose);
}

//---------------------------------------------------------------------------
static void run_full_scan(const dedup_params_t &params)
{
  uint64_t total_bytes_before = 0;
  uint64_t reduced_bytes = 0;

  clean_intermediate_objs(params.pool_name, params.verbose);
  collect_fingerprints(&total_bytes_before, params);
  reduce_fingerprints(&reduced_bytes, params);
  merge_reduced_fingerprints(params);
  uint64_t bytes_after_dedup = total_bytes_before-reduced_bytes;
  if (bytes_after_dedup) {
    cout << "size before dedup = " << (double)total_bytes_before / (1024*1024)
	 << " MiB, size after dedup = "
	 << (double)bytes_after_dedup / (1024*1024)  << " MiB" << std::endl;
    std::cout << "Unique Count = " << bytes_after_dedup/params.chunk_size
	      << ", Dup Count = " << reduced_bytes/params.chunk_size << std::endl;
    cout << "dedup ratio = "
	 << ((double)total_bytes_before/(total_bytes_before-reduced_bytes)) << std::endl;
  }
}

//================================================================================
//                                   I N I T
//================================================================================

//---------------------------------------------------------------------------
static void process_arguments(vector<const char*> &args, map<string, string> &opts, dedup_params_t &params)
{
  std::string val;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "--verbose", (char*)NULL)) {
      cout << "set verbose mode" << std::endl;
      params.verbose = true;
    }else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      opts["pool"] = val;
      params.pool_name =  val;
    } else if (ceph_argparse_witharg(args, i, &val, "--pgid", (char*)NULL)) {
      opts["pgid"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--input-file", (char*)NULL)) {
      opts["input_file"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-objs", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0) {
	cout << "Max-Objs was set to " << count << std::endl;
	params.max_objs = count;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk_algo", (char*)NULL)) {
      if (val == "FBC") {
	params.chunk_algo = CHUNK_ALGO_FBC;
	cout << "Chunking algorithm was set to Fixed Block Chunking(FBC)" << std::endl;
      }
      else if (val == "CDC") {
	params.chunk_algo = CHUNK_ALGO_CDC;
	cout << "Chunking algorithm was set to Content Defined Chunking(CDC)" << std::endl;
      }
      else {
	cerr << "** illegal chunking algorithm [" << val << "]**" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--shards-count", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0 && count <= SHARDS_COUNT_MAX) {
	cout << "Shards Count was set to " << count << std::endl;
	params.num_shards = count;
      }
      else {
	cerr << "** illegal shards-count **" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-size", (char*)NULL)) {
      int size = atoi(val.c_str());
      if (size >= CHUNK_BASESIZE && (size % CHUNK_BASESIZE == 0)) {
	params.chunk_size = size;
	cout << "Chunk Size was set to " << size/1024 << " KiB" << std::endl;
      }
      else {
	cerr << "** illegal chunk-size (must be a full multiplication of " << CHUNK_BASESIZE << ") **" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--thread-count", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0 && count <= THREAD_LIMIT_STRICT) {
	params.max_threads = count;
      }
      else {
	cerr << "** illegal thread-count (must be between 1-" << THREAD_LIMIT_STRICT << ") **" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--dedup_threshold", (char*)NULL)) {
      constexpr int DEDUP_THRESHOLD_MAX = 10;
      int count = atoi(val.c_str());
      if (count > 0 && count <= DEDUP_THRESHOLD_MAX) {
	params.dedup_threshold = count;
      }
      else {
	cerr << "** illegal dedup-threshold (must be between 1-" <<
	  DEDUP_THRESHOLD_MAX << ") **" << std::endl;
	usage_exit();
      }
    } else {
      if (val[0] == '-') {
	cerr << "** Bad argument <" << val[0] << "> **" << std::endl;
	usage_exit();
      }
      ++i;
    }
  }

  if (params.pool_name.length() == 0) {
    cerr << "** No poolname was passed **" << std::endl;
    usage_exit();
  }
}

//---------------------------------------------------------------------------
int main(int argc, char **argv)
{
  map<string, string> opts;
  vector<const char*> args = argv_to_vec(argc, argv);
  if (ceph_argparse_need_usage(args)) {
    usage_exit();
  }
  dedup_params_t params;
  process_arguments(args, opts, params);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  if (args.size() == 0) {
    cerr << "Missing command argument" << std::endl;
    usage_exit();
  }
  else if (strncmp(args[0], "clean", strlen("clean")) == 0) {
    clean_intermediate_objs(params.pool_name, params.verbose);
  }
  else if (strncmp(args[0], "resume", strlen("resume")) == 0) {
    resume_operation(params);
  }
  else if (strncmp(args[0], "all", strlen("all")) == 0) {
    run_full_scan(params);
  }
  else if (strncmp(args[0], "merge-reduce", strlen("merge-reduce")) == 0) {
    uint64_t reduced_bytes = 0;
    reduce_fingerprints(&reduced_bytes, params);
    merge_reduced_fingerprints(params);
  }
  else if (strncmp(args[0], "collect", strlen("collect")) == 0) {
    uint64_t total_bytes_before = 0;
    clean_intermediate_objs(params.pool_name, params.verbose);
    collect_fingerprints(&total_bytes_before, params);
  }
  else if (strncmp(args[0], "reduce", strlen("reduce")) == 0) {
    uint64_t reduced_bytes = 0;
    reduce_fingerprints(&reduced_bytes, params);
  }
  else if (strncmp(args[0], "merge", strlen("merge")) == 0) {
    merge_reduced_fingerprints(params);
  }
  else {
    cerr << "Bad command argument <" << args[0] << ">" << std::endl;
    usage_exit();
  }
}

//===========================================================================
//                               E    O    F
//===========================================================================
