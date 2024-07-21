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
//#include "rgw/rgw_zone_types.h"
//#include "rgw/driver/rados/rgw_obj_manifest.h"


#include "rgw_manifest.h"

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
#include <limits>
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

const char *REDUCED_FP_PREFIX = "rc";
const char *REDUCED_FP_FORMAT = "rc%04hX.%08X";
const unsigned REDUCED_FP_NAME_SIZE = 16;

enum verbose_mode_t {
  VERBOSE_NONE = 0,
  VERBOSE_MIN  = 1,
  VERBOSE_MID  = 2,
  VERBOSE_MAX  = 3
};
enum chunk_algo_t {
  CHUNK_ALGO_NONE = 0,
  CHUNK_ALGO_FBC  = 1,
  CHUNK_ALGO_CDC  = 2
};

struct dedup_params_t {
  uint32_t dbg_max_objs  = 0;
  uint32_t dbg_max_shards = 0;
  uint32_t chunk_size = 2 * 1024;
  uint16_t num_threads = 16;
  uint16_t num_shards = 1;
  uint16_t dedup_threshold = 2;
  verbose_mode_t verbose = VERBOSE_NONE;
  chunk_algo_t chunk_algo = CHUNK_ALGO_NONE;
  string pool_name;
  bool clean_raw_fp = false;
  bool clean_reduce = false;
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
    "   list:    list intermediate objects created by previous runs\n"
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
  cout << dec;
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
static void print_report_merged(const FP_Dict &fp_dict,
				uint64_t reduced_bytes,
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

  cout << dec;
  std::cout << __func__ << "::After Merge-Reduce" << std::endl;
  std::cout << "We got " << unique_data_chunks << " data chunks with "
	    << params.dedup_threshold << " or more duplications" << std::endl;
  std::cout << "reduced bytes = " << reduced_bytes << std::endl;

#if 0
  uint64_t bytes_after_dedup = total_bytes_before-reduced_bytes_read;
  if (bytes_after_dedup) {
    cout << "size before dedup = " << (double)total_bytes_before / (1024*1024)
	 << " MiB, size after dedup = "
	 << (double)bytes_after_dedup / (1024*1024)  << " MiB" << std::endl;
    std::cout << "Unique Count = " << bytes_after_dedup/params.chunk_size
	      << ", Dup Count = " << reduced_bytes_read/params.chunk_size << std::endl;
    uint64_t bytes_after_reduce = (total_bytes_before-reduced_bytes_read);
    if (bytes_after_reduce) {
      cout << "dedup ratio = " << ((double)total_bytes_before/bytes_after_reduce) << std::endl;
    }
  }
  else {
    std::cerr << "ERR: total_bytes_before=" << total_bytes_before
	      << "reduced_bytes_read=" << reduced_bytes_read << std::endl;
  }
#endif
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
    uint16_t cursor_len;
    uint16_t fp_size;
    uint32_t fp_count;

    uint64_t bytes_read = 0;

    uint16_t num_threads;
    uint16_t pad16 = 0;
    uint32_t pad32 = 0;

    uint32_t offset_in_obj = 0;
    uint32_t crc;

    DENC(FP_BUFFER_HEADER, v, p) {
      uint64_t *p_cur = (uint64_t*)v.cursor;
      uint64_t *p_end = (uint64_t*)(v.cursor + MAX_CURSOR_SIZE);
      for ( ; p_cur < p_end; p_cur++) {
	denc(*p_cur, p);
      }

      denc(v.cursor_len, p);
      denc(v.fp_size, p);
      denc(v.fp_count, p);
      denc(v.bytes_read, p);
      denc(v.num_threads, p);
      denc(v.pad16, p);
      denc(v.pad32, p);
      denc(v.offset_in_obj, p);
      denc(v.crc, p);
    }

  };// __attribute__((__packed__)); // 1056 Bytes

  //---------------------------------------------------------------------------
  FP_BUFFER(unsigned fp_size,
	    IoCtx   *obj_ioctx,
	    uint16_t thread_id,
	    uint16_t num_threads,
	    int      verbose,
	    uint64_t bytes_read,
	    uint32_t part_number = 0) {
    d_byte_offset = FP_HEADER_LEN;
    d_fp_size     = fp_size;
    d_obj_ioctx   = obj_ioctx;
    d_thread_id   = thread_id;
    d_num_threads = num_threads;
    d_verbose     = verbose;
    d_part_number = part_number;
    d_bytes_read  = bytes_read;
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
    d_bytes_read += chunk_size;
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
    header.bytes_read  = HTOCEPH_64(d_bytes_read);
    header.num_threads = HTOCEPH_16(d_num_threads);
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
    if (d_verbose >= VERBOSE_MID) {
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << __func__ << "::" << d_obj_destage << "::oid=" << oid
	   << "::cursor=" << cursor << "::bytes_read=" << d_bytes_read << std::endl;
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
  uint16_t d_num_threads;
  int      d_verbose = VERBOSE_NONE;
  uint32_t d_obj_destage = 0;
  uint32_t d_fp_count = 0;
  uint32_t d_bytes_read = 0;
};
WRITE_CLASS_DENC(FP_BUFFER::FP_BUFFER_HEADER)

//---------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const FP_BUFFER::FP_BUFFER_HEADER &hdr)
{
  std::string_view cursor(hdr.cursor, hdr.cursor_len);
  os << cursor
     << "::fp_size= "         << hdr.fp_size
     << "::fp_count="         << hdr.fp_count
     << "::bytes_read="       << hdr.bytes_read
     << "::num_threads="      << hdr.num_threads
     << "::offset_in_object=" << hdr.offset_in_obj;

  if (hdr.pad16 || hdr.pad32) {
    // should never happen
    os << "\n\nhdr.pad16=" << hdr.pad16 << "::hdr.pad32=" << hdr.pad32 << "\n\n";
  }
  return os;
}
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
			    const ObjectCursor &cursor,
			    uint32_t offset_in_obj,
			    uint64_t version,
			    const dedup_params_t &params)
{
  unsigned chunk_size = params.chunk_size;
  //bool verbose = params.verbose;
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

  return fp_count;
}

//---------------------------------------------------------------------------
static int calculate_fp_cdc(FP_BUFFER *fp_buffer,
			    bufferlist &bl,
			    const ObjectCursor &cursor,
			    uint32_t offset_in_obj,
			    uint64_t version,
			    const dedup_params_t &params)
{
  unsigned chunk_size = params.chunk_size;
  //bool verbose = params.verbose;
  unsigned fp_count = 0;
  constexpr unsigned MAX_CHUNK_SIZE = 256*1024;
  ceph_assert(chunk_size*2 < MAX_CHUNK_SIZE);
  uint8_t temp[MAX_CHUNK_SIZE];
  auto bl_itr = bl.cbegin();

  std::vector<std::pair<uint64_t, uint64_t>> chunks;
  std::string chunk_algo = "fastcdc";
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
      cout << __func__ << "::***************************************::\n\n" << std::endl;

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
			       uint16_t thread_id,
			       int verbose)
{
  ObjectCursor delimiter;
  if (delimiter.from_str(*cursor)) {
    start_itr.seek(delimiter);
  }
  else {
    std::cerr << __func__ << "::bad cursor " << *cursor << std::endl;
    ceph_abort("bad cursor");
  }

  if (start_itr.get_cursor() == delimiter) {
    if (*offset_in_obj == FULL_OBJ_READ) {
      if (verbose) {
	std::cout << thread_id << "]skipping oid: " << start_itr->get_oid()
		  << ", cursor: " << start_itr.get_cursor() << std::endl;
      }
      start_itr++;
      *offset_in_obj = 0;
    }
    else {
      if (verbose) {
	std::cout << thread_id << "]start oid: " << start_itr->get_oid()
		  << ", offset_in_obj: " << *offset_in_obj << std::endl;
      }
    }
  }

  if (verbose) {
    std::cout << thread_id << "]cursor=" << start_itr.get_cursor()
	      << " || " << *cursor << std::endl;
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
  const unsigned dbg_max_objs = params.dbg_max_objs;
  const char *pool_name = params.pool_name.c_str();
  const int verbose = params.verbose;
  uint64_t objs_read = 0;
  std::vector<std::string> oid_vec;
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;

  unique_ptr<FP_BUFFER> fp_buffer = std::make_unique<FP_BUFFER>(
    sha1_digest_t::SIZE,
    &obj_ioctx,
    thread_id,
    params.num_threads,
    verbose,
    *p_bytes_read,
    start_part_num);

  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name) != 0) {
    return -1;
  }

  ObjectCursor split_start, split_finish;
  itr_ioctx.object_list_slice(itr_ioctx.object_list_begin(),
			      itr_ioctx.object_list_end(),
			      thread_id,
			      params.num_threads,
			      &split_start,
			      &split_finish);
  auto start_itr = itr_ioctx.nobjects_begin(split_start);
  //start_itr.set_cursor(itr.get_cursor());
  uint64_t fp_count = 0, fp_count_total = 0;;
  //utime_t total_time, max_time, min_time = utime_t::max();

  if (cursor) {
    move_itr_by_cursor(start_itr, cursor, &offset_in_obj, thread_id, verbose);
  }

  unsigned skip_count = 0;
  for (auto itr = start_itr; itr.get_cursor() < split_finish; ++itr) {
    string oid = itr->get_oid();
    if (itr->get_nspace() == RAW_FP_NSPACE) {
      skip_count++;
      if(verbose >= VERBOSE_MID) {
	std::unique_lock<std::mutex> lock(print_mtx);
	std::cout << skip_count << "] skip oid " << oid << std::endl;
      }
      continue;
    }
    if(verbose >= VERBOSE_MAX) {
      std::unique_lock<std::mutex> lock(print_mtx);
      std::cout << oid << std::endl;
    }
    oid_vec.push_back(oid);
    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());

    uint64_t version = obj_ioctx.get_last_version();
    bufferlist bl;
    //utime_t start_time = ceph_clock_now();
    // 0 @len means read until the end
    int ret = obj_ioctx.read(oid, bl, 0, offset_in_obj);
    //utime_t duration = ceph_clock_now() - start_time;
    if (unlikely(ret <= 0)) {
      cerr << "failed to read file: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
#if 0
    total_time += duration;
    min_time = std::min(min_time, duration);
    max_time = std::max(max_time, duration);
#endif
    if (params.chunk_algo == CHUNK_ALGO_FBC) {
      fp_count = calculate_fp_fbc(fp_buffer.get(), bl, itr.get_cursor(), offset_in_obj, version, params);
    }
    else {
      assert(params.chunk_algo == CHUNK_ALGO_CDC);
      fp_count = calculate_fp_cdc(fp_buffer.get(), bl, itr.get_cursor(), offset_in_obj, version, params);
    }

    objs_read++;
    *p_bytes_read += bl.length();

    offset_in_obj = 0;
    fp_count_total += fp_count;
    if (unlikely(dbg_max_objs && objs_read >= dbg_max_objs)) {
      //fp_buffer->flush(itr.get_cursor());
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << std::dec << thread_id << "]DEBUG EXIT::Num Objects read=" << objs_read << std::endl;
      return 0;
    }
  }

  fp_buffer->flush(split_finish, FULL_OBJ_READ);
  if (objs_read > 0 || verbose) {
    std::unique_lock<std::mutex> lock(print_mtx);
    if (verbose) {
      cout << thread_id << "] Num Objects read=" << objs_read
	   << ", Num Bytes read=" << std::setw(4)
	   << *p_bytes_read/(1024*1024) << " MiB"
	   << ", chunks-written=" << fp_count_total
	   << std::endl;
#if 0
      cout << thread_id
	   << "] Total-Time=" << total_time
	   << ", Max-Time=" << max_time
	   << ", Min-Time=" << min_time
	   << ", Avg-Time=" << total_time/objs_read
	   << std::endl;

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

  unsigned num_threads = params.num_threads;
  std::cout << "num_threads = " << num_threads << std::endl;

  std::thread* thread_arr[num_threads];
  memset(thread_arr, 0, sizeof(thread_arr));

  uint64_t bytes_read_arr[num_threads];
  memset(bytes_read_arr, 0, sizeof(bytes_read_arr));

  for (unsigned thread_id = 0; thread_id < num_threads; thread_id++) {
    thread_arr[thread_id] = new std::thread(thread_collect, thread_id,
					    bytes_read_arr+thread_id, nullptr, 0, 0, params);
  }

  for (unsigned thread_id = 0; thread_id < num_threads; thread_id++ ) {
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
  static constexpr uint16_t HAS_MORE   = 0xCEA1;
  static constexpr uint16_t LAST_ENTRY = 0x1FD0;

  uint16_t shard_id;
  uint16_t num_shards;
  uint16_t threshold;
  uint16_t fp_size;

  uint32_t fp_count;
  uint32_t serial_id;

  uint64_t reduced_bytes;

  uint16_t signature;
  uint16_t pad16;
  uint32_t crc;

  DENC(REDUCED_FP_HEADER, v, p) {
    denc(v.shard_id, p);
    denc(v.num_shards, p);
    denc(v.threshold, p);
    denc(v.fp_size, p);

    denc(v.fp_count, p);
    denc(v.serial_id, p);

    denc(v.reduced_bytes, p);

    denc(v.signature, p);
    denc(v.pad16, p);
    denc(v.crc, p);
  }
}; // 32 Bytes header
WRITE_CLASS_DENC(REDUCED_FP_HEADER)

std::ostream &operator<<(std::ostream &stream, const REDUCED_FP_HEADER & rfh)
{
  bool need_to_set_dec_mode = false;
  if (std::cout.flags() & std::ios_base::dec) {
    need_to_set_dec_mode = true;
    stream << std::hex << std::showbase;
  }
  stream << "fp_size   = " << rfh.fp_size    << "::"
	 << "fp_count  = " << rfh.fp_count   << "::"
	 << "shard_id  = " << rfh.shard_id   << "::"
	 << "num_shards= " << rfh.num_shards << "::"
	 << "threshold = " << rfh.threshold  << "::"
	 << "serial_id = " << rfh.serial_id  << "::"
	 << "reduced_bytes = " << rfh.reduced_bytes << "::";
  if (rfh.signature == REDUCED_FP_HEADER::HAS_MORE) {
    stream << "HAS MORE";
  }
  else if (rfh.signature == REDUCED_FP_HEADER::LAST_ENTRY) {
    stream << "LAST ENTRY";
  }
  else {
    // should never happend
    stream << "\n\nBAD SIGNATURE: " << std::hex << rfh.signature << "\n\n";
  }
  if (rfh.pad16) {
    stream << "\n\npad16     = " << rfh.pad16 << "\n\n";
  }
  //stream << "crc       = " << rfh.crc << std::endl;
  if (need_to_set_dec_mode) {
    stream << std::dec << std::noshowbase;
  }
  return stream;
}

//---------------------------------------------------------------------------
static void reduced_fp_flush(REDUCED_FP_HEADER header,
			     uint8_t  buffer[],
			     uint32_t offset,
			     uint32_t fp_size,
			     uint32_t shard_id,
			     uint32_t serial_id,
			     IoCtx   &obj_ioctx,
			     bool     has_more,
			     uint64_t reduced_bytes)
{
  uint16_t signature = (has_more ? REDUCED_FP_HEADER::HAS_MORE : REDUCED_FP_HEADER::LAST_ENTRY);
  header.signature = HTOCEPH_16(signature);
#ifndef DISABLE_CDC_CODE
  header.fp_count  = HTOCEPH_32((offset - sizeof(REDUCED_FP_HEADER)) / sizeof(chunk_fp_t));
#else
  header.fp_count  = HTOCEPH_32((offset - sizeof(REDUCED_FP_HEADER)) / fp_size);
#endif
  header.serial_id = HTOCEPH_32(serial_id);
  header.reduced_bytes = HTOCEPH_64(reduced_bytes);
  header.crc       = 0;
  memcpy(buffer, (uint8_t*)&header, sizeof(REDUCED_FP_HEADER));
  uint32_t crc = -1;
  crc = ceph_crc32c(crc, buffer, offset);
  ((REDUCED_FP_HEADER*)buffer)->crc = HTOCEPH_32(crc);

  char name_buf[REDUCED_FP_NAME_SIZE];
  // TBD: allow 0xFF server-shards each with 0xFF internal shards
  // make sure shard_id will fit in 4 hex digits
  static_assert(SHARDS_COUNT_MAX < 0xFFFF);
  unsigned n = snprintf(name_buf, sizeof(name_buf),
			REDUCED_FP_FORMAT, shard_id, serial_id);
  std::string oid(name_buf, n);

  if (has_more == false) {
    std::unique_lock<std::mutex> lock(print_mtx);
    cout << oid << "::" << header << std::endl;
  }

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
  // fixed parts of the header which can be reused
  header.shard_id   = HTOCEPH_16(shard_id);
  header.num_shards = HTOCEPH_16(params.num_shards);
  header.threshold  = HTOCEPH_16(params.dedup_threshold);
  header.fp_size    = HTOCEPH_16(fp_size);
  header.pad16      = 0;
#ifndef DISABLE_CDC_CODE
  unsigned key_size = sizeof(chunk_fp_t);
#else
  unsigned key_size = fp_size;
#endif
  uint32_t serial_id = 0;
  unsigned offset    = sizeof(REDUCED_FP_HEADER);
  for (auto itr = fp_dict.begin(); itr != fp_dict.end(); itr++){
    const chunk_fp_t & key = itr->first;
    const unsigned count = itr->second;

    if (count >= params.dedup_threshold) {
      if (unlikely(offset+key_size >= sizeof(buffer))) {
	bool has_more = (std::next(itr) != fp_dict.end());
	reduced_fp_flush(header, buffer, offset, fp_size, shard_id,
			 serial_id++, obj_ioctx, has_more, *p_reduced_bytes);
	offset = sizeof(REDUCED_FP_HEADER);
      }
      memcpy(buffer+offset, key.get_fp_buff(), fp_size);
      offset += fp_size;
#ifndef DISABLE_CDC_CODE
      *(uint32_t*)(buffer+offset) = HTOCEPH_32(key.get_chunk_size());
      offset += sizeof(key.get_chunk_size());
#endif
      *p_reduced_bytes += (count-1)*(key.get_chunk_size());
    }
  }

  if (offset > 0) {
    {
      std::unique_lock<std::mutex> lock(print_mtx);
      if (unlikely(params.dbg_max_shards && (shard_id % 2 == 1))) {
	cout << "==DEBUG EXIT::Skipping the last entry for shard_id "
	     << shard_id << "=="<< std::endl;
	return 0;
      }
      else {
	cout << "Writing the last entry for shard_id " << std::hex << std::showbase
	     << shard_id << ":" << serial_id << std::dec << std::endl;
      }
    }
    reduced_fp_flush(header, buffer, offset, fp_size, shard_id,
		     serial_id++, obj_ioctx, false, *p_reduced_bytes);
  }

  return 0;
}

//---------------------------------------------------------------------------
static int reduce_fingerprints_single_object(FP_Dict &fp_dict,
					     bufferlist &bl,
					     unsigned shard_id,
					     const dedup_params_t &params)
{
  uint64_t uniq_cnt = 0;
  uint64_t cnt = 0;
  FP_BUFFER::FP_BUFFER_HEADER fp_header;
  auto bl_itr = bl.cbegin();
  decode(fp_header, bl_itr);

  if (params.verbose >= VERBOSE_MID) {
    std::unique_lock<std::mutex> lock(print_mtx);
    cout << "reduce::" << fp_header << std::endl;
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
  FP_Dict fp_dict;
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }
  itr_ioctx.set_namespace(RAW_FP_NSPACE);
  uint64_t count, objs_read = 0, chunks_read = 0;
  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with(HASH_PREFIX));
    if(params.verbose >= VERBOSE_MAX) {
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
    count = reduce_fingerprints_single_object(fp_dict, bl, shard_id, params);
    chunks_read += count;
  }
  if (params.num_shards == 1) {
    cout << __func__ << "::single shard mode" << std::endl;
    cout << "Num Objects read=" << objs_read << ", chunks-read=" << chunks_read << std::endl;
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
  unsigned dbg_max_shards = params.dbg_max_shards;
  for (unsigned shard_id = 0; shard_id < params.num_shards; shard_id++) {
    uint64_t shard_reduced_bytes = 0;
    reduce_shard_fingerprints(shard_id, &shard_reduced_bytes, params);
    *p_reduced_bytes += shard_reduced_bytes;
    if (unlikely(dbg_max_shards && shard_id >= dbg_max_shards)) {
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << "DEBUG EXIT::Breaking after shard_id = " << shard_id << std::endl;
      return 0;
    }
  }

  return 0;
}

//================================================================================
//                                   M E R G E
//================================================================================

//---------------------------------------------------------------------------
static int merge_fingerprints_single_object(FP_Dict &fp_dict,
					    bufferlist &bl,
					    uint64_t *p_reduced_bytes,
					    const dedup_params_t &params)
{
  uint64_t uniq_cnt    = 0;
  uint64_t cnt         = 0;
  REDUCED_FP_HEADER header;
  bufferlist::const_iterator bl_itr = bl.cbegin();
  decode(header, bl_itr);
  if (header.signature == REDUCED_FP_HEADER::HAS_MORE) {
  }
  else if (header.signature == REDUCED_FP_HEADER::LAST_ENTRY) {
    *p_reduced_bytes += header.reduced_bytes;
  }
  else {
    std::cerr << header << std::endl;
    ceph_abort("Bad Header Signature");
  }
  ceph_assert(header.pad16 == 0);
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
static int merge_reduced_fingerprints(uint64_t *p_reduced_bytes, const dedup_params_t &params)
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
    ceph_assert(oid.starts_with(REDUCED_FP_PREFIX));

    if (params.verbose >= VERBOSE_MID) {
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
    merge_fingerprints_single_object(fp_dict, bl, p_reduced_bytes, params);
  }

  //store_merged_reduced_fingerprints(params, fp_dict, shard_id);
  print_report_merged(fp_dict, *p_reduced_bytes, params);

  return 0;
}

//===========================================================================
//---------------------------------------------------------------------------
static bool read_shard_last_reduced_fp_header(struct REDUCED_FP_HEADER *p_header,
					      uint16_t shard_id,
					      uint16_t last_part_num,
					      IoCtx *p_obj_ioctx,
					      const dedup_params_t &params)
{
  char name_buf[REDUCED_FP_NAME_SIZE];
  unsigned n = snprintf(name_buf, sizeof(name_buf), REDUCED_FP_FORMAT,
			shard_id, last_part_num);
  std::string oid(name_buf, n);
  bufferlist bl;
  // Read only the HEADER section from the object
  int len = sizeof(REDUCED_FP_HEADER);
  int ret = p_obj_ioctx->read(oid, bl, len, 0);
  if (ret != len) {
    std::cerr << __func__ << "::failed obj_ioctx.read(" << oid << ")"
	      << ", error = " << cpp_strerror(ret) << std::endl;
    // if no valid object was found start the dedup from the beginning
    return true;
  }

  auto bl_itr = bl.cbegin();
  decode(*p_header, bl_itr);

  if (params.verbose >= VERBOSE_MID) {
    cout << *p_header << std::endl;
  }

  // TBD: add CRC check
  if (p_header->pad16     == 0        &&
      p_header->shard_id  == shard_id &&
      p_header->signature == REDUCED_FP_HEADER::LAST_ENTRY) {
    // this shard was completed -> nothing to do
    if (params.verbose ) {
      std::cout << "shard_id: " << shard_id
		<< " was successfully validated using oid: " << oid << std::endl;
    }
    return false;
  }

  // If arrived here it is either not the last-entry or it is corrupted
  // Repeat the reduce step for the whole shard
  if (params.verbose ) {
    std::cout << "shard_id: " << shard_id
	      << " failed validation from oid: " << oid << std::endl;
  }
  return true;
}

//---------------------------------------------------------------------------
static int read_thread_last_fp_header(FP_BUFFER::FP_BUFFER_HEADER *p_fp_header,
				      uint16_t thread_id,
				      uint16_t last_part_num,
				      IoCtx   *p_obj_ioctx,
				      int      verbose)
{
  char name_buf[HASH_OID_NAME_SIZE];
  unsigned n = snprintf(name_buf, sizeof(name_buf), HASH_FORMAT,
			thread_id, last_part_num);
  std::string oid(name_buf, n);
  if (verbose) {
    std::cout << __func__ << "[" << thread_id << "]::oid: " << oid << std::endl;
  }

  bufferlist bl;
  // Read only the HEADER section from the object
  int ret = p_obj_ioctx->read(oid, bl, FP_HEADER_LEN, 0);
  if (ret != FP_HEADER_LEN) {
    std::cerr << __func__ << "::failed obj_ioctx.read(" << oid << ")"
	      << ", error = " << cpp_strerror(ret) << std::endl;
    // TBD: maybe move to the prev obj until a valid one is found
    // if no valid object was found start the dedup from the beginning
    return ret;
  }
  auto bl_itr = bl.cbegin();
  decode(*p_fp_header, bl_itr);
  if (verbose >= VERBOSE_MID) {
    cout << *p_fp_header << std::endl;
  }

  // TBD: check CRC
  if (p_fp_header->pad16 == 0 && p_fp_header->pad32 == 0) {
    return 0;
  }
  else {
    cerr << __func__ << "::Bad Padding in header:\n"
	 << *p_fp_header << std::endl;
    return -1;
  }
}

//---------------------------------------------------------------------------
static int resume_thread_collect(uint16_t thread_id,
				 uint16_t last_part_num,
				 IoCtx *p_obj_ioctx,
				 uint64_t *p_bytes_read,
				 const dedup_params_t &params)
{
  FP_BUFFER::FP_BUFFER_HEADER fp_header;
  int ret = read_thread_last_fp_header(&fp_header,
				       thread_id,
				       last_part_num,
				       p_obj_ioctx,
				       params.verbose);
  if (ret == 0) {
    std::string cursor(fp_header.cursor, fp_header.cursor_len);
    *p_bytes_read = fp_header.bytes_read;
    if (params.verbose) {
      std::cout << thread_id << "] read_bytes=" << *p_bytes_read << "::"
		<< "last_part=" << last_part_num << "::" << cursor << std::endl;
    }

    return thread_collect(thread_id, p_bytes_read, &cursor, last_part_num+1,
			  fp_header.offset_in_obj, params);
  }
  else {
    return ret;
  }
}

//---------------------------------------------------------------------------
static int parse_object_name(const string &oid,
			     const char *fmt,
			     uint16_t *p_id,
			     uint32_t *p_part_num,
			     int verbose)
{
  // convert CPP string to c style string without dynamic allocation
  size_t size = oid.length();
  char buff[size + 1];
  strncpy(buff, oid.data(), size);
  buff[size] = '\0';

  sscanf(buff, fmt, p_id, p_part_num);
  if (verbose >= VERBOSE_MID) {
    cout << std::hex << "id=" << *p_id << ", part_number="
	 << *p_part_num << std::endl;
  }
  return 0;
}

//---------------------------------------------------------------------------
static void find_last_part_number_per_id(IoCtx *p_itr_ioctx,
					 const char *fmt,
					 uint32_t *table,
					 unsigned max_id,
					 uint16_t *p_highest_id,
					 int verbose)
{
  *p_highest_id = 0;
  memset(table, 0x0, sizeof(uint32_t) * max_id);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();

  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (verbose >= VERBOSE_MID) {
      cout << __func__ << "::oid = " << oid << std::endl;
    }

    uint16_t id;
    uint32_t part_num;
    parse_object_name(oid, fmt, &id, &part_num, verbose);
    if (id < max_id) {
      table[id] = std::max(table[id], part_num);
      *p_highest_id = std::max(*p_highest_id, id);
    }
    else {
      std::cerr << "bad id was found in oid = " << oid << std::endl;
      // skip this entry
    }
  }
}

//---------------------------------------------------------------------------
static int recover_reduce_fingerprints_step(IoCtx *p_itr_ioctx,
					    IoCtx *p_obj_ioctx,
					    uint64_t *p_reduced_bytes,
					    dedup_params_t &params)
{
  uint32_t last_part_num_table[UINT16_MAX];
  uint16_t highest_shard_id = 0, num_shards = params.num_shards;
  cout << __func__ << "::Start Recovery...\n" << std::endl;
  p_itr_ioctx->set_namespace(REDUCED_FP_NSPACE);
  p_obj_ioctx->set_namespace(REDUCED_FP_NSPACE);
  find_last_part_number_per_id(p_itr_ioctx, REDUCED_FP_FORMAT, last_part_num_table,
			       UINT16_MAX, &highest_shard_id, params.verbose);

  bool shards_to_process[highest_shard_id+1];
  for (uint16_t shard_id = 0; shard_id <= highest_shard_id; shard_id++) {
    struct REDUCED_FP_HEADER header;
    uint32_t last_part_num = last_part_num_table[shard_id];
    int ret = read_shard_last_reduced_fp_header(&header, shard_id, last_part_num,
						p_obj_ioctx, params);
    if (ret == 0) {
      shards_to_process[shard_id] = false;
      // TBD: should verify that all shards agree on num_shards
      num_shards = header.num_shards;
      params.num_shards = header.num_shards;
      *p_reduced_bytes += header.reduced_bytes;
    }
    else {
      shards_to_process[shard_id] = true;
      if (params.verbose) {
	if (shards_to_process[shard_id]) {
	  cout << __func__ << "::Adding shard_id " << shard_id
	       << " to process list" << std::endl;
	}
      }
    }
  }

  uint16_t shard_id = 0;
  for ( ; shard_id <= highest_shard_id; shard_id++) {
    if (shards_to_process[shard_id]) {
      cout << __func__ << "::shard_id " << shard_id << " starts recovery" << std::endl;
      uint64_t shard_reduced_bytes = 0;
      reduce_shard_fingerprints(shard_id, &shard_reduced_bytes, params);
      *p_reduced_bytes += shard_reduced_bytes;
    }
  }

  for ( ; shard_id < num_shards; shard_id++) {
    cout << __func__ << "::shard_id " << shard_id << " starts recovery" << std::endl;
    uint64_t shard_reduced_bytes = 0;
    reduce_shard_fingerprints(shard_id, &shard_reduced_bytes, params);
    *p_reduced_bytes += shard_reduced_bytes;
  }

  return 0;
}

//---------------------------------------------------------------------------
uint16_t find_num_threads(IoCtx *p_obj_ioctx,
			  uint32_t last_part_num_table[],
			  uint16_t highest_thread_id,
			  const dedup_params_t &params)
{
  bool has_more  = true;
  unsigned count = 3;
  while (has_more && count > 0) {
    for (unsigned thread_id = 0; thread_id <= highest_thread_id; thread_id++) {
      FP_BUFFER::FP_BUFFER_HEADER fp_header;
      uint32_t last_part_num = last_part_num_table[thread_id];
      int ret = read_thread_last_fp_header(&fp_header, thread_id, last_part_num,
					   p_obj_ioctx, params.verbose);
      if (ret == 0) {
	uint16_t num_threads = fp_header.num_threads;
	if (params.verbose && params.verbose < VERBOSE_MID) {
	  cout << "num_threads=" << num_threads << std::endl;
	}
	ceph_assert(num_threads <= THREAD_LIMIT_STRICT);
	ceph_assert(num_threads > 0);
	ceph_assert(highest_thread_id <= num_threads);
	return num_threads;
      }
    }

    cerr << __func__ << "::" << count
	 << "::corrupted object headers" << std::endl;
    // if arrived here all last obejcts are corrupted
    // try the one before and so on ...
    has_more = false;
    for (unsigned thread_id = 0; thread_id <= highest_thread_id; thread_id++) {
      if (last_part_num_table[thread_id] > 0) {
	last_part_num_table[thread_id] --;
	has_more = true;
      }
    }
    count --;
  }

  // fail the call and restart work from the beginning
  return 0;
}

//---------------------------------------------------------------------------
static int recover_collect_fingerprints_step(IoCtx *p_itr_ioctx,
					     IoCtx *p_obj_ioctx,
					     uint64_t *p_total_bytes_read,
					     dedup_params_t &params)
{
  uint32_t last_part_num_table[THREAD_LIMIT_STRICT];
  uint16_t highest_thread_id = 0;

  p_itr_ioctx->set_namespace(RAW_FP_NSPACE);
  p_obj_ioctx->set_namespace(RAW_FP_NSPACE);

  cout << __func__ << "::Start Recovery...\n" << std::endl;
  find_last_part_number_per_id(p_itr_ioctx, HASH_FORMAT, last_part_num_table,
			       THREAD_LIMIT_STRICT, &highest_thread_id, params.verbose);
  ceph_assert(highest_thread_id < THREAD_LIMIT_STRICT);
  if (params.verbose) {
    for (unsigned thread_id = 0; thread_id <= highest_thread_id; thread_id++) {
      cout << thread_id << "] last_part=" << last_part_num_table[thread_id] << std::endl;
    }
  }
  std::thread* thread_arr[THREAD_LIMIT_STRICT];
  memset(thread_arr, 0, sizeof(thread_arr));

  uint64_t bytes_read_arr[THREAD_LIMIT_STRICT];
  memset(bytes_read_arr, 0, sizeof(bytes_read_arr));

  uint16_t num_threads = find_num_threads(p_obj_ioctx, last_part_num_table,
					  highest_thread_id, params);
  if (num_threads == 0) {
    // we failed to recover from last session -> run_full_scan
    return -1;
  }
  cout << __func__ << "::Resume collect, num_threads=" << num_threads << std::endl;
  // change num_threads inside params!!!
  params.num_threads = num_threads;
  for (unsigned thread_id = 0; thread_id < num_threads; thread_id++) {
    uint32_t last_part_num = last_part_num_table[thread_id];
    thread_arr[thread_id] = new std::thread(resume_thread_collect, thread_id, last_part_num,
					    p_obj_ioctx, bytes_read_arr+thread_id, params);
  }

  for (unsigned thread_id = 0; thread_id < num_threads; thread_id++ ) {
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
static bool namespace_exists(IoCtx *p_itr_ioctx, const string &nspace, int verbose)
{
  p_itr_ioctx->set_namespace(nspace);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    ceph_assert(oid.starts_with(HASH_PREFIX) ||
		oid.starts_with(REDUCED_FP_PREFIX));
    if (verbose >= VERBOSE_MID) {
      cout << __func__ << "::first oid = " << oid << std::endl;
    }
    return true;
  }

  return false;
}

//---------------------------------------------------------------------------
static int resume_operation(const dedup_params_t &old_params)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  const int   verbose   = old_params.verbose;
  const char* pool_name = old_params.pool_name.c_str();
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name) != 0) {
    return -1;
  }

  // clear all debug setting
  dedup_params_t params  = old_params;
  params.dbg_max_objs    = 0;
  params.dbg_max_shards  = 0;
  // check if we have any work done from before
  if (namespace_exists(&itr_ioctx, RAW_FP_NSPACE, verbose) == false) {
    // nothing was done, start from the beginning
    cerr << __func__ << "::nothing to resume -> run_full_scan" << std::endl;
    run_full_scan(params);
    return 0;
  }

  uint64_t total_bytes_before = 0;
  uint64_t reduced_bytes = 0;
  int ret = recover_collect_fingerprints_step(&itr_ioctx, &obj_ioctx,
					      &total_bytes_before, params);
  if (ret != 0) {
    // we failed to recover from last session -> run_full_scan
    cerr << __func__ << "::Failed resume -> run_full_scan" << std::endl;
    run_full_scan(params);
    return 0;
  }

  if (namespace_exists(&itr_ioctx, REDUCED_FP_NSPACE, verbose) == false) {
    params.verbose = VERBOSE_NONE;
    reduce_fingerprints(&reduced_bytes, params);
  }
  else {
    recover_reduce_fingerprints_step(&itr_ioctx, &obj_ioctx, &reduced_bytes, params);
    params.verbose = VERBOSE_NONE;
  }

  uint64_t bytes_after_dedup = total_bytes_before-reduced_bytes;
  cout << "size before dedup = " << (double)total_bytes_before / (1024*1024)
       << " MiB, size after dedup = "
       << (double)bytes_after_dedup / (1024*1024)  << " MiB" << std::endl;

  if (params.chunk_algo == CHUNK_ALGO_FBC) {
    std::cout << "Unique Count = " << bytes_after_dedup/params.chunk_size
	      << ", Dup Count = "  << reduced_bytes/params.chunk_size << std::endl;
  }
  uint64_t bytes_after_reduce = (total_bytes_before-reduced_bytes);
  if (bytes_after_reduce) {
    cout << "dedup ratio = " << ((double)total_bytes_before/bytes_after_reduce) << std::endl;
  }

  uint64_t reduced_bytes_wrt = 0;
  merge_reduced_fingerprints(&reduced_bytes_wrt, params);
  return 0;
}

//===========================================================================
enum scan_namespace_op_t {
  SCAN_NAMESPACE_OP_LIST,
  SCAN_NAMESPACE_OP_CLEAN
};

//---------------------------------------------------------------------------
static int scan_namespace(IoCtx *p_itr_ioctx,
			  IoCtx *p_obj_ioctx,
			  const string &nspace,
			  scan_namespace_op_t op,
			  int verbose)
{
  p_itr_ioctx->set_namespace(nspace);
  auto start_itr = p_itr_ioctx->nobjects_begin();
  auto end_itr   = p_itr_ioctx->nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (op == SCAN_NAMESPACE_OP_LIST) {
      cout << oid << std::endl;
      continue;
    }
    ceph_assert(oid.starts_with(HASH_PREFIX) ||
		oid.starts_with(REDUCED_FP_PREFIX));
    if (op == SCAN_NAMESPACE_OP_CLEAN) {
      if (verbose >= VERBOSE_MID) {
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
  }

  return 0;
}

//---------------------------------------------------------------------------
static int list_intermediate_objs(const string &pool_name, int verbose)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name.c_str()) != 0) {
    return -1;
  }
  scan_namespace_op_t op = SCAN_NAMESPACE_OP_LIST;
  if (scan_namespace(&itr_ioctx, &obj_ioctx, RAW_FP_NSPACE, op, verbose) != 0) {
    return -1;
  }
  return scan_namespace(&itr_ioctx, &obj_ioctx, REDUCED_FP_NSPACE, op, verbose);
}

//---------------------------------------------------------------------------
static int clean_intermediate_objs(const string &pool_name,
				   bool clean_raw_fp,
				   bool clean_reduce,
				   bool verbose)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name.c_str()) != 0) {
    return -1;
  }
  scan_namespace_op_t op = SCAN_NAMESPACE_OP_CLEAN;
  if (clean_raw_fp) {
    if (scan_namespace(&itr_ioctx, &obj_ioctx, RAW_FP_NSPACE, op, verbose) != 0) {
      return -1;
    }
  }
  if (clean_reduce) {
    return scan_namespace(&itr_ioctx, &obj_ioctx, REDUCED_FP_NSPACE, op, verbose);
  }
  return 0;
}
#define RGW_ATTR_MANIFEST "user.rgw.manifest"
//---------------------------------------------------------------------------
[[maybe_unused]]static void print_attrset(std::map<std::string, bufferlist> &attrset)
{
  for (auto itr = attrset.begin(); itr != attrset.end(); ++itr) {
    const std::string attr_name = itr->first;
    const char* attr_val  = itr->second.c_str();

    std::cout << __func__ << "::attr_name=" << attr_name;
    if (itr->second.length() > 0) {
      std::cout << ", len=" << itr->second.length() << ", attr_val=" << attr_val;
    }
    std::cout << std::endl;
  }

  auto src_itr = attrset.find(RGW_ATTR_MANIFEST);
  if (src_itr == attrset.end()) {
    cerr << "failed to get attribute: " << RGW_ATTR_MANIFEST << std::endl;
    return;
  }
  RGWObjManifest manifest;
  decode(manifest, src_itr->second);
  std::cout << "obj_size:  " << manifest.get_obj_size()
	    << "::head_size: " << manifest.get_head_size()
	    << "::prefix: " << manifest.get_prefix() << std::endl;

  std::cout << "bucket: " << manifest.get_obj().bucket << std::endl;
  std::cout << "key: " << manifest.get_obj().key << std::endl;
  std::cout << "tail_instance: " << manifest.get_tail_instance() << std::endl;
  //std::map<uint64_t, RGWObjManifestPart> objs;
  std::map<uint64_t, RGWObjManifestPart>& objs = manifest.get_explicit_objs();
  for (auto itr = objs.begin(); itr != objs.end(); ++itr) {
    uint64_t num = itr->first;
    RGWObjManifestPart part = itr->second;
    rgw_obj &loc = part.loc;          /* the object where the data is located */
    rgw_bucket &bucket = loc.bucket;
    rgw_obj_key &key   = loc.key;
    uint64_t loc_ofs = part.loc_ofs;  /* the offset at that object where the data is located */
    uint64_t size = part.size ;       /* the part size */
    std::cout << num << "::" << loc_ofs << "::" << size << std::endl;
    std::cout << "bucket: " << bucket << std::endl;
    std::cout << "key: " << key << std::endl;
  }
}

//---------------------------------------------------------------------------
static int dedup_full_object(const string &pool_name,
			     const string &src_oid,
			     const string &dup_oid)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name.c_str()) != 0) {
    return -1;
  }
  bufferlist bl;
  int ret = obj_ioctx.read_full(src_oid, bl);
  if (unlikely(ret < 0)) {
    cerr << "failed to read src_oid: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  std::cout << __func__ << "::src_oid len=" << bl.length() << std::endl;

  std::map<std::string, bufferlist> attrset;
  ret = obj_ioctx.getxattrs(src_oid, attrset);
  if (unlikely(ret < 0)) {
    cerr << "failed to read src_oid xattrs: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  print_attrset(attrset);
  return 0;

  ret = obj_ioctx.read_full(dup_oid, bl);
  if (unlikely(ret < 0)) {
    cerr << "failed to read dup_oid: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  std::cout << __func__ << "::dup_oid len=" << bl.length() << std::endl;
  
  auto itr = attrset.find(RGW_ATTR_MANIFEST);
  if (itr == attrset.end()) {
    cerr << "failed to get attribute: " << RGW_ATTR_MANIFEST << std::endl;
    return -1;
  }
#if 1
  std::cout << "setxattr::attr_name=" << itr->first
	    << ", attr_val.len()=" << itr->second.length() << std::endl;
  ret = obj_ioctx.setxattr(dup_oid, itr->first.c_str(), itr->second);
  if (unlikely(ret < 0)) {
    cerr << "failed to set dup_oid xattrs: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
#endif


#if 0
  ret = obj_ioctx.getxattr(oid, RGW_ATTR_ETAG, bl);

  ret = obj_ioctx.trunc(dup_oid, 0);
  if (unlikely(ret < 0)) {
    cerr << "failed to trunc dup_oid: " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  RGW_ATTR_ETAG;
  RGW_ATTR_MANIFEST;
  int getxattr(const std::string& oid, const char *name, bufferlist& bl);
  int getxattrs(const std::string& oid, std::map<std::string, bufferlist>& attrset);
  int setxattr(const std::string& oid, const char *name, bufferlist& bl);
  int rmxattr(const std::string& oid, const char *name);
  int exec(const std::string& oid, const char *cls, const char *method,
	   bufferlist& inbl, bufferlist& outbl);
#endif
  return 0;
}

//---------------------------------------------------------------------------
static void run_full_scan(const dedup_params_t &params)
{
  uint64_t total_bytes_before = 0;
  uint64_t reduced_bytes_wrt  = 0;
  uint64_t reduced_bytes_read = 0;

  clean_intermediate_objs(params.pool_name, true, true, params.verbose);
  collect_fingerprints(&total_bytes_before, params);
  reduce_fingerprints(&reduced_bytes_wrt, params);
  merge_reduced_fingerprints(&reduced_bytes_read, params);
  if (reduced_bytes_read != reduced_bytes_wrt) {
    std::cerr << "Mismatch!! reduced_bytes_read=" << reduced_bytes_read
	      << " reduced_bytes_wrt =" << reduced_bytes_wrt << std::endl;
    exit(1);
    ceph_abort("mismatched reduced_bytes_wrt/reduced_bytes_read");
  }

  uint64_t bytes_after_dedup = total_bytes_before-reduced_bytes_read;
  if (bytes_after_dedup) {
    cout << "size before dedup = " << (double)total_bytes_before / (1024*1024)
	 << " MiB, size after dedup = "
	 << (double)bytes_after_dedup / (1024*1024)  << " MiB" << std::endl;
    if (params.chunk_algo == CHUNK_ALGO_FBC) {
      std::cout << "Unique Count = " << bytes_after_dedup/params.chunk_size
		<< ", Dup Count = " << reduced_bytes_read/params.chunk_size << std::endl;
    }
    uint64_t bytes_after_reduce = (total_bytes_before-reduced_bytes_read);
    if (bytes_after_reduce) {
      cout << "dedup ratio = " << ((double)total_bytes_before/bytes_after_reduce) << std::endl;
    }
  }
  else {
    std::cerr << "ERR: total_bytes_before=" << total_bytes_before
	      << "reduced_bytes_read=" << reduced_bytes_read << std::endl;
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
    } else if (ceph_argparse_flag(args, i, "--clean_raw", (char*)NULL)) {
      params.clean_raw_fp = true;
    } else if (ceph_argparse_flag(args, i, "--clean_reduce", (char*)NULL)) {
      params.clean_reduce = true;
    } else if (ceph_argparse_flag(args, i, "--clean_all", (char*)NULL)) {
      params.clean_raw_fp = true;
      params.clean_reduce = true;
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
	cout << "Debug Mode!!! Max-Objs was set to " << count << std::endl;
	params.dbg_max_objs = count;
      }
      else {
	cerr << "** illegal max-objs **" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-shards", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0) {
	params.dbg_max_shards = count;
      }
      else {
	cerr << "** illegal max-shards **" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--verbose", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0) {
	cout << "set verbose mode to " << count << std::endl;
	params.verbose = (verbose_mode_t)count;
      }
      else {
	cerr << "** illegal verbose mode **" << std::endl;
	usage_exit();
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
	params.num_threads = count;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--src_obj", (char*)NULL)) {
      opts["src_oid"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dup_obj", (char*)NULL)) {
      opts["dup_oid"] = val;
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

  // clear collect debug-mode if not in collect mode
  if (strncmp(args[0], "collect", strlen("collect")) != 0) {
    params.dbg_max_objs = 0;
  }

  // clear reduce debug-mode if not in reduce mode
  if (strncmp(args[0], "reduce", strlen("reduce")) != 0) {
    params.dbg_max_shards = 0;
  }

  if (strncmp(args[0], "list", strlen("list")) == 0) {
    list_intermediate_objs(params.pool_name, params.verbose);
  }
  else if (strncmp(args[0], "dedup", strlen("dedup")) == 0) {
    cout << "src_oid=" << opts["src_oid"] << ", dup_oid=" << opts["dup_oid"] << std::endl;
    dedup_full_object(params.pool_name, opts["src_oid"], opts["dup_oid"]);
  }
  else if (strncmp(args[0], "clean", strlen("clean")) == 0) {
    bool clean_raw_fp = params.clean_raw_fp;
    bool clean_reduce = params.clean_reduce;
    clean_intermediate_objs(params.pool_name, clean_raw_fp, clean_reduce, params.verbose);
  }
  else if (strncmp(args[0], "resume", strlen("resume")) == 0) {
    resume_operation(params);
  }
  else if (strncmp(args[0], "all", strlen("all")) == 0) {
    run_full_scan(params);
  }
  else if (strncmp(args[0], "merge-reduce", strlen("merge-reduce")) == 0) {
    uint64_t reduced_bytes_read = 0, reduced_bytes_wrt = 0;
    reduce_fingerprints(&reduced_bytes_read, params);
    merge_reduced_fingerprints(&reduced_bytes_wrt, params);
  }
  else if (strncmp(args[0], "collect", strlen("collect")) == 0) {
    uint64_t total_bytes_before = 0;
    clean_intermediate_objs(params.pool_name, true, true, params.verbose);
    collect_fingerprints(&total_bytes_before, params);
  }
  else if (strncmp(args[0], "reduce", strlen("reduce")) == 0) {
    if (params.dbg_max_shards) {
      cout << "Debug Mode!!! Max-Shards was set to "
	   << params.dbg_max_shards << std::endl;
    }
    uint64_t reduced_bytes = 0;
    reduce_fingerprints(&reduced_bytes, params);
  }
  else if (strncmp(args[0], "merge", strlen("merge")) == 0) {
    uint64_t reduced_bytes_wrt = 0;
    merge_reduced_fingerprints(&reduced_bytes_wrt, params);
  }
  else {
    cerr << "Bad command argument <" << args[0] << ">" << std::endl;
    usage_exit();
  }
}

//===========================================================================
//                               E    O    F
//===========================================================================
