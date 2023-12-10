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
#include <cinttypes>
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

uint8_t g_4MB_buffer[4*1024*1024];
struct dedup_params_t {
  uint32_t max_objs    = 0;
  uint32_t chunk_size  = 2 * 1024;
  uint16_t max_threads = 16;
  uint16_t num_shards  = 1;
  bool     verbose     = false;
  uint16_t dedup_threshold = 2;
  string   pool_name;
};

constexpr int CHUNK_BASESIZE      = 1024;
constexpr int SHARDS_COUNT_MAX    = 256;
constexpr int THREAD_LIMIT_STRICT = 16;
//---------------------------------------------------------------------------
void usage(ostream& out)
{
  out << "usage: ceph-dedup-estimate-tool [options] [commands]\n\n"
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
struct Key
{
  constexpr static uint32_t FP_SIZE = SHA1_SIZE;
  static_assert(FP_SIZE % sizeof(uint32_t) == 0);
  static_assert(FP_SIZE >= sizeof(uint64_t));

  Key(uint8_t *p) {
    memcpy(fp_buff, p, FP_SIZE);
  }

  Key(bufferlist::const_iterator &ci) {
    uint32_t count  = FP_SIZE;
    uint8_t *p_dest = fp_buff;
    while (count > 0) {
      const char *p_src = nullptr;
      size_t n = ci.get_ptr_and_advance(count, &p_src);
      memcpy(p_dest, p_src, n);
      count  -= n;
      p_dest += n;
    }
  }

  const uint8_t* get_fp_buff() const {
    return fp_buff;
  }

  // TBD: When we calculate FP for the same data chunk will we get the same FP
  //      on little endian and big endian systems???
  unsigned get_shard(unsigned num_shards) {
    uint64_t val = *(uint64_t*)fp_buff;
    val = CEPHTOH_64(val);
    return val % num_shards;
  }

private:
  uint8_t fp_buff[FP_SIZE];

public:
  struct KeyHash
  {
    // No need to change the endianness since it will be internally consistent
    std::size_t operator()(const Key& k) const
    {
      // The SHA is already a hashing function so no need for another hash
      return *(uint64_t*)(k.get_fp_buff());
    }
  };

  struct KeyEqual
  {
    bool operator()(const Key& lhs, const Key& rhs) const
    {
      return (memcmp(lhs.get_fp_buff(), rhs.get_fp_buff(), lhs.FP_SIZE) == 0);
    }
  };
} __attribute__((__packed__));

std::ostream &operator<<(std::ostream &stream, const Key & k)
{
  stream << std::hex << "0x";
  for (unsigned i = 0; i < k.FP_SIZE; i++) {
    stream << std::hex << k.get_fp_buff()[i];
  }
  return stream;
}

static_assert(sizeof(Key) == SHA1_SIZE);
using FP_Dict = std::unordered_map<Key, uint32_t, Key::KeyHash, Key::KeyEqual>;

//---------------------------------------------------------------------------
[[maybe_unused]]static void print_report(const dedup_params_t &params, const FP_Dict &fp_dict, bool list)
{
  constexpr unsigned ARR_SIZE = (64*1024);
  std::array<uint32_t, ARR_SIZE+1> summery;
  for (unsigned idx = 0; idx <= ARR_SIZE; idx ++) {
    summery[idx] = 0;
  }
  uint64_t duplicated_data = 0, duplicated_data_chunks = 0;
  uint64_t unique_data     = 0, unique_data_chunks = 0;

  for (auto const& entry : fp_dict) {
    //const Key & key = entry.first;
    const unsigned count = entry.second;
    //unique_data     += params.chunk_size;
    //duplicated_data += (count-1)*(params.chunk_size);
    unique_data_chunks++;
    duplicated_data_chunks += (count-1);
    if (count < ARR_SIZE) {
      summery[count]++;
    }
    else {
      summery[ARR_SIZE]++;
    }
  }
  unique_data     = unique_data_chunks * params.chunk_size;
  duplicated_data = duplicated_data_chunks * params.chunk_size;
  unsigned sample_rate = 1;
  duplicated_data *= sample_rate;
  unique_data     *= sample_rate;
  //uint64_t total_size = duplicated_data + unique_data;
  std::cout << "============================================================" << std::endl;
  std::cout << "Summery:" << std::endl;
  std::cout << "Chunks repeating " << params.dedup_threshold << " times or more = " << unique_data_chunks << std::endl;

  //std::cout << "Unique Count = " << unique_data_chunks << std::endl;
  //<< ", Dup Count = " << duplicated_data_chunks << std::endl;
#if 0
  std::cout << "We had a total of " << total_size/1024 << " KiB stored in the system\n";
  std::cout << "We had " << unique_data/1024 << " Unique Data KiB stored in the system\n";
  std::cout << "We had " << duplicated_data/1024 << " Duplicated KiB Bytes stored in the system\n";
  std::cout << "Dedup Ratio = " << (double)total_size/(double)unique_data << std::endl;
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

constexpr unsigned RADOS_OBJ_SIZE  = (4 * 1024 * 1024);
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
    uint32_t crc;
    uint32_t pad32;

    DENC(FP_BUFFER_HEADER, v, p) {
      uint64_t *p_cur = (uint64_t*)v.cursor;
      uint64_t *p_end = (uint64_t*)(v.cursor + MAX_CURSOR_SIZE);
      for ( ; p_cur < p_end; p_cur++) {
	denc(*p_cur, p);
      }

      denc(v.fp_size, p);
      denc(v.cursor_len, p);
      denc(v.fp_count, p);
      denc(v.crc, p);
      denc(v.pad32, p);
    }

  }; // 1040 Bytes

  //---------------------------------------------------------------------------
  FP_BUFFER(unsigned fp_size, IoCtx *obj_ioctx, uint16_t thread_id, bool verbose, uint32_t part_number = 0) {
    d_byte_offset = FP_HEADER_LEN;
    d_fp_size     = fp_size;
    d_obj_ioctx   = obj_ioctx;
    d_thread_id   = thread_id;
    d_verbose     = verbose;
    d_part_number = part_number;
    assert(d_fp_size < MAX_FP);
  }

  //---------------------------------------------------------------------------
  int add_fp(const char *fp, const ObjectCursor &cursor) {
    if (unlikely(d_byte_offset+d_fp_size >= sizeof(d_buffer))) {
      if (0) {
	std::unique_lock<std::mutex> lock(print_mtx);
	cout << __func__ << ":: calling flush() byte_offset=" << d_byte_offset << std::endl;
      }
      return flush(cursor);
    }

    memcpy(d_buffer+d_byte_offset, fp, d_fp_size);
    d_byte_offset += d_fp_size;
    return 0;
  }

  //---------------------------------------------------------------------------
  int flush(const ObjectCursor &cursor)
  {
    struct FP_BUFFER_HEADER header;
    header.fp_size    = HTOCEPH_16(d_fp_size);
    uint16_t len      = cursor.to_str().copy(header.cursor, sizeof(header.cursor));
    header.cursor_len = HTOCEPH_16(len);
    header.fp_count   = HTOCEPH_32((d_byte_offset - FP_HEADER_LEN) / d_fp_size);
    header.pad32      = 0;
    header.crc        = 0;
    memcpy(d_buffer, (uint8_t*)&header, FP_HEADER_LEN);
    uint32_t crc = -1;
    crc = ceph_crc32c(crc, d_buffer, d_byte_offset);
    ((FP_BUFFER_HEADER*)d_buffer)->crc = HTOCEPH_32(crc);

    if (0) {
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << __func__ << ", byte_offset=" << d_byte_offset
	   << ", crc=0x" << hex << crc << dec << std::endl;
    }

    char name_buf[16];
    // make sure thread_id will fit in 2 hex digits 
    static_assert(THREAD_LIMIT_STRICT < 0xFF);
    unsigned n = snprintf(name_buf, sizeof(name_buf),
			  "hash%02X.%08X", d_thread_id, d_part_number);
    std::string oid(name_buf, n);
    if (d_verbose) {
      std::unique_lock<std::mutex> lock(print_mtx);
      cout << __func__ << "::oid=" << oid << std::endl;
    }
    bufferlist bl = bufferlist::static_from_mem((char*)d_buffer, d_byte_offset);
    d_obj_ioctx->write_full(oid, bl);
    d_byte_offset = FP_HEADER_LEN;
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
};
WRITE_CLASS_DENC(FP_BUFFER::FP_BUFFER_HEADER)
//---------------------------------------------------------------------------
static void calc_and_write_fp(FP_BUFFER *fp_buffer,
			      const uint8_t *p,
			      unsigned chunk_size,
			      const ObjectCursor &cursor)
{
  sha1_digest_t sha1 = ceph::crypto::digest<ceph::crypto::SHA1>(p, chunk_size);
  fp_buffer->add_fp((const char*)(sha1.v), cursor);
}

//---------------------------------------------------------------------------
static void calculate_fp(FP_BUFFER *fp_buffer,
			 bufferlist &bl,
			 unsigned chunk_size,
			 const ObjectCursor &cursor)
{
  uint8_t temp[chunk_size];
  auto bl_itr = bl.cbegin();

  for (unsigned i = 0; i < bl.length() / chunk_size; i++ ) {
    const char *p_src = nullptr;
    size_t n = bl_itr.get_ptr_and_advance(chunk_size, &p_src);
    if (n == chunk_size) {
      calc_and_write_fp(fp_buffer, (uint8_t*)p_src, chunk_size, cursor);
    }
    else {
      memcpy(temp, p_src, n);
      uint32_t count  = chunk_size - n;
      uint8_t *p_dest = temp + n;
      do {
	n = bl_itr.get_ptr_and_advance(count, &p_src);
	memcpy(p_dest, p_src, n);
	count  -= n;
	p_dest += n;
      } while (count > 0);
      calc_and_write_fp(fp_buffer, temp, chunk_size, cursor);
    }
  }
}

//---------------------------------------------------------------------------
static int thread_collect(const dedup_params_t &params, unsigned thread_id, uint64_t *p_bytes_read)
{
  uint64_t objs_read = 0, min_size = 0xFFFFFFFF, max_size = 0;
  unsigned max_objs = params.max_objs;
  *p_bytes_read = 0;
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }

  unique_ptr<FP_BUFFER> fp_buffer = std::make_unique<FP_BUFFER>(sha1_digest_t::SIZE, &obj_ioctx, thread_id, params.verbose);
  ObjectCursor split_start, split_finish;
  itr_ioctx.object_list_slice(itr_ioctx.object_list_begin(),
			      itr_ioctx.object_list_end(),
			      thread_id,
			      params.max_threads,
			      &split_start,
			      &split_finish);
  auto start_itr = itr_ioctx.nobjects_begin(split_start);
  for (auto itr = start_itr; itr.get_cursor() < split_finish; ++itr) {
    string oid = itr->get_oid();
    if (oid.starts_with("obj") == false) {
      continue;
    }
    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());
    bufferlist bl;
    int ret = obj_ioctx.read_full(oid, bl);
    if (unlikely(ret <= 0)) {
      cerr << "failed to read file: " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    objs_read++;
    *p_bytes_read += bl.length();
    min_size = std::min(min_size, (uint64_t)bl.length());
    max_size = std::max(max_size, (uint64_t)bl.length());
    calculate_fp(fp_buffer.get(), bl, params.chunk_size, itr.get_cursor());
    if (unlikely(max_objs && objs_read >= max_objs)) {
      break;
    }
  }
  if (0) {
    std::unique_lock<std::mutex> lock(print_mtx);
    cout << __func__ << ":: calling flush() " << std::endl;
  }
  fp_buffer->flush(split_finish);
  if (0 && objs_read > 0) {
    std::unique_lock<std::mutex> lock(print_mtx);
    cout << thread_id << "] Num Objects read=" << objs_read
	 << ", Num Bytes read=" << *p_bytes_read/(1024*1024) << " MiB"
	 << ", Max-Size=" << max_size / 1024 << " KiB"
	 << ", Min-Size=" << min_size / 1024 << " KiB"
	 << std::endl;
  }

  return 0;
}

//---------------------------------------------------------------------------
static int collect_fingerprints(const dedup_params_t &params, uint64_t *p_total_bytes_read)
{
  cout << "collect chunks finger prints" << std::endl;
  unsigned max_threads = params.max_threads;
  std::cout << "max_threads = " << max_threads << std::endl;

  std::thread* thread_arr[max_threads];
  memset(thread_arr, 0, sizeof(thread_arr));

  uint64_t bytes_read_arr[max_threads];
  memset(bytes_read_arr, 0, sizeof(bytes_read_arr));

  for (unsigned thread_id = 0; thread_id < max_threads; thread_id++) {
    thread_arr[thread_id] = new std::thread(thread_collect, params, thread_id, bytes_read_arr+thread_id);
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
  uint32_t crc;
  uint32_t pad32;

  DENC(REDUCED_FP_HEADER, v, p) {
    denc(v.fp_size, p);
    denc(v.shard_id, p);
    denc(v.threshold, p);
    denc(v.pad16, p);

    denc(v.fp_count, p);
    denc(v.serial_id, p);
    denc(v.crc, p);
    denc(v.pad32, p);
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
  header.fp_count  = HTOCEPH_32((offset - sizeof(REDUCED_FP_HEADER)) / fp_size);
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
  // make sure shard_id will fit in 3 hex digits 
  static_assert(SHARDS_COUNT_MAX < 0x999);
  unsigned n = snprintf(name_buf, sizeof(name_buf),
			"rdc%03X.%08X", shard_id, serial_id);
  std::string oid(name_buf, n);

  bufferlist bl = bufferlist::static_from_mem((char*)buffer, offset);
  obj_ioctx.write_full(oid, bl);
}

//---------------------------------------------------------------------------
static int store_reduced_fingerprints(const dedup_params_t &params,
				      const FP_Dict &fp_dict,
				      unsigned shard_id,
				      uint64_t *p_reduced_bytes,
				      IoCtx &obj_ioctx)
{
  uint8_t buffer[RADOS_OBJ_SIZE];
  struct REDUCED_FP_HEADER header;
  unsigned fp_size = Key::FP_SIZE; // TBD - use macro
  // fixed parts of the header which can be reuse
  header.fp_size   = HTOCEPH_16(fp_size);
  header.shard_id  = HTOCEPH_16(shard_id);
  header.threshold = HTOCEPH_16(params.dedup_threshold);
  header.pad16     = 0;
  header.pad32     = 0;

  uint32_t serial_id = 0;
  unsigned offset    = sizeof(REDUCED_FP_HEADER);
  for (auto const& entry : fp_dict) {
    const Key & key = entry.first;
    const unsigned count = entry.second;
    if (count >= params.dedup_threshold) {
      if (unlikely(offset+fp_size >= sizeof(buffer))) {
	reduced_fp_flush(header, buffer, offset, fp_size, shard_id, serial_id++, obj_ioctx);
	offset = sizeof(REDUCED_FP_HEADER);
      }
      memcpy(buffer+offset, key.get_fp_buff(), fp_size);
      offset += fp_size;

      //file.write((const char*)(key.get_fp_buff()), key.FP_SIZE);
      *p_reduced_bytes += (count-1)*params.chunk_size;
    }
  }

  if (offset > 0) {
    reduced_fp_flush(header, buffer, offset, fp_size, shard_id, serial_id++, obj_ioctx);
  }

  return 0;
}

//---------------------------------------------------------------------------
static int reduce_fingerprints_single_object(FP_Dict &fp_dict,
					     const dedup_params_t &params,
					     bufferlist &bl,
					     unsigned shard_id,
					     [[maybe_unused]]const ObjectCursor &cursor)
{
  uint64_t uniq_cnt = 0;
  uint64_t cnt = 0;
  FP_BUFFER::FP_BUFFER_HEADER fp_header;
  auto bl_itr = bl.cbegin();
  decode(fp_header, bl_itr);
  if (fp_header.pad32 != 0) {
    std::string_view sv(fp_header.cursor, fp_header.cursor_len);
    cout << "cursor   = " << sv << "\n"
	 << "fp_size  = " << fp_header.fp_size  << "\n"
	 << "fp_count = " << fp_header.fp_count  << "\n";
    exit (1);
  }
  assert(fp_header.pad32 == 0);

#if 0
  uint32_t buffer_crc = header.crc;
  header.crc = 0;
  uint32_t calc_crc = ceph_crc32c(-1, buffer, data_size);
  //cout << "buffer_crc=" << hex << buffer_crc << ", calc_crc=" << calc_crc << dec << std::endl;
  assert(calc_crc == buffer_crc);
#endif

  for (unsigned fp_idx = 0; fp_idx < fp_header.fp_count; fp_idx++) {
    Key key(bl_itr);
    if (key.get_shard(params.num_shards) == shard_id) {
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
  return 0;
}

//---------------------------------------------------------------------------
static int reduce_shard_fingerprints(const dedup_params_t &params,
				     unsigned shard_id,
				     uint64_t *p_reduced_bytes)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, params.pool_name.c_str()) != 0) {
    return -1;
  }

  FP_Dict fp_dict;
  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (oid.starts_with("hash") == false) {
      continue;
    }
    //cout << oid << std::endl;
    obj_ioctx.set_namespace(itr->get_nspace());
    obj_ioctx.locator_set_key(itr->get_locator());
    bufferlist bl;
    int ret = obj_ioctx.read_full(oid, bl);
    if (unlikely(ret <= 0)) {
      cerr << "failed to read file: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    reduce_fingerprints_single_object(fp_dict, params, bl, shard_id, itr.get_cursor());
  }
  if (params.num_shards == 1) {
    print_report(params, fp_dict, true);
  }
  store_reduced_fingerprints(params, fp_dict, shard_id, p_reduced_bytes, obj_ioctx);
  return 0;
}

//---------------------------------------------------------------------------
static int reduce_fingerprints(const dedup_params_t &params, uint64_t *p_reduced_bytes)
{
  cout << "reduce fingerprints, leaving only FP with "
       << params.dedup_threshold << " or more duplicates" << std::endl;
  for (unsigned shard_id = 0; shard_id < params.num_shards; shard_id++) {
    reduce_shard_fingerprints(params, shard_id, p_reduced_bytes);
  }

  return 0;
}

//================================================================================
//                                   M E R G E
//================================================================================

//---------------------------------------------------------------------------
static int merge_fingerprints_single_object(FP_Dict &fp_dict,
					    const dedup_params_t &params,
					    bufferlist &bl)
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
    Key key(bl_itr);
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

  FP_Dict fp_dict;
  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (oid.starts_with("rdc") == false) {
      continue;
    }
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
    merge_fingerprints_single_object(fp_dict, params, bl);
  }

  //store_merged_reduced_fingerprints(params, fp_dict, shard_id);
  print_report(params, fp_dict, false);

  return 0;

}

//---------------------------------------------------------------------------
static int clean_intermediate_objs(const string & pool_name, bool verbose)
{
  Rados rados;
  IoCtx itr_ioctx, obj_ioctx;
  if (setup_rados_objects(&rados, &itr_ioctx, &obj_ioctx, pool_name.c_str()) != 0) {
    return -1;
  }

  auto start_itr = itr_ioctx.nobjects_begin();
  auto end_itr   = itr_ioctx.nobjects_end();
  for (auto itr = start_itr; itr != end_itr; ++itr) {
    string oid = itr->get_oid();
    if (oid.starts_with("hash") || oid.starts_with("rdc")) {
      if (verbose ) {
	cout << "removing " << oid << std::endl;
      }
      obj_ioctx.set_namespace(itr->get_nspace());
      obj_ioctx.locator_set_key(itr->get_locator());
      int ret = obj_ioctx.remove(oid);
      if (ret != 0) {
	cerr << "failed to remove obj: " << oid
	     << ", error = " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
  }

  return 0;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--shards-count", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0 && count <= SHARDS_COUNT_MAX) {
	cout << "Shards Count was set to " << count << std::endl;
	params.num_shards = count;
      }
      else {
	cerr << "illegal shards-count" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-size", (char*)NULL)) {
      int size = atoi(val.c_str());
      if (size >= CHUNK_BASESIZE && (size % CHUNK_BASESIZE == 0)) {
	params.chunk_size = size;
	cout << "Chunk Size was set to " << size/1024 << " KiB" << std::endl;
      }
      else {
	cerr << "illegal chunk-size (must be a full multiplication of " << CHUNK_BASESIZE << ")" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--thread-count", (char*)NULL)) {
      int count = atoi(val.c_str());
      if (count > 0 && count <= THREAD_LIMIT_STRICT) {
	params.max_threads = count;
      }
      else {
	cerr << "illegal thread-count (must be between 1-" << THREAD_LIMIT_STRICT << ")" << std::endl;
	usage_exit();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--dedup_threshold", (char*)NULL)) {
      constexpr int DEDUP_THRESHOLD_MAX = 10;
      int count = atoi(val.c_str());
      if (count > 0 && count <= DEDUP_THRESHOLD_MAX) {
	params.dedup_threshold = count;
      }
      else {
	cerr << "illegal dedup-threshold (must be between 1-" <<
	  DEDUP_THRESHOLD_MAX << ")" << std::endl;
	usage_exit();
      }
    } else {
      if (val[0] == '-') {
	cerr << "Bad argument <" << val[0] << ">" << std::endl;
	usage_exit();
      }
      ++i;
    }
  }

  if (params.pool_name.length() == 0) {
    cerr << "No poolname was passed" << std::endl;
    usage_exit();
  }
}

//---------------------------------------------------------------------------
int main(int argc, char **argv)
{
  uint64_t total_bytes_before = 0;
  uint64_t reduced_bytes = 0;
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
  else if (strncmp(args[0], "all", strlen("all")) == 0) {
    clean_intermediate_objs(params.pool_name, params.verbose);
    collect_fingerprints(params, &total_bytes_before);
    reduce_fingerprints(params, &reduced_bytes);
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
  else if (strncmp(args[0], "merge-reduce", strlen("merge-reduce")) == 0) {
    reduce_fingerprints(params, &reduced_bytes);
    merge_reduced_fingerprints(params);
  }
  else if (strncmp(args[0], "collect", strlen("collect")) == 0) {
    collect_fingerprints(params, &total_bytes_before);
  }
  else if (strncmp(args[0], "reduce", strlen("reduce")) == 0) {
    reduce_fingerprints(params, &reduced_bytes);
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
