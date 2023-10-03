// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <bit>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

#include <boost/container/flat_set.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real.hpp>

#include "include/cpp-btree/btree_set.h"

#include "BlueStore.h"
#include "bluestore_common.h"
#include "simple_bitmap.h"
#include "os/kv.h"
#include "include/compat.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "include/str_map.h"
#include "include/util.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/PriorityCache.h"
#include "common/url_escape.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "BlueFS.h"
#include "BlueRocksEnv.h"
#include "auth/Crypto.h"
#include "common/EventTrace.h"
#include "perfglue/heap_profiler.h"
#include "common/blkdev.h"
#include "common/numa.h"
#include "common/pretty_binary.h"
#include "common/WorkQueue.h"
#include "kv/KeyValueHistogram.h"

#if defined(WITH_LTTNG)
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/bluestore.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

using bid_t = decltype(BlueStore::Blob::id);

// bluestore_cache_onode
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Onode, bluestore_onode,
			      bluestore_cache_onode);

MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Buffer, bluestore_buffer,
			      bluestore_cache_buffer);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Extent, bluestore_extent,
			      bluestore_extent);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Blob, bluestore_blob,
			      bluestore_blob);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::SharedBlob, bluestore_shared_blob,
			      bluestore_shared_blob);

// bluestore_txc
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::TransContext, bluestore_transcontext,
			      bluestore_txc);
using std::byte;
using std::deque;
using std::min;
using std::make_pair;
using std::numeric_limits;
using std::pair;
using std::less;
using std::list;
using std::make_unique;
using std::map;
using std::max;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::coarse_mono_clock;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_timespan;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;

// kv store prefixes
const string PREFIX_SUPER = "S";       // field -> value
const string PREFIX_STAT = "T";        // field -> value(int64 array)
const string PREFIX_COLL = "C";        // collection name -> cnode_t
const string PREFIX_OBJ = "O";         // object name -> onode_t
const string PREFIX_OMAP = "M";        // u64 + keyname -> value
const string PREFIX_PGMETA_OMAP = "P"; // u64 + keyname -> value(for meta coll)
const string PREFIX_PERPOOL_OMAP = "m"; // s64 + u64 + keyname -> value
const string PREFIX_PERPG_OMAP = "p";   // u64(pool) + u32(hash) + u64(id) + keyname -> value
const string PREFIX_DEFERRED = "L";    // id -> deferred_transaction_t
const string PREFIX_ALLOC = "B";       // u64 offset -> u64 length (freelist)
const string PREFIX_ALLOC_BITMAP = "b";// (see BitmapFreelistManager)
const string PREFIX_SHARED_BLOB = "X"; // u64 SB id -> shared_blob_t

const string BLUESTORE_GLOBAL_STATFS_KEY = "bluestore_statfs";

#define OBJECT_MAX_SIZE 0xffffffff // 32 bits


/*
 * extent map blob encoding
 *
 * we use the low bits of the blobid field to indicate some common scenarios
 * and spanning vs local ids.  See ExtentMap::{encode,decode}_some().
 */
#define BLOBID_FLAG_CONTIGUOUS 0x1  // this extent starts at end of previous
#define BLOBID_FLAG_ZEROOFFSET 0x2  // blob_offset is 0
#define BLOBID_FLAG_SAMELENGTH 0x4  // length matches previous extent
#define BLOBID_FLAG_SPANNING   0x8  // has spanning blob id
#define BLOBID_SHIFT_BITS        4

/*
 * object name key structure
 *
 * encoded u8: shard + 2^7 (so that it sorts properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * escaped string: namespace
 *
 * escaped string: key or object name
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are done.  otherwise, we are followed by the object name.
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 * 'o'
 */
#define ONODE_KEY_SUFFIX 'o'

/*
 * extent shard key
 *
 * object prefix key
 * u32
 * 'x'
 */
#define EXTENT_SHARD_KEY_SUFFIX 'x'

/*
 * string encoding in the key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '#' with #
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < #
 * and will get escaped if it is present in the string.
 *
 * NOTE: There is a bug in this implementation: due to implicit
 * character type conversion in comparison it may produce unexpected
 * ordering. Unfortunately fixing the bug would mean invalidating the
 * keys in existing deployments. Instead we do additional sorting
 * where it is needed.
 */
template<typename S>
static void append_escaped(const string &in, S *out)
{
  char hexbyte[in.length() * 3 + 1];
  char* ptr = &hexbyte[0];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') { // bug: unexpected result for *i > 0x7f
      *ptr++ = '#';
      *ptr++ = "0123456789abcdef"[(*i >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[*i & 0x0f];
    } else if (*i >= '~') { // bug: unexpected result for *i > 0x7f
      *ptr++ = '~';
      *ptr++ = "0123456789abcdef"[(*i >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[*i & 0x0f];
    } else {
      *ptr++  = *i;
    }
  }
  *ptr++ = '!';
  out->append(hexbyte, ptr - &hexbyte[0]);
}

inline unsigned h2i(char c)
{
  if ((c >= '0') && (c <= '9')) {
    return c - 0x30;
  } else if ((c >= 'a') && (c <= 'f')) {
    return c - 'a' + 10;
  } else if ((c >= 'A') && (c <= 'F')) {
    return c - 'A' + 10;
  } else {
    return 256; // make it always larger than 255
  }
}

static int decode_escaped(const char *p, string *out)
{
  char buff[256];
  char* ptr = &buff[0];
  char* max = &buff[252];
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex = 0;
      p++;
      hex = h2i(*p++) << 4;
      if (hex > 255) {
        return -EINVAL;
      }
      hex |= h2i(*p++);
      if (hex > 255) {
        return -EINVAL;
      }
      *ptr++ = hex;
    } else {
      *ptr++ = *p++;
    }
    if (ptr > max) {
       out->append(buff, ptr-buff);
       ptr = &buff[0];
    }
  }
  if (ptr != buff) {
     out->append(buff, ptr-buff);
  }
  return p - orig_p;
}

template<typename T>
static void _key_encode_shard(shard_id_t shard, T *key)
{
  key->push_back((char)((uint8_t)shard.id + (uint8_t)0x80));
}

static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  pshard->id = (uint8_t)*key - (uint8_t)0x80;
  return key + 1;
}

static void get_coll_range(const coll_t& cid, int bits,
  ghobject_t *temp_start, ghobject_t *temp_end,
  ghobject_t *start, ghobject_t *end, bool legacy)
{
  spg_t pgid;
  constexpr uint32_t MAX_HASH = std::numeric_limits<uint32_t>::max();
  // use different nspaces due to we use different schemes when encoding
  // keys for listing objects
  const std::string_view MAX_NSPACE = legacy ? "\x7f" : "\xff";
  if (cid.is_pg(&pgid)) {
    start->shard_id = pgid.shard;
    *temp_start = *start;

    start->hobj.pool = pgid.pool();
    temp_start->hobj.pool = -2ll - pgid.pool();

    *end = *start;
    *temp_end = *temp_start;

    uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
    start->hobj.set_bitwise_key_u32(reverse_hash);
    temp_start->hobj.set_bitwise_key_u32(reverse_hash);

    uint64_t end_hash = reverse_hash  + (1ull << (32 - bits));
    if (end_hash > MAX_HASH) {
      // make sure end hobj is even greater than the maximum possible hobj
      end->hobj.set_bitwise_key_u32(MAX_HASH);
      temp_end->hobj.set_bitwise_key_u32(MAX_HASH);
      end->hobj.nspace = MAX_NSPACE;
    } else {
      end->hobj.set_bitwise_key_u32(end_hash);
      temp_end->hobj.set_bitwise_key_u32(end_hash);
    }
  } else {
    start->shard_id = shard_id_t::NO_SHARD;
    start->hobj.pool = -1ull;

    *end = *start;
    start->hobj.set_bitwise_key_u32(0);
    end->hobj.set_bitwise_key_u32(MAX_HASH);
    end->hobj.nspace = MAX_NSPACE;
    // no separate temp section
    *temp_start = *end;
    *temp_end = *end;
  }

  start->generation = 0;
  end->generation = 0;
  temp_start->generation = 0;
  temp_end->generation = 0;
}

static void get_shared_blob_key(uint64_t sbid, string *key)
{
  key->clear();
  _key_encode_u64(sbid, key);
}

static int get_key_shared_blob(const string& key, uint64_t *sbid)
{
  const char *p = key.c_str();
  if (key.length() < sizeof(uint64_t))
    return -1;
  _key_decode_u64(p, sbid);
  return 0;
}

template<typename S>
static void _key_encode_prefix(const ghobject_t& oid, S *key)
{
  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
}

static const char *_key_decode_prefix(const char *p, ghobject_t *oid)
{
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);

  oid->hobj.set_bitwise_key_u32(hash);

  return p;
}


#define ENCODED_KEY_PREFIX_LEN (1 + 8 + 4)

static int _get_key_object(const char *p, ghobject_t *oid)
{
  int r;

  p = _key_decode_prefix(p, oid);

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -2;
  p += r + 1;

  string k;
  r = decode_escaped(p, &k);
  if (r < 0)
    return -3;
  p += r + 1;
  if (*p == '=') {
    // no key
    ++p;
    oid->hobj.oid.name = k;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -5;
    p += r + 1;
    oid->hobj.set_key(k);
  } else {
    // malformed
    return -6;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);

  if (*p != ONODE_KEY_SUFFIX) {
    return -7;
  }
  p++;
  if (*p) {
    // if we get something other than a null terminator here,
    // something goes wrong.
    return -8;
  }

  return 0;
}

template<typename S>
static int get_key_object(const S& key, ghobject_t *oid)
{
  if (key.length() < ENCODED_KEY_PREFIX_LEN)
    return -1;
  if (key.length() == ENCODED_KEY_PREFIX_LEN)
    return -2;
  const char *p = key.c_str();
  return _get_key_object(p, oid);
}

template<typename S>
static void _get_object_key(const ghobject_t& oid, S *key)
{
  size_t max_len = ENCODED_KEY_PREFIX_LEN +
                  (oid.hobj.nspace.length() * 3 + 1) +
                  (oid.hobj.get_key().length() * 3 + 1) +
                   1 + // for '<', '=', or '>'
                  (oid.hobj.oid.name.length() * 3 + 1) +
                   8 + 8 + 1;
  key->reserve(max_len);

  _key_encode_prefix(oid, key);

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    append_escaped(oid.hobj.get_key(), key);
    // (ASCII chars < = and > sort in that order, yay)
    int r = oid.hobj.get_key().compare(oid.hobj.oid.name);
    if (r) {
      key->append(r > 0 ? ">" : "<");
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
    }
  } else {
    // no key
    append_escaped(oid.hobj.oid.name, key);
    key->append("=");
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  key->push_back(ONODE_KEY_SUFFIX);
}

template<typename S>
static void get_object_key(CephContext *cct, const ghobject_t& oid, S *key)
{
  key->clear();
  _get_object_key(oid, key);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      ceph_assert(r == 0 && t == oid);
    }
  }
}

// extent shard keys are the onode key, plus a u32, plus 'x'.  the trailing
// char lets us quickly test whether it is a shard key without decoding any
// of the prefix bytes.
template<typename S>
static void get_extent_shard_key(const S& onode_key, uint32_t offset,
				 string *key)
{
  key->clear();
  key->reserve(onode_key.length() + 4 + 1);
  key->append(onode_key.c_str(), onode_key.size());
  _key_encode_u32(offset, key);
  key->push_back(EXTENT_SHARD_KEY_SUFFIX);
}

static void rewrite_extent_shard_key(uint32_t offset, string *key)
{
  ceph_assert(key->size() > sizeof(uint32_t) + 1);
  ceph_assert(*key->rbegin() == EXTENT_SHARD_KEY_SUFFIX);
  _key_encode_u32(offset, key->size() - sizeof(uint32_t) - 1, key);
}

template<typename S>
static void generate_extent_shard_key_and_apply(
  const S& onode_key,
  uint32_t offset,
  string *key,
  std::function<void(const string& final_key)> apply)
{
  if (key->empty()) { // make full key
    ceph_assert(!onode_key.empty());
    get_extent_shard_key(onode_key, offset, key);
  } else {
    rewrite_extent_shard_key(offset, key);
  }
  apply(*key);
}

int get_key_extent_shard(const string& key, string *onode_key, uint32_t *offset)
{
  ceph_assert(key.size() > sizeof(uint32_t) + 1);
  ceph_assert(*key.rbegin() == EXTENT_SHARD_KEY_SUFFIX);
  int okey_len = key.size() - sizeof(uint32_t) - 1;
  *onode_key = key.substr(0, okey_len);
  const char *p = key.data() + okey_len;
  _key_decode_u32(p, offset);
  return 0;
}

static bool is_extent_shard_key(const string& key)
{
  return *key.rbegin() == EXTENT_SHARD_KEY_SUFFIX;
}

static void get_deferred_key(uint64_t seq, string *out)
{
  _key_encode_u64(seq, out);
}

static void get_pool_stat_key(int64_t pool_id, string *key)
{
  key->clear();
  _key_encode_u64(pool_id, key);
}

static int get_key_pool_stat(const string& key, uint64_t* pool_id)
{
  const char *p = key.c_str();
  if (key.length() < sizeof(uint64_t))
    return -1;
  _key_decode_u64(p, pool_id);
  return 0;
}


template <int LogLevelV>
void _dump_extent_map(CephContext *cct, const BlueStore::ExtentMap &em)
{
  uint64_t pos = 0;
  for (auto& s : em.shards) {
    dout(LogLevelV) << __func__ << "  shard " << *s.shard_info
		    << (s.loaded ? " (loaded)" : "")
		    << (s.dirty ? " (dirty)" : "")
		    << dendl;
  }
  for (auto& e : em.extent_map) {
    dout(LogLevelV) << __func__ << "  " << e << dendl;
    ceph_assert(e.logical_offset >= pos);
    pos = e.logical_offset + e.length;
    const bluestore_blob_t& blob = e.blob->get_blob();
    if (blob.has_csum()) {
      vector<uint64_t> v;
      unsigned n = blob.get_csum_count();
      for (unsigned i = 0; i < n; ++i)
	v.push_back(blob.get_csum_item(i));
      dout(LogLevelV) << __func__ << "      csum: " << std::hex << v << std::dec
		      << dendl;
    }
    std::lock_guard l(e.blob->get_cache()->lock);
    for (auto& i : e.blob->get_bc().buffer_map) {
      dout(LogLevelV) << __func__ << "       0x" << std::hex << i.first
		      << "~" << i.second->length << std::dec
		      << " " << *i.second << dendl;
    }
  }
}

template <int LogLevelV>
void _dump_onode(CephContext *cct, const BlueStore::Onode& o)
{
  if (!cct->_conf->subsys.should_gather<ceph_subsys_bluestore, LogLevelV>())
    return;
  dout(LogLevelV) << __func__ << " " << &o << " " << o.oid
		  << " nid " << o.onode.nid
		  << " size 0x" << std::hex << o.onode.size
		  << " (" << std::dec << o.onode.size << ")"
		  << " expected_object_size " << o.onode.expected_object_size
		  << " expected_write_size " << o.onode.expected_write_size
		  << " in " << o.onode.extent_map_shards.size() << " shards"
		  << ", " << o.extent_map.spanning_blob_map.size()
		  << " spanning blobs"
		  << dendl;
  for (auto& [zone, offset] : o.onode.zone_offset_refs) {
    dout(LogLevelV) << __func__ << " zone ref 0x" << std::hex << zone
		    << " offset 0x" << offset << std::dec << dendl;
  }
  for (auto p = o.onode.attrs.begin();
       p != o.onode.attrs.end();
       ++p) {
    dout(LogLevelV) << __func__ << "  attr " << p->first
		    << " len " << p->second.length() << dendl;
  }
  _dump_extent_map<LogLevelV>(cct, o.extent_map);
}

template <int LogLevelV>
void _dump_transaction(CephContext *cct, ObjectStore::Transaction *t)
{
  dout(LogLevelV) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t->dump(&f);
  f.close_section();
  f.flush(*_dout);
  *_dout << dendl;
}

// Buffer

ostream& operator<<(ostream& out, const BlueStore::Buffer& b)
{
  out << "buffer(" << &b << " space " << b.space << " 0x" << std::hex
      << b.offset << "~" << b.length << std::dec
      << " " << BlueStore::Buffer::get_state_name(b.state);
  if (b.flags)
    out << " " << BlueStore::Buffer::get_flag_name(b.flags);
  return out << ")";
}

namespace {

/*
 * Due to a bug in key string encoding (see a comment for append_escaped)
 * the KeyValueDB iterator does not lexicographically sort the same
 * way that ghobject_t does: objects with the same hash may have wrong order.
 *
 * This is the iterator wrapper that fixes the keys order.
 */

class CollectionListIterator {
public:
  CollectionListIterator(const KeyValueDB::Iterator &it)
    : m_it(it) {
  }
  virtual ~CollectionListIterator() {
  }

  virtual bool valid() const = 0;
  virtual const ghobject_t &oid() const = 0;
  virtual void lower_bound(const ghobject_t &oid) = 0;
  virtual void upper_bound(const ghobject_t &oid) = 0;
  virtual void next() = 0;

  virtual int cmp(const ghobject_t &oid) const = 0;

  bool is_ge(const ghobject_t &oid) const {
    return cmp(oid) >= 0;
  }

  bool is_lt(const ghobject_t &oid) const {
    return cmp(oid) < 0;
  }

protected:
  KeyValueDB::Iterator m_it;
};

class SimpleCollectionListIterator : public CollectionListIterator {
public:
  SimpleCollectionListIterator(CephContext *cct, const KeyValueDB::Iterator &it)
    : CollectionListIterator(it), m_cct(cct) {
  }

  bool valid() const override {
    return m_it->valid();
  }

  const ghobject_t &oid() const override {
    ceph_assert(valid());

    return m_oid;
  }

  void lower_bound(const ghobject_t &oid) override {
    string key;
    get_object_key(m_cct, oid, &key);

    m_it->lower_bound(key);
    get_oid();
  }

  void upper_bound(const ghobject_t &oid) override {
    string key;
    get_object_key(m_cct, oid, &key);

    m_it->upper_bound(key);
    get_oid();
  }

  void next() override {
    ceph_assert(valid());

    m_it->next();
    get_oid();
  }

  int cmp(const ghobject_t &oid) const override {
    ceph_assert(valid());

    string key;
    get_object_key(m_cct, oid, &key);

    return m_it->key().compare(key);
  }

private:
  CephContext *m_cct;
  ghobject_t m_oid;

  void get_oid() {
    m_oid = ghobject_t();
    while (m_it->valid() && is_extent_shard_key(m_it->key())) {
      m_it->next();
    }
    if (!valid()) {
      return;
    }

    int r = get_key_object(m_it->key(), &m_oid);
    ceph_assert(r == 0);
  }
};

class SortedCollectionListIterator : public CollectionListIterator {
public:
  SortedCollectionListIterator(const KeyValueDB::Iterator &it)
    : CollectionListIterator(it), m_chunk_iter(m_chunk.end()) {
  }

  bool valid() const override {
    return m_chunk_iter != m_chunk.end();
  }

  const ghobject_t &oid() const override {
    ceph_assert(valid());

    return m_chunk_iter->first;
  }

  void lower_bound(const ghobject_t &oid) override {
    std::string key;
    _key_encode_prefix(oid, &key);

    m_it->lower_bound(key);
    m_chunk_iter = m_chunk.end();
    if (!get_next_chunk()) {
      return;
    }

    if (this->oid().shard_id != oid.shard_id ||
        this->oid().hobj.pool != oid.hobj.pool ||
        this->oid().hobj.get_bitwise_key_u32() != oid.hobj.get_bitwise_key_u32()) {
      return;
    }

    m_chunk_iter = m_chunk.lower_bound(oid);
    if (m_chunk_iter == m_chunk.end()) {
      get_next_chunk();
    }
  }

  void upper_bound(const ghobject_t &oid) override {
    lower_bound(oid);

    if (valid() && this->oid() == oid) {
      next();
    }
  }

  void next() override {
    ceph_assert(valid());

    m_chunk_iter++;
    if (m_chunk_iter == m_chunk.end()) {
      get_next_chunk();
    }
  }

  int cmp(const ghobject_t &oid) const override {
    ceph_assert(valid());

    if (this->oid() < oid) {
      return -1;
    }
    if (this->oid() > oid) {
      return 1;
    }
    return 0;
  }

private:
  std::map<ghobject_t, std::string> m_chunk;
  std::map<ghobject_t, std::string>::iterator m_chunk_iter;

  bool get_next_chunk() {
    while (m_it->valid() && is_extent_shard_key(m_it->key())) {
      m_it->next();
    }

    if (!m_it->valid()) {
      return false;
    }

    ghobject_t oid;
    int r = get_key_object(m_it->key(), &oid);
    ceph_assert(r == 0);

    m_chunk.clear();
    while (true) {
      m_chunk.insert({oid, m_it->key()});

      do {
        m_it->next();
      } while (m_it->valid() && is_extent_shard_key(m_it->key()));

      if (!m_it->valid()) {
        break;
      }

      ghobject_t next;
      r = get_key_object(m_it->key(), &next);
      ceph_assert(r == 0);
      if (next.shard_id != oid.shard_id ||
          next.hobj.pool != oid.hobj.pool ||
          next.hobj.get_bitwise_key_u32() != oid.hobj.get_bitwise_key_u32()) {
        break;
      }
      oid = next;
    }

    m_chunk_iter = m_chunk.begin();
    return true;
  }
};

} // anonymous namespace

// Garbage Collector

void BlueStore::GarbageCollector::process_protrusive_extents(
  const BlueStore::ExtentMap& extent_map, 
  uint64_t start_offset,
  uint64_t end_offset,
  uint64_t start_touch_offset,
  uint64_t end_touch_offset,
  uint64_t min_alloc_size)
{
  ceph_assert(start_offset <= start_touch_offset && end_offset>= end_touch_offset);

  uint64_t lookup_start_offset = p2align(start_offset, min_alloc_size);
  uint64_t lookup_end_offset = round_up_to(end_offset, min_alloc_size);

  dout(30) << __func__ << " (hex): [" << std::hex
           << lookup_start_offset << ", " << lookup_end_offset 
           << ")" << std::dec << dendl;

  for (auto it = extent_map.seek_lextent(lookup_start_offset);
       it != extent_map.extent_map.end() &&
         it->logical_offset < lookup_end_offset;
       ++it) {
    uint64_t alloc_unit_start = it->logical_offset / min_alloc_size;
    uint64_t alloc_unit_end = (it->logical_end() - 1) / min_alloc_size;

    dout(30) << __func__ << " " << *it
             << "alloc_units: " << alloc_unit_start << ".." << alloc_unit_end
             << dendl;

    Blob* b = it->blob.get();

    if (it->logical_offset >=start_touch_offset &&
        it->logical_end() <= end_touch_offset) {
      // Process extents within the range affected by 
      // the current write request.
      // Need to take into account if existing extents
      // can be merged with them (uncompressed case)
      if (!b->get_blob().is_compressed()) {
        if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
	  --blob_info_counted->expected_allocations; // don't need to allocate
                                                     // new AU for compressed
                                                     // data since another
                                                     // collocated uncompressed
                                                     // blob already exists
          dout(30) << __func__  << " --expected:"
                   << alloc_unit_start << dendl;
        }
        used_alloc_unit = alloc_unit_end;
        blob_info_counted =  nullptr;
      }
    } else if (b->get_blob().is_compressed()) {

      // additionally we take compressed blobs that were not impacted
      // by the write into account too
      BlobInfo& bi =
        affected_blobs.emplace(
          b, BlobInfo(b->get_referenced_bytes())).first->second;

      int adjust =
       (used_alloc_unit && used_alloc_unit == alloc_unit_start) ? 0 : 1;
      bi.expected_allocations += alloc_unit_end - alloc_unit_start + adjust;
      dout(30) << __func__  << " expected_allocations=" 
               << bi.expected_allocations << " end_au:"
               << alloc_unit_end << dendl;

      blob_info_counted =  &bi;
      used_alloc_unit = alloc_unit_end;

      ceph_assert(it->length <= bi.referenced_bytes);
       bi.referenced_bytes -= it->length;
      dout(30) << __func__ << " affected_blob:" << *b
               << " unref 0x" << std::hex << it->length
               << " referenced = 0x" << bi.referenced_bytes
               << std::dec << dendl;
      // NOTE: we can't move specific blob to resulting GC list here
      // when reference counter == 0 since subsequent extents might
      // decrement its expected_allocation. 
      // Hence need to enumerate all the extents first.
      if (!bi.collect_candidate) {
        bi.first_lextent = it;
        bi.collect_candidate = true;
      }
      bi.last_lextent = it;
    } else {
      if (blob_info_counted && used_alloc_unit == alloc_unit_start) {
        // don't need to allocate new AU for compressed data since another
        // collocated uncompressed blob already exists
    	--blob_info_counted->expected_allocations;
        dout(30) << __func__  << " --expected_allocations:"
		 << alloc_unit_start << dendl;
      }
      used_alloc_unit = alloc_unit_end;
      blob_info_counted = nullptr;
    }
  }

  for (auto b_it = affected_blobs.begin();
       b_it != affected_blobs.end();
       ++b_it) {
    Blob* b = b_it->first;
    BlobInfo& bi = b_it->second;
    if (bi.referenced_bytes == 0) {
      uint64_t len_on_disk = b_it->first->get_blob().get_ondisk_length();
      int64_t blob_expected_for_release =
        round_up_to(len_on_disk, min_alloc_size) / min_alloc_size;

      dout(30) << __func__ << " " << *(b_it->first)
               << " expected4release=" << blob_expected_for_release
               << " expected_allocations=" << bi.expected_allocations
               << dendl;
      int64_t benefit = blob_expected_for_release - bi.expected_allocations;
      if (benefit >= g_conf()->bluestore_gc_enable_blob_threshold) {
        if (bi.collect_candidate) {
          auto it = bi.first_lextent;
          bool bExit = false;
          do {
            if (it->blob.get() == b) {
              extents_to_collect.insert(it->logical_offset, it->length);
            }
            bExit = it == bi.last_lextent;
            ++it;
          } while (!bExit);
        }
        expected_for_release += blob_expected_for_release;
        expected_allocations += bi.expected_allocations;
      }
    }
  }
}

int64_t BlueStore::GarbageCollector::estimate(
  uint64_t start_offset,
  uint64_t length,
  const BlueStore::ExtentMap& extent_map,
  const BlueStore::old_extent_map_t& old_extents,
  uint64_t min_alloc_size)
{

  affected_blobs.clear();
  extents_to_collect.clear();
  used_alloc_unit = boost::optional<uint64_t >();
  blob_info_counted = nullptr;

  uint64_t gc_start_offset = start_offset;
  uint64_t gc_end_offset = start_offset + length;

  uint64_t end_offset = start_offset + length;

  for (auto it = old_extents.begin(); it != old_extents.end(); ++it) {
    Blob* b = it->e.blob.get();
    if (b->get_blob().is_compressed()) {

      // update gc_start_offset/gc_end_offset if needed
      gc_start_offset = min(gc_start_offset, (uint64_t)it->e.blob_start());
      gc_end_offset = std::max(gc_end_offset, (uint64_t)it->e.blob_end());

      auto o = it->e.logical_offset;
      auto l = it->e.length;

      uint64_t ref_bytes = b->get_referenced_bytes();
      // micro optimization to bypass blobs that have no more references
      if (ref_bytes != 0) {
        dout(30) << __func__ << " affected_blob:" << *b
                 << " unref 0x" << std::hex << o << "~" << l
                 << std::dec << dendl;
	affected_blobs.emplace(b, BlobInfo(ref_bytes));
      }
    }
  }
  dout(30) << __func__ << " gc range(hex): [" << std::hex
           << gc_start_offset << ", " << gc_end_offset 
           << ")" << std::dec << dendl;

  // enumerate preceeding extents to check if they reference affected blobs
  if (gc_start_offset < start_offset || gc_end_offset > end_offset) {
    process_protrusive_extents(extent_map,
                               gc_start_offset,
			       gc_end_offset,
			       start_offset,
			       end_offset,
			       min_alloc_size);
  }
  return expected_for_release - expected_allocations;
}

// LruOnodeCacheShard
struct LruOnodeCacheShard : public BlueStore::OnodeCacheShard {
  typedef boost::intrusive::list<
    BlueStore::Onode,
    boost::intrusive::member_hook<
      BlueStore::Onode,
      boost::intrusive::list_member_hook<>,
      &BlueStore::Onode::lru_item> > list_t;

  list_t lru;

  explicit LruOnodeCacheShard(CephContext *cct) : BlueStore::OnodeCacheShard(cct) {}

  void _add(BlueStore::Onode* o, int level) override
  {
    o->set_cached();
    if (o->pin_nref == 1) {
      (level > 0) ? lru.push_front(*o) : lru.push_back(*o);
      o->cache_age_bin = age_bins.front();
      *(o->cache_age_bin) += 1;
    }
    ++num; // we count both pinned and unpinned entries
    dout(20) << __func__ << " " << this << " " << o->oid << " added, num="
             << num << dendl;
  }
  void _rm(BlueStore::Onode* o) override
  {
    o->clear_cached();
    if (o->lru_item.is_linked()) {
      *(o->cache_age_bin) -= 1;
      lru.erase(lru.iterator_to(*o));
    }
    ceph_assert(num);
    --num;
    dout(20) << __func__ << " " << this << " " << " " << o->oid << " removed, num=" << num << dendl;
  }

  void maybe_unpin(BlueStore::Onode* o) override
  {
    OnodeCacheShard* ocs = this;
    ocs->lock.lock();
    // It is possible that during waiting split_cache moved us to different OnodeCacheShard.
    while (ocs != o->c->get_onode_cache()) {
      ocs->lock.unlock();
      ocs = o->c->get_onode_cache();
      ocs->lock.lock();
    }
    if (o->is_cached() && o->pin_nref == 1) {
      if(!o->lru_item.is_linked()) {
        if (o->exists) {
	  lru.push_front(*o);
	  o->cache_age_bin = age_bins.front();
	  *(o->cache_age_bin) += 1;
	  dout(20) << __func__ << " " << this << " " << o->oid << " unpinned"
                   << dendl;
        } else {
	  ceph_assert(num);
	  --num;
	  o->clear_cached();
	  dout(20) << __func__ << " " << this << " " << o->oid << " removed"
                   << dendl;
          // remove will also decrement nref
          o->c->onode_space._remove(o->oid);
        }
      } else if (o->exists) {
        // move onode within LRU
        lru.erase(lru.iterator_to(*o));
        lru.push_front(*o);
        if (o->cache_age_bin != age_bins.front()) {
          *(o->cache_age_bin) -= 1;
          o->cache_age_bin = age_bins.front();
          *(o->cache_age_bin) += 1;
        }
        dout(20) << __func__ << " " << this << " " << o->oid << " touched"
                 << dendl;
      }
    }
    ocs->lock.unlock();
  }

  void _trim_to(uint64_t new_size) override
  {
    if (new_size >= lru.size()) {
      return; // don't even try
    } 
    uint64_t n = num - new_size; // note: we might get empty LRU
                                 // before n == 0 due to pinned
                                 // entries. And hence being unable
                                 // to reach new_size target.
    while (n-- > 0 && lru.size() > 0) {
      BlueStore::Onode *o = &lru.back();
      lru.pop_back();

      dout(20) << __func__ << "  rm " << o->oid << " "
               << o->nref << " " << o->cached << dendl;

      *(o->cache_age_bin) -= 1;
      if (o->pin_nref > 1) {
        dout(20) << __func__ << " " << this << " " << " " << " " << o->oid << dendl;
      } else {
	ceph_assert(num);
        --num;
        o->clear_cached();
        o->c->onode_space._remove(o->oid);
      }
    }
  }
  void _move_pinned(OnodeCacheShard *to, BlueStore::Onode *o) override
  {
    if (to == this) {
      return;
    }
    _rm(o);
    ceph_assert(o->nref > 1);
    to->_add(o, 0);
  }
  void add_stats(uint64_t *onodes, uint64_t *pinned_onodes) override
  {
    std::lock_guard l(lock);
    *onodes += num;
    *pinned_onodes += num - lru.size();
  }
#ifdef DEBUG_CACHE
  void _audit(const char *when) override
  {
  }
#endif
};

// OnodeCacheShard
BlueStore::OnodeCacheShard *BlueStore::OnodeCacheShard::create(
    CephContext* cct,
    string type,
    PerfCounters *logger)
{
  BlueStore::OnodeCacheShard *c = nullptr;
  // Currently we only implement an LRU cache for onodes
  c = new LruOnodeCacheShard(cct);
  c->logger = logger;
  return c;
}

// LruBufferCacheShard
struct LruBufferCacheShard : public BlueStore::BufferCacheShard {
  typedef boost::intrusive::list<
    BlueStore::Buffer,
    boost::intrusive::member_hook<
      BlueStore::Buffer,
      boost::intrusive::list_member_hook<>,
      &BlueStore::Buffer::lru_item> > list_t;
  list_t lru;

  explicit LruBufferCacheShard(CephContext *cct) : BlueStore::BufferCacheShard(cct) {}

  void _add(BlueStore::Buffer *b, int level, BlueStore::Buffer *near) override {
    if (near) {
      auto q = lru.iterator_to(*near);
      lru.insert(q, *b);
    } else if (level > 0) {
      lru.push_front(*b);
    } else {
      lru.push_back(*b);
    }
    buffer_bytes += b->length;
    b->cache_age_bin = age_bins.front();
    *(b->cache_age_bin) += b->length;
    num = lru.size();
  }
  void _rm(BlueStore::Buffer *b) override {
    ceph_assert(buffer_bytes >= b->length);
    buffer_bytes -= b->length;
    assert(*(b->cache_age_bin) >= b->length);
    *(b->cache_age_bin) -= b->length;
    auto q = lru.iterator_to(*b);
    lru.erase(q);
    num = lru.size();
  }
  void _move(BlueStore::BufferCacheShard *src, BlueStore::Buffer *b) override {
    src->_rm(b);
    _add(b, 0, nullptr);
  }
  void _adjust_size(BlueStore::Buffer *b, int64_t delta) override {
    ceph_assert((int64_t)buffer_bytes + delta >= 0);
    buffer_bytes += delta;
    assert(*(b->cache_age_bin) + delta >= 0);
    *(b->cache_age_bin) += delta;
  }
  void _touch(BlueStore::Buffer *b) override {
    auto p = lru.iterator_to(*b);
    lru.erase(p);
    lru.push_front(*b);
    *(b->cache_age_bin) -= b->length;
    b->cache_age_bin = age_bins.front();
    *(b->cache_age_bin) += b->length;
    num = lru.size();
    _audit("_touch_buffer end");
  }

  void _trim_to(uint64_t max) override
  {
    while (buffer_bytes > max) {
      auto i = lru.rbegin();
      if (i == lru.rend()) {
        // stop if lru is now empty
        break;
      }

      BlueStore::Buffer *b = &*i;
      ceph_assert(b->is_clean());
      dout(20) << __func__ << " rm " << *b << dendl;
      assert(*(b->cache_age_bin) >= b->length);
      *(b->cache_age_bin) -= b->length;
      b->space->_rm_buffer(this, b);
    }
    num = lru.size();
  }

  void add_stats(uint64_t *extents,
                 uint64_t *blobs,
                 uint64_t *buffers,
                 uint64_t *bytes) override {
    std::lock_guard l(lock);
    *extents += num_extents;
    *blobs += num_blobs;
    *buffers += num;
    *bytes += buffer_bytes;
  }
#ifdef DEBUG_CACHE
  void _audit(const char *when) override
  {
    dout(10) << __func__ << " " << when << " start" << dendl;
    uint64_t s = 0;
    for (auto i = lru.begin(); i != lru.end(); ++i) {
      s += i->length;
    }
    if (s != buffer_bytes) {
      derr << __func__ << " buffer_size " << buffer_bytes << " actual " << s
           << dendl;
      for (auto i = lru.begin(); i != lru.end(); ++i) {
        derr << __func__ << " " << *i << dendl;
      }
      ceph_assert(s == buffer_bytes);
    }
    dout(20) << __func__ << " " << when << " buffer_bytes " << buffer_bytes
             << " ok" << dendl;
  }
#endif
};

// TwoQBufferCacheShard

struct TwoQBufferCacheShard : public BlueStore::BufferCacheShard {
  typedef boost::intrusive::list<
    BlueStore::Buffer,
    boost::intrusive::member_hook<
      BlueStore::Buffer,
      boost::intrusive::list_member_hook<>,
      &BlueStore::Buffer::lru_item> > list_t;
  list_t hot;      ///< "Am" hot buffers
  list_t warm_in;  ///< "A1in" newly warm buffers
  list_t warm_out; ///< "A1out" empty buffers we've evicted

  enum {
    BUFFER_NEW = 0,
    BUFFER_WARM_IN,   ///< in warm_in
    BUFFER_WARM_OUT,  ///< in warm_out
    BUFFER_HOT,       ///< in hot
    BUFFER_TYPE_MAX
  };

  uint64_t list_bytes[BUFFER_TYPE_MAX] = {0}; ///< bytes per type

public:
  explicit TwoQBufferCacheShard(CephContext *cct) : BufferCacheShard(cct) {}

  void _add(BlueStore::Buffer *b, int level, BlueStore::Buffer *near) override
  {
    dout(20) << __func__ << " level " << level << " near " << near
             << " on " << *b
             << " which has cache_private " << b->cache_private << dendl;
    if (near) {
      b->cache_private = near->cache_private;
      switch (b->cache_private) {
      case BUFFER_WARM_IN:
        warm_in.insert(warm_in.iterator_to(*near), *b);
        break;
      case BUFFER_WARM_OUT:
        ceph_assert(b->is_empty());
        warm_out.insert(warm_out.iterator_to(*near), *b);
        break;
      case BUFFER_HOT:
        hot.insert(hot.iterator_to(*near), *b);
        break;
      default:
        ceph_abort_msg("bad cache_private");
      }
    } else if (b->cache_private == BUFFER_NEW) {
      b->cache_private = BUFFER_WARM_IN;
      if (level > 0) {
        warm_in.push_front(*b);
      } else {
        // take caller hint to start at the back of the warm queue
        warm_in.push_back(*b);
      }
    } else {
      // we got a hint from discard
      switch (b->cache_private) {
      case BUFFER_WARM_IN:
        // stay in warm_in.  move to front, even though 2Q doesn't actually
        // do this.
        dout(20) << __func__ << " move to front of warm " << *b << dendl;
        warm_in.push_front(*b);
        break;
      case BUFFER_WARM_OUT:
        b->cache_private = BUFFER_HOT;
        // move to hot.  fall-thru
      case BUFFER_HOT:
        dout(20) << __func__ << " move to front of hot " << *b << dendl;
        hot.push_front(*b);
        break;
      default:
        ceph_abort_msg("bad cache_private");
      }
    }
    b->cache_age_bin = age_bins.front();
    if (!b->is_empty()) {
      buffer_bytes += b->length;
      list_bytes[b->cache_private] += b->length;
      *(b->cache_age_bin) += b->length;
    }
    num = hot.size() + warm_in.size();
  }

  void _rm(BlueStore::Buffer *b) override
  {
    dout(20) << __func__ << " " << *b << dendl;
    if (!b->is_empty()) {
      ceph_assert(buffer_bytes >= b->length);
      buffer_bytes -= b->length;
      ceph_assert(list_bytes[b->cache_private] >= b->length);
      list_bytes[b->cache_private] -= b->length;
      assert(*(b->cache_age_bin) >= b->length);
      *(b->cache_age_bin) -= b->length;
    }
    switch (b->cache_private) {
    case BUFFER_WARM_IN:
      warm_in.erase(warm_in.iterator_to(*b));
      break;
    case BUFFER_WARM_OUT:
      warm_out.erase(warm_out.iterator_to(*b));
      break;
    case BUFFER_HOT:
      hot.erase(hot.iterator_to(*b));
      break;
    default:
      ceph_abort_msg("bad cache_private");
    }
    num = hot.size() + warm_in.size();
  }

  void _move(BlueStore::BufferCacheShard *srcc, BlueStore::Buffer *b) override
  {
    TwoQBufferCacheShard *src = static_cast<TwoQBufferCacheShard*>(srcc);
    src->_rm(b);

    // preserve which list we're on (even if we can't preserve the order!)
    switch (b->cache_private) {
    case BUFFER_WARM_IN:
      ceph_assert(!b->is_empty());
      warm_in.push_back(*b);
      break;
    case BUFFER_WARM_OUT:
      ceph_assert(b->is_empty());
      warm_out.push_back(*b);
      break;
    case BUFFER_HOT:
      ceph_assert(!b->is_empty());
      hot.push_back(*b);
      break;
    default:
      ceph_abort_msg("bad cache_private");
    }
    if (!b->is_empty()) {
      buffer_bytes += b->length;
      list_bytes[b->cache_private] += b->length;
      *(b->cache_age_bin) += b->length;
    }
    num = hot.size() + warm_in.size();
  }

  void _adjust_size(BlueStore::Buffer *b, int64_t delta) override
  {
    dout(20) << __func__ << " delta " << delta << " on " << *b << dendl;
    if (!b->is_empty()) {
      ceph_assert((int64_t)buffer_bytes + delta >= 0);
      buffer_bytes += delta;
      ceph_assert((int64_t)list_bytes[b->cache_private] + delta >= 0);
      list_bytes[b->cache_private] += delta;
      assert(*(b->cache_age_bin) + delta >= 0);
      *(b->cache_age_bin) += delta;
    }
  }

  void _touch(BlueStore::Buffer *b) override {
    switch (b->cache_private) {
    case BUFFER_WARM_IN:
      // do nothing (somewhat counter-intuitively!)
      break;
    case BUFFER_WARM_OUT:
      // move from warm_out to hot LRU
      ceph_abort_msg("this happens via discard hint");
      break;
    case BUFFER_HOT:
      // move to front of hot LRU
      hot.erase(hot.iterator_to(*b));
      hot.push_front(*b);
      break;
    }
    *(b->cache_age_bin) -= b->length;
    b->cache_age_bin = age_bins.front();
    *(b->cache_age_bin) += b->length;
    num = hot.size() + warm_in.size();
    _audit("_touch_buffer end");
  }

  void _trim_to(uint64_t max) override
  {
    if (buffer_bytes > max) {
      uint64_t kin = max * cct->_conf->bluestore_2q_cache_kin_ratio;
      uint64_t khot = max - kin;

      // pre-calculate kout based on average buffer size too,
      // which is typical(the warm_in and hot lists may change later)
      uint64_t kout = 0;
      uint64_t buffer_num = hot.size() + warm_in.size();
      if (buffer_num) {
        uint64_t avg_size = buffer_bytes / buffer_num;
        ceph_assert(avg_size);
        uint64_t calculated_num = max / avg_size;
        kout = calculated_num * cct->_conf->bluestore_2q_cache_kout_ratio;
      }

      if (list_bytes[BUFFER_HOT] < khot) {
        // hot is small, give slack to warm_in
        kin += khot - list_bytes[BUFFER_HOT];
      } else if (list_bytes[BUFFER_WARM_IN] < kin) {
        // warm_in is small, give slack to hot
        khot += kin - list_bytes[BUFFER_WARM_IN];
      }

      // adjust warm_in list
      int64_t to_evict_bytes = list_bytes[BUFFER_WARM_IN] - kin;
      uint64_t evicted = 0;

      while (to_evict_bytes > 0) {
        auto p = warm_in.rbegin();
        if (p == warm_in.rend()) {
          // stop if warm_in list is now empty
          break;
        }

        BlueStore::Buffer *b = &*p;
        ceph_assert(b->is_clean());
        dout(20) << __func__ << " buffer_warm_in -> out " << *b << dendl;
        ceph_assert(buffer_bytes >= b->length);
        buffer_bytes -= b->length;
        ceph_assert(list_bytes[BUFFER_WARM_IN] >= b->length);
        list_bytes[BUFFER_WARM_IN] -= b->length;
        assert(*(b->cache_age_bin) >= b->length);
        *(b->cache_age_bin) -= b->length;
	to_evict_bytes -= b->length;
        evicted += b->length;
        b->state = BlueStore::Buffer::STATE_EMPTY;
        b->data.clear();
        warm_in.erase(warm_in.iterator_to(*b));
        warm_out.push_front(*b);
        b->cache_private = BUFFER_WARM_OUT;
      }

      if (evicted > 0) {
        dout(20) << __func__ << " evicted " << byte_u_t(evicted)
                 << " from warm_in list, done evicting warm_in buffers"
                 << dendl;
      }

      // adjust hot list
      to_evict_bytes = list_bytes[BUFFER_HOT] - khot;
      evicted = 0;

      while (to_evict_bytes > 0) {
        auto p = hot.rbegin();
        if (p == hot.rend()) {
          // stop if hot list is now empty
          break;
        }

        BlueStore::Buffer *b = &*p;
        dout(20) << __func__ << " buffer_hot rm " << *b << dendl;
        ceph_assert(b->is_clean());
        // adjust evict size before buffer goes invalid
        to_evict_bytes -= b->length;
        evicted += b->length;
        b->space->_rm_buffer(this, b);
      }

      if (evicted > 0) {
        dout(20) << __func__ << " evicted " << byte_u_t(evicted)
                 << " from hot list, done evicting hot buffers"
                 << dendl;
      }

      // adjust warm out list too, if necessary
      int64_t n = warm_out.size() - kout;
      while (n-- > 0) {
        BlueStore::Buffer *b = &*warm_out.rbegin();
        ceph_assert(b->is_empty());
        dout(20) << __func__ << " buffer_warm_out rm " << *b << dendl;
        b->space->_rm_buffer(this, b);
      }
    }
    num = hot.size() + warm_in.size();
  }

  void add_stats(uint64_t *extents,
                 uint64_t *blobs,
                 uint64_t *buffers,
                 uint64_t *bytes) override {
    std::lock_guard l(lock);
    *extents += num_extents;
    *blobs += num_blobs;
    *buffers += num;
    *bytes += buffer_bytes;
  }

#ifdef DEBUG_CACHE
  void _audit(const char *when) override
  {
    dout(10) << __func__ << " " << when << " start" << dendl;
    uint64_t s = 0;
    for (auto i = hot.begin(); i != hot.end(); ++i) {
      ceph_assert(i->cache_private == BUFFER_HOT);
      s += i->length;
    }

    uint64_t hot_bytes = s;
    if (hot_bytes != list_bytes[BUFFER_HOT]) {
      derr << __func__ << " hot_list_bytes "
           << list_bytes[BUFFER_HOT]
           << " != actual " << hot_bytes
           << dendl;
      ceph_assert(hot_bytes == list_bytes[BUFFER_HOT]);
    }

    for (auto i = warm_in.begin(); i != warm_in.end(); ++i) {
      ceph_assert(i->cache_private == BUFFER_WARM_IN);
      s += i->length;
    }

    uint64_t warm_in_bytes = s - hot_bytes;
    if (warm_in_bytes != list_bytes[BUFFER_WARM_IN]) {
      derr << __func__ << " warm_in_list_bytes "
           << list_bytes[BUFFER_WARM_IN]
           << " != actual " << warm_in_bytes
           << dendl;
      ceph_assert(warm_in_bytes == list_bytes[BUFFER_WARM_IN]);
    }

    if (s != buffer_bytes) {
      derr << __func__ << " buffer_bytes " << buffer_bytes << " actual " << s
           << dendl;
      ceph_assert(s == buffer_bytes);
    }

    for (auto i = warm_out.begin(); i != warm_out.end(); ++i) {
      ceph_assert(i->cache_private == BUFFER_WARM_OUT);
      ceph_assert(i->is_empty());
    }
    dout(20) << __func__ << " " << when << " buffer_bytes " << buffer_bytes
             << " ok" << dendl;
  }
#endif
};

// BuferCacheShard

BlueStore::BufferCacheShard *BlueStore::BufferCacheShard::create(
    CephContext* cct,
    string type,
    PerfCounters *logger)
{
  BufferCacheShard *c = nullptr;
  if (type == "lru")
    c = new LruBufferCacheShard(cct);
  else if (type == "2q")
    c = new TwoQBufferCacheShard(cct);
  else
    ceph_abort_msg("unrecognized cache type");
  c->logger = logger;
  return c;
}

// BufferSpace

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.BufferSpace(" << this << " in " << cache << ") "

void BlueStore::BufferSpace::_clear(BufferCacheShard* cache)
{
  // note: we already hold cache->lock
  ldout(cache->cct, 20) << __func__ << dendl;
  while (!buffer_map.empty()) {
    _rm_buffer(cache, buffer_map.begin());
  }
}

int BlueStore::BufferSpace::_discard(BufferCacheShard* cache, uint32_t offset, uint32_t length)
{
  // note: we already hold cache->lock
  ldout(cache->cct, 20) << __func__ << std::hex << " 0x" << offset << "~" << length
           << std::dec << dendl;
  int cache_private = 0;
  cache->_audit("discard start");
  auto i = _data_lower_bound(offset);
  uint32_t end = offset + length;
  while (i != buffer_map.end()) {
    Buffer *b = i->second.get();
    if (b->offset >= end) {
      break;
    }
    if (b->cache_private > cache_private) {
      cache_private = b->cache_private;
    }
    if (b->offset < offset) {
      int64_t front = offset - b->offset;
      if (b->end() > end) {
	// drop middle (split)
	uint32_t tail = b->end() - end;
	if (b->data.length()) {
	  bufferlist bl;
	  bl.substr_of(b->data, b->length - tail, tail);
	  Buffer *nb = new Buffer(this, b->state, b->seq, end, bl, b->flags);
	  nb->maybe_rebuild();
	  _add_buffer(cache, nb, 0, b);
	} else {
	  _add_buffer(cache, new Buffer(this, b->state, b->seq, end, tail,
                                        b->flags),
	              0, b);
	}
	if (!b->is_writing()) {
	  cache->_adjust_size(b, front - (int64_t)b->length);
	}
	b->truncate(front);
	b->maybe_rebuild();
	cache->_audit("discard end 1");
	break;
      } else {
	// drop tail
	if (!b->is_writing()) {
	  cache->_adjust_size(b, front - (int64_t)b->length);
	}
	b->truncate(front);
	b->maybe_rebuild();
	++i;
	continue;
      }
    }
    if (b->end() <= end) {
      // drop entire buffer
      _rm_buffer(cache, i++);
      continue;
    }
    // drop front
    uint32_t keep = b->end() - end;
    if (b->data.length()) {
      bufferlist bl;
      bl.substr_of(b->data, b->length - keep, keep);
      Buffer *nb = new Buffer(this, b->state, b->seq, end, bl, b->flags);
      nb->maybe_rebuild();
      _add_buffer(cache, nb, 0, b);
    } else {
      _add_buffer(cache, new Buffer(this, b->state, b->seq, end, keep,
                                    b->flags),
                  0, b);
    }
    _rm_buffer(cache, i);
    cache->_audit("discard end 2");
    break;
  }
  return cache_private;
}

void BlueStore::BufferSpace::read(
  BufferCacheShard* cache, 
  uint32_t offset,
  uint32_t length,
  BlueStore::ready_regions_t& res,
  interval_set<uint32_t>& res_intervals,
  int flags)
{
  res.clear();
  res_intervals.clear();
  uint32_t want_bytes = length;
  uint32_t end = offset + length;

  {
    std::lock_guard l(cache->lock);
    for (auto i = _data_lower_bound(offset);
         i != buffer_map.end() && offset < end && i->first < end;
         ++i) {
      Buffer *b = i->second.get();
      ceph_assert(b->end() > offset);

      bool val = false;
      if (flags & BYPASS_CLEAN_CACHE)
        val = b->is_writing();
      else
        val = b->is_writing() || b->is_clean();
      if (val) {
        if (b->offset < offset) {
	  uint32_t skip = offset - b->offset;
	  uint32_t l = min(length, b->length - skip);
	  res[offset].substr_of(b->data, skip, l);
	  res_intervals.insert(offset, l);
	  offset += l;
	  length -= l;
	  if (!b->is_writing()) {
	    cache->_touch(b);
          }
	  continue;
        }
        if (b->offset > offset) {
	  uint32_t gap = b->offset - offset;
	  if (length <= gap) {
	    break;
	  }
	  offset += gap;
	  length -= gap;
        }
        if (!b->is_writing()) {
	  cache->_touch(b);
        }
        if (b->length > length) {
	  res[offset].substr_of(b->data, 0, length);
	  res_intervals.insert(offset, length);
          break;
        } else {
	  res[offset].append(b->data);
	  res_intervals.insert(offset, b->length);
          if (b->length == length)
            break;
	  offset += b->length;
	  length -= b->length;
        }
      }
    }
  }

  uint64_t hit_bytes = res_intervals.size();
  ceph_assert(hit_bytes <= want_bytes);
  uint64_t miss_bytes = want_bytes - hit_bytes;
  cache->logger->inc(l_bluestore_buffer_hit_bytes, hit_bytes);
  cache->logger->inc(l_bluestore_buffer_miss_bytes, miss_bytes);
}

void BlueStore::BufferSpace::_finish_write(BufferCacheShard* cache, uint64_t seq)
{
  auto i = writing.begin();
  while (i != writing.end()) {
    if (i->seq > seq) {
      break;
    }
    if (i->seq < seq) {
      ++i;
      continue;
    }

    Buffer *b = &*i;
    ceph_assert(b->is_writing());

    if (b->flags & Buffer::FLAG_NOCACHE) {
      writing.erase(i++);
      ldout(cache->cct, 20) << __func__ << " discard " << *b << dendl;
      buffer_map.erase(b->offset);
    } else {
      b->state = Buffer::STATE_CLEAN;
      writing.erase(i++);
      b->maybe_rebuild();
      b->data.reassign_to_mempool(mempool::mempool_bluestore_cache_data);
      cache->_add(b, 1, nullptr);
      ldout(cache->cct, 20) << __func__ << " added " << *b << dendl;
    }
  }
  cache->_trim();
  cache->_audit("finish_write end");
}

/*
  copy Buffers that are in writing queue
  returns:
  true  if something copied
  false if nothing copied
*/
bool BlueStore::BufferSpace::_dup_writing(BufferCacheShard* cache, BufferSpace* to)
{
  bool copied = false;
  if (!writing.empty()) {
    copied = true;
    for (auto it = writing.begin(); it != writing.end(); ++it) {
      Buffer& b = *it;
      Buffer* to_b = new Buffer(to, b.state, b.seq, b.offset, b.data, b.flags);
      ceph_assert(to_b->is_writing());
      to->_add_buffer(cache, to_b, 0, nullptr);
    }
  }
  return copied;
}

void BlueStore::BufferSpace::split(BufferCacheShard* cache, size_t pos, BlueStore::BufferSpace &r)
{
  std::lock_guard lk(cache->lock);
  if (buffer_map.empty())
    return;

  auto p = --buffer_map.end();
  while (true) {
    if (p->second->end() <= pos)
      break;

    if (p->second->offset < pos) {
      ldout(cache->cct, 30) << __func__ << " cut " << *p->second << dendl;
      size_t left = pos - p->second->offset;
      size_t right = p->second->length - left;
      if (p->second->data.length()) {
	bufferlist bl;
	bl.substr_of(p->second->data, left, right);
	r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                                        0, bl, p->second->flags),
		      0, p->second.get());
      } else {
	r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                                        0, right, p->second->flags),
		      0, p->second.get());
      }
      cache->_adjust_size(p->second.get(), -right);
      p->second->truncate(left);
      break;
    }

    ceph_assert(p->second->end() > pos);
    ldout(cache->cct, 30) << __func__ << " move " << *p->second << dendl;
    if (p->second->data.length()) {
      r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                               p->second->offset - pos, p->second->data, p->second->flags),
                    0, p->second.get());
    } else {
      r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                               p->second->offset - pos, p->second->length, p->second->flags),
                    0, p->second.get());
    }
    if (p == buffer_map.begin()) {
      _rm_buffer(cache, p);
      break;
    } else {
      _rm_buffer(cache, p--);
    }
  }
  ceph_assert(writing.empty());
  cache->_trim();
}

// lists content of BufferSpace
// BufferSpace must be under exclusive access
std::ostream& operator<<(std::ostream& out, const BlueStore::BufferSpace& bc)
{
  for (auto& [i, j] : bc.buffer_map) {
    out << " [0x" << std::hex << i << "]=" << *j << std::dec;
  }
  if (!bc.writing.empty()) {
    out << " writing:";
    for (auto i = bc.writing.begin(); i != bc.writing.end(); ++i) {
      out << " " << *i;
    }
  }
  return out;
}


// OnodeSpace

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.OnodeSpace(" << this << " in " << cache << ") "

BlueStore::OnodeRef BlueStore::OnodeSpace::add_onode(const ghobject_t& oid,
  OnodeRef& o)
{
  std::lock_guard l(cache->lock);
  // add entry or return existing one
  auto p = onode_map.emplace(oid, o);
  if (!p.second) {
    ldout(cache->cct, 30) << __func__ << " " << oid << " " << o
			  << " raced, returning existing " << p.first->second
			  << dendl;
    return p.first->second;
  }
  ldout(cache->cct, 20) << __func__ << " " << oid << " " << o << dendl;
  cache->_add(o.get(), 1);
  cache->_trim();
  return o;
}

void BlueStore::OnodeSpace::_remove(const ghobject_t& oid)
{
  ldout(cache->cct, 20) << __func__ << " " << oid << " " << dendl;
  onode_map.erase(oid);
}

BlueStore::OnodeRef BlueStore::OnodeSpace::lookup(const ghobject_t& oid)
{
  ldout(cache->cct, 30) << __func__ << dendl;
  OnodeRef o;

  {
    std::lock_guard l(cache->lock);
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
    if (p == onode_map.end()) {
      ldout(cache->cct, 30) << __func__ << " " << oid << " miss" << dendl;
      cache->logger->inc(l_bluestore_onode_misses);
    } else {
      ldout(cache->cct, 30) << __func__ << " " << oid << " hit " << p->second
                            << " " << p->second->nref
                            << " " << p->second->cached
			    << dendl;
      // This will pin onode and implicitly touch the cache when Onode
      // eventually will become unpinned
      o = p->second;

      cache->logger->inc(l_bluestore_onode_hits);
    }
  }

  return o;
}

void BlueStore::OnodeSpace::clear()
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 10) << __func__ << " " << onode_map.size()<< dendl;
  for (auto &p : onode_map) {
    cache->_rm(p.second.get());
  }
  onode_map.clear();
}

bool BlueStore::OnodeSpace::empty()
{
  std::lock_guard l(cache->lock);
  return onode_map.empty();
}

void BlueStore::OnodeSpace::rename(
  OnodeRef& oldo,
  const ghobject_t& old_oid,
  const ghobject_t& new_oid,
  const mempool::bluestore_cache_meta::string& new_okey)
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 30) << __func__ << " " << old_oid << " -> " << new_oid
			<< dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);
  ceph_assert(po != pn);

  ceph_assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    ldout(cache->cct, 30) << __func__ << "  removing target " << pn->second
			  << dendl;
    cache->_rm(pn->second.get());
    onode_map.erase(pn);
  }
  OnodeRef o = po->second;

  // install a non-existent onode at old location
  oldo.reset(new Onode(o->c, old_oid, o->key));
  po->second = oldo;
  cache->_add(oldo.get(), 1);
  // add at new position and fix oid, key.
  // This will pin 'o' and implicitly touch cache
  // when it will eventually become unpinned
  onode_map.insert(make_pair(new_oid, o));

  o->oid = new_oid;
  o->key = new_okey;
  cache->_trim();
}

bool BlueStore::OnodeSpace::map_any(std::function<bool(Onode*)> f)
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 20) << __func__ << dendl;
  for (auto& i : onode_map) {
    if (f(i.second.get())) {
      return true;
    }
  }
  return false;
}

template <int LogLevelV = 30>
void BlueStore::OnodeSpace::dump(CephContext *cct)
{
  for (auto& i : onode_map) {
    ldout(cct, LogLevelV) << i.first << " : " << i.second
      << " " << i.second->nref
      << " " << i.second->cached
      << dendl;
  }
}

// SharedBlob

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.sharedblob(" << this << ") "
#undef dout_context
#define dout_context collection->store->cct

void BlueStore::SharedBlob::dump(Formatter* f) const
{
  f->dump_bool("loaded", loaded);
  if (loaded) {
    persistent->dump(f);
  } else {
    f->dump_unsigned("sbid_unloaded", sbid_unloaded);
  }
}

ostream& operator<<(ostream& out, const BlueStore::SharedBlob& sb)
{
  out << "SharedBlob(" << &sb;
  
  if (sb.loaded) {
    out << " loaded " << *sb.persistent;
  } else {
    out << " sbid 0x" << std::hex << sb.sbid_unloaded << std::dec;
  }
  return out << ")";
}

BlueStore::SharedBlob::SharedBlob(uint64_t i, Collection *_coll)
  : collection(_coll), sbid_unloaded(i)
{
  ceph_assert(sbid_unloaded > 0);
}

BlueStore::SharedBlob::~SharedBlob()
{
  if (loaded && persistent) {
    delete persistent; 
  }
}

void BlueStore::SharedBlob::put()
{
  if (--nref == 0) {
    dout(20) << __func__ << " " << this
	     << " removing self from set " << get_parent()
	     << dendl;
  again:
    auto coll_snap = collection;
    if (coll_snap) {
      std::lock_guard l(coll_snap->cache->lock);
      if (coll_snap != collection) {
	goto again;
      }
      if (!coll_snap->shared_blob_set.remove(this, true)) {
	// race with lookup
	return;
      }
    }
    delete this;
  }
}

void BlueStore::SharedBlob::get_ref(uint64_t offset, uint32_t length)
{
  ceph_assert(persistent);
  persistent->ref_map.get(offset, length);
}

void BlueStore::SharedBlob::put_ref(uint64_t offset, uint32_t length,
				    PExtentVector *r,
				    bool *unshare)
{
  ceph_assert(persistent);
  persistent->ref_map.put(offset, length, r,
    unshare && !*unshare ? unshare : nullptr);
}

// SharedBlobSet

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.sharedblobset(" << this << ") "

template <int LogLevelV = 30>
void BlueStore::SharedBlobSet::dump(CephContext *cct)
{
  std::lock_guard l(lock);
  for (auto& i : sb_map) {
    ldout(cct, LogLevelV) << i.first << " : " << *i.second << dendl;
  }
}

// Blob

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.blob(" << this << ") "

BlueStore::Blob::~Blob()
{
 again:
  auto coll_cache = get_cache();
  if (coll_cache) {
    std::lock_guard l(coll_cache->lock);
    if (coll_cache != get_cache()) {
      goto again;
    }
    bc._clear(coll_cache);
    coll_cache->rm_blob();
  }
  SharedBlob* sb = shared_blob.get();
  ceph_assert(sb || (!sb && bc.buffer_map.empty()));
}

void BlueStore::Blob::dump(Formatter* f) const
{
  if (is_spanning()) {
    f->dump_unsigned("spanning_id ", id);
  }
  blob.dump(f);
  if (shared_blob) {
    f->dump_object("shared", *shared_blob);
  }
}

ostream& operator<<(ostream& out, const BlueStore::Blob& b)
{
  out << "Blob(" << &b;
  if (b.is_spanning()) {
    out << " spanning " << b.id;
  }
  out << " " << b.get_blob() << " " << b.get_blob_use_tracker();
  if (b.shared_blob) {
    out << " " << *b.shared_blob;
  } else {
    out << " (shared_blob=NULL)";
  }
  out << ")";
  return out;
}

void BlueStore::Blob::discard_unallocated(Collection *coll)
{
  if (get_blob().is_shared()) {
    return;
  }
  if (get_blob().is_compressed()) {
    bool discard = false;
    bool all_invalid = true;
    for (auto e : get_blob().get_extents()) {
      if (!e.is_valid()) {
        discard = true;
      } else {
        all_invalid = false;
      }
    }
    ceph_assert(discard == all_invalid); // in case of compressed blob all
				    // or none pextents are invalid.
    if (discard) {
      dirty_bc().discard(get_cache(), 0,
                              get_blob().get_logical_length());
    }
  } else {
    size_t pos = 0;
    for (auto e : get_blob().get_extents()) {
      if (!e.is_valid()) {
	dout(20) << __func__ << " 0x" << std::hex << pos
		 << "~" << e.length
		 << std::dec << dendl;
	dirty_bc().discard(get_cache(), pos, e.length);
      }
      pos += e.length;
    }
    if (get_blob().can_prune_tail()) {
      dirty_blob().prune_tail();
      used_in_blob.prune_tail(get_blob().get_ondisk_length());
      dout(20) << __func__ << " pruned tail, now " << get_blob() << dendl;
    }
  }
}

void BlueStore::Blob::get_ref(
  Collection *coll,
  uint32_t offset,
  uint32_t length)
{
  // Caller has to initialize Blob's logical length prior to increment 
  // references.  Otherwise one is neither unable to determine required
  // amount of counters in case of per-au tracking nor obtain min_release_size
  // for single counter mode.
  ceph_assert(get_blob().get_logical_length() != 0);
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
           << std::dec << " " << *this << dendl;

  if (used_in_blob.is_empty()) {
    uint32_t min_release_size =
      get_blob().get_release_size(coll->store->min_alloc_size);
    uint64_t l = get_blob().get_logical_length();
    dout(20) << __func__ << " init 0x" << std::hex << l << ", "
             << min_release_size << std::dec << dendl;
    used_in_blob.init(l, min_release_size);
  }
  used_in_blob.get(
    offset,
    length);
}

bool BlueStore::Blob::put_ref(
  Collection *coll,
  uint32_t offset,
  uint32_t length,
  PExtentVector *r)
{
  PExtentVector logical;

  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
           << std::dec << " " << *this << dendl;
  
  bool empty = used_in_blob.put(
    offset,
    length,
    &logical);
  r->clear();
  // nothing to release
  if (!empty && logical.empty()) {
    return false;
  }

  bluestore_blob_t& b = dirty_blob();
  return b.release_extents(empty, logical, r);
}

bool BlueStore::Blob::can_reuse_blob(uint32_t min_alloc_size,
                		     uint32_t target_blob_size,
		                     uint32_t b_offset,
		                     uint32_t *length0) {
  ceph_assert(min_alloc_size);
  ceph_assert(target_blob_size);
  if (!get_blob().is_mutable()) {
    return false;
  }

  uint32_t length = *length0;
  uint32_t end = b_offset + length;

  // Currently for the sake of simplicity we omit blob reuse if data is
  // unaligned with csum chunk. Later we can perform padding if needed.
  if (get_blob().has_csum() &&
     ((b_offset % get_blob().get_csum_chunk_size()) != 0 ||
      (end % get_blob().get_csum_chunk_size()) != 0)) {
    return false;
  }

  auto blen = get_blob().get_logical_length();
  uint32_t new_blen = blen;

  // make sure target_blob_size isn't less than current blob len
  target_blob_size = std::max(blen, target_blob_size);

  if (b_offset >= blen) {
    // new data totally stands out of the existing blob
    new_blen = end;
  } else {
    // new data overlaps with the existing blob
    new_blen = std::max(blen, end);

    uint32_t overlap = 0;
    if (new_blen > blen) {
      overlap = blen - b_offset;
    } else {
      overlap = length;
    }

    if (!get_blob().is_unallocated(b_offset, overlap)) {
      // abort if any piece of the overlap has already been allocated
      return false;
    }
  }

  if (new_blen > blen) {
    int64_t overflow = int64_t(new_blen) - target_blob_size;
    // Unable to decrease the provided length to fit into max_blob_size
    if (overflow >= length) {
      return false;
    }

    // FIXME: in some cases we could reduce unused resolution
    if (get_blob().has_unused()) {
      return false;
    }

    if (overflow > 0) {
      new_blen -= overflow;
      length -= overflow;
      *length0 = length;
    }

    if (new_blen > blen) {
      ceph_assert(dirty_blob().is_mutable());
      dirty_blob().add_tail(new_blen);
      used_in_blob.add_tail(new_blen,
                            get_blob().get_release_size(min_alloc_size));
    }
  }
  return true;
}

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.blob(" << this << ") "
#undef dout_context
#define dout_context cct

// Cut Buffers that are not covered by extents.
// It happens when we punch hole in Blob, but not refill with new data.
// Normally it is not a problem (other then wasted memory),
// but when 2 Blobs are merged Buffers might collide.
// Todo: in future cut Buffers when we delete extents from Blobs,
//       and get rid of this function.
void BlueStore::Blob::discard_unused_buffers(CephContext* cct, BufferCacheShard* cache)
{
  dout(25) << __func__ << " input " << *this << " bc=" << bc << dendl;
  const PExtentVector& extents = get_blob().get_extents();
  uint32_t epos = 0;
  auto e = extents.begin();
  while(e != extents.end()) {
    if (!e->is_valid()) {
      bc._discard(cache, epos, e->length);
    }
    epos += e->length;
    ++e;
  }
  ceph_assert(epos <= blob.get_logical_length());
  // Preferably, we would trim up to blob.get_logical_length(),
  // but we copied writing buffers (see _dup_writing) before blob logical_length is fixed.
  bc._discard(cache, epos, OBJECT_MAX_SIZE - epos);
  dout(25) << __func__ << " output bc=" << bc << dendl;
}

void BlueStore::Blob::dup(const Blob& from, bool copy_used_in_blob)
{
  set_shared_blob(from.shared_blob);
  blob.dup(from.blob);
  if (copy_used_in_blob) {
    used_in_blob = from.used_in_blob;
  } else {
    ceph_assert(from.blob.is_compressed());
    ceph_assert(from.used_in_blob.num_au <= 1);
    used_in_blob.init(from.used_in_blob.au_size, from.used_in_blob.au_size);
  }
  for (auto p : blob.get_extents()) {
    if (p.is_valid()) {
      shared_blob->get_ref(p.offset, p.length);
    }
  }
}

// copies part of a Blob
// it is used to create a consistent blob out of parts of other blobs
void BlueStore::Blob::copy_from(
  CephContext* cct, const Blob& from, uint32_t min_release_size, uint32_t start, uint32_t len)
{
  dout(20) << __func__ << " to=" << *this << " from=" << from
	   << " [" << std::hex << start << "~" << len
	   << "] min_release=" << min_release_size << std::dec << dendl;

  auto& bto = blob;
  auto& bfrom = from.blob;
  ceph_assert(!bfrom.is_compressed()); // not suitable for compressed (immutable) blobs
  ceph_assert(!bfrom.has_unused());
  // below to asserts are not required to make function work
  // they check if it is run in desired context
  ceph_assert(bfrom.is_shared());
  ceph_assert(shared_blob);
  ceph_assert(shared_blob == from.shared_blob);

  // split len to pre_len, main_len, post_len
  uint32_t start_aligned = p2align(start, min_release_size);
  uint32_t start_roundup = p2roundup(start, min_release_size);
  uint32_t end_aligned = p2align(start + len, min_release_size);
  uint32_t end_roundup = p2roundup(start + len, min_release_size);
  dout(25) << __func__ << " extent split:"
	   << std::hex << start_aligned << "~" << start_roundup << "~"
	   << end_aligned << "~" << end_roundup << std::dec << dendl;

  if (bto.get_logical_length() == 0) {
    // this is initialization
    bto.adjust_to(from.blob, end_roundup);
    ceph_assert(min_release_size == from.used_in_blob.au_size);
    used_in_blob.init(end_roundup, min_release_size);
  } else if (bto.get_logical_length() < end_roundup) {
    ceph_assert(!bto.is_compressed());
    bto.add_tail(end_roundup);
    used_in_blob.add_tail(end_roundup, used_in_blob.au_size);
  }

  if (end_aligned >= start_roundup) {
    copy_extents(cct, from, start_aligned,
		 start_roundup - start_aligned,/*pre_len*/
		 end_aligned - start_roundup,/*main_len*/
		 end_roundup - end_aligned/*post_len*/);
  } else {
    // it is uncommon case that <start, start + len) in single allocation unit
    copy_extents(cct, from, start_aligned,
		 start_roundup - start_aligned,/*pre_len*/
		 0 /*main_len*/, 0/*post_len*/);
  }
  // copy relevant csum items
  if (bto.has_csum()) {
    size_t csd_value_size = bto.get_csum_value_size();
    size_t csd_item_start = p2align(start, uint32_t(1 << bto.csum_chunk_order)) >> bto.csum_chunk_order;
    size_t csd_item_end = p2roundup(start + len, uint32_t(1 << bto.csum_chunk_order)) >> bto.csum_chunk_order;
    ceph_assert(bto.  csum_data.length() >= csd_item_end * csd_value_size);
    ceph_assert(bfrom.csum_data.length() >= csd_item_end * csd_value_size);
    memcpy(bto.  csum_data.c_str() + csd_item_start * csd_value_size,
	   bfrom.csum_data.c_str() + csd_item_start * csd_value_size,
	   (csd_item_end - csd_item_start) * csd_value_size);
  }
  used_in_blob.get(start, len);
  dout(20) << __func__ << " result=" << *this << dendl;
}

void BlueStore::Blob::copy_extents(
  CephContext* cct, const Blob& from, uint32_t start,
  uint32_t pre_len, uint32_t main_len, uint32_t post_len)
{
  // There are 2 valid states:
  // 1) `to` is not defined on [pos~len] range
  //    (need to copy this region - return true)
  // 2) `from` and `to` are exact on [pos~len] range
  //    (no need to copy region - return false)
  // Otherwise just assert.
  auto check_sane_need_copy = [&](
    const PExtentVector& from,
    const PExtentVector& to,
    uint32_t pos, uint32_t len) -> bool
  {
    uint32_t pto = pos;
    auto ito = to.begin();
    while (ito != to.end() && pto >= ito->length) {
      pto -= ito->length;
      ++ito;
    }
    if (ito == to.end()) return true; // case 1 - obviously empty
    if (!ito->is_valid()) {
      // now sanity check that all the rest is invalid too
      pto += len;
      while (ito != to.end() && pto >= ito->length) {
        ceph_assert(!ito->is_valid());
        pto -= ito->length;
        ++ito;
      }
      return true;
    }
    uint32_t pfrom = pos;
    auto ifrom = from.begin();
    while (ifrom != from.end() && pfrom >= ifrom->length) {
      pfrom -= ifrom->length;
      ++ifrom;
    }
    ceph_assert(ifrom != from.end());
    ceph_assert(ifrom->is_valid());
    // here we require from and to be the same
    while (len > 0) {
      ceph_assert(ifrom->offset + pfrom == ito->offset + pto);
      uint32_t jump = std::min(len, ifrom->length - pfrom);
      jump = std::min(jump, ito->length - pto);
      pfrom += jump;
      if (pfrom == ifrom->length) {
        pfrom = 0;
        ++ifrom;
      }
      pto += jump;
      if (pto == ito->length) {
        pto = 0;
        ++ito;
      }
      len -= jump;
    }
    return false;
  };
  const PExtentVector& exfrom = from.blob.get_extents();
  PExtentVector& exto = blob.dirty_extents();
  dout(20) << __func__ << " 0x" << std::hex << start << " "
	   << pre_len << "/" << main_len << "/" << post_len << std::dec << dendl;

  // the extents that cover same area must be the same
  if (pre_len > 0) {
    if (check_sane_need_copy(exfrom, exto, start, pre_len)) {
      main_len += pre_len; // also copy pre_len
    } else {
      start += pre_len; // skip, already there
    }
  }
  if (post_len > 0) {
    if (check_sane_need_copy(exfrom, exto, start + main_len, post_len)) {
      main_len += post_len; // also copy post_len
    } else {
      // skip, already there
    }
  }
  // it is possible that here is nothing to copy
  if (main_len > 0) {
    copy_extents_over_empty(cct, from, start, main_len);
  }
}

// assumes that target (this->extents) has hole in relevant location
void BlueStore::Blob::copy_extents_over_empty(
  CephContext* cct, const Blob& from, uint32_t start, uint32_t len)
{
  dout(20) << __func__ << " to=" << *this << " from=" << from
	   << "[0x" << std::hex << start << "~" << len << std::dec << "]" << dendl;
  uint32_t padding;
  auto& exto = blob.dirty_extents();
  auto ito = exto.begin();
  PExtentVector::iterator prev = exto.end();
  uint32_t sto = start;

  auto try_append = [&](PExtentVector::iterator& it, uint64_t disk_offset, uint32_t disk_len) {
    if (prev != exto.end()) {
      if (prev->is_valid()) {
	if (prev->offset + prev->length == disk_offset) {
	  shared_blob->get_ref(disk_offset, disk_len);
	  prev->length += disk_len;
	  return;
	}
      }
    }
    it = exto.insert(it, bluestore_pextent_t(disk_offset, disk_len));
    prev = it;
    ++it;
    shared_blob->get_ref(disk_offset, disk_len);
  };

  while (ito != exto.end() && sto >= ito->length) {
    sto -= ito->length;
    prev = ito;
    ++ito;
  }
  if (ito == exto.end()) {
    // putting data after end, just expand / push back
    if (sto > 0) {
      exto.emplace_back(bluestore_pextent_t::INVALID_OFFSET, sto);
      ito = exto.end();
      prev = ito;
    }
    padding = 0;
  } else {
    ceph_assert(!ito->is_valid()); // there can be no collision
    ceph_assert(ito->length >= sto + len); // for at least len, starting with remainder sto
    padding = ito->length - (sto + len); // add this much after copying
    ito = exto.erase(ito); // cut a hole
    if (sto > 0) {
      ito = exto.insert(ito, bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, sto));
      prev = ito;
      ++ito;
    }
  }

  const auto& exfrom = from.blob.get_extents();
  auto itf = exfrom.begin();
  uint32_t sf = start;
  while (itf != exfrom.end() && sf >= itf->length) {
    sf -= itf->length;
    ++itf;
  }

  uint32_t skip_on_first = sf;
  while (itf != exfrom.end() && len > 0) {
    ceph_assert(itf->is_valid());
    uint32_t to_copy = std::min<uint32_t>(itf->length - skip_on_first, len);
    try_append(ito, itf->offset + skip_on_first, to_copy);
    len -= to_copy;
    skip_on_first = 0;
    ++itf;
  }
  ceph_assert(len == 0);

  if (padding > 0) {
    exto.insert(ito, bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, padding));
  }
  dout(20) << __func__ << " result=" << *this << dendl;
}

// Checks if two Blobs can be joined together.
// The important (unchecked) condition is that both Blobs belong to the same object.
// Verifies if 'other' Blob can be deleted but its content moved to 'this' Blob.
// Requirements:
// 1) checksums: same type and size
// 2) tracker: same au size
// 3) extents: must be disjointed
// 4) unused: ignored, will be cleared
//
// Returns:
// false - Blobs are incompatible
// true - Blobs can be merged
//
// Returned blob_width is a distance between 'other' Blob's blob_start() and last logical_offset
// that can refer to 'other' Blob extents. It is used to limit iteration on ExtentMap.
bool BlueStore::Blob::can_merge_blob(const Blob* other, uint32_t& blob_width) const
{
  const Blob* x = other;
  const Blob* y = this;
  // checksums
  const bluestore_blob_t& xb = x->get_blob();
  const bluestore_blob_t& yb = y->get_blob();
  if (xb.has_csum() != yb.has_csum()) return false;
  if (xb.has_csum()) {
    if (xb.csum_type != yb.csum_type) return false;
    if (xb.csum_chunk_order != yb.csum_chunk_order) return false;
  }
  // trackers
  const bluestore_blob_use_tracker_t& xtr = x->get_blob_use_tracker();
  const bluestore_blob_use_tracker_t& ytr = y->get_blob_use_tracker();
  if (xtr.au_size != ytr.au_size) return false;
  // unused
  // ignore unused, we will clear it up anyway
  // extents
  // the success is when there is no offset that is used by both blobs
  auto skip_empty = [&](const PExtentVector& list, PExtentVector::const_iterator& it, uint32_t& pos) {
    while (it != list.end() && !it->is_valid()) {
      pos += it->length;
      ++it;
    }
  };
  bool can_merge = true;
  const PExtentVector& xe = x->get_blob().get_extents();
  const PExtentVector& ye = y->get_blob().get_extents();
  PExtentVector::const_iterator xi = xe.begin();
  PExtentVector::const_iterator yi = ye.begin();
  uint32_t xp = 0;
  uint32_t yp = 0;

  skip_empty(xe, xi, xp);
  skip_empty(ye, yi, yp);

  while (xi != xe.end() && yi != ye.end()) {
    if (xp <= yp) {
      if (yp < xp + xi->length) {
	// collision
	can_merge = false;
	break;
      }
      xp += xi->length;
      ++xi;
      skip_empty(xe, xi, xp);
    } else {
      if (xp < yp + yi->length) {
	// collision
	can_merge = false;
	break;
      }
      yp += yi->length;
      ++yi;
      skip_empty(ye, yi, yp);
    }
  }
  if (can_merge) {
    // scan remaining extents in x
    while (xi != xe.end()) {
      xp += xi->length;
      ++xi;
    }
    blob_width = xp;
  }
  return can_merge;
}

// Merges 2 blobs together. Move extents, csum, tracker from src to dst.
uint32_t BlueStore::Blob::merge_blob(CephContext* cct, Blob* blob_to_dissolve)
{
  Blob* dst = this;
  Blob* src = blob_to_dissolve;
  const bluestore_blob_t& src_blob = src->get_blob();
  bluestore_blob_t& dst_blob = dst->dirty_blob();
  dout(20) << __func__ << " to=" << *dst << " from" << *src << dendl;

  // drop unused, do not recalc it, unlikely those chunks could be used in future
  dst_blob.clear_flag(bluestore_blob_t::FLAG_HAS_UNUSED);
  if (dst_blob.get_logical_length() < src_blob.get_logical_length()) {
    // expand to accomodate
    ceph_assert(!dst_blob.is_compressed());
    dst_blob.add_tail(src_blob.get_logical_length());
    used_in_blob.add_tail(src_blob.get_logical_length(), used_in_blob.au_size);
  }
  const PExtentVector& src_extents = src_blob.get_extents();
  const PExtentVector& dst_extents = dst_blob.get_extents();
  PExtentVector tmp_extents;
  tmp_extents.reserve(src_extents.size() + dst_extents.size());

  uint32_t csum_chunk_order = src_blob.csum_chunk_order;
  uint32_t csum_value_size = 0;
  const char* src_csum_ptr = nullptr;
  char* dst_csum_ptr = nullptr;
  if (src_blob.has_csum()) {
    ceph_assert(src_blob.csum_type == dst_blob.csum_type);
    ceph_assert(src_blob.csum_chunk_order == dst_blob.csum_chunk_order);
    csum_value_size = src_blob.get_csum_value_size();
    src_csum_ptr = src_blob.csum_data.c_str();
    dst_csum_ptr = dst_blob.csum_data.c_str();
  }
  const bluestore_blob_use_tracker_t& src_tracker = src->get_blob_use_tracker();
  bluestore_blob_use_tracker_t& dst_tracker = dst->dirty_blob_use_tracker();
  ceph_assert(src_tracker.au_size == dst_tracker.au_size);
  uint32_t tracker_au_size = src_tracker.au_size;
  const uint32_t* src_tracker_aus = src_tracker.get_au_array();
  uint32_t* dst_tracker_aus = dst_tracker.dirty_au_array();

  auto skip_empty = [&](const PExtentVector& list, PExtentVector::const_iterator& it, uint32_t& pos) {
    while (it != list.end()) {
      if (it->is_valid()) {
	return;
      }
      pos += it->length;
      ++it;
    }
    pos = std::numeric_limits<uint32_t>::max();
    return;
  };

  auto move_data = [&](uint32_t pos, uint32_t len) {
    if (src_blob.has_csum()) {
      // copy csum
      ceph_assert((pos % (1 << csum_chunk_order)) == 0);
      ceph_assert((len % (1 << csum_chunk_order)) == 0);
      uint32_t start = p2align(pos, uint32_t(1 << csum_chunk_order));
      uint32_t end = p2roundup(pos + len, uint32_t(1 << csum_chunk_order));
      uint32_t item_no = start >> csum_chunk_order;
      uint32_t item_cnt = (end - start) >> csum_chunk_order;
      ceph_assert(dst_blob.csum_data.length() >= (item_no + item_cnt) * csum_value_size);
      memcpy(dst_csum_ptr + item_no * csum_value_size,
	     src_csum_ptr + item_no * csum_value_size,
	     item_cnt * csum_value_size);
    }
    uint32_t start = p2align(pos, tracker_au_size) / tracker_au_size;
    uint32_t end = p2roundup(pos + len, tracker_au_size) / tracker_au_size;
    for (uint32_t i = start; i < end; i++) {
      ceph_assert(i < dst_tracker.get_num_au());
      dst_tracker_aus[i] += src_tracker_aus[i];
    }
  };

  // Main loop creates new PExtentVector by merging src and dst PExtentVectors.
  // It will replace dst's PExtentVector.
  // When we process extent from dst, csum and tracer data is already in place.
  // When we process extent from src, we need to copy csum and tracer to dst.

  uint32_t src_pos = 0; //offset of next non-empty extent
  uint32_t dst_pos = 0;
  uint32_t pos = 0; //already processed amount
  auto src_it = src_extents.begin(); // iterator to next non-empty extent
  auto dst_it = dst_extents.begin();

  skip_empty(src_extents, src_it, src_pos);
  skip_empty(dst_extents, dst_it, dst_pos);
  while (src_it != src_extents.end() || dst_it != dst_extents.end()) {
    if (src_pos > pos) {
      if (dst_pos > pos) {
	// empty space
	uint32_t m = std::min(src_pos - pos, dst_pos - pos);
	// emit empty
	tmp_extents.emplace_back(bluestore_pextent_t::INVALID_OFFSET, m);
	pos += m;
      } else {
	// copy from dst, src must not have conflicting extent
	ceph_assert(src_pos >= dst_pos + dst_it->length);
	// use extent from destination
	tmp_extents.push_back(*dst_it);
	dst_pos += dst_it->length;
	pos = dst_pos;
	++dst_it;
	skip_empty(dst_extents, dst_it, dst_pos);
      }
    } else {
      // copy from src, dst must not have conflicting extent
      ceph_assert(dst_pos >= src_pos + src_it->length);
      // use extent from source
      tmp_extents.push_back(*src_it);
      // copy blob data
      move_data(src_pos, src_it->length);
      src_pos += src_it->length;
      pos = src_pos;
      ++src_it;
      skip_empty(src_extents, src_it, src_pos);
    }
  }
  if (pos < dst_blob.get_logical_length()) {
    // this is a candidate for improvement;
    // instead of artifically add extents, trim blob
    tmp_extents.emplace_back(bluestore_pextent_t::INVALID_OFFSET, dst_blob.get_logical_length() - pos);
  }
  // now apply freshly merged tmp_extents into dst blob
  dst_blob.dirty_extents().swap(tmp_extents);

  // move BufferSpace buffers
  while(!src->bc.buffer_map.empty()) {
    auto buf = src->bc.buffer_map.extract(src->bc.buffer_map.cbegin());
    buf.mapped()->space = &dst->bc;
    if (dst->bc.buffer_map.count(buf.key()) == 0) {
      dst->bc.buffer_map[buf.key()] = std::move(buf.mapped());
    }
  }
  // move BufferSpace writing
  auto wrt_dst_it = dst->bc.writing.begin();
  while(!src->bc.writing.empty()) {
    Buffer& buf = src->bc.writing.front();
    src->bc.writing.pop_front();
    while (wrt_dst_it != dst->bc.writing.end() && wrt_dst_it->seq < buf.seq) {
      ++wrt_dst_it;
    }
    dst->bc.writing.insert(wrt_dst_it, buf);
  }
  dout(20) << __func__ << " result=" << *dst << dendl;
  return dst_blob.get_logical_length();
}

#undef dout_context
#define dout_context collection->store->cct

void BlueStore::Blob::finish_write(uint64_t seq)
{
  while (true) {
    auto coll = get_collection();
    BufferCacheShard *cache = coll->cache;
    std::lock_guard l(cache->lock);
    if (coll->cache != cache) {
      dout(20) << __func__
	       << " raced with sb cache update, was " << cache
	       << ", now " << coll->cache << ", retrying"
	       << dendl;
      continue;
    }
    bc._finish_write(cache, seq);
    break;
  }
}

void BlueStore::Blob::split(Collection *coll, uint32_t blob_offset, Blob *r)
{
  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << " start " << *this << dendl;
  ceph_assert(blob.can_split());
  ceph_assert(used_in_blob.can_split());
  bluestore_blob_t &lb = dirty_blob();
  bluestore_blob_t &rb = r->dirty_blob();

  used_in_blob.split(
    blob_offset,
    &(r->used_in_blob));

  lb.split(blob_offset, rb);
  dirty_bc().split(get_cache(), blob_offset, r->dirty_bc());

  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << " finish " << *this << dendl;
  dout(10) << __func__ << " 0x" << std::hex << blob_offset << std::dec
	   << "    and " << *r << dendl;
}

#ifndef CACHE_BLOB_BL
void BlueStore::Blob::decode(
  bufferptr::const_iterator& p,
  uint64_t struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  Collection *coll)
{
  denc(blob, p, struct_v);
  if (blob.is_shared()) {
    denc(*sbid, p);
  }
  if (include_ref_map) {
    if (struct_v > 1) {
      used_in_blob.decode(p);
    } else {
      used_in_blob.clear();
      bluestore_extent_ref_map_t legacy_ref_map;
      legacy_ref_map.decode(p);
      if (coll) {
        for (auto r : legacy_ref_map.ref_map) {
          get_ref(
            coll,
            r.first,
            r.second.refs * r.second.length);
        }
      }
    }
  }
}
#endif

// Extent

void BlueStore::Extent::dump(Formatter* f) const
{
  f->dump_unsigned("logical_offset", logical_offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("blob_offset", blob_offset);
  f->dump_object("blob", *blob);
}

ostream& operator<<(ostream& out, const BlueStore::Extent& e)
{
  return out << std::hex << "0x" << e.logical_offset << "~" << e.length
	     << ": 0x" << e.blob_offset << "~" << e.length << std::dec
	     << " " << *e.blob;
}

// OldExtent
BlueStore::OldExtent* BlueStore::OldExtent::create(CollectionRef c,
						   uint32_t lo,
						   uint32_t o,
						   uint32_t l,
						   BlobRef& b) {
  OldExtent* oe = new OldExtent(lo, o, l, b);
  b->put_ref(c.get(), o, l, &(oe->r));
  oe->blob_empty = !b->is_referenced();
  return oe;
}

// ExtentMap

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.extentmap(" << this << ") "
#undef dout_context
#define dout_context onode->c->store->cct

BlueStore::ExtentMap::ExtentMap(Onode *o, size_t inline_shard_prealloc_size)
  : onode(o),
    inline_bl(inline_shard_prealloc_size) {
}

void BlueStore::ExtentMap::dump(Formatter* f) const
{
  f->open_array_section("extents");

  for (auto& e : extent_map) {
      f->dump_object("extent", e);
  }
  f->close_section();
}

void BlueStore::ExtentMap::scan_shared_blobs(
  uint64_t start, uint64_t length,
  std::multimap<uint64_t /*blob.logical_offset*/, Blob*>& candidates)
{
  Collection* c = onode->c;
  uint64_t end = start + length;
  // last_encoded_id will be used to process each blob only once
  // so reset them first
  auto ep_start = seek_lextent(start);
  for (auto ep = ep_start; ep != extent_map.end(); ++ep) {
    // ep->logical_offset and ep->blob_start() are different
    // ep->blob_start() allows us to include blobs that do have some empty space in the beginning
    if (ep->blob_start() >= end) {
      break;
    }
    ep->blob->last_encoded_id = -1;
  }

  // reuse, extent_map could not change
  for (auto ep = ep_start; ep != extent_map.end(); ++ep) {
    if (ep->blob_start() >= end) {
      break;
    }
    if (ep->blob->last_encoded_id == -1) {
      const bluestore_blob_t& blob = ep->blob->get_blob();
      if (blob.is_shared()) {
	// excellent time to load the blob
	c->load_shared_blob(ep->blob->shared_blob);
	if (!blob.is_compressed()) {
	  // Restrict elastic shared blobs to non-compressed blobs.
	  // Fsck cannot handle case when one shared blob contains refs to
	  // both shared and non-shared blobs.

	  // todo consider change to emplace_hint
	  candidates.emplace(ep->blob_start(), ep->blob.get());
	}
      }
      // mark as processed
      ep->blob->last_encoded_id = 0;
    }
  }
}

BlueStore::Blob* BlueStore::ExtentMap::find_mergable_companion(
  Blob* blob_to_dissolve, uint32_t blob_start, uint32_t& blob_width,
  std::multimap<uint64_t /*blob_start*/, Blob*>& candidates)
{
  dout(30) << __func__ << std::hex << " blob_start=0x" << blob_start << std::dec << dendl;
  Blob* result = nullptr;
  for (auto it = candidates.find(blob_start);
       it != candidates.end() && it->first == blob_start;
       ++it) {
    dout(30) << __func__ << " trying " << it->second << dendl;
    if (it->second->can_merge_blob(blob_to_dissolve, blob_width)) {
      dout(20) << __func__ << " merging " << blob_to_dissolve << " to " << it->second << dendl;
      result = it->second;
      break;
    }
  }
  return result;
}

void BlueStore::ExtentMap::reblob_extents(uint32_t blob_start, uint32_t blob_end,
					  BlobRef from_blob, BlobRef to_blob)
{
  if (from_blob->is_spanning()) {
    // Mark spanning blobs no longer spanning.
    // If needed will be re-spanned again in reshard().
    dout(20) << __func__ << " removing spanning blob" << dendl;
    spanning_blob_map.erase(from_blob->id);
    from_blob->id = -1;
  }
  auto prev = extent_map.end();
  for (auto ep = seek_lextent(blob_start); ep != extent_map.end();) {
    Extent* e = &(*ep);
    if (e->logical_offset > blob_end) break;
    if (e->blob == from_blob) {
      e->blob = to_blob;
    }
    if (prev != extent_map.end()) {
      if (prev->blob == e->blob &&
	  prev->blob_offset + prev->length == e->blob_offset &&
	  prev->logical_offset + prev->length == e->logical_offset) {
	prev->length += e->length;
	ep = extent_map.erase(ep);
	// we have to manually delete Extent, otherwise memory leak
	delete e;
	// prev still the same
	continue;
      }
    }
    prev = ep;
    ++ep;
  }
}

// Convert blobs in selected range to shared blobs.
void BlueStore::ExtentMap::make_range_shared_maybe_merge(
  TransContext* txc, OnodeRef& onoderef, uint64_t srcoff, uint64_t length)
{
  ceph_assert(onoderef == onode);
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = OBJECT_MAX_SIZE;
  uint32_t dirty_range_end = 0;
  Collection* c = onode->c;
  BlueStore* store = c->store;
  // load entire object; in most cases we clone entire object anyway
  fault_range(store->db, 0, OBJECT_MAX_SIZE);
  std::multimap<uint64_t /*blob_start*/, Blob*> candidates;
  scan_shared_blobs(srcoff, length, candidates);

  for (auto ep = seek_lextent(srcoff);
    ep != extent_map.end(); ) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(25) << __func__ << " src " << e
	     << " bc=" << e.blob->bc << dendl;
    const bluestore_blob_t& blob = e.blob->get_blob();
    // make sure it is shared
    if (!blob.is_shared()) {
      dirty_range_begin = std::min<uint32_t>(dirty_range_begin, e.blob_start());
      // first try to find a shared blob nearby
      // that can accomodate extra extents
      uint32_t blob_width; //to signal when extents end
      dout(20) << __func__ << std::hex
	       << " e.blob_start=" << e.blob_start()
	       << " e.logical_offset=" << e.logical_offset
	       << std::dec << dendl;
      Blob* b = blob.is_compressed() ? nullptr :
	find_mergable_companion(e.blob.get(), e.blob_start(), blob_width, candidates);
      if (b) {
	dout(20) << __func__ << " merging to: " << *b << " bc=" << b->bc << dendl;
	e.blob->discard_unused_buffers(store->cct, c->cache);
	b->discard_unused_buffers(store->cct, c->cache);
	uint32_t b_logical_length = b->merge_blob(store->cct, e.blob.get());
	for (auto p : blob.get_extents()) {
	  if (p.is_valid()) {
	    b->shared_blob->get_ref(p.offset, p.length);
	  }
	}
	// reblob extents might erase e
	dirty_range_end = std::max<uint32_t>(dirty_range_end, e.blob_start() + b_logical_length);
	uint32_t goto_logical_offset = e.logical_offset + e.length;
	reblob_extents(e.blob_start(), e.blob_start() + blob_width,
		       e.blob, b);
	ep = seek_lextent(goto_logical_offset);
	dout(20) << __func__ << " merged: " << *b << dendl;
      } else {
	// no candidate, has to convert to shared
	c->make_blob_shared(store->_assign_blobid(txc), e.blob);
	ceph_assert(e.logical_end() > 0);
	dirty_range_end = std::max<uint32_t>(dirty_range_end, e.logical_end());
	++ep;
      }
    } else {
      c->load_shared_blob(e.blob->shared_blob);
      ++ep;
    }
  }
  if (dirty_range_begin < dirty_range_end) {
    // source onode got modified in the process
    dirty_range(dirty_range_begin, dirty_range_end - dirty_range_begin);
    maybe_reshard(dirty_range_begin, dirty_range_end);
    txc->write_onode(onoderef);
  }
}

void BlueStore::ExtentMap::dup(BlueStore* b, TransContext* txc,
  CollectionRef& c, OnodeRef& oldo, OnodeRef& newo, uint64_t& srcoff,
  uint64_t& length, uint64_t& dstoff) {
  //_dup_writing needs cache lock
  BufferCacheShard* bcs = c->cache;
  bcs->lock.lock();
  while(bcs != c->cache) {
    bcs->lock.unlock();
    bcs = c->cache;
    bcs->lock.lock();
  }

  vector<BlobRef> id_to_blob(oldo->extent_map.extent_map.size());
  for (auto& e : oldo->extent_map.extent_map) {
    e.blob->last_encoded_id = -1;
  }

  int n = 0;
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = 0;
  uint32_t dirty_range_end = 0;
  bool src_dirty = false;
  for (auto ep = oldo->extent_map.seek_lextent(srcoff);
    ep != oldo->extent_map.extent_map.end();
    ++ep) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(20) << __func__ << "  src " << e << dendl;
    BlobRef cb;
    bool blob_duped = true;
    if (e.blob->last_encoded_id >= 0) {
      cb = id_to_blob[e.blob->last_encoded_id];
      blob_duped = false;
    } else {
      // dup the blob
      const bluestore_blob_t& blob = e.blob->get_blob();
      // make sure it is shared
      if (!blob.is_shared()) {
        c->make_blob_shared(b->_assign_blobid(txc), e.blob);
	if (!src_dirty) {
          src_dirty = true;
          dirty_range_begin = e.logical_offset;
	}
        ceph_assert(e.logical_end() > 0);
        // -1 to exclude next potential shard
        dirty_range_end = e.logical_end() - 1;
      } else {
        c->load_shared_blob(e.blob->shared_blob);
      }
      cb = c->new_blob();
      e.blob->last_encoded_id = n;
      id_to_blob[n] = cb;
      e.blob->dup(*cb);
      // By default do not copy buffers to clones, and let them read data by themselves.
      // The exception are 'writing' buffers, which are not yet stable on device.
      bool some_copied = e.blob->bc._dup_writing(cb->get_cache(), &cb->bc);
      if (some_copied) {
	// Pretend we just wrote those buffers;
	// we need to get _finish_write called, so we can clear then from writing list.
	// Otherwise it will be stuck until someone does write-op on clone.
	txc->blobs_written.insert(cb);
      }

      // bump the extent refs on the copied blob's extents
      for (auto p : blob.get_extents()) {
        if (p.is_valid()) {
          e.blob->shared_blob->get_ref(p.offset, p.length);
        }
      }
      txc->write_shared_blob(e.blob->shared_blob);
      dout(20) << __func__ << "    new " << *cb << dendl;
    }

    int skip_front, skip_back;
    if (e.logical_offset < srcoff) {
      skip_front = srcoff - e.logical_offset;
    } else {
      skip_front = 0;
    }
    if (e.logical_end() > end) {
      skip_back = e.logical_end() - end;
    } else {
      skip_back = 0;
    }

    Extent* ne = new Extent(e.logical_offset + skip_front + dstoff - srcoff,
      e.blob_offset + skip_front, e.length - skip_front - skip_back, cb);
    newo->extent_map.extent_map.insert(*ne);
    ne->blob->get_ref(c.get(), ne->blob_offset, ne->length);
    // fixme: we may leave parts of new blob unreferenced that could
    // be freed (relative to the shared_blob).
    txc->statfs_delta.stored() += ne->length;
    if (e.blob->get_blob().is_compressed()) {
      txc->statfs_delta.compressed_original() += ne->length;
      if (blob_duped) {
        txc->statfs_delta.compressed() +=
          cb->get_blob().get_compressed_payload_length();
      }
    }
    dout(20) << __func__ << "  dst " << *ne << dendl;
    ++n;
  }
  if (src_dirty) {
    oldo->extent_map.dirty_range(dirty_range_begin,
      dirty_range_end - dirty_range_begin);
    txc->write_onode(oldo);
  }
  txc->write_onode(newo);

  if (dstoff + length > newo->onode.size) {
    newo->onode.size = dstoff + length;
  }
  newo->extent_map.dirty_range(dstoff, length);
  //_dup_writing needs cache lock
  bcs->lock.unlock();
}

void BlueStore::ExtentMap::dup_esb(BlueStore* b, TransContext* txc,
  CollectionRef& c, OnodeRef& oldo, OnodeRef& newo, uint64_t& srcoff,
  uint64_t& length, uint64_t& dstoff) {
  ceph_assert(onode == oldo);
  ceph_assert(onode->c == c);
  BufferCacheShard* bcs = c->cache;
  bcs->lock.lock();
  while(bcs != c->cache) {
    bcs->lock.unlock();
    bcs = c->cache;
    bcs->lock.lock();
  }

  dout(25) << __func__ << " start oldo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *oldo);
  dout(25) << __func__ << " start newo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *newo);

  make_range_shared_maybe_merge(txc, oldo, srcoff, length);
  vector<BlobRef> id_to_blob(extent_map.size());
  for (auto& e : extent_map) {
    e.blob->last_encoded_id = -1;
  }

  int n = 0;
  uint64_t end = srcoff + length;
  uint32_t dirty_range_begin = 0;
  uint32_t dirty_range_end = 0;
  bool src_dirty = false;
  for (auto ep = seek_lextent(srcoff); ep != extent_map.end(); ++ep) {
    auto& e = *ep;
    if (e.logical_offset >= end) {
      break;
    }
    dout(20) << __func__ << "  src " << e << dendl;
    BlobRef cb;
    bool blob_duped = true;
    if (e.blob->last_encoded_id >= 0) {
      cb = id_to_blob[e.blob->last_encoded_id];
      blob_duped = false;
    } else {
      // dup the blob
      const bluestore_blob_t& blob = e.blob->get_blob();
      ceph_assert(blob.is_shared());
      ceph_assert(e.blob->is_shared_loaded());
      ceph_assert(!blob.has_unused());
      cb = c->new_blob();
      e.blob->last_encoded_id = n;
      id_to_blob[n] = cb;
      ceph_assert(ep->blob_start() < end);
      // dup entire blob or dup parts only
      if (blob.is_compressed()) {
	// copy whole blob, but without used_in_blob
	cb->dup(*e.blob, false);
      } else if (e.blob_start() >= srcoff && e.blob_end() <= end) {
	// copy whole blob, including used_in_blob
	cb->dup(*e.blob, true);
      } else {
	// we must copy source blob diligently region-by-region
	// initialize shared_blob
	cb->dirty_blob().set_flag(bluestore_blob_t::FLAG_SHARED);
	cb->set_shared_blob(e.blob->shared_blob);
      }
      // By default do not copy buffers to clones, and let them read data by themselves.
      // The exception are 'writing' buffers, which are not yet stable on device.
      bool some_copied = e.blob->bc._dup_writing(cb->get_cache(), &cb->bc);
      if (some_copied) {
	// Pretend we just wrote those buffers;
	// we need to get _finish_write called, so we can clear then from writing list.
	// Otherwise it will be stuck until someone does write-op on the clone.
	txc->blobs_written.insert(cb);
      }

      txc->write_shared_blob(e.blob->shared_blob);
      dout(20) << __func__ << "    new " << *cb << dendl;
    }

    int skip_front, skip_back;
    if (e.logical_offset < srcoff) {
      skip_front = srcoff - e.logical_offset;
    } else {
      skip_front = 0;
    }
    if (e.logical_end() > end) {
      skip_back = e.logical_end() - end;
    } else {
      skip_back = 0;
    }

    Extent* ne = new Extent(e.logical_offset + skip_front + dstoff - srcoff,
      e.blob_offset + skip_front, e.length - skip_front - skip_back, cb);
    newo->extent_map.extent_map.insert(*ne);
    if (e.blob->get_blob().is_compressed()) {
      // blob itself was copied, but used_in_blob was not
      cb->get_ref(c.get(), e.blob_offset + skip_front, e.length - skip_front - skip_back);
    } else
      if (e.blob_start() >= srcoff && e.blob_end() <= end) {
      // blob already copied
    } else {
      // copy part
      uint32_t min_release_size = e.blob->get_blob().get_release_size(c->store->min_alloc_size);
      cb->copy_from(b->cct, *e.blob, min_release_size,
		    e.blob_offset + skip_front, e.length - skip_front - skip_back);
    }

    // fixme: we may leave parts of new blob unreferenced that could
    // be freed (relative to the shared_blob).
    txc->statfs_delta.stored() += ne->length;
    if (e.blob->get_blob().is_compressed()) {
      txc->statfs_delta.compressed_original() += ne->length;
      if (blob_duped) {
        txc->statfs_delta.compressed() +=
          cb->get_blob().get_compressed_payload_length();
      }
    }
    dout(20) << __func__ << "  dst " << *ne << dendl;
    ++n;
  }
  if (src_dirty) {
    dirty_range(dirty_range_begin, dirty_range_end - dirty_range_begin);
    txc->write_onode(oldo);
  }

  if (dstoff + length > newo->onode.size) {
    newo->onode.size = dstoff + length;
  }
  newo->extent_map.dirty_range(dstoff, length);
  newo->extent_map.maybe_reshard(dstoff, dstoff + length);
  txc->write_onode(newo);
  dout(25) << __func__ << " end oldo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *oldo);
  dout(25) << __func__ << " end newo=" << dendl;
  _dump_onode<25>(onode->c->store->cct, *newo);
  bcs->lock.unlock();
}

void BlueStore::ExtentMap::update(KeyValueDB::Transaction t,
                                  bool force)
{
  auto cct = onode->c->store->cct; //used by dout
  dout(20) << __func__ << " " << onode->oid << (force ? " force" : "") << dendl;
  if (onode->onode.extent_map_shards.empty()) {
    if (inline_bl.length() == 0) {
      unsigned n;
      // we need to encode inline_bl to measure encoded length
      bool never_happen = encode_some(0, OBJECT_MAX_SIZE, inline_bl, &n);
      inline_bl.reassign_to_mempool(mempool::mempool_bluestore_inline_bl);
      ceph_assert(!never_happen);
      size_t len = inline_bl.length();
      dout(20) << __func__ << "  inline shard " << len << " bytes from " << n
	       << " extents" << dendl;
      if (!force && len > cct->_conf->bluestore_extent_map_shard_max_size) {
	request_reshard(0, OBJECT_MAX_SIZE);
	return;
      }
    }
    // will persist in the onode key.
  } else {
    // pending shard update
    struct dirty_shard_t {
      Shard *shard;
      bufferlist bl;
      dirty_shard_t(Shard *s) : shard(s) {}
    };
    vector<dirty_shard_t> encoded_shards;
    // allocate slots for all shards in a single call instead of
    // doing multiple allocations - one per each dirty shard
    encoded_shards.reserve(shards.size());

    auto p = shards.begin();
    auto prev_p = p;
    while (p != shards.end()) {
      ceph_assert(p->shard_info->offset >= prev_p->shard_info->offset);
      auto n = p;
      ++n;
      if (p->dirty) {
	uint32_t endoff;
	if (n == shards.end()) {
	  endoff = OBJECT_MAX_SIZE;
	} else {
	  endoff = n->shard_info->offset;
	}
	encoded_shards.emplace_back(dirty_shard_t(&(*p)));
        bufferlist& bl = encoded_shards.back().bl;
	if (encode_some(p->shard_info->offset, endoff - p->shard_info->offset,
			bl, &p->extents)) {
	  if (force) {
	    _dump_extent_map<-1>(cct, *this);
	    derr << __func__ << "  encode_some needs reshard" << dendl;
	    ceph_assert(!force);
	  }
	}
        size_t len = bl.length();

	dout(20) << __func__ << "  shard 0x" << std::hex
		 << p->shard_info->offset << std::dec << " is " << len
		 << " bytes (was " << p->shard_info->bytes << ") from "
		 << p->extents << " extents" << dendl;

        if (!force) {
	  if (len > cct->_conf->bluestore_extent_map_shard_max_size) {
	    // we are big; reshard ourselves
	    request_reshard(p->shard_info->offset, endoff);
	  }
	  // avoid resharding the trailing shard, even if it is small
	  else if (n != shards.end() &&
		   len < g_conf()->bluestore_extent_map_shard_min_size) {
            ceph_assert(endoff != OBJECT_MAX_SIZE);
	    if (p == shards.begin()) {
	      // we are the first shard, combine with next shard
	      request_reshard(p->shard_info->offset, endoff + 1);
	    } else {
	      // combine either with the previous shard or the next,
	      // whichever is smaller
	      if (prev_p->shard_info->bytes > n->shard_info->bytes) {
		request_reshard(p->shard_info->offset, endoff + 1);
	      } else {
		request_reshard(prev_p->shard_info->offset, endoff);
	      }
	    }
	  }
        }
      }
      prev_p = p;
      p = n;
    }
    if (needs_reshard()) {
      return;
    }

    // schedule DB update for dirty shards
    string key;
    for (auto& it : encoded_shards) {
      dout(20) << __func__ << "  encoding key for shard 0x" << std::hex
	       << it.shard->shard_info->offset << std::dec << dendl;
      it.shard->dirty = false;
      it.shard->shard_info->bytes = it.bl.length();
      generate_extent_shard_key_and_apply(
	onode->key,
	it.shard->shard_info->offset,
	&key,
        [&](const string& final_key) {
          t->set(PREFIX_OBJ, final_key, it.bl);
        }
      );
    }
  }
}

bid_t BlueStore::ExtentMap::allocate_spanning_blob_id()
{
  if (spanning_blob_map.empty())
    return 0;
  bid_t bid = spanning_blob_map.rbegin()->first + 1;
  // bid is valid and available.
  if (bid >= 0)
    return bid;
  // Find next unused bid;
  bid = rand() % (numeric_limits<bid_t>::max() + 1);
  const auto begin_bid = bid;
  do {
    if (!spanning_blob_map.count(bid))
      return bid;
    else {
      bid++;
      if (bid < 0) bid = 0;
    }
  } while (bid != begin_bid);
  auto cct = onode->c->store->cct; // used by dout
  _dump_onode<0>(cct, *onode);
  ceph_abort_msg("no available blob id");
}

void BlueStore::ExtentMap::reshard(
  KeyValueDB *db,
  KeyValueDB::Transaction t)
{
  auto cct = onode->c->store->cct; // used by dout

  dout(10) << __func__ << " 0x[" << std::hex << needs_reshard_begin << ","
	   << needs_reshard_end << ")" << std::dec
	   << " of " << onode->onode.extent_map_shards.size()
	   << " shards on " << onode->oid << dendl;
  for (auto& p : spanning_blob_map) {
    dout(20) << __func__ << "   spanning blob " << p.first << " " << *p.second
	     << dendl;
  }
  // determine shard index range
  unsigned si_begin = 0, si_end = 0;
  if (!shards.empty()) {
    while (si_begin + 1 < shards.size() &&
	   shards[si_begin + 1].shard_info->offset <= needs_reshard_begin) {
      ++si_begin;
    }
    needs_reshard_begin = shards[si_begin].shard_info->offset;
    for (si_end = si_begin; si_end < shards.size(); ++si_end) {
      if (shards[si_end].shard_info->offset >= needs_reshard_end) {
	needs_reshard_end = shards[si_end].shard_info->offset;
	break;
      }
    }
    if (si_end == shards.size()) {
      needs_reshard_end = OBJECT_MAX_SIZE;
    }
    dout(20) << __func__ << "   shards [" << si_begin << "," << si_end << ")"
	     << " over 0x[" << std::hex << needs_reshard_begin << ","
	     << needs_reshard_end << ")" << std::dec << dendl;
  } else {
    // When sharding is not applied yet, it is an error to request reshard on range.
    // The problem is that reshard() function will not touch any extent outside the range.
    // Thus initial reshard() must encompass whole object.
    needs_reshard_begin = 0;
    needs_reshard_end = OBJECT_MAX_SIZE;
  }

  fault_range(db, needs_reshard_begin, (needs_reshard_end - needs_reshard_begin));

  // we may need to fault in a larger interval later must have all
  // referring extents for spanning blobs loaded in order to have
  // accurate use_tracker values.
  uint32_t spanning_scan_begin = needs_reshard_begin;
  uint32_t spanning_scan_end = needs_reshard_end;

  // remove old keys
  string key;
  for (unsigned i = si_begin; i < si_end; ++i) {
    generate_extent_shard_key_and_apply(
      onode->key, shards[i].shard_info->offset, &key,
      [&](const string& final_key) {
	t->rmkey(PREFIX_OBJ, final_key);
      }
      );
  }

  // calculate average extent size
  unsigned bytes = 0;
  unsigned extents = 0;
  if (onode->onode.extent_map_shards.empty()) {
    bytes = inline_bl.length();
    extents = extent_map.size();
  } else {
    for (unsigned i = si_begin; i < si_end; ++i) {
      bytes += shards[i].shard_info->bytes;
      extents += shards[i].extents;
    }
  }
  unsigned target = cct->_conf->bluestore_extent_map_shard_target_size;
  unsigned slop = target *
    cct->_conf->bluestore_extent_map_shard_target_size_slop;
  unsigned extent_avg = bytes / std::max(1u, extents);
  dout(20) << __func__ << "  extent_avg " << extent_avg << ", target " << target
	   << ", slop " << slop << dendl;

  // reshard
  unsigned estimate = 0;
  unsigned offset = needs_reshard_begin;
  vector<bluestore_onode_t::shard_info> new_shard_info;
  unsigned max_blob_end = 0;
  Extent dummy(needs_reshard_begin);
  for (auto e = extent_map.lower_bound(dummy);
       e != extent_map.end();
       ++e) {
    if (e->logical_offset >= needs_reshard_end) {
      break;
    }
    dout(30) << " extent " << *e << dendl;

    // disfavor shard boundaries that span a blob
    bool would_span = (e->logical_offset < max_blob_end) || e->blob_offset;
    if (estimate &&
	estimate + extent_avg > target + (would_span ? slop : 0)) {
      // new shard
      if (offset == needs_reshard_begin) {
	new_shard_info.emplace_back(bluestore_onode_t::shard_info());
	new_shard_info.back().offset = offset;
	dout(20) << __func__ << "  new shard 0x" << std::hex << offset
                 << std::dec << dendl;
      }
      offset = e->logical_offset;
      new_shard_info.emplace_back(bluestore_onode_t::shard_info());
      new_shard_info.back().offset = offset;
      dout(20) << __func__ << "  new shard 0x" << std::hex << offset
	       << std::dec << dendl;
      estimate = 0;
    }
    estimate += extent_avg;
    unsigned bs = e->blob_start();
    if (bs < spanning_scan_begin) {
      spanning_scan_begin = bs;
    }
    uint32_t be = e->blob_end();
    if (be > max_blob_end) {
      max_blob_end = be;
    }
    if (be > spanning_scan_end) {
      spanning_scan_end = be;
    }
  }
  if (new_shard_info.empty() && (si_begin > 0 ||
				 si_end < shards.size())) {
    // we resharded a partial range; we must produce at least one output
    // shard
    new_shard_info.emplace_back(bluestore_onode_t::shard_info());
    new_shard_info.back().offset = needs_reshard_begin;
    dout(20) << __func__ << "  new shard 0x" << std::hex << needs_reshard_begin
	     << std::dec << " (singleton degenerate case)" << dendl;
  }

  auto& sv = onode->onode.extent_map_shards;
  dout(20) << __func__ << "  new " << new_shard_info << dendl;
  dout(20) << __func__ << "  old " << sv << dendl;
  if (sv.empty()) {
    // no old shards to keep
    sv.swap(new_shard_info);
    init_shards(true, true);
  } else {
    // splice in new shards
    sv.erase(sv.begin() + si_begin, sv.begin() + si_end);
    shards.erase(shards.begin() + si_begin, shards.begin() + si_end);
    sv.insert(
      sv.begin() + si_begin,
      new_shard_info.begin(),
      new_shard_info.end());
    shards.insert(shards.begin() + si_begin, new_shard_info.size(), Shard());
    si_end = si_begin + new_shard_info.size();

    ceph_assert(sv.size() == shards.size());

    // note that we need to update every shard_info of shards here,
    // as sv might have been totally re-allocated above
    for (unsigned i = 0; i < shards.size(); i++) {
      shards[i].shard_info = &sv[i];
    }

    // mark newly added shards as dirty
    for (unsigned i = si_begin; i < si_end; ++i) {
      shards[i].loaded = true;
      shards[i].dirty = true;
    }
  }
  dout(20) << __func__ << "  fin " << sv << dendl;
  inline_bl.clear();

  if (sv.empty()) {
    // no more shards; unspan all previously spanning blobs
    auto p = spanning_blob_map.begin();
    while (p != spanning_blob_map.end()) {
      p->second->id = -1;
      dout(30) << __func__ << " un-spanning " << *p->second << dendl;
      p = spanning_blob_map.erase(p);
    }
  } else {
    // identify new spanning blobs
    dout(20) << __func__ << " checking spanning blobs 0x[" << std::hex
	     << spanning_scan_begin << "," << spanning_scan_end << ")" << dendl;
    if (spanning_scan_begin < needs_reshard_begin) {
      fault_range(db, spanning_scan_begin,
		  needs_reshard_begin - spanning_scan_begin);
    }
    if (spanning_scan_end > needs_reshard_end) {
      fault_range(db, needs_reshard_end,
		  spanning_scan_end - needs_reshard_end);
    }
    auto sp = sv.begin() + si_begin;
    auto esp = sv.end();
    unsigned shard_start = sp->offset;
    unsigned shard_end;
    ++sp;
    if (sp == esp) {
      shard_end = OBJECT_MAX_SIZE;
    } else {
      shard_end = sp->offset;
    }
    Extent dummy(needs_reshard_begin);

    bool was_too_many_blobs_check = false;
    auto too_many_blobs_threshold =
      g_conf()->bluestore_debug_too_many_blobs_threshold;
    auto& dumped_onodes = onode->c->onode_space.cache->dumped_onodes;
    decltype(onode->c->onode_space.cache->dumped_onodes)::value_type* oid_slot = nullptr;
    decltype(onode->c->onode_space.cache->dumped_onodes)::value_type* oldest_slot = nullptr;

    for (auto e = extent_map.lower_bound(dummy); e != extent_map.end(); ++e) {
      if (e->logical_offset >= needs_reshard_end) {
	break;
      }
      dout(30) << " extent " << *e << dendl;
      while (e->logical_offset >= shard_end) {
	shard_start = shard_end;
	ceph_assert(sp != esp);
	++sp;
	if (sp == esp) {
	  shard_end = OBJECT_MAX_SIZE;
	} else {
	  shard_end = sp->offset;
	}
	dout(30) << __func__ << "  shard 0x" << std::hex << shard_start
		 << " to 0x" << shard_end << std::dec << dendl;
      }

      if (e->blob_escapes_range(shard_start, shard_end - shard_start)) {
	if (!e->blob->is_spanning()) {
	  // We have two options: (1) split the blob into pieces at the
	  // shard boundaries (and adjust extents accordingly), or (2)
	  // mark it spanning.  We prefer to cut the blob if we can.  Note that
	  // we may have to split it multiple times--potentially at every
	  // shard boundary.
	  bool must_span = false;
	  BlobRef b = e->blob;
	  if (b->can_split()) {
	    uint32_t bstart = e->blob_start();
	    uint32_t bend = e->blob_end();
	    for (const auto& sh : shards) {
	      if (bstart < sh.shard_info->offset &&
		  bend > sh.shard_info->offset) {
		uint32_t blob_offset = sh.shard_info->offset - bstart;
		if (b->can_split_at(blob_offset)) {
		  dout(20) << __func__ << "    splitting blob, bstart 0x"
			   << std::hex << bstart << " blob_offset 0x"
			   << blob_offset << std::dec << " " << *b << dendl;
		  b = split_blob(b, blob_offset, sh.shard_info->offset);
		  // switch b to the new right-hand side, in case it
		  // *also* has to get split.
		  bstart += blob_offset;
		  onode->c->store->logger->inc(l_bluestore_blob_split);
		} else {
		  must_span = true;
		  break;
		}
	      }
	    }
	  } else {
	    must_span = true;
	  }
	  if (must_span) {
            auto bid = allocate_spanning_blob_id();
            b->id = bid;
	    spanning_blob_map[b->id] = b;
	    dout(20) << __func__ << "    adding spanning " << *b << dendl;
	    if (!was_too_many_blobs_check &&
	      too_many_blobs_threshold &&
	      spanning_blob_map.size() >= size_t(too_many_blobs_threshold)) {

	      was_too_many_blobs_check = true;
	      for (size_t i = 0; i < dumped_onodes.size(); ++i) {
		if (dumped_onodes[i].first == onode->oid) {
		  oid_slot = &dumped_onodes[i];
		  break;
		}
		if (!oldest_slot || (oldest_slot &&
		    dumped_onodes[i].second < oldest_slot->second)) {
		  oldest_slot = &dumped_onodes[i];
		}
	      }
	    }
	  }
	}
      } else {
	if (e->blob->is_spanning()) {
	  spanning_blob_map.erase(e->blob->id);
	  e->blob->id = -1;
	  dout(30) << __func__ << "    un-spanning " << *e->blob << dendl;
	}
      }
    }
    bool do_dump = (!oid_slot && was_too_many_blobs_check) ||
      (oid_slot &&
	(mono_clock::now() - oid_slot->second >= make_timespan(5 * 60)));
    if (do_dump) {
      dout(0) << __func__
	      << " spanning blob count exceeds threshold, "
	      << spanning_blob_map.size() << " spanning blobs"
	      << dendl;
      _dump_onode<0>(cct, *onode);
      if (oid_slot) {
	oid_slot->second = mono_clock::now();
      } else {
	ceph_assert(oldest_slot);
	oldest_slot->first = onode->oid;
	oldest_slot->second = mono_clock::now();
      }
    }
  }

  clear_needs_reshard();
}

bool BlueStore::ExtentMap::encode_some(
  uint32_t offset,
  uint32_t length,
  bufferlist& bl,
  unsigned *pn)
{
  Extent dummy(offset);
  auto start = extent_map.lower_bound(dummy);
  uint32_t end = offset + length;

  __u8 struct_v = 2; // Version 2 differs from v1 in blob's ref_map
                     // serialization only. Hence there is no specific
                     // handling at ExtentMap level.

  unsigned n = 0;
  size_t bound = 0;
  bool must_reshard = false;
  for (auto p = start;
       p != extent_map.end() && p->logical_offset < end;
       ++p, ++n) {
    ceph_assert(p->logical_offset >= offset);
    p->blob->last_encoded_id = -1;
    if (!p->blob->is_spanning() && p->blob_escapes_range(offset, length)) {
      dout(30) << __func__ << " 0x" << std::hex << offset << "~" << length
	       << std::dec << " hit new spanning blob " << *p << dendl;
      request_reshard(p->blob_start(), p->blob_end());
      must_reshard = true;
    }
    if (!must_reshard) {
      denc_varint(0, bound); // blobid
      denc_varint(0, bound); // logical_offset
      denc_varint(0, bound); // len
      denc_varint(0, bound); // blob_offset

      p->blob->bound_encode(
        bound,
        struct_v,
        p->blob->get_sbid(),
        false);
    }
  }
  if (must_reshard) {
    return true;
  }

  denc(struct_v, bound);
  denc_varint(0, bound); // number of extents

  {
    auto app = bl.get_contiguous_appender(bound);
    denc(struct_v, app);
    denc_varint(n, app);
    if (pn) {
      *pn = n;
    }

    n = 0;
    uint64_t pos = 0;
    uint64_t prev_len = 0;
    for (auto p = start;
	 p != extent_map.end() && p->logical_offset < end;
	 ++p, ++n) {
      unsigned blobid;
      bool include_blob = false;
      if (p->blob->is_spanning()) {
	blobid = p->blob->id << BLOBID_SHIFT_BITS;
	blobid |= BLOBID_FLAG_SPANNING;
      } else if (p->blob->last_encoded_id < 0) {
	p->blob->last_encoded_id = n + 1;  // so it is always non-zero
	include_blob = true;
	blobid = 0;  // the decoder will infer the id from n
      } else {
	blobid = p->blob->last_encoded_id << BLOBID_SHIFT_BITS;
      }
      if (p->logical_offset == pos) {
	blobid |= BLOBID_FLAG_CONTIGUOUS;
      }
      if (p->blob_offset == 0) {
	blobid |= BLOBID_FLAG_ZEROOFFSET;
      }
      if (p->length == prev_len) {
	blobid |= BLOBID_FLAG_SAMELENGTH;
      } else {
	prev_len = p->length;
      }
      denc_varint(blobid, app);
      if ((blobid & BLOBID_FLAG_CONTIGUOUS) == 0) {
	denc_varint_lowz(p->logical_offset - pos, app);
      }
      if ((blobid & BLOBID_FLAG_ZEROOFFSET) == 0) {
	denc_varint_lowz(p->blob_offset, app);
      }
      if ((blobid & BLOBID_FLAG_SAMELENGTH) == 0) {
	denc_varint_lowz(p->length, app);
      }
      pos = p->logical_end();
      if (include_blob) {
	p->blob->encode(app, struct_v, p->blob->get_sbid(), false);
      }
    }
  }
  /*derr << __func__ << bl << dendl;
  derr << __func__ << ":";
  bl.hexdump(*_dout);
  *_dout << dendl;
  */
  return false;
}

/////////////////// BlueStore::ExtentMap::DecoderExtent ///////////
void BlueStore::ExtentMap::ExtentDecoder::decode_extent(
  Extent* le,
  __u8 struct_v,
  bptr_c_it_t& p,
  Collection* c)
{
  uint64_t blobid;
  denc_varint(blobid, p);
  if ((blobid & BLOBID_FLAG_CONTIGUOUS) == 0) {
    uint64_t gap;
    denc_varint_lowz(gap, p);
    pos += gap;
  }
  le->logical_offset = pos;
  if ((blobid & BLOBID_FLAG_ZEROOFFSET) == 0) {
    denc_varint_lowz(le->blob_offset, p);
  } else {
    le->blob_offset = 0;
  }
  if ((blobid & BLOBID_FLAG_SAMELENGTH) == 0) {
    denc_varint_lowz(prev_len, p);
  }
  le->length = prev_len;
  if (blobid & BLOBID_FLAG_SPANNING) {
    consume_blobid(le, true, blobid >> BLOBID_SHIFT_BITS);
  } else {
    blobid >>= BLOBID_SHIFT_BITS;
    if (blobid) {
      consume_blobid(le, false, blobid - 1);
    } else {
      BlobRef b = c->new_blob();
      uint64_t sbid = 0;
      b->decode(p, struct_v, &sbid, false, c);
      consume_blob(le, extent_pos, sbid, b);
    }
  }
  pos += prev_len;
  ++extent_pos;
}

unsigned BlueStore::ExtentMap::ExtentDecoder::decode_some(
  const bufferlist& bl, Collection* c)
{
  __u8 struct_v;
  uint32_t num;

  ceph_assert(bl.get_num_buffers() <= 1);
  auto p = bl.front().begin_deep();
  denc(struct_v, p);
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level below.
  ceph_assert(struct_v == 1 || struct_v == 2);
  denc_varint(num, p);

  extent_pos = 0;
  while (!p.end()) {
    Extent* le = get_next_extent();
    decode_extent(le, struct_v, p, c);
    add_extent(le);
  }
  ceph_assert(extent_pos == num);
  return num;
}

void BlueStore::ExtentMap::ExtentDecoder::decode_spanning_blobs(
  bptr_c_it_t& p, Collection* c)
{
  __u8 struct_v;
  denc(struct_v, p);
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  ceph_assert(struct_v == 1 || struct_v == 2);

  unsigned n;
  denc_varint(n, p);
  while (n--) {
    BlueStore::BlobRef b = c->new_blob();
    denc_varint(b->id, p);
    uint64_t sbid = 0;
    b->decode(p, struct_v, &sbid, true, c);
    consume_spanning_blob(sbid, b);
  }
}

/////////////////// BlueStore::ExtentMap::DecoderExtentFull ///////////
void BlueStore::ExtentMap::ExtentDecoderFull::consume_blobid(
  BlueStore::Extent* le, bool spanning, uint64_t blobid) {
  ceph_assert(le);
  if (spanning) {
    le->assign_blob(extent_map.get_spanning_blob(blobid));
  } else {
    ceph_assert(blobid < blobs.size());
    le->assign_blob(blobs[blobid]);
    // we build ref_map dynamically for non-spanning blobs
    le->blob->get_ref(
      extent_map.onode->c,
      le->blob_offset,
      le->length);
  }
}

void BlueStore::ExtentMap::ExtentDecoderFull::consume_blob(
  BlueStore::Extent* le, uint64_t extent_no, uint64_t sbid, BlobRef b) {
  ceph_assert(le);
  blobs.resize(extent_no + 1);
  blobs[extent_no] = b;
  extent_map.onode->c->open_shared_blob(sbid, b);
  le->assign_blob(b);
  le->blob->get_ref(
    extent_map.onode->c,
    le->blob_offset,
    le->length);
}

void BlueStore::ExtentMap::ExtentDecoderFull::consume_spanning_blob(
  uint64_t sbid, BlueStore::BlobRef b) {
  extent_map.spanning_blob_map[b->id] = b;
  extent_map.onode->c->open_shared_blob(sbid, b);
}

BlueStore::Extent* BlueStore::ExtentMap::ExtentDecoderFull::get_next_extent()
{
  return new Extent();
}

void BlueStore::ExtentMap::ExtentDecoderFull::add_extent(BlueStore::Extent* le)
{
  extent_map.extent_map.insert(*le);
}

unsigned BlueStore::ExtentMap::decode_some(bufferlist& bl)
{
  ExtentDecoderFull edecoder(*this);
  unsigned n = edecoder.decode_some(bl, onode->c);
  return n;
}

void BlueStore::ExtentMap::bound_encode_spanning_blobs(size_t& p)
{
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  __u8 struct_v = 2;

  denc(struct_v, p);
  denc_varint((uint32_t)0, p);
  size_t key_size = 0;
  denc_varint((uint32_t)0, key_size);
  p += spanning_blob_map.size() * key_size;
  for (const auto& i : spanning_blob_map) {
    i.second->bound_encode(p, struct_v, i.second->get_sbid(), true);
  }
}

void BlueStore::ExtentMap::encode_spanning_blobs(
  bufferlist::contiguous_appender& p)
{
  // Version 2 differs from v1 in blob's ref_map
  // serialization only. Hence there is no specific
  // handling at ExtentMap level.
  __u8 struct_v = 2;

  denc(struct_v, p);
  denc_varint(spanning_blob_map.size(), p);
  for (auto& i : spanning_blob_map) {
    denc_varint(i.second->id, p);
    i.second->encode(p, struct_v, i.second->get_sbid(), true);
  }
}

void BlueStore::ExtentMap::init_shards(bool loaded, bool dirty)
{
  shards.resize(onode->onode.extent_map_shards.size());
  unsigned i = 0;
  for (auto &s : onode->onode.extent_map_shards) {
    shards[i].shard_info = &s;
    shards[i].loaded = loaded;
    shards[i].dirty = dirty;
    ++i;
  }
}

void BlueStore::ExtentMap::fault_range(
  KeyValueDB *db,
  uint32_t offset,
  uint32_t length)
{
  dout(30) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (shards.size() == 0) {
    // no sharding yet; everyting is loaded
    return;
  }
  auto start = seek_shard(offset);
  auto last = seek_shard(offset + length);
  ceph_assert(last >= start);
  ceph_assert(start >= 0);

  string key;
  while (start <= last) {
    ceph_assert((size_t)start < shards.size());
    auto p = &shards[start];
    if (!p->loaded) {
      dout(30) << __func__ << " opening shard 0x" << std::hex
	       << p->shard_info->offset << std::dec << dendl;
      bufferlist v;
      generate_extent_shard_key_and_apply(
	onode->key, p->shard_info->offset, &key,
        [&](const string& final_key) {
          int r = db->get(PREFIX_OBJ, final_key, &v);
          if (r < 0) {
	    derr << __func__ << " missing shard 0x" << std::hex
		 << p->shard_info->offset << std::dec << " for " << onode->oid
		 << dendl;
	    ceph_assert(r >= 0);
          }
        }
      );
      p->extents = decode_some(v);
      p->loaded = true;
      dout(20) << __func__ << " open shard 0x" << std::hex
	       << p->shard_info->offset
	       << " for range 0x" << offset << "~" << length << std::dec
	       << " (" << v.length() << " bytes)" << dendl;
      ceph_assert(p->dirty == false);
      ceph_assert(v.length() == p->shard_info->bytes);
      onode->c->store->logger->inc(l_bluestore_onode_shard_misses);
    } else {
      onode->c->store->logger->inc(l_bluestore_onode_shard_hits);
    }
    ++start;
  }
}

void BlueStore::ExtentMap::dirty_range(
  uint32_t offset,
  uint32_t length)
{
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (shards.empty()) {
    dout(20) << __func__ << " mark inline shard dirty" << dendl;
    inline_bl.clear();
    return;
  }
  auto start = seek_shard(offset);
  if (length == 0) {
    length = 1;
  }
  auto last = seek_shard(offset + length - 1);
  if (start < 0)
    return;

  ceph_assert(last >= start);
  while (start <= last) {
    ceph_assert((size_t)start < shards.size());
    auto p = &shards[start];
    if (!p->loaded) {
      derr << __func__ << "on write 0x" << std::hex << offset
	   << "~" << length << " shard 0x" << p->shard_info->offset
	   << std::dec << " is not loaded, can't mark dirty" << dendl;
      ceph_abort_msg("can't mark unloaded shard dirty");
    }
    if (!p->dirty) {
      dout(20) << __func__ << " mark shard 0x" << std::hex
	       << p->shard_info->offset << std::dec << " dirty" << dendl;
      p->dirty = true;
    }
    ++start;
  }
}

BlueStore::extent_map_t::iterator BlueStore::ExtentMap::find(
  uint64_t offset)
{
  Extent dummy(offset);
  return extent_map.find(dummy);
}

BlueStore::extent_map_t::iterator BlueStore::ExtentMap::seek_lextent(
  uint64_t offset)
{
  Extent dummy(offset);
  auto fp = extent_map.lower_bound(dummy);
  if (fp != extent_map.begin()) {
    --fp;
    if (fp->logical_end() <= offset) {
      ++fp;
    }
  }
  return fp;
}

BlueStore::extent_map_t::const_iterator BlueStore::ExtentMap::seek_lextent(
  uint64_t offset) const
{
  Extent dummy(offset);
  auto fp = extent_map.lower_bound(dummy);
  if (fp != extent_map.begin()) {
    --fp;
    if (fp->logical_end() <= offset) {
      ++fp;
    }
  }
  return fp;
}

bool BlueStore::ExtentMap::has_any_lextents(uint64_t offset, uint64_t length)
{
  auto fp = seek_lextent(offset);
  if (fp == extent_map.end() || fp->logical_offset >= offset + length) {
    return false;
  }
  return true;
}

int BlueStore::ExtentMap::compress_extent_map(
  uint64_t offset,
  uint64_t length)
{
  if (extent_map.empty())
    return 0;
  int removed = 0;
  auto p = seek_lextent(offset);
  if (p != extent_map.begin()) {
    --p;  // start to the left of offset
  }
  // the caller should have just written to this region
  ceph_assert(p != extent_map.end());

  // identify the *next* shard
  auto pshard = shards.begin();
  while (pshard != shards.end() &&
	 p->logical_offset >= pshard->shard_info->offset) {
    ++pshard;
  }
  uint64_t shard_end;
  if (pshard != shards.end()) {
    shard_end = pshard->shard_info->offset;
  } else {
    shard_end = OBJECT_MAX_SIZE;
  }

  auto n = p;
  for (++n; n != extent_map.end(); p = n++) {
    if (n->logical_offset > offset + length) {
      break;  // stop after end
    }
    while (n != extent_map.end() &&
	   p->logical_end() == n->logical_offset &&
	   p->blob == n->blob &&
	   p->blob_offset + p->length == n->blob_offset &&
	   n->logical_offset < shard_end) {
      dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	       << " next shard 0x" << shard_end << std::dec
	       << " merging " << *p << " and " << *n << dendl;
      p->length += n->length;
      rm(n++);
      ++removed;
    }
    if (n == extent_map.end()) {
      break;
    }
    if (n->logical_offset >= shard_end) {
      ceph_assert(pshard != shards.end());
      ++pshard;
      if (pshard != shards.end()) {
	shard_end = pshard->shard_info->offset;
      } else {
	shard_end = OBJECT_MAX_SIZE;
      }
    }
  }
  if (removed) {
    onode->c->store->logger->inc(l_bluestore_extent_compress, removed);
  }
  return removed;
}

void BlueStore::ExtentMap::punch_hole(
  CollectionRef &c, 
  uint64_t offset,
  uint64_t length,
  old_extent_map_t *old_extents)
{
  auto p = seek_lextent(offset);
  uint64_t end = offset + length;
  while (p != extent_map.end()) {
    if (p->logical_offset >= end) {
      break;
    }
    if (p->logical_offset < offset) {
      if (p->logical_end() > end) {
	// split and deref middle
	uint64_t front = offset - p->logical_offset;
	OldExtent* oe = OldExtent::create(c, offset, p->blob_offset + front, 
					  length, p->blob);
	old_extents->push_back(*oe);
	add(end,
	    p->blob_offset + front + length,
	    p->length - front - length,
	    p->blob);
	p->length = front;
	break;
      } else {
	// deref tail
	ceph_assert(p->logical_end() > offset); // else seek_lextent bug
	uint64_t keep = offset - p->logical_offset;
	OldExtent* oe = OldExtent::create(c, offset, p->blob_offset + keep,
					  p->length - keep, p->blob);
	old_extents->push_back(*oe);
	p->length = keep;
	++p;
	continue;
      }
    }
    if (p->logical_offset + p->length <= end) {
      // deref whole lextent
      OldExtent* oe = OldExtent::create(c, p->logical_offset, p->blob_offset,
				        p->length, p->blob);
      old_extents->push_back(*oe);
      rm(p++);
      continue;
    }
    // deref head
    uint64_t keep = p->logical_end() - end;
    OldExtent* oe = OldExtent::create(c, p->logical_offset, p->blob_offset,
				      p->length - keep, p->blob);
    old_extents->push_back(*oe);

    add(end, p->blob_offset + p->length - keep, keep, p->blob);
    rm(p);
    break;
  }
}

BlueStore::Extent *BlueStore::ExtentMap::set_lextent(
  CollectionRef &c,
  uint64_t logical_offset,
  uint64_t blob_offset, uint64_t length, BlobRef b,
  old_extent_map_t *old_extents)
{
  // We need to have completely initialized Blob to increment its ref counters.
  ceph_assert(b->get_blob().get_logical_length() != 0);

  // Do get_ref prior to punch_hole to prevent from putting reused blob into 
  // old_extents list if we overwre the blob totally
  // This might happen during WAL overwrite.
  b->get_ref(onode->c, blob_offset, length);

  if (old_extents) {
    punch_hole(c, logical_offset, length, old_extents);
  }

  Extent *le = new Extent(logical_offset, blob_offset, length, b);
  extent_map.insert(*le);
  maybe_reshard(logical_offset, logical_offset + length);
  return le;
}

BlueStore::BlobRef BlueStore::ExtentMap::split_blob(
  BlobRef lb,
  uint32_t blob_offset,
  uint32_t pos)
{
  uint32_t end_pos = pos + lb->get_blob().get_logical_length() - blob_offset;
  dout(20) << __func__ << " 0x" << std::hex << pos << " end 0x" << end_pos
	   << " blob_offset 0x" << blob_offset << std::dec << " " << *lb
	   << dendl;
  BlobRef rb = onode->c->new_blob();
  lb->split(onode->c, blob_offset, rb.get());

  for (auto ep = seek_lextent(pos);
       ep != extent_map.end() && ep->logical_offset < end_pos;
       ++ep) {
    if (ep->blob != lb) {
      continue;
    }
    if (ep->logical_offset < pos) {
      // split extent
      size_t left = pos - ep->logical_offset;
      Extent *ne = new Extent(pos, 0, ep->length - left, rb);
      extent_map.insert(*ne);
      ep->length = left;
      dout(30) << __func__ << "  split " << *ep << dendl;
      dout(30) << __func__ << "     to " << *ne << dendl;
    } else {
      // switch blob
      ceph_assert(ep->blob_offset >= blob_offset);

      ep->blob = rb;
      ep->blob_offset -= blob_offset;
      dout(30) << __func__ << "  adjusted " << *ep << dendl;
    }
  }
  return rb;
}

BlueStore::ExtentMap::debug_au_vector_t
BlueStore::ExtentMap::debug_list_disk_layout()
{
  BlueStore::ExtentMap::debug_au_vector_t res;
  uint32_t l_pos = 0;
  for (auto ep = extent_map.begin(); ep != extent_map.end(); ++ep) {
    if (l_pos < ep->logical_offset) {
      // a hole in logical mapping, mark it
      res.emplace_back(-1ULL, ep->logical_offset - l_pos, 0, 0);
    }
    l_pos = ep->logical_offset + ep->length;
    const bluestore_blob_t& bblob = ep->blob->get_blob();
    uint32_t chunk_size = bblob.get_chunk_size(onode->c->store->block_size);
    uint32_t length_left = ep->length;

    bluestore_extent_ref_map_t* ref_map = nullptr;
    if (bblob.is_shared()) {
      ceph_assert(ep->blob->is_shared_loaded());
      bluestore_shared_blob_t* bsblob = ep->blob->shared_blob->persistent;
      ref_map = &bsblob->ref_map;
    }

    unsigned csum_i = 0;
    size_t csum_cnt = 0;
    uint32_t length;
    if (bblob.has_csum()) {
      csum_cnt = bblob.get_csum_count();
      uint32_t csum_chunk_size = bblob.get_csum_chunk_size();
      uint64_t csum_offset_align = p2align(ep->blob_offset, csum_chunk_size);
      csum_i = csum_offset_align / csum_chunk_size;
      // size of first chunk
      length = p2align(ep->blob_offset + csum_chunk_size, csum_chunk_size) - ep->blob_offset;
      length = std::min<uint32_t>(length_left, length);
      if (csum_chunk_size < chunk_size) {
	chunk_size = csum_chunk_size;
      }
    } else {
      length = p2align(ep->blob_offset + chunk_size, chunk_size) - ep->blob_offset;
      length = std::min<uint32_t>(length_left, length);
    }

    uint32_t bo = ep->blob_offset;
    while (length_left > 0) {
      uint64_t csum_val = 0;
      if (bblob.has_csum()) {
	ceph_assert(csum_cnt > csum_i);
	csum_val = bblob.get_csum_item(csum_i);
	++csum_i;
      }
      //extract AU from extents
      uint64_t disk_extent_left; // length till the end of disk extent
      uint64_t disk_offset = bblob.calc_offset(bo, &disk_extent_left);
      bluestore_extent_ref_map_t::debug_len_cnt l_c = {0, std::numeric_limits<uint32_t>::max()};
      if (bblob.is_shared()) {
	l_c = ref_map->debug_peek(disk_offset);
	if (l_c.len < length) {
	  length = l_c.len;
	}
      }
      res.emplace_back(disk_offset, length, csum_val, l_c.cnt);
      bo += length;
      length_left -= length;
      length = chunk_size;
    };
  }
  return res;
}

std::ostream& operator<<(std::ostream& out, const BlueStore::ExtentMap::debug_au_vector_t& auv)
{
  out << "[";
  for (size_t i = 0; i < auv.size(); ++i) {
    if (i != 0) {
      out << " ";
    }
    out << "0x" << std::hex;
    if (auv[i].disk_offset != -1ULL) {
      out << auv[i].disk_offset << "~" << auv[i].disk_length
	  << "(" << std::dec << int32_t(auv[i].ref_cnts)
	  << "):" << std::hex << auv[i].chksum;
    } else {
      out << "~" << auv[i].disk_length << std::dec;
    }
  }
  out << "]" << std::dec;
  return out;
}

// Onode
//
// Mapping blobs over Onode's logical offsets.
//
// Blob is always continous. Blobs may overlap.
// Non-mapped regions are "0" when read.
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// <blob.a.blob.a><blob.b.blo>        <blob.c.blob.c.blob.c.blob>
//       <blob.d.blob.d.b>                      <blob.e.blob.e>
// blob.a starts at 0x0 length 0xe
// blob.b starts at 0xf length 0xb
// blob.c starts at 0x23 length 0x1b
// blob.d starts at 0x06 length 0x12
// blob.e starts at 0x2d length 0xf
//
// Blobs can have non-encoded parts:
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// aaaaaa......aaabbbbb...bbbb        ccccccccccccccc..........cc
//       dddddd........ddd                      .....eeeeeeeeee
// "." - non-encoded parts of blob (holes)
//
// Mapping logical to blob:
// extent_map maps {Onode's logical offset, length}=>{Blob, in-blob offset}
// {0x0, 0x6}=>{blob.a, 0x0}
// {0x6, 0x6}=>{blob.d, 0x0}
// {0xc, 0x3}=>{blob.a, 0xc}
// {0xf, 0x5}=>{blob.b, 0x0}
// {0x14, 0x3}=>{blob.d, 0xe}
// {0x17, 0x4}=>{blob.b, 0x8}
// a hole here
// {0x23, 0xe}=>{blob.c, 0x0}
// and so on...
//
// Compressed blobs do not have non-encoded parts.
// Same example as above but all blobs are compressed:
//                 1               2               3
// 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
// aaaaaaAAAAAAaaabbbbbBBBbbbb        cccccccccccccccCCCCCCCCCCcc
//       ddddddDDDDDDDDddd                      EEEEEeeeeeeeeee
// A-E: parts of blobs that are never used.
// This can happen when a compressed blob is overwritten partially.
// The target ranges are no longer used, but are left there because they are necessary
// for successful decompression.
//
// In compressed blobs PExtentVector and csum refer to actually occupied disk space.
// Blob's logical length is larger then occupied disk space.
// Mapping from extent_map always uses offsets of decompressed data.

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.onode(" << this << ")." << __func__ << " "

const std::string& BlueStore::Onode::calc_omap_prefix(uint8_t flags)
{
  if (bluestore_onode_t::is_pgmeta_omap(flags)) {
    return PREFIX_PGMETA_OMAP;
  }
  if (bluestore_onode_t::is_perpg_omap(flags)) {
    return PREFIX_PERPG_OMAP;
  }
  if (bluestore_onode_t::is_perpool_omap(flags)) {
    return PREFIX_PERPOOL_OMAP;
  }
  return PREFIX_OMAP;
}

// '-' < '.' < '~'
void BlueStore::Onode::calc_omap_header(
  uint8_t flags,
  const Onode* o,
  std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('-');
}

void BlueStore::Onode::calc_omap_key(uint8_t flags,
				    const Onode* o,
				    const std::string& key,
				    std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('.');
  out->append(key);
}

void BlueStore::Onode::calc_omap_tail(
  uint8_t flags,
  const Onode* o,
  std::string* out)
{
  if (!bluestore_onode_t::is_pgmeta_omap(flags)) {
    if (bluestore_onode_t::is_perpg_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
      _key_encode_u32(o->oid.hobj.get_bitwise_key_u32(), out);
    } else if (bluestore_onode_t::is_perpool_omap(flags)) {
      _key_encode_u64(o->c->pool(), out);
    }
  }
  _key_encode_u64(o->onode.nid, out);
  out->push_back('~');
}

void BlueStore::Onode::get()
{
  ++nref;
  ++pin_nref;
}
void BlueStore::Onode::put()
{
  if (--pin_nref == 1) {
    c->get_onode_cache()->maybe_unpin(this);
  }
  if (--nref == 0) {
    delete this;
  }
}

void BlueStore::Onode::decode_raw(
  BlueStore::Onode* on,
  const bufferlist& v,
  BlueStore::ExtentMap::ExtentDecoder& edecoder)
{
  on->exists = true;
  auto p = v.front().begin_deep();
  on->onode.decode(p);

  // initialize extent_map
  edecoder.decode_spanning_blobs(p, on->c);
  if (on->onode.extent_map_shards.empty()) {
    denc(on->extent_map.inline_bl, p);
    edecoder.decode_some(on->extent_map.inline_bl, on->c);
  }
}

BlueStore::Onode* BlueStore::Onode::create_decode(
  CollectionRef c,
  const ghobject_t& oid,
  const string& key,
  const bufferlist& v,
  bool allow_empty)
{
  ceph_assert(v.length() || allow_empty);
  Onode* on = new Onode(c.get(), oid, key);

  if (v.length()) {
    ExtentMap::ExtentDecoderFull edecoder(on->extent_map);
    decode_raw(on, v, edecoder);

    for (auto& i : on->onode.attrs) {
      i.second.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    }

    // initialize extent_map
    if (on->onode.extent_map_shards.empty()) {
      on->extent_map.inline_bl.reassign_to_mempool(
        mempool::mempool_bluestore_cache_data);
    } else {
      on->extent_map.init_shards(false, false);
    }
  }
  return on;
}

void BlueStore::Onode::flush()
{
  if (flushing_count.load()) {
    ldout(c->store->cct, 20) << __func__ << " cnt:" << flushing_count << dendl;
    waiting_count++;
    std::unique_lock l(flush_lock);
    while (flushing_count.load()) {
      flush_cond.wait(l);
    }
    waiting_count--;
  }
  ldout(c->store->cct, 20) << __func__ << " done" << dendl;
}

void BlueStore::Onode::dump(Formatter* f) const
{
  onode.dump(f);
  extent_map.dump(f);
}

void BlueStore::Onode::rewrite_omap_key(const string& old, string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->append(old.c_str() + out->length(), old.size() - out->length());
}

void BlueStore::Onode::decode_omap_key(const string& key, string *user_key)
{
  size_t pos = sizeof(uint64_t) + 1;
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      pos += sizeof(uint64_t) + sizeof(uint32_t);
    } else if (onode.is_perpool_omap()) {
      pos += sizeof(uint64_t);
    }
  }
  *user_key = key.substr(pos);
}

// =======================================================
// WriteContext
 
/// Checks for writes to the same pextent within a blob
bool BlueStore::WriteContext::has_conflict(
  BlobRef b,
  uint64_t loffs,
  uint64_t loffs_end,
  uint64_t min_alloc_size)
{
  ceph_assert((loffs % min_alloc_size) == 0);
  ceph_assert((loffs_end % min_alloc_size) == 0);
  for (auto w : writes) {
    if (b == w.b) {
      auto loffs2 = p2align(w.logical_offset, min_alloc_size);
      auto loffs2_end = p2roundup(w.logical_offset + w.length0, min_alloc_size);
      if ((loffs <= loffs2 && loffs_end > loffs2) ||
          (loffs >= loffs2 && loffs < loffs2_end)) {
        return true;
      }
    }
  }
  return false;
}
 
// =======================================================

// DeferredBatch
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.DeferredBatch(" << this << ") "
#undef dout_context
#define dout_context cct

void BlueStore::DeferredBatch::prepare_write(
  CephContext *cct,
  uint64_t seq, uint64_t offset, uint64_t length,
  bufferlist::const_iterator& blp)
{
  _discard(cct, offset, length);
  auto i = iomap.insert(make_pair(offset, deferred_io()));
  ceph_assert(i.second);  // this should be a new insertion
  i.first->second.seq = seq;
  blp.copy(length, i.first->second.bl);
  i.first->second.bl.reassign_to_mempool(
    mempool::mempool_bluestore_writing_deferred);
  dout(20) << __func__ << " seq " << seq
	   << " 0x" << std::hex << offset << "~" << length
	   << " crc " << i.first->second.bl.crc32c(-1)
	   << std::dec << dendl;
  seq_bytes[seq] += length;
#ifdef DEBUG_DEFERRED
  _audit(cct);
#endif
}

void BlueStore::DeferredBatch::_discard(
  CephContext *cct, uint64_t offset, uint64_t length)
{
  generic_dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
		   << std::dec << dendl;
  auto p = iomap.lower_bound(offset);
  if (p != iomap.begin()) {
    --p;
    auto end = p->first + p->second.bl.length();
    if (end > offset) {
      bufferlist head;
      head.substr_of(p->second.bl, 0, offset - p->first);
      dout(20) << __func__ << "  keep head " << p->second.seq
	       << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	       << " -> 0x" << head.length() << std::dec << dendl;
      auto i = seq_bytes.find(p->second.seq);
      ceph_assert(i != seq_bytes.end());
      if (end > offset + length) {
	bufferlist tail;
	tail.substr_of(p->second.bl, offset + length - p->first,
		       end - (offset + length));
	dout(20) << __func__ << "  keep tail " << p->second.seq
		 << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
		 << " -> 0x" << tail.length() << std::dec << dendl;
	auto &n = iomap[offset + length];
	n.bl.swap(tail);
	n.seq = p->second.seq;
	i->second -= length;
      } else {
	i->second -= end - offset;
      }
      ceph_assert(i->second >= 0);
      p->second.bl.swap(head);
    }
    ++p;
  }
  while (p != iomap.end()) {
    if (p->first >= offset + length) {
      break;
    }
    auto i = seq_bytes.find(p->second.seq);
    ceph_assert(i != seq_bytes.end());
    auto end = p->first + p->second.bl.length();
    if (end > offset + length) {
      unsigned drop_front = offset + length - p->first;
      unsigned keep_tail = end - (offset + length);
      dout(20) << __func__ << "  truncate front " << p->second.seq
	       << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	       << " drop_front 0x" << drop_front << " keep_tail 0x" << keep_tail
	       << " to 0x" << (offset + length) << "~" << keep_tail
	       << std::dec << dendl;
      auto &s = iomap[offset + length];
      s.seq = p->second.seq;
      s.bl.substr_of(p->second.bl, drop_front, keep_tail);
      i->second -= drop_front;
    } else {
      dout(20) << __func__ << "  drop " << p->second.seq
	       << " 0x" << std::hex << p->first << "~" << p->second.bl.length()
	       << std::dec << dendl;
      i->second -= p->second.bl.length();
    }
    ceph_assert(i->second >= 0);
    p = iomap.erase(p);
  }
}

void BlueStore::DeferredBatch::_audit(CephContext *cct)
{
  map<uint64_t,int> sb;
  for (auto p : seq_bytes) {
    sb[p.first] = 0;  // make sure we have the same set of keys
  }
  uint64_t pos = 0;
  for (auto& p : iomap) {
    ceph_assert(p.first >= pos);
    sb[p.second.seq] += p.second.bl.length();
    pos = p.first + p.second.bl.length();
  }
  ceph_assert(sb == seq_bytes);
}


// Collection

#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << store->path << ").collection(" << cid << " " << this << ") "

BlueStore::Collection::Collection(BlueStore *store_, OnodeCacheShard *oc, BufferCacheShard *bc, coll_t cid)
  : CollectionImpl(store_->cct, cid),
    store(store_),
    cache(bc),
    exists(true),
    onode_space(oc),
    commit_queue(nullptr)
{
}

bool BlueStore::Collection::flush_commit(Context *c)
{
  return osr->flush_commit(c);
}

void BlueStore::Collection::flush()
{
  osr->flush();
}

void BlueStore::Collection::flush_all_but_last()
{
  osr->flush_all_but_last();
}

void BlueStore::Collection::open_shared_blob(uint64_t sbid, BlobRef b)
{
  ceph_assert(!b->shared_blob);
  const bluestore_blob_t& blob = b->get_blob();
  if (!blob.is_shared()) {
    return;
  }

  SharedBlobRef sb = shared_blob_set.lookup(sbid);
  if (sb) {
    b->set_shared_blob(sb);
    ldout(store->cct, 10) << __func__ << " sbid 0x" << std::hex << sbid
			  << std::dec << " had " << *b->shared_blob << dendl;
  } else {
    b->set_shared_blob(new SharedBlob(sbid, this));
    shared_blob_set.add(this, b->shared_blob.get());
    ldout(store->cct, 10) << __func__ << " sbid 0x" << std::hex << sbid
			  << std::dec << " opened " << *b->shared_blob
			  << dendl;
  }
}

void BlueStore::Collection::load_shared_blob(SharedBlobRef sb)
{
  if (!sb->is_loaded()) {

    bufferlist v;
    string key;
    auto sbid = sb->get_sbid();
    get_shared_blob_key(sbid, &key);
    int r = store->db->get(PREFIX_SHARED_BLOB, key, &v);
    if (r < 0) {
	lderr(store->cct) << __func__ << " sbid 0x" << std::hex << sbid
			  << std::dec << " not found at key "
			  << pretty_binary_string(key) << dendl;
      ceph_abort_msg("uh oh, missing shared_blob");
    }

    sb->loaded = true;
    sb->persistent = new bluestore_shared_blob_t(sbid);
    auto p = v.cbegin();
    decode(*(sb->persistent), p);
    ldout(store->cct, 10) << __func__ << " sbid 0x" << std::hex << sbid
			  << std::dec << " loaded shared_blob " << *sb << dendl;
  }
}

void BlueStore::Collection::make_blob_shared(uint64_t sbid, BlobRef b)
{
  ldout(store->cct, 10) << __func__ << " " << *b << dendl;

  // update blob
  bluestore_blob_t& blob = b->dirty_blob();
  blob.set_flag(bluestore_blob_t::FLAG_SHARED);
  // drop any unused parts, unlikely we could use them in future
  blob.clear_flag(bluestore_blob_t::FLAG_HAS_UNUSED);
  // update shared blob
  b->set_shared_blob(new SharedBlob(sbid, this));
  b->shared_blob->loaded = true;
  b->shared_blob->persistent = new bluestore_shared_blob_t(sbid);
  shared_blob_set.add(this, b->shared_blob.get());
  for (auto p : blob.get_extents()) {
    if (p.is_valid()) {
      b->shared_blob->get_ref(
	p.offset,
	p.length);
    }
  }
  ldout(store->cct, 20) << __func__ << " now " << *b << dendl;
}

uint64_t BlueStore::Collection::make_blob_unshared(SharedBlob *sb)
{
  ldout(store->cct, 10) << __func__ << " " << *sb << dendl;
  ceph_assert(sb->is_loaded());

  uint64_t sbid = sb->get_sbid();
  shared_blob_set.remove(sb);
  sb->loaded = false;
  delete sb->persistent;
  sb->sbid_unloaded = 0;
  ldout(store->cct, 20) << __func__ << " now " << *sb << dendl;
  return sbid;
}

BlueStore::OnodeRef BlueStore::Collection::get_onode(
  const ghobject_t& oid,
  bool create,
  bool is_createop)
{
  ceph_assert(create ? ceph_mutex_is_wlocked(lock) : ceph_mutex_is_locked(lock));

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    if (!oid.match(cnode.bits, pgid.ps())) {
      lderr(store->cct) << __func__ << " oid " << oid << " not part of "
			<< pgid << " bits " << cnode.bits << dendl;
      ceph_abort();
    }
  }

  OnodeRef o = onode_space.lookup(oid);
  if (o)
    return o;

  string key;
  get_object_key(store->cct, oid, &key);

  ldout(store->cct, 20) << __func__ << " oid " << oid << " key "
			<< pretty_binary_string(key) << dendl;

  bufferlist v;
  int r = -ENOENT;
  Onode *on;
  if (!is_createop) {
    r = store->db->get(PREFIX_OBJ, key.c_str(), key.size(), &v);
    ldout(store->cct, 20) << " r " << r << " v.len " << v.length() << dendl;
  }
  if (v.length() == 0) {
    ceph_assert(r == -ENOENT);
    if (!create)
      return OnodeRef();
  } else {
    ceph_assert(r >= 0);
  }

  // new object, load onode if available
  on = Onode::create_decode(this, oid, key, v, true);
  o.reset(on);
  return onode_space.add_onode(oid, o);
}

void BlueStore::Collection::split_cache(
  Collection *dest)
{
  ldout(store->cct, 10) << __func__ << " to " << dest << dendl;

  auto *ocache = get_onode_cache();
  auto *ocache_dest = dest->get_onode_cache();

 // lock cache shards
  std::lock(ocache->lock, ocache_dest->lock, cache->lock, dest->cache->lock);
  std::lock_guard l(ocache->lock, std::adopt_lock);
  std::lock_guard l2(ocache_dest->lock, std::adopt_lock);
  std::lock_guard l3(cache->lock, std::adopt_lock);
  std::lock_guard l4(dest->cache->lock, std::adopt_lock);

  int destbits = dest->cnode.bits;
  spg_t destpg;
  bool is_pg = dest->cid.is_pg(&destpg);
  ceph_assert(is_pg);

  auto p = onode_space.onode_map.begin();
  while (p != onode_space.onode_map.end()) {
    OnodeRef o = p->second;
    if (!p->second->oid.match(destbits, destpg.pgid.ps())) {
      // onode does not belong to this child
      ldout(store->cct, 20) << __func__ << " not moving " << o << " " << o->oid
			    << dendl;
      ++p;
    } else {
      ldout(store->cct, 20) << __func__ << " moving " << o << " " << o->oid
			    << dendl;

      // ensuring that nref is always >= 2 and hence onode is pinned
      OnodeRef o_pin = o;

      p = onode_space.onode_map.erase(p);
      dest->onode_space.onode_map[o->oid] = o;
      if (o->cached) {
        get_onode_cache()->_move_pinned(dest->get_onode_cache(), o.get());
      }
      o->c = dest;

      // move over shared blobs and buffers.  cover shared blobs from
      // both extent map and spanning blob map (the full extent map
      // may not be faulted in)

      auto rehome_blob = [&](Blob* b) {
	for (auto& i : b->bc.buffer_map) {
	  if (!i.second->is_writing()) {
	    ldout(store->cct, 1) << __func__ << "   moving " << *i.second
				 << dendl;
	    dest->cache->_move(cache, i.second.get());
	  } else {
	    ldout(store->cct, 1) << __func__ << "   not moving " << *i.second
				 << dendl;
	  }
	}
	cache->rm_blob();
	dest->cache->add_blob();
	SharedBlob* sb = b->shared_blob.get();
        b->collection = dest;
        if (sb) {
          if (sb->collection == dest) {
            ldout(store->cct, 20) << __func__ << "  already moved " << *sb
              << dendl;
            return;
          }
          ldout(store->cct, 20) << __func__ << "  moving " << *b << dendl;
          ldout(store->cct, 20) << __func__ << "  moving " << *sb << dendl;
          shared_blob_set.remove(sb);
          dest->shared_blob_set.add(dest, sb);
          sb->collection = dest;
        }
      };

      for (auto& e : o->extent_map.extent_map) {
	e.blob->last_encoded_id = -1;
      }
      for (auto& b : o->extent_map.spanning_blob_map) {
	b.second->last_encoded_id = -1;
      }
      for (auto& e : o->extent_map.extent_map) {
	cache->rm_extent();
	dest->cache->add_extent();
	Blob* tb = e.blob.get();
	if (tb->last_encoded_id == -1) {
	  rehome_blob(tb);
	  tb->last_encoded_id = 0;
	}
      }
      for (auto& b : o->extent_map.spanning_blob_map) {
	Blob* tb = b.second.get();
	if (tb->last_encoded_id == -1) {
	  // Having blob in spanning but not mapped is an error.
	  // It will be dropped during encode_some(),
	  // but in the meantime we want cache to be consistent.
	  ldout(store->cct, 10) << __func__ << " spanning blob not in map " << *tb << dendl;
	  rehome_blob(tb);
	  tb->last_encoded_id = 0;
	}
      }
    }
  }
  dest->cache->_trim();
}

// =======================================================

// MempoolThread

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.MempoolThread(" << this << ") "
#undef dout_context
#define dout_context store->cct

void *BlueStore::MempoolThread::entry()
{
  std::unique_lock l{lock};

  uint32_t prev_config_change = store->config_changed.load();
  uint64_t base = store->osd_memory_base;
  double fragmentation = store->osd_memory_expected_fragmentation;
  uint64_t target = store->osd_memory_target;
  uint64_t min = store->osd_memory_cache_min;
  uint64_t max = min;

  // When setting the maximum amount of memory to use for cache, first 
  // assume some base amount of memory for the OSD and then fudge in
  // some overhead for fragmentation that scales with cache usage.
  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  binned_kv_cache = store->db->get_priority_cache();
  binned_kv_onode_cache = store->db->get_priority_cache(PREFIX_OBJ);
  if (store->cache_autotune && binned_kv_cache != nullptr) {
    pcm = std::make_shared<PriorityCache::Manager>(
        store->cct, min, max, target, true, "bluestore-pricache");
    pcm->insert("kv", binned_kv_cache, true);
    pcm->insert("meta", meta_cache, true);
    pcm->insert("data", data_cache, true);
    if (binned_kv_onode_cache != nullptr) {
      pcm->insert("kv_onode", binned_kv_onode_cache, true);
    }
  }

  utime_t next_balance = ceph_clock_now();
  utime_t next_resize = ceph_clock_now();
  utime_t next_bin_rotation = ceph_clock_now();
  utime_t next_deferred_force_submit = ceph_clock_now();
  utime_t alloc_stats_dump_clock = ceph_clock_now();

  bool interval_stats_trim = false;
  while (!stop) {
    // Update pcm cache settings if related configuration was changed
    uint32_t cur_config_change = store->config_changed.load();
    if (cur_config_change != prev_config_change) {
      _update_cache_settings();
      prev_config_change = cur_config_change;
    }

    // define various intervals for background work
    double age_bin_interval = store->cache_age_bin_interval;
    double autotune_interval = store->cache_autotune_interval;
    double resize_interval = store->osd_memory_cache_resize_interval;
    double max_defer_interval = store->max_defer_interval;
    double alloc_stats_dump_interval =
      store->cct->_conf->bluestore_alloc_stats_dump_interval;

    // alloc stats dump
    if (alloc_stats_dump_interval > 0 &&
        alloc_stats_dump_clock + alloc_stats_dump_interval < ceph_clock_now()) {
      store->_record_allocation_stats();
      alloc_stats_dump_clock = ceph_clock_now();
    }
    // cache age binning
    if (age_bin_interval > 0 && next_bin_rotation < ceph_clock_now()) {
      if (binned_kv_cache != nullptr) {
        binned_kv_cache->import_bins(store->kv_bins);
      }
      if (binned_kv_onode_cache != nullptr) {
        binned_kv_onode_cache->import_bins(store->kv_onode_bins);
      }
      meta_cache->import_bins(store->meta_bins);
      data_cache->import_bins(store->data_bins);

      if (pcm != nullptr) {
        pcm->shift_bins();
      }
      next_bin_rotation = ceph_clock_now();
      next_bin_rotation += age_bin_interval;
    }
    // cache balancing
    if (autotune_interval > 0 && next_balance < ceph_clock_now()) {
      if (binned_kv_cache != nullptr) {
        binned_kv_cache->set_cache_ratio(store->cache_kv_ratio);
      }
      if (binned_kv_onode_cache != nullptr) {
        binned_kv_onode_cache->set_cache_ratio(store->cache_kv_onode_ratio);
      }
      meta_cache->set_cache_ratio(store->cache_meta_ratio);
      data_cache->set_cache_ratio(store->cache_data_ratio);

      // Log events at 5 instead of 20 when balance happens.
      interval_stats_trim = true;

      if (pcm != nullptr) {
        pcm->balance();
      }

      next_balance = ceph_clock_now();
      next_balance += autotune_interval;
    }
    // memory resizing (ie autotuning)
    if (resize_interval > 0 && next_resize < ceph_clock_now()) {
      if (ceph_using_tcmalloc() && pcm != nullptr) {
        pcm->tune_memory();
      }
      next_resize = ceph_clock_now();
      next_resize += resize_interval;
    }
    // deferred force submit
    if (max_defer_interval > 0 &&
	next_deferred_force_submit < ceph_clock_now()) {
      if (store->get_deferred_last_submitted() + max_defer_interval <
	  ceph_clock_now()) {
	store->deferred_try_submit();
      }
      next_deferred_force_submit = ceph_clock_now();
      next_deferred_force_submit += max_defer_interval/3;
    }

    // Now Resize the shards 
    _resize_shards(interval_stats_trim);
    interval_stats_trim = false;

    store->refresh_perf_counters();
    auto wait = ceph::make_timespan(
      store->cct->_conf->bluestore_cache_trim_interval);
    cond.wait_for(l, wait);
  }
  // do final dump
  store->_record_allocation_stats();
  stop = false;
  pcm = nullptr;
  return NULL;
}

void BlueStore::MempoolThread::_resize_shards(bool interval_stats)
{
  size_t onode_shards = store->onode_cache_shards.size();
  size_t buffer_shards = store->buffer_cache_shards.size();
  int64_t kv_used = store->db->get_cache_usage();
  int64_t kv_onode_used = store->db->get_cache_usage(PREFIX_OBJ);
  int64_t meta_used = meta_cache->_get_used_bytes();
  int64_t data_used = data_cache->_get_used_bytes();

  uint64_t cache_size = store->cache_size;
  int64_t kv_alloc =
     static_cast<int64_t>(store->cache_kv_ratio * cache_size);
  int64_t kv_onode_alloc =
     static_cast<int64_t>(store->cache_kv_onode_ratio * cache_size);
  int64_t meta_alloc =
     static_cast<int64_t>(store->cache_meta_ratio * cache_size);
  int64_t data_alloc =
     static_cast<int64_t>(store->cache_data_ratio * cache_size);

  if (pcm != nullptr && binned_kv_cache != nullptr) {
    cache_size = pcm->get_tuned_mem();
    kv_alloc = binned_kv_cache->get_committed_size();
    meta_alloc = meta_cache->get_committed_size();
    data_alloc = data_cache->get_committed_size();
    if (binned_kv_onode_cache != nullptr) {
      kv_onode_alloc = binned_kv_onode_cache->get_committed_size();
    }
  }
  
  if (interval_stats) {
    dout(5) << __func__  << " cache_size: " << cache_size
                  << " kv_alloc: " << kv_alloc
                  << " kv_used: " << kv_used
                  << " kv_onode_alloc: " << kv_onode_alloc
                  << " kv_onode_used: " << kv_onode_used
                  << " meta_alloc: " << meta_alloc
                  << " meta_used: " << meta_used
                  << " data_alloc: " << data_alloc
                  << " data_used: " << data_used << dendl;
  } else {
    dout(20) << __func__  << " cache_size: " << cache_size
                   << " kv_alloc: " << kv_alloc
                   << " kv_used: " << kv_used
                   << " kv_onode_alloc: " << kv_onode_alloc
                   << " kv_onode_used: " << kv_onode_used
                   << " meta_alloc: " << meta_alloc
                   << " meta_used: " << meta_used
                   << " data_alloc: " << data_alloc
                   << " data_used: " << data_used << dendl;
  }

  uint64_t max_shard_onodes = static_cast<uint64_t>(
      (meta_alloc / (double) onode_shards) / meta_cache->get_bytes_per_onode());
  uint64_t max_shard_buffer = static_cast<uint64_t>(data_alloc / buffer_shards);

  dout(30) << __func__ << " max_shard_onodes: " << max_shard_onodes
                 << " max_shard_buffer: " << max_shard_buffer << dendl;

  for (auto i : store->onode_cache_shards) {
    i->set_max(max_shard_onodes);
  }
  for (auto i : store->buffer_cache_shards) {
    i->set_max(max_shard_buffer);
  }
}

void BlueStore::MempoolThread::_update_cache_settings()
{
  // Nothing to do if pcm is not used.
  if (pcm == nullptr) {
    return;
  }

  uint64_t target = store->osd_memory_target;
  uint64_t base = store->osd_memory_base;
  uint64_t min = store->osd_memory_cache_min;
  uint64_t max = min;
  double fragmentation = store->osd_memory_expected_fragmentation;

  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  // set pcm cache levels
  pcm->set_target_memory(target);
  pcm->set_min_memory(min);
  pcm->set_max_memory(max);

  dout(5) << __func__  << " updated pcm target: " << target
                << " pcm min: " << min
                << " pcm max: " << max
                << dendl;
}

// =======================================================

// OmapIteratorImpl

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.OmapIteratorImpl(" << this << ") "

BlueStore::OmapIteratorImpl::OmapIteratorImpl(
  PerfCounters* _logger, CollectionRef c, OnodeRef& o, KeyValueDB::Iterator it)
  : logger(_logger), c(c), o(o), it(it)
{
  logger->inc(l_bluestore_omap_iterator_count);
  std::shared_lock l(c->lock);
  if (o->onode.has_omap()) {
    o->get_omap_key(string(), &head);
    o->get_omap_tail(&tail);
    it->lower_bound(head);
  }
}
BlueStore::OmapIteratorImpl::~OmapIteratorImpl()
{
  logger->dec(l_bluestore_omap_iterator_count);
}

string BlueStore::OmapIteratorImpl::_stringify() const
{
  stringstream s;
  s << " omap_iterator(cid = " << c->cid
    <<", oid = " << o->oid << ")";
  return s.str();
}

int BlueStore::OmapIteratorImpl::seek_to_first()
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency(
    __func__,
    l_bluestore_omap_seek_to_first_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  return 0;
}

int BlueStore::OmapIteratorImpl::upper_bound(const string& after)
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    string key;
    o->get_omap_key(after, &key);
    ldout(c->store->cct,20) << __func__ << " after " << after << " key "
			    << pretty_binary_string(key) << dendl;
    it->upper_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency_fn(
    __func__,
    l_bluestore_omap_upper_bound_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age,
    [&] (const ceph::timespan& lat) {
      return ", after = " + after +
	_stringify();
    }
  );
  return 0;
}

int BlueStore::OmapIteratorImpl::lower_bound(const string& to)
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    string key;
    o->get_omap_key(to, &key);
    ldout(c->store->cct,20) << __func__ << " to " << to << " key "
			    << pretty_binary_string(key) << dendl;
    it->lower_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency_fn(
    __func__,
    l_bluestore_omap_lower_bound_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age,
    [&] (const ceph::timespan& lat) {
      return ", to = " + to +
	_stringify();
    }
  );
  return 0;
}

bool BlueStore::OmapIteratorImpl::valid()
{
  std::shared_lock l(c->lock);
  bool r = o->onode.has_omap() && it && it->valid() &&
    it->raw_key().second < tail;
  if (it && it->valid()) {
    ldout(c->store->cct,20) << __func__ << " is at "
			    << pretty_binary_string(it->raw_key().second)
			    << dendl;
  }
  return r;
}

int BlueStore::OmapIteratorImpl::next()
{
  int r = -1;
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    it->next();
    r = 0;
  }
  c->store->log_latency(
    __func__,
    l_bluestore_omap_next_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  return r;
}

string BlueStore::OmapIteratorImpl::key()
{
  std::shared_lock l(c->lock);
  ceph_assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  o->decode_omap_key(db_key, &user_key);

  return user_key;
}

bufferlist BlueStore::OmapIteratorImpl::value()
{
  std::shared_lock l(c->lock);
  ceph_assert(it->valid());
  return it->value();
}


// =====================================

#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << path << ") "
#undef dout_context
#define dout_context cct


static void aio_cb(void *priv, void *priv2)
{
  BlueStore *store = static_cast<BlueStore*>(priv);
  BlueStore::AioContext *c = static_cast<BlueStore::AioContext*>(priv2);
  c->aio_finish(store);
}

static void discard_cb(void *priv, void *priv2)
{
  BlueStore *store = static_cast<BlueStore*>(priv);
  interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  store->handle_discard(*tmp);
}

void BlueStore::handle_discard(interval_set<uint64_t>& to_release)
{
  dout(10) << __func__ << dendl;
  ceph_assert(alloc);
  alloc->release(to_release);
}

BlueStore::BlueStore(CephContext *cct, const string& path)
  : BlueStore(cct, path, 0) {}

BlueStore::BlueStore(CephContext *cct,
  const string& path,
  uint64_t _min_alloc_size)
  : ObjectStore(cct, path),
    throttle(cct),
    finisher(cct, "commit_finisher", "cfin"),
    kv_sync_thread(this),
    kv_finalize_thread(this),
    min_alloc_size(_min_alloc_size),
    min_alloc_size_order(std::countr_zero(_min_alloc_size)),
    mempool_thread(this)
{
  _init_logger();
  cct->_conf.add_observer(this);
  set_cache_shards(1);
}

BlueStore::~BlueStore()
{
  cct->_conf.remove_observer(this);
  _shutdown_logger();
  ceph_assert(!mounted);
  ceph_assert(db == NULL);
  ceph_assert(bluefs == NULL);
  ceph_assert(fsid_fd < 0);
  ceph_assert(path_fd < 0);
  for (auto i : onode_cache_shards) {
    delete i;
  }
  for (auto i : buffer_cache_shards) {
    delete i;
  }
  onode_cache_shards.clear();
  buffer_cache_shards.clear();
}

const char **BlueStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "bluestore_csum_type",
    "bluestore_compression_mode",
    "bluestore_compression_algorithm",
    "bluestore_compression_min_blob_size",
    "bluestore_compression_min_blob_size_ssd",
    "bluestore_compression_min_blob_size_hdd",
    "bluestore_compression_max_blob_size",
    "bluestore_compression_max_blob_size_ssd",
    "bluestore_compression_max_blob_size_hdd",
    "bluestore_compression_required_ratio",
    "bluestore_max_alloc_size",
    "bluestore_prefer_deferred_size",
    "bluestore_prefer_deferred_size_hdd",
    "bluestore_prefer_deferred_size_ssd",
    "bluestore_deferred_batch_ops",
    "bluestore_deferred_batch_ops_hdd",
    "bluestore_deferred_batch_ops_ssd",
    "bluestore_throttle_bytes",
    "bluestore_throttle_deferred_bytes",
    "bluestore_throttle_cost_per_io_hdd",
    "bluestore_throttle_cost_per_io_ssd",
    "bluestore_throttle_cost_per_io",
    "bluestore_max_blob_size",
    "bluestore_max_blob_size_ssd",
    "bluestore_max_blob_size_hdd",
    "osd_memory_target",
    "osd_memory_target_cgroup_limit_ratio",
    "osd_memory_base",
    "osd_memory_cache_min",
    "osd_memory_expected_fragmentation",
    "bluestore_cache_autotune",
    "bluestore_cache_autotune_interval",
    "bluestore_cache_age_bin_interval",
    "bluestore_cache_kv_age_bins",
    "bluestore_cache_kv_onode_age_bins",
    "bluestore_cache_meta_age_bins",
    "bluestore_cache_data_age_bins",
    "bluestore_warn_on_legacy_statfs",
    "bluestore_warn_on_no_per_pool_omap",
    "bluestore_warn_on_no_per_pg_omap",
    "bluestore_max_defer_interval",
    NULL
  };
  return KEYS;
}

void BlueStore::handle_conf_change(const ConfigProxy& conf,
				   const std::set<std::string> &changed)
{
  if (changed.count("bluestore_warn_on_legacy_statfs")) {
    _check_legacy_statfs_alert();
  }
  if (changed.count("bluestore_warn_on_no_per_pool_omap") ||
      changed.count("bluestore_warn_on_no_per_pg_omap")) {
    _check_no_per_pg_or_pool_omap_alert();
  }

  if (changed.count("bluestore_csum_type")) {
    _set_csum();
  }
  if (changed.count("bluestore_compression_mode") ||
      changed.count("bluestore_compression_algorithm") ||
      changed.count("bluestore_compression_min_blob_size") ||
      changed.count("bluestore_compression_max_blob_size")) {
    if (bdev) {
      _set_compression();
    }
  }
  if (changed.count("bluestore_max_blob_size") ||
      changed.count("bluestore_max_blob_size_ssd") ||
      changed.count("bluestore_max_blob_size_hdd")) {
    if (bdev) {
      // only after startup
      _set_blob_size();
    }
  }
  if (changed.count("bluestore_prefer_deferred_size") ||
      changed.count("bluestore_prefer_deferred_size_hdd") ||
      changed.count("bluestore_prefer_deferred_size_ssd") ||
      changed.count("bluestore_max_alloc_size") ||
      changed.count("bluestore_deferred_batch_ops") ||
      changed.count("bluestore_deferred_batch_ops_hdd") ||
      changed.count("bluestore_deferred_batch_ops_ssd")) {
    if (bdev) {
      // only after startup
      _set_alloc_sizes();
    }
  }
  if (changed.count("bluestore_throttle_cost_per_io") ||
      changed.count("bluestore_throttle_cost_per_io_hdd") ||
      changed.count("bluestore_throttle_cost_per_io_ssd")) {
    if (bdev) {
      _set_throttle_params();
    }
  }
  if (changed.count("bluestore_throttle_bytes") ||
      changed.count("bluestore_throttle_deferred_bytes") ||
      changed.count("bluestore_throttle_trace_rate")) {
    throttle.reset_throttle(conf);
  }
  if (changed.count("bluestore_max_defer_interval")) {
    if (bdev) {
      _set_max_defer_interval();
    }
  }
  if (changed.count("osd_memory_target") ||
      changed.count("osd_memory_base") ||
      changed.count("osd_memory_cache_min") ||
      changed.count("osd_memory_expected_fragmentation")) {
    _update_osd_memory_options();
  }
}

void BlueStore::_set_compression()
{
  auto m = Compressor::get_comp_mode_type(cct->_conf->bluestore_compression_mode);
  if (m) {
    _clear_compression_alert();
    comp_mode = *m;
  } else {
    derr << __func__ << " unrecognized value '"
         << cct->_conf->bluestore_compression_mode
         << "' for bluestore_compression_mode, reverting to 'none'"
         << dendl;
    comp_mode = Compressor::COMP_NONE;
    string s("unknown mode: ");
    s += cct->_conf->bluestore_compression_mode;
    _set_compression_alert(true, s.c_str());
  }

  compressor = nullptr;

  if (cct->_conf->bluestore_compression_min_blob_size) {
    comp_min_blob_size = cct->_conf->bluestore_compression_min_blob_size;
  } else {
    ceph_assert(bdev);
    if (_use_rotational_settings()) {
      comp_min_blob_size = cct->_conf->bluestore_compression_min_blob_size_hdd;
    } else {
      comp_min_blob_size = cct->_conf->bluestore_compression_min_blob_size_ssd;
    }
  }

  if (cct->_conf->bluestore_compression_max_blob_size) {
    comp_max_blob_size = cct->_conf->bluestore_compression_max_blob_size;
  } else {
    ceph_assert(bdev);
    if (_use_rotational_settings()) {
      comp_max_blob_size = cct->_conf->bluestore_compression_max_blob_size_hdd;
    } else {
      comp_max_blob_size = cct->_conf->bluestore_compression_max_blob_size_ssd;
    }
  }

  auto& alg_name = cct->_conf->bluestore_compression_algorithm;
  if (!alg_name.empty()) {
    compressor = Compressor::create(cct, alg_name);
    if (!compressor) {
      derr << __func__ << " unable to initialize " << alg_name.c_str() << " compressor"
           << dendl;
      _set_compression_alert(false, alg_name.c_str());
    }
  }
 
  dout(10) << __func__ << " mode " << Compressor::get_comp_mode_name(comp_mode)
	   << " alg " << (compressor ? compressor->get_type_name() : "(none)")
	   << " min_blob " << comp_min_blob_size
	   << " max_blob " << comp_max_blob_size
	   << dendl;
}

void BlueStore::_set_csum()
{
  csum_type = Checksummer::CSUM_NONE;
  int t = Checksummer::get_csum_string_type(cct->_conf->bluestore_csum_type);
  if (t > Checksummer::CSUM_NONE)
    csum_type = t;

  dout(10) << __func__ << " csum_type "
	   << Checksummer::get_csum_type_string(csum_type)
	   << dendl;
}

void BlueStore::_set_throttle_params()
{
  if (cct->_conf->bluestore_throttle_cost_per_io) {
    throttle_cost_per_io = cct->_conf->bluestore_throttle_cost_per_io;
  } else {
    ceph_assert(bdev);
    if (_use_rotational_settings()) {
      throttle_cost_per_io = cct->_conf->bluestore_throttle_cost_per_io_hdd;
    } else {
      throttle_cost_per_io = cct->_conf->bluestore_throttle_cost_per_io_ssd;
    }
  }

  dout(10) << __func__ << " throttle_cost_per_io " << throttle_cost_per_io
	   << dendl;
}
void BlueStore::_set_blob_size()
{
  if (cct->_conf->bluestore_max_blob_size) {
    max_blob_size = cct->_conf->bluestore_max_blob_size;
  } else {
    ceph_assert(bdev);
    if (_use_rotational_settings()) {
      max_blob_size = cct->_conf->bluestore_max_blob_size_hdd;
    } else {
      max_blob_size = cct->_conf->bluestore_max_blob_size_ssd;
    }
  }
  dout(10) << __func__ << " max_blob_size 0x" << std::hex << max_blob_size
           << std::dec << dendl;
}

void BlueStore::_update_osd_memory_options()
{
  osd_memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  osd_memory_base = cct->_conf.get_val<Option::size_t>("osd_memory_base");
  osd_memory_expected_fragmentation = cct->_conf.get_val<double>("osd_memory_expected_fragmentation");
  osd_memory_cache_min = cct->_conf.get_val<Option::size_t>("osd_memory_cache_min");
  config_changed++;
  dout(10) << __func__
           << " osd_memory_target " << osd_memory_target
           << " osd_memory_base " << osd_memory_base
           << " osd_memory_expected_fragmentation " << osd_memory_expected_fragmentation
           << " osd_memory_cache_min " << osd_memory_cache_min
           << dendl;
}

int BlueStore::_set_cache_sizes()
{
  ceph_assert(bdev);
  cache_autotune = cct->_conf.get_val<bool>("bluestore_cache_autotune");
  cache_autotune_interval =
      cct->_conf.get_val<double>("bluestore_cache_autotune_interval");
  cache_age_bin_interval =
      cct->_conf.get_val<double>("bluestore_cache_age_bin_interval");
  auto _set_bin = [&](std::string conf_name, std::vector<uint64_t>* intervals)
  {
    std::string intervals_str = cct->_conf.get_val<std::string>(conf_name);
    std::istringstream interval_stream(intervals_str);
    std::copy(
      std::istream_iterator<uint64_t>(interval_stream),
      std::istream_iterator<uint64_t>(),
      std::back_inserter(*intervals));
  };
  _set_bin("bluestore_cache_age_bins_kv", &kv_bins);
  _set_bin("bluestore_cache_age_bins_kv_onode", &kv_onode_bins);
  _set_bin("bluestore_cache_age_bins_meta", &meta_bins);
  _set_bin("bluestore_cache_age_bins_data", &data_bins);

  osd_memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  osd_memory_base = cct->_conf.get_val<Option::size_t>("osd_memory_base");
  osd_memory_expected_fragmentation =
      cct->_conf.get_val<double>("osd_memory_expected_fragmentation");
  osd_memory_cache_min = cct->_conf.get_val<Option::size_t>("osd_memory_cache_min");
  osd_memory_cache_resize_interval = 
      cct->_conf.get_val<double>("osd_memory_cache_resize_interval");

  if (cct->_conf->bluestore_cache_size) {
    cache_size = cct->_conf->bluestore_cache_size;
  } else {
    // choose global cache size based on backend type
    if (_use_rotational_settings()) {
      cache_size = cct->_conf->bluestore_cache_size_hdd;
    } else {
      cache_size = cct->_conf->bluestore_cache_size_ssd;
    }
  }

  cache_meta_ratio = cct->_conf.get_val<double>("bluestore_cache_meta_ratio");
  if (cache_meta_ratio < 0 || cache_meta_ratio > 1.0) {
    derr << __func__ << " bluestore_cache_meta_ratio (" << cache_meta_ratio
         << ") must be in range [0,1.0]" << dendl;
    return -EINVAL;
  }

  cache_kv_ratio = cct->_conf.get_val<double>("bluestore_cache_kv_ratio");
  if (cache_kv_ratio < 0 || cache_kv_ratio > 1.0) {
    derr << __func__ << " bluestore_cache_kv_ratio (" << cache_kv_ratio
         << ") must be in range [0,1.0]" << dendl;
    return -EINVAL;
  }

  cache_kv_onode_ratio = cct->_conf.get_val<double>("bluestore_cache_kv_onode_ratio");
  if (cache_kv_onode_ratio < 0 || cache_kv_onode_ratio > 1.0) {
    derr << __func__ << " bluestore_cache_kv_onode_ratio (" << cache_kv_onode_ratio
         << ") must be in range [0,1.0]" << dendl;
    return -EINVAL;
  }

  if (cache_meta_ratio + cache_kv_ratio + cache_kv_onode_ratio > 1.0) {
    derr << __func__ << " bluestore_cache_meta_ratio (" << cache_meta_ratio
         << ") + bluestore_cache_kv_ratio (" << cache_kv_ratio
         << ") + bluestore_cache_kv_onode_ratio (" << cache_kv_onode_ratio
         << ") = " << cache_meta_ratio + cache_kv_ratio + cache_kv_onode_ratio << "; must be <= 1.0"
         << dendl;
    return -EINVAL;
  }

  cache_data_ratio = (double)1.0 - 
                     (double)cache_meta_ratio - 
                     (double)cache_kv_ratio - 
                     (double)cache_kv_onode_ratio;
  if (cache_data_ratio < 0) {
    // deal with floating point imprecision
    cache_data_ratio = 0;
  }
    
  dout(1) << __func__ << " cache_size " << cache_size
          << " meta " << cache_meta_ratio
	  << " kv " << cache_kv_ratio
	  << " kv_onode " << cache_kv_onode_ratio
	  << " data " << cache_data_ratio
	  << dendl;
  return 0;
}

int BlueStore::write_meta(const std::string& key, const std::string& value)
{
  bluestore_bdev_label_t label;
  string p = path + "/block";
  int r = _read_bdev_label(cct, p, &label);
  if (r < 0) {
    return ObjectStore::write_meta(key, value);
  }
  label.meta[key] = value;
  r = _write_bdev_label(cct, p, label);
  ceph_assert(r == 0);
  return ObjectStore::write_meta(key, value);
}

int BlueStore::read_meta(const std::string& key, std::string *value)
{
  bluestore_bdev_label_t label;
  string p = path + "/block";
  int r = _read_bdev_label(cct, p, &label);
  if (r < 0) {
    return ObjectStore::read_meta(key, value);
  }
  auto i = label.meta.find(key);
  if (i == label.meta.end()) {
    return ObjectStore::read_meta(key, value);
  }
  *value = i->second;
  return 0;
}


// Reads configuration.
// Validates values.
//
// In future this should be the only place that reads meta,
// except initialization of components, like BlueFS, FreeListManager
//
// NOTE: Any configuration settings that affect data layout on disk
//       must be persisted to meta.
int BlueStore::read_meta_conf_check_env()
{
  int r = 0;
  std::string esb;
  r = read_meta("elastic_shared_blobs",&esb);
  if (r == 0) {
    if (esb != "1" && esb != "0") {
      derr << __func__ << " wrong meta.elastic_shared_blobs=" << esb << dendl;
      r = -EIO;
    } else {
      elastic_shared_blobs = esb == "1";
    }
  } else {
    if (r == -ENOENT) {
      dout(1) << __func__ << " meta.elastic_shared_blobs not set, using legacy mode" << dendl;
      elastic_shared_blobs = false;
      r = 0;
    }
  }
  return r;
}


void BlueStore::_init_logger()
{
  PerfCountersBuilder b(cct, "bluestore",
                        l_bluestore_first, l_bluestore_last);

  // space utilization stats
  //****************************************
  b.add_u64(l_bluestore_allocated, "allocated",
	    "Sum for allocated bytes",
	    "al_b",
	    PerfCountersBuilder::PRIO_CRITICAL,
	    unit_t(UNIT_BYTES));
  b.add_u64(l_bluestore_stored, "stored",
	    "Sum for stored bytes",
	    "st_b",
	    PerfCountersBuilder::PRIO_CRITICAL,
	    unit_t(UNIT_BYTES));
  b.add_u64(l_bluestore_fragmentation, "fragmentation_micros",
            "How fragmented bluestore free space is (free extents / max possible number of free extents) * 1000",
	    "fbss",
	    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64(l_bluestore_alloc_unit, "alloc_unit",
	    "allocation unit size in bytes",
	    "au_b",
	    PerfCountersBuilder::PRIO_CRITICAL,
	    unit_t(UNIT_BYTES));
  //****************************************

  // Update op processing state latencies
  //****************************************
  b.add_time_avg(l_bluestore_state_prepare_lat, "state_prepare_lat",
		 "Average prepare state latency",
		 "sprl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_aio_wait_lat, "state_aio_wait_lat",
		 "Average aio_wait state latency",
		 "sawl", PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg(l_bluestore_state_io_done_lat, "state_io_done_lat",
		 "Average io_done state latency",
		 "sidl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_kv_queued_lat, "state_kv_queued_lat",
		"Average kv_queued state latency",
		"skql", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_kv_committing_lat, "state_kv_commiting_lat",
		 "Average kv_commiting state latency",
		 "skcl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_kv_done_lat, "state_kv_done_lat",
		 "Average kv_done state latency",
		 "skdl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_finishing_lat, "state_finishing_lat",
		 "Average finishing state latency",
		 "sfnl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_done_lat, "state_done_lat",
		 "Average done state latency",
		 "sdnl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_deferred_queued_lat, "state_deferred_queued_lat",
		 "Average deferred_queued state latency",
		 "sdql", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_deferred_aio_wait_lat, "state_deferred_aio_wait_lat",
		 "Average aio_wait state latency",
		 "sdal", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_state_deferred_cleanup_lat, "state_deferred_cleanup_lat",
		 "Average cleanup state latency",
		 "sdcl", PerfCountersBuilder::PRIO_USEFUL);
  //****************************************

  // Update Transaction stats
  //****************************************
  b.add_time_avg(l_bluestore_throttle_lat, "txc_throttle_lat",
		 "Average submit throttle latency",
		 "th_l", PerfCountersBuilder::PRIO_CRITICAL);
  b.add_time_avg(l_bluestore_submit_lat, "txc_submit_lat",
		 "Average submit latency",
		 "s_l", PerfCountersBuilder::PRIO_CRITICAL);
  b.add_time_avg(l_bluestore_commit_lat, "txc_commit_lat",
		 "Average commit latency",
		 "c_l", PerfCountersBuilder::PRIO_CRITICAL);
  b.add_u64_counter(l_bluestore_txc, "txc_count", "Transactions committed");
  //****************************************

  // Read op stats
  //****************************************
  b.add_time_avg(l_bluestore_read_onode_meta_lat, "read_onode_meta_lat",
		 "Average read onode metadata latency",
		 "roml", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_read_wait_aio_lat, "read_wait_aio_lat",
		 "Average read I/O waiting latency",
		 "rwal", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_csum_lat, "csum_lat",
		 "Average checksum latency",
		 "csml", PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_read_eio, "read_eio",
                    "Read EIO errors propagated to high level callers");
  b.add_u64_counter(l_bluestore_reads_with_retries, "reads_with_retries",
                    "Read operations that required at least one retry due to failed checksum validation",
		    "rd_r", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_read_lat, "read_lat",
		 "Average read latency",
		 "r_l", PerfCountersBuilder::PRIO_CRITICAL);
  //****************************************

  // kv_thread latencies
  //****************************************
  b.add_time_avg(l_bluestore_kv_flush_lat, "kv_flush_lat",
		 "Average kv_thread flush latency",
		 "kfsl", PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg(l_bluestore_kv_commit_lat, "kv_commit_lat",
		 "Average kv_thread commit latency",
		 "kcol", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_kv_sync_lat, "kv_sync_lat",
		 "Average kv_sync thread latency",
		 "kscl", PerfCountersBuilder::PRIO_INTERESTING);
  b.add_time_avg(l_bluestore_kv_final_lat, "kv_final_lat",
		 "Average kv_finalize thread latency",
		 "kfll", PerfCountersBuilder::PRIO_INTERESTING);
  //****************************************

  // write op stats
  //****************************************
  b.add_u64_counter(l_bluestore_write_big, "write_big",
		    "Large aligned writes into fresh blobs");
  b.add_u64_counter(l_bluestore_write_big_bytes, "write_big_bytes",
		    "Large aligned writes into fresh blobs (bytes)",
		    NULL,
		    PerfCountersBuilder::PRIO_DEBUGONLY,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_write_big_blobs, "write_big_blobs",
		    "Large aligned writes into fresh blobs (blobs)");
  b.add_u64_counter(l_bluestore_write_big_deferred,
		    "write_big_deferred",
		    "Big overwrites using deferred");

  b.add_u64_counter(l_bluestore_write_small, "write_small",
		    "Small writes into existing or sparse small blobs");
  b.add_u64_counter(l_bluestore_write_small_bytes, "write_small_bytes",
		    "Small writes into existing or sparse small blobs (bytes)",
		    NULL,
		    PerfCountersBuilder::PRIO_DEBUGONLY,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_write_small_unused,
		    "write_small_unused",
		    "Small writes into unused portion of existing blob");
  b.add_u64_counter(l_bluestore_write_small_pre_read,
		    "write_small_pre_read",
		    "Small writes that required we read some data (possibly "
		    "cached) to fill out the block");

  b.add_u64_counter(l_bluestore_write_pad_bytes, "write_pad_bytes",
		    "Sum for write-op padded bytes",
		    NULL,
		    PerfCountersBuilder::PRIO_DEBUGONLY,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_write_penalty_read_ops, "write_penalty_read_ops",
		    "Sum for write penalty read ops");
  b.add_u64_counter(l_bluestore_write_new, "write_new",
		    "Write into new blob");

  b.add_u64_counter(l_bluestore_issued_deferred_writes,
		    "issued_deferred_writes",
		    "Total deferred writes issued");
  b.add_u64_counter(l_bluestore_issued_deferred_write_bytes,
		    "issued_deferred_write_bytes",
		    "Total bytes in issued deferred writes",
		    NULL,
		    PerfCountersBuilder::PRIO_DEBUGONLY,
		    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_submitted_deferred_writes,
		    "submitted_deferred_writes",
		    "Total deferred writes submitted to disk");
  b.add_u64_counter(l_bluestore_submitted_deferred_write_bytes,
		    "submitted_deferred_write_bytes",
		    "Total bytes submitted to disk by deferred writes",
		    NULL,
		    PerfCountersBuilder::PRIO_DEBUGONLY,
		    unit_t(UNIT_BYTES));

  b.add_u64_counter(l_bluestore_write_big_skipped_blobs,
      "write_big_skipped_blobs",
      "Large aligned writes into fresh blobs skipped due to zero detection (blobs)");
  b.add_u64_counter(l_bluestore_write_big_skipped_bytes,
      "write_big_skipped_bytes",
      "Large aligned writes into fresh blobs skipped due to zero detection (bytes)");
  b.add_u64_counter(l_bluestore_write_small_skipped,
      "write_small_skipped",
      "Small writes into existing or sparse small blobs skipped due to zero detection");
  b.add_u64_counter(l_bluestore_write_small_skipped_bytes,
      "write_small_skipped_bytes",
      "Small writes into existing or sparse small blobs skipped due to zero detection (bytes)");
  //****************************************

  // compressions stats
  //****************************************
  b.add_u64(l_bluestore_compressed, "compressed",
	    "Sum for stored compressed bytes",
	    "c", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluestore_compressed_allocated, "compressed_allocated",
	    "Sum for bytes allocated for compressed data",
	    "c_a", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_u64(l_bluestore_compressed_original, "compressed_original",
	    "Sum for original bytes that were compressed",
	    "c_o", PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  b.add_time_avg(l_bluestore_compress_lat, "compress_lat",
	    "Average compress latency",
	    "_cpl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_decompress_lat, "decompress_lat",
	    "Average decompress latency",
	    "dcpl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_compress_success_count, "compress_success_count",
	    "Sum for beneficial compress ops");
  b.add_u64_counter(l_bluestore_compress_rejected_count, "compress_rejected_count",
	    "Sum for compress ops rejected due to low net gain of space");
  //****************************************

  // onode cache stats
  //****************************************
  b.add_u64(l_bluestore_onodes, "onodes",
	    "Number of onodes in cache");
  b.add_u64(l_bluestore_pinned_onodes, "onodes_pinned",
            "Number of pinned onodes in cache");
  b.add_u64_counter(l_bluestore_onode_hits, "onode_hits",
		    "Count of onode cache lookup hits",
		    "o_ht", PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_onode_misses, "onode_misses",
		    "Count of onode cache lookup misses",
		    "o_ms", PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_onode_shard_hits, "onode_shard_hits",
		    "Count of onode shard cache lookups hits");
  b.add_u64_counter(l_bluestore_onode_shard_misses,
		    "onode_shard_misses",
		    "Count of onode shard cache lookups misses");
  b.add_u64(l_bluestore_extents, "onode_extents",
	    "Number of extents in cache");
  b.add_u64(l_bluestore_blobs, "onode_blobs",
	    "Number of blobs in cache");
  //****************************************

  // buffer cache stats
  //****************************************
  b.add_u64(l_bluestore_buffers, "buffers",
	    "Number of buffers in cache");
  b.add_u64(l_bluestore_buffer_bytes, "buffer_bytes",
	    "Number of buffer bytes in cache",
	     NULL,
	     PerfCountersBuilder::PRIO_DEBUGONLY,
	     unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_buffer_hit_bytes, "buffer_hit_bytes",
	    "Sum for bytes of read hit in the cache",
	    NULL,
	    PerfCountersBuilder::PRIO_DEBUGONLY,
	    unit_t(UNIT_BYTES));
  b.add_u64_counter(l_bluestore_buffer_miss_bytes, "buffer_miss_bytes",
	    "Sum for bytes of read missed in the cache",
	    NULL,
	    PerfCountersBuilder::PRIO_DEBUGONLY,
	    unit_t(UNIT_BYTES));
  //****************************************

  // internal stats
  //****************************************
  b.add_u64_counter(l_bluestore_onode_reshard, "onode_reshard",
		    "Onode extent map reshard events");
  b.add_u64_counter(l_bluestore_blob_split, "blob_split",
		    "Sum for blob splitting due to resharding");
  b.add_u64_counter(l_bluestore_extent_compress, "extent_compress",
		    "Sum for extents that have been removed due to compression");
  b.add_u64_counter(l_bluestore_gc_merged, "gc_merged",
		    "Sum for extents that have been merged due to garbage "
		    "collection");
  //****************************************
  // misc
  //****************************************
  b.add_u64_counter(l_bluestore_omap_iterator_count, "omap_iterator_count",
    "Open omap iterators count");
  b.add_u64_counter(l_bluestore_omap_rmkeys_count, "omap_rmkeys_count",
    "amount of omap keys removed via rmkeys");
  b.add_u64_counter(l_bluestore_omap_rmkey_ranges_count, "omap_rmkey_range_count",
    "amount of omap key ranges removed via rmkeys");
  //****************************************
  // other client ops latencies
  //****************************************
  b.add_time_avg(l_bluestore_omap_seek_to_first_lat, "omap_seek_to_first_lat",
    "Average omap iterator seek_to_first call latency",
    "osfl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_upper_bound_lat, "omap_upper_bound_lat",
    "Average omap iterator upper_bound call latency",
    "oubl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_lower_bound_lat, "omap_lower_bound_lat",
    "Average omap iterator lower_bound call latency",
    "olbl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_next_lat, "omap_next_lat",
    "Average omap iterator next call latency",
    "onxl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_get_keys_lat, "omap_get_keys_lat",
    "Average omap get_keys call latency",
    "ogkl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_get_values_lat, "omap_get_values_lat",
    "Average omap get_values call latency",
    "ogvl", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_omap_clear_lat, "omap_clear_lat",
    "Average omap clear call latency");
  b.add_time_avg(l_bluestore_clist_lat, "clist_lat",
    "Average collection listing latency",
    "cl_l", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_remove_lat, "remove_lat",
    "Average removal latency",
    "rm_l", PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_bluestore_truncate_lat, "truncate_lat",
    "Average truncate latency",
    "tr_l", PerfCountersBuilder::PRIO_USEFUL);
  //****************************************

  // slow op count
  //****************************************
  b.add_u64_counter(l_bluestore_slow_aio_wait_count,
    "slow_aio_wait_count",
    "Slow op count for aio wait",
    "sawc",
    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_slow_committed_kv_count,
    "slow_committed_kv_count",
    "Slow op count for committed kv",
    "sckc",
    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_slow_read_onode_meta_count,
    "slow_read_onode_meta_count",
    "Slow op count for read onode meta",
    "sroc",
    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_bluestore_slow_read_wait_aio_count,
    "slow_read_wait_aio_count",
    "Slow op count for read wait aio",
    "srwc",
    PerfCountersBuilder::PRIO_USEFUL);

  // Resulting size axis configuration for op histograms, values are in bytes
  PerfHistogramCommon::axis_config_d alloc_hist_x_axis_config{
    "Given size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    4096,                            ///< Quantization unit
    13,                               ///< Enough to cover 4+M requests
  };
  // Req size axis configuration for op histograms, values are in bytes
  PerfHistogramCommon::axis_config_d alloc_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    4096,                            ///< Quantization unit
    13,                               ///< Enough to cover 4+M requests
  };
  b.add_u64_counter_histogram(
    l_bluestore_allocate_hist, "allocate_histogram",
    alloc_hist_x_axis_config, alloc_hist_y_axis_config,
    "Histogram of requested block allocations vs. given ones");
  b.add_time_avg(l_bluestore_allocator_lat, "allocator_lat",
    "Average bluestore allocator latency",
    "bsal",
    PerfCountersBuilder::PRIO_USEFUL);

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

int BlueStore::_reload_logger()
{
  struct store_statfs_t store_statfs;
  int r = statfs(&store_statfs);
  if (r >= 0) {
    logger->set(l_bluestore_allocated, store_statfs.allocated);
    logger->set(l_bluestore_stored, store_statfs.data_stored);
    logger->set(l_bluestore_compressed, store_statfs.data_compressed);
    logger->set(l_bluestore_compressed_allocated, store_statfs.data_compressed_allocated);
    logger->set(l_bluestore_compressed_original, store_statfs.data_compressed_original);
  }
  return r;
}

void BlueStore::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

int BlueStore::get_block_device_fsid(CephContext* cct, const string& path,
				     uuid_d *fsid)
{
  bluestore_bdev_label_t label;
  int r = _read_bdev_label(cct, path, &label);
  if (r < 0)
    return r;
  *fsid = label.osd_uuid;
  return 0;
}

int BlueStore::_open_path()
{
  // sanity check(s)
  ceph_assert(path_fd < 0);
  path_fd = TEMP_FAILURE_RETRY(::open(path.c_str(), O_DIRECTORY|O_CLOEXEC));
  if (path_fd < 0) {
    int r = -errno;
    derr << __func__ << " unable to open " << path << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  return 0;
}

void BlueStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
}

int BlueStore::_write_bdev_label(CephContext *cct,
				 const string &path, bluestore_bdev_label_t label)
{
  dout(10) << __func__ << " path " << path << " label " << label << dendl;
  bufferlist bl;
  encode(label, bl);
  uint32_t crc = bl.crc32c(-1);
  encode(crc, bl);
  ceph_assert(bl.length() <= BDEV_LABEL_BLOCK_SIZE);
  bufferptr z(BDEV_LABEL_BLOCK_SIZE - bl.length());
  z.zero();
  bl.append(std::move(z));

  int fd = TEMP_FAILURE_RETRY(::open(path.c_str(), O_WRONLY|O_CLOEXEC|O_DIRECT));
  if (fd < 0) {
    fd = -errno;
    derr << __func__ << " failed to open " << path << ": " << cpp_strerror(fd)
	 << dendl;
    return fd;
  }
  bl.rebuild_aligned_size_and_memory(BDEV_LABEL_BLOCK_SIZE, BDEV_LABEL_BLOCK_SIZE, IOV_MAX);
  int r = bl.write_fd(fd);
  if (r < 0) {
    derr << __func__ << " failed to write to " << path
	 << ": " << cpp_strerror(r) << dendl;
    goto out;
  }
  r = ::fsync(fd);
  if (r < 0) {
    derr << __func__ << " failed to fsync " << path
	 << ": " << cpp_strerror(r) << dendl;
  }
out:
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return r;
}

int BlueStore::_read_bdev_label(CephContext* cct, const string &path,
				bluestore_bdev_label_t *label)
{
  dout(10) << __func__ << dendl;
  int fd = TEMP_FAILURE_RETRY(::open(path.c_str(), O_RDONLY|O_CLOEXEC));
  if (fd < 0) {
    fd = -errno;
    derr << __func__ << " failed to open " << path << ": " << cpp_strerror(fd)
	 << dendl;
    return fd;
  }
  bufferlist bl;
  int r = bl.read_fd(fd, BDEV_LABEL_BLOCK_SIZE);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  if (r < 0) {
    derr << __func__ << " failed to read from " << path
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  uint32_t crc, expected_crc;
  auto p = bl.cbegin();
  try {
    decode(*label, p);
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);
    decode(expected_crc, p);
  }
  catch (ceph::buffer::error& e) {
    derr << __func__ << " unable to decode label " << path.c_str()
         << " at offset " << p.get_off()
	 << ": " << e.what()
	 << dendl;
    return -ENOENT;
  }
  if (crc != expected_crc) {
    derr << __func__ << " bad crc on label, expected " << expected_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " got " << *label << dendl;
  return 0;
}

int BlueStore::_check_or_set_bdev_label(
  string path, uint64_t size, string desc, bool create)
{
  bluestore_bdev_label_t label;
  if (create) {
    label.osd_uuid = fsid;
    label.size = size;
    label.btime = ceph_clock_now();
    label.description = desc;
    int r = _write_bdev_label(cct, path, label);
    if (r < 0)
      return r;
  } else {
    int r = _read_bdev_label(cct, path, &label);
    if (r < 0)
      return r;
    if (cct->_conf->bluestore_debug_permit_any_bdev_label) {
      dout(20) << __func__ << " bdev " << path << " fsid " << label.osd_uuid
	   << " and fsid " << fsid << " check bypassed" << dendl;
    } else if (label.osd_uuid != fsid) {
      derr << __func__ << " bdev " << path << " fsid " << label.osd_uuid
	   << " does not match our fsid " << fsid << dendl;
      return -EIO;
    }
  }
  return 0;
}

void BlueStore::_set_alloc_sizes(void)
{
  max_alloc_size = cct->_conf->bluestore_max_alloc_size;

  if (cct->_conf->bluestore_prefer_deferred_size) {
    prefer_deferred_size = cct->_conf->bluestore_prefer_deferred_size;
  } else {
    if (_use_rotational_settings()) {
      prefer_deferred_size = cct->_conf->bluestore_prefer_deferred_size_hdd;
    } else {
      prefer_deferred_size = cct->_conf->bluestore_prefer_deferred_size_ssd;
    }
  }

  if (cct->_conf->bluestore_deferred_batch_ops) {
    deferred_batch_ops = cct->_conf->bluestore_deferred_batch_ops;
  } else {
    if (_use_rotational_settings()) {
      deferred_batch_ops = cct->_conf->bluestore_deferred_batch_ops_hdd;
    } else {
      deferred_batch_ops = cct->_conf->bluestore_deferred_batch_ops_ssd;
    }
  }

  dout(10) << __func__ << " min_alloc_size 0x" << std::hex << min_alloc_size
	   << std::dec << " order " << (int)min_alloc_size_order
	   << " max_alloc_size 0x" << std::hex << max_alloc_size
	   << " prefer_deferred_size 0x" << prefer_deferred_size
	   << std::dec
	   << " deferred_batch_ops " << deferred_batch_ops
	   << dendl;
}

int BlueStore::_open_bdev(bool create)
{
  ceph_assert(bdev == NULL);
  string p = path + "/block";
  bdev = BlockDevice::create(cct, p, aio_cb, static_cast<void*>(this), discard_cb, static_cast<void*>(this));
  int r = bdev->open(p);
  if (r < 0)
    goto fail;

  if (create && cct->_conf->bdev_enable_discard) {
    interval_set<uint64_t> whole_device;
    whole_device.insert(0, bdev->get_size());
    bdev->try_discard(whole_device, false);
  }

  if (bdev->supported_bdev_label()) {
    r = _check_or_set_bdev_label(p, bdev->get_size(), "main", create);
    if (r < 0)
      goto fail_close;
  }

  // initialize global block parameters
  block_size = bdev->get_block_size();
  block_mask = ~(block_size - 1);
  block_size_order = std::countr_zero(block_size);
  ceph_assert(block_size == 1u << block_size_order);
  _set_max_defer_interval();
  // and set cache_size based on device type
  r = _set_cache_sizes();
  if (r < 0) {
    goto fail_close;
  }
  // get block dev optimal io size
  optimal_io_size = bdev->get_optimal_io_size();

  return 0;

 fail_close:
  bdev->close();
 fail:
  delete bdev;
  bdev = NULL;
  return r;
}

void BlueStore::_validate_bdev()
{
  ceph_assert(bdev);
  uint64_t dev_size = bdev->get_size();
  ceph_assert(dev_size > _get_ondisk_reserved());
}

void BlueStore::_close_bdev()
{
  ceph_assert(bdev);
  bdev->close();
  delete bdev;
  bdev = NULL;
}

int BlueStore::_open_fm(KeyValueDB::Transaction t,
                        bool read_only,
                        bool db_avail,
                        bool fm_restore)
{
  int r;

  dout(5) << __func__ << "::NCB::freelist_type=" << freelist_type << dendl;
  ceph_assert(fm == NULL);
  // fm_restore means we are transitioning from null-fm to bitmap-fm
  ceph_assert(!fm_restore || (freelist_type != "null"));
  // fm restore must pass in a valid transaction
  ceph_assert(!fm_restore || (t != nullptr));

  // when function is called in repair mode (to_repair=true) we skip db->open()/create()
  bool can_have_null_fm = !is_db_rotational() &&
                          !read_only &&
                          db_avail &&
                          cct->_conf->bluestore_allocation_from_file;

  // When allocation-info is stored in a single file we set freelist_type to "null"
  if (can_have_null_fm) {
    freelist_type = "null";
    need_to_destage_allocation_file = true;
  }
  fm = FreelistManager::create(cct, freelist_type, PREFIX_ALLOC);
  ceph_assert(fm);
  if (t) {
    // create mode. initialize freespace
    dout(20) << __func__ << " initializing freespace" << dendl;
    {
      bufferlist bl;
      bl.append(freelist_type);
      t->set(PREFIX_SUPER, "freelist_type", bl);
    }
    // being able to allocate in units less than bdev block size 
    // seems to be a bad idea.
    ceph_assert(cct->_conf->bdev_block_size <= min_alloc_size);

    uint64_t alloc_size = min_alloc_size;
    if (!bdev->is_smr() && freelist_type == "zoned") {
      derr << "non-SMR device (or SMR support not built-in) but freelist_type = zoned"
	   << dendl;
      return -EINVAL;
    }

    fm->create(bdev->get_size(), alloc_size, t);

    // allocate superblock reserved space.  note that we do not mark
    // bluefs space as allocated in the freelist; we instead rely on
    // bluefs doing that itself.
    auto reserved = _get_ondisk_reserved();
    if (fm_restore) {
      // we need to allocate the full space in restore case
      // as later we will add free-space marked in the allocator file
      fm->allocate(0, bdev->get_size(), t);
    } else {
      // allocate superblock reserved space.  note that we do not mark
      // bluefs space as allocated in the freelist; we instead rely on
      // bluefs doing that itself.
      fm->allocate(0, reserved, t);
    }
    // debug code - not needed for NULL FM
    if (cct->_conf->bluestore_debug_prefill > 0) {
      uint64_t end = bdev->get_size() - reserved;
      dout(1) << __func__ << " pre-fragmenting freespace, using "
	      << cct->_conf->bluestore_debug_prefill << " with max free extent "
	      << cct->_conf->bluestore_debug_prefragment_max << dendl;
      uint64_t start = p2roundup(reserved, min_alloc_size);
      uint64_t max_b = cct->_conf->bluestore_debug_prefragment_max / min_alloc_size;
      float r = cct->_conf->bluestore_debug_prefill;
      r /= 1.0 - r;
      bool stop = false;

      while (!stop && start < end) {
	uint64_t l = (rand() % max_b + 1) * min_alloc_size;
	if (start + l > end) {
	  l = end - start;
          l = p2align(l, min_alloc_size);
        }
        ceph_assert(start + l <= end);

	uint64_t u = 1 + (uint64_t)(r * (double)l);
	u = p2roundup(u, min_alloc_size);
        if (start + l + u > end) {
          u = end - (start + l);
          // trim to align so we don't overflow again
          u = p2align(u, min_alloc_size);
          stop = true;
        }
        ceph_assert(start + l + u <= end);

	dout(20) << __func__ << " free 0x" << std::hex << start << "~" << l
		 << " use 0x" << u << std::dec << dendl;

        if (u == 0) {
          // break if u has been trimmed to nothing
          break;
        }

	fm->allocate(start + l, u, t);
	start += l + u;
      }
    }
    r = _write_out_fm_meta(0);
    ceph_assert(r == 0);
  } else {
    if (can_have_null_fm) {
      commit_to_null_manager();
    }
    r = fm->init(db, read_only,
      [&](const std::string& key, std::string* result) {
        return read_meta(key, result);
    });
    if (r < 0) {
      derr << __func__ << " failed: " << cpp_strerror(r) << dendl;
      delete fm;
      fm = NULL;
      return r;
    }
  }
  // if space size tracked by free list manager is that higher than actual
  // dev size one can hit out-of-space allocation which will result
  // in data loss and/or assertions
  // Probably user altered the device size somehow.
  // The only fix for now is to redeploy OSD.
  if (fm->get_size() >= bdev->get_size() + min_alloc_size) {
    ostringstream ss;
    ss << "slow device size mismatch detected, "
	<< " fm size(" << fm->get_size()
	<< ") > slow device size(" << bdev->get_size()
	<< "), Please stop using this OSD as it might cause data loss.";
    _set_disk_size_mismatch_alert(ss.str());
  }
  return 0;
}

void BlueStore::_close_fm()
{
  dout(10) << __func__ << dendl;
  ceph_assert(fm);
  fm->shutdown();
  delete fm;
  fm = NULL;
}

int BlueStore::_write_out_fm_meta(uint64_t target_size)
{
  int r = 0;
  string p = path + "/block";

  std::vector<std::pair<string, string>> fm_meta;
  fm->get_meta(target_size, &fm_meta);

  for (auto& m : fm_meta) {
    r = write_meta(m.first, m.second);
    ceph_assert(r == 0);
  }
  return r;
}

int BlueStore::_create_alloc()
{
  ceph_assert(alloc == NULL);
  ceph_assert(shared_alloc.a == NULL);
  ceph_assert(bdev->get_size());

  uint64_t alloc_size = min_alloc_size;

  std::string allocator_type = cct->_conf->bluestore_allocator;

  alloc = Allocator::create(
    cct, allocator_type,
    bdev->get_size(),
    alloc_size,
    "block");
  if (!alloc) {
    lderr(cct) << __func__ << " failed to create " << allocator_type << " allocator"
	       << dendl;
    return -EINVAL;
  }

  // BlueFS will share the same allocator
  shared_alloc.set(alloc, alloc_size);

  return 0;
}

int BlueStore::_init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments)
{
  int r = _create_alloc();
  if (r < 0) {
    return r;
  }
  ceph_assert(alloc != NULL);

  uint64_t num = 0, bytes = 0;
  utime_t start_time = ceph_clock_now();
  if (!fm->is_null_manager()) {
    // This is the original path - loading allocation map from RocksDB and feeding into the allocator
    dout(5) << __func__ << "::NCB::loading allocation from FM -> alloc" << dendl;
    // initialize from freelist
    fm->enumerate_reset();
    uint64_t offset, length;
    while (fm->enumerate_next(db, &offset, &length)) {
      alloc->init_add_free(offset, length);
      ++num;
      bytes += length;
    }
    fm->enumerate_reset();

    utime_t duration = ceph_clock_now() - start_time;
    dout(5) << __func__ << "::num_entries=" << num << " free_size=" << bytes << " alloc_size=" <<
      alloc->get_capacity() - bytes << " time=" << duration << " seconds" << dendl;
  } else {
    // This is the new path reading the allocation map from a flat bluefs file and feeding them into the allocator

    if (!cct->_conf->bluestore_allocation_from_file) {
      derr << __func__ << "::NCB::cct->_conf->bluestore_allocation_from_file is set to FALSE with an active NULL-FM" << dendl;
      derr << __func__ << "::NCB::Please change the value of bluestore_allocation_from_file to TRUE in your ceph.conf file" << dendl;
      return -ENOTSUP; // Operation not supported
    }
    if (restore_allocator(alloc, &num, &bytes) == 0) {
      dout(5) << __func__ << "::NCB::restore_allocator() completed successfully alloc=" << alloc << dendl;
    } else {
      // This must mean that we had an unplanned shutdown and didn't manage to destage the allocator
      dout(0) << __func__ << "::NCB::restore_allocator() failed! Run Full Recovery from ONodes (might take a while) ..." << dendl;
      // if failed must recover from on-disk ONode internal state
      if (read_allocation_from_drive_on_startup() != 0) {
	derr << __func__ << "::NCB::Failed Recovery" << dendl;
	derr << __func__ << "::NCB::Ceph-OSD won't start, make sure your drives are connected and readable" << dendl;
	derr << __func__ << "::NCB::If no HW fault is found, please report failure and consider redeploying OSD" << dendl;
	return -ENOTRECOVERABLE;
      }
    }
  }
  dout(1) << __func__
          << " loaded " << byte_u_t(bytes) << " in " << num << " extents"
          << std::hex
          << ", allocator type " << alloc->get_type()
          << ", capacity 0x" << alloc->get_capacity()
          << ", block size 0x" << alloc->get_block_size()
          << ", free 0x" << alloc->get_free()
          << ", fragmentation " << alloc->get_fragmentation()
          << std::dec << dendl;

  return 0;
}

void BlueStore::_post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments)
{
  int r = 0;
  if (fm->is_null_manager()) {
    // Now that we load the allocation map we need to invalidate the file as new allocation won't be reflected
    // Changes to the allocation map (alloc/release) are not updated inline and will only be stored on umount()
    // This means that we should not use the existing file on failure case (unplanned shutdown) and must resort
    //  to recovery from RocksDB::ONodes
    r = invalidate_allocation_file_on_bluefs();
  }
  ceph_assert(r >= 0);
}

void BlueStore::_close_alloc()
{
  ceph_assert(bdev);
  bdev->discard_drain();

  ceph_assert(alloc);
  alloc->shutdown();
  delete alloc;

  ceph_assert(shared_alloc.a);
  if (alloc != shared_alloc.a) {
    shared_alloc.a->shutdown();
    delete shared_alloc.a;
  }

  shared_alloc.reset();
  alloc = nullptr;
}

int BlueStore::_open_fsid(bool create)
{
  ceph_assert(fsid_fd < 0);
  int flags = O_RDWR|O_CLOEXEC;
  if (create)
    flags |= O_CREAT;
  fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
  if (fsid_fd < 0) {
    int err = -errno;
    derr << __func__ << " " << cpp_strerror(err) << dendl;
    return err;
  }
  return 0;
}

int BlueStore::_read_fsid(uuid_d *uuid)
{
  char fsid_str[40];
  memset(fsid_str, 0, sizeof(fsid_str));
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0) {
    derr << __func__ << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  if (ret > 36)
    fsid_str[36] = 0;
  else
    fsid_str[ret] = 0;
  if (!uuid->parse(fsid_str)) {
    derr << __func__ << " unparsable uuid " << fsid_str << dendl;
    return -EINVAL;
  }
  return 0;
}

int BlueStore::_write_fsid()
{
  int r = ::ftruncate(fsid_fd, 0);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  string str = stringify(fsid) + "\n";
  r = safe_write(fsid_fd, str.c_str(), str.length());
  if (r < 0) {
    derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = ::fsync(fsid_fd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void BlueStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int BlueStore::_lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    derr << __func__ << " failed to lock " << path << "/fsid"
	 << " (is another ceph-osd still running?)"
	 << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool BlueStore::is_rotational()
{
  if (bdev) {
    return bdev->is_rotational();
  }

  bool rotational = true;
  int r = _open_path();
  if (r < 0)
    goto out;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;
  r = _read_fsid(&fsid);
  if (r < 0)
    goto out_fsid;
  r = _lock_fsid();
  if (r < 0)
    goto out_fsid;
  r = _open_bdev(false);
  if (r < 0)
    goto out_fsid;
  rotational = bdev->is_rotational();
  _close_bdev();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  out:
  return rotational;
}

bool BlueStore::is_journal_rotational()
{
  if (!bluefs) {
    dout(5) << __func__ << " bluefs disabled, default to store media type"
            << dendl;
    return is_rotational();
  }
  dout(10) << __func__ << " " << (int)bluefs->wal_is_rotational() << dendl;
  return bluefs->wal_is_rotational();
}

bool BlueStore::is_db_rotational()
{
  if (!bluefs) {
    dout(5) << __func__ << " bluefs disabled, default to store media type"
            << dendl;
    return is_rotational();
  }
  dout(10) << __func__ << " " << (int)bluefs->db_is_rotational() << dendl;
  return bluefs->db_is_rotational();
}

bool BlueStore::_use_rotational_settings()
{
  if (cct->_conf->bluestore_debug_enforce_settings == "hdd") {
    return true;
  }
  if (cct->_conf->bluestore_debug_enforce_settings == "ssd") {
    return false;
  }
  return bdev->is_rotational();
}

bool BlueStore::is_statfs_recoverable() const
{
  // abuse fm for now
  return has_null_manager();
}

bool BlueStore::test_mount_in_use()
{
  // most error conditions mean the mount is not in use (e.g., because
  // it doesn't exist).  only if we fail to lock do we conclude it is
  // in use.
  bool ret = false;
  int r = _open_path();
  if (r < 0)
    return false;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;
  r = _lock_fsid();
  if (r < 0)
    ret = true; // if we can't lock, it is in use
  _close_fsid();
 out_path:
  _close_path();
  return ret;
}

int BlueStore::_minimal_open_bluefs(bool create)
{
  int r;
  bluefs = new BlueFS(cct);

  string bfn;
  struct stat st;

  bfn = path + "/block.db";
  if (::stat(bfn.c_str(), &st) == 0) {
    r = bluefs->add_block_device(
      BlueFS::BDEV_DB, bfn,
      create && cct->_conf->bdev_enable_discard);
    if (r < 0) {
      derr << __func__ << " add block device(" << bfn << ") returned: "
            << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }

    if (bluefs->bdev_support_label(BlueFS::BDEV_DB)) {
      r = _check_or_set_bdev_label(
	bfn,
	bluefs->get_block_device_size(BlueFS::BDEV_DB),
        "bluefs db", create);
      if (r < 0) {
        derr << __func__
	      << " check block device(" << bfn << ") label returned: "
              << cpp_strerror(r) << dendl;
        goto free_bluefs;
      }
    }
    bluefs_layout.shared_bdev = BlueFS::BDEV_SLOW;
    bluefs_layout.dedicated_db = true;
  } else {
    r = -errno;
    if (::lstat(bfn.c_str(), &st) == -1) {
      r = 0;
      bluefs_layout.shared_bdev = BlueFS::BDEV_DB;
    } else {
      derr << __func__ << " " << bfn << " symlink exists but target unusable: "
	    << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }
  }

  // shared device
  bfn = path + "/block";
  // never trim here
  r = bluefs->add_block_device(bluefs_layout.shared_bdev, bfn, false,
                               &shared_alloc);
  if (r < 0) {
    derr << __func__ << " add block device(" << bfn << ") returned: "
	  << cpp_strerror(r) << dendl;
    goto free_bluefs;
  }

  bfn = path + "/block.wal";
  if (::stat(bfn.c_str(), &st) == 0) {
    r = bluefs->add_block_device(BlueFS::BDEV_WAL, bfn,
				 create && cct->_conf->bdev_enable_discard);
    if (r < 0) {
      derr << __func__ << " add block device(" << bfn << ") returned: "
	    << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }

    if (bluefs->bdev_support_label(BlueFS::BDEV_WAL)) {
      r = _check_or_set_bdev_label(
	bfn,
	bluefs->get_block_device_size(BlueFS::BDEV_WAL),
        "bluefs wal", create);
      if (r < 0) {
        derr << __func__ << " check block device(" << bfn
              << ") label returned: " << cpp_strerror(r) << dendl;
        goto free_bluefs;
      }
    }

    bluefs_layout.dedicated_wal = true;
  } else {
    r = 0;
    if (::lstat(bfn.c_str(), &st) != -1) {
      r = -errno;
      derr << __func__ << " " << bfn << " symlink exists but target unusable: "
           << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }
  }
  return 0;

free_bluefs:
  ceph_assert(bluefs);
  delete bluefs;
  bluefs = NULL;
  return r;
}

int BlueStore::_open_bluefs(bool create, bool read_only)
{
  int r = _minimal_open_bluefs(create);
  if (r < 0) {
    return r;
  }
  BlueFSVolumeSelector* vselector = nullptr;
  if (bluefs_layout.shared_bdev == BlueFS::BDEV_SLOW ||
      cct->_conf->bluestore_volume_selection_policy == "use_some_extra_enforced" ||
      cct->_conf->bluestore_volume_selection_policy == "fit_to_fast") {

    string options = cct->_conf->bluestore_rocksdb_options;
    string options_annex = cct->_conf->bluestore_rocksdb_options_annex;
    if (!options_annex.empty()) {
      if (!options.empty() &&
        *options.rbegin() != ',') {
        options += ',';
      }
      options += options_annex;
    }

    rocksdb::Options rocks_opts;
    r = RocksDBStore::ParseOptionsFromStringStatic(
      cct,
      options,
      rocks_opts,
      nullptr);
    if (r < 0) {
      return r;
    }
    if (cct->_conf->bluestore_volume_selection_policy == "fit_to_fast") {
      vselector = new FitToFastVolumeSelector(
        bluefs->get_block_device_size(BlueFS::BDEV_WAL) * 95 / 100,
        bluefs->get_block_device_size(BlueFS::BDEV_DB) * 95 / 100,
        bluefs->get_block_device_size(BlueFS::BDEV_SLOW) * 95 / 100);
    } else {
      double reserved_factor = cct->_conf->bluestore_volume_selection_reserved_factor;
      vselector =
        new RocksDBBlueFSVolumeSelector(
          bluefs->get_block_device_size(BlueFS::BDEV_WAL) * 95 / 100,
          bluefs->get_block_device_size(BlueFS::BDEV_DB) * 95 / 100,
          bluefs->get_block_device_size(BlueFS::BDEV_SLOW) * 95 / 100,
	  rocks_opts.write_buffer_size * rocks_opts.max_write_buffer_number,
          rocks_opts.max_bytes_for_level_base,
          rocks_opts.max_bytes_for_level_multiplier,
          reserved_factor,
          cct->_conf->bluestore_volume_selection_reserved,
          cct->_conf->bluestore_volume_selection_policy.find("use_some_extra")
             == 0);
    }    
  }
  if (create) {
    bluefs->mkfs(fsid, bluefs_layout);
  }
  bluefs->set_volume_selector(vselector);
  r = bluefs->mount();
  if (r < 0) {
    derr << __func__ << " failed bluefs mount: " << cpp_strerror(r) << dendl;
  }
  ceph_assert_always(bluefs->maybe_verify_layout(bluefs_layout) == 0);
  return r;
}

void BlueStore::_close_bluefs()
{
  bluefs->umount(db_was_opened_read_only);
  _minimal_close_bluefs();
}

void BlueStore::_minimal_close_bluefs()
{
  delete bluefs;
  bluefs = NULL;
}

int BlueStore::_is_bluefs(bool create, bool* ret)
{
  if (create) {
    *ret = cct->_conf->bluestore_bluefs;
  } else {
    string s;
    int r = read_meta("bluefs", &s);
    if (r < 0) {
      derr << __func__ << " unable to read 'bluefs' meta" << dendl;
      return -EIO;
    }
    if (s == "1") {
      *ret = true;
    } else if (s == "0") {
      *ret = false;
    } else {
      derr << __func__ << " bluefs = " << s << " : not 0 or 1, aborting"
	   << dendl;
      return -EIO;
    }
  }
  return 0;
}

/*
* opens both DB and dependant super_meta, FreelistManager and allocator
* in the proper order
*/
int BlueStore::_open_db_and_around(bool read_only, bool to_repair)
{
  dout(5) << __func__ << "::NCB::read_only=" << read_only << ", to_repair=" << to_repair << dendl;
  {
    string type;
    int r = read_meta("type", &type);
    if (r < 0) {
      derr << __func__ << " failed to load os-type: " << cpp_strerror(r)
        << dendl;
      return r;
    }

    if (type != "bluestore") {
      derr << __func__ << " expected bluestore, but type is " << type << dendl;
      return -EIO;
    }
  }

  // SMR devices may require a freelist adjustment, but that can only happen after
  // the db is read-write. we'll stash pending changes here.
  std::map<uint64_t, uint64_t> zone_adjustments;

  int r = _open_path();
  if (r < 0)
    return r;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;

  r = _read_fsid(&fsid);
  if (r < 0)
    goto out_fsid;

  r = _lock_fsid();
  if (r < 0)
    goto out_fsid;

  r = _open_bdev(false);
  if (r < 0)
    goto out_fsid;

  // GBH: can probably skip open_db step in REad-Only mode when operating in NULL-FM mode
  // (might need to open if failed to restore from file)

  // open in read-only first to read FM list and init allocator
  // as they might be needed for some BlueFS procedures
  r = _open_db(false, false, true);
  if (r < 0)
    goto out_bdev;

  r = _open_super_meta();
  if (r < 0) {
    goto out_db;
  }

  r = _open_fm(nullptr, true, false);
  if (r < 0)
    goto out_db;

  r = _init_alloc(&zone_adjustments);
  if (r < 0)
    goto out_fm;

  // Re-open in the proper mode(s).

  // Can't simply bypass second open for read-only mode as we need to
  // load allocated extents from bluefs into allocator.
  // And now it's time to do that
  //
  _close_db();
  r = _open_db(false, to_repair, read_only);
  if (r < 0) {
    goto out_alloc;
  }

  if (!read_only) {
    _post_init_alloc(zone_adjustments);
  }

  // when function is called in repair mode (to_repair=true) we skip db->open()/create()
  // we can't change bluestore allocation so no need to invlidate allocation-file
  if (fm->is_null_manager() && !read_only && !to_repair) {
    // Now that we load the allocation map we need to invalidate the file as new allocation won't be reflected
    // Changes to the allocation map (alloc/release) are not updated inline and will only be stored on umount()
    // This means that we should not use the existing file on failure case (unplanned shutdown) and must resort
    //  to recovery from RocksDB::ONodes
    r = invalidate_allocation_file_on_bluefs();
    if (r != 0) {
      derr << __func__ << "::NCB::invalidate_allocation_file_on_bluefs() failed!" << dendl;
      goto out_alloc;
    }
  }

  // when function is called in repair mode (to_repair=true) we skip db->open()/create()
  if (!is_db_rotational() && !read_only && !to_repair && cct->_conf->bluestore_allocation_from_file) {
    dout(5) << __func__ << "::NCB::Commit to Null-Manager" << dendl;
    commit_to_null_manager();
    need_to_destage_allocation_file = true;
    dout(10) << __func__ << "::NCB::need_to_destage_allocation_file was set" << dendl;
  }

  return 0;

out_alloc:
  _close_alloc();
out_fm:
  _close_fm();
 out_db:
  _close_db();
 out_bdev:
  _close_bdev();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  return r;
}

void BlueStore::_close_db_and_around()
{
  if (db) {
    _close_db();
  }
  _close_around_db();
}

void BlueStore::_close_around_db()
{
  if (bluefs) {
    _close_bluefs();
  }
  _close_fm();
  _close_alloc();
  _close_bdev();
  _close_fsid();
  _close_path();
}

int BlueStore::open_db_environment(KeyValueDB **pdb, bool to_repair)
{
  _kv_only = true;
  int r = _open_db_and_around(false, to_repair);
  if (r == 0) {
    *pdb = db;
  } else {
    *pdb = nullptr;
  }
  return r;
}

int BlueStore::close_db_environment()
{
  if (db) {
    delete db;
    db = nullptr;
  }
  _close_around_db();
  return 0;
}

/* gets access to bluefs supporting RocksDB */
BlueFS* BlueStore::get_bluefs() {
  return bluefs;
}

int BlueStore::_prepare_db_environment(bool create, bool read_only,
				       std::string* _fn, std::string* _kv_backend)
{
  int r;
  ceph_assert(!db);
  std::string& fn=*_fn;
  std::string& kv_backend=*_kv_backend;
  fn = path + "/db";
  std::shared_ptr<Int64ArrayMergeOperator> merge_op(new Int64ArrayMergeOperator);

  if (create) {
    kv_backend = cct->_conf->bluestore_kvbackend;
  } else {
    r = read_meta("kv_backend", &kv_backend);
    if (r < 0) {
      derr << __func__ << " unable to read 'kv_backend' meta" << dendl;
      return -EIO;
    }
  }
  dout(10) << __func__ << " kv_backend = " << kv_backend << dendl;

  bool do_bluefs;
  r = _is_bluefs(create, &do_bluefs);
  if (r < 0) {
    return r;
  }
  dout(10) << __func__ << " do_bluefs = " << do_bluefs << dendl;

  map<string,string> kv_options;
  // force separate wal dir for all new deployments.
  kv_options["separate_wal_dir"] = 1;
  rocksdb::Env *env = NULL;
  if (do_bluefs) {
    dout(10) << __func__ << " initializing bluefs" << dendl;
    if (kv_backend != "rocksdb") {
      derr << " backend must be rocksdb to use bluefs" << dendl;
      return -EINVAL;
    }

    r = _open_bluefs(create, read_only);
    if (r < 0) {
      return r;
    }

    if (cct->_conf->bluestore_bluefs_env_mirror) {
      rocksdb::Env* a = new BlueRocksEnv(bluefs);
      rocksdb::Env* b = rocksdb::Env::Default();
      if (create) {
        string cmd = "rm -rf " + path + "/db " +
          path + "/db.slow " +
          path + "/db.wal";
        int r = system(cmd.c_str());
        (void)r;
      }
      env = new rocksdb::EnvMirror(b, a, false, true);
    } else {
      env = new BlueRocksEnv(bluefs);

      // simplify the dir names, too, as "seen" by rocksdb
      fn = "db";
    }
    BlueFSVolumeSelector::paths paths;
    bluefs->get_vselector_paths(fn, paths);

    {
      ostringstream db_paths;
      bool first = true;
      for (auto& p : paths) {
        if (!first) {
          db_paths << " ";
        }
        first = false;
        db_paths << p.first << "," << p.second;

      }
      kv_options["db_paths"] = db_paths.str();
      dout(1) << __func__ << " set db_paths to " << db_paths.str() << dendl;
    }

    if (create) {
      for (auto& p : paths) {
        env->CreateDir(p.first);
      }
      // Selectors don't provide wal path so far hence create explicitly
      env->CreateDir(fn + ".wal");
    } else {
      std::vector<std::string> res;
      // check for dir presence
      auto r = env->GetChildren(fn+".wal", &res);
      if (r.IsNotFound()) {
	kv_options.erase("separate_wal_dir");
      }
    }
  } else {
    string walfn = path + "/db.wal";

    if (create) {
      int r = ::mkdir(fn.c_str(), 0755);
      if (r < 0)
	r = -errno;
      if (r < 0 && r != -EEXIST) {
	derr << __func__ << " failed to create " << fn << ": " << cpp_strerror(r)
	     << dendl;
	return r;
      }

      // wal_dir, too!
      r = ::mkdir(walfn.c_str(), 0755);
      if (r < 0)
	r = -errno;
      if (r < 0 && r != -EEXIST) {
	derr << __func__ << " failed to create " << walfn
	  << ": " << cpp_strerror(r)
	  << dendl;
	return r;
      }
    } else {
      struct stat st;
      r = ::stat(walfn.c_str(), &st);
      if (r < 0 && errno == ENOENT) {
	kv_options.erase("separate_wal_dir");
      }
    }
  }


  db = KeyValueDB::create(cct,
			  kv_backend,
			  fn,
			  kv_options,
			  static_cast<void*>(env));
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    if (bluefs) {
      _close_bluefs();
    }
    // delete env manually here since we can't depend on db to do this
    // under this case
    delete env;
    env = NULL;
    return -EIO;
  }

  FreelistManager::setup_merge_operators(db, freelist_type);
  db->set_merge_operator(PREFIX_STAT, merge_op);
  db->set_cache_size(cache_kv_ratio * cache_size);
  return 0;
}

int BlueStore::_open_db(bool create, bool to_repair_db, bool read_only)
{
  int r;
  ceph_assert(!(create && read_only));
  string options;
  string options_annex;
  stringstream err;
  string kv_dir_fn;
  string kv_backend;
  std::string sharding_def;
  // prevent write attempts to BlueFS in case we failed before BlueFS was opened
  db_was_opened_read_only = true;
  r = _prepare_db_environment(create, read_only, &kv_dir_fn, &kv_backend);
  if (r < 0) {
    derr << __func__ << " failed to prepare db environment: " << err.str() << dendl;
    return -EIO;
  }
  // if reached here then BlueFS is already opened
  db_was_opened_read_only = read_only;
  dout(10) << __func__ << "::db_was_opened_read_only was set to " << read_only << dendl;
  if (kv_backend == "rocksdb") {
    options = cct->_conf->bluestore_rocksdb_options;
    options_annex = cct->_conf->bluestore_rocksdb_options_annex;
    if (!options_annex.empty()) {
      if (!options.empty() &&
        *options.rbegin() != ',') {
        options += ',';
      }
      options += options_annex;
    }

    if (cct->_conf.get_val<bool>("bluestore_rocksdb_cf")) {
      sharding_def = cct->_conf.get_val<std::string>("bluestore_rocksdb_cfs");
    }
  }

  db->init(options);
  if (to_repair_db)
    return 0;
  if (create) {
    r = db->create_and_open(err, sharding_def);
  } else {
    // we pass in cf list here, but it is only used if the db already has
    // column families created.
    r = read_only ?
      db->open_read_only(err, sharding_def) :
      db->open(err, sharding_def);
  }
  if (r) {
    derr << __func__ << " erroring opening db: " << err.str() << dendl;
    _close_db();
    return -EIO;
  }
  dout(1) << __func__ << " opened " << kv_backend
	  << " path " << kv_dir_fn << " options " << options << dendl;
  return 0;
}

void BlueStore::_close_db()
{
  dout(10) << __func__ << ":read_only=" << db_was_opened_read_only
           << " fm=" << fm
           << " destage_alloc_file=" << need_to_destage_allocation_file
           << " per_pool=" << per_pool_stat_collection
           << " pool stats=" << osd_pools.size()
           << dendl;
  bool do_destage = !db_was_opened_read_only && need_to_destage_allocation_file;
  if (do_destage && is_statfs_recoverable()) {
    auto t = db->get_transaction();
    store_statfs_t s;
    if (per_pool_stat_collection) {
      KeyValueDB::Iterator it = db->get_iterator(PREFIX_STAT, KeyValueDB::ITERATOR_NOCACHE);
      uint64_t pool_id;
      for (it->upper_bound(string()); it->valid(); it->next()) {
        int r = get_key_pool_stat(it->key(), &pool_id);
        if (r >= 0) {
          dout(10) << __func__ << " wiping statfs for: " << pool_id << dendl;
        } else {
          derr << __func__ << " wiping invalid statfs key: " << it->key() << dendl;
        }
        t->rmkey(PREFIX_STAT, it->key());
      }

      std::lock_guard l(vstatfs_lock);
      for(auto &p : osd_pools) {
        string key;
        get_pool_stat_key(p.first, &key);
        bufferlist bl;
        if (!p.second.is_empty()) {
          p.second.encode(bl);
          p.second.publish(&s);
          t->set(PREFIX_STAT, key, bl);
          dout(10) << __func__ << " persisting: "
                   << p.first << "->"  << s
                   << dendl;
        }
      }
    } else {
      bufferlist bl;
      {
        std::lock_guard l(vstatfs_lock);
        vstatfs.encode(bl);
        vstatfs.publish(&s);
      }
      t->set(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, bl);
      dout(10) << __func__ << "persisting: " << s << dendl;
    }
    int r = db->submit_transaction_sync(t);
    dout(10) << __func__ << " statfs persisted." << dendl;
    ceph_assert(r >= 0);
  }
  ceph_assert(db);
  delete db;
  db = nullptr;

  if (do_destage && fm && fm->is_null_manager()) {
    int ret = store_allocator(alloc);
    if (ret != 0) {
      derr << __func__ << "::NCB::store_allocator() failed (continue with bitmapFreelistManager)" << dendl;
    }
  }

  if (bluefs) {
    _close_bluefs();
  }
}

void BlueStore::_dump_alloc_on_failure()
{
  auto dump_interval =
    cct->_conf->bluestore_bluefs_alloc_failure_dump_interval;
  if (dump_interval > 0 &&
    next_dump_on_bluefs_alloc_failure <= ceph_clock_now()) {
    shared_alloc.a->dump();
    next_dump_on_bluefs_alloc_failure = ceph_clock_now();
    next_dump_on_bluefs_alloc_failure += dump_interval;
  }
}

int BlueStore::_open_collections()
{
  if (!coll_map.empty()) {
    // could be opened from another path
    dout(20) << __func__ << "::NCB::collections are already opened, nothing to do" << dendl;
    return 0;
  }

  dout(10) << __func__ << dendl;
  collections_had_errors = false;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL);
  size_t load_cnt = 0;
  for (it->upper_bound(string());
       it->valid();
       it->next()) {
    coll_t cid;
    if (cid.parse(it->key())) {
      auto c = ceph::make_ref<Collection>(
	  this,
	  onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
          buffer_cache_shards[cid.hash_to_shard(buffer_cache_shards.size())],
	  cid);
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      try {
        decode(c->cnode, p);
      } catch (ceph::buffer::error& e) {
        derr << __func__ << " failed to decode cnode, key:"
             << pretty_binary_string(it->key()) << dendl;
        return -EIO;
      }   
      dout(20) << __func__ << " opened " << cid << " " << c
	       << " " << c->cnode << dendl;
      _osr_attach(c.get());
      coll_map[cid] = c;
      load_cnt++;
    } else {
      derr << __func__ << " unrecognized collection " << it->key() << dendl;
      collections_had_errors = true;
    }
  }
  dout(10) << __func__ << " collections loaded: " << load_cnt
           <<  dendl;
  return 0;
}

void BlueStore::_fsck_collections(int64_t* errors)
{
  if (collections_had_errors) {
    dout(10) << __func__ << dendl;
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL, KeyValueDB::ITERATOR_NOCACHE);
    for (it->upper_bound(string());
      it->valid();
      it->next()) {
      coll_t cid;
      if (!cid.parse(it->key())) {
        derr << __func__ << " unrecognized collection " << it->key() << dendl;
        if (errors) {
          (*errors)++;
        }
      }
    }
  }
}

void BlueStore::_set_per_pool_omap()
{
  per_pool_omap = OMAP_BULK;
  bufferlist bl;
  db->get(PREFIX_SUPER, "per_pool_omap", &bl);
  if (bl.length()) {
    auto s = bl.to_str();
    if (s == stringify(OMAP_PER_POOL)) {
      per_pool_omap = OMAP_PER_POOL;
    } else if (s == stringify(OMAP_PER_PG)) {
      per_pool_omap = OMAP_PER_PG;
    } else {
      ceph_assert(s == stringify(OMAP_BULK));
    }
    dout(10) << __func__ << " per_pool_omap = " << per_pool_omap << dendl;
  } else {
    dout(10) << __func__ << " per_pool_omap not present" << dendl;
  }
  _check_no_per_pg_or_pool_omap_alert();
}

void BlueStore::_open_statfs()
{
  osd_pools.clear();
  vstatfs.reset();

  bufferlist bl;
  int r = db->get(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, &bl);
  if (r >= 0) {
    per_pool_stat_collection = false;
    if (size_t(bl.length()) >= sizeof(vstatfs.values)) {
      auto it = bl.cbegin();
      vstatfs.decode(it);
      dout(10) << __func__ << " store_statfs is found" << dendl;
    } else {
      dout(10) << __func__ << " store_statfs is corrupt, using empty" << dendl;
    }
    _check_legacy_statfs_alert();
  } else {
    per_pool_stat_collection = true;
    dout(10) << __func__ << " per-pool statfs is enabled" << dendl;
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_STAT, KeyValueDB::ITERATOR_NOCACHE);
    for (it->upper_bound(string());
	 it->valid();
	 it->next()) {

      uint64_t pool_id;
      int r = get_key_pool_stat(it->key(), &pool_id);
      ceph_assert(r == 0);

      bufferlist bl;
      bl = it->value();
      auto p = bl.cbegin();
      auto& st = osd_pools[pool_id];
      try {
        st.decode(p);
        vstatfs += st;

        dout(10) << __func__ << " pool " << std::hex << pool_id
		 << " statfs(hex) " << st
		 << std::dec << dendl;
      } catch (ceph::buffer::error& e) {
        derr << __func__ << " failed to decode pool stats, key:"
             << pretty_binary_string(it->key()) << dendl;
      }   
    }
  }
  dout(10) << __func__ << " statfs " << std::hex
           << vstatfs  << std::dec << dendl;

}

int BlueStore::_setup_block_symlink_or_file(
  string name,
  string epath,
  uint64_t size,
  bool create)
{
  dout(20) << __func__ << " name " << name << " path " << epath
	   << " size " << size << " create=" << (int)create << dendl;
  int r = 0;
  int flags = O_RDWR|O_CLOEXEC;
  if (create)
    flags |= O_CREAT;
  if (epath.length()) {
    r = ::symlinkat(epath.c_str(), path_fd, name.c_str());
    if (r < 0) {
      r = -errno;
      derr << __func__ << " failed to create " << name << " symlink to "
           << epath << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    if (!epath.compare(0, strlen(SPDK_PREFIX), SPDK_PREFIX)) {
      int fd = ::openat(path_fd, epath.c_str(), flags, 0644);
      if (fd < 0) {
	r = -errno;
	derr << __func__ << " failed to open " << epath << " file: "
	     << cpp_strerror(r) << dendl;
	return r;
      }
      // write the Transport ID of the NVMe device
      // a transport id for PCIe looks like: "trtype:PCIe traddr:0000:02:00.0"
      // where "0000:02:00.0" is the selector of a PCI device, see
      // the first column of "lspci -mm -n -D"
      // a transport id for tcp looks like: "trype:TCP adrfam:IPv4 traddr:172.31.89.152 trsvcid:4420"
      string trid = epath.substr(strlen(SPDK_PREFIX));
      r = ::write(fd, trid.c_str(), trid.size());
      ceph_assert(r == static_cast<int>(trid.size()));
      dout(1) << __func__ << " created " << name << " symlink to "
              << epath << dendl;
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
  }
  if (size) {
    int fd = ::openat(path_fd, name.c_str(), flags, 0644);
    if (fd >= 0) {
      // block file is present
      struct stat st;
      int r = ::fstat(fd, &st);
      if (r == 0 &&
	  S_ISREG(st.st_mode) &&   // if it is a regular file
	  st.st_size == 0) {       // and is 0 bytes
	r = ::ftruncate(fd, size);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " failed to resize " << name << " file to "
	       << size << ": " << cpp_strerror(r) << dendl;
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	  return r;
	}

	if (cct->_conf->bluestore_block_preallocate_file) {
          r = ::ceph_posix_fallocate(fd, 0, size);
          if (r > 0) {
	    derr << __func__ << " failed to prefallocate " << name << " file to "
	      << size << ": " << cpp_strerror(r) << dendl;
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	    return -r;
	  }
	}
	dout(1) << __func__ << " resized " << name << " file to "
		<< byte_u_t(size) << dendl;
      }
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    } else {
      int r = -errno;
      if (r != -ENOENT) {
	derr << __func__ << " failed to open " << name << " file: "
	     << cpp_strerror(r) << dendl;
	return r;
      }
    }
  }
  return 0;
}

int BlueStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;
  uint64_t reserved;
  if (cct->_conf->osd_max_object_size > OBJECT_MAX_SIZE) {
    derr << __func__ << " osd_max_object_size "
	 << cct->_conf->osd_max_object_size << " > bluestore max "
	 << OBJECT_MAX_SIZE << dendl;
    return -EINVAL;
  }

  {
    string done;
    r = read_meta("mkfs_done", &done);
    if (r == 0) {
      dout(1) << __func__ << " already created" << dendl;
      if (cct->_conf->bluestore_fsck_on_mkfs) {
        r = fsck(cct->_conf->bluestore_fsck_on_mkfs_deep);
        if (r < 0) {
          derr << __func__ << " fsck found fatal error: " << cpp_strerror(r)
               << dendl;
          return r;
        }
        if (r > 0) {
          derr << __func__ << " fsck found " << r << " errors" << dendl;
          r = -EIO;
        }
      }
      return r; // idempotent
    }
  }

  {
    string type;
    r = read_meta("type", &type);
    if (r == 0) {
      if (type != "bluestore") {
	derr << __func__ << " expected bluestore, but type is " << type << dendl;
	return -EIO;
      }
    } else {
      r = write_meta("type", "bluestore");
      if (r < 0)
        return r;
    }
  }

  r = _open_path();
  if (r < 0)
    return r;

  r = _open_fsid(true);
  if (r < 0)
    goto out_path_fd;

  r = _lock_fsid();
  if (r < 0)
    goto out_close_fsid;

  r = _read_fsid(&old_fsid);
  if (r < 0 || old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    // we'll write it later.
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
	   << " != provided " << fsid << dendl;
      r = -EINVAL;
      goto out_close_fsid;
    }
    fsid = old_fsid;
  }

  r = _setup_block_symlink_or_file("block", cct->_conf->bluestore_block_path,
				   cct->_conf->bluestore_block_size,
				   cct->_conf->bluestore_block_create);
  if (r < 0)
    goto out_close_fsid;
  if (cct->_conf->bluestore_bluefs) {
    r = _setup_block_symlink_or_file("block.wal", cct->_conf->bluestore_block_wal_path,
	cct->_conf->bluestore_block_wal_size,
	cct->_conf->bluestore_block_wal_create);
    if (r < 0)
      goto out_close_fsid;
    r = _setup_block_symlink_or_file("block.db", cct->_conf->bluestore_block_db_path,
	cct->_conf->bluestore_block_db_size,
	cct->_conf->bluestore_block_db_create);
    if (r < 0)
      goto out_close_fsid;
  }

  r = _open_bdev(true);
  if (r < 0)
    goto out_close_fsid;

  freelist_type = "bitmap";
  dout(10) << " freelist_type " << freelist_type << dendl;

  // choose min_alloc_size
  dout(5) << __func__ << " optimal_io_size 0x" << std::hex << optimal_io_size
	  << " block_size: 0x" << block_size << std::dec << dendl;
  if ((cct->_conf->bluestore_use_optimal_io_size_for_min_alloc_size) && (optimal_io_size != 0)) {
    dout(5) << __func__ << " optimal_io_size 0x" << std::hex << optimal_io_size
		<< " for min_alloc_size 0x" << min_alloc_size << std::dec << dendl;
    min_alloc_size = optimal_io_size;
  }
  else if (cct->_conf->bluestore_min_alloc_size) {
    min_alloc_size = cct->_conf->bluestore_min_alloc_size;
  } else {
    ceph_assert(bdev);
    if (_use_rotational_settings()) {
      min_alloc_size = cct->_conf->bluestore_min_alloc_size_hdd;
    } else {
      min_alloc_size = cct->_conf->bluestore_min_alloc_size_ssd;
    }
  }
  _validate_bdev();

  // make sure min_alloc_size is power of 2 aligned.
  if (!std::has_single_bit(min_alloc_size)) {
    derr << __func__ << " min_alloc_size 0x"
	 << std::hex << min_alloc_size << std::dec
	 << " is not power of 2 aligned!"
	 << dendl;
    r = -EINVAL;
    goto out_close_bdev;
  }

  // make sure min_alloc_size is >= and aligned with block size
  if (min_alloc_size % block_size != 0) {
    derr << __func__ << " min_alloc_size 0x"
	 << std::hex << min_alloc_size
	 << " is less or not aligned with block_size: 0x"
	 << block_size << std::dec <<  dendl;
    r = -EINVAL;
    goto out_close_bdev;
  }

  r = _create_alloc();
  if (r < 0) {
    goto out_close_bdev;
  }

  reserved = _get_ondisk_reserved();
  alloc->init_add_free(reserved,
    p2align(bdev->get_size(), min_alloc_size) - reserved);

  r = _open_db(true);
  if (r < 0)
    goto out_close_alloc;

  {
    KeyValueDB::Transaction t = db->get_transaction();
    r = _open_fm(t, false, true);
    if (r < 0)
      goto out_close_db;
    {
      bufferlist bl;
      encode((uint64_t)0, bl);
      t->set(PREFIX_SUPER, "nid_max", bl);
      t->set(PREFIX_SUPER, "blobid_max", bl);
    }

    {
      bufferlist bl;
      encode((uint64_t)min_alloc_size, bl);
      t->set(PREFIX_SUPER, "min_alloc_size", bl);
    }
    {
      bufferlist bl;
      if (cct->_conf.get_val<bool>("bluestore_debug_legacy_omap")) {
	bl.append(stringify(OMAP_BULK));
      } else {
	bl.append(stringify(OMAP_PER_PG));
      }
      t->set(PREFIX_SUPER, "per_pool_omap", bl);
    }

    ondisk_format = latest_ondisk_format;
    _prepare_ondisk_format_super(t);
    db->submit_transaction_sync(t);
  }

  r = write_meta("kv_backend", cct->_conf->bluestore_kvbackend);
  if (r < 0)
    goto out_close_fm;

  r = write_meta("bluefs", stringify(bluefs ? 1 : 0));
  if (r < 0)
    goto out_close_fm;

  r = write_meta("elastic_shared_blobs",
		 cct->_conf.get_val<bool>("bluestore_elastic_shared_blobs") ? "1" : "0");
  if (r < 0)
    goto out_close_fm;

  if (fsid != old_fsid) {
    r = _write_fsid();
    if (r < 0) {
      derr << __func__ << " error writing fsid: " << cpp_strerror(r) << dendl;
      goto out_close_fm;
    }
  }

 out_close_fm:
  _close_fm();
 out_close_db:
  _close_db();
 out_close_alloc:
  _close_alloc();
 out_close_bdev:
  _close_bdev();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();

  if (r == 0 &&
      cct->_conf->bluestore_fsck_on_mkfs) {
    int rc = fsck(cct->_conf->bluestore_fsck_on_mkfs_deep);
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      r = -EIO;
    }
  }

  if (r == 0) {
    // indicate success by writing the 'mkfs_done' file
    r = write_meta("mkfs_done", "yes");
  }

  if (r < 0) {
    derr << __func__ << " failed, " << cpp_strerror(r) << dendl;
  } else {
    dout(0) << __func__ << " success" << dendl;
  }
  return r;
}

int BlueStore::add_new_bluefs_device(int id, const string& dev_path)
{
  dout(10) << __func__ << " path " << dev_path << " id:" << id << dendl;
  int r;
  ceph_assert(path_fd < 0);

  ceph_assert(id == BlueFS::BDEV_NEWWAL || id == BlueFS::BDEV_NEWDB);

  if (!cct->_conf->bluestore_bluefs) {
    derr << __func__ << " bluefs isn't configured, can't add new device " << dendl;
    return -EIO;
  }
  dout(5) << __func__ << "::NCB::calling open_db_and_around(read-only)" << dendl;
  r = _open_db_and_around(true);
  if (r < 0) {
    return r;
  }

  if (id == BlueFS::BDEV_NEWWAL) {
    string p = path + "/block.wal";
    r = _setup_block_symlink_or_file("block.wal", dev_path,
	cct->_conf->bluestore_block_wal_size,
	true);
    ceph_assert(r == 0);

    r = bluefs->add_block_device(BlueFS::BDEV_NEWWAL, p,
				 cct->_conf->bdev_enable_discard);
    ceph_assert(r == 0);

    if (bluefs->bdev_support_label(BlueFS::BDEV_NEWWAL)) {
      r = _check_or_set_bdev_label(
	p,
	bluefs->get_block_device_size(BlueFS::BDEV_NEWWAL),
        "bluefs wal",
	true);
      ceph_assert(r == 0);
    }

    bluefs_layout.dedicated_wal = true;
  } else if (id == BlueFS::BDEV_NEWDB) {
    string p = path + "/block.db";
    r = _setup_block_symlink_or_file("block.db", dev_path,
	cct->_conf->bluestore_block_db_size,
	true);
    ceph_assert(r == 0);

    r = bluefs->add_block_device(BlueFS::BDEV_NEWDB, p,
				 cct->_conf->bdev_enable_discard);
    ceph_assert(r == 0);

    if (bluefs->bdev_support_label(BlueFS::BDEV_NEWDB)) {
      r = _check_or_set_bdev_label(
	p,
	bluefs->get_block_device_size(BlueFS::BDEV_NEWDB),
        "bluefs db",
	true);
      ceph_assert(r == 0);
    }
    bluefs_layout.shared_bdev = BlueFS::BDEV_SLOW;
    bluefs_layout.dedicated_db = true;
  }
  bluefs->umount();
  bluefs->mount();

  r = bluefs->prepare_new_device(id, bluefs_layout);
  ceph_assert(r == 0);

  if (r < 0) {
    derr << __func__ << " failed, " << cpp_strerror(r) << dendl;
  } else {
    dout(0) << __func__ << " success" << dendl;
  }

  _close_db_and_around();
  return r;
}

int BlueStore::migrate_to_existing_bluefs_device(const set<int>& devs_source,
  int id)
{
  dout(10) << __func__ << " id:" << id << dendl;
  ceph_assert(path_fd < 0);

  ceph_assert(id == BlueFS::BDEV_SLOW || id == BlueFS::BDEV_DB);

  if (!cct->_conf->bluestore_bluefs) {
    derr << __func__ << " bluefs isn't configured, can't add new device " << dendl;
    return -EIO;
  }

  int r = _open_db_and_around(true);
  if (r < 0) {
    return r;
  }
  auto close_db = make_scope_guard([&] {
    _close_db_and_around();
  });
  uint64_t used_space = 0;
  for(auto src_id : devs_source) {
    used_space += bluefs->get_used(src_id);
  }
  uint64_t target_free = bluefs->get_free(id);
  if (target_free < used_space) {
    derr << __func__
         << " can't migrate, free space at target: " << target_free
	 << " is less than required space: " << used_space
	 << dendl;
    return -ENOSPC;
  }
  if (devs_source.count(BlueFS::BDEV_DB)) {
    bluefs_layout.shared_bdev = BlueFS::BDEV_DB;
    bluefs_layout.dedicated_db = false;
  }
  if (devs_source.count(BlueFS::BDEV_WAL)) {
    bluefs_layout.dedicated_wal = false;
  }
  r = bluefs->device_migrate_to_existing(cct, devs_source, id, bluefs_layout);
  if (r < 0) {
    derr << __func__ << " failed during BlueFS migration, " << cpp_strerror(r) << dendl;
    return r;
  }

  if (devs_source.count(BlueFS::BDEV_DB)) {
    r = unlink(string(path + "/block.db").c_str());
    ceph_assert(r == 0);
  }
  if (devs_source.count(BlueFS::BDEV_WAL)) {
    r = unlink(string(path + "/block.wal").c_str());
    ceph_assert(r == 0);
  }
  return r;
}

int BlueStore::migrate_to_new_bluefs_device(const set<int>& devs_source,
  int id,
  const string& dev_path)
{
  dout(10) << __func__ << " path " << dev_path << " id:" << id << dendl;
  ceph_assert(path_fd < 0);

  ceph_assert(id == BlueFS::BDEV_NEWWAL || id == BlueFS::BDEV_NEWDB);

  if (!cct->_conf->bluestore_bluefs) {
    derr << __func__ << " bluefs isn't configured, can't add new device " << dendl;
    return -EIO;
  }

  int r = _open_db_and_around(true);
  if (r < 0) {
    return r;
  }
  auto close_db = make_scope_guard([&] {
    _close_db_and_around();
  });

  string link_db;
  string link_wal;
  if (devs_source.count(BlueFS::BDEV_DB) &&
      bluefs_layout.shared_bdev != BlueFS::BDEV_DB) {
    link_db = path + "/block.db";
    bluefs_layout.shared_bdev = BlueFS::BDEV_DB;
    bluefs_layout.dedicated_db = false;
  }
  if (devs_source.count(BlueFS::BDEV_WAL)) {
    link_wal = path + "/block.wal";
    bluefs_layout.dedicated_wal = false;
  }

  size_t target_size = 0;
  string target_name;
  if (id == BlueFS::BDEV_NEWWAL) {
    target_name = "block.wal";
    target_size = cct->_conf->bluestore_block_wal_size;
    bluefs_layout.dedicated_wal = true;

    r = bluefs->add_block_device(BlueFS::BDEV_NEWWAL, dev_path,
				 cct->_conf->bdev_enable_discard);
    ceph_assert(r == 0);

    if (bluefs->bdev_support_label(BlueFS::BDEV_NEWWAL)) {
      r = _check_or_set_bdev_label(
	dev_path,
	bluefs->get_block_device_size(BlueFS::BDEV_NEWWAL),
        "bluefs wal",
	true);
      ceph_assert(r == 0);
    }
  } else if (id == BlueFS::BDEV_NEWDB) {
    target_name = "block.db";
    target_size = cct->_conf->bluestore_block_db_size;
    bluefs_layout.shared_bdev = BlueFS::BDEV_SLOW;
    bluefs_layout.dedicated_db = true;

    r = bluefs->add_block_device(BlueFS::BDEV_NEWDB, dev_path,
				 cct->_conf->bdev_enable_discard);
    ceph_assert(r == 0);

    if (bluefs->bdev_support_label(BlueFS::BDEV_NEWDB)) {
      r = _check_or_set_bdev_label(
	dev_path,
	bluefs->get_block_device_size(BlueFS::BDEV_NEWDB),
        "bluefs db",
	true);
      ceph_assert(r == 0);
    }
  }

  bluefs->umount();
  bluefs->mount();

  r = bluefs->device_migrate_to_new(cct, devs_source, id, bluefs_layout);

  if (r < 0) {
    derr << __func__ << " failed during BlueFS migration, " << cpp_strerror(r) << dendl;
    return r;
  }

  if (!link_db.empty()) {
    r = unlink(link_db.c_str());
    ceph_assert(r == 0);
  }
  if (!link_wal.empty()) {
    r = unlink(link_wal.c_str());
    ceph_assert(r == 0);
  }
  r = _setup_block_symlink_or_file(
    target_name,
    dev_path,
    target_size,
    true);
  ceph_assert(r == 0);
  dout(0) << __func__ << " success" << dendl;

  return r;
}

string BlueStore::get_device_path(unsigned id)
{
  string res;
  if (id < BlueFS::MAX_BDEV) {
    switch (id) {
    case BlueFS::BDEV_WAL:
      res = path + "/block.wal";
      break;
    case BlueFS::BDEV_DB:
      if (id == bluefs_layout.shared_bdev) {
	res = path + "/block";
      } else {
	res = path + "/block.db";
      }
      break;
    case BlueFS::BDEV_SLOW:
      res = path + "/block";
      break;
    }
  }
  return res;
}

int BlueStore::_set_bdev_label_size(const string& path, uint64_t size)
{
  bluestore_bdev_label_t label;
  int r = _read_bdev_label(cct, path, &label);
  if (r < 0) {
    derr << "unable to read label for " << path << ": "
          << cpp_strerror(r) << dendl;
  } else {
    label.size = size;
    r = _write_bdev_label(cct, path, label);
    if (r < 0) {
      derr << "unable to write label for " << path << ": "
            << cpp_strerror(r) << dendl;
    }
  }
  return r;
}

int BlueStore::expand_devices(ostream& out)
{
  int r = _open_db_and_around(true);
  ceph_assert(r == 0);
  bluefs->dump_block_extents(out);
  out << "Expanding DB/WAL..." << std::endl;
  for (auto devid : { BlueFS::BDEV_WAL, BlueFS::BDEV_DB}) {
    if (devid == bluefs_layout.shared_bdev ) {
      continue;
    }
    uint64_t size = bluefs->get_block_device_size(devid);
    if (size == 0) {
      // no bdev
      continue;
    }

    out << devid
	<<" : expanding " << " to 0x" << size << std::dec << std::endl;
    string p = get_device_path(devid);
    const char* path = p.c_str();
    if (path == nullptr) {
      derr << devid
	    <<": can't find device path " << dendl;
      continue;
    }
    if (bluefs->bdev_support_label(devid)) {
      if (_set_bdev_label_size(p, size) >= 0) {
        out << devid
          << " : size label updated to " << size
          << std::endl;
      }
    }
  }
  uint64_t size0 = fm->get_size();
  uint64_t size = bdev->get_size();
  if (size0 < size) {
    out << bluefs_layout.shared_bdev
      << " : expanding " << " from 0x" << std::hex
      << size0 << " to 0x" << size << std::dec << std::endl;
    _write_out_fm_meta(size);
    if (bdev->supported_bdev_label()) {
      if (_set_bdev_label_size(path, size) >= 0) {
        out << bluefs_layout.shared_bdev
          << " : size label updated to " << size
          << std::endl;
      }
    }
    _close_db_and_around();

    // mount in read/write to sync expansion changes
    r = _mount();
    ceph_assert(r == 0);
    if (fm && fm->is_null_manager()) {
      // we grow the allocation range, must reflect it in the allocation file
      alloc->init_add_free(size0, size - size0);
      need_to_destage_allocation_file = true;
    }
    umount();
  } else {
    _close_db_and_around();
  }
  return r;
}

int BlueStore::dump_bluefs_sizes(ostream& out)
{
  int r = _open_db_and_around(true);
  ceph_assert(r == 0);
  bluefs->dump_block_extents(out);
  _close_db_and_around();
  return r;
}

void BlueStore::set_cache_shards(unsigned num)
{
  dout(10) << __func__ << " " << num << dendl;
  size_t oold = onode_cache_shards.size();
  size_t bold = buffer_cache_shards.size();
  ceph_assert(num >= oold && num >= bold);
  onode_cache_shards.resize(num);
  buffer_cache_shards.resize(num);
  for (unsigned i = oold; i < num; ++i) {
    onode_cache_shards[i] = 
        OnodeCacheShard::create(cct, cct->_conf->bluestore_cache_type,
                                 logger);
  }
  for (unsigned i = bold; i < num; ++i) {
    buffer_cache_shards[i] = 
        BufferCacheShard::create(cct, cct->_conf->bluestore_cache_type,
                                 logger);
  }
}

//---------------------------------------------
bool BlueStore::has_null_manager() const
{
  return (fm && fm->is_null_manager());
}

int BlueStore::_mount()
{
  dout(5) << __func__ << " path " << path << dendl;

  {
    int r = read_meta_conf_check_env();
    if (r < 0) {
      return r;
    }
  }

  _kv_only = false;
  if (cct->_conf->bluestore_fsck_on_mount) {
    int rc = fsck(cct->_conf->bluestore_fsck_on_mount_deep);
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      return -EIO;
    }
  }

  if (cct->_conf->osd_max_object_size > OBJECT_MAX_SIZE) {
    derr << __func__ << " osd_max_object_size "
	 << cct->_conf->osd_max_object_size << " > bluestore max "
	 << OBJECT_MAX_SIZE << dendl;
    return -EINVAL;
  }

  dout(5) << __func__ << "::NCB::calling open_db_and_around(read/write)" << dendl;
  int r = _open_db_and_around(false);
  if (r < 0) {
    return r;
  }
  auto close_db = make_scope_guard([&] {
    if (!mounted) {
      _close_db_and_around();
    }
  });

  r = _upgrade_super();
  if (r < 0) {
    return r;
  }

  // The recovery process for allocation-map needs to open collection early
  r = _open_collections();
  if (r < 0) {
    return r;
  }
  auto shutdown_cache = make_scope_guard([&] {
    if (!mounted) {
      _shutdown_cache();
    }
  });

  r = _reload_logger();
  if (r < 0) {
    return r;
  }

  _kv_start();
  auto stop_kv = make_scope_guard([&] {
    if (!mounted) {
      _kv_stop();
    }
  });

  r = _deferred_replay();
  if (r < 0) {
    return r;
  }

  mempool_thread.init();

  if ((!per_pool_stat_collection || per_pool_omap != OMAP_PER_PG) &&
    cct->_conf->bluestore_fsck_quick_fix_on_mount == true) {

    auto was_per_pool_omap = per_pool_omap;

    dout(1) << __func__ << " quick-fix on mount" << dendl;
    _fsck_on_open(FSCK_SHALLOW, true);

    //set again as hopefully it has been fixed
    if (was_per_pool_omap != OMAP_PER_PG) {
      _set_per_pool_omap();
    }
  }

  mounted = true;
  return 0;
}

int BlueStore::umount()
{
  dout(5) << __func__ << dendl;
  ceph_assert(_kv_only || mounted);
  _osr_drain_all();

  mounted = false;

  ceph_assert(alloc);

  if (!_kv_only) {
    mempool_thread.shutdown();
    dout(20) << __func__ << " stopping kv thread" << dendl;
    _kv_stop();
    // skip cache cleanup step on fast shutdown
    if (likely(!m_fast_shutdown)) {
      _shutdown_cache();
    }
    dout(20) << __func__ << " closing" << dendl;
  }
  _close_db_and_around();
  // disable fsck on fast-shutdown
  if (cct->_conf->bluestore_fsck_on_umount && !m_fast_shutdown) {
    int rc = fsck(cct->_conf->bluestore_fsck_on_umount_deep);
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      return -EIO;
    }
  }
  return 0;
}

int BlueStore::cold_open()
{
  return _open_db_and_around(true);
}

int BlueStore::cold_close()
{
  _close_db_and_around();
  return 0;
}

// derr wrapper to limit enormous output and avoid log flooding.
// Of limited use where such output is expected for now
#define fsck_derr(err_cnt, threshold) \
  if (err_cnt <= threshold) {         \
    bool need_skip_print = err_cnt == threshold; \
    derr

#define fsck_dendl \
    dendl;          \
    if (need_skip_print) \
      derr << "more error lines skipped..." << dendl; \
  }

int _fsck_sum_extents(
  const PExtentVector& extents,
  bool compressed,
  store_statfs_t& expected_statfs)
{
  for (auto e : extents) {
    if (!e.is_valid())
      continue;
    expected_statfs.allocated += e.length;
    if (compressed) {
      expected_statfs.data_compressed_allocated += e.length;
    }
  }
  return 0;
}

int BlueStore::_fsck_check_extents(
  std::string_view ctx_descr,
  const PExtentVector& extents,
  bool compressed,
  mempool_dynamic_bitset &used_blocks,
  uint64_t granularity,
  BlueStoreRepairer* repairer,
  store_statfs_t& expected_statfs,
  FSCKDepth depth)
{
  dout(30) << __func__ << " " << ctx_descr << ", extents " << extents << dendl;
  int errors = 0;
  for (auto e : extents) {
    if (!e.is_valid())
      continue;
    expected_statfs.allocated += e.length;
    if (compressed) {
      expected_statfs.data_compressed_allocated += e.length;
    }
    if (depth != FSCK_SHALLOW) {
      bool already = false;
      apply_for_bitset_range(
        e.offset, e.length, granularity, used_blocks,
        [&](uint64_t pos, mempool_dynamic_bitset &bs) {
	  if (bs.test(pos)) {
	    if (repairer) {
	      repairer->note_misreference(
	        pos * min_alloc_size, min_alloc_size, !already);
	    }
            if (!already) {
              derr << __func__ << "::fsck error: " << ctx_descr << ", extent " << e
		   << " or a subset is already allocated (misreferenced)" << dendl;
	      ++errors;
	      already = true;
	    }
	  }
	  else
	    bs.set(pos);
        });

      if (e.end() > bdev->get_size()) {
        derr << "fsck error:  " << ctx_descr << ", extent " << e
	     << " past end of block device" << dendl;
        ++errors;
      }
    }
  }
  return errors;
}

void BlueStore::_fsck_check_statfs(
  const store_statfs_t& expected_statfs,
  const per_pool_statfs& expected_pool_statfs,
  int64_t& errors,
  int64_t& warnings,
  BlueStoreRepairer* repairer)
{
  string key;
  store_statfs_t actual_statfs;
  store_statfs_t s;
  {
    // make a copy
    per_pool_statfs my_expected_pool_statfs(expected_pool_statfs);
    auto op = osd_pools.begin();
    while (op != osd_pools.end()) {
      get_pool_stat_key(op->first, &key);
      op->second.publish(&s);
      auto it_expected = my_expected_pool_statfs.find(op->first);
      if (it_expected == my_expected_pool_statfs.end()) {
        auto op0 = op++;
        if (op0->second.is_empty()) {
          // It's OK to lack relevant empty statfs record
          continue;
        }
        derr << __func__ << "::fsck error: " << std::hex
             << "pool " << op0->first << " has got no statfs to match against: "
             << s
             << std::dec << dendl;
        ++errors;
        if (repairer) {
          osd_pools.erase(op0);
          repairer->remove_key(db, PREFIX_STAT, key);
        }
      } else {
        if (!(s == it_expected->second)) {
          derr << "fsck error: actual " << s
	       << " != expected " << it_expected->second
	       << " for pool "
	       << std::hex << op->first << std::dec << dendl;
	  ++errors;
	  if (repairer) {
	    // repair in-memory in a hope this would be flushed properly on shutdown
	    s = it_expected->second;
	    op->second = it_expected->second;
	    repairer->fix_statfs(db, key, it_expected->second);
	  }
	}
        actual_statfs.add(s);
        my_expected_pool_statfs.erase(it_expected);
        ++op;
      }
    }
    // check stats that lack matching entities in osd_pools
    for (auto &p : my_expected_pool_statfs) {
      if (p.second.is_zero()) {
        // It's OK to lack relevant empty statfs record
        continue;
      }
      get_pool_stat_key(p.first, &key);
      derr << __func__ << "::fsck error: " << std::hex
           << "pool " << p.first << " has got no actual statfs: "
           << std::dec << p.second
           << dendl;
      ++errors;
      if (repairer) {
	osd_pools[p.first] = p.second;
        repairer->fix_statfs(db, key, p.second);
        actual_statfs.add(p.second);
      }
    }
  }
  // process global statfs
  if (repairer) {
    if (!per_pool_stat_collection) {
      // by virtue of running this method, we correct the top-level
      // error of having global stats
      repairer->remove_key(db, PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY);
      per_pool_stat_collection = true;
    }
    vstatfs = actual_statfs;
    dout(20) << __func__ << " setting vstatfs to " << actual_statfs << dendl;
  } else if (!per_pool_stat_collection) {
    // check global stats only if fscking (not repairing) w/o per-pool stats
    vstatfs.publish(&s);
    if (!(s == expected_statfs)) {
      derr << "fsck error: actual " << s
           << " != expected " << expected_statfs << dendl;
      ++errors;
    }
  }
}

void BlueStore::_fsck_foreach_shared_blob(
  std::function< bool (coll_t, ghobject_t, uint64_t, const bluestore_blob_t&)> cb) {
  auto it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
  if (it) {
    CollectionRef c;
    spg_t pgid;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      dout(30) << __func__ << " key "
	       << pretty_binary_string(it->key())
	       << dendl;
      if (is_extent_shard_key(it->key())) {
	continue;
      }

      ghobject_t oid;
      int r = get_key_object(it->key(), &oid);
      if (r < 0) {
	continue;
      }

      if (!c ||
	  oid.shard_id != pgid.shard ||
	  oid.hobj.get_logical_pool() != (int64_t)pgid.pool() ||
	  !c->contains(oid)) {
	c = nullptr;
	for (auto& p : coll_map) {
	  if (p.second->contains(oid)) {
	    c = p.second;
	    break;
	  }
	}
	if (!c) {
	  continue;
	}
      }
      dout(20) << __func__
	       << " inspecting shared blob refs for col:" << c->cid
	       << " obj:" << oid
	       << dendl;

      OnodeRef o;
      o.reset(Onode::create_decode(c, oid, it->key(), it->value()));
      o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);

      _dump_onode<30>(cct, *o);

      mempool::bluestore_fsck::set<BlobRef> passed_sbs;
      for (auto& e : o->extent_map.extent_map) {
	auto& b = e.blob->get_blob();
	if (b.is_shared() && passed_sbs.count(e.blob) == 0) {
	  auto sbid = e.blob->get_sbid();
	  if (cb(c->cid, oid, sbid, b) == false) {
	    goto stop_iterating;
	  }
	  passed_sbs.emplace(e.blob);
	}
      } // for ... extent_map
    } // for ... it->valid
  } //if (it(PREFIX_OBJ))
 stop_iterating:;
}

void BlueStore::_fsck_repair_shared_blobs(
  BlueStoreRepairer& repairer,
  shared_blob_2hash_tracker_t& sb_ref_counts,
  sb_info_space_efficient_map_t& sb_info)
{
  auto sb_ref_mismatches = sb_ref_counts.count_non_zero();
  dout(1) << __func__ << " repairing shared_blobs, ref mismatch estimate: "
	  << sb_ref_mismatches << dendl;
  if (!sb_ref_mismatches) // not expected to succeed, just in case
    return;

  mempool::bluestore_fsck::map<uint64_t, bluestore_extent_ref_map_t> refs_map;

  // first iteration over objects to identify all the broken sbids
  _fsck_foreach_shared_blob( [&](coll_t cid,
                           ghobject_t oid,
                           uint64_t sbid,
                           const bluestore_blob_t& b) {
    auto it = refs_map.lower_bound(sbid);
    if(it != refs_map.end() && it->first == sbid) {
      return true;
    }
    for (auto& p : b.get_extents()) {
      if (p.is_valid() &&
	  !sb_ref_counts.test_all_zero_range(sbid,
					     p.offset,
					     p.length)) {
	refs_map.emplace_hint(it, sbid, bluestore_extent_ref_map_t());
        dout(20) << __func__
                 << " broken shared blob found for col:" << cid
	         << " obj:" << oid
	         << " sbid 0x" << std::hex << sbid << std::dec
	         << dendl;
	break;
      }
    }
    return true;
  });

  // second iteration over objects to build new ref map for the broken sbids
  _fsck_foreach_shared_blob( [&](coll_t cid,
                           ghobject_t oid,
                           uint64_t sbid,
                           const bluestore_blob_t& b) {
    auto it = refs_map.find(sbid);
    if(it == refs_map.end()) {
      return true;
    }
    for (auto& p : b.get_extents()) {
      if (p.is_valid()) {
	it->second.get(p.offset, p.length);
	break;
      }
    }
    return true;
  });

  // update shared blob records
  auto ref_it = refs_map.begin();
  while (ref_it != refs_map.end()) {
    size_t cnt = 0;
    const size_t max_transactions = 4096;
    KeyValueDB::Transaction txn = db->get_transaction();
    for (cnt = 0;
      cnt < max_transactions && ref_it != refs_map.end();
      ref_it++) {
      auto sbid = ref_it->first;
      dout(20) << __func__ << " repaired shared_blob 0x"
	<< std::hex << sbid << std::dec
	<< ref_it->second << dendl;
      repairer.fix_shared_blob(txn, sbid, &ref_it->second, 0);
      cnt++;
    }
    if (cnt) {
      db->submit_transaction_sync(txn);
      cnt = 0;
    }
  }
  // remove stray shared blob records
  size_t cnt = 0;
  const size_t max_transactions = 4096;
  KeyValueDB::Transaction txn = db->get_transaction();
  sb_info.foreach_stray([&](const sb_info_t& sbi) {
    auto sbid = sbi.get_sbid();
    dout(20) << __func__ << " removing stray shared_blob 0x"
      << std::hex << sbid << std::dec
      << dendl;
    repairer.fix_shared_blob(txn, sbid, nullptr, 0);
    cnt++;
    if (cnt >= max_transactions) {}
      db->submit_transaction_sync(txn);
      txn = db->get_transaction();
      cnt = 0;
    });
  if (cnt > 0) {
    db->submit_transaction_sync(txn);
  }

  // amount of repairs to report to be equal to previously
  // determined error estimation, not the actual number of updated shared blobs
  repairer.inc_repaired(sb_ref_mismatches);
}

BlueStore::OnodeRef BlueStore::fsck_check_objects_shallow(
  BlueStore::FSCKDepth depth,
  int64_t pool_id,
  BlueStore::CollectionRef c,
  const ghobject_t& oid,
  const string& key,
  const bufferlist& value,
  mempool::bluestore_fsck::list<string>* expecting_shards,
  map<BlobRef, bluestore_blob_t::unused_t>* referenced,
  const BlueStore::FSCK_ObjectCtx& ctx)
{
  auto& errors = ctx.errors;
  auto& num_objects = ctx.num_objects;
  auto& num_extents = ctx.num_extents;
  auto& num_blobs = ctx.num_blobs;
  auto& num_sharded_objects = ctx.num_sharded_objects;
  auto& num_spanning_blobs = ctx.num_spanning_blobs;
  auto used_blocks = ctx.used_blocks;
  auto sb_info_lock = ctx.sb_info_lock;
  auto& sb_info = ctx.sb_info;
  auto& sb_ref_counts = ctx.sb_ref_counts;
  auto repairer = ctx.repairer;

  store_statfs_t* res_statfs = (per_pool_stat_collection || repairer) ?
    &ctx.expected_pool_statfs[pool_id] :
    &ctx.expected_store_statfs;


  dout(10) << __func__ << "  " << oid << dendl;
  OnodeRef o;
  o.reset(Onode::create_decode(c, oid, key, value));
  ++num_objects;

  num_spanning_blobs += o->extent_map.spanning_blob_map.size();

  o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);
  _dump_onode<30>(cct, *o);
  // shards
  if (!o->extent_map.shards.empty()) {
    ++num_sharded_objects;
    if (depth != FSCK_SHALLOW) {
      ceph_assert(expecting_shards);
      for (auto& s : o->extent_map.shards) {
        dout(20) << __func__ << "    shard " << *s.shard_info << dendl;
        expecting_shards->push_back(string());
        get_extent_shard_key(o->key, s.shard_info->offset,
          &expecting_shards->back());
        if (s.shard_info->offset >= o->onode.size) {
          derr << "fsck error: " << oid << " shard 0x" << std::hex
            << s.shard_info->offset << " past EOF at 0x" << o->onode.size
            << std::dec << dendl;
          ++errors;
        }
      }
    }
  }

  // lextents
  uint64_t pos = 0;
  mempool::bluestore_fsck::map<BlobRef,
    bluestore_blob_use_tracker_t> ref_map;
  for (auto& l : o->extent_map.extent_map) {
    dout(20) << __func__ << "    " << l << dendl;
    if (l.logical_offset < pos) {
      derr << "fsck error: " << oid << " lextent at 0x"
        << std::hex << l.logical_offset
        << " overlaps with the previous, which ends at 0x" << pos
        << std::dec << dendl;
      ++errors;
    }
    if (depth != FSCK_SHALLOW &&
      o->extent_map.spans_shard(l.logical_offset, l.length)) {
      derr << "fsck error: " << oid << " lextent at 0x"
        << std::hex << l.logical_offset << "~" << l.length
        << " spans a shard boundary"
        << std::dec << dendl;
      ++errors;
    }
    pos = l.logical_offset + l.length;
    res_statfs->data_stored += l.length;
    ceph_assert(l.blob);
    const bluestore_blob_t& blob = l.blob->get_blob();

    auto& ref = ref_map[l.blob];
    if (ref.is_empty()) {
      uint32_t min_release_size = blob.get_release_size(min_alloc_size);
      uint32_t l = blob.get_logical_length();
      ref.init(l, min_release_size);
    }
    ref.get(
      l.blob_offset,
      l.length);
    ++num_extents;
    if (depth != FSCK_SHALLOW &&
      blob.has_unused()) {
      ceph_assert(referenced);
      auto p = referenced->find(l.blob);
      bluestore_blob_t::unused_t* pu;
      if (p == referenced->end()) {
        pu = &(*referenced)[l.blob];
      }
      else {
        pu = &p->second;
      }
      uint64_t blob_len = blob.get_logical_length();
      ceph_assert((blob_len % (sizeof(*pu) * 8)) == 0);
      ceph_assert(l.blob_offset + l.length <= blob_len);
      uint64_t chunk_size = blob_len / (sizeof(*pu) * 8);
      uint64_t start = l.blob_offset / chunk_size;
      uint64_t end =
        round_up_to(l.blob_offset + l.length, chunk_size) / chunk_size;
      for (auto i = start; i < end; ++i) {
        (*pu) |= (1u << i);
      }
    }
  } //for (auto& l : o->extent_map.extent_map)

  for (auto& i : ref_map) {
    ++num_blobs;
    const bluestore_blob_t& blob = i.first->get_blob();
    bool equal =
      depth == FSCK_SHALLOW ? true :
      i.first->get_blob_use_tracker().equal(i.second);
    if (!equal) {
      derr << "fsck error: " << oid << " blob " << *i.first
        << " doesn't match expected ref_map " << i.second << dendl;
      ++errors;
    }
    if (blob.is_compressed()) {
      res_statfs->data_compressed += blob.get_compressed_payload_length();
      res_statfs->data_compressed_original +=
        i.first->get_referenced_bytes();
    }
    if (depth != FSCK_SHALLOW && repairer) {
      for (auto e : blob.get_extents()) {
	if (!e.is_valid())
	  continue;
	repairer->set_space_used(e.offset, e.length, c->cid, oid);
      }
    }
    if (blob.is_shared()) {
      if (i.first->get_sbid() > blobid_max) {
        derr << "fsck error: " << oid << " blob " << blob
          << " sbid " << i.first->get_sbid() << " > blobid_max "
          << blobid_max << dendl;
        ++errors;
      } else if (i.first->get_sbid() == 0) {
        derr << "fsck error: " << oid << " blob " << blob
          << " marked as shared but has uninitialized sbid"
          << dendl;
        ++errors;
      }
      // the below lock is optional and provided in multithreading mode only
      if (sb_info_lock) {
        sb_info_lock->lock();
      }
      auto sbid = i.first->get_sbid();
      sb_info_t& sbi = sb_info.add_or_adopt(i.first->get_sbid());
      ceph_assert(sbi.pool_id == sb_info_t::INVALID_POOL_ID ||
        sbi.pool_id == oid.hobj.get_logical_pool());
      sbi.pool_id = oid.hobj.get_logical_pool();
      bool compressed = blob.is_compressed();
      for (auto e : blob.get_extents()) {
        if (e.is_valid()) {
	  if (compressed) {
	    ceph_assert(sbi.allocated_chunks <= 0);
	    sbi.allocated_chunks -= (e.length >> min_alloc_size_order);
	  } else {
	    ceph_assert(sbi.allocated_chunks >= 0);
	    sbi.allocated_chunks += (e.length >> min_alloc_size_order);
	  }
	  sb_ref_counts.inc_range(sbid, e.offset, e.length, 1);
        }
      }
      if (sb_info_lock) {
        sb_info_lock->unlock();
      }
    } else if (depth != FSCK_SHALLOW) {
      ceph_assert(used_blocks);
      string ctx_descr = " oid " + stringify(oid);
      errors += _fsck_check_extents(ctx_descr,
	blob.get_extents(),
        blob.is_compressed(),
        *used_blocks,
        fm->get_alloc_size(),
	repairer,
        *res_statfs,
        depth);
    } else {
      errors += _fsck_sum_extents(
        blob.get_extents(),
        blob.is_compressed(),
        *res_statfs);
    }
  } // for (auto& i : ref_map)

  {
    auto &sbm = o->extent_map.spanning_blob_map;
    size_t broken = 0;
    BlobRef first_broken;
    for (auto it = sbm.begin(); it != sbm.end();) {
      auto it1 = it++;
      if (ref_map.count(it1->second) == 0) {
        if (!broken) {
          first_broken = it1->second;
          ++errors;
          derr << "fsck error:" << " stray spanning blob found:" << it1->first
               << dendl;
        }
        broken++;
        if (repairer) {
          sbm.erase(it1);
        }
      }
    }

    if (broken) {
      derr << "fsck error: " << oid << " - " << broken
           << " zombie spanning blob(s) found, the first one: "
           << *first_broken << dendl;
      if(repairer) {
        repairer->fix_spanning_blobs(
	  db,
	  [&](KeyValueDB::Transaction txn) {
	    _record_onode(o, txn);
	  });
      }
    }
  }

  if (o->onode.has_omap()) {
    _fsck_check_object_omap(depth, o, ctx);
  }

  return o;
}

class ShallowFSCKThreadPool : public ThreadPool
{
public:
  ShallowFSCKThreadPool(CephContext* cct_, std::string nm, std::string tn, int n) :
    ThreadPool(cct_, nm, tn, n) {
  }
  void worker(ThreadPool::WorkThread* wt) override {
    int next_wq = 0;
    while (!_stop) {
      next_wq %= work_queues.size();
      WorkQueue_ *wq = work_queues[next_wq++];

      void* item = wq->_void_dequeue();
      if (item) {
        processing++;
        TPHandle tp_handle(cct, nullptr, wq->timeout_interval.load(), wq->suicide_interval.load());
        wq->_void_process(item, tp_handle);
        processing--;
      }
    }
  }
  template <size_t BatchLen>
  struct FSCKWorkQueue : public ThreadPool::WorkQueue_
  {
    struct Entry {
      int64_t pool_id;
      BlueStore::CollectionRef c;
      ghobject_t oid;
      string key;
      bufferlist value;
    };
    struct Batch {
      std::atomic<size_t> running = { 0 };
      size_t entry_count = 0;
      std::array<Entry, BatchLen> entries;

      int64_t errors = 0;
      int64_t warnings = 0;
      uint64_t num_objects = 0;
      uint64_t num_extents = 0;
      uint64_t num_blobs = 0;
      uint64_t num_sharded_objects = 0;
      uint64_t num_spanning_blobs = 0;
      store_statfs_t expected_store_statfs;
      BlueStore::per_pool_statfs expected_pool_statfs;
    };

    size_t batchCount;
    BlueStore* store = nullptr;

    ceph::mutex* sb_info_lock = nullptr;
    sb_info_space_efficient_map_t* sb_info = nullptr;
    shared_blob_2hash_tracker_t* sb_ref_counts = nullptr;
    BlueStoreRepairer* repairer = nullptr;

    Batch* batches = nullptr;
    size_t last_batch_pos = 0;
    bool batch_acquired = false;

    FSCKWorkQueue(std::string n,
                  size_t _batchCount,
                  BlueStore* _store,
                  ceph::mutex* _sb_info_lock,
                  sb_info_space_efficient_map_t& _sb_info,
		  shared_blob_2hash_tracker_t& _sb_ref_counts,
                  BlueStoreRepairer* _repairer) :
      WorkQueue_(n, ceph::timespan::zero(), ceph::timespan::zero()),
      batchCount(_batchCount),
      store(_store),
      sb_info_lock(_sb_info_lock),
      sb_info(&_sb_info),
      sb_ref_counts(&_sb_ref_counts),
      repairer(_repairer)
    {
      batches = new Batch[batchCount];
    }
    ~FSCKWorkQueue() {
      delete[] batches;
    }

    /// Remove all work items from the queue.
    void _clear() override {
      //do nothing
    }
    /// Check whether there is anything to do.
    bool _empty() override {
      ceph_assert(false);
    }

    /// Get the next work item to process.
    void* _void_dequeue() override {
      size_t pos = rand() % batchCount;
      size_t pos0 = pos;
      do {
        auto& batch = batches[pos];
        if (batch.running.fetch_add(1) == 0) {
          if (batch.entry_count) {
            return &batch;
          }
        }
        batch.running--;
        pos++;
        pos %= batchCount;
      } while (pos != pos0);
      return nullptr;
    }
    /** @brief Process the work item.
     * This function will be called several times in parallel
     * and must therefore be thread-safe. */
    void _void_process(void* item, TPHandle& handle) override {
      Batch* batch = (Batch*)item;

      BlueStore::FSCK_ObjectCtx ctx(
        batch->errors,
        batch->warnings,
        batch->num_objects,
        batch->num_extents,
        batch->num_blobs,
        batch->num_sharded_objects,
        batch->num_spanning_blobs,
        nullptr, // used_blocks
        nullptr, //used_omap_head
	nullptr,
        sb_info_lock,
        *sb_info,
	*sb_ref_counts,
        batch->expected_store_statfs,
        batch->expected_pool_statfs,
        repairer);

      for (size_t i = 0; i < batch->entry_count; i++) {
        auto& entry = batch->entries[i];

        store->fsck_check_objects_shallow(
          BlueStore::FSCK_SHALLOW,
          entry.pool_id,
          entry.c,
          entry.oid,
          entry.key,
          entry.value,
          nullptr, // expecting_shards - this will need a protection if passed
          nullptr, // referenced
          ctx);
      }
      batch->entry_count = 0;
      batch->running--;
    }
    /** @brief Synchronously finish processing a work item.
     * This function is called after _void_process with the global thread pool lock held,
     * so at most one copy will execute simultaneously for a given thread pool.
     * It can be used for non-thread-safe finalization. */
    void _void_process_finish(void*) override {
      ceph_assert(false);
    }

    bool queue(
      int64_t pool_id,
      BlueStore::CollectionRef c,
      const ghobject_t& oid,
      const string& key,
      const bufferlist& value) {
      bool res = false;
      size_t pos0 = last_batch_pos;
      if (!batch_acquired) {
        do {
          auto& batch = batches[last_batch_pos];
          if (batch.running.fetch_add(1) == 0) {
            if (batch.entry_count < BatchLen) {
              batch_acquired = true;
              break;
            }
          }
          batch.running.fetch_sub(1);
          last_batch_pos++;
          last_batch_pos %= batchCount;
        } while (last_batch_pos != pos0);
      }
      if (batch_acquired) {
        auto& batch = batches[last_batch_pos];
        ceph_assert(batch.running);
        ceph_assert(batch.entry_count < BatchLen);

        auto& entry = batch.entries[batch.entry_count];
        entry.pool_id = pool_id;
        entry.c = c;
        entry.oid = oid;
        entry.key = key;
        entry.value = value;

        ++batch.entry_count;
        if (batch.entry_count == BatchLen) {
          batch_acquired = false;
          batch.running.fetch_sub(1);
          last_batch_pos++;
          last_batch_pos %= batchCount;
        }
        res = true;
      }
      return res;
    }

    void finalize(ThreadPool& tp,
                  BlueStore::FSCK_ObjectCtx& ctx) {
      if (batch_acquired) {
        auto& batch = batches[last_batch_pos];
        ceph_assert(batch.running);
        batch.running.fetch_sub(1);
      }
      tp.stop();

      for (size_t i = 0; i < batchCount; i++) {
        auto& batch = batches[i];

        //process leftovers if any
        if (batch.entry_count) {
          TPHandle tp_handle(store->cct,
            nullptr,
            timeout_interval.load(),
            suicide_interval.load());
          ceph_assert(batch.running == 0);

          batch.running++; // just to be on-par with the regular call
          _void_process(&batch, tp_handle);
        }
        ceph_assert(batch.entry_count == 0);

        ctx.errors += batch.errors;
        ctx.warnings += batch.warnings;
        ctx.num_objects += batch.num_objects;
        ctx.num_extents += batch.num_extents;
        ctx.num_blobs += batch.num_blobs;
        ctx.num_sharded_objects += batch.num_sharded_objects;
        ctx.num_spanning_blobs += batch.num_spanning_blobs;

        ctx.expected_store_statfs.add(batch.expected_store_statfs);

        for (auto it = batch.expected_pool_statfs.begin();
          it != batch.expected_pool_statfs.end();
          it++) {
          ctx.expected_pool_statfs[it->first].add(it->second);
        }
      }
    }
  };
};

void BlueStore::_fsck_check_object_omap(FSCKDepth depth,
  OnodeRef& o,
  const BlueStore::FSCK_ObjectCtx& ctx)
{
  auto& errors = ctx.errors;
  auto& warnings = ctx.warnings;
  auto repairer = ctx.repairer;

  ceph_assert(o->onode.has_omap());
  if (!o->onode.is_perpool_omap() && !o->onode.is_pgmeta_omap()) {
    if (per_pool_omap == OMAP_PER_POOL) {
      fsck_derr(errors, MAX_FSCK_ERROR_LINES)
        << "fsck error: " << o->oid
        << " has omap that is not per-pool or pgmeta"
        << fsck_dendl;
      ++errors;
    } else {
      const char* w;
      int64_t num;
      if (cct->_conf->bluestore_fsck_error_on_no_per_pool_omap) {
        ++errors;
        num = errors;
        w = "error";
      } else {
        ++warnings;
        num = warnings;
        w = "warning";
      }
      fsck_derr(num, MAX_FSCK_ERROR_LINES)
        << "fsck " << w << ": " << o->oid
        << " has omap that is not per-pool or pgmeta"
        << fsck_dendl;
    }
  } else if (!o->onode.is_perpg_omap() && !o->onode.is_pgmeta_omap()) {
    if (per_pool_omap == OMAP_PER_PG) {
      fsck_derr(errors, MAX_FSCK_ERROR_LINES)
        << "fsck error: " << o->oid
        << " has omap that is not per-pg or pgmeta"
        << fsck_dendl;
      ++errors;
    } else {
      const char* w;
      int64_t num;
      if (cct->_conf->bluestore_fsck_error_on_no_per_pg_omap) {
        ++errors;
        num = errors;
        w = "error";
      } else {
        ++warnings;
        num = warnings;
        w = "warning";
      }
      fsck_derr(num, MAX_FSCK_ERROR_LINES)
        << "fsck " << w << ": " << o->oid
        << " has omap that is not per-pg or pgmeta"
        << fsck_dendl;
    }
  }
  if (repairer &&
    !o->onode.is_perpg_omap() &&
    !o->onode.is_pgmeta_omap()) {
    dout(10) << "fsck converting " << o->oid << " omap to per-pg" << dendl;
    bufferlist header;
    map<string, bufferlist> kv;
    {
      KeyValueDB::Transaction txn = db->get_transaction();
      uint64_t txn_cost = 0;
      const string& prefix = Onode::calc_omap_prefix(o->onode.flags);
      uint8_t new_flags = o->onode.flags |
	bluestore_onode_t::FLAG_PERPOOL_OMAP |
	bluestore_onode_t::FLAG_PERPG_OMAP;
      const string& new_omap_prefix = Onode::calc_omap_prefix(new_flags);

      KeyValueDB::Iterator it = db->get_iterator(prefix);
      string head, tail;
      o->get_omap_header(&head);
      o->get_omap_tail(&tail);
      it->lower_bound(head);
      // head
      if (it->valid() && it->key() == head) {
	dout(30) << __func__ << "  got header" << dendl;
	header = it->value();
	if (header.length()) {
	  string new_head;
	  Onode::calc_omap_header(new_flags, o.get(), &new_head);
	  txn->set(new_omap_prefix, new_head, header);
	  txn_cost += new_head.length() + header.length();
	}
	it->next();
      }
      // tail
      {
	string new_tail;
	Onode::calc_omap_tail(new_flags, o.get(), &new_tail);
	bufferlist empty;
	txn->set(new_omap_prefix, new_tail, empty);
	txn_cost += new_tail.length() + new_tail.length();
      }
      // values
      string final_key;
      Onode::calc_omap_key(new_flags, o.get(), string(), &final_key);
      size_t base_key_len = final_key.size();
      while (it->valid() && it->key() < tail) {
	string user_key;
	o->decode_omap_key(it->key(), &user_key);
	dout(20) << __func__ << "  got " << pretty_binary_string(it->key())
	  << " -> " << user_key << dendl;

	final_key.resize(base_key_len);
	final_key += user_key;
	auto v = it->value();
	txn->set(new_omap_prefix, final_key, v);
	txn_cost += final_key.length() + v.length();

	// submit a portion if cost exceeds 16MB
	if (txn_cost >= 16 * (1 << 20) ) {
	  db->submit_transaction_sync(txn);
	  txn = db->get_transaction();
	  txn_cost = 0;
	}
	it->next();
      }
      if (txn_cost > 0) {
	db->submit_transaction_sync(txn);
      }
    }
    // finalize: remove legacy data
    {
      KeyValueDB::Transaction txn = db->get_transaction();
      // remove old keys
      const string& old_omap_prefix = o->get_omap_prefix();
      string old_head, old_tail;
      o->get_omap_header(&old_head);
      o->get_omap_tail(&old_tail);
      txn->rm_range_keys(old_omap_prefix, old_head, old_tail);
      txn->rmkey(old_omap_prefix, old_tail);
      // set flag
      o->onode.set_flag(bluestore_onode_t::FLAG_PERPOOL_OMAP | bluestore_onode_t::FLAG_PERPG_OMAP);
      _record_onode(o, txn);
      db->submit_transaction_sync(txn);
      repairer->inc_repaired();
      repairer->request_compaction();
    }
  }
}

void BlueStore::_fsck_check_objects(
  FSCKDepth depth,
  BlueStore::FSCK_ObjectCtx& ctx)
{
  auto& errors = ctx.errors;
  auto sb_info_lock = ctx.sb_info_lock;
  auto& sb_info = ctx.sb_info;
  auto& sb_ref_counts = ctx.sb_ref_counts;
  auto repairer = ctx.repairer;

  uint64_t_btree_t used_nids;

  size_t processed_myself = 0;

  auto it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
  mempool::bluestore_fsck::list<string> expecting_shards;
  if (it) {
    const size_t thread_count = cct->_conf->bluestore_fsck_quick_fix_threads;
    typedef ShallowFSCKThreadPool::FSCKWorkQueue<256> WQ;
    std::unique_ptr<WQ> wq(
      new WQ(
        "FSCKWorkQueue",
        (thread_count ? : 1) * 32,
        this,
        sb_info_lock,
        sb_info,
	sb_ref_counts,
        repairer));

    ShallowFSCKThreadPool thread_pool(cct, "ShallowFSCKThreadPool", "ShallowFSCK", thread_count);

    thread_pool.add_work_queue(wq.get());
    if (depth == FSCK_SHALLOW && thread_count > 0) {
      //not the best place but let's check anyway
      ceph_assert(sb_info_lock);
      thread_pool.start();
    }

    // fill global if not overriden below
    CollectionRef c;
    int64_t pool_id = -1;
    spg_t pgid;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      dout(30) << __func__ << " key "
        << pretty_binary_string(it->key()) << dendl;
      if (is_extent_shard_key(it->key())) {
        if (depth == FSCK_SHALLOW) {
          continue;
        }
        while (!expecting_shards.empty() &&
          expecting_shards.front() < it->key()) {
          derr << "fsck error: missing shard key "
            << pretty_binary_string(expecting_shards.front())
            << dendl;
          ++errors;
          expecting_shards.pop_front();
        }
        if (!expecting_shards.empty() &&
          expecting_shards.front() == it->key()) {
          // all good
          expecting_shards.pop_front();
          continue;
        }

        uint32_t offset;
        string okey;
        get_key_extent_shard(it->key(), &okey, &offset);
        derr << "fsck error: stray shard 0x" << std::hex << offset
          << std::dec << dendl;
        if (expecting_shards.empty()) {
          derr << "fsck error: " << pretty_binary_string(it->key())
            << " is unexpected" << dendl;
          ++errors;
          continue;
        }
        while (expecting_shards.front() > it->key()) {
          derr << "fsck error:   saw " << pretty_binary_string(it->key())
            << dendl;
          derr << "fsck error:   exp "
            << pretty_binary_string(expecting_shards.front()) << dendl;
          ++errors;
          expecting_shards.pop_front();
          if (expecting_shards.empty()) {
            break;
          }
        }
        continue;
      }

      ghobject_t oid;
      int r = get_key_object(it->key(), &oid);
      if (r < 0) {
        derr << "fsck error: bad object key "
          << pretty_binary_string(it->key()) << dendl;
        ++errors;
        continue;
      }
      if (!c ||
        oid.shard_id != pgid.shard ||
        oid.hobj.get_logical_pool() != (int64_t)pgid.pool() ||
        !c->contains(oid)) {
        c = nullptr;
        for (auto& p : coll_map) {
          if (p.second->contains(oid)) {
            c = p.second;
            break;
          }
        }
        if (!c) {
          derr << "fsck error: stray object " << oid
            << " not owned by any collection" << dendl;
          ++errors;
          continue;
        }
        pool_id = c->cid.is_pg(&pgid) ? pgid.pool() : META_POOL_ID;
        dout(20) << __func__ << "  collection " << c->cid << " " << c->cnode
          << dendl;
      }

      if (depth != FSCK_SHALLOW &&
        !expecting_shards.empty()) {
        for (auto& k : expecting_shards) {
          derr << "fsck error: missing shard key "
            << pretty_binary_string(k) << dendl;
        }
        ++errors;
        expecting_shards.clear();
      }

      bool queued = false;
      if (depth == FSCK_SHALLOW && thread_count > 0) {
        queued = wq->queue(
          pool_id,
          c,
          oid,
          it->key(),
          it->value());
      }
      OnodeRef o;
      map<BlobRef, bluestore_blob_t::unused_t> referenced;

      if (!queued) {
        ++processed_myself;
         o = fsck_check_objects_shallow(
          depth,
          pool_id,
          c,
          oid,
          it->key(),
          it->value(),
          &expecting_shards,
          &referenced,
          ctx);
      }

      if (depth != FSCK_SHALLOW) {
        ceph_assert(o != nullptr);
        if (o->onode.nid) {
          if (o->onode.nid > nid_max) {
            derr << "fsck error: " << oid << " nid " << o->onode.nid
              << " > nid_max " << nid_max << dendl;
            ++errors;
          }
          if (used_nids.count(o->onode.nid)) {
            derr << "fsck error: " << oid << " nid " << o->onode.nid
              << " already in use" << dendl;
            ++errors;
            continue; // go for next object
          }
          used_nids.insert(o->onode.nid);
        }
        for (auto& i : referenced) {
          dout(20) << __func__ << "  referenced 0x" << std::hex << i.second
            << std::dec << " for " << *i.first << dendl;
          const bluestore_blob_t& blob = i.first->get_blob();
          if (i.second & blob.unused) {
            derr << "fsck error: " << oid << " blob claims unused 0x"
              << std::hex << blob.unused
              << " but extents reference 0x" << i.second << std::dec
              << " on blob " << *i.first << dendl;
            ++errors;
          }
          if (blob.has_csum()) {
            uint64_t blob_len = blob.get_logical_length();
            uint64_t unused_chunk_size = blob_len / (sizeof(blob.unused) * 8);
            unsigned csum_count = blob.get_csum_count();
            unsigned csum_chunk_size = blob.get_csum_chunk_size();
            for (unsigned p = 0; p < csum_count; ++p) {
              unsigned pos = p * csum_chunk_size;
              unsigned firstbit = pos / unused_chunk_size;    // [firstbit,lastbit]
              unsigned lastbit = (pos + csum_chunk_size - 1) / unused_chunk_size;
              unsigned mask = 1u << firstbit;
              for (unsigned b = firstbit + 1; b <= lastbit; ++b) {
                mask |= 1u << b;
              }
              if ((blob.unused & mask) == mask) {
                // this csum chunk region is marked unused
                if (blob.get_csum_item(p) != 0) {
                  derr << "fsck error: " << oid
                    << " blob claims csum chunk 0x" << std::hex << pos
                    << "~" << csum_chunk_size
                    << " is unused (mask 0x" << mask << " of unused 0x"
                    << blob.unused << ") but csum is non-zero 0x"
                    << blob.get_csum_item(p) << std::dec << " on blob "
                    << *i.first << dendl;
                  ++errors;
                }
              }
            }
          }
        }
        // omap
        if (o->onode.has_omap()) {
          ceph_assert(ctx.used_omap_head);
          if (ctx.used_omap_head->count(o->onode.nid)) {
            derr << "fsck error: " << o->oid << " omap_head " << o->onode.nid
                 << " already in use" << dendl;
            ++errors;
          } else {
            ctx.used_omap_head->insert(o->onode.nid);
          }
        } // if (o->onode.has_omap())
        if (depth == FSCK_DEEP) {
          bufferlist bl;
          uint64_t max_read_block = cct->_conf->bluestore_fsck_read_bytes_cap;
          uint64_t offset = 0;
          do {
            uint64_t l = std::min(uint64_t(o->onode.size - offset), max_read_block);
            int r = _do_read(c.get(), o, offset, l, bl,
              CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
            if (r < 0) {
              ++errors;
              derr << "fsck error: " << oid << std::hex
                << " error during read: "
                << " " << offset << "~" << l
                << " " << cpp_strerror(r) << std::dec
                << dendl;
              break;
            }
            offset += l;
          } while (offset < o->onode.size);
        } // deep
      } //if (depth != FSCK_SHALLOW)
    } // for (it->lower_bound(string()); it->valid(); it->next())
    if (depth == FSCK_SHALLOW && thread_count > 0) {
      wq->finalize(thread_pool, ctx);
      if (processed_myself) {
        // may be needs more threads?
        dout(0) << __func__ << " partial offload"
                << ", done myself " << processed_myself
                << " of " << ctx.num_objects
                << "objects, threads " << thread_count
                << dendl;
      }
    }
  } // if (it)
}
/**
An overview for currently implemented repair logics 
performed in fsck in two stages: detection(+preparation) and commit.
Detection stage (in processing order):
  (Issue -> Repair action to schedule)
  - Detect undecodable keys for Shared Blobs -> Remove
  - Detect undecodable records for Shared Blobs -> Remove 
    (might trigger missed Shared Blob detection below)
  - Detect stray records for Shared Blobs -> Remove
  - Detect misreferenced pextents -> Fix
    Prepare Bloom-like filter to track cid/oid -> pextent 
    Prepare list of extents that are improperly referenced
    Enumerate Onode records that might use 'misreferenced' pextents
    (Bloom-like filter applied to reduce computation)
      Per each questinable Onode enumerate all blobs and identify broken ones 
      (i.e. blobs having 'misreferences')
      Rewrite each broken blob data by allocating another extents and 
      copying data there
      If blob is shared - unshare it and mark corresponding Shared Blob 
      for removal
      Release previously allocated space
      Update Extent Map
  - Detect missed Shared Blobs -> Recreate
  - Detect undecodable deferred transaction -> Remove
  - Detect Freelist Manager's 'false free' entries -> Mark as used
  - Detect Freelist Manager's leaked entries -> Mark as free
  - Detect statfs inconsistency - Update
  Commit stage (separate DB commit per each step):
  - Apply leaked FM entries fix
  - Apply 'false free' FM entries fix
  - Apply 'Remove' actions
  - Apply fix for misreference pextents
  - Apply Shared Blob recreate 
    (can be merged with the step above if misreferences were dectected)
  - Apply StatFS update
*/
int BlueStore::_fsck(BlueStore::FSCKDepth depth, bool repair)
{
  dout(5) << __func__
    << (repair ? " repair" : " check")
    << (depth == FSCK_DEEP ? " (deep)" :
      depth == FSCK_SHALLOW ? " (shallow)" : " (regular)")
    << dendl;

  // in deep mode we need R/W write access to be able to replay deferred ops
  const bool read_only = !(repair || depth == FSCK_DEEP);
  int r = _open_db_and_around(read_only);
  if (r < 0) {
    return r;
  }
  auto close_db = make_scope_guard([&] {
    _close_db_and_around();
  });

  if (!read_only) {
    r = _upgrade_super();
    if (r < 0) {
      return r;
    }
  }

  // NullFreelistManager needs to open collection early
  r = _open_collections();
  if (r < 0) {
    return r;
  }

  mempool_thread.init();
  auto stop_mempool = make_scope_guard([&] {
    mempool_thread.shutdown();
    _shutdown_cache();
  });
  // we need finisher and kv_{sync,finalize}_thread *just* for replay
  // enable in repair or deep mode modes only
  if (!read_only) {
    _kv_start();
    r = _deferred_replay();
    _kv_stop();
  }

  if (r < 0) {
    return r;
  }
  return _fsck_on_open(depth, repair);
}

int BlueStore::_fsck_on_open(BlueStore::FSCKDepth depth, bool repair)
{
  uint64_t sb_hash_size = uint64_t(
    cct->_conf.get_val<Option::size_t>("osd_memory_target") *
    cct->_conf.get_val<double>(
      "bluestore_fsck_shared_blob_tracker_size"));

  dout(1) << __func__
	  << " <<<START>>>"
	  << (repair ? " repair" : " check")
	  << (depth == FSCK_DEEP ? " (deep)" :
                depth == FSCK_SHALLOW ? " (shallow)" : " (regular)")
          << " start sb_tracker_hash_size:" << sb_hash_size
          << dendl;
  int64_t errors = 0;
  int64_t warnings = 0;
  unsigned repaired = 0;

  uint64_t_btree_t used_omap_head;
  uint64_t_btree_t used_sbids;

  mempool_dynamic_bitset used_blocks, bluefs_used_blocks;
  KeyValueDB::Iterator it;
  store_statfs_t expected_store_statfs;
  per_pool_statfs expected_pool_statfs;

  sb_info_space_efficient_map_t sb_info;
  shared_blob_2hash_tracker_t sb_ref_counts(
    sb_hash_size,
    min_alloc_size);
  size_t sb_ref_mismatches = 0;

  /// map of oid -> (first_)offset for each zone
  std::vector<std::unordered_map<ghobject_t, uint64_t>> zone_refs;   // FIXME: this may be a lot of RAM!

  uint64_t num_objects = 0;
  uint64_t num_extents = 0;
  uint64_t num_blobs = 0;
  uint64_t num_spanning_blobs = 0;
  uint64_t num_shared_blobs = 0;
  uint64_t num_sharded_objects = 0;
  BlueStoreRepairer repairer;

  auto alloc_size = fm->get_alloc_size();

  utime_t start = ceph_clock_now();

  _fsck_collections(&errors);
  used_blocks.resize(fm->get_alloc_units());

  if (bluefs) {
    interval_set<uint64_t> bluefs_extents;

    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&](uint64_t start, uint32_t len) {
        apply_for_bitset_range(start, len, alloc_size, used_blocks,
          [&](uint64_t pos, mempool_dynamic_bitset& bs) {
            ceph_assert(pos < bs.size());
            bs.set(pos);
          }
        );
      }
    );
  }

  bluefs_used_blocks = used_blocks;

  apply_for_bitset_range(
    0, std::max<uint64_t>(min_alloc_size, DB_SUPER_RESERVED), alloc_size, used_blocks,
    [&](uint64_t pos, mempool_dynamic_bitset &bs) {
      bs.set(pos);
    }
  );


  if (repair) {
    repairer.init_space_usage_tracker(
      bdev->get_size(),
      min_alloc_size);
  }

  if (bluefs) {
    int r = bluefs->fsck();
    if (r < 0) {
      return r;
    }
    if (r > 0)
      errors += r;
  }

  if (!per_pool_stat_collection) {
    const char *w;
    if (cct->_conf->bluestore_fsck_error_on_no_per_pool_stats) {
      w = "error";
      ++errors;
    } else {
      w = "warning";
      ++warnings;
    }
    derr << "fsck " << w << ": store not yet converted to per-pool stats"
	 << dendl;
  }
  if (per_pool_omap != OMAP_PER_PG) {
    const char *w;
    if (cct->_conf->bluestore_fsck_error_on_no_per_pool_omap) {
      w = "error";
      ++errors;
    } else {
      w = "warning";
      ++warnings;
    }
    derr << "fsck " << w << ": store not yet converted to per-pg omap"
	 << dendl;
  }

  if (g_conf()->bluestore_debug_fsck_abort) {
    dout(1) << __func__ << " debug abort" << dendl;
    goto out_scan;
  }

  dout(1) << __func__ << " checking shared_blobs (phase 1)" << dendl;
  it = db->get_iterator(PREFIX_SHARED_BLOB, KeyValueDB::ITERATOR_NOCACHE);
  if (it) {
    for (it->lower_bound(string()); it->valid(); it->next()) {
      string key = it->key();
      uint64_t sbid;
      if (get_key_shared_blob(key, &sbid) < 0) {
        // Failed to parse the key.
	// This gonna to be handled at the second stage
	continue;
      }
      bluestore_shared_blob_t shared_blob(sbid);
      bufferlist bl = it->value();
      auto blp = bl.cbegin();
      try {
	decode(shared_blob, blp);
      }
      catch (ceph::buffer::error& e) {
	// this gonna to be handled at the second stage
	continue;
      }
      dout(20) << __func__ << "  " << shared_blob << dendl;
      auto& sbi = sb_info.add_maybe_stray(sbid);

      // primarily to silent the 'unused' warning
      ceph_assert(sbi.pool_id == sb_info_t::INVALID_POOL_ID);

      for (auto& r : shared_blob.ref_map.ref_map) {
	sb_ref_counts.inc_range(
	  sbid,
	  r.first,
	  r.second.length,
	  -r.second.refs);
      }
    }
  } // if (it) //checking shared_blobs (phase1)

  // walk PREFIX_OBJ
  {
    dout(1) << __func__ << " walking object keyspace" << dendl;
    ceph::mutex sb_info_lock =  ceph::make_mutex("BlueStore::fsck::sbinfo_lock");
    BlueStore::FSCK_ObjectCtx ctx(
      errors,
      warnings,
      num_objects,
      num_extents,
      num_blobs,
      num_sharded_objects,
      num_spanning_blobs,
      &used_blocks,
      &used_omap_head,
      &zone_refs,
      //no need for the below lock when in non-shallow mode as
      // there is no multithreading in this case
      depth == FSCK_SHALLOW ? &sb_info_lock : nullptr,
      sb_info,
      sb_ref_counts,
      expected_store_statfs,
      expected_pool_statfs,
      repair ? &repairer : nullptr);

    _fsck_check_objects(depth, ctx);
  }

  sb_ref_mismatches = sb_ref_counts.count_non_zero();
  if (sb_ref_mismatches != 0) {
    derr << "fsck error:" << "*" << sb_ref_mismatches
         << " shared blob references aren't matching, at least "
         << sb_ref_mismatches << " found" << dendl;
    errors += sb_ref_mismatches;
    if (!repair) {
      uint32_t cnts = 0;
      _fsck_foreach_shared_blob( [&](coll_t cid,
				     ghobject_t oid,
				     uint64_t sbid,
				     const bluestore_blob_t& b) {
	for (auto& p : b.get_extents()) {
	  if (p.is_valid() &&
	      !sb_ref_counts.test_all_zero_range(sbid,
						 p.offset,
						 p.length)) {
	    derr << "fsck possibly broken shared blob found for col:" << cid
		 << " obj:" << oid
		 << " sbid 0x" << std::hex << sbid << std::dec
		 << " " << p
		 << dendl;
	    ++cnts;
	    break;
	  }
	}
	return cnts <= MAX_FSCK_ERROR_LINES;
      });
    }
  }

  if (depth != FSCK_SHALLOW && repair) {
    _fsck_repair_shared_blobs(repairer, sb_ref_counts, sb_info);
  }
  dout(1) << __func__ << " checking shared_blobs (phase 2)" << dendl;
  it = db->get_iterator(PREFIX_SHARED_BLOB, KeyValueDB::ITERATOR_NOCACHE);
  if (it) {
    // FIXME minor: perhaps simplify for shallow mode?
    // fill global if not overriden below
    auto expected_statfs = &expected_store_statfs;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      string key = it->key();
      uint64_t sbid;
      if (get_key_shared_blob(key, &sbid)) {
	derr << "fsck error: bad key '" << key
	  << "' in shared blob namespace" << dendl;
	if (repair) {
	  repairer.remove_key(db, PREFIX_SHARED_BLOB, key);
	}
	++errors;
	continue;
      }
      auto p = sb_info.find(sbid);
      if (p == sb_info.end()) {
        if (sb_ref_mismatches > 0) {
	  // highly likely this has been already reported before, ignoring...
	  dout(5) << __func__ << " found duplicate(?) stray shared blob data for sbid 0x"
	    << std::hex << sbid << std::dec << dendl;
	} else {
	  derr<< "fsck error: found stray shared blob data for sbid 0x"
	    << std::hex << sbid << std::dec << dendl;
	  ++errors;
	  if (repair) {
	    repairer.remove_key(db, PREFIX_SHARED_BLOB, key);
	  }
	}
      } else {
	++num_shared_blobs;
	sb_info_t& sbi = *p;
	bluestore_shared_blob_t shared_blob(sbid);
	bufferlist bl = it->value();
	auto blp = bl.cbegin();
	try {
	  decode(shared_blob, blp);
	}
	catch (ceph::buffer::error& e) {
	  ++errors;

	  derr << "fsck error: failed to decode Shared Blob"
	    << pretty_binary_string(key) << dendl;
	  if (repair) {
	    dout(20) << __func__ << " undecodable Shared Blob, key:'"
	      << pretty_binary_string(key)
	      << "', removing" << dendl;
	    repairer.remove_key(db, PREFIX_SHARED_BLOB, key);
	  }
	  continue;
	}
	dout(20) << __func__ << "  " << shared_blob << dendl;
	PExtentVector extents;
	for (auto& r : shared_blob.ref_map.ref_map) {
	  extents.emplace_back(bluestore_pextent_t(r.first, r.second.length));
	}
	if (sbi.pool_id != sb_info_t::INVALID_POOL_ID &&
	    (per_pool_stat_collection || repair)) {
	  expected_statfs = &expected_pool_statfs[sbi.pool_id];
	}
	std::stringstream ss;
	ss << "sbid 0x" << std::hex << sbid << std::dec;
	errors += _fsck_check_extents(ss.str(),
	  extents,
	  sbi.allocated_chunks < 0,
	  used_blocks,
	  fm->get_alloc_size(),
	  repair ? &repairer : nullptr,
	  *expected_statfs,
	  depth);
      }
    }
  } // if (it) /* checking shared_blobs (phase 2)*/

  if (repair && repairer.preprocess_misreference(db)) {

    dout(1) << __func__ << " sorting out misreferenced extents" << dendl;
    auto& misref_extents = repairer.get_misreferences();
    interval_set<uint64_t> to_release;
    it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      // fill global if not overriden below
      auto expected_statfs = &expected_store_statfs;

      CollectionRef c;
      spg_t pgid;
      KeyValueDB::Transaction txn = repairer.get_fix_misreferences_txn();
      bool bypass_rest = false;
      for (it->lower_bound(string()); it->valid() && !bypass_rest;
	   it->next()) {
	dout(30) << __func__ << " key "
		 << pretty_binary_string(it->key()) << dendl;
	if (is_extent_shard_key(it->key())) {
	  continue;
	}

	ghobject_t oid;
	int r = get_key_object(it->key(), &oid);
	if (r < 0 || !repairer.is_used(oid)) {
	  continue;
	}

	if (!c ||
	    oid.shard_id != pgid.shard ||
	    oid.hobj.get_logical_pool() != (int64_t)pgid.pool() ||
	    !c->contains(oid)) {
	  c = nullptr;
	  for (auto& p : coll_map) {
	    if (p.second->contains(oid)) {
	      c = p.second;
	      break;
	    }
	  }
	  if (!c) {
	    continue;
	  }
	  if (per_pool_stat_collection || repair) {
	    auto pool_id = c->cid.is_pg(&pgid) ? pgid.pool() : META_POOL_ID;
	    expected_statfs = &expected_pool_statfs[pool_id];
	  }
	}
	if (!repairer.is_used(c->cid)) {
	  continue;
	}

	dout(20) << __func__ << " check misreference for col:" << c->cid
		  << " obj:" << oid << dendl;

        OnodeRef o;
        o.reset(Onode::create_decode(c, oid, it->key(), it->value()));
	o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);
	mempool::bluestore_fsck::set<BlobRef> blobs;

	for (auto& e : o->extent_map.extent_map) {
	  blobs.insert(e.blob);
	}
	bool need_onode_update = false;
	bool first_dump = true;
	for(auto b : blobs) {
	  bool broken_blob = false;
	  auto& pextents = b->dirty_blob().dirty_extents();
	  for (auto& e : pextents) {
	    if (!e.is_valid()) {
	      continue;
	    }
	    // for the sake of simplicity and proper shared blob handling
	    // always rewrite the whole blob even when it's partially
	    // misreferenced.
	    if (misref_extents.intersects(e.offset, e.length)) {
	      if (first_dump) {
		first_dump = false;
		_dump_onode<10>(cct, *o);
	      }
	      broken_blob = true;
	      break;
	    }
	  }
	  if (!broken_blob)
	    continue;
	  bool compressed = b->get_blob().is_compressed();
          need_onode_update = true;
	  dout(10) << __func__
		    << " fix misreferences in oid:" << oid
		    << " " << *b << dendl;
	  uint64_t b_off = 0;
	  PExtentVector pext_to_release;
	  pext_to_release.reserve(pextents.size());
	  // rewriting all valid pextents
	  for (auto e = pextents.begin(); e != pextents.end();
	         e++) {
	    auto b_off_cur = b_off;
	    b_off += e->length;
	    if (!e->is_valid()) {
	      continue;
	    }
	    PExtentVector exts;
	    dout(5) << __func__ << "::NCB::(F)alloc=" << alloc << ", length=" << e->length << dendl;
	    int64_t alloc_len =
              alloc->allocate(e->length, min_alloc_size,
				       0, 0, &exts);
	    if (alloc_len < 0 || alloc_len < (int64_t)e->length) {
	      derr << __func__
	           << " failed to allocate 0x" << std::hex << e->length
		   << " allocated 0x " << (alloc_len < 0 ? 0 : alloc_len)
		   << " min_alloc_size 0x" << min_alloc_size
		   << " available 0x " << alloc->get_free()
		   << std::dec << dendl;
	      if (alloc_len > 0) {
                alloc->release(exts);
	      }
	      bypass_rest = true;
	      break;
	    }
            expected_statfs->allocated += e->length;
	    if (compressed) {
	      expected_statfs->data_compressed_allocated += e->length;
	    }

	    bufferlist bl;
	    IOContext ioc(cct, NULL, !cct->_conf->bluestore_fail_eio);
	    r = bdev->read(e->offset, e->length, &bl, &ioc, false);
	    if (r < 0) {
	      derr << __func__ << " failed to read from 0x" << std::hex << e->offset
		    <<"~" << e->length << std::dec << dendl;
	      ceph_abort_msg("read failed, wtf");
	    }
	    pext_to_release.push_back(*e);
	    e = pextents.erase(e);
    	    e = pextents.insert(e, exts.begin(), exts.end());
	    b->get_blob().map_bl(
	      b_off_cur, bl,
	      [&](uint64_t offset, bufferlist& t) {
		int r = bdev->write(offset, t, false);
		ceph_assert(r == 0);
	      });
	    e += exts.size() - 1;
            for (auto& p : exts) {
	      fm->allocate(p.offset, p.length, txn);
	    }
	  } // for (auto e = pextents.begin(); e != pextents.end(); e++) {

	  if (b->get_blob().is_shared()) {
            b->dirty_blob().clear_flag(bluestore_blob_t::FLAG_SHARED);

	    auto sbid = b->get_sbid();
	    auto sb_it = sb_info.find(sbid);
	    ceph_assert(sb_it != sb_info.end());
	    sb_info_t& sbi = *sb_it;

	    if (sbi.allocated_chunks < 0) {
	      // NB: it's crucial to use compressed_allocated_chunks from sb_info_t
	      // as we originally used that value while accumulating
	      // expected_statfs
	      expected_statfs->allocated -= uint64_t(-sbi.allocated_chunks) << min_alloc_size_order;
	      expected_statfs->data_compressed_allocated -=
		uint64_t(-sbi.allocated_chunks) << min_alloc_size_order;
	    } else {
	      expected_statfs->allocated -= uint64_t(sbi.allocated_chunks) << min_alloc_size_order;
	    }
	    sbi.allocated_chunks = 0;
	    repairer.fix_shared_blob(txn, sbid, nullptr, 0);

	    // relying on blob's pextents to decide what to release.
	    for (auto& p : pext_to_release) {
	      to_release.union_insert(p.offset, p.length);
	    }
	  } else {
	    for (auto& p : pext_to_release) {
	      expected_statfs->allocated -= p.length;
	      if (compressed) {
		expected_statfs->data_compressed_allocated -= p.length;
	      }
	      to_release.union_insert(p.offset, p.length);
	    }
	  }
	  if (bypass_rest) {
	    break;
	  }
	} // for(auto b : blobs) 
	if (need_onode_update) {
	  o->extent_map.dirty_range(0, OBJECT_MAX_SIZE);
	  _record_onode(o, txn);
	}
      } // for (it->lower_bound(string()); it->valid(); it->next())

      for (auto it = to_release.begin(); it != to_release.end(); ++it) {
	dout(10) << __func__ << " release 0x" << std::hex << it.get_start()
		 << "~" << it.get_len() << std::dec << dendl;
	fm->release(it.get_start(), it.get_len(), txn);
      }
      alloc->release(to_release);
      to_release.clear();
    } // if (it) {
  } //if (repair && repairer.preprocess_misreference()) {
  sb_info.clear();
  sb_ref_counts.reset();

  dout(1) << __func__ << " checking pool_statfs" << dendl;
  _fsck_check_statfs(expected_store_statfs, expected_pool_statfs,
    errors, warnings, repair ? &repairer : nullptr);
  if (depth != FSCK_SHALLOW) {
    dout(1) << __func__ << " checking for stray omap data " << dendl;
    it = db->get_iterator(PREFIX_OMAP, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      uint64_t last_omap_head = 0;
      for (it->lower_bound(string()); it->valid(); it->next()) {
        uint64_t omap_head;

        _key_decode_u64(it->key().c_str(), &omap_head);

        if (used_omap_head.count(omap_head) == 0 &&
           omap_head != last_omap_head) {
          pair<string,string> rk = it->raw_key();
          fsck_derr(errors, MAX_FSCK_ERROR_LINES)
            << "fsck error: found stray omap data on omap_head "
            << omap_head << " " << last_omap_head
            << " prefix/key: " << url_escape(rk.first)
            << " " << url_escape(rk.second)
            << fsck_dendl;
          ++errors;
          last_omap_head = omap_head;
        }
      }
    }
    it = db->get_iterator(PREFIX_PGMETA_OMAP, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      uint64_t last_omap_head = 0;
      for (it->lower_bound(string()); it->valid(); it->next()) {
        uint64_t omap_head;
        _key_decode_u64(it->key().c_str(), &omap_head);
        if (used_omap_head.count(omap_head) == 0 &&
	    omap_head != last_omap_head) {
          pair<string,string> rk = it->raw_key();
          fsck_derr(errors, MAX_FSCK_ERROR_LINES)
            << "fsck error: found stray (pgmeta) omap data on omap_head "
            << omap_head << " " << last_omap_head
            << " prefix/key: " << url_escape(rk.first)
            << " " << url_escape(rk.second)
            << fsck_dendl;
          last_omap_head = omap_head;
	  ++errors;
        }
      }
    }
    it = db->get_iterator(PREFIX_PERPOOL_OMAP, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      uint64_t last_omap_head = 0;
      for (it->lower_bound(string()); it->valid(); it->next()) {
        uint64_t pool;
        uint64_t omap_head;
        string k = it->key();
        const char *c = k.c_str();
        c = _key_decode_u64(c, &pool);
        c = _key_decode_u64(c, &omap_head);
        if (used_omap_head.count(omap_head) == 0 &&
          omap_head != last_omap_head) {
          pair<string,string> rk = it->raw_key();
          fsck_derr(errors, MAX_FSCK_ERROR_LINES)
            << "fsck error: found stray (per-pool) omap data on omap_head "
            << omap_head << " " << last_omap_head
            << " prefix/key: " << url_escape(rk.first)
            << " " << url_escape(rk.second)
            << fsck_dendl;
          ++errors;
          last_omap_head = omap_head;
        }
      }
    }
    it = db->get_iterator(PREFIX_PERPG_OMAP, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      uint64_t last_omap_head = 0;
      for (it->lower_bound(string()); it->valid(); it->next()) {
        uint64_t pool;
        uint32_t hash;
        uint64_t omap_head;
        string k = it->key();
        const char* c = k.c_str();
        c = _key_decode_u64(c, &pool);
        c = _key_decode_u32(c, &hash);
        c = _key_decode_u64(c, &omap_head);
        if (used_omap_head.count(omap_head) == 0 &&
          omap_head != last_omap_head) {
          fsck_derr(errors, MAX_FSCK_ERROR_LINES)
            << "fsck error: found stray (per-pg) omap data on omap_head "
	    << " key " << pretty_binary_string(it->key())
            << omap_head << " " << last_omap_head << " " << used_omap_head.count(omap_head) << fsck_dendl;
          ++errors;
          last_omap_head = omap_head;
        }
      }
    }
    dout(1) << __func__ << " checking deferred events" << dendl;
    it = db->get_iterator(PREFIX_DEFERRED, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      for (it->lower_bound(string()); it->valid(); it->next()) {
        bufferlist bl = it->value();
        auto p = bl.cbegin();
        bluestore_deferred_transaction_t wt;
        try {
	  decode(wt, p);
        } catch (ceph::buffer::error& e) {
	  derr << "fsck error: failed to decode deferred txn "
	       << pretty_binary_string(it->key()) << dendl;
	  if (repair) {
            dout(20) << __func__ << " undecodable deferred TXN record, key: '"
		     << pretty_binary_string(it->key())
		     << "', removing" << dendl;
	    repairer.remove_key(db, PREFIX_DEFERRED, it->key());
	  }
	  continue;
        }
        dout(20) << __func__ << "  deferred " << wt.seq
	         << " ops " << wt.ops.size()
	         << " released 0x" << std::hex << wt.released << std::dec << dendl;
        for (auto e = wt.released.begin(); e != wt.released.end(); ++e) {
          apply_for_bitset_range(
            e.get_start(), e.get_len(), alloc_size, used_blocks,
            [&](uint64_t pos, mempool_dynamic_bitset &bs) {
              bs.set(pos);
            }
          );
        }
      }
    }

    // skip freelist vs allocated compare when we have Null fm
    if (!fm->is_null_manager()) {
      dout(1) << __func__ << " checking freelist vs allocated" << dendl;
      fm->enumerate_reset();
      uint64_t offset, length;
      while (fm->enumerate_next(db, &offset, &length)) {
        bool intersects = false;
        apply_for_bitset_range(
          offset, length, alloc_size, used_blocks,
          [&](uint64_t pos, mempool_dynamic_bitset &bs) {
            ceph_assert(pos < bs.size());
            if (bs.test(pos) && !bluefs_used_blocks.test(pos)) {
              if (offset == DB_SUPER_RESERVED &&
                  length == min_alloc_size - DB_SUPER_RESERVED) {
                // this is due to the change just after luminous to min_alloc_size
                // granularity allocations, and our baked in assumption at the top
                // of _fsck that 0~round_up_to(DB_SUPER_RESERVED,min_alloc_size) is used
                // (vs luminous's round_up_to(DB_SUPER_RESERVED,block_size)).  harmless,
                // since we will never allocate this region below min_alloc_size.
                dout(10) << __func__ << " ignoring free extent between DB_SUPER_RESERVED"
                         << " and min_alloc_size, 0x" << std::hex << offset << "~"
                         << length << std::dec << dendl;
              } else {
                intersects = true;
                if (repair) {
                  repairer.fix_false_free(db, fm,
                                          pos * min_alloc_size,
                                          min_alloc_size);
                }
              }
            } else {
              bs.set(pos);
            }
          }
          );
        if (intersects) {
          derr << "fsck error: free extent 0x" << std::hex << offset
               << "~" << length << std::dec
               << " intersects allocated blocks" << dendl;
          ++errors;
        }
      }
      fm->enumerate_reset();

      // check for leaked extents
      size_t count = used_blocks.count();
      if (used_blocks.size() != count) {
        ceph_assert(used_blocks.size() > count);
        used_blocks.flip();
        size_t start = used_blocks.find_first();
        while (start != decltype(used_blocks)::npos) {
          size_t cur = start;
          while (true) {
            size_t next = used_blocks.find_next(cur);
            if (next != cur + 1) {
              ++errors;
              derr << "fsck error: leaked extent 0x" << std::hex
                   << ((uint64_t)start * fm->get_alloc_size()) << "~"
                   << ((cur + 1 - start) * fm->get_alloc_size()) << std::dec
                   << dendl;
              if (repair) {
                repairer.fix_leaked(db,
                                    fm,
                                    start * min_alloc_size,
                                    (cur + 1 - start) * min_alloc_size);
              }
              start = next;
              break;
            }
            cur = next;
          }
        }
        used_blocks.flip();
      }
    }
  }
  if (repair) {
    if (per_pool_omap != OMAP_PER_PG) {
      dout(5) << __func__ << " fixing per_pg_omap" << dendl;
      repairer.fix_per_pool_omap(db, OMAP_PER_PG);
    }

    dout(5) << __func__ << " applying repair results" << dendl;
    repaired = repairer.apply(db);
    dout(5) << __func__ << " repair applied" << dendl;
  }

out_scan:
  dout(2) << __func__ << " " << num_objects << " objects, "
	  << num_sharded_objects << " of them sharded.  "
	  << dendl;
  dout(2) << __func__ << " " << num_extents << " extents to "
	  << num_blobs << " blobs, "
	  << num_spanning_blobs << " spanning, "
	  << num_shared_blobs << " shared."
	  << dendl;

  utime_t duration = ceph_clock_now() - start;
  dout(1) << __func__ << " <<<FINISH>>> with " << errors << " errors, "
	  << warnings << " warnings, "
	  << repaired << " repaired, "
	  << (errors + warnings - (int)repaired) << " remaining in "
	  << duration << " seconds" << dendl;

  // In non-repair mode we should return error count only as
  // it indicates if store status is OK.
  // In repair mode both errors and warnings are taken into account
  // since repaired counter relates to them both.
  return repair ? errors + warnings - (int)repaired : errors;
}

/// methods to inject various errors fsck can repair
void BlueStore::inject_broken_shared_blob_key(const string& key,
				  const bufferlist& bl)
{
  KeyValueDB::Transaction txn;
  txn = db->get_transaction();
  txn->set(PREFIX_SHARED_BLOB, key, bl);
  db->submit_transaction_sync(txn);
};

void BlueStore::inject_no_shared_blob_key()
{
  KeyValueDB::Transaction txn;
  txn = db->get_transaction();
  ceph_assert(blobid_last > 0);
  // kill the last used sbid, this can be broken due to blobid preallocation
  // in rare cases, leaving as-is for the sake of simplicity
  uint64_t sbid = blobid_last;

  string key;
  dout(5) << __func__<< " " << sbid << dendl;
  get_shared_blob_key(sbid, &key);
  txn->rmkey(PREFIX_SHARED_BLOB, key);
  db->submit_transaction_sync(txn);
};

void BlueStore::inject_stray_shared_blob_key(uint64_t sbid)
{
  KeyValueDB::Transaction txn;
  txn = db->get_transaction();

  dout(5) << __func__ << " " << sbid << dendl;

  string key;
  get_shared_blob_key(sbid, &key);
  bluestore_shared_blob_t persistent(sbid);
  persistent.ref_map.get(0xdead0000, min_alloc_size);
  bufferlist bl;
  encode(persistent, bl);
  dout(20) << __func__ << " sbid " << sbid
    << " takes " << bl.length() << " bytes, updating"
    << dendl;

  txn->set(PREFIX_SHARED_BLOB, key, bl);
  db->submit_transaction_sync(txn);
};


void BlueStore::inject_leaked(uint64_t len)
{
  PExtentVector exts;
  int64_t alloc_len = alloc->allocate(len, min_alloc_size,
					   min_alloc_size * 256, 0, &exts);

  if (fm->is_null_manager()) {
    return;
  }

  KeyValueDB::Transaction txn;
  txn = db->get_transaction();

  ceph_assert(alloc_len >= (int64_t)len);
  for (auto& p : exts) {
    fm->allocate(p.offset, p.length, txn);
  }
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_false_free(coll_t cid, ghobject_t oid)
{
  ceph_assert(!fm->is_null_manager());

  KeyValueDB::Transaction txn;
  OnodeRef o;
  CollectionRef c = _get_collection(cid);
  ceph_assert(c);
  {
    std::unique_lock l{c->lock}; // just to avoid internal asserts
    o = c->get_onode(oid, false);
    ceph_assert(o);
    o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);
  }

  bool injected = false;
  txn = db->get_transaction();
  auto& em = o->extent_map.extent_map;
  std::vector<const PExtentVector*> v;
  if (em.size()) {
    v.push_back(&em.begin()->blob->get_blob().get_extents());
  }
  if (em.size() > 1) {
    auto it = em.end();
    --it;
    v.push_back(&(it->blob->get_blob().get_extents()));
  }
  for (auto pext : v) {
    if (pext->size()) {
      auto p = pext->begin();
      while (p != pext->end()) {
	if (p->is_valid()) {
	  dout(20) << __func__ << " release 0x" << std::hex << p->offset
	           << "~" << p->length << std::dec << dendl;
	  fm->release(p->offset, p->length, txn);
	  injected = true;
	  break;
	}
	++p;
      }
    }
  }
  ceph_assert(injected);
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_legacy_omap()
{
  dout(1) << __func__ << dendl;
  per_pool_omap = OMAP_BULK;
  KeyValueDB::Transaction txn;
  txn = db->get_transaction();
  txn->rmkey(PREFIX_SUPER, "per_pool_omap");
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_legacy_omap(coll_t cid, ghobject_t oid)
{
  dout(1) << __func__ << " "
          << cid << " " << oid
          <<dendl;
  KeyValueDB::Transaction txn;
  OnodeRef o;
  CollectionRef c = _get_collection(cid);
  ceph_assert(c);
  {
    std::unique_lock l{ c->lock }; // just to avoid internal asserts
    o = c->get_onode(oid, false);
    ceph_assert(o);
  }
  o->onode.clear_flag(
    bluestore_onode_t::FLAG_PERPG_OMAP |
    bluestore_onode_t::FLAG_PERPOOL_OMAP |
    bluestore_onode_t::FLAG_PGMETA_OMAP);
  txn = db->get_transaction();
  _record_onode(o, txn);
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_stray_omap(uint64_t head, const string& name)
{
  dout(1) << __func__ << dendl;
  KeyValueDB::Transaction txn = db->get_transaction();

  string key;
  bufferlist bl;
  _key_encode_u64(head, &key);
  key.append(name);
  txn->set(PREFIX_OMAP, key, bl);

  db->submit_transaction_sync(txn);
}

void BlueStore::inject_statfs(const string& key, const store_statfs_t& new_statfs)
{
  BlueStoreRepairer repairer;
  repairer.fix_statfs(db, key, new_statfs);
  repairer.apply(db);
}

void BlueStore::inject_global_statfs(const store_statfs_t& new_statfs)
{
  KeyValueDB::Transaction t = db->get_transaction();
  volatile_statfs v;
  v = new_statfs;
  bufferlist bl;
  v.encode(bl);
  t->set(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, bl);
  db->submit_transaction_sync(t);
  // must set these; are needed at _close_db() statfs persisting
  per_pool_stat_collection = false;
  vstatfs = new_statfs;
}

void BlueStore::inject_misreference(coll_t cid1, ghobject_t oid1,
				    coll_t cid2, ghobject_t oid2,
				    uint64_t offset)
{
  OnodeRef o1;
  CollectionRef c1 = _get_collection(cid1);
  ceph_assert(c1);
  {
    std::unique_lock l{c1->lock}; // just to avoid internal asserts
    o1 = c1->get_onode(oid1, false);
    ceph_assert(o1);
    o1->extent_map.fault_range(db, offset, OBJECT_MAX_SIZE);
  }
  OnodeRef o2;
  CollectionRef c2 = _get_collection(cid2);
  ceph_assert(c2);
  {
    std::unique_lock l{c2->lock}; // just to avoid internal asserts
    o2 = c2->get_onode(oid2, false);
    ceph_assert(o2);
    o2->extent_map.fault_range(db, offset, OBJECT_MAX_SIZE);
  }
  Extent& e1 = *(o1->extent_map.seek_lextent(offset));
  Extent& e2 = *(o2->extent_map.seek_lextent(offset));

  // require onode/extent layout to be the same (and simple)
  // to make things easier
  ceph_assert(o1->onode.extent_map_shards.empty());
  ceph_assert(o2->onode.extent_map_shards.empty());
  ceph_assert(o1->extent_map.spanning_blob_map.size() == 0);
  ceph_assert(o2->extent_map.spanning_blob_map.size() == 0);
  ceph_assert(e1.logical_offset == e2.logical_offset);
  ceph_assert(e1.length == e2.length);
  ceph_assert(e1.blob_offset == e2.blob_offset);

  KeyValueDB::Transaction txn;
  txn = db->get_transaction();

  // along with misreference error this will create space leaks errors
  e2.blob->dirty_blob() = e1.blob->get_blob();
  o2->extent_map.dirty_range(offset, e2.length);
  o2->extent_map.update(txn, false);

  _record_onode(o2, txn);
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_zombie_spanning_blob(coll_t cid, ghobject_t oid,
                                            int16_t blob_id)
{
  OnodeRef o;
  CollectionRef c = _get_collection(cid);
  ceph_assert(c);
  {
    std::unique_lock l{ c->lock }; // just to avoid internal asserts
    o = c->get_onode(oid, false);
    ceph_assert(o);
    o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);
  }

  BlobRef b = c->new_blob();
  b->id = blob_id;
  o->extent_map.spanning_blob_map[blob_id] = b;

  KeyValueDB::Transaction txn;
  txn = db->get_transaction();

  _record_onode(o, txn);
  db->submit_transaction_sync(txn);
}

void BlueStore::inject_bluefs_file(std::string_view dir, std::string_view name, size_t new_size)
{
  ceph_assert(bluefs);

  BlueFS::FileWriter* p_handle = nullptr;
  auto ret = bluefs->open_for_write(dir, name, &p_handle, false);
  ceph_assert(ret == 0);

  std::string s('0', new_size);
  bufferlist bl;
  bl.append(s);
  p_handle->append(bl);

  bluefs->fsync(p_handle);
  bluefs->close_writer(p_handle);
}

void BlueStore::collect_metadata(map<string,string> *pm)
{
  dout(10) << __func__ << dendl;
  bdev->collect_metadata("bluestore_bdev_", pm);
  if (bluefs) {
    (*pm)["bluefs"] = "1";
    // this value is for backward compatibility only
    (*pm)["bluefs_single_shared_device"] = \
      stringify((int)bluefs_layout.single_shared_device());
    (*pm)["bluefs_dedicated_db"] = \
       stringify((int)bluefs_layout.dedicated_db);
    (*pm)["bluefs_dedicated_wal"] = \
       stringify((int)bluefs_layout.dedicated_wal);
    bluefs->collect_metadata(pm, bluefs_layout.shared_bdev);
  } else {
    (*pm)["bluefs"] = "0";
  }

  // report numa mapping for underlying devices
  int node = -1;
  set<int> nodes;
  set<string> failed;
  int r = get_numa_node(&node, &nodes, &failed);
  if (r >= 0) {
    if (!failed.empty()) {
      (*pm)["objectstore_numa_unknown_devices"] = stringify(failed);
    }
    if (!nodes.empty()) {
      dout(1) << __func__ << " devices span numa nodes " << nodes << dendl;
      (*pm)["objectstore_numa_nodes"] = stringify(nodes);
    }
    if (node >= 0) {
      (*pm)["objectstore_numa_node"] = stringify(node);
    }
  }
  (*pm)["bluestore_min_alloc_size"] = stringify(min_alloc_size);
  (*pm)["bluestore_allocation_from_file"] = stringify(fm && fm->is_null_manager());
}

int BlueStore::get_numa_node(
  int *final_node,
  set<int> *out_nodes,
  set<string> *out_failed)
{
  int node = -1;
  set<string> devices;
  get_devices(&devices);
  set<int> nodes;
  set<string> failed;
  for (auto& devname : devices) {
    int n;
    BlkDev bdev(devname);
    int r = bdev.get_numa_node(&n);
    if (r < 0) {
      dout(10) << __func__ << " bdev " << devname << " can't detect numa_node"
	       << dendl;
      failed.insert(devname);
      continue;
    }
    dout(10) << __func__ << " bdev " << devname << " on numa_node " << n
	     << dendl;
    nodes.insert(n);
    if (node < 0) {
      node = n;
    }
  }
  if (node >= 0 && nodes.size() == 1 && failed.empty()) {
    *final_node = node;
  }
  if (out_nodes) {
    *out_nodes = nodes;
  }
  if (out_failed) {
    *out_failed = failed;
  }
  return 0;
}

void BlueStore::prepare_for_fast_shutdown()
{
  m_fast_shutdown = true;
}

int BlueStore::get_devices(set<string> *ls)
{
  if (bdev) {
    bdev->get_devices(ls);
    if (bluefs) {
      bluefs->get_devices(ls);
    }
    return 0;
  }

  // grumble, we haven't started up yet.
  if (int r = _open_path(); r < 0) {
    return r;
  }
  auto close_path = make_scope_guard([&] {
    _close_path();
  });
  if (int r = _open_fsid(false); r < 0) {
    return r;
  }
  auto close_fsid = make_scope_guard([&] {
    _close_fsid();
  });
  if (int r = _read_fsid(&fsid); r < 0) {
    return r;
  }
  if (int r = _lock_fsid(); r < 0) {
    return r;
  }
  if (int r = _open_bdev(false); r < 0) {
    return r;
  }
  auto close_bdev = make_scope_guard([&] {
    _close_bdev();
  });
  if (int r = _minimal_open_bluefs(false); r < 0) {
    return r;
  }
  bdev->get_devices(ls);
  if (bluefs) {
    bluefs->get_devices(ls);
  }
  _minimal_close_bluefs();
  return 0;
}

void BlueStore::_get_statfs_overall(struct store_statfs_t *buf)
{
  buf->reset();

  auto prefix = per_pool_omap == OMAP_BULK ?
    PREFIX_OMAP :
    per_pool_omap == OMAP_PER_POOL ?
      PREFIX_PERPOOL_OMAP :
      PREFIX_PERPG_OMAP;
  buf->omap_allocated =
    db->estimate_prefix_size(prefix, string());

  uint64_t bfree = alloc->get_free();

  if (bluefs) {
    buf->internally_reserved = 0;
    // include dedicated db, too, if that isn't the shared device.
    if (bluefs_layout.shared_bdev != BlueFS::BDEV_DB) {
      buf->total += bluefs->get_total(BlueFS::BDEV_DB);
    }
    // call any non-omap bluefs space "internal metadata"
    buf->internal_metadata =
      bluefs->get_used()
      - buf->omap_allocated;
  }

  ExtBlkDevState ebd_state;
  int rc = bdev->get_ebd_state(ebd_state);
  if (rc == 0) {
    buf->total += ebd_state.get_physical_total();

    // we are limited by both the size of the virtual device and the
    // underlying physical device.
    bfree = std::min(bfree, ebd_state.get_physical_avail());

    buf->allocated = ebd_state.get_physical_total() - ebd_state.get_physical_avail();;
  } else {
    buf->total += bdev->get_size();
  }
  buf->available = bfree;
}

int BlueStore::statfs(struct store_statfs_t *buf,
		      osd_alert_list_t* alerts)
{
  if (alerts) {
    alerts->clear();
    _log_alerts(*alerts);
  }
  _get_statfs_overall(buf);
  {
    std::lock_guard l(vstatfs_lock);
    buf->allocated = vstatfs.allocated();
    buf->data_stored = vstatfs.stored();
    buf->data_compressed = vstatfs.compressed();
    buf->data_compressed_original = vstatfs.compressed_original();
    buf->data_compressed_allocated = vstatfs.compressed_allocated();
  }

  dout(20) << __func__ << " " << *buf << dendl;
  return 0;
}

int BlueStore::pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
			   bool *out_per_pool_omap)
{
  dout(20) << __func__ << " pool " << pool_id<< dendl;

  if (!per_pool_stat_collection) {
    dout(20) << __func__ << " not supported in legacy mode " << dendl;
    return -ENOTSUP;
  }
  buf->reset();

  {
    std::lock_guard l(vstatfs_lock);
    osd_pools[pool_id].publish(buf);
  }

  string key_prefix;
  _key_encode_u64(pool_id, &key_prefix);
  *out_per_pool_omap = per_pool_omap != OMAP_BULK;
  // stop calls after db was closed
  if (*out_per_pool_omap && db) {
    auto prefix = per_pool_omap == OMAP_PER_POOL ?
      PREFIX_PERPOOL_OMAP :
      PREFIX_PERPG_OMAP;
    buf->omap_allocated = db->estimate_prefix_size(prefix, key_prefix);
  }

  dout(10) << __func__ << *buf << dendl;
  return 0;
}

void BlueStore::_check_legacy_statfs_alert()
{
  string s;
  if (!per_pool_stat_collection &&
      cct->_conf->bluestore_warn_on_legacy_statfs) {
    s = "legacy statfs reporting detected, "
        "suggest to run store repair to get consistent statistic reports";
  }
  std::lock_guard l(qlock);
  legacy_statfs_alert = s;
}

void BlueStore::_check_no_per_pg_or_pool_omap_alert()
{
  string per_pg, per_pool;
  if (per_pool_omap != OMAP_PER_PG) {
    if (cct->_conf->bluestore_warn_on_no_per_pg_omap) {
      per_pg = "legacy (not per-pg) omap detected, "
	"suggest to run store repair to benefit from faster PG removal";
    }
    if (per_pool_omap != OMAP_PER_POOL) {
      if (cct->_conf->bluestore_warn_on_no_per_pool_omap) {
	per_pool = "legacy (not per-pool) omap detected, "
	  "suggest to run store repair to benefit from per-pool omap usage statistics";
      }
    }
  }
  std::lock_guard l(qlock);
  no_per_pg_omap_alert = per_pg;
  no_per_pool_omap_alert = per_pool;
}

// ---------------
// cache

BlueStore::CollectionRef BlueStore::_get_collection(const coll_t& cid)
{
  std::shared_lock l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

BlueStore::CollectionRef BlueStore::_get_collection_by_oid(const ghobject_t& oid)
{
  std::shared_lock l(coll_lock);

  // FIXME: we must replace this with something more efficient

  for (auto& i : coll_map) {
    spg_t spgid;
    if (i.first.is_pg(&spgid) &&
	i.second->contains(oid)) {
      return i.second;
    }
  }
  return CollectionRef();
}

void BlueStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c << " " << c->cid << dendl;
  // _reap_collections and this in the same thread,
  // so no need a lock.
  removed_collections.push_back(c);
}

void BlueStore::_reap_collections()
{

  list<CollectionRef> removed_colls;
  {
    // _queue_reap_collection and this in the same thread.
    // So no need a lock.
    if (!removed_collections.empty())
      removed_colls.swap(removed_collections);
    else
      return;
  }

  list<CollectionRef>::iterator p = removed_colls.begin();
  while (p != removed_colls.end()) {
    CollectionRef c = *p;
    dout(10) << __func__ << " " << c << " " << c->cid << dendl;
    if (c->onode_space.map_any([&](Onode* o) {
	  ceph_assert(!o->exists);
	  if (o->flushing_count.load()) {
	    dout(10) << __func__ << " " << c << " " << c->cid << " " << o->oid
		     << " flush_txns " << o->flushing_count << dendl;
	    return true;
	  }
	  return false;
	})) {
      ++p;
      continue;
    }
    c->onode_space.clear();
    p = removed_colls.erase(p);
    dout(10) << __func__ << " " << c << " " << c->cid << " done" << dendl;
  }
  if (removed_colls.empty()) {
    dout(10) << __func__ << " all reaped" << dendl;
  } else {
    removed_collections.splice(removed_collections.begin(), removed_colls);
  }
}

void BlueStore::refresh_perf_counters()
{
  uint64_t num_onodes = 0;
  uint64_t num_pinned_onodes = 0;
  uint64_t num_extents = 0;
  uint64_t num_blobs = 0;
  uint64_t num_buffers = 0;
  uint64_t num_buffer_bytes = 0;
  for (auto c : onode_cache_shards) {
    c->add_stats(&num_onodes, &num_pinned_onodes);
  }
  for (auto c : buffer_cache_shards) {
    c->add_stats(&num_extents, &num_blobs,
                 &num_buffers, &num_buffer_bytes);
  }
  logger->set(l_bluestore_onodes, num_onodes);
  logger->set(l_bluestore_pinned_onodes, num_pinned_onodes);
  logger->set(l_bluestore_extents, num_extents);
  logger->set(l_bluestore_blobs, num_blobs);
  logger->set(l_bluestore_buffers, num_buffers);
  logger->set(l_bluestore_buffer_bytes, num_buffer_bytes);
}

// ---------------
// read operations

ObjectStore::CollectionHandle BlueStore::open_collection(const coll_t& cid)
{
  return _get_collection(cid);
}

ObjectStore::CollectionHandle BlueStore::create_new_collection(
  const coll_t& cid)
{
  std::unique_lock l{coll_lock};
  auto c = ceph::make_ref<Collection>(
    this,
    onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
    buffer_cache_shards[cid.hash_to_shard(buffer_cache_shards.size())],
    cid);
  new_coll_map[cid] = c;
  _osr_attach(c.get());
  return c;
}

void BlueStore::set_collection_commit_queue(
    const coll_t& cid,
    ContextQueue *commit_queue)
{
  if (commit_queue) {
    std::shared_lock l(coll_lock);
    if (coll_map.count(cid)) {
      coll_map[cid]->commit_queue = commit_queue;
    } else if (new_coll_map.count(cid)) {
      new_coll_map[cid]->commit_queue = commit_queue;
    }
  }
}


bool BlueStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return false;

  bool r = true;

  {
    std::shared_lock l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      r = false;
  }

  return r;
}

int BlueStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;

  {
    std::shared_lock l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      return -ENOENT;
    st->st_size = o->onode.size;
    st->st_blksize = 4096;
    st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
    st->st_nlink = 1;
  }

  int r = 0;
  if (_debug_mdata_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  }
  return r;
}
int BlueStore::set_collection_opts(
  CollectionHandle& ch,
  const pool_opts_t& opts)
{
  Collection *c = static_cast<Collection *>(ch.get());
  dout(15) << __func__ << " " << ch->cid << " options " << opts << dendl;
  if (!c->exists)
    return -ENOENT;
  std::unique_lock l{c->lock};
  c->pool_opts = opts;
  return 0;
}

int BlueStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags)
{
  auto start = mono_clock::now();
  Collection *c = static_cast<Collection *>(c_.get());
  const coll_t &cid = c->get_cid();
  dout(15) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  if (!c->exists)
    return -ENOENT;

  bl.clear();
  int r;
  {
    std::shared_lock l(c->lock);
    auto start1 = mono_clock::now();
    OnodeRef o = c->get_onode(oid, false);
    log_latency("get_onode@read",
      l_bluestore_read_onode_meta_lat,
      mono_clock::now() - start1,
      cct->_conf->bluestore_log_op_age,
      "", l_bluestore_slow_read_onode_meta_count);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (offset == length && offset == 0)
      length = o->onode.size;

    r = _do_read(c, o, offset, length, bl, op_flags);
    if (r == -EIO) {
      logger->inc(l_bluestore_read_eio);
    }
  }

 out:
  if (r >= 0 && _debug_data_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  } else if (oid.hobj.pool > 0 &&  /* FIXME, see #23029 */
	     cct->_conf->bluestore_debug_random_read_err &&
	     (rand() % (int)(cct->_conf->bluestore_debug_random_read_err *
			     100.0)) == 0) {
    dout(0) << __func__ << ": inject random EIO" << dendl;
    r = -EIO;
  }
  dout(10) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  log_latency(__func__,
    l_bluestore_read_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  return r;
}

void BlueStore::_read_cache(
  OnodeRef& o,
  uint64_t offset,
  size_t length,
  int read_cache_policy,
  ready_regions_t& ready_regions,
  blobs2read_t& blobs2read)
{
  // build blob-wise list to of stuff read (that isn't cached)
  unsigned left = length;
  uint64_t pos = offset;
  auto lp = o->extent_map.seek_lextent(offset);
  while (left > 0 && lp != o->extent_map.extent_map.end()) {
    if (pos < lp->logical_offset) {
      unsigned hole = lp->logical_offset - pos;
      if (hole >= left) {
        break;
      }
      dout(30) << __func__ << "  hole 0x" << std::hex << pos << "~" << hole
               << std::dec << dendl;
      pos += hole;
      left -= hole;
    }
    BlobRef& bptr = lp->blob;
    unsigned l_off = pos - lp->logical_offset;
    unsigned b_off = l_off + lp->blob_offset;
    unsigned b_len = std::min(left, lp->length - l_off);

    ready_regions_t cache_res;
    interval_set<uint32_t> cache_interval;
    bptr->dirty_bc().read(
      bptr->get_cache(), b_off, b_len, cache_res, cache_interval,
      read_cache_policy);
    dout(20) << __func__ << "  blob " << *bptr << std::hex
             << " need 0x" << b_off << "~" << b_len
             << " cache has 0x" << cache_interval
             << std::dec << dendl;

    auto pc = cache_res.begin();
    uint64_t chunk_size = bptr->get_blob().get_chunk_size(block_size);
    while (b_len > 0) {
      unsigned l;
      if (pc != cache_res.end() &&
          pc->first == b_off) {
        l = pc->second.length();
        ready_regions[pos] = std::move(pc->second);
        dout(30) << __func__ << "    use cache 0x" << std::hex << pos << ": 0x"
                 << b_off << "~" << l << std::dec << dendl;
        ++pc;
      } else {
        l = b_len;
        if (pc != cache_res.end()) {
          ceph_assert(pc->first > b_off);
          l = pc->first - b_off;
        }
        dout(30) << __func__ << "    will read 0x" << std::hex << pos << ": 0x"
                 << b_off << "~" << l << std::dec << dendl;
        // merge regions
        {
          uint64_t r_off = b_off;
          uint64_t r_len = l;
          uint64_t front = r_off % chunk_size;
          if (front) {
            r_off -= front;
            r_len += front;
          }
          unsigned tail = r_len % chunk_size;
          if (tail) {
            r_len += chunk_size - tail;
          }
          bool merged = false;
          regions2read_t& r2r = blobs2read[bptr];
          if (r2r.size()) {
            read_req_t& pre = r2r.back();
            if (r_off <= (pre.r_off + pre.r_len)) {
              front += (r_off - pre.r_off);
              pre.r_len += (r_off + r_len - pre.r_off - pre.r_len);
              pre.regs.emplace_back(region_t(pos, b_off, l, front));
              merged = true;
            }
          }
          if (!merged) {
            read_req_t req(r_off, r_len);
            req.regs.emplace_back(region_t(pos, b_off, l, front));
            r2r.emplace_back(std::move(req));
          }
        }
      }
      pos += l;
      b_off += l;
      left -= l;
      b_len -= l;
    }
    ++lp;
  }
}

int BlueStore::_prepare_read_ioc(
  blobs2read_t& blobs2read,
  vector<bufferlist>* compressed_blob_bls,
  IOContext* ioc)
{
  for (auto& p : blobs2read) {
    const BlobRef& bptr = p.first;
    regions2read_t& r2r = p.second;
    dout(20) << __func__ << "  blob " << *bptr << " need "
             << r2r << dendl;
    if (bptr->get_blob().is_compressed()) {
      // read the whole thing
      if (compressed_blob_bls->empty()) {
        // ensure we avoid any reallocation on subsequent blobs
        compressed_blob_bls->reserve(blobs2read.size());
      }
      compressed_blob_bls->push_back(bufferlist());
      bufferlist& bl = compressed_blob_bls->back();
      auto r = bptr->get_blob().map(
        0, bptr->get_blob().get_ondisk_length(),
        [&](uint64_t offset, uint64_t length) {
          int r = bdev->aio_read(offset, length, &bl, ioc);
          if (r < 0)
            return r;
          return 0;
        });
      if (r < 0) {
        derr << __func__ << " bdev-read failed: " << cpp_strerror(r) << dendl;
        if (r == -EIO) {
          // propagate EIO to caller
          return r;
        }
        ceph_assert(r == 0);
      }
    } else {
      // read the pieces
      for (auto& req : r2r) {
        dout(20) << __func__ << "    region 0x" << std::hex
                 << req.regs.front().logical_offset
                 << ": 0x" << req.regs.front().blob_xoffset
                 << " reading 0x" << req.r_off
                 << "~" << req.r_len << std::dec
                 << dendl;

        // read it
        auto r = bptr->get_blob().map(
          req.r_off, req.r_len,
          [&](uint64_t offset, uint64_t length) {
            int r = bdev->aio_read(offset, length, &req.bl, ioc);
            if (r < 0)
              return r;
            return 0;
          });
        if (r < 0) {
          derr << __func__ << " bdev-read failed: " << cpp_strerror(r)
               << dendl;
          if (r == -EIO) {
            // propagate EIO to caller
            return r;
          }
          ceph_assert(r == 0);
        }
        ceph_assert(req.bl.length() == req.r_len);
      }
    }
  }
  return 0;
}

int BlueStore::_generate_read_result_bl(
  OnodeRef& o,
  uint64_t offset,
  size_t length,
  ready_regions_t& ready_regions,
  vector<bufferlist>& compressed_blob_bls,
  blobs2read_t& blobs2read,
  bool buffered,
  bool* csum_error,
  bufferlist& bl)
{
 // enumerate and decompress desired blobs
  auto p = compressed_blob_bls.begin();
  blobs2read_t::iterator b2r_it = blobs2read.begin();
  while (b2r_it != blobs2read.end()) {
    const BlobRef& bptr = b2r_it->first;
    regions2read_t& r2r = b2r_it->second;
    dout(20) << __func__ << "  blob " << *bptr << " need "
             << r2r << dendl;
    if (bptr->get_blob().is_compressed()) {
      ceph_assert(p != compressed_blob_bls.end());
      bufferlist& compressed_bl = *p++;
      if (_verify_csum(o, &bptr->get_blob(), 0, compressed_bl,
                       r2r.front().regs.front().logical_offset) < 0) {
        *csum_error = true;
        return -EIO;
      }
      bufferlist raw_bl;
      auto r = _decompress(compressed_bl, &raw_bl);
      if (r < 0)
        return r;
      if (buffered) {
        bptr->dirty_bc().did_read(bptr->get_cache(), 0,
                                       raw_bl);
      }
      for (auto& req : r2r) {
        for (auto& r : req.regs) {
          ready_regions[r.logical_offset].substr_of(
            raw_bl, r.blob_xoffset, r.length);
        }
      }
    } else {
      for (auto& req : r2r) {
        if (_verify_csum(o, &bptr->get_blob(), req.r_off, req.bl,
                         req.regs.front().logical_offset) < 0) {
          *csum_error = true;
          return -EIO;
        }
        if (buffered) {
          bptr->dirty_bc().did_read(bptr->get_cache(),
                                         req.r_off, req.bl);
        }

        // prune and keep result
        for (const auto& r : req.regs) {
          ready_regions[r.logical_offset].substr_of(req.bl, r.front, r.length);
        }
      }
    }
    ++b2r_it;
  }

  // generate a resulting buffer
  auto pr = ready_regions.begin();
  auto pr_end = ready_regions.end();
  uint64_t pos = 0;
  while (pos < length) {
    if (pr != pr_end && pr->first == pos + offset) {
      dout(30) << __func__ << " assemble 0x" << std::hex << pos
               << ": data from 0x" << pr->first << "~" << pr->second.length()
               << std::dec << dendl;
      pos += pr->second.length();
      bl.claim_append(pr->second);
      ++pr;
    } else {
      uint64_t l = length - pos;
      if (pr != pr_end) {
        ceph_assert(pr->first > pos + offset);
        l = pr->first - (pos + offset);
      }
      dout(30) << __func__ << " assemble 0x" << std::hex << pos
               << ": zeros for 0x" << (pos + offset) << "~" << l
               << std::dec << dendl;
      bl.append_zero(l);
      pos += l;
    }
  }
  ceph_assert(bl.length() == length);
  ceph_assert(pos == length);
  ceph_assert(pr == pr_end);
  return 0;
}

int BlueStore::_do_read(
  Collection *c,
  OnodeRef& o,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  uint64_t retry_count)
{
  FUNCTRACE(cct);
  int r = 0;
  int read_cache_policy = 0; // do not bypass clean or dirty cache

  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
           << " size 0x" << o->onode.size << " (" << std::dec
           << o->onode.size << ")" << dendl;
  bl.clear();

  if (offset >= o->onode.size) {
    return r;
  }

  // generally, don't buffer anything, unless the client explicitly requests
  // it.
  bool buffered = false;
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered read" << dendl;
    buffered = true;
  } else if (cct->_conf->bluestore_default_buffered_read &&
	     (op_flags & (CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
			  CEPH_OSD_OP_FLAG_FADVISE_NOCACHE)) == 0) {
    dout(20) << __func__ << " defaulting to buffered read" << dendl;
    buffered = true;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  auto start = mono_clock::now();
  o->extent_map.fault_range(db, offset, length);
  log_latency(__func__,
    l_bluestore_read_onode_meta_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age,
    "", l_bluestore_slow_read_onode_meta_count);
  _dump_onode<30>(cct, *o);

  // for deep-scrub, we only read dirty cache and bypass clean cache in
  // order to read underlying block device in case there are silent disk errors.
  if (op_flags & CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE) {
    dout(20) << __func__ << " will bypass cache and do direct read" << dendl;
    read_cache_policy = BufferSpace::BYPASS_CLEAN_CACHE;
  }

  // build blob-wise list to of stuff read (that isn't cached)
  ready_regions_t ready_regions;
  blobs2read_t blobs2read;
  _read_cache(o, offset, length, read_cache_policy, ready_regions, blobs2read);


  // read raw blob data.
  start = mono_clock::now(); // for the sake of simplicity
                             // measure the whole block below.
                             // The error isn't that much...
  vector<bufferlist> compressed_blob_bls;
  IOContext ioc(cct, NULL, !cct->_conf->bluestore_fail_eio);
  r = _prepare_read_ioc(blobs2read, &compressed_blob_bls, &ioc);
  // we always issue aio for reading, so errors other than EIO are not allowed
  if (r < 0)
    return r;

  int64_t num_ios = blobs2read.size();
  if (ioc.has_pending_aios()) {
    num_ios = ioc.get_num_ios();
    bdev->aio_submit(&ioc);
    dout(20) << __func__ << " waiting for aio" << dendl;
    ioc.aio_wait();
    r = ioc.get_return_value();
    if (r < 0) {
      ceph_assert(r == -EIO); // no other errors allowed
      return -EIO;
    }
  }
  log_latency_fn(__func__,
    l_bluestore_read_wait_aio_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age,
    [&](auto lat) { return ", num_ios = " + stringify(num_ios); },
    l_bluestore_slow_read_wait_aio_count
  );

  bool csum_error = false;
  r = _generate_read_result_bl(o, offset, length, ready_regions,
                              compressed_blob_bls, blobs2read,
                              buffered && !ioc.skip_cache(),
                              &csum_error, bl);
  if (csum_error) {
    // Handles spurious read errors caused by a kernel bug.
    // We sometimes get all-zero pages as a result of the read under
    // high memory pressure. Retrying the failing read succeeds in most 
    // cases.
    // See also: http://tracker.ceph.com/issues/22464
    if (retry_count >= cct->_conf->bluestore_retry_disk_reads) {
      return -EIO;
    }
    return _do_read(c, o, offset, length, bl, op_flags, retry_count + 1);
  }
  r = bl.length();
  if (retry_count) {
    logger->inc(l_bluestore_reads_with_retries);
    dout(5) << __func__ << " read at 0x" << std::hex << offset << "~" << length
            << " failed " << std::dec << retry_count << " times before succeeding" << dendl;
    stringstream s;
    s << " reads with retries: " << logger->get(l_bluestore_reads_with_retries);
    _set_spurious_read_errors_alert(s.str());
  }
  return r;
}

int BlueStore::_verify_csum(OnodeRef& o,
			    const bluestore_blob_t* blob, uint64_t blob_xoffset,
			    const bufferlist& bl,
			    uint64_t logical_offset) const
{
  int bad;
  uint64_t bad_csum;
  auto start = mono_clock::now();
  int r = blob->verify_csum(blob_xoffset, bl, &bad, &bad_csum);
  if (cct->_conf->bluestore_debug_inject_csum_err_probability > 0 &&
      (rand() % 10000) < cct->_conf->bluestore_debug_inject_csum_err_probability * 10000.0) {
    derr << __func__ << " injecting bluestore checksum verifcation error" << dendl;
    bad = blob_xoffset;
    r = -1;
    bad_csum = 0xDEADBEEF;
  }
  if (r < 0) {
    if (r == -1) {
      PExtentVector pex;
      blob->map(
	bad,
	blob->get_csum_chunk_size(),
	[&](uint64_t offset, uint64_t length) {
	  pex.emplace_back(bluestore_pextent_t(offset, length));
          return 0;
	});
      derr << __func__ << " bad "
	   << Checksummer::get_csum_type_string(blob->csum_type)
	   << "/0x" << std::hex << blob->get_csum_chunk_size()
	   << " checksum at blob offset 0x" << bad
	   << ", got 0x" << bad_csum << ", expected 0x"
	   << blob->get_csum_item(bad / blob->get_csum_chunk_size()) << std::dec
	   << ", device location " << pex
	   << ", logical extent 0x" << std::hex
	   << (logical_offset + bad - blob_xoffset) << "~"
	   << blob->get_csum_chunk_size() << std::dec
	   << ", object " << o->oid
	   << dendl;
    } else {
      derr << __func__ << " failed with exit code: " << cpp_strerror(r) << dendl;
    }
  }
  log_latency(__func__,
    l_bluestore_csum_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  if (cct->_conf->bluestore_ignore_data_csum) {
    return 0;
  }
  return r;
}

int BlueStore::_decompress(bufferlist& source, bufferlist* result)
{
  int r = 0;
  auto start = mono_clock::now();
  auto i = source.cbegin();
  bluestore_compression_header_t chdr;
  decode(chdr, i);
  int alg = int(chdr.type);
  CompressorRef cp = compressor;
  if (!cp || (int)cp->get_type() != alg) {
    cp = Compressor::create(cct, alg);
  }

  if (!cp.get()) {
    // if compressor isn't available - error, because cannot return
    // decompressed data?
    
    const char* alg_name = Compressor::get_comp_alg_name(alg);
    derr << __func__ << " can't load decompressor " << alg_name << dendl;
    _set_compression_alert(false, alg_name);
    r = -EIO;
  } else {
    r = cp->decompress(i, chdr.length, *result, chdr.compressor_message);
    if (r < 0) {
      derr << __func__ << " decompression failed with exit code " << r << dendl;
      r = -EIO;
    }
  }
  log_latency(__func__,
    l_bluestore_decompress_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  return r;
}

// this stores fiemap into interval_set, other variations
// use it internally
int BlueStore::_fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  interval_set<uint64_t>& destset)
{
  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;
  {
    std::shared_lock l(c->lock);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      return -ENOENT;
    }
    _dump_onode<30>(cct, *o);

    dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	     << " size 0x" << o->onode.size << std::dec << dendl;

    boost::intrusive::set<Extent>::iterator ep, eend;
    if (offset >= o->onode.size)
      goto out;

    if (offset + length > o->onode.size) {
      length = o->onode.size - offset;
    }

    o->extent_map.fault_range(db, offset, length);
    eend = o->extent_map.extent_map.end();
    ep = o->extent_map.seek_lextent(offset);
    while (length > 0) {
      dout(20) << __func__ << " offset " << offset << dendl;
      if (ep != eend && ep->logical_offset + ep->length <= offset) {
        ++ep;
        continue;
      }

      uint64_t x_len = length;
      if (ep != eend && ep->logical_offset <= offset) {
        uint64_t x_off = offset - ep->logical_offset;
        x_len = std::min(x_len, ep->length - x_off);
        dout(30) << __func__ << " lextent 0x" << std::hex << offset << "~"
	         << x_len << std::dec << " blob " << ep->blob << dendl;
        destset.insert(offset, x_len);
        length -= x_len;
        offset += x_len;
        if (x_off + x_len == ep->length)
	  ++ep;
        continue;
      }
      if (ep != eend &&
	  ep->logical_offset > offset &&
	  ep->logical_offset - offset < x_len) {
        x_len = ep->logical_offset - offset;
      }
      offset += x_len;
      length -= x_len;
    }
  }

 out:
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " size = 0x(" << destset << ")" << std::dec << dendl;
  return 0;
}

int BlueStore::fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl)
{
  interval_set<uint64_t> m;
  int r = _fiemap(c_, oid, offset, length, m);
  if (r >= 0) {
    encode(m, bl);
  }
  return r;
}

int BlueStore::fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  map<uint64_t, uint64_t>& destmap)
{
  interval_set<uint64_t> m;
  int r = _fiemap(c_, oid, offset, length, m);
  if (r >= 0) {
    destmap = std::move(m).detach();
  }
  return r;
}

int BlueStore::readv(
  CollectionHandle &c_,
  const ghobject_t& oid,
  interval_set<uint64_t>& m,
  bufferlist& bl,
  uint32_t op_flags)
{
  auto start = mono_clock::now();
  Collection *c = static_cast<Collection *>(c_.get());
  const coll_t &cid = c->get_cid();
  dout(15) << __func__ << " " << cid << " " << oid
           << " fiemap " << m
           << dendl;
  if (!c->exists)
    return -ENOENT;

  bl.clear();
  int r;
  {
    std::shared_lock l(c->lock);
    auto start1 = mono_clock::now();
    OnodeRef o = c->get_onode(oid, false);
    log_latency("get_onode@read",
      l_bluestore_read_onode_meta_lat,
      mono_clock::now() - start1,
      cct->_conf->bluestore_log_op_age);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (m.empty()) {
      r = 0;
      goto out;
    }

    r = _do_readv(c, o, m, bl, op_flags);
    if (r == -EIO) {
      logger->inc(l_bluestore_read_eio);
    }
  }

 out:
  if (r >= 0 && _debug_data_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  } else if (oid.hobj.pool > 0 &&  /* FIXME, see #23029 */
             cct->_conf->bluestore_debug_random_read_err &&
             (rand() % (int)(cct->_conf->bluestore_debug_random_read_err *
                             100.0)) == 0) {
    dout(0) << __func__ << ": inject random EIO" << dendl;
    r = -EIO;
  }
  dout(10) << __func__ << " " << cid << " " << oid
           << " fiemap " << m << std::dec
           << " = " << r << dendl;
  log_latency(__func__,
    l_bluestore_read_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  return r;
}

int BlueStore::_do_readv(
  Collection *c,
  OnodeRef& o,
  const interval_set<uint64_t>& m,
  bufferlist& bl,
  uint32_t op_flags,
  uint64_t retry_count)
{
  FUNCTRACE(cct);
  int r = 0;
  int read_cache_policy = 0; // do not bypass clean or dirty cache

  dout(20) << __func__ << " fiemap " << m << std::hex
           << " size 0x" << o->onode.size << " (" << std::dec
           << o->onode.size << ")" << dendl;

  // generally, don't buffer anything, unless the client explicitly requests
  // it.
  bool buffered = false;
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered read" << dendl;
    buffered = true;
  } else if (cct->_conf->bluestore_default_buffered_read &&
             (op_flags & (CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
                          CEPH_OSD_OP_FLAG_FADVISE_NOCACHE)) == 0) {
    dout(20) << __func__ << " defaulting to buffered read" << dendl;
    buffered = true;
  }
  // this method must be idempotent since we may call it several times
  // before we finally read the expected result.
  bl.clear();

  // call fiemap first!
  ceph_assert(m.range_start() <= o->onode.size);
  ceph_assert(m.range_end() <= o->onode.size);
  auto start = mono_clock::now();
  o->extent_map.fault_range(db, m.range_start(), m.range_end() - m.range_start());
  log_latency(__func__,
    l_bluestore_read_onode_meta_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age,
    "", l_bluestore_slow_read_onode_meta_count);
  _dump_onode<30>(cct, *o);

  IOContext ioc(cct, NULL, !cct->_conf->bluestore_fail_eio);
  vector<std::tuple<ready_regions_t, vector<bufferlist>, blobs2read_t>> raw_results;
  raw_results.reserve(m.num_intervals());
  int i = 0;
  for (auto p = m.begin(); p != m.end(); p++, i++) {
    raw_results.push_back({});
    _read_cache(o, p.get_start(), p.get_len(), read_cache_policy,
                std::get<0>(raw_results[i]), std::get<2>(raw_results[i]));
    r = _prepare_read_ioc(std::get<2>(raw_results[i]), &std::get<1>(raw_results[i]), &ioc);
    // we always issue aio for reading, so errors other than EIO are not allowed
    if (r < 0)
      return r;
  }

  auto num_ios = m.size();
  if (ioc.has_pending_aios()) {
    num_ios = ioc.get_num_ios();
    bdev->aio_submit(&ioc);
    dout(20) << __func__ << " waiting for aio" << dendl;
    ioc.aio_wait();
    r = ioc.get_return_value();
    if (r < 0) {
      ceph_assert(r == -EIO); // no other errors allowed
      return -EIO;
    }
  }
  log_latency_fn(__func__,
    l_bluestore_read_wait_aio_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age,
    [&](auto lat) { return ", num_ios = " + stringify(num_ios); },
    l_bluestore_slow_read_wait_aio_count
  );

  ceph_assert(raw_results.size() == (size_t)m.num_intervals());
  i = 0;
  for (auto p = m.begin(); p != m.end(); p++, i++) {
    bool csum_error = false;
    bufferlist t;
    r = _generate_read_result_bl(o, p.get_start(), p.get_len(),
                                 std::get<0>(raw_results[i]),
                                 std::get<1>(raw_results[i]),
                                 std::get<2>(raw_results[i]),
                                 buffered, &csum_error, t);
    if (csum_error) {
      // Handles spurious read errors caused by a kernel bug.
      // We sometimes get all-zero pages as a result of the read under
      // high memory pressure. Retrying the failing read succeeds in most
      // cases.
      // See also: http://tracker.ceph.com/issues/22464
      if (retry_count >= cct->_conf->bluestore_retry_disk_reads) {
        return -EIO;
      }
      return _do_readv(c, o, m, bl, op_flags, retry_count + 1);
    }
    bl.claim_append(t);
  }
  if (retry_count) {
    logger->inc(l_bluestore_reads_with_retries);
    dout(5) << __func__ << " read fiemap " << m
            << " failed " << retry_count << " times before succeeding"
            << dendl;
  }
  return bl.length();
}

int BlueStore::dump_onode(CollectionHandle &c_,
  const ghobject_t& oid,
  const string& section_name,
  Formatter *f)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  int r;
  {
    std::shared_lock l(c->lock);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }
    // FIXME minor: actually the next line isn't enough to
    // load shared blobs. Leaving as is for now..
    //
    o->extent_map.fault_range(db, 0, OBJECT_MAX_SIZE);

    _dump_onode<0>(cct, *o);
    f->open_object_section(section_name.c_str());
    o->dump(f);
    f->close_section();
    r = 0;
  }
 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int BlueStore::getattr(
  CollectionHandle &c_,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;

  int r;
  {
    std::shared_lock l(c->lock);
    mempool::bluestore_cache_meta::string k(name);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (!o->onode.attrs.count(k)) {
      r = -ENODATA;
      goto out;
    }
    value = o->onode.attrs[k];
    r = 0;
  }
 out:
  if (r == 0 && _debug_mdata_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  }
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int BlueStore::getattrs(
  CollectionHandle &c_,
  const ghobject_t& oid,
  map<string,bufferptr,less<>>& aset)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  int r;
  {
    std::shared_lock l(c->lock);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }
    for (auto& i : o->onode.attrs) {
      aset.emplace(i.first.c_str(), i.second);
    }
    r = 0;
  }

 out:
  if (r == 0 && _debug_mdata_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  }
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int BlueStore::list_collections(vector<coll_t>& ls)
{
  std::shared_lock l(coll_lock);
  ls.reserve(coll_map.size());
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool BlueStore::collection_exists(const coll_t& c)
{
  std::shared_lock l(coll_lock);
  return coll_map.count(c);
}

int BlueStore::collection_empty(CollectionHandle& ch, bool *empty)
{
  dout(15) << __func__ << " " << ch->cid << dendl;
  vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list(ch, ghobject_t(), ghobject_t::get_max(), 1,
			  &ls, &next);
  if (r < 0) {
    derr << __func__ << " collection_list returned: " << cpp_strerror(r)
         << dendl;
    return r;
  }
  *empty = ls.empty();
  dout(10) << __func__ << " " << ch->cid << " = " << (int)(*empty) << dendl;
  return 0;
}

int BlueStore::collection_bits(CollectionHandle& ch)
{
  dout(15) << __func__ << " " << ch->cid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l(c->lock);
  dout(10) << __func__ << " " << ch->cid << " = " << c->cnode.bits << dendl;
  return c->cnode.bits;
}

int BlueStore::collection_list(
  CollectionHandle &c_, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  Collection *c = static_cast<Collection *>(c_.get());
  c->flush();
  dout(15) << __func__ << " " << c->cid
           << " start " << start << " end " << end << " max " << max << dendl;
  int r;
  {
    std::shared_lock l(c->lock);
    r = _collection_list(c, start, end, max, false, ls, pnext);
  }

  dout(10) << __func__ << " " << c->cid
    << " start " << start << " end " << end << " max " << max
    << " = " << r << ", ls.size() = " << ls->size()
    << ", next = " << (pnext ? *pnext : ghobject_t())  << dendl;
  return r;
}

int BlueStore::collection_list_legacy(
  CollectionHandle &c_, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  Collection *c = static_cast<Collection *>(c_.get());
  c->flush();
  dout(15) << __func__ << " " << c->cid
           << " start " << start << " end " << end << " max " << max << dendl;
  int r;
  {
    std::shared_lock l(c->lock);
    r = _collection_list(c, start, end, max, true, ls, pnext);
  }

  dout(10) << __func__ << " " << c->cid
    << " start " << start << " end " << end << " max " << max
    << " = " << r << ", ls.size() = " << ls->size()
    << ", next = " << (pnext ? *pnext : ghobject_t())  << dendl;
  return r;
}

int BlueStore::_collection_list(
  Collection *c, const ghobject_t& start, const ghobject_t& end, int max,
  bool legacy, vector<ghobject_t> *ls, ghobject_t *pnext)
{

  if (!c->exists)
    return -ENOENT;

  ghobject_t static_next;
  std::unique_ptr<CollectionListIterator> it;
  ghobject_t coll_range_temp_start, coll_range_temp_end;
  ghobject_t coll_range_start, coll_range_end;
  ghobject_t pend;
  bool temp;

  if (!pnext)
    pnext = &static_next;

  auto log_latency = make_scope_guard(
    [&, start_time = mono_clock::now(), func_name = __func__] {
    log_latency_fn(
      func_name,
      l_bluestore_clist_lat,
      mono_clock::now() - start_time,
      cct->_conf->bluestore_log_collection_list_age,
      [&](const ceph::timespan& lat) {
	ostringstream ostr;
	ostr << ", lat = " << timespan_str(lat)
	     << " cid =" << c->cid
	     << " start " << start << " end " << end
	     << " max " << max;
	return ostr.str();
      });
  });

  if (start.is_max() || start.hobj.is_max()) {
    *pnext = ghobject_t::get_max();
    return 0;
  }
  get_coll_range(c->cid, c->cnode.bits, &coll_range_temp_start,
                 &coll_range_temp_end, &coll_range_start, &coll_range_end, legacy);
  dout(20) << __func__
    << " range " << coll_range_temp_start
    << " to " << coll_range_temp_end
    << " and " << coll_range_start
    << " to " << coll_range_end
    << " start " << start << dendl;
  if (legacy) {
    it = std::make_unique<SimpleCollectionListIterator>(
      cct, db->get_iterator(PREFIX_OBJ));
  } else {
    it = std::make_unique<SortedCollectionListIterator>(
      db->get_iterator(PREFIX_OBJ));
  }
  if (start == ghobject_t() ||
    start.hobj == hobject_t() ||
    start == c->cid.get_min_hobj()) {
    it->upper_bound(coll_range_temp_start);
    temp = true;
  } else {
    if (start.hobj.is_temp()) {
      temp = true;
      ceph_assert(start >= coll_range_temp_start && start < coll_range_temp_end);
    } else {
      temp = false;
      ceph_assert(start >= coll_range_start && start < coll_range_end);
    }
    dout(20) << __func__ << " temp=" << (int)temp << dendl;
    it->lower_bound(start);
  }
  if (end.hobj.is_max()) {
    pend = temp ? coll_range_temp_end : coll_range_end;
  } else {
    if (end.hobj.is_temp()) {
      if (temp) {
        pend = end;
      } else {
        *pnext = ghobject_t::get_max();
        return 0;
      }
    } else {
      pend = temp ? coll_range_temp_end : end;
    }
  }
  dout(20) << __func__ << " pend " << pend << dendl;
  while (true) {
    if (!it->valid() || it->is_ge(pend)) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " oid " << it->oid() << " >= " << pend << dendl;
      if (temp) {
	if (end.hobj.is_temp()) {
          if (it->valid() && it->is_lt(coll_range_temp_end)) {
            *pnext = it->oid();
            return 0;
          }
	  break;
	}
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(coll_range_start);
        if (end.hobj.is_max())
          pend = coll_range_end;
        else
          pend = end;
	dout(30) << __func__ << " pend " << pend << dendl;
	continue;
      }
      if (it->valid() && it->is_lt(coll_range_end)) {
        *pnext = it->oid();
        return 0;
      }
      break;
    }
    dout(20) << __func__ << " oid " << it->oid() << " end " << end << dendl;
    if (ls->size() >= (unsigned)max) {
      dout(20) << __func__ << " reached max " << max << dendl;
      *pnext = it->oid();
      return 0;
    }
    ls->push_back(it->oid());
    it->next();
  }
  *pnext = ghobject_t::get_max();
  return 0;
}

int BlueStore::omap_get(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  return _omap_get(c, oid, header, out);
}

int BlueStore::_omap_get(
  Collection *c,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _onode_omap_get(o, header, out);
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::_onode_omap_get(
  const OnodeRef &o,           ///< [in] Object containing omap
  bufferlist *header,          ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
)
{
  int r = 0;
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap())
    goto out;
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    string head, tail;
    o->get_omap_header(&head);
    o->get_omap_tail(&tail);
    KeyValueDB::Iterator it = db->get_iterator(prefix, 0, KeyValueDB::IteratorBounds{head, tail});
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() == head) {
        dout(30) << __func__ << "  got header" << dendl;
        *header = it->value();
      } else if (it->key() >= tail) {
        dout(30) << __func__ << "  reached tail" << dendl;
        break;
      } else {
        string user_key;
        o->decode_omap_key(it->key(), &user_key);
        dout(20) << __func__ << "  got " << pretty_binary_string(it->key())
          << " -> " << user_key << dendl;
        (*out)[user_key] = it->value();
      }
      it->next();
    }
  }
out:
  return r;
}

int BlueStore::omap_get_header(
  CollectionHandle &c_,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap())
    goto out;
  o->flush();
  {
    string head;
    o->get_omap_header(&head);
    if (db->get(o->get_omap_prefix(), head, header) >= 0) {
      dout(30) << __func__ << "  got header" << dendl;
    } else {
      dout(30) << __func__ << "  no header" << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::omap_get_keys(
  CollectionHandle &c_,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  auto start1 = mono_clock::now();
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap())
    goto out;
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    string head, tail;
    o->get_omap_key(string(), &head);
    o->get_omap_tail(&tail);
    KeyValueDB::Iterator it = db->get_iterator(prefix, 0, KeyValueDB::IteratorBounds{head, tail});
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      }
      string user_key;
      o->decode_omap_key(it->key(), &user_key);
      dout(20) << __func__ << "  got " << pretty_binary_string(it->key())
	       << " -> " << user_key << dendl;
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  c->store->log_latency(
    __func__,
    l_bluestore_omap_get_keys_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::omap_get_values(
  CollectionHandle &c_,        ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  int r = 0;
  string final_key;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
      final_key.resize(base_key_len); // keep prefix
      final_key += *p;
      bufferlist val;
      if (db->get(prefix, final_key, &val) >= 0) {
	dout(30) << __func__ << "  got " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
	out->insert(make_pair(*p, val));
      }
    }
  }
 out:
  c->store->log_latency(
    __func__,
    l_bluestore_omap_get_values_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

#ifdef WITH_SEASTAR
int BlueStore::omap_get_values(
  CollectionHandle &c_,        ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const std::optional<string> &start_after,     ///< [in] Keys to get
  map<string, bufferlist> *output ///< [out] Returned keys and values
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    ObjectMap::ObjectMapIterator iter = get_omap_iterator(c_, oid);
    if (!iter) {
      r = -ENOENT;
      goto out;
    }
    if (start_after) {
      iter->upper_bound(*start_after);
    } else {
      iter->seek_to_first();
    }
    for (; iter->valid(); iter->next()) {
      output->insert(make_pair(iter->key(), iter->value()));
    }
  }

out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
          << dendl;
  return r;
}
#endif

int BlueStore::omap_check_keys(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  string final_key;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
      final_key.resize(base_key_len); // keep prefix
      final_key += *p;
      bufferlist val;
      if (db->get(prefix, final_key, &val) >= 0) {
	dout(30) << __func__ << "  have " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
	out->insert(*p);
      } else {
	dout(30) << __func__ << "  miss " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
      }
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

ObjectMap::ObjectMapIterator BlueStore::get_omap_iterator(
  CollectionHandle &c_,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists) {
    return ObjectMap::ObjectMapIterator();
  }
  std::shared_lock l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  o->flush();
  dout(10) << __func__ << " has_omap = " << (int)o->onode.has_omap() <<dendl;
  auto bounds = KeyValueDB::IteratorBounds();
  if (o->onode.has_omap()) {
    std::string lower_bound, upper_bound;
    o->get_omap_key(string(), &lower_bound);
    o->get_omap_tail(&upper_bound);
    bounds.lower_bound = std::move(lower_bound);
    bounds.upper_bound = std::move(upper_bound);
  }
  KeyValueDB::Iterator it = db->get_iterator(o->get_omap_prefix(), 0, std::move(bounds));
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(logger,c, o, it));
}

// -----------------
// write helpers

uint64_t BlueStore::_get_ondisk_reserved() const {
  ceph_assert(min_alloc_size);
  return round_up_to(
    std::max<uint64_t>(DB_SUPER_RESERVED, min_alloc_size), min_alloc_size);
}

void BlueStore::_prepare_ondisk_format_super(KeyValueDB::Transaction& t)
{
  dout(10) << __func__ << " ondisk_format " << ondisk_format
	   << " min_compat_ondisk_format " << min_compat_ondisk_format
	   << dendl;
  ceph_assert(ondisk_format == latest_ondisk_format);
  {
    bufferlist bl;
    encode(ondisk_format, bl);
    t->set(PREFIX_SUPER, "ondisk_format", bl);
  }
  {
    bufferlist bl;
    encode(min_compat_ondisk_format, bl);
    t->set(PREFIX_SUPER, "min_compat_ondisk_format", bl);
  }
}

int BlueStore::_open_super_meta()
{
  // nid
  {
    nid_max = 0;
    bufferlist bl;
    db->get(PREFIX_SUPER, "nid_max", &bl);
    auto p = bl.cbegin();
    try {
      uint64_t v;
      decode(v, p);
      nid_max = v;
    } catch (ceph::buffer::error& e) {
      derr << __func__ << " unable to read nid_max" << dendl;
      return -EIO;
    }
    dout(1) << __func__ << " old nid_max " << nid_max << dendl;
    nid_last = nid_max.load();
  }

  // blobid
  {
    blobid_max = 0;
    bufferlist bl;
    db->get(PREFIX_SUPER, "blobid_max", &bl);
    auto p = bl.cbegin();
    try {
      uint64_t v;
      decode(v, p);
      blobid_max = v;
    } catch (ceph::buffer::error& e) {
      derr << __func__ << " unable to read blobid_max" << dendl;
      return -EIO;
    }
    dout(1) << __func__ << " old blobid_max " << blobid_max << dendl;
    blobid_last = blobid_max.load();
  }

  // freelist
  {
    bufferlist bl;
    db->get(PREFIX_SUPER, "freelist_type", &bl);
    if (bl.length()) {
      freelist_type = std::string(bl.c_str(), bl.length());
    } else {
      ceph_abort_msg("Not Support extent freelist manager");
    }
    dout(5) << __func__ << "::NCB::freelist_type=" << freelist_type << dendl;
  }
  // ondisk format
  int32_t compat_ondisk_format = 0;
  {
    bufferlist bl;
    int r = db->get(PREFIX_SUPER, "ondisk_format", &bl);
    if (r < 0) {
      // base case: kraken bluestore is v1 and readable by v1
      dout(20) << __func__ << " missing ondisk_format; assuming kraken"
	       << dendl;
      ondisk_format = 1;
      compat_ondisk_format = 1;
    } else {
      auto p = bl.cbegin();
      try {
	decode(ondisk_format, p);
      } catch (ceph::buffer::error& e) {
	derr << __func__ << " unable to read ondisk_format" << dendl;
	return -EIO;
      }
      bl.clear();
      {
	r = db->get(PREFIX_SUPER, "min_compat_ondisk_format", &bl);
	ceph_assert(!r);
	auto p = bl.cbegin();
	try {
	  decode(compat_ondisk_format, p);
	} catch (ceph::buffer::error& e) {
	  derr << __func__ << " unable to read compat_ondisk_format" << dendl;
	  return -EIO;
	}
      }
    }
    dout(1) << __func__ << " ondisk_format " << ondisk_format
	     << " compat_ondisk_format " << compat_ondisk_format
	     << dendl;
  }

  if (latest_ondisk_format < compat_ondisk_format) {
    derr << __func__ << " compat_ondisk_format is "
	 << compat_ondisk_format << " but we only understand version "
	 << latest_ondisk_format << dendl;
    return -EPERM;
  }

  {
    bufferlist bl;
    db->get(PREFIX_SUPER, "min_alloc_size", &bl);
    auto p = bl.cbegin();
    try {
      uint64_t val;
      decode(val, p);
      min_alloc_size = val;
      min_alloc_size_order = std::countr_zero(val);
      min_alloc_size_mask  = min_alloc_size - 1;

      ceph_assert(min_alloc_size == 1u << min_alloc_size_order);
    } catch (ceph::buffer::error& e) {
      derr << __func__ << " unable to read min_alloc_size" << dendl;
      return -EIO;
    }
    dout(1) << __func__ << " min_alloc_size 0x" << std::hex << min_alloc_size
	     << std::dec << dendl;
    logger->set(l_bluestore_alloc_unit, min_alloc_size);
  }

  _set_per_pool_omap();

  _open_statfs();
  _set_alloc_sizes();
  _set_throttle_params();

  _set_csum();
  _set_compression();
  _set_blob_size();

  _validate_bdev();
  return 0;
}

int BlueStore::_upgrade_super()
{
  dout(1) << __func__ << " from " << ondisk_format << ", latest "
	  << latest_ondisk_format << dendl;
  if (ondisk_format < latest_ondisk_format) {
    ceph_assert(ondisk_format > 0);
    ceph_assert(ondisk_format < latest_ondisk_format);

    KeyValueDB::Transaction t = db->get_transaction();
    if (ondisk_format == 1) {
      // changes:
      // - super: added ondisk_format
      // - super: added min_readable_ondisk_format
      // - super: added min_compat_ondisk_format
      // - super: added min_alloc_size
      // - super: removed min_min_alloc_size
      {
	bufferlist bl;
	db->get(PREFIX_SUPER, "min_min_alloc_size", &bl);
	auto p = bl.cbegin();
	try {
	  uint64_t val;
	  decode(val, p);
	  min_alloc_size = val;
	} catch (ceph::buffer::error& e) {
	  derr << __func__ << " failed to read min_min_alloc_size" << dendl;
	  return -EIO;
	}
	t->set(PREFIX_SUPER, "min_alloc_size", bl);
	t->rmkey(PREFIX_SUPER, "min_min_alloc_size");
      }
      ondisk_format = 2;
    }
    if (ondisk_format == 2) {
      // changes:
      // - onode has FLAG_PERPOOL_OMAP.  Note that we do not know that *all*
      //   oondes are using the per-pool prefix until a repair is run; at that
      //   point the per_pool_omap=1 key will be set.
      // - super: added per_pool_omap key, which indicates that *all* objects
      //   are using the new prefix and key format
      ondisk_format = 3;
    }
    if (ondisk_format == 3) {
      // changes:
      // - FreelistManager keeps meta within bdev label
      int r = _write_out_fm_meta(0);
      ceph_assert(r == 0);
      ondisk_format = 4;
    }
    // This to be the last operation
    _prepare_ondisk_format_super(t);
    int r = db->submit_transaction_sync(t);
    ceph_assert(r == 0);
  }
  // done
  dout(1) << __func__ << " done" << dendl;
  return 0;
}

void BlueStore::_assign_nid(TransContext *txc, OnodeRef& o)
{
  if (o->onode.nid) {
    ceph_assert(o->exists);
    return;
  }
  uint64_t nid = ++nid_last;
  dout(20) << __func__ << " " << nid << dendl;
  o->onode.nid = nid;
  txc->last_nid = nid;
  o->exists = true;
}

uint64_t BlueStore::_assign_blobid(TransContext *txc)
{
  uint64_t bid = ++blobid_last;
  dout(20) << __func__ << " " << bid << dendl;
  txc->last_blobid = bid;
  return bid;
}

void BlueStore::get_db_statistics(Formatter *f)
{
  db->get_statistics(f);
}

BlueStore::TransContext *BlueStore::_txc_create(
  Collection *c, OpSequencer *osr,
  list<Context*> *on_commits,
  TrackedOpRef osd_op)
{
  TransContext *txc = new TransContext(cct, c, osr, on_commits);
  txc->t = db->get_transaction();

#ifdef WITH_BLKIN
  if (osd_op && osd_op->pg_trace) {
    txc->trace.init("TransContext", &trace_endpoint,
                    &osd_op->pg_trace);
    txc->trace.event("txc create");
    txc->trace.keyval("txc seq", txc->seq);
  }
#endif

  osr->queue_new(txc);
  dout(20) << __func__ << " osr " << osr << " = " << txc
	   << " seq " << txc->seq << dendl;
  return txc;
}

void BlueStore::_txc_calc_cost(TransContext *txc)
{
  // one "io" for the kv commit
  auto ios = 1 + txc->ioc.get_num_ios();
  auto cost = throttle_cost_per_io.load();
  txc->cost = ios * cost + txc->bytes;
  txc->ios = ios;
  dout(10) << __func__ << " " << txc << " cost " << txc->cost << " ("
	   << ios << " ios * " << cost << " + " << txc->bytes
	   << " bytes)" << dendl;
}

void BlueStore::_txc_update_store_statfs(TransContext *txc)
{
  if (txc->statfs_delta.is_empty())
    return;

  logger->inc(l_bluestore_allocated, txc->statfs_delta.allocated());
  logger->inc(l_bluestore_stored, txc->statfs_delta.stored());
  logger->inc(l_bluestore_compressed, txc->statfs_delta.compressed());
  logger->inc(l_bluestore_compressed_allocated, txc->statfs_delta.compressed_allocated());
  logger->inc(l_bluestore_compressed_original, txc->statfs_delta.compressed_original());

  if (per_pool_stat_collection) {
    if (!is_statfs_recoverable()) {
      bufferlist bl;
      txc->statfs_delta.encode(bl);
      string key;
      get_pool_stat_key(txc->osd_pool_id, &key);
      txc->t->merge(PREFIX_STAT, key, bl);
    }

    std::lock_guard l(vstatfs_lock);
    auto& stats = osd_pools[txc->osd_pool_id];
    stats += txc->statfs_delta;
    
    vstatfs += txc->statfs_delta; //non-persistent in this mode

  } else {
    if (!is_statfs_recoverable()) {
      bufferlist bl;
      txc->statfs_delta.encode(bl);
      txc->t->merge(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, bl);
    }

    std::lock_guard l(vstatfs_lock);
    vstatfs += txc->statfs_delta;
  } 
  txc->statfs_delta.reset();
}

void BlueStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    dout(10) << __func__ << " txc " << txc
	     << " " << txc->get_state_name() << dendl;
    switch (txc->get_state()) {
    case TransContext::STATE_PREPARE:
      throttle.log_state_latency(*txc, logger, l_bluestore_state_prepare_lat);
      if (txc->ioc.has_pending_aios()) {
	txc->set_state(TransContext::STATE_AIO_WAIT);
#ifdef WITH_BLKIN
        if (txc->trace) {
          txc->trace.keyval("pending aios", txc->ioc.num_pending.load());
        }
#endif
	txc->had_ios = true;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_AIO_WAIT:
      {
	mono_clock::duration lat = throttle.log_state_latency(
	  *txc, logger, l_bluestore_state_aio_wait_lat);
	if (ceph::to_seconds<double>(lat) >= cct->_conf->bluestore_log_op_age) {
	  logger->inc(l_bluestore_slow_aio_wait_count);
	  dout(0) << __func__ << " slow aio_wait, txc = " << txc
		  << ", latency = " << lat
		  << dendl;
	}
      }

      _txc_finish_io(txc);  // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      ceph_assert(ceph_mutex_is_locked(txc->osr->qlock));  // see _txc_finish_io
      if (txc->had_ios) {
	++txc->osr->txc_with_unstable_io;
      }
      throttle.log_state_latency(*txc, logger, l_bluestore_state_io_done_lat);
      txc->set_state(TransContext::STATE_KV_QUEUED);
      if (cct->_conf->bluestore_sync_submit_transaction) {
	if (txc->last_nid >= nid_max ||
	    txc->last_blobid >= blobid_max) {
	  dout(20) << __func__
		   << " last_{nid,blobid} exceeds max, submit via kv thread"
		   << dendl;
	} else if (txc->osr->kv_committing_serially) {
	  dout(20) << __func__ << " prior txc submitted via kv thread, us too"
		   << dendl;
	  // note: this is starvation-prone.  once we have a txc in a busy
	  // sequencer that is committing serially it is possible to keep
	  // submitting new transactions fast enough that we get stuck doing
	  // so.  the alternative is to block here... fixme?
	} else if (txc->osr->txc_with_unstable_io) {
	  dout(20) << __func__ << " prior txc(s) with unstable ios "
		   << txc->osr->txc_with_unstable_io.load() << dendl;
	} else if (cct->_conf->bluestore_debug_randomize_serial_transaction &&
		   rand() % cct->_conf->bluestore_debug_randomize_serial_transaction
		   == 0) {
	  dout(20) << __func__ << " DEBUG randomly forcing submit via kv thread"
		   << dendl;
	} else {
	  _txc_apply_kv(txc, true);
	}
      }
      {
	std::lock_guard l(kv_lock);
	kv_queue.push_back(txc);
	if (!kv_sync_in_progress) {
	  kv_sync_in_progress = true;
	  kv_cond.notify_one();
	}
	if (txc->get_state() != TransContext::STATE_KV_SUBMITTED) {
	  kv_queue_unsubmitted.push_back(txc);
	  ++txc->osr->kv_committing_serially;
	}
	if (txc->had_ios)
	  kv_ios++;
	kv_throttle_costs += txc->cost;
      }
      return;
    case TransContext::STATE_KV_SUBMITTED:
      _txc_committed_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
      throttle.log_state_latency(*txc, logger, l_bluestore_state_kv_done_lat);
      if (txc->deferred_txn) {
	txc->set_state(TransContext::STATE_DEFERRED_QUEUED);
	_deferred_queue(txc);
	return;
      }
      txc->set_state(TransContext::STATE_FINISHING);
      break;

    case TransContext::STATE_DEFERRED_CLEANUP:
      throttle.log_state_latency(*txc, logger, l_bluestore_state_deferred_cleanup_lat);
      txc->set_state(TransContext::STATE_FINISHING);
      // ** fall-thru **

    case TransContext::STATE_FINISHING:
      throttle.log_state_latency(*txc, logger, l_bluestore_state_finishing_lat);
      _txc_finish(txc);
      return;

    default:
      derr << __func__ << " unexpected txc " << txc
	   << " state " << txc->get_state_name() << dendl;
      ceph_abort_msg("unexpected txc state");
      return;
    }
  }
}

void BlueStore::_txc_finish_io(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << dendl;

  /*
   * we need to preserve the order of kv transactions,
   * even though aio will complete in any order.
   */

  OpSequencer *osr = txc->osr.get();
  std::lock_guard l(osr->qlock);
  txc->set_state(TransContext::STATE_IO_DONE);
  txc->ioc.release_running_aios();
  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin()) {
    --p;
    if (p->get_state() < TransContext::STATE_IO_DONE) {
      dout(20) << __func__ << " " << txc << " blocked by " << &*p << " "
	       << p->get_state_name() << dendl;
      return;
    }
    if (p->get_state() > TransContext::STATE_IO_DONE) {
      ++p;
      break;
    }
  }
  do {
    _txc_state_proc(&*p++);
  } while (p != osr->q.end() &&
	   p->get_state() == TransContext::STATE_IO_DONE);

  if (osr->kv_submitted_waiters) {
    osr->qcond.notify_all();
  }
}

void BlueStore::_txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t)
{
  dout(20) << __func__ << " txc " << txc
	   << " onodes " << txc->onodes
	   << " shared_blobs " << txc->shared_blobs
	   << dendl;

  // finalize onodes
  for (auto o : txc->onodes) {
    _record_onode(o, t);
    o->flushing_count++;
  }

  // objects we modified but didn't affect the onode
  auto p = txc->modified_objects.begin();
  while (p != txc->modified_objects.end()) {
    if (txc->onodes.count(*p) == 0) {
      (*p)->flushing_count++;
      ++p;
    } else {
      // remove dups with onodes list to avoid problems in _txc_finish
      p = txc->modified_objects.erase(p);
    }
  }

  // finalize shared_blobs
  for (auto sb : txc->shared_blobs) {
    string key;
    auto sbid = sb->get_sbid();
    get_shared_blob_key(sbid, &key);
    if (sb->persistent->empty()) {
      dout(20) << __func__ << " shared_blob 0x"
               << std::hex << sbid << std::dec
	       << " is empty" << dendl;
      t->rmkey(PREFIX_SHARED_BLOB, key);
    } else {
      bufferlist bl;
      encode(*(sb->persistent), bl);
      dout(20) << __func__ << " shared_blob 0x"
               << std::hex << sbid << std::dec
	       << " is " << bl.length() << " " << *sb << dendl;
      t->set(PREFIX_SHARED_BLOB, key, bl);
    }
  }
}

void BlueStore::BSPerfTracker::update_from_perfcounters(
  PerfCounters &logger)
{
  os_commit_latency_ns.consume_next(
    logger.get_tavg_ns(
      l_bluestore_commit_lat));
  os_apply_latency_ns.consume_next(
    logger.get_tavg_ns(
      l_bluestore_commit_lat));
}

void BlueStore::_txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t)
{
  dout(20) << __func__ << " txc " << txc << std::hex
	   << " allocated 0x" << txc->allocated
	   << " released 0x" << txc->released
	   << std::dec << dendl;

  if (!fm->is_null_manager())
  {
    // We have to handle the case where we allocate *and* deallocate the
    // same region in this transaction.  The freelist doesn't like that.
    // (Actually, the only thing that cares is the BitmapFreelistManager
    // debug check. But that's important.)
    interval_set<uint64_t> tmp_allocated, tmp_released;
    interval_set<uint64_t> *pallocated = &txc->allocated;
    interval_set<uint64_t> *preleased = &txc->released;
    if (!txc->allocated.empty() && !txc->released.empty()) {
      interval_set<uint64_t> overlap;
      overlap.intersection_of(txc->allocated, txc->released);
      if (!overlap.empty()) {
	tmp_allocated = txc->allocated;
	tmp_allocated.subtract(overlap);
	tmp_released = txc->released;
	tmp_released.subtract(overlap);
	dout(20) << __func__ << "  overlap 0x" << std::hex << overlap
		 << ", new allocated 0x" << tmp_allocated
		 << " released 0x" << tmp_released << std::dec
		 << dendl;
	pallocated = &tmp_allocated;
	preleased = &tmp_released;
      }
    }

    // update freelist with non-overlap sets
    for (interval_set<uint64_t>::iterator p = pallocated->begin();
	 p != pallocated->end();
	 ++p) {
      fm->allocate(p.get_start(), p.get_len(), t);
    }
    for (interval_set<uint64_t>::iterator p = preleased->begin();
	 p != preleased->end();
	 ++p) {
      dout(20) << __func__ << " release 0x" << std::hex << p.get_start()
	       << "~" << p.get_len() << std::dec << dendl;
      fm->release(p.get_start(), p.get_len(), t);
    }
  }

  _txc_update_store_statfs(txc);
}

void BlueStore::_txc_apply_kv(TransContext *txc, bool sync_submit_transaction)
{
  ceph_assert(txc->get_state() == TransContext::STATE_KV_QUEUED);
  {
#if defined(WITH_LTTNG)
    auto start = mono_clock::now();
#endif

#ifdef WITH_BLKIN
    if (txc->trace) {
      txc->trace.event("db async submit");
    }
#endif

    int r = cct->_conf->bluestore_debug_omit_kv_commit ? 0 : db->submit_transaction(txc->t);
    ceph_assert(r == 0);
    txc->set_state(TransContext::STATE_KV_SUBMITTED);
    if (txc->osr->kv_submitted_waiters) {
      std::lock_guard l(txc->osr->qlock);
      txc->osr->qcond.notify_all();
    }

#if defined(WITH_LTTNG)
    if (txc->tracing) {
      tracepoint(
	bluestore,
	transaction_kv_submit_latency,
	txc->osr->get_sequencer_id(),
	txc->seq,
	sync_submit_transaction,
	ceph::to_seconds<double>(mono_clock::now() - start));
    }
#endif
  }

  for (auto ls : { &txc->onodes, &txc->modified_objects }) {
    for (auto& o : *ls) {
      dout(20) << __func__ << " onode " << o << " had " << o->flushing_count
	       << dendl;
      if (--o->flushing_count == 0 && o->waiting_count.load()) {
        std::lock_guard l(o->flush_lock);
	o->flush_cond.notify_all();
      }
    }
  }
}

void BlueStore::_txc_committed_kv(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  throttle.complete_kv(*txc);
  {
    std::lock_guard l(txc->osr->qlock);
    txc->set_state(TransContext::STATE_KV_DONE);
    if (txc->ch->commit_queue) {
      txc->ch->commit_queue->queue(txc->oncommits);
    } else {
      finisher.queue(txc->oncommits);
    }
  }
  throttle.log_state_latency(*txc, logger, l_bluestore_state_kv_committing_lat);
  log_latency_fn(
    __func__,
    l_bluestore_commit_lat,
    mono_clock::now() - txc->start,
    cct->_conf->bluestore_log_op_age,
    [&](auto lat) {
      return ", txc = " + stringify(txc);
    },
    l_bluestore_slow_committed_kv_count
  );
}

void BlueStore::_txc_finish(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
  ceph_assert(txc->get_state() == TransContext::STATE_FINISHING);

  for (auto& sb : txc->blobs_written) {
    sb->finish_write(txc->seq);
  }
  txc->blobs_written.clear();
  while (!txc->removed_collections.empty()) {
    _queue_reap_collection(txc->removed_collections.front());
    txc->removed_collections.pop_front();
  }

  OpSequencerRef osr = txc->osr;
  bool empty = false;
  bool submit_deferred = false;
  OpSequencer::q_list_t releasing_txc;
  {
    std::lock_guard l(osr->qlock);
    txc->set_state(TransContext::STATE_DONE);
    bool notify = false;
    while (!osr->q.empty()) {
      TransContext *txc = &osr->q.front();
      dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	       << dendl;
      if (txc->get_state() != TransContext::STATE_DONE) {
	if (txc->get_state() == TransContext::STATE_PREPARE &&
	  deferred_aggressive) {
	  // for _osr_drain_preceding()
          notify = true;
	}
	if (txc->get_state() == TransContext::STATE_DEFERRED_QUEUED &&
	    osr->q.size() > g_conf()->bluestore_max_deferred_txc) {
	  submit_deferred = true;
	}
        break;
      }

      osr->q.pop_front();
      releasing_txc.push_back(*txc);
    }

    if (osr->q.empty()) {
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
      empty = true;
    }

    // only drain()/drain_preceding() need wakeup,
    // other cases use kv_submitted_waiters
    if (notify || empty) {
      osr->qcond.notify_all();
    }
  }

  while (!releasing_txc.empty()) {
    // release to allocator only after all preceding txc's have also
    // finished any deferred writes that potentially land in these
    // blocks
    auto txc = &releasing_txc.front();
    _txc_release_alloc(txc);
    releasing_txc.pop_front();
    throttle.log_state_latency(*txc, logger, l_bluestore_state_done_lat);
    throttle.complete(*txc);
    delete txc;
  }

  if (submit_deferred) {
    // we're pinning memory; flush!  we could be more fine-grained here but
    // i'm not sure it's worth the bother.
    deferred_try_submit();
  }

  if (empty && osr->zombie) {
    std::lock_guard l(zombie_osr_lock);
    if (zombie_osr_set.erase(osr->cid)) {
      dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
    } else {
      dout(10) << __func__ << " empty zombie osr " << osr << " already reaped"
	       << dendl;
    }
  }
}

void BlueStore::_txc_release_alloc(TransContext *txc)
{
  bool discard_queued = false;
  // it's expected we're called with lazy_release_lock already taken!
  if (unlikely(cct->_conf->bluestore_debug_no_reuse_blocks ||
               txc->released.size() == 0)) {
      goto out;
  }
  discard_queued = bdev->try_discard(txc->released);
  // if async discard succeeded, will do alloc->release when discard callback
  // else we should release here
  if (!discard_queued) {
      dout(10) << __func__ << "(sync) " << txc << " " << std::hex
               << txc->released << std::dec << dendl;
      alloc->release(txc->released);
  }

out:
  txc->allocated.clear();
  txc->released.clear();
}

void BlueStore::_osr_attach(Collection *c)
{
  // note: caller has coll_lock
  auto q = coll_map.find(c->cid);
  if (q != coll_map.end()) {
    c->osr = q->second->osr;
    ldout(cct, 10) << __func__ << " " << c->cid
		   << " reusing osr " << c->osr << " from existing coll "
		   << q->second << dendl;
  } else {
    std::lock_guard l(zombie_osr_lock);
    auto p = zombie_osr_set.find(c->cid);
    if (p == zombie_osr_set.end()) {
      c->osr = ceph::make_ref<OpSequencer>(this, next_sequencer_id++, c->cid);
      ldout(cct, 10) << __func__ << " " << c->cid
		     << " fresh osr " << c->osr << dendl;
    } else {
      c->osr = p->second;
      zombie_osr_set.erase(p);
      ldout(cct, 10) << __func__ << " " << c->cid
		     << " resurrecting zombie osr " << c->osr << dendl;
      c->osr->zombie = false;
    }
  }
}

void BlueStore::_osr_register_zombie(OpSequencer *osr)
{
  std::lock_guard l(zombie_osr_lock);
  dout(10) << __func__ << " " << osr << " " << osr->cid << dendl;
  osr->zombie = true;
  auto i = zombie_osr_set.emplace(osr->cid, osr);
  // this is either a new insertion or the same osr is already there
  ceph_assert(i.second || i.first->second == osr);
}

void BlueStore::_osr_drain_preceding(TransContext *txc)
{
  OpSequencer *osr = txc->osr.get();
  dout(10) << __func__ << " " << txc << " osr " << osr << dendl;
  ++deferred_aggressive; // FIXME: maybe osr-local aggressive flag?
  {
    // submit anything pending
    osr->deferred_lock.lock();
    if (osr->deferred_pending && !osr->deferred_running) {
      _deferred_submit_unlock(osr);
    } else {
      osr->deferred_lock.unlock();
    }
  }
  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    if (!kv_sync_in_progress) {
      kv_sync_in_progress = true;
      kv_cond.notify_one();
    }
  }
  osr->drain_preceding(txc);
  --deferred_aggressive;
  dout(10) << __func__ << " " << osr << " done" << dendl;
}

void BlueStore::_osr_drain(OpSequencer *osr)
{
  dout(10) << __func__ << " " << osr << dendl;
  ++deferred_aggressive; // FIXME: maybe osr-local aggressive flag?
  {
    // submit anything pending
    osr->deferred_lock.lock();
    if (osr->deferred_pending && !osr->deferred_running) {
      _deferred_submit_unlock(osr);
    } else {
      osr->deferred_lock.unlock();
    }
  }
  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    if (!kv_sync_in_progress) {
      kv_sync_in_progress = true;
      kv_cond.notify_one();
    }
  }
  osr->drain();
  --deferred_aggressive;
  dout(10) << __func__ << " " << osr << " done" << dendl;
}

void BlueStore::_osr_drain_all()
{
  dout(10) << __func__ << dendl;

  set<OpSequencerRef> s;
  vector<OpSequencerRef> zombies;
  {
    std::shared_lock l(coll_lock);
    for (auto& i : coll_map) {
      s.insert(i.second->osr);
    }
  }
  {
    std::lock_guard l(zombie_osr_lock);
    for (auto& i : zombie_osr_set) {
      s.insert(i.second);
      zombies.push_back(i.second);
    }
  }
  dout(20) << __func__ << " osr_set " << s << dendl;

  ++deferred_aggressive;
  {
    // submit anything pending
    deferred_try_submit();
  }
  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    kv_cond.notify_one();
  }
  {
    std::lock_guard l(kv_finalize_lock);
    kv_finalize_cond.notify_one();
  }
  for (auto osr : s) {
    dout(20) << __func__ << " drain " << osr << dendl;
    osr->drain();
  }
  --deferred_aggressive;

  {
    std::lock_guard l(zombie_osr_lock);
    for (auto& osr : zombies) {
      if (zombie_osr_set.erase(osr->cid)) {
	dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
	ceph_assert(osr->q.empty());
      } else if (osr->zombie) {
	dout(10) << __func__ << " empty zombie osr " << osr
		 << " already reaped" << dendl;
	ceph_assert(osr->q.empty());
      } else {
	dout(10) << __func__ << " empty zombie osr " << osr
		 << " resurrected" << dendl;
      }
    }
  }

  dout(10) << __func__ << " done" << dendl;
}


void BlueStore::_kv_start()
{
  dout(10) << __func__ << dendl;

  finisher.start();
  kv_sync_thread.create("bstore_kv_sync");
  kv_finalize_thread.create("bstore_kv_final");
}

void BlueStore::_kv_stop()
{
  dout(10) << __func__ << dendl;
  {
    std::unique_lock l{kv_lock};
    while (!kv_sync_started) {
      kv_cond.wait(l);
    }
    kv_stop = true;
    kv_cond.notify_all();
  }
  {
    std::unique_lock l{kv_finalize_lock};
    while (!kv_finalize_started) {
      kv_finalize_cond.wait(l);
    }
    kv_finalize_stop = true;
    kv_finalize_cond.notify_all();
  }
  kv_sync_thread.join();
  kv_finalize_thread.join();
  ceph_assert(removed_collections.empty());
  {
    std::lock_guard l(kv_lock);
    kv_stop = false;
  }
  {
    std::lock_guard l(kv_finalize_lock);
    kv_finalize_stop = false;
  }
  dout(10) << __func__ << " stopping finishers" << dendl;
  finisher.wait_for_empty();
  finisher.stop();
  dout(10) << __func__ << " stopped" << dendl;
}

void BlueStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  deque<DeferredBatch*> deferred_stable_queue; ///< deferred ios done + stable
  std::unique_lock l{kv_lock};
  ceph_assert(!kv_sync_started);
  kv_sync_started = true;
  kv_cond.notify_all();

  auto t0 = mono_clock::now();
  timespan twait = ceph::make_timespan(0);
  size_t kv_submitted = 0;

  while (true) {
    auto period = cct->_conf->bluestore_kv_sync_util_logging_s;
    auto observation_period =
      ceph::make_timespan(period);
    auto elapsed = mono_clock::now() - t0;
    if (period && elapsed >= observation_period) {
      dout(5) << __func__ << " utilization: idle "
	      << twait << " of " << elapsed
	      << ", submitted: " << kv_submitted
	      <<dendl;
      t0 = mono_clock::now();
      twait = ceph::make_timespan(0);
      kv_submitted = 0;
    }
    ceph_assert(kv_committing.empty());
    if (kv_queue.empty() &&
	((deferred_done_queue.empty() && deferred_stable_queue.empty()) ||
	 !deferred_aggressive)) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      auto t = mono_clock::now();
      kv_sync_in_progress = false;
      kv_cond.wait(l);
      twait += mono_clock::now() - t;

      dout(20) << __func__ << " wake" << dendl;
    } else {
      deque<TransContext*> kv_submitting;
      deque<DeferredBatch*> deferred_done, deferred_stable;
      uint64_t aios = 0, costs = 0;

      dout(20) << __func__ << " committing " << kv_queue.size()
	       << " submitting " << kv_queue_unsubmitted.size()
	       << " deferred done " << deferred_done_queue.size()
	       << " stable " << deferred_stable_queue.size()
	       << dendl;
      kv_committing.swap(kv_queue);
      kv_submitting.swap(kv_queue_unsubmitted);
      deferred_done.swap(deferred_done_queue);
      deferred_stable.swap(deferred_stable_queue);
      aios = kv_ios;
      costs = kv_throttle_costs;
      kv_ios = 0;
      kv_throttle_costs = 0;
      l.unlock();

      dout(30) << __func__ << " committing " << kv_committing << dendl;
      dout(30) << __func__ << " submitting " << kv_submitting << dendl;
      dout(30) << __func__ << " deferred_done " << deferred_done << dendl;
      dout(30) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      bool force_flush = false;
      // if bluefs is sharing the same device as data (only), then we
      // can rely on the bluefs commit to flush the device and make
      // deferred aios stable.  that means that if we do have done deferred
      // txcs AND we are not on a single device, we need to force a flush.
      if (bluefs && bluefs_layout.single_shared_device()) {
	if (aios) {
	  force_flush = true;
	} else if (kv_committing.empty() && deferred_stable.empty()) {
	  force_flush = true;  // there's nothing else to commit!
	} else if (deferred_aggressive) {
	  force_flush = true;
	}
      } else {
      	if (aios || !deferred_done.empty()) {
	  force_flush = true;
      	} else {
	  dout(20) << __func__ << " skipping flush (no aios, no deferred_done)" << dendl;
      	}
      }

      if (force_flush) {
	dout(20) << __func__ << " num_aios=" << aios
		 << " force_flush=" << (int)force_flush
		 << ", flushing, deferred done->stable" << dendl;
	// flush/barrier on block device
	bdev->flush();

        // if we flush then deferred done are now deferred stable
        if (deferred_stable.empty()) {
          deferred_stable.swap(deferred_done);
        } else {
          deferred_stable.insert(deferred_stable.end(), deferred_done.begin(),
                                 deferred_done.end());
          deferred_done.clear();
        }
      }
      auto after_flush = mono_clock::now();

      // we will use one final transaction to force a sync
      KeyValueDB::Transaction synct = db->get_transaction();

      // increase {nid,blobid}_max?  note that this covers both the
      // case where we are approaching the max and the case we passed
      // it.  in either case, we increase the max in the earlier txn
      // we submit.
      uint64_t new_nid_max = 0, new_blobid_max = 0;
      if (nid_last + cct->_conf->bluestore_nid_prealloc/2 > nid_max) {
	KeyValueDB::Transaction t =
	  kv_submitting.empty() ? synct : kv_submitting.front()->t;
	new_nid_max = nid_last + cct->_conf->bluestore_nid_prealloc;
	bufferlist bl;
	encode(new_nid_max, bl);
	t->set(PREFIX_SUPER, "nid_max", bl);
	dout(10) << __func__ << " new_nid_max " << new_nid_max << dendl;
      }
      if (blobid_last + cct->_conf->bluestore_blobid_prealloc/2 > blobid_max) {
	KeyValueDB::Transaction t =
	  kv_submitting.empty() ? synct : kv_submitting.front()->t;
	new_blobid_max = blobid_last + cct->_conf->bluestore_blobid_prealloc;
	bufferlist bl;
	encode(new_blobid_max, bl);
	t->set(PREFIX_SUPER, "blobid_max", bl);
	dout(10) << __func__ << " new_blobid_max " << new_blobid_max << dendl;
      }

      for (auto txc : kv_committing) {
	throttle.log_state_latency(*txc, logger, l_bluestore_state_kv_queued_lat);
	if (txc->get_state() == TransContext::STATE_KV_QUEUED) {
	  ++kv_submitted;
	  _txc_apply_kv(txc, false);
	  --txc->osr->kv_committing_serially;
	} else {
	  ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
	}
	if (txc->had_ios) {
	  --txc->osr->txc_with_unstable_io;
	}
      }

      // release throttle *before* we commit.  this allows new ops
      // to be prepared and enter pipeline while we are waiting on
      // the kv commit sync/flush.  then hopefully on the next
      // iteration there will already be ops awake.  otherwise, we
      // end up going to sleep, and then wake up when the very first
      // transaction is ready for commit.
      throttle.release_kv_throttle(costs);

      // cleanup sync deferred keys
      for (auto b : deferred_stable) {
	for (auto& txc : b->txcs) {
	  bluestore_deferred_transaction_t& wt = *txc.deferred_txn;
	  ceph_assert(wt.released.empty()); // only kraken did this
	  string key;
	  get_deferred_key(wt.seq, &key);
	  synct->rm_single_key(PREFIX_DEFERRED, key);
	}
      }

#if defined(WITH_LTTNG)
      auto sync_start = mono_clock::now();
#endif
      // submit synct synchronously (block and wait for it to commit)
      int r = cct->_conf->bluestore_debug_omit_kv_commit ? 0 : db->submit_transaction_sync(synct);
      ceph_assert(r == 0);

#ifdef WITH_BLKIN
      for (auto txc : kv_committing) {
        if (txc->trace) {
          txc->trace.event("db sync submit");
          txc->trace.keyval("kv_committing size", kv_committing.size());
        }
      }
#endif

      int committing_size = kv_committing.size();
      int deferred_size = deferred_stable.size();

#if defined(WITH_LTTNG)
      double sync_latency = ceph::to_seconds<double>(mono_clock::now() - sync_start);
      for (auto txc: kv_committing) {
	if (txc->tracing) {
	  tracepoint(
	    bluestore,
	    transaction_kv_sync_latency,
	    txc->osr->get_sequencer_id(),
	    txc->seq,
	    kv_committing.size(),
	    deferred_done.size(),
	    deferred_stable.size(),
	    sync_latency);
	}
      }
#endif

      {
	std::unique_lock m{kv_finalize_lock};
	if (kv_committing_to_finalize.empty()) {
	  kv_committing_to_finalize.swap(kv_committing);
	} else {
	  kv_committing_to_finalize.insert(
	      kv_committing_to_finalize.end(),
	      kv_committing.begin(),
	      kv_committing.end());
	  kv_committing.clear();
	}
	if (deferred_stable_to_finalize.empty()) {
	  deferred_stable_to_finalize.swap(deferred_stable);
	} else {
	  deferred_stable_to_finalize.insert(
	      deferred_stable_to_finalize.end(),
	      deferred_stable.begin(),
	      deferred_stable.end());
	  deferred_stable.clear();
	}
	if (!kv_finalize_in_progress) {
	  kv_finalize_in_progress = true;
	  kv_finalize_cond.notify_one();
	}
      }

      if (new_nid_max) {
	nid_max = new_nid_max;
	dout(10) << __func__ << " nid_max now " << nid_max << dendl;
      }
      if (new_blobid_max) {
	blobid_max = new_blobid_max;
	dout(10) << __func__ << " blobid_max now " << blobid_max << dendl;
      }

      {
	auto finish = mono_clock::now();
	ceph::timespan dur_flush = after_flush - start;
	ceph::timespan dur_kv = finish - after_flush;
	ceph::timespan dur = finish - start;
	dout(20) << __func__ << " committed " << committing_size
	  << " cleaned " << deferred_size
	  << " in " << dur
	  << " (" << dur_flush << " flush + " << dur_kv << " kv commit)"
	  << dendl;
	log_latency("kv_flush",
	  l_bluestore_kv_flush_lat,
	  dur_flush,
	  cct->_conf->bluestore_log_op_age);
	log_latency("kv_commit",
	  l_bluestore_kv_commit_lat,
	  dur_kv,
	  cct->_conf->bluestore_log_op_age);
	log_latency("kv_sync",
	  l_bluestore_kv_sync_lat,
	  dur,
	  cct->_conf->bluestore_log_op_age);
      }

      l.lock();
      // previously deferred "done" are now "stable" by virtue of this
      // commit cycle.
      deferred_stable_queue.swap(deferred_done);
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  kv_sync_started = false;
}

void BlueStore::_kv_finalize_thread()
{
  deque<TransContext*> kv_committed;
  deque<DeferredBatch*> deferred_stable;
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock l(kv_finalize_lock);
  ceph_assert(!kv_finalize_started);
  kv_finalize_started = true;
  kv_finalize_cond.notify_all();
  while (true) {
    ceph_assert(kv_committed.empty());
    ceph_assert(deferred_stable.empty());
    if (kv_committing_to_finalize.empty() &&
	deferred_stable_to_finalize.empty()) {
      if (kv_finalize_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_finalize_in_progress = false;
      kv_finalize_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      kv_committed.swap(kv_committing_to_finalize);
      deferred_stable.swap(deferred_stable_to_finalize);
      l.unlock();
      dout(20) << __func__ << " kv_committed " << kv_committed << dendl;
      dout(20) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      while (!kv_committed.empty()) {
	TransContext *txc = kv_committed.front();
	ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
	_txc_state_proc(txc);
	kv_committed.pop_front();
      }

      for (auto b : deferred_stable) {
	auto p = b->txcs.begin();
	while (p != b->txcs.end()) {
	  TransContext *txc = &*p;
	  p = b->txcs.erase(p); // unlink here because
	  _txc_state_proc(txc); // this may destroy txc
	}
	delete b;
      }
      deferred_stable.clear();

      if (!deferred_aggressive) {
	if (deferred_queue_size >= deferred_batch_ops.load() ||
	    throttle.should_submit_deferred()) {
	  deferred_try_submit();
	}
      }

      // this is as good a place as any ...
      _reap_collections();

      logger->set(l_bluestore_fragmentation,
	  (uint64_t)(alloc->get_fragmentation() * 1000));

      log_latency("kv_final",
	l_bluestore_kv_final_lat,
	mono_clock::now() - start,
	cct->_conf->bluestore_log_op_age);

      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  kv_finalize_started = false;
}


bluestore_deferred_op_t *BlueStore::_get_deferred_op(
  TransContext *txc, uint64_t len)
{
  if (!txc->deferred_txn) {
    txc->deferred_txn = new bluestore_deferred_transaction_t;
  }
  txc->deferred_txn->ops.push_back(bluestore_deferred_op_t());
  logger->inc(l_bluestore_issued_deferred_writes);
  logger->inc(l_bluestore_issued_deferred_write_bytes, len);
  return &txc->deferred_txn->ops.back();
}

void BlueStore::_deferred_queue(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << " osr " << txc->osr << dendl;

  DeferredBatch *tmp;
  txc->osr->deferred_lock.lock();
  {
    if (!txc->osr->deferred_pending) {
      tmp = new DeferredBatch(cct, txc->osr.get());
    } else {
      tmp  = txc->osr->deferred_pending;
    }
  }

  tmp->txcs.push_back(*txc);
  bluestore_deferred_transaction_t& wt = *txc->deferred_txn;
  for (auto opi = wt.ops.begin(); opi != wt.ops.end(); ++opi) {
    const auto& op = *opi;
    ceph_assert(op.op == bluestore_deferred_op_t::OP_WRITE);
    bufferlist::const_iterator p = op.data.begin();
    for (auto e : op.extents) {
      tmp->prepare_write(cct, wt.seq, e.offset, e.length, p);
    }
  }

  {
    ++deferred_queue_size;
    txc->osr->deferred_pending = tmp;
    // condition "tmp->txcs.size() == 1" mean deferred_pending was originally empty.
    // So we should add osr into deferred_queue.
    if (!txc->osr->deferred_running && (tmp->txcs.size() == 1)) {
      deferred_lock.lock();
      deferred_queue.push_back(*txc->osr);
      deferred_lock.unlock();
    }

    if (deferred_aggressive &&
	!txc->osr->deferred_running) {
      _deferred_submit_unlock(txc->osr.get());
    } else {
      txc->osr->deferred_lock.unlock();
    }
  }
 }

void BlueStore::deferred_try_submit()
{
  dout(20) << __func__ << " " << deferred_queue.size() << " osrs, "
	   << deferred_queue_size << " txcs" << dendl;
  vector<OpSequencerRef> osrs;

  {
    std::lock_guard l(deferred_lock);
    osrs.reserve(deferred_queue.size());
    for (auto& osr : deferred_queue) {
      osrs.push_back(&osr);
    }
  }

  for (auto& osr : osrs) {
    osr->deferred_lock.lock();
    if (osr->deferred_pending) {
      if (!osr->deferred_running) {
	_deferred_submit_unlock(osr.get());
      } else {
	osr->deferred_lock.unlock();
	dout(20) << __func__ << "  osr " << osr << " already has running"
		 << dendl;
      }
    } else {
      osr->deferred_lock.unlock();
      dout(20) << __func__ << "  osr " << osr << " has no pending" << dendl;
    }
  }

  {
    std::lock_guard l(deferred_lock);
    deferred_last_submitted = ceph_clock_now();
  }
}

void BlueStore::_deferred_submit_unlock(OpSequencer *osr)
{
  dout(10) << __func__ << " osr " << osr
	   << " " << osr->deferred_pending->iomap.size() << " ios pending "
	   << dendl;
  ceph_assert(osr->deferred_pending);
  ceph_assert(!osr->deferred_running);

  auto b = osr->deferred_pending;
  deferred_queue_size -= b->seq_bytes.size();
  ceph_assert(deferred_queue_size >= 0);

  osr->deferred_running = osr->deferred_pending;
  osr->deferred_pending = nullptr;

  osr->deferred_lock.unlock();

  for (auto& txc : b->txcs) {
    throttle.log_state_latency(txc, logger, l_bluestore_state_deferred_queued_lat);
  }
  uint64_t start = 0, pos = 0;
  bufferlist bl;
  auto i = b->iomap.begin();
  while (true) {
    if (i == b->iomap.end() || i->first != pos) {
      if (bl.length()) {
	dout(20) << __func__ << " write 0x" << std::hex
		 << start << "~" << bl.length()
		 << " crc " << bl.crc32c(-1) << std::dec << dendl;
	if (!g_conf()->bluestore_debug_omit_block_device_write) {
	  logger->inc(l_bluestore_submitted_deferred_writes);
	  logger->inc(l_bluestore_submitted_deferred_write_bytes, bl.length());
	  int r = bdev->aio_write(start, bl, &b->ioc, false);
	  ceph_assert(r == 0);
	}
      }
      if (i == b->iomap.end()) {
	break;
      }
      start = 0;
      pos = i->first;
      bl.clear();
    }
    dout(20) << __func__ << "   seq " << i->second.seq << " 0x"
	     << std::hex << pos << "~" << i->second.bl.length() << std::dec
	     << dendl;
    if (!bl.length()) {
      start = pos;
    }
    pos += i->second.bl.length();
    bl.claim_append(i->second.bl);
    ++i;
  }

  bdev->aio_submit(&b->ioc);
}

struct C_DeferredTrySubmit : public Context {
  BlueStore *store;
  C_DeferredTrySubmit(BlueStore *s) : store(s) {}
  void finish(int r) {
    store->deferred_try_submit();
  }
};

void BlueStore::_deferred_aio_finish(OpSequencer *osr)
{
  dout(10) << __func__ << " osr " << osr << dendl;
  ceph_assert(osr->deferred_running);
  DeferredBatch *b = osr->deferred_running;

  {
    osr->deferred_lock.lock();
    ceph_assert(osr->deferred_running == b);
    osr->deferred_running = nullptr;
    if (!osr->deferred_pending) {
      dout(20) << __func__ << " dequeueing" << dendl;
      {
	deferred_lock.lock();
	auto q = deferred_queue.iterator_to(*osr);
	deferred_queue.erase(q);
	deferred_lock.unlock();
      }
      osr->deferred_lock.unlock();
    } else {
      osr->deferred_lock.unlock();
      if (deferred_aggressive) {
	dout(20) << __func__ << " queuing async deferred_try_submit" << dendl;
	finisher.queue(new C_DeferredTrySubmit(this));
      } else {
	dout(20) << __func__ << " leaving queued, more pending" << dendl;
      }
    }
  }

  {
    uint64_t costs = 0;
    {
      for (auto& i : b->txcs) {
	TransContext *txc = &i;
	throttle.log_state_latency(*txc, logger, l_bluestore_state_deferred_aio_wait_lat);
	txc->set_state(TransContext::STATE_DEFERRED_CLEANUP);
	costs += txc->cost;
      }
    }
    throttle.release_deferred_throttle(costs);
  }

  {
    std::lock_guard l(kv_lock);
    deferred_done_queue.emplace_back(b);

    // in the normal case, do not bother waking up the kv thread; it will
    // catch us on the next commit anyway.
    if (deferred_aggressive && !kv_sync_in_progress) {
	kv_sync_in_progress = true;
	kv_cond.notify_one();
    }
  }
}

int BlueStore::_deferred_replay()
{
  dout(10) << __func__ << " start" << dendl;
  int count = 0;
  int r = 0;
  interval_set<uint64_t> bluefs_extents;
  if (bluefs) {
    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&] (uint64_t start, uint32_t len) {
        bluefs_extents.insert(start, len);
      }
    );
  }
  CollectionRef ch = _get_collection(coll_t::meta());
  bool fake_ch = false;
  if (!ch) {
    // hmm, replaying initial mkfs?
    ch = static_cast<Collection*>(create_new_collection(coll_t::meta()).get());
    fake_ch = true;
  }
  OpSequencer *osr = static_cast<OpSequencer*>(ch->osr.get());
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_DEFERRED);
  for (it->lower_bound(string()); it->valid(); it->next(), ++count) {
    dout(20) << __func__ << " replay " << pretty_binary_string(it->key())
	     << dendl;
    bluestore_deferred_transaction_t *deferred_txn =
      new bluestore_deferred_transaction_t;
    bufferlist bl = it->value();
    auto p = bl.cbegin();
    try {
      decode(*deferred_txn, p);
    } catch (ceph::buffer::error& e) {
      derr << __func__ << " failed to decode deferred txn "
	   << pretty_binary_string(it->key()) << dendl;
      delete deferred_txn;
      r = -EIO;
      goto out;
    }
    bool has_some = _eliminate_outdated_deferred(deferred_txn, bluefs_extents);
    if (has_some) {
      TransContext *txc = _txc_create(ch.get(), osr,  nullptr);
      txc->deferred_txn = deferred_txn;
      txc->set_state(TransContext::STATE_KV_DONE);
      _txc_state_proc(txc);
    } else {
      delete deferred_txn;
    }
  }
 out:
  dout(20) << __func__ << " draining osr" << dendl;
  _osr_register_zombie(osr);
  _osr_drain_all();
  if (fake_ch) {
    new_coll_map.clear();
  }
  dout(10) << __func__ << " completed " << count << " events" << dendl;
  return r;
}

bool BlueStore::_eliminate_outdated_deferred(bluestore_deferred_transaction_t* deferred_txn,
					     interval_set<uint64_t>& bluefs_extents)
{
  bool has_some = false;
  dout(30) << __func__ << " bluefs_extents: " << std::hex << bluefs_extents << std::dec << dendl;
  auto it = deferred_txn->ops.begin();
  while (it != deferred_txn->ops.end()) {
    // We process a pair of _data_/_extents_ (here: it->data/it->extents)
    // by eliminating _extents_ that belong to bluefs, removing relevant parts of _data_
    // example:
    // +------------+---------------+---------------+---------------+
    // | data       | aaaaaaaabbbbb | bbbbcccccdddd | ddddeeeeeefff |
    // | extent     | 40000 - 44000 | 50000 - 58000 | 58000 - 60000 |
    // | in bluefs? |       no      |      yes      |       no      |
    // +------------+---------------+---------------+---------------+
    // result:
    // +------------+---------------+---------------+
    // | data       | aaaaaaaabbbbb | ddddeeeeeefff |
    // | extent     | 40000 - 44000 | 58000 - 60000 |
    // +------------+---------------+---------------+
    PExtentVector new_extents;
    ceph::buffer::list new_data;
    uint32_t data_offset = 0; // this tracks location of extent 'e' inside it->data
    dout(30) << __func__ << " input extents: " << it->extents << dendl;
    for (auto& e: it->extents) {
      interval_set<uint64_t> region;
      region.insert(e.offset, e.length);

      auto mi = bluefs_extents.lower_bound(e.offset);
      if (mi != bluefs_extents.begin()) {
	--mi;
	if (mi.get_end() <= e.offset) {
	  ++mi;
	}
      }
      while (mi != bluefs_extents.end() && mi.get_start() < e.offset + e.length) {
	// The interval_set does not like (asserts) when we erase interval that does not exist.
	// Hence we do we implement (region-mi) by ((region+mi)-mi).
	region.union_insert(mi.get_start(), mi.get_len());
	region.erase(mi.get_start(), mi.get_len());
	++mi;
      }
      // 'region' is now a subset of e, without parts used by bluefs
      // we trim coresponding parts from it->data (actally constructing new_data / new_extents)
      for (auto ki = region.begin(); ki != region.end(); ki++) {
	ceph::buffer::list chunk;
	// A chunk from it->data; data_offset is a an offset where 'e' was located;
	// 'ki.get_start() - e.offset' is an offset of ki inside 'e'.
	chunk.substr_of(it->data, data_offset + (ki.get_start() - e.offset), ki.get_len());
	new_data.claim_append(chunk);
	new_extents.emplace_back(bluestore_pextent_t(ki.get_start(), ki.get_len()));
      }
      data_offset += e.length;
    }
    dout(30) << __func__ << " output extents: " << new_extents << dendl;
    if (it->data.length() != new_data.length()) {
      dout(10) << __func__ << " trimmed deferred extents: " << it->extents << "->" << new_extents << dendl;
    }
    if (new_extents.size() == 0) {
      it = deferred_txn->ops.erase(it);
    } else {
      has_some = true;
      std::swap(it->extents, new_extents);
      std::swap(it->data, new_data);
      ++it;
    }
  }
  return has_some;
}

// ---------------------------
// transactions

int BlueStore::queue_transactions(
  CollectionHandle& ch,
  vector<Transaction>& tls,
  TrackedOpRef op,
  ThreadPool::TPHandle *handle)
{
  FUNCTRACE(cct);
  list<Context *> on_applied, on_commit, on_applied_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &on_applied, &on_commit, &on_applied_sync);

  auto start = mono_clock::now();

  Collection *c = static_cast<Collection*>(ch.get());
  OpSequencer *osr = c->osr.get();
  dout(10) << __func__ << " ch " << c << " " << c->cid << dendl;

  // prepare
  TransContext *txc = _txc_create(static_cast<Collection*>(ch.get()), osr,
				  &on_commit, op);

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }
  _txc_calc_cost(txc);

  _txc_write_nodes(txc, txc->t);

  // journal deferred items
  if (txc->deferred_txn) {
    txc->deferred_txn->seq = ++deferred_seq;
    bufferlist bl;
    encode(*txc->deferred_txn, bl);
    string key;
    get_deferred_key(txc->deferred_txn->seq, &key);
    txc->t->set(PREFIX_DEFERRED, key, bl);
  }

  _txc_finalize_kv(txc, txc->t);

#ifdef WITH_BLKIN
  if (txc->trace) {
    txc->trace.event("txc encode finished");
  }
#endif

  if (handle)
    handle->suspend_tp_timeout();

  auto tstart = mono_clock::now();

  if (!throttle.try_start_transaction(
	*db,
	*txc,
	tstart)) {
    // ensure we do not block here because of deferred writes
    dout(10) << __func__ << " failed get throttle_deferred_bytes, aggressive"
	     << dendl;
    ++deferred_aggressive;
    deferred_try_submit();
    {
      // wake up any previously finished deferred events
      std::lock_guard l(kv_lock);
      if (!kv_sync_in_progress) {
	kv_sync_in_progress = true;
	kv_cond.notify_one();
      }
    }
    throttle.finish_start_transaction(*db, *txc, tstart);
    --deferred_aggressive;
  }
  auto tend = mono_clock::now();

  if (handle)
    handle->reset_tp_timeout();

  logger->inc(l_bluestore_txc);

  // execute (start)
  _txc_state_proc(txc);

  // we're immediately readable (unlike FileStore)
  for (auto c : on_applied_sync) {
    c->complete(0);
  }
  if (!on_applied.empty()) {
    if (c->commit_queue) {
      c->commit_queue->queue(on_applied);
    } else {
      finisher.queue(on_applied);
    }
  }

#ifdef WITH_BLKIN
  if (txc->trace) {
    txc->trace.event("txc applied");
  }
#endif

  log_latency("submit_transact",
    l_bluestore_submit_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  log_latency("throttle_transact",
    l_bluestore_throttle_lat,
    tend - tstart,
    cct->_conf->bluestore_log_op_age);
  return 0;
}

void BlueStore::_txc_aio_submit(TransContext *txc)
{
  dout(10) << __func__ << " txc " << txc << dendl;
  bdev->aio_submit(&txc->ioc);
}

void BlueStore::_txc_add_transaction(TransContext *txc, Transaction *t)
{
  Transaction::iterator i = t->begin();

  _dump_transaction<30>(cct, t);

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }
  
  vector<OnodeRef> ovec(i.objects.size());

  for (int pos = 0; i.have_op(); ++pos) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;


    // collection operations
    CollectionRef &c = cvec[op->cid];

    // initialize osd_pool_id and do a smoke test that all collections belong
    // to the same pool
    spg_t pgid;
    if (!!c ? c->cid.is_pg(&pgid) : false) {
      ceph_assert(txc->osd_pool_id == META_POOL_ID ||
                  txc->osd_pool_id == pgid.pool());
      txc->osd_pool_id = pgid.pool();
    }

    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        const coll_t &cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	ceph_assert(!c);
	const coll_t &cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, op->split_bits, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      {
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
	r = _split_collection(txc, c, cvec[op->dest_cid], bits, rem);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MERGE_COLLECTION:
      {
        uint32_t bits = op->split_bits;
	r = _merge_collection(txc, &c, cvec[op->dest_cid], bits);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        uint32_t type = op->hint;
        bufferlist hint;
        i.decode_bl(hint);
        auto hiter = hint.cbegin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          decode(pg_num, hiter);
          decode(num_objs, hiter);
          dout(10) << __func__ << " collection hint objects is a no-op, "
		   << " pg_num " << pg_num << " num_objects " << num_objs
		   << dendl;
        } else {
          // Ignore the hint
          dout(10) << __func__ << " unknown collection hint " << type << dendl;
        }
	continue;
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      ceph_abort_msg("not implemented");
      break;
    }
    if (r < 0) {
      derr << __func__ << " error " << cpp_strerror(r)
           << " not handled on operation " << op->op
           << " (op " << pos << ", counting from 0)" << dendl;
      _dump_transaction<0>(cct, t);
      if (!g_conf().get_val<bool>("objectstore_debug_throw_on_failed_txc")) {
	ceph_abort_msg("unexpected error");
      } else {
	txc->osr->undo_queue(txc);
	delete txc;
	throw r;
      }
    }

    // these operations implicity create the object
    bool create = false;
    if (op->op == Transaction::OP_TOUCH ||
	op->op == Transaction::OP_CREATE ||
	op->op == Transaction::OP_WRITE ||
	op->op == Transaction::OP_ZERO) {
      create = true;
    }

    // object operations
    std::unique_lock l(c->lock);
    OnodeRef &o = ovec[op->oid];
    if (!o) {
      ghobject_t oid = i.get_oid(op->oid);
      o = c->get_onode(oid, create, op->op == Transaction::OP_CREATE);
    }
    if (!create && (!o || !o->exists)) {
      dout(10) << __func__ << " op " << op->op << " got ENOENT on "
	       << i.get_oid(op->oid) << dendl;
      r = -ENOENT;
      goto endop;
    }

    switch (op->op) {
    case Transaction::OP_CREATE:
    case Transaction::OP_TOUCH:
      r = _touch(txc, c, o);
      break;

    case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, o, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, o, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        uint64_t off = op->off;
	r = _truncate(txc, c, o, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
	r = _remove(txc, c, o);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        string name = i.decode_string();
        bufferptr bp;
        i.decode_bp(bp);
	r = _setattr(txc, c, o, name, bp);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, o, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	string name = i.decode_string();
	r = _rmattr(txc, c, o, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	r = _rmattrs(txc, c, o);
      }
      break;

    case Transaction::OP_CLONE:
      {
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
          const ghobject_t& noid = i.get_oid(op->dest_oid);
	  no = c->get_onode(noid, true);
	}
	r = _clone(txc, c, o, no);
      }
      break;

    case Transaction::OP_CLONERANGE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  const ghobject_t& noid = i.get_oid(op->dest_oid);
	  no = c->get_onode(noid, true);
	}
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_COLL_ADD:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_REMOVE:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_MOVE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
    case Transaction::OP_TRY_RENAME:
      {
	ceph_assert(op->cid == op->dest_cid);
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  no = c->get_onode(noid, false);
	}
	r = _rename(txc, c, o, no, noid);
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	r = _omap_clear(txc, c, o);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(txc, c, o, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(txc, c, o, keys_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkey_range(txc, c, o, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(txc, c, o, bl);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
	r = _set_alloc_hint(txc, c, o,
			    op->expected_object_size,
			    op->expected_write_size,
			    op->hint);
      }
      break;

    default:
      derr << __func__ << " bad op " << op->op << dendl;
      ceph_abort();
    }

  endop:
    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD ||
			    op->op == Transaction::OP_SETATTR ||
			    op->op == Transaction::OP_SETATTRS ||
			    op->op == Transaction::OP_RMATTR ||
			    op->op == Transaction::OP_OMAP_SETKEYS ||
			    op->op == Transaction::OP_OMAP_RMKEYS ||
			    op->op == Transaction::OP_OMAP_RMKEYRANGE ||
			    op->op == Transaction::OP_OMAP_SETHEADER))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from bluestore, misconfigured cluster";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

        derr << __func__ << " error " << cpp_strerror(r)
             << " not handled on operation " << op->op
             << " (op " << pos << ", counting from 0)"
             << dendl;
        derr << msg << dendl;
        _dump_transaction<0>(cct, t);
	if (!g_conf().get_val<bool>("objectstore_debug_throw_on_failed_txc")) {
	  ceph_abort_msg("unexpected error");
	} else {
	  txc->osr->undo_queue(txc);
	  delete txc;
	  throw r;
	}
      }
    }
  }
}



// -----------------
// write operations

int BlueStore::_touch(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  _assign_nid(txc, o);
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

void BlueStore::_pad_zeros(
  bufferlist *bl, uint64_t *offset,
  uint64_t chunk_size)
{
  auto length = bl->length();
  dout(30) << __func__ << " 0x" << std::hex << *offset << "~" << length
	   << " chunk_size 0x" << chunk_size << std::dec << dendl;
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  // front
  size_t front_pad = *offset % chunk_size;
  size_t back_pad = 0;
  size_t pad_count = 0;
  if (front_pad) {
    size_t front_copy = std::min<uint64_t>(chunk_size - front_pad, length);
    bufferptr z = ceph::buffer::create_small_page_aligned(chunk_size);
    z.zero(0, front_pad, false);
    pad_count += front_pad;
    bl->begin().copy(front_copy, z.c_str() + front_pad);
    if (front_copy + front_pad < chunk_size) {
      back_pad = chunk_size - (length + front_pad);
      z.zero(front_pad + length, back_pad, false);
      pad_count += back_pad;
    }
    bufferlist old, t;
    old.swap(*bl);
    t.substr_of(old, front_copy, length - front_copy);
    bl->append(z);
    bl->claim_append(t);
    *offset -= front_pad;
    length += pad_count;
  }

  // back
  uint64_t end = *offset + length;
  unsigned back_copy = end % chunk_size;
  if (back_copy) {
    ceph_assert(back_pad == 0);
    back_pad = chunk_size - back_copy;
    ceph_assert(back_copy <= length);
    bufferptr tail(chunk_size);
    bl->begin(length - back_copy).copy(back_copy, tail.c_str());
    tail.zero(back_copy, back_pad, false);
    bufferlist old;
    old.swap(*bl);
    bl->substr_of(old, 0, length - back_copy);
    bl->append(tail);
    length += back_pad;
    pad_count += back_pad;
  }
  dout(20) << __func__ << " pad 0x" << std::hex << front_pad << " + 0x"
	   << back_pad << " on front/back, now 0x" << *offset << "~"
	   << length << std::dec << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  if (pad_count)
    logger->inc(l_bluestore_write_pad_bytes, pad_count);
  ceph_assert(bl->length() == length);
}

void BlueStore::_do_write_small(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  ceph_assert(length < min_alloc_size);

  uint64_t end_offs = offset + length;

  logger->inc(l_bluestore_write_small);
  logger->inc(l_bluestore_write_small_bytes, length);

  bufferlist bl;
  blp.copy(length, bl);

  auto max_bsize = std::max(wctx->target_blob_size, min_alloc_size);
  auto min_off = offset >= max_bsize ? offset - max_bsize : 0;
  uint32_t alloc_len = min_alloc_size;
  auto offset0 = p2align<uint64_t>(offset, alloc_len);

  bool any_change;

  // search suitable extent in both forward and reverse direction in
  // [offset - target_max_blob_size, offset + target_max_blob_size] range
  // then check if blob can be reused via can_reuse_blob func or apply
  // direct/deferred write (the latter for extents including or higher
  // than 'offset' only).
  o->extent_map.fault_range(db, min_off, offset + max_bsize - min_off);

  // Look for an existing mutable blob we can use.
  auto begin = o->extent_map.extent_map.begin();
  auto end = o->extent_map.extent_map.end();
  auto ep = o->extent_map.seek_lextent(offset);
  if (ep != begin) {
    --ep;
    if (ep->blob_end() <= offset) {
      ++ep;
    }
  }
  auto prev_ep = end;
  if (ep != begin) {
    prev_ep = ep;
    --prev_ep;
  }

  boost::container::flat_set<const bluestore_blob_t*> inspected_blobs;
  // We don't want to have more blobs than min alloc units fit
  // into 2 max blobs
  size_t blob_threshold = max_blob_size / min_alloc_size * 2 + 1;
  bool above_blob_threshold = false;

  inspected_blobs.reserve(blob_threshold);

  uint64_t max_off = 0;
  auto start_ep = ep;
  auto end_ep = ep; // exclusively
  do {
    any_change = false;

    if (ep != end && ep->logical_offset < offset + max_bsize) {
      BlobRef b = ep->blob;
      if (!above_blob_threshold) {
	inspected_blobs.insert(&b->get_blob());
	above_blob_threshold = inspected_blobs.size() >= blob_threshold;
      }
      max_off = ep->logical_end();
      auto bstart = ep->blob_start();

      dout(20) << __func__ << " considering " << *b
	       << " bstart 0x" << std::hex << bstart << std::dec << dendl;
      if (bstart >= end_offs) {
	dout(20) << __func__ << " ignoring distant " << *b << dendl;
      } else if (!b->get_blob().is_mutable()) {
	dout(20) << __func__ << " ignoring immutable " << *b << dendl;
      } else if (ep->logical_offset % min_alloc_size !=
		  ep->blob_offset % min_alloc_size) {
	dout(20) << __func__ << " ignoring offset-skewed " << *b << dendl;
      } else {
	uint64_t chunk_size = b->get_blob().get_chunk_size(block_size);
	// can we pad our head/tail out with zeros?
	uint64_t head_pad, tail_pad;
	head_pad = p2phase(offset, chunk_size);
	tail_pad = p2nphase(end_offs, chunk_size);
	if (head_pad || tail_pad) {
	  o->extent_map.fault_range(db, offset - head_pad,
				    end_offs - offset + head_pad + tail_pad);
	}
	if (head_pad &&
	    o->extent_map.has_any_lextents(offset - head_pad, head_pad)) {
	  head_pad = 0;
	}
	if (tail_pad && o->extent_map.has_any_lextents(end_offs, tail_pad)) {
	  tail_pad = 0;
	}

	uint64_t b_off = offset - head_pad - bstart;
	uint64_t b_len = length + head_pad + tail_pad;

	// direct write into unused blocks of an existing mutable blob?
	if ((b_off % chunk_size == 0 && b_len % chunk_size == 0) &&
	    b->get_blob().get_ondisk_length() >= b_off + b_len &&
	    b->get_blob().is_unused(b_off, b_len) &&
	    b->get_blob().is_allocated(b_off, b_len)) {
	  _apply_padding(head_pad, tail_pad, bl);

	  dout(20) << __func__ << "  write to unused 0x" << std::hex
		   << b_off << "~" << b_len
		   << " pad 0x" << head_pad << " + 0x" << tail_pad
		   << std::dec << " of mutable " << *b << dendl;
	  _buffer_cache_write(txc, b, b_off, bl,
			      wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

	  if (!g_conf()->bluestore_debug_omit_block_device_write) {
	    if (b_len < prefer_deferred_size) {
	      dout(20) << __func__ << " deferring small 0x" << std::hex
		       << b_len << std::dec << " unused write via deferred" << dendl;
	      bluestore_deferred_op_t *op = _get_deferred_op(txc, bl.length());
	      op->op = bluestore_deferred_op_t::OP_WRITE;
	      b->get_blob().map(
		b_off, b_len,
		[&](uint64_t offset, uint64_t length) {
		  op->extents.emplace_back(bluestore_pextent_t(offset, length));
		  return 0;
		});
	      op->data = bl;
	    } else {
	      b->get_blob().map_bl(
		b_off, bl,
		[&](uint64_t offset, bufferlist& t) {
		  bdev->aio_write(offset, t,
				  &txc->ioc, wctx->buffered);
		});
	    }
	  }
	  b->dirty_blob().calc_csum(b_off, bl);
	  dout(20) << __func__ << "  lex old " << *ep << dendl;
	  Extent *le = o->extent_map.set_lextent(c, offset, b_off + head_pad, length,
						 b,
						 &wctx->old_extents);
	  b->dirty_blob().mark_used(le->blob_offset, le->length);

	  txc->statfs_delta.stored() += le->length;
	  dout(20) << __func__ << "  lex " << *le << dendl;
	  logger->inc(l_bluestore_write_small_unused);
	  return;
	}
	// read some data to fill out the chunk?
	uint64_t head_read = p2phase(b_off, chunk_size);
	uint64_t tail_read = p2nphase(b_off + b_len, chunk_size);
	if ((head_read || tail_read) &&
	    (b->get_blob().get_ondisk_length() >= b_off + b_len + tail_read) &&
	    head_read + tail_read < min_alloc_size) {
	  b_off -= head_read;
	  b_len += head_read + tail_read;

	} else {
	  head_read = tail_read = 0;
	}

	// chunk-aligned deferred overwrite?
	if (b->get_blob().get_ondisk_length() >= b_off + b_len &&
	    b_off % chunk_size == 0 &&
	    b_len % chunk_size == 0 &&
	    b->get_blob().is_allocated(b_off, b_len)) {

	  _apply_padding(head_pad, tail_pad, bl);

	  dout(20) << __func__ << "  reading head 0x" << std::hex << head_read
		   << " and tail 0x" << tail_read << std::dec << dendl;
	  if (head_read) {
	    bufferlist head_bl;
	    int r = _do_read(c.get(), o, offset - head_pad - head_read, head_read,
			     head_bl, 0);
	    ceph_assert(r >= 0 && r <= (int)head_read);
	    size_t zlen = head_read - r;
	    if (zlen) {
	      head_bl.append_zero(zlen);
	      logger->inc(l_bluestore_write_pad_bytes, zlen);
	    }
	    head_bl.claim_append(bl);
	    bl.swap(head_bl);
	    logger->inc(l_bluestore_write_penalty_read_ops);
	  }
	  if (tail_read) {
	    bufferlist tail_bl;
	    int r = _do_read(c.get(), o, offset + length + tail_pad, tail_read,
			     tail_bl, 0);
	    ceph_assert(r >= 0 && r <= (int)tail_read);
	    size_t zlen = tail_read - r;
	    if (zlen) {
	      tail_bl.append_zero(zlen);
	      logger->inc(l_bluestore_write_pad_bytes, zlen);
	    }
	    bl.claim_append(tail_bl);
	    logger->inc(l_bluestore_write_penalty_read_ops);
	  }
          logger->inc(l_bluestore_write_small_pre_read);

	  _buffer_cache_write(txc, b, b_off, bl,
			      wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

	  b->dirty_blob().calc_csum(b_off, bl);

	  if (!g_conf()->bluestore_debug_omit_block_device_write) {
	    bluestore_deferred_op_t *op = _get_deferred_op(txc, bl.length());
	    op->op = bluestore_deferred_op_t::OP_WRITE;
	    int r = b->get_blob().map(
	      b_off, b_len,
	      [&](uint64_t offset, uint64_t length) {
		op->extents.emplace_back(bluestore_pextent_t(offset, length));
		return 0;
	      });
	    ceph_assert(r == 0);
	    op->data = std::move(bl);
	    dout(20) << __func__ << "  deferred write 0x" << std::hex << b_off << "~"
		     << b_len << std::dec << " of mutable " << *b
		     << " at " << op->extents << dendl;
	  }

	  Extent *le = o->extent_map.set_lextent(c, offset, offset - bstart, length,
						 b, &wctx->old_extents);
	  b->dirty_blob().mark_used(le->blob_offset, le->length);
	  txc->statfs_delta.stored() += le->length;
	  dout(20) << __func__ << "  lex " << *le << dendl;
	  return;
	}
	// try to reuse blob if we can
	if (b->can_reuse_blob(min_alloc_size,
			      max_bsize,
			      offset0 - bstart,
			      &alloc_len)) {
	  ceph_assert(alloc_len == min_alloc_size); // expecting data always
					       // fit into reused blob
	  // Need to check for pending writes desiring to
	  // reuse the same pextent. The rationale is that during GC two chunks
	  // from garbage blobs(compressed?) can share logical space within the same
	  // AU. That's in turn might be caused by unaligned len in clone_range2.
	  // Hence the second write will fail in an attempt to reuse blob at
	  // do_alloc_write().
	  if (!wctx->has_conflict(b,
				  offset0,
				  offset0 + alloc_len, 
				  min_alloc_size)) {

	    // we can't reuse pad_head/pad_tail since they might be truncated 
	    // due to existent extents
	    uint64_t b_off = offset - bstart;
	    uint64_t b_off0 = b_off;
	    o->extent_map.punch_hole(c, offset, length, &wctx->old_extents);

	    // Zero detection -- small block
	    if (!cct->_conf->bluestore_zero_block_detection || !bl.is_zero()) {
	      _pad_zeros(&bl, &b_off0, chunk_size);

	      dout(20) << __func__ << " reuse blob " << *b << std::hex
		       << " (0x" << b_off0 << "~" << bl.length() << ")"
		       << " (0x" << b_off << "~" << length << ")"
		       << std::dec << dendl;

	      wctx->write(offset, b, alloc_len, b_off0, bl, b_off, length,
		  false, false);
	      logger->inc(l_bluestore_write_small_unused);
	    } else { // if (bl.is_zero())
	      dout(20) << __func__ << " skip small zero block " << std::hex
                << " (0x" << b_off0 << "~" << bl.length() << ")"
                << " (0x" << b_off << "~" << length << ")"
                << std::dec << dendl;
	      logger->inc(l_bluestore_write_small_skipped);
	      logger->inc(l_bluestore_write_small_skipped_bytes, length);
	    }

	    return;
	  }
	}
      }
      ++ep;
      end_ep = ep;
      any_change = true;
    } // if (ep != end && ep->logical_offset < offset + max_bsize)

    // check extent for reuse in reverse order
    if (prev_ep != end && prev_ep->logical_offset >= min_off) {
      BlobRef b = prev_ep->blob;
      if (!above_blob_threshold) {
	inspected_blobs.insert(&b->get_blob());
	above_blob_threshold = inspected_blobs.size() >= blob_threshold;
      }
      start_ep = prev_ep;
      auto bstart = prev_ep->blob_start();
      dout(20) << __func__ << " considering " << *b
	       << " bstart 0x" << std::hex << bstart << std::dec << dendl;
      if (b->can_reuse_blob(min_alloc_size,
			    max_bsize,
                            offset0 - bstart,
                            &alloc_len)) {
	ceph_assert(alloc_len == min_alloc_size); // expecting data always
					     // fit into reused blob
	// Need to check for pending writes desiring to
	// reuse the same pextent. The rationale is that during GC two chunks
	// from garbage blobs(compressed?) can share logical space within the same
	// AU. That's in turn might be caused by unaligned len in clone_range2.
	// Hence the second write will fail in an attempt to reuse blob at
	// do_alloc_write().
	if (!wctx->has_conflict(b,
				offset0,
				offset0 + alloc_len, 
				min_alloc_size)) {

	  uint64_t b_off = offset - bstart;
	  uint64_t b_off0 = b_off;
	  o->extent_map.punch_hole(c, offset, length, &wctx->old_extents);

	  // Zero detection -- small block
	  if (!cct->_conf->bluestore_zero_block_detection || !bl.is_zero()) {
	    uint64_t chunk_size = b->get_blob().get_chunk_size(block_size);
	    _pad_zeros(&bl, &b_off0, chunk_size);

	    dout(20) << __func__ << " reuse blob " << *b << std::hex
	      << " (0x" << b_off0 << "~" << bl.length() << ")"
	      << " (0x" << b_off << "~" << length << ")"
	      << std::dec << dendl;

	    wctx->write(offset, b, alloc_len, b_off0, bl, b_off, length,
		false, false);
	    logger->inc(l_bluestore_write_small_unused);
	  } else { // if (bl.is_zero())
	    dout(20) << __func__ << " skip small zero block " << std::hex
	      << " (0x" << b_off0 << "~" << bl.length() << ")"
	      << " (0x" << b_off << "~" << length << ")"
	      << std::dec << dendl;
	    logger->inc(l_bluestore_write_small_skipped);
	    logger->inc(l_bluestore_write_small_skipped_bytes, length);
	  }

	  return;
	}
      } 
      if (prev_ep != begin) {
	--prev_ep;
	any_change = true;
      } else {
	prev_ep = end; // to avoid useless first extent re-check
      }
    } // if (prev_ep != end && prev_ep->logical_offset >= min_off) 
  } while (any_change);

  if (above_blob_threshold) {
    dout(10) << __func__ << " request GC, blobs >= " << inspected_blobs.size()
            << " " << std::hex << min_off << "~" << max_off << std::dec
	    << dendl;
    ceph_assert(start_ep != end_ep);
    for (auto ep = start_ep; ep != end_ep; ++ep) {
      dout(20) << __func__ << " inserting for GC "
              << std::hex << ep->logical_offset << "~" << ep->length
	      << std::dec << dendl;

      wctx->extents_to_gc.union_insert(ep->logical_offset, ep->length);
    }
    // insert newly written extent to GC
    wctx->extents_to_gc.union_insert(offset, length);
      dout(20) << __func__ << " inserting (last) for GC "
              << std::hex << offset << "~" << length
	      << std::dec << dendl;
  }
  uint64_t b_off = p2phase<uint64_t>(offset, alloc_len);
  uint64_t b_off0 = b_off;
  o->extent_map.punch_hole(c, offset, length, &wctx->old_extents);

  // Zero detection -- small block
  if (!cct->_conf->bluestore_zero_block_detection || !bl.is_zero()) {
    // new blob.
    BlobRef b = c->new_blob();
    _pad_zeros(&bl, &b_off0, block_size);
    wctx->write(offset, b, alloc_len, b_off0, bl, b_off, length,
	min_alloc_size != block_size, // use 'unused' bitmap when alloc granularity
                                      // doesn't match disk one only
	true);
  } else { // if (bl.is_zero())
    dout(20) << __func__ << " skip small zero block " << std::hex
      << " (0x" << b_off0 << "~" << bl.length() << ")"
      << " (0x" << b_off << "~" << length << ")"
      << std::dec << dendl;
    logger->inc(l_bluestore_write_small_skipped);
    logger->inc(l_bluestore_write_small_skipped_bytes, length);
  }

  return;
}

bool BlueStore::BigDeferredWriteContext::can_defer(
    BlueStore::extent_map_t::iterator ep,
    uint64_t prefer_deferred_size,
    uint64_t block_size,
    uint64_t offset,
    uint64_t l)
{
  bool res = false;
  auto& blob = ep->blob->get_blob();
  if (offset >= ep->blob_start() &&
    blob.is_mutable()) {
    off = offset;
    b_off = offset - ep->blob_start();
    uint64_t chunk_size = blob.get_chunk_size(block_size);
    uint64_t ondisk = blob.get_ondisk_length();
    used = std::min(l, ondisk - b_off);

    // will read some data to fill out the chunk?
    head_read = p2phase<uint64_t>(b_off, chunk_size);
    tail_read = p2nphase<uint64_t>(b_off + used, chunk_size);
    b_off -= head_read;

    ceph_assert(b_off % chunk_size == 0);
    ceph_assert(blob_aligned_len() % chunk_size == 0);

    res = blob_aligned_len() < prefer_deferred_size &&
      blob_aligned_len() <= ondisk &&
      blob.is_allocated(b_off, blob_aligned_len());
    if (res) {
      blob_ref = ep->blob;
      blob_start = ep->blob_start();
    }
  }
  return res;
}

bool BlueStore::BigDeferredWriteContext::apply_defer()
{
  int r = blob_ref->get_blob().map(
    b_off, blob_aligned_len(),
    [&](const bluestore_pextent_t& pext,
      uint64_t offset,
      uint64_t length) {
        // apply deferred if overwrite breaks blob continuity only.
        // if it totally overlaps some pextent - fallback to regular write
        if (pext.offset < offset ||
          pext.end() > offset + length) {
          res_extents.emplace_back(bluestore_pextent_t(offset, length));
          return 0;
        }
        return -1;
    });
  return r >= 0;
}

void BlueStore::_do_write_big_apply_deferred(
    TransContext* txc,
    CollectionRef& c,
    OnodeRef& o,
    BlueStore::BigDeferredWriteContext& dctx,
    bufferlist::iterator& blp,
    WriteContext* wctx)
{
  bufferlist bl;
  dout(20) << __func__ << "  reading head 0x" << std::hex << dctx.head_read
    << " and tail 0x" << dctx.tail_read << std::dec << dendl;
  if (dctx.head_read) {
    int r = _do_read(c.get(), o,
      dctx.off - dctx.head_read,
      dctx.head_read,
      bl,
      0);
    ceph_assert(r >= 0 && r <= (int)dctx.head_read);
    size_t zlen = dctx.head_read - r;
    if (zlen) {
      bl.append_zero(zlen);
      logger->inc(l_bluestore_write_pad_bytes, zlen);
    }
    logger->inc(l_bluestore_write_penalty_read_ops);
  }
  blp.copy(dctx.used, bl);

  if (dctx.tail_read) {
    bufferlist tail_bl;
    int r = _do_read(c.get(), o,
      dctx.off + dctx.used, dctx.tail_read,
      tail_bl, 0);
    ceph_assert(r >= 0 && r <= (int)dctx.tail_read);
    size_t zlen = dctx.tail_read - r;
    if (zlen) {
      tail_bl.append_zero(zlen);
      logger->inc(l_bluestore_write_pad_bytes, zlen);
    }
    bl.claim_append(tail_bl);
    logger->inc(l_bluestore_write_penalty_read_ops);
  }
  auto& b0 = dctx.blob_ref;
  _buffer_cache_write(txc, b0, dctx.b_off, bl,
    wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

  b0->dirty_blob().calc_csum(dctx.b_off, bl);

  Extent* le = o->extent_map.set_lextent(c, dctx.off,
    dctx.off - dctx.blob_start, dctx.used, b0, &wctx->old_extents);

  // in fact this is a no-op for big writes but left here to maintain
  // uniformity and avoid missing after some refactor.
  b0->dirty_blob().mark_used(le->blob_offset, le->length);
  txc->statfs_delta.stored() += le->length;

  if (!g_conf()->bluestore_debug_omit_block_device_write) {
    bluestore_deferred_op_t* op = _get_deferred_op(txc, bl.length());
    op->op = bluestore_deferred_op_t::OP_WRITE;
    op->extents.swap(dctx.res_extents);
    op->data = std::move(bl);
  }
}

void BlueStore::_do_write_big(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " target_blob_size 0x" << wctx->target_blob_size << std::dec
	   << " compress " << (int)wctx->compress
	   << dendl;
  logger->inc(l_bluestore_write_big);
  logger->inc(l_bluestore_write_big_bytes, length);
  auto max_bsize = std::max(wctx->target_blob_size, min_alloc_size);
  uint64_t prefer_deferred_size_snapshot = prefer_deferred_size.load();
  while (length > 0) {
    bool new_blob = false;
    BlobRef b;
    uint32_t b_off = 0;
    uint32_t l = 0;

    //attempting to reuse existing blob
    if (!wctx->compress) {
      // enforce target blob alignment with max_bsize
      l = max_bsize - p2phase(offset, max_bsize);
      l = std::min(uint64_t(l), length);

      auto end = o->extent_map.extent_map.end();

      dout(20) << __func__ << " may be defer: 0x" << std::hex
	       << offset << "~" << l
               << std::dec << dendl;

      if (prefer_deferred_size_snapshot &&
          l <= prefer_deferred_size_snapshot * 2) {
        // Single write that spans two adjusted existing blobs can result
        // in up to two deferred blocks of 'prefer_deferred_size'
        // So we're trying to minimize the amount of resulting blobs
        // and preserve 2 blobs rather than inserting one more in between
        // E.g. write 0x10000~20000 over existing blobs
        // (0x0~20000 and 0x20000~20000) is better (from subsequent reading
        // performance point of view) to result in two deferred writes to
        // existing blobs than having 3 blobs: 0x0~10000, 0x10000~20000, 0x30000~10000

        // look for an existing mutable blob we can write into
        auto ep = o->extent_map.seek_lextent(offset);
        auto ep_next = end;
        BigDeferredWriteContext head_info, tail_info;

        bool will_defer = ep != end ?
          head_info.can_defer(ep,
            prefer_deferred_size_snapshot,
            block_size,
            offset,
            l) :
          false;
        auto offset_next = offset + head_info.used;
        auto remaining = l - head_info.used;
        if (will_defer && remaining) {
          will_defer = false;
          if (remaining <= prefer_deferred_size_snapshot) {
            ep_next = o->extent_map.seek_lextent(offset_next);
            // check if we can defer remaining totally
            will_defer = ep_next == end ?
              false :
              tail_info.can_defer(ep_next,
                prefer_deferred_size_snapshot,
                block_size,
                offset_next,
                remaining);
            will_defer = will_defer && remaining == tail_info.used;
          }
        }
        if (will_defer) {
          dout(20) << __func__ << " " << *(head_info.blob_ref)
            << " deferring big " << std::hex
            << " (0x" << head_info.b_off << "~" << head_info.blob_aligned_len() << ")"
            << std::dec << " write via deferred"
            << dendl;
          if (remaining) {
            dout(20) << __func__ << " " << *(tail_info.blob_ref)
              << " deferring big " << std::hex
              << " (0x" << tail_info.b_off << "~" << tail_info.blob_aligned_len() << ")"
              << std::dec << " write via deferred"
              << dendl;
          }

          will_defer = head_info.apply_defer();
          if (!will_defer) {
            dout(20) << __func__
              << " deferring big fell back, head isn't continuous"
              << dendl;
          } else if (remaining) {
            will_defer = tail_info.apply_defer();
            if (!will_defer) {
              dout(20) << __func__
                << " deferring big fell back, tail isn't continuous"
                << dendl;
            }
          }
        }
        if (will_defer) {
          _do_write_big_apply_deferred(txc, c, o, head_info, blp, wctx);
          if (remaining) {
            _do_write_big_apply_deferred(txc, c, o, tail_info,
              blp, wctx);
          }
	  dout(20) << __func__ << " defer big: 0x" << std::hex
		   << offset << "~" << l
		   << std::dec << dendl;
          offset += l;
          length -= l;
          logger->inc(l_bluestore_write_big_blobs, remaining ? 2 : 1);
          logger->inc(l_bluestore_write_big_deferred, remaining ? 2 : 1);
          continue;
        }
      }
      dout(20) << __func__ << " lookup for blocks to reuse..." << dendl;

      o->extent_map.punch_hole(c, offset, l, &wctx->old_extents);

      // seek again as punch_hole could invalidate ep
      auto ep = o->extent_map.seek_lextent(offset);
      auto begin = o->extent_map.extent_map.begin();
      auto prev_ep = end;
      if (ep != begin) {
        prev_ep = ep;
        --prev_ep;
      }

      auto min_off = offset >= max_bsize ? offset - max_bsize : 0;
      // search suitable extent in both forward and reverse direction in
      // [offset - target_max_blob_size, offset + target_max_blob_size] range
      // then check if blob can be reused via can_reuse_blob func.
      bool any_change;
      do {
	any_change = false;
	if (ep != end && ep->logical_offset < offset + max_bsize) {
          dout(20) << __func__ << " considering " << *ep
                   << " bstart 0x" << std::hex << ep->blob_start() << std::dec << dendl;

          if (offset >= ep->blob_start() &&
              ep->blob->can_reuse_blob(min_alloc_size, max_bsize,
	                               offset - ep->blob_start(),
	                               &l)) {
	    b = ep->blob;
            b_off = offset - ep->blob_start();
            prev_ep = end; // to avoid check below
	    dout(20) << __func__ << " reuse blob " << *b << std::hex
		     << " (0x" << b_off << "~" << l << ")" << std::dec << dendl;
	  } else {
	    ++ep;
	    any_change = true;
	  }
	}

	if (prev_ep != end && prev_ep->logical_offset >= min_off) {
          dout(20) << __func__ << " considering rev " << *prev_ep
                   << " bstart 0x" << std::hex << prev_ep->blob_start() << std::dec << dendl;
          if (prev_ep->blob->can_reuse_blob(min_alloc_size, max_bsize,
                                    	    offset - prev_ep->blob_start(),
                                    	    &l)) {
	    b = prev_ep->blob;
	    b_off = offset - prev_ep->blob_start();
	    dout(20) << __func__ << " reuse blob " << *b << std::hex
		     << " (0x" << b_off << "~" << l << ")" << std::dec << dendl;
	  } else if (prev_ep != begin) {
	    --prev_ep;
	    any_change = true;
	  } else {
	    prev_ep = end; // to avoid useless first extent re-check
	  }
	}
      } while (b == nullptr && any_change);
    } else {
      // trying to utilize as longer chunk as permitted in case of compression.
      l = std::min(max_bsize, length);
      o->extent_map.punch_hole(c, offset, l, &wctx->old_extents);
    } // if (!wctx->compress)

    if (b == nullptr) {
      b = c->new_blob();
      b_off = 0;
      new_blob = true;
    }
    bufferlist t;
    blp.copy(l, t);

    // Zero detection -- big block
    if (!cct->_conf->bluestore_zero_block_detection || !t.is_zero()) {
      wctx->write(offset, b, l, b_off, t, b_off, l, false, new_blob);

      dout(20) << __func__ << " schedule write big: 0x"
      << std::hex << offset << "~" << l << std::dec
      << (new_blob ? " new " : " reuse ")
      << *b << dendl;

      logger->inc(l_bluestore_write_big_blobs);
    } else { // if (!t.is_zero())
      dout(20) << __func__ << " skip big zero block " << std::hex
        << " (0x" << b_off << "~" << t.length() << ")"
        << " (0x" << b_off << "~" << l << ")"
        << std::dec << dendl;
      logger->inc(l_bluestore_write_big_skipped_blobs);
      logger->inc(l_bluestore_write_big_skipped_bytes, l);
    }

    offset += l;
    length -= l;
  }
}

int BlueStore::_do_alloc_write(
  TransContext *txc,
  CollectionRef coll,
  OnodeRef& o,
  WriteContext *wctx)
{
  dout(20) << __func__ << " txc " << txc
	   << " " << wctx->writes.size() << " blobs"
	   << dendl;
  if (wctx->writes.empty()) {
    return 0;
  }

  CompressorRef c;
  double crr = 0;
  if (wctx->compress) {
    c = select_option(
      "compression_algorithm",
      compressor,
      [&]() {
        string val;
        if (coll->pool_opts.get(pool_opts_t::COMPRESSION_ALGORITHM, &val)) {
          CompressorRef cp = compressor;
          if (!cp || cp->get_type_name() != val) {
            cp = Compressor::create(cct, val);
	    if (!cp) {
	      if (_set_compression_alert(false, val.c_str())) {
	        derr << __func__ << " unable to initialize " << val.c_str()
		     << " compressor" << dendl;
	      }
	    }
          }
          return std::optional<CompressorRef>(cp);
        }
        return std::optional<CompressorRef>();
      }
    );

    crr = select_option(
      "compression_required_ratio",
      cct->_conf->bluestore_compression_required_ratio,
      [&]() {
        double val;
        if (coll->pool_opts.get(pool_opts_t::COMPRESSION_REQUIRED_RATIO, &val)) {
          return std::optional<double>(val);
        }
        return std::optional<double>();
      }
    );
  }

  // checksum
  int64_t csum = csum_type.load();
  csum = select_option(
    "csum_type",
    csum,
    [&]() {
      int64_t val;
      if (coll->pool_opts.get(pool_opts_t::CSUM_TYPE, &val)) {
        return std::optional<int64_t>(val);
      }
      return std::optional<int64_t>();
    }
  );

  // compress (as needed) and calc needed space
  uint64_t need = 0;
  uint64_t data_size = 0;
  // 'need' is amount of space that must be provided by allocator.
  // 'data_size' is a size of data that will be transferred to disk.
  // Note that data_size is always <= need. This comes from:
  // - write to blob was unaligned, and there is free space
  // - data has been compressed
  //
  // We make one decision and apply it to all blobs.
  // All blobs will be deferred or none will.
  // We assume that allocator does its best to provide contiguous space,
  // and the condition is : (data_size < deferred).

  auto max_bsize = std::max(wctx->target_blob_size, min_alloc_size);
  for (auto& wi : wctx->writes) {
    if (c && wi.blob_length > min_alloc_size) {
      auto start = mono_clock::now();

      // compress
      ceph_assert(wi.b_off == 0);
      ceph_assert(wi.blob_length == wi.bl.length());

      // FIXME: memory alignment here is bad
      bufferlist t;
      std::optional<int32_t> compressor_message;
      int r = c->compress(wi.bl, t, compressor_message);
      uint64_t want_len_raw = wi.blob_length * crr;
      uint64_t want_len = p2roundup(want_len_raw, min_alloc_size);
      bool rejected = false;
      uint64_t compressed_len = t.length();
      // do an approximate (fast) estimation for resulting blob size
      // that doesn't take header overhead  into account
      uint64_t result_len = p2roundup(compressed_len, min_alloc_size);
      if (r == 0 && result_len <= want_len && result_len < wi.blob_length) {
	bluestore_compression_header_t chdr;
	chdr.type = c->get_type();
	chdr.length = t.length();
	chdr.compressor_message = compressor_message;
	encode(chdr, wi.compressed_bl);
	wi.compressed_bl.claim_append(t);

	compressed_len = wi.compressed_bl.length();
	result_len = p2roundup(compressed_len, min_alloc_size);
	if (result_len <= want_len && result_len < wi.blob_length) {
	  // Cool. We compressed at least as much as we were hoping to.
	  // pad out to min_alloc_size
	  wi.compressed_bl.append_zero(result_len - compressed_len);
	  wi.compressed_len = compressed_len;
	  wi.compressed = true;
	  logger->inc(l_bluestore_write_pad_bytes, result_len - compressed_len);
	  dout(20) << __func__ << std::hex << "  compressed 0x" << wi.blob_length
		   << " -> 0x" << compressed_len << " => 0x" << result_len
		   << " with " << c->get_type()
		   << std::dec << dendl;
	  txc->statfs_delta.compressed() += compressed_len;
	  txc->statfs_delta.compressed_original() += wi.blob_length;
	  txc->statfs_delta.compressed_allocated() += result_len;
	  logger->inc(l_bluestore_compress_success_count);
	  need += result_len;
	  data_size += result_len;
	} else {
	  rejected = true;
	}
      } else if (r != 0) {
	dout(5) << __func__ << std::hex << "  0x" << wi.blob_length
		 << " bytes compressed using " << c->get_type_name()
		 << std::dec
		 << " failed with errcode = " << r
		 << ", leaving uncompressed"
		 << dendl;
	logger->inc(l_bluestore_compress_rejected_count);
	need += wi.blob_length;
	data_size += wi.bl.length();
      } else {
	rejected = true;
      }

      if (rejected) {
	dout(20) << __func__ << std::hex << "  0x" << wi.blob_length
		 << " compressed to 0x" << compressed_len << " -> 0x" << result_len
		 << " with " << c->get_type()
		 << ", which is more than required 0x" << want_len_raw
		 << " -> 0x" << want_len
		 << ", leaving uncompressed"
		 << std::dec << dendl;
	logger->inc(l_bluestore_compress_rejected_count);
	need += wi.blob_length;
	data_size += wi.bl.length();
      }
      log_latency("compress@_do_alloc_write",
	l_bluestore_compress_lat,
        mono_clock::now() - start,
	cct->_conf->bluestore_log_op_age );
    } else {
      need += wi.blob_length;
      data_size += wi.bl.length();
    }
  }
  PExtentVector prealloc;
  prealloc.reserve(2 * wctx->writes.size());
  int64_t prealloc_left = 0;
  auto start = mono_clock::now();
  prealloc_left = alloc->allocate(
    need, min_alloc_size, need,
    0, &prealloc);
  log_latency("allocator@_do_alloc_write",
    l_bluestore_allocator_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  if (prealloc_left < 0 || prealloc_left < (int64_t)need) {
    derr << __func__ << " failed to allocate 0x" << std::hex << need
         << " allocated 0x " << (prealloc_left < 0 ? 0 : prealloc_left)
         << " min_alloc_size 0x" << min_alloc_size
         << " available 0x " << alloc->get_free()
         << std::dec << dendl;
    if (prealloc.size()) {
      alloc->release(prealloc);
    }
    return -ENOSPC;
  }
  _collect_allocation_stats(need, min_alloc_size, prealloc);

  dout(20) << __func__ << std::hex << " need=0x" << need << " data=0x" << data_size
	   << " prealloc " << prealloc << dendl;
  auto prealloc_pos = prealloc.begin();
  ceph_assert(prealloc_pos != prealloc.end());

  for (auto& wi : wctx->writes) {
    bluestore_blob_t& dblob = wi.b->dirty_blob();
    uint64_t b_off = wi.b_off;
    bufferlist *l = &wi.bl;
    uint64_t final_length = wi.blob_length;
    uint64_t csum_length = wi.blob_length;
    if (wi.compressed) {
      final_length = wi.compressed_bl.length();
      csum_length = final_length;
      unsigned csum_order = std::countr_zero(csum_length);
      l = &wi.compressed_bl;
      dblob.set_compressed(wi.blob_length, wi.compressed_len);
      if (csum != Checksummer::CSUM_NONE) {
        dout(20) << __func__
		 << " initialize csum setting for compressed blob " << *wi.b
                 << " csum_type " << Checksummer::get_csum_type_string(csum)
                 << " csum_order " << csum_order
                 << " csum_length 0x" << std::hex << csum_length
                 << " blob_length 0x" << wi.blob_length
                 << " compressed_length 0x" << wi.compressed_len << std::dec
                 << dendl;
        dblob.init_csum(csum, csum_order, csum_length);
      }
    } else if (wi.new_blob) {
      unsigned csum_order;
      // initialize newly created blob only
      ceph_assert(dblob.is_mutable());
      if (l->length() != wi.blob_length) {
        // hrm, maybe we could do better here, but let's not bother.
        dout(20) << __func__ << " forcing csum_order to block_size_order "
                << block_size_order << dendl;
	csum_order = block_size_order;
      } else {
        csum_order = std::min<unsigned>(wctx->csum_order, std::countr_zero(l->length()));
      }
      // try to align blob with max_blob_size to improve
      // its reuse ratio, e.g. in case of reverse write
      uint32_t suggested_boff =
       (wi.logical_offset - (wi.b_off0 - wi.b_off)) % max_bsize;
      if ((suggested_boff % (1 << csum_order)) == 0 &&
           suggested_boff + final_length <= max_bsize &&
           suggested_boff > b_off) {
        dout(20) << __func__ << " forcing blob_offset to 0x"
                 << std::hex << suggested_boff << std::dec << dendl;
        ceph_assert(suggested_boff >= b_off);
        csum_length += suggested_boff - b_off;
        b_off = suggested_boff;
      }
      if (csum != Checksummer::CSUM_NONE) {
        dout(20) << __func__
		 << " initialize csum setting for new blob " << *wi.b
                 << " csum_type " << Checksummer::get_csum_type_string(csum)
                 << " csum_order " << csum_order
                 << " csum_length 0x" << std::hex << csum_length << std::dec
                 << dendl;
        dblob.init_csum(csum, csum_order, csum_length);
      }
    }

    PExtentVector extents;
    int64_t left = final_length;
    auto prefer_deferred_size_snapshot = prefer_deferred_size.load();
    while (left > 0) {
      ceph_assert(prealloc_left > 0);
      if (prealloc_pos->length <= left) {
	prealloc_left -= prealloc_pos->length;
	left -= prealloc_pos->length;
	txc->statfs_delta.allocated() += prealloc_pos->length;
	extents.push_back(*prealloc_pos);
	++prealloc_pos;
      } else {
	extents.emplace_back(prealloc_pos->offset, left);
	prealloc_pos->offset += left;
	prealloc_pos->length -= left;
	prealloc_left -= left;
	txc->statfs_delta.allocated() += left;
	left = 0;
	break;
      }
    }
    for (auto& p : extents) {
      txc->allocated.insert(p.offset, p.length);
    }
    dblob.allocated(p2align(b_off, min_alloc_size), final_length, extents);

    dout(20) << __func__ << " blob " << *wi.b << dendl;
    if (dblob.has_csum()) {
      dblob.calc_csum(b_off, *l);
    }

    if (wi.mark_unused) {
      ceph_assert(!dblob.is_compressed());
      auto b_end = b_off + wi.bl.length();
      if (b_off) {
        dblob.add_unused(0, b_off);
      }
      uint64_t llen = dblob.get_logical_length();
      if (b_end < llen) {
        dblob.add_unused(b_end, llen - b_end);
      }
    }

    Extent *le = o->extent_map.set_lextent(coll, wi.logical_offset,
                                           b_off + (wi.b_off0 - wi.b_off),
                                           wi.length0,
                                           wi.b,
                                           nullptr);
    wi.b->dirty_blob().mark_used(le->blob_offset, le->length);
    txc->statfs_delta.stored() += le->length;
    dout(20) << __func__ << "  lex " << *le << dendl;
    _buffer_cache_write(txc, wi.b, b_off, wi.bl,
                        wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);

    // queue io
    if (!g_conf()->bluestore_debug_omit_block_device_write) {
      if (data_size < prefer_deferred_size_snapshot) {
	dout(20) << __func__ << " deferring 0x" << std::hex
		 << l->length() << std::dec << " write via deferred" << dendl;
	bluestore_deferred_op_t *op = _get_deferred_op(txc, l->length());
	op->op = bluestore_deferred_op_t::OP_WRITE;
	int r = wi.b->get_blob().map(
	  b_off, l->length(),
	  [&](uint64_t offset, uint64_t length) {
	    op->extents.emplace_back(bluestore_pextent_t(offset, length));
	    return 0;
	  });
        ceph_assert(r == 0);
	op->data = *l;
      } else {
	wi.b->get_blob().map_bl(
	  b_off, *l,
	  [&](uint64_t offset, bufferlist& t) {
	    bdev->aio_write(offset, t, &txc->ioc, false);
	  });
	logger->inc(l_bluestore_write_new);
      }
    }
  }
  ceph_assert(prealloc_pos == prealloc.end());
  ceph_assert(prealloc_left == 0);
  return 0;
}

void BlueStore::_wctx_finish(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  WriteContext *wctx,
  set<SharedBlob*> *maybe_unshared_blobs)
{
  auto oep = wctx->old_extents.begin();
  while (oep != wctx->old_extents.end()) {
    auto &lo = *oep;
    oep = wctx->old_extents.erase(oep);
    dout(20) << __func__ << " lex_old " << lo.e << dendl;
    BlobRef b = lo.e.blob;
    const bluestore_blob_t& blob = b->get_blob();
    if (blob.is_compressed()) {
      if (lo.blob_empty) {
	txc->statfs_delta.compressed() -= blob.get_compressed_payload_length();
      }
      txc->statfs_delta.compressed_original() -= lo.e.length;
    }
    auto& r = lo.r;
    txc->statfs_delta.stored() -= lo.e.length;
    if (!r.empty()) {
      dout(20) << __func__ << "  blob " << *b << " release " << r << dendl;
      if (blob.is_shared()) {
	PExtentVector final;
        c->load_shared_blob(b->shared_blob);
	bool unshare = false;
	bool* unshare_ptr =
	  !maybe_unshared_blobs || b->is_referenced() ? nullptr : &unshare;
	for (auto e : r) {
	  b->shared_blob->put_ref(
	    e.offset, e.length, &final,
	    unshare_ptr);
	}
	if (unshare) {
	  ceph_assert(maybe_unshared_blobs);
	  maybe_unshared_blobs->insert(b->shared_blob.get());
	}
	dout(20) << __func__ << "  shared_blob release " << final
		 << " from " << *b->shared_blob << dendl;
	txc->write_shared_blob(b->shared_blob);
	r.clear();
	r.swap(final);
      }
    }
    // we can't invalidate our logical extents as we drop them because
    // other lextents (either in our onode or others) may still
    // reference them.  but we can throw out anything that is no
    // longer allocated.  Note that this will leave behind edge bits
    // that are no longer referenced but not deallocated (until they
    // age out of the cache naturally).
    b->discard_unallocated(c.get());
    for (auto e : r) {
      dout(20) << __func__ << "  release " << e << dendl;
      txc->released.insert(e.offset, e.length);
      txc->statfs_delta.allocated() -= e.length;
      if (blob.is_compressed()) {
        txc->statfs_delta.compressed_allocated() -= e.length;
      }
    }

    if (b->is_spanning() && !b->is_referenced() && lo.blob_empty) {
      dout(20) << __func__ << "  spanning_blob_map removing empty " << *b
	       << dendl;
      o->extent_map.spanning_blob_map.erase(b->id);
    }
    delete &lo;
  }
}

void BlueStore::_do_write_data(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset,
  uint64_t length,
  bufferlist& bl,
  WriteContext *wctx)
{
  uint64_t end = offset + length;
  bufferlist::iterator p = bl.begin();

  if (offset / min_alloc_size == (end - 1) / min_alloc_size &&
      (length != min_alloc_size)) {
    // we fall within the same block
    _do_write_small(txc, c, o, offset, length, p, wctx);
  } else {
    uint64_t head_offset, head_length;
    uint64_t middle_offset, middle_length;
    uint64_t tail_offset, tail_length;

    head_offset = offset;
    head_length = p2nphase(offset, min_alloc_size);

    tail_offset = p2align(end, min_alloc_size);
    tail_length = p2phase(end, min_alloc_size);

    middle_offset = head_offset + head_length;
    middle_length = length - head_length - tail_length;

    if (head_length) {
      _do_write_small(txc, c, o, head_offset, head_length, p, wctx);
    }

    _do_write_big(txc, c, o, middle_offset, middle_length, p, wctx);

    if (tail_length) {
      _do_write_small(txc, c, o, tail_offset, tail_length, p, wctx);
    }
  }
}

void BlueStore::_choose_write_options(
   CollectionRef& c,
   OnodeRef& o,
   uint32_t fadvise_flags,
   WriteContext *wctx)
{
  if (fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered write" << dendl;
    wctx->buffered = true;
  } else if (cct->_conf->bluestore_default_buffered_write &&
	     (fadvise_flags & (CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
			       CEPH_OSD_OP_FLAG_FADVISE_NOCACHE)) == 0) {
    dout(20) << __func__ << " defaulting to buffered write" << dendl;
    wctx->buffered = true;
  }

  // apply basic csum block size
  wctx->csum_order = block_size_order;

  // compression parameters
  unsigned alloc_hints = o->onode.alloc_hint_flags;
  auto cm = select_option(
    "compression_mode",
    comp_mode.load(),
    [&]() {
      string val;
      if (c->pool_opts.get(pool_opts_t::COMPRESSION_MODE, &val)) {
	return std::optional<Compressor::CompressionMode>(
	  Compressor::get_comp_mode_type(val));
      }
      return std::optional<Compressor::CompressionMode>();
    }
  );

  wctx->compress = (cm != Compressor::COMP_NONE) &&
    ((cm == Compressor::COMP_FORCE) ||
     (cm == Compressor::COMP_AGGRESSIVE &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE) == 0) ||
     (cm == Compressor::COMP_PASSIVE &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE)));

  if ((alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ) == 0 &&
      (alloc_hints & (CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE |
                      CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY)) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE) == 0) {

    dout(20) << __func__ << " will prefer large blob and csum sizes" << dendl;

    if (o->onode.expected_write_size) {
      wctx->csum_order = std::max(min_alloc_size_order,
			          (uint8_t)std::countr_zero(o->onode.expected_write_size));
    } else {
      wctx->csum_order = min_alloc_size_order;
    }

    if (wctx->compress) {
      wctx->target_blob_size = select_option(
        "compression_max_blob_size",
        comp_max_blob_size.load(),
        [&]() {
          int64_t val;
          if (c->pool_opts.get(pool_opts_t::COMPRESSION_MAX_BLOB_SIZE, &val)) {
   	    return std::optional<uint64_t>((uint64_t)val);
          }
          return std::optional<uint64_t>();
        }
      );
    }
  } else {
    if (wctx->compress) {
      wctx->target_blob_size = select_option(
        "compression_min_blob_size",
        comp_min_blob_size.load(),
        [&]() {
          int64_t val;
          if (c->pool_opts.get(pool_opts_t::COMPRESSION_MIN_BLOB_SIZE, &val)) {
   	    return std::optional<uint64_t>((uint64_t)val);
          }
          return std::optional<uint64_t>();
        }
      );
    }
  }

  uint64_t max_bsize = max_blob_size.load();
  if (wctx->target_blob_size == 0 || wctx->target_blob_size > max_bsize) {
    wctx->target_blob_size = max_bsize;
  }

  // set the min blob size floor at 2x the min_alloc_size, or else we
  // won't be able to allocate a smaller extent for the compressed
  // data.
  if (wctx->compress &&
      wctx->target_blob_size < min_alloc_size * 2) {
    wctx->target_blob_size = min_alloc_size * 2;
  }

  dout(20) << __func__ << " prefer csum_order " << wctx->csum_order
           << " target_blob_size 0x" << std::hex << wctx->target_blob_size
	   << " compress=" << (int)wctx->compress
	   << " buffered=" << (int)wctx->buffered
           << std::dec << dendl;
}

int BlueStore::_do_gc(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  const WriteContext& wctx,
  uint64_t *dirty_start,
  uint64_t *dirty_end)
{

  bool dirty_range_updated = false;
  WriteContext wctx_gc;
  wctx_gc.fork(wctx); // make a clone for garbage collection

  auto & extents_to_collect = wctx.extents_to_gc;
  for (auto it = extents_to_collect.begin();
       it != extents_to_collect.end();
       ++it) {
    bufferlist bl;
    auto offset = (*it).first;
    auto length = (*it).second;
    dout(20) << __func__ << " processing " << std::hex
            << offset << "~" << length << std::dec
	    << dendl;
    int r = _do_read(c.get(), o, offset, length, bl, 0);
    ceph_assert(r == (int)length);

    _do_write_data(txc, c, o, offset, length, bl, &wctx_gc);
    logger->inc(l_bluestore_gc_merged, length);

    if (*dirty_start > offset) {
      *dirty_start = offset;
      dirty_range_updated = true;
    }

    if (*dirty_end < offset + length) {
      *dirty_end = offset + length;
      dirty_range_updated = true;
    }
  }
  if (dirty_range_updated) {
    o->extent_map.fault_range(db, *dirty_start, *dirty_end);
  }

  dout(30) << __func__ << " alloc write" << dendl;
  int r = _do_alloc_write(txc, c, o, &wctx_gc);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
         << dendl;
    return r;
  }

  _wctx_finish(txc, c, o, &wctx_gc);
  return 0;
}

int BlueStore::_do_write(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset,
  uint64_t length,
  bufferlist& bl,
  uint32_t fadvise_flags)
{
  int r = 0;

  dout(20) << __func__
	   << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length
	   << " - have 0x" << o->onode.size
	   << " (" << std::dec << o->onode.size << ")"
	   << " bytes" << std::hex
	   << " fadvise_flags 0x" << fadvise_flags
	   << " alloc_hint 0x" << o->onode.alloc_hint_flags
           << " expected_object_size " << o->onode.expected_object_size
           << " expected_write_size " << o->onode.expected_write_size
           << std::dec
	   << dendl;
  _dump_onode<30>(cct, *o);

  if (length == 0) {
    return 0;
  }

  uint64_t end = offset + length;

  GarbageCollector gc(c->store->cct);
  int64_t benefit = 0;
  auto dirty_start = offset;
  auto dirty_end = end;

  WriteContext wctx;
  _choose_write_options(c, o, fadvise_flags, &wctx);
  o->extent_map.fault_range(db, offset, length);
  _do_write_data(txc, c, o, offset, length, bl, &wctx);
  r = _do_alloc_write(txc, c, o, &wctx);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
	 << dendl;
    goto out;
  }

  if (wctx.extents_to_gc.empty() ||
      wctx.extents_to_gc.range_start() > offset ||
      wctx.extents_to_gc.range_end() < offset + length) {
    benefit = gc.estimate(offset,
			  length,
			  o->extent_map,
			  wctx.old_extents,
			  min_alloc_size);
  }

  // NB: _wctx_finish() will empty old_extents
  // so we must do gc estimation before that
  _wctx_finish(txc, c, o, &wctx);
  if (end > o->onode.size) {
    dout(20) << __func__ << " extending size to 0x" << std::hex << end
             << std::dec << dendl;
    o->onode.size = end;
  }

  if (benefit >= g_conf()->bluestore_gc_enable_total_threshold) {
    wctx.extents_to_gc.union_of(gc.get_extents_to_collect());
    dout(20) << __func__
             << " perform garbage collection for compressed extents, "
             << "expected benefit = " << benefit << " AUs" << dendl;
  }
  if (!wctx.extents_to_gc.empty()) {
    dout(20) << __func__ << " perform garbage collection" << dendl;

    r = _do_gc(txc, c, o,
      wctx,
      &dirty_start, &dirty_end);
    if (r < 0) {
      derr << __func__ << " _do_gc failed with " << cpp_strerror(r)
            << dendl;
      goto out;
    }
    dout(20)<<__func__<<" gc range is " << std::hex << dirty_start
	    << "~" << dirty_end - dirty_start << std::dec << dendl;
  }
  o->extent_map.compress_extent_map(dirty_start, dirty_end - dirty_start);
  o->extent_map.dirty_range(dirty_start, dirty_end - dirty_start);

  r = 0;

 out:
  return r;
}

int BlueStore::_write(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      uint64_t offset, size_t length,
		      bufferlist& bl,
		      uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(txc, o);
    r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);
    txc->write_onode(o);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_zero(TransContext *txc,
		     CollectionRef& c,
		     OnodeRef& o,
		     uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(txc, o);
    r = _do_zero(txc, c, o, offset, length);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_zero(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;

  _dump_onode<30>(cct, *o);

  WriteContext wctx;
  o->extent_map.fault_range(db, offset, length);
  o->extent_map.punch_hole(c, offset, length, &wctx.old_extents);
  o->extent_map.dirty_range(offset, length);
  _wctx_finish(txc, c, o, &wctx);

  if (length > 0 && offset + length > o->onode.size) {
    o->onode.size = offset + length;
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
  }
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

void BlueStore::_do_truncate(
  TransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t offset,
  set<SharedBlob*> *maybe_unshared_blobs)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec << dendl;

  _dump_onode<30>(cct, *o);

  if (offset == o->onode.size)
    return;

  WriteContext wctx;
  if (offset < o->onode.size) {
    uint64_t length = o->onode.size - offset;
    o->extent_map.fault_range(db, offset, length);
    o->extent_map.punch_hole(c, offset, length, &wctx.old_extents);
    o->extent_map.dirty_range(offset, length);

    _wctx_finish(txc, c, o, &wctx, maybe_unshared_blobs);

    // if we have shards past EOF, ask for a reshard
    if (!o->onode.extent_map_shards.empty() &&
	o->onode.extent_map_shards.back().offset >= offset) {
      dout(10) << __func__ << "  request reshard past EOF" << dendl;
      if (offset) {
	o->extent_map.request_reshard(offset - 1, offset + length);
      } else {
	o->extent_map.request_reshard(0, length);
      }
    }
  }

  o->onode.size = offset;

  txc->write_onode(o);
}

int BlueStore::_truncate(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << dendl;

  auto start_time = mono_clock::now();
  int r = 0;
  if (offset >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _do_truncate(txc, c, o, offset);
  }
  log_latency_fn(
    __func__,
    l_bluestore_truncate_lat,
    mono_clock::now() - start_time,
    cct->_conf->bluestore_log_op_age,
    [&](const ceph::timespan& lat) {
      ostringstream ostr;
      ostr << ", lat = " << timespan_str(lat)
        << " cid =" << c->cid
        << " oid =" << o->oid;
      return ostr.str();
    }
  );
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_remove(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o)
{
  set<SharedBlob*> maybe_unshared_blobs;
  bool is_gen = !o->oid.is_no_gen();
  _do_truncate(txc, c, o, 0, is_gen ? &maybe_unshared_blobs : nullptr);
  if (o->onode.has_omap()) {
    o->flush();
    _do_omap_clear(txc, o);
  }
  o->exists = false;
  string key;
  for (auto &s : o->extent_map.shards) {
    dout(20) << __func__ << "  removing shard 0x" << std::hex
	     << s.shard_info->offset << std::dec << dendl;
    generate_extent_shard_key_and_apply(o->key, s.shard_info->offset, &key,
      [&](const string& final_key) {
        txc->t->rmkey(PREFIX_OBJ, final_key);
      }
    );
  }
  txc->t->rmkey(PREFIX_OBJ, o->key.c_str(), o->key.size());
  txc->note_removed_object(o);
  o->extent_map.clear();
  o->onode = bluestore_onode_t();
  _debug_obj_on_delete(o->oid);

  if (!is_gen || maybe_unshared_blobs.empty()) {
    return 0;
  }

  // see if we can unshare blobs still referenced by the head
  dout(10) << __func__ << " gen and maybe_unshared_blobs "
	   << maybe_unshared_blobs << dendl;
  ghobject_t nogen = o->oid;
  nogen.generation = ghobject_t::NO_GEN;
  OnodeRef h = c->get_onode(nogen, false);

  if (!h || !h->exists) {
    return 0;
  }
  // Set maybe_unshared_blobs contains those shared blobs that have all nref=1.
  // Is .head object is using all those segments?
  // If it is using all, then no one else can use the shared blob,
  // and we can fallback to regular non-shared blob.

  // Note. We only process loaded shared blobs.
  // This is very smart optimization - there is no way that we can unshare blob
  // that is not yet loaded! We must have had inspected it to even check nrefs.
  dout(20) << __func__ << " checking for unshareable blobs on " << h
	   << " " << h->oid << dendl;
  map<SharedBlob*,bluestore_extent_ref_map_t> expect;
  for (auto& e : h->extent_map.extent_map) {
    const bluestore_blob_t& b = e.blob->get_blob();
    SharedBlob *sb = e.blob->shared_blob.get();
    if (b.is_shared() &&
	sb->loaded &&
	maybe_unshared_blobs.count(sb)) {
      if (b.is_compressed()) {
	expect[sb].get(0, b.get_ondisk_length());
      } else {
	// todo: it seems to be an overkill to go through map()
	b.map(e.blob_offset, e.length, [&](uint64_t off, uint64_t len) {
	    expect[sb].get(off, len);
	    return 0;
	  });
      }
    }
  }

  // expect has now refs set exactly as .head is using it
  vector<SharedBlob*> unshared_blobs;
  unshared_blobs.reserve(maybe_unshared_blobs.size());
  for (auto& p : expect) {
    dout(20) << " ? " << *p.first << " vs " << p.second << dendl;
    if (p.first->persistent->ref_map == p.second) {
      // yup, .head is only one that is using the shared blob now
      SharedBlob *sb = p.first;
      dout(20) << __func__ << "  unsharing " << *sb << dendl;
      unshared_blobs.push_back(sb);
      txc->unshare_blob(sb);
      uint64_t sbid = c->make_blob_unshared(sb);
      string key;
      get_shared_blob_key(sbid, &key);
      txc->t->rmkey(PREFIX_SHARED_BLOB, key);
    }
  }

  if (unshared_blobs.empty()) {
    return 0;
  }

  // And now a run through .head extents to clear up freshly unshared blobs.
  for (auto& e : h->extent_map.extent_map) {
    const bluestore_blob_t& b = e.blob->get_blob();
    SharedBlob *sb = e.blob->shared_blob.get();
    if (b.is_shared() &&
        std::find(unshared_blobs.begin(), unshared_blobs.end(),
                  sb) != unshared_blobs.end()) {
      dout(20) << __func__ << "  unsharing " << e << dendl;
      bluestore_blob_t& blob = e.blob->dirty_blob();
      blob.clear_flag(bluestore_blob_t::FLAG_SHARED);
      if (e.blob->shared_blob->nref > 1) {
	// Each blob on creation gets its own unique (empty) shared_blob.
	// In function ExtentMap::dup() we sometimes merge 2 blobs,
	// so they share common shared_blob used for ref counting.
	// Imagine 2 blobs having same shared_blob, and shared blob gets just unshared.
	// We cleared shared_blob content so it is now logically empty,
	// but now those 2 blobs share it.
	// This is illegal, as empty shared blobs should be unique.
	// Fixing by re-creation.

	// Here we skip set_shared_blob() because e.blob is already in BufferCacheShard
	// and cannot do add_blob() twice
	e.blob->shared_blob = new SharedBlob(c.get());
      }
      h->extent_map.dirty_range(e.logical_offset, 1);
    }
  }
  txc->write_onode(h);

  return 0;
}

int BlueStore::_remove(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " onode " << o.get()
	   << " txc "<< txc << dendl;
 auto start_time = mono_clock::now();
  int r = _do_remove(txc, c, o);

  log_latency_fn(
    __func__,
    l_bluestore_remove_lat,
    mono_clock::now() - start_time,
    cct->_conf->bluestore_log_op_age,
    [&](const ceph::timespan& lat) {
      ostringstream ostr;
      ostr << ", lat = " << timespan_str(lat)
        << " cid =" << c->cid
        << " oid =" << o->oid;
      return ostr.str();
    }
  );

  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_setattr(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const string& name,
			bufferptr& val)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << dendl;
  int r = 0;
  if (val.is_partial()) {
    auto& b = o->onode.attrs[name.c_str()] = bufferptr(val.c_str(),
						       val.length());
    b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
  } else {
    auto& b = o->onode.attrs[name.c_str()] = val;
    b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
  }
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_setattrs(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 const map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << aset.size() << " keys"
	   << dendl;
  int r = 0;
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end(); ++p) {
    if (p->second.is_partial()) {
      auto& b = o->onode.attrs[p->first.c_str()] =
	bufferptr(p->second.c_str(), p->second.length());
      b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    } else {
      auto& b = o->onode.attrs[p->first.c_str()] = p->second;
      b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    }
  }
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << aset.size() << " keys"
	   << " = " << r << dendl;
  return r;
}


int BlueStore::_rmattr(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef& o,
		       const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << dendl;
  int r = 0;
  auto it = o->onode.attrs.find(name.c_str());
  if (it == o->onode.attrs.end())
    goto out;

  o->onode.attrs.erase(it);
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int BlueStore::_rmattrs(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;

  if (o->onode.attrs.empty())
    goto out;

  o->onode.attrs.clear();
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

void BlueStore::_do_omap_clear(TransContext *txc, OnodeRef& o)
{
  const string& omap_prefix = o->get_omap_prefix();
  string prefix, tail;
  o->get_omap_header(&prefix);
  o->get_omap_tail(&tail);
  txc->t->rm_range_keys(omap_prefix, prefix, tail);
  txc->t->rmkey(omap_prefix, tail);
  o->onode.clear_omap_flag();
  dout(20) << __func__ << " remove range start: "
           << pretty_binary_string(prefix) << " end: "
           << pretty_binary_string(tail) << dendl;
}

int BlueStore::_omap_clear(TransContext *txc,
			   CollectionRef& c,
			   OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  auto t0 = mono_clock::now();

  int r = 0;
  if (o->onode.has_omap()) {
    o->flush();
    _do_omap_clear(txc, o);
    txc->write_onode(o);
  }
  logger->tinc(l_bluestore_omap_clear_lat, mono_clock::now() - t0);

  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_setkeys(TransContext *txc,
			     CollectionRef& c,
			     OnodeRef& o,
			     bufferlist &bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r;
  auto p = bl.cbegin();
  __u32 num;
  if (!o->onode.has_omap()) {
    if (o->oid.is_pgmeta()) {
      o->onode.set_omap_flags_pgmeta();
    } else {
      o->onode.set_omap_flags(per_pool_omap == OMAP_BULK);
    }
    txc->write_onode(o);

    const string& prefix = o->get_omap_prefix();
    string key_tail;
    bufferlist tail;
    o->get_omap_tail(&key_tail);
    txc->t->set(prefix, key_tail, tail);
  } else {
    txc->note_modified_object(o);
  }
  const string& prefix = o->get_omap_prefix();
  string final_key;
  o->get_omap_key(string(), &final_key);
  size_t base_key_len = final_key.size();
  decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    decode(key, p);
    decode(value, p);
    final_key.resize(base_key_len); // keep prefix
    final_key += key;
    dout(20) << __func__ << "  " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->set(prefix, final_key, value);
  }
  r = 0;
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_setheader(TransContext *txc,
			       CollectionRef& c,
			       OnodeRef& o,
			       bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r;
  string key;
  if (!o->onode.has_omap()) {
    if (o->oid.is_pgmeta()) {
      o->onode.set_omap_flags_pgmeta();
    } else {
      o->onode.set_omap_flags(per_pool_omap == OMAP_BULK);
    }
    txc->write_onode(o);

    const string& prefix = o->get_omap_prefix();
    string key_tail;
    bufferlist tail;
    o->get_omap_tail(&key_tail);
    txc->t->set(prefix, key_tail, tail);
  } else {
    txc->note_modified_object(o);
  }
  const string& prefix = o->get_omap_prefix();
  o->get_omap_header(&key);
  txc->t->set(prefix, key, bl);
  r = 0;
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_rmkeys(TransContext *txc,
			    CollectionRef& c,
			    OnodeRef& o,
			    bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  auto p = bl.cbegin();
  __u32 num;
  string final_key;
  if (!o->onode.has_omap()) {
    goto out;
  }
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    decode(num, p);
    logger->inc(l_bluestore_omap_rmkeys_count, num);
    while (num--) {
      string key;
      decode(key, p);
      final_key.resize(base_key_len); // keep prefix
      final_key += key;
      dout(20) << __func__ << "  rm " << pretty_binary_string(final_key)
	       << " <- " << key << dendl;
      txc->t->rmkey(prefix, final_key);
    }
  }
  txc->note_modified_object(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_rmkey_range(TransContext *txc,
				 CollectionRef& c,
				 OnodeRef& o,
				 const string& first, const string& last)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  string key_first, key_last;
  int r = 0;
  if (!o->onode.has_omap()) {
    goto out;
  }
  {
    const string& prefix = o->get_omap_prefix();
    o->flush();
    o->get_omap_key(first, &key_first);
    o->get_omap_key(last, &key_last);
    logger->inc(l_bluestore_omap_rmkey_ranges_count);
    txc->t->rm_range_keys(prefix, key_first, key_last);
    dout(20) << __func__ << " remove range start: "
             << pretty_binary_string(key_first) << " end: "
             << pretty_binary_string(key_last) << dendl;
  }
  txc->note_modified_object(o);

 out:
  return r;
}

int BlueStore::_set_alloc_hint(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  uint64_t expected_object_size,
  uint64_t expected_write_size,
  uint32_t flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " flags " << ceph_osd_alloc_hint_flag_string(flags)
	   << dendl;
  int r = 0;
  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  o->onode.alloc_hint_flags = flags;
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " flags " << ceph_osd_alloc_hint_flag_string(flags)
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_clone(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& oldo,
		      OnodeRef& newo)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << dendl;
  int r = 0;
  if (oldo->oid.hobj.get_hash() != newo->oid.hobj.get_hash()) {
    derr << __func__ << " mismatched hash on " << oldo->oid
	 << " and " << newo->oid << dendl;
    return -EINVAL;
  }

  _assign_nid(txc, newo);

  // clone data
  oldo->flush();
  _do_truncate(txc, c, newo, 0);
  if (cct->_conf->bluestore_clone_cow) {
    _do_clone_range(txc, c, oldo, newo, 0, oldo->onode.size, 0);
  } else {
    bufferlist bl;
    r = _do_read(c.get(), oldo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
      goto out;
    r = _do_write(txc, c, newo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
      goto out;
  }

  // clone attrs
  newo->onode.attrs = oldo->onode.attrs;

  // clone omap
  if (newo->onode.has_omap()) {
    dout(20) << __func__ << " clearing old omap data" << dendl;
    newo->flush();
    _do_omap_clear(txc, newo);
  }
  if (oldo->onode.has_omap()) {
    dout(20) << __func__ << " copying omap data" << dendl;
    if (newo->oid.is_pgmeta()) {
      newo->onode.set_omap_flags_pgmeta();
    } else {
      newo->onode.set_omap_flags(per_pool_omap == OMAP_BULK);
    }
    // check if prefix for omap key is exactly the same size for both objects
    // otherwise rewrite_omap_key will corrupt data
    ceph_assert(oldo->onode.flags == newo->onode.flags);
    const string& prefix = newo->get_omap_prefix();
    string head, tail;
    oldo->get_omap_header(&head);
    oldo->get_omap_tail(&tail);
    KeyValueDB::Iterator it = db->get_iterator(prefix, 0, KeyValueDB::IteratorBounds{head, tail});
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	dout(30) << __func__ << "  got header/data "
		 << pretty_binary_string(it->key()) << dendl;
        string key;
	newo->rewrite_omap_key(it->key(), &key);
	txc->t->set(prefix, key, it->value());
      }
      it->next();
    }
    string new_tail;
    bufferlist new_tail_value;
    newo->get_omap_tail(&new_tail);
    txc->t->set(prefix, new_tail, new_tail_value);
  }

  txc->write_onode(newo);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_do_clone_range(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& oldo,
  OnodeRef& newo,
  uint64_t srcoff,
  uint64_t length,
  uint64_t dstoff)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid
	   << " 0x" << std::hex << srcoff << "~" << length << " -> "
	   << " 0x" << dstoff << "~" << length << std::dec << dendl;
  oldo->extent_map.fault_range(db, srcoff, length);
  newo->extent_map.fault_range(db, dstoff, length);
  _dump_onode<30>(cct, *oldo);
  _dump_onode<30>(cct, *newo);

  if (elastic_shared_blobs) {
    oldo->extent_map.dup_esb(this, txc, c, oldo, newo, srcoff, length, dstoff);
  } else {
    oldo->extent_map.dup(this, txc, c, oldo, newo, srcoff, length, dstoff);
  }

  _dump_onode<30>(cct, *oldo);
  _dump_onode<30>(cct, *newo);
  return 0;
}

int BlueStore::_clone_range(TransContext *txc,
			    CollectionRef& c,
			    OnodeRef& oldo,
			    OnodeRef& newo,
			    uint64_t srcoff, uint64_t length, uint64_t dstoff)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << " from 0x" << std::hex << srcoff << "~" << length
	   << " to offset 0x" << dstoff << std::dec << dendl;
  int r = 0;

  if (srcoff + length >= OBJECT_MAX_SIZE ||
      dstoff + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
    goto out;
  }
  if (srcoff + length > oldo->onode.size) {
    r = -EINVAL;
    goto out;
  }

  _assign_nid(txc, newo);

  if (length > 0) {
    if (cct->_conf->bluestore_clone_cow) {
      _do_zero(txc, c, newo, dstoff, length);
      _do_clone_range(txc, c, oldo, newo, srcoff, length, dstoff);
    } else {
      bufferlist bl;
      r = _do_read(c.get(), oldo, srcoff, length, bl, 0);
      if (r < 0)
	goto out;
      r = _do_write(txc, c, newo, dstoff, bl.length(), bl, 0);
      if (r < 0)
	goto out;
    }
  }

  txc->write_onode(newo);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << " from 0x" << std::hex << srcoff << "~" << length
	   << " to offset 0x" << dstoff << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_rename(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef& oldo,
		       OnodeRef& newo,
		       const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << new_oid << dendl;
  int r;
  ghobject_t old_oid = oldo->oid;
  mempool::bluestore_cache_meta::string new_okey;

  if (newo) {
    if (newo->exists) {
      r = -EEXIST;
      goto out;
    }
    ceph_assert(txc->onodes.count(newo) == 0);
  }

  txc->t->rmkey(PREFIX_OBJ, oldo->key.c_str(), oldo->key.size());

  // rewrite shards
  {
    oldo->extent_map.fault_range(db, 0, oldo->onode.size);
    get_object_key(cct, new_oid, &new_okey);
    string key;
    for (auto &s : oldo->extent_map.shards) {
      generate_extent_shard_key_and_apply(oldo->key, s.shard_info->offset, &key,
        [&](const string& final_key) {
          txc->t->rmkey(PREFIX_OBJ, final_key);
        }
      );
      s.dirty = true;
    }
  }

  newo = oldo;
  txc->write_onode(newo);

  // this adjusts oldo->{oid,key}, and reset oldo to a fresh empty
  // Onode in the old slot
  c->onode_space.rename(oldo, old_oid, new_oid, new_okey);
  r = 0;

  // hold a ref to new Onode in old name position, to ensure we don't drop
  // it from the cache before this txc commits (or else someone may come along
  // and read newo's metadata via the old name).
  txc->note_modified_object(oldo);

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

// collections

int BlueStore::_create_collection(
  TransContext *txc,
  const coll_t &cid,
  unsigned bits,
  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << " bits " << bits << dendl;
  int r;
  bufferlist bl;

  {
    std::unique_lock l(coll_lock);
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    auto p = new_coll_map.find(cid);
    ceph_assert(p != new_coll_map.end());
    *c = p->second;
    (*c)->cnode.bits = bits;
    coll_map[cid] = *c;
    new_coll_map.erase(p);
  }
  encode((*c)->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(cid), bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " bits " << bits << " = " << r << dendl;
  return r;
}

int BlueStore::_remove_collection(TransContext *txc, const coll_t &cid,
				  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;

  (*c)->flush_all_but_last();
  {
    std::unique_lock l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    size_t nonexistent_count = 0;
    ceph_assert((*c)->exists);
    if ((*c)->onode_space.map_any([&](Onode* o) {
      if (o->exists) {
        dout(1) << __func__ << " " << o->oid << " " << o
	        << " exists in onode_map" << dendl;
          return true;
      }
      ++nonexistent_count;
      return false;
    })) {
      r = -ENOTEMPTY;
      goto out;
    }
    vector<ghobject_t> ls;
    ghobject_t next;
    // Enumerate onodes in db, up to nonexistent_count + 1
    // then check if all of them are marked as non-existent.
    // Bypass the check if (next != ghobject_t::get_max())
    r = _collection_list(c->get(), ghobject_t(), ghobject_t::get_max(),
                         nonexistent_count + 1, false, &ls, &next);
    if (r >= 0) {
      // If true mean collecton has more objects than nonexistent_count,
      // so bypass check.
      bool exists = (!next.is_max());
      for (auto it = ls.begin(); !exists && it < ls.end(); ++it) {
        dout(10) << __func__ << " oid " << *it << dendl;
        auto onode = (*c)->onode_space.lookup(*it);
        exists = !onode || onode->exists;
        if (exists) {
          dout(1) << __func__ << " " << *it
	  << " exists in db, "
	  << (!onode ? "not present in ram" : "present in ram")
	  << dendl;
        }
      }
      if (!exists) {
        _do_remove_collection(txc, c);
        r = 0;
      } else {
        dout(10) << __func__ << " " << cid
                 << " is non-empty" << dendl;
	r = -ENOTEMPTY;
      }
    }
  }
out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

void BlueStore::_do_remove_collection(TransContext *txc,
				      CollectionRef *c)
{
  coll_map.erase((*c)->cid);
  txc->removed_collections.push_back(*c);
  (*c)->exists = false;
  _osr_register_zombie((*c)->osr.get());
  txc->t->rmkey(PREFIX_COLL, stringify((*c)->cid));
  c->reset();
}

int BlueStore::_split_collection(TransContext *txc,
				CollectionRef& c,
				CollectionRef& d,
				unsigned bits, int rem)
{
  dout(15) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << dendl;
  std::unique_lock l(c->lock);
  std::unique_lock l2(d->lock);
  int r;

  // flush all previous deferred writes on this sequencer.  this is a bit
  // heavyweight, but we need to make sure all deferred writes complete
  // before we split as the new collection's sequencer may need to order
  // this after those writes, and we don't bother with the complexity of
  // moving those TransContexts over to the new osr.
  _osr_drain_preceding(txc);

  // move any cached items (onodes and referenced shared blobs) that will
  // belong to the child collection post-split.  leave everything else behind.
  // this may include things that don't strictly belong to the now-smaller
  // parent split, but the OSD will always send us a split for every new
  // child.

  spg_t pgid, dest_pgid;
  bool is_pg = c->cid.is_pg(&pgid);
  ceph_assert(is_pg);
  is_pg = d->cid.is_pg(&dest_pgid);
  ceph_assert(is_pg);

  // the destination should initially be empty.
  ceph_assert(d->onode_space.empty());
  ceph_assert(d->shared_blob_set.empty());
  ceph_assert(d->cnode.bits == bits);

  c->split_cache(d.get());

  // adjust bits.  note that this will be redundant for all but the first
  // split call for this parent (first child).
  c->cnode.bits = bits;
  ceph_assert(d->cnode.bits == bits);
  r = 0;

  bufferlist bl;
  encode(c->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(c->cid), bl);

  dout(10) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

int BlueStore::_merge_collection(
  TransContext *txc,
  CollectionRef *c,
  CollectionRef& d,
  unsigned bits)
{
  dout(15) << __func__ << " " << (*c)->cid << " to " << d->cid
	   << " bits " << bits << dendl;
  std::unique_lock l((*c)->lock);
  std::unique_lock l2(d->lock);
  int r;

  coll_t cid = (*c)->cid;

  // flush all previous deferred writes on the source collection to ensure
  // that all deferred writes complete before we merge as the target collection's
  // sequencer may need to order new ops after those writes.

  _osr_drain((*c)->osr.get());

  // move any cached items (onodes and referenced shared blobs) that will
  // belong to the child collection post-split.  leave everything else behind.
  // this may include things that don't strictly belong to the now-smaller
  // parent split, but the OSD will always send us a split for every new
  // child.

  spg_t pgid, dest_pgid;
  bool is_pg = cid.is_pg(&pgid);
  ceph_assert(is_pg);
  is_pg = d->cid.is_pg(&dest_pgid);
  ceph_assert(is_pg);

  // adjust bits.  note that this will be redundant for all but the first
  // merge call for the parent/target.
  d->cnode.bits = bits;

  // behavior depends on target (d) bits, so this after that is updated.
  (*c)->split_cache(d.get());

  // remove source collection
  {
    std::unique_lock l3(coll_lock);
    _do_remove_collection(txc, c);
  }

  r = 0;

  bufferlist bl;
  encode(d->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(d->cid), bl);

  dout(10) << __func__ << " " << cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

void BlueStore::log_latency(
  const char* name,
  int idx,
  const ceph::timespan& l,
  double lat_threshold,
  const char* info,
  int idx2) const
{
  logger->tinc(idx, l);
  if (lat_threshold > 0.0 &&
      l >= make_timespan(lat_threshold)) {
    dout(0) << __func__ << " slow operation observed for " << name
      << ", latency = " << l
      << info
      << dendl;
    if (idx2 > l_bluestore_first && idx2 < l_bluestore_last) {
      logger->inc(idx2);
    }
  }
}

void BlueStore::log_latency_fn(
  const char* name,
  int idx,
  const ceph::timespan& l,
  double lat_threshold,
  std::function<string (const ceph::timespan& lat)> fn,
  int idx2) const
{
  logger->tinc(idx, l);
  if (lat_threshold > 0.0 &&
      l >= make_timespan(lat_threshold)) {
    dout(0) << __func__ << " slow operation observed for " << name
      << ", latency = " << l
      << fn(l)
      << dendl;
    if (idx2 > l_bluestore_first && idx2 < l_bluestore_last) {
      logger->inc(idx2);
    }
  }
}

#if defined(WITH_LTTNG)
void BlueStore::BlueStoreThrottle::emit_initial_tracepoint(
  KeyValueDB &db,
  TransContext &txc,
  mono_clock::time_point start_throttle_acquire)
{
  pending_kv_ios += txc.ios;
  if (txc.deferred_txn) {
    pending_deferred_ios += txc.ios;
  }

  uint64_t started = 0;
  uint64_t completed = 0;
  if (should_trace(&started, &completed)) {
    txc.tracing = true;
    uint64_t rocksdb_base_level,
      rocksdb_estimate_pending_compaction_bytes,
      rocksdb_cur_size_all_mem_tables,
      rocksdb_compaction_pending,
      rocksdb_mem_table_flush_pending,
      rocksdb_num_running_compactions,
      rocksdb_num_running_flushes,
      rocksdb_actual_delayed_write_rate;
    db.get_property(
      "rocksdb.base-level",
      &rocksdb_base_level);
    db.get_property(
      "rocksdb.estimate-pending-compaction-bytes",
      &rocksdb_estimate_pending_compaction_bytes);
    db.get_property(
      "rocksdb.cur-size-all-mem-tables",
      &rocksdb_cur_size_all_mem_tables);
    db.get_property(
      "rocksdb.compaction-pending",
      &rocksdb_compaction_pending);
    db.get_property(
      "rocksdb.mem-table-flush-pending",
      &rocksdb_mem_table_flush_pending);
    db.get_property(
      "rocksdb.num-running-compactions",
      &rocksdb_num_running_compactions);
    db.get_property(
      "rocksdb.num-running-flushes",
      &rocksdb_num_running_flushes);
    db.get_property(
      "rocksdb.actual-delayed-write-rate",
      &rocksdb_actual_delayed_write_rate);

  
    tracepoint(
      bluestore,
      transaction_initial_state,
      txc.osr->get_sequencer_id(),
      txc.seq,
      throttle_bytes.get_current(),
      throttle_deferred_bytes.get_current(),
      pending_kv_ios,
      pending_deferred_ios,
      started,
      completed,
      ceph::to_seconds<double>(mono_clock::now() - start_throttle_acquire));

    tracepoint(
      bluestore,
      transaction_initial_state_rocksdb,
      txc.osr->get_sequencer_id(),
      txc.seq,
      rocksdb_base_level,
      rocksdb_estimate_pending_compaction_bytes,
      rocksdb_cur_size_all_mem_tables,
      rocksdb_compaction_pending,
      rocksdb_mem_table_flush_pending,
      rocksdb_num_running_compactions,
      rocksdb_num_running_flushes,
      rocksdb_actual_delayed_write_rate);
  }
}
#endif

mono_clock::duration BlueStore::BlueStoreThrottle::log_state_latency(
  TransContext &txc, PerfCounters *logger, int state)
{
  mono_clock::time_point now = mono_clock::now();
  mono_clock::duration lat = now - txc.last_stamp;
  logger->tinc(state, lat);
#if defined(WITH_LTTNG)
  if (txc.tracing &&
      state >= l_bluestore_state_prepare_lat &&
      state <= l_bluestore_state_done_lat) {
    OID_ELAPSED("", lat.to_nsec() / 1000.0, txc.get_state_latency_name(state));
    tracepoint(
      bluestore,
      transaction_state_duration,
      txc.osr->get_sequencer_id(),
      txc.seq,
      state,
      ceph::to_seconds<double>(lat));
  }
#endif
  txc.last_stamp = now;
  return lat;
}

bool BlueStore::BlueStoreThrottle::try_start_transaction(
  KeyValueDB &db,
  TransContext &txc,
  mono_clock::time_point start_throttle_acquire)
{
  throttle_bytes.get(txc.cost);

  if (!txc.deferred_txn || throttle_deferred_bytes.get_or_fail(txc.cost)) {
    emit_initial_tracepoint(db, txc, start_throttle_acquire);
    return true;
  } else {
    return false;
  }
}

void BlueStore::BlueStoreThrottle::finish_start_transaction(
  KeyValueDB &db,
  TransContext &txc,
  mono_clock::time_point start_throttle_acquire)
{
  ceph_assert(txc.deferred_txn);
  throttle_deferred_bytes.get(txc.cost);
  emit_initial_tracepoint(db, txc, start_throttle_acquire);
}

#if defined(WITH_LTTNG)
void BlueStore::BlueStoreThrottle::complete_kv(TransContext &txc)
{
  pending_kv_ios -= 1;
  ios_completed_since_last_traced++;
  if (txc.tracing) {
    tracepoint(
      bluestore,
      transaction_commit_latency,
      txc.osr->get_sequencer_id(),
      txc.seq,
      ceph::to_seconds<double>(mono_clock::now() - txc.start));
  }
}
#endif

#if defined(WITH_LTTNG)
void BlueStore::BlueStoreThrottle::complete(TransContext &txc)
{
  if (txc.deferred_txn) {
    pending_deferred_ios -= 1;
  }
  if (txc.tracing) {
    mono_clock::time_point now = mono_clock::now();
    mono_clock::duration lat = now - txc.start;
    tracepoint(
      bluestore,
      transaction_total_duration,
      txc.osr->get_sequencer_id(),
      txc.seq,
      ceph::to_seconds<double>(lat));
  }
}
#endif

const string prefix_onode = "o";
const string prefix_onode_shard = "x";
const string prefix_other = "Z";
//Itrerates through the db and collects the stats
void BlueStore::generate_db_histogram(Formatter *f)
{
  //globals
  uint64_t num_onodes = 0;
  uint64_t num_shards = 0;
  uint64_t num_super = 0;
  uint64_t num_coll = 0;
  uint64_t num_omap = 0;
  uint64_t num_pgmeta_omap = 0;
  uint64_t num_deferred = 0;
  uint64_t num_alloc = 0;
  uint64_t num_stat = 0;
  uint64_t num_others = 0;
  uint64_t num_shared_shards = 0;
  size_t max_key_size =0, max_value_size = 0;
  uint64_t total_key_size = 0, total_value_size = 0;
  size_t key_size = 0, value_size = 0;
  KeyValueHistogram hist;

  auto start = coarse_mono_clock::now();

  KeyValueDB::WholeSpaceIterator iter = db->get_wholespace_iterator();
  iter->seek_to_first();
  while (iter->valid()) {
    dout(30) << __func__ << " Key: " << iter->key() << dendl;
    key_size = iter->key_size();
    value_size = iter->value_size();
    hist.value_hist[hist.get_value_slab(value_size)]++;
    max_key_size = std::max(max_key_size, key_size);
    max_value_size = std::max(max_value_size, value_size);
    total_key_size += key_size;
    total_value_size += value_size;

    pair<string,string> key(iter->raw_key());

    if (key.first == PREFIX_SUPER) {
	hist.update_hist_entry(hist.key_hist, PREFIX_SUPER, key_size, value_size);
	num_super++;
    } else if (key.first == PREFIX_STAT) {
	hist.update_hist_entry(hist.key_hist, PREFIX_STAT, key_size, value_size);
	num_stat++;
    } else if (key.first == PREFIX_COLL) {
	hist.update_hist_entry(hist.key_hist, PREFIX_COLL, key_size, value_size);
	num_coll++;
    } else if (key.first == PREFIX_OBJ) {
      if (key.second.back() == ONODE_KEY_SUFFIX) {
	hist.update_hist_entry(hist.key_hist, prefix_onode, key_size, value_size);
	num_onodes++;
      } else {
	hist.update_hist_entry(hist.key_hist, prefix_onode_shard, key_size, value_size);
	num_shards++;
      }
    } else if (key.first == PREFIX_OMAP) {
	hist.update_hist_entry(hist.key_hist, PREFIX_OMAP, key_size, value_size);
	num_omap++;
    } else if (key.first == PREFIX_PERPOOL_OMAP) {
	hist.update_hist_entry(hist.key_hist, PREFIX_PERPOOL_OMAP, key_size, value_size);
	num_omap++;
    } else if (key.first == PREFIX_PERPG_OMAP) {
	hist.update_hist_entry(hist.key_hist, PREFIX_PERPG_OMAP, key_size, value_size);
	num_omap++;
    } else if (key.first == PREFIX_PGMETA_OMAP) {
	hist.update_hist_entry(hist.key_hist, PREFIX_PGMETA_OMAP, key_size, value_size);
	num_pgmeta_omap++;
    } else if (key.first == PREFIX_DEFERRED) {
	hist.update_hist_entry(hist.key_hist, PREFIX_DEFERRED, key_size, value_size);
	num_deferred++;
    } else if (key.first == PREFIX_ALLOC || key.first == PREFIX_ALLOC_BITMAP) {
	hist.update_hist_entry(hist.key_hist, PREFIX_ALLOC, key_size, value_size);
	num_alloc++;
    } else if (key.first == PREFIX_SHARED_BLOB) {
	hist.update_hist_entry(hist.key_hist, PREFIX_SHARED_BLOB, key_size, value_size);
	num_shared_shards++;
    } else {
	hist.update_hist_entry(hist.key_hist, prefix_other, key_size, value_size);
	num_others++;
    }
    iter->next();
  }

  ceph::timespan duration = coarse_mono_clock::now() - start;
  f->open_object_section("rocksdb_key_value_stats");
  f->dump_unsigned("num_onodes", num_onodes);
  f->dump_unsigned("num_shards", num_shards);
  f->dump_unsigned("num_super", num_super);
  f->dump_unsigned("num_coll", num_coll);
  f->dump_unsigned("num_omap", num_omap);
  f->dump_unsigned("num_pgmeta_omap", num_pgmeta_omap);
  f->dump_unsigned("num_deferred", num_deferred);
  f->dump_unsigned("num_alloc", num_alloc);
  f->dump_unsigned("num_stat", num_stat);
  f->dump_unsigned("num_shared_shards", num_shared_shards);
  f->dump_unsigned("num_others", num_others);
  f->dump_unsigned("max_key_size", max_key_size);
  f->dump_unsigned("max_value_size", max_value_size);
  f->dump_unsigned("total_key_size", total_key_size);
  f->dump_unsigned("total_value_size", total_value_size);
  f->close_section();

  hist.dump(f);

  dout(20) << __func__ << " finished in " << duration << " seconds" << dendl;

}

void BlueStore::_shutdown_cache()
{
  dout(10) << __func__ << dendl;
  for (auto i : buffer_cache_shards) {
    i->flush();
    ceph_assert(i->empty());
  }
  for (auto& p : coll_map) {
    p.second->onode_space.clear();
    if (!p.second->shared_blob_set.empty()) {
      derr << __func__ << " stray shared blobs on " << p.first << dendl;
      p.second->shared_blob_set.dump<0>(cct);
    }
    ceph_assert(p.second->onode_space.empty());
    ceph_assert(p.second->shared_blob_set.empty());
  }
  coll_map.clear();
  for (auto i : onode_cache_shards) {
    ceph_assert(i->empty());
  }
}

// For external caller.
// We use a best-effort policy instead, e.g.,
// we don't care if there are still some pinned onodes/data in the cache
// after this command is completed.
int BlueStore::flush_cache(ostream *os)
{
  dout(10) << __func__ << dendl;
  for (auto i : onode_cache_shards) {
    i->flush();
  }
  for (auto i : buffer_cache_shards) {
    i->flush();
  }

  return 0;
}

void BlueStore::_apply_padding(uint64_t head_pad,
			       uint64_t tail_pad,
			       bufferlist& padded)
{
  if (head_pad) {
    padded.prepend_zero(head_pad);
  }
  if (tail_pad) {
    padded.append_zero(tail_pad);
  }
  if (head_pad || tail_pad) {
    dout(20) << __func__ << "  can pad head 0x" << std::hex << head_pad
	      << " tail 0x" << tail_pad << std::dec << dendl;
    logger->inc(l_bluestore_write_pad_bytes, head_pad + tail_pad);
  }
}

void BlueStore::_record_onode(OnodeRef& o, KeyValueDB::Transaction &txn)
{
  // finalize extent_map shards
  o->extent_map.update(txn, false);
  if (o->extent_map.needs_reshard()) {
    o->extent_map.reshard(db, txn);
    o->extent_map.update(txn, true);
    if (o->extent_map.needs_reshard()) {
      dout(20) << __func__ << " warning: still wants reshard, check options?"
		<< dendl;
      o->extent_map.clear_needs_reshard();
    }
    logger->inc(l_bluestore_onode_reshard);
  }

  // bound encode
  size_t bound = 0;
  denc(o->onode, bound);
  o->extent_map.bound_encode_spanning_blobs(bound);
  if (o->onode.extent_map_shards.empty()) {
    denc(o->extent_map.inline_bl, bound);
  }

  // encode
  bufferlist bl;
  unsigned onode_part, blob_part, extent_part;
  {
    auto p = bl.get_contiguous_appender(bound, true);
    denc(o->onode, p);
    onode_part = p.get_logical_offset();
    o->extent_map.encode_spanning_blobs(p);
    blob_part = p.get_logical_offset() - onode_part;
    if (o->onode.extent_map_shards.empty()) {
      denc(o->extent_map.inline_bl, p);
    }
    extent_part = p.get_logical_offset() - onode_part - blob_part;
  }

  dout(20) << __func__  << " onode " << o->oid << " is " << bl.length()
	    << " (" << onode_part << " bytes onode + "
	    << blob_part << " bytes spanning blobs + "
	    << extent_part << " bytes inline extents)"
	    << dendl;


  txn->set(PREFIX_OBJ, o->key.c_str(), o->key.size(), bl);
}

void BlueStore::_log_alerts(osd_alert_list_t& alerts)
{
  std::lock_guard l(qlock);
  size_t used = bluefs && bluefs_layout.shared_bdev == BlueFS::BDEV_SLOW ?
    bluefs->get_used(BlueFS::BDEV_SLOW) : 0;
  if (used > 0) {
      auto db_used = bluefs->get_used(BlueFS::BDEV_DB);
      auto db_total = bluefs->get_total(BlueFS::BDEV_DB);
      ostringstream ss;
      ss << "spilled over " << byte_u_t(used)
         << " metadata from 'db' device (" << byte_u_t(db_used)
         << " used of " << byte_u_t(db_total) << ") to slow device";
      spillover_alert = ss.str();
  } else if (!spillover_alert.empty()){
    spillover_alert.clear();
  }

  if (!spurious_read_errors_alert.empty() &&
      cct->_conf->bluestore_warn_on_spurious_read_errors) {
    alerts.emplace(
      "BLUESTORE_SPURIOUS_READ_ERRORS",
      spurious_read_errors_alert);
  }
  if (!disk_size_mismatch_alert.empty()) {
    alerts.emplace(
      "BLUESTORE_DISK_SIZE_MISMATCH",
      disk_size_mismatch_alert);
  }
  if (!legacy_statfs_alert.empty()) {
    alerts.emplace(
      "BLUESTORE_LEGACY_STATFS",
      legacy_statfs_alert);
  }
  if (!spillover_alert.empty() &&
      cct->_conf->bluestore_warn_on_bluefs_spillover) {
    alerts.emplace(
      "BLUEFS_SPILLOVER",
      spillover_alert);
  }
  if (!no_per_pg_omap_alert.empty()) {
    alerts.emplace(
      "BLUESTORE_NO_PER_PG_OMAP",
      no_per_pg_omap_alert);
  }
  if (!no_per_pool_omap_alert.empty()) {
    alerts.emplace(
      "BLUESTORE_NO_PER_POOL_OMAP",
      no_per_pool_omap_alert);
  }
  string s0(failed_cmode);

  if (!failed_compressors.empty()) {
    if (!s0.empty()) {
      s0 += ", ";
    }
    s0 += "unable to load:";
    bool first = true;
    for (auto& s : failed_compressors) {
      if (first) {
	first = false;
      } else {
	s0 += ", ";
      }
      s0 += s;
    }
    alerts.emplace(
      "BLUESTORE_NO_COMPRESSION",
      s0);
  }
}

void BlueStore::_collect_allocation_stats(uint64_t need, uint32_t alloc_size,
                                          const PExtentVector& extents)
{
  if (alloc_size != min_alloc_size) {
    alloc_stats_count++;
    alloc_stats_fragments += extents.size();
    alloc_stats_size += need;
  }

  for (auto& e : extents) {
    logger->hinc(l_bluestore_allocate_hist, e.length, need);
  }
}

void BlueStore::_record_allocation_stats()
{
  // don't care about data consistency,
  // fields can be partially modified while making the tuple
  auto t0 = std::make_tuple(
    alloc_stats_count.exchange(0),
    alloc_stats_fragments.exchange(0),
    alloc_stats_size.exchange(0));

  dout(0) << " allocation stats probe "
    << probe_count << ":"
    << " cnt: " << std::get<0>(t0)
    << " frags: " << std::get<1>(t0)
    << " size: " << std::get<2>(t0)
    << dendl;


  //
  // Keep the history for probes from the power-of-two sequence:
  // -1, -2, -4, -8, -16
  // 
  size_t base = 1;
  for (auto& t : alloc_stats_history) {
    dout(0) << " probe -"
      << base + (probe_count % base) << ": "
      << std::get<0>(t)
      << ",  " << std::get<1>(t)
      << ", " << std::get<2>(t)
      << dendl;
    base <<= 1;
  }
  dout(0) << "------------" << dendl;

  ++ probe_count;

  for (ssize_t i = alloc_stats_history.size() - 1 ; i > 0 ; --i) {
    if ((probe_count % (1 << i)) == 0) {
      alloc_stats_history[i] = alloc_stats_history[i - 1];
    }
  }
  alloc_stats_history[0].swap(t0);
}

// ===========================================
// BlueStoreRepairer

size_t BlueStoreRepairer::StoreSpaceTracker::filter_out(
  const interval_set<uint64_t>& extents)
{
  ceph_assert(granularity); // initialized
  // can't call for the second time
  ceph_assert(!was_filtered_out);
  ceph_assert(collections_bfs.size() == objects_bfs.size());

  uint64_t prev_pos = 0;
  uint64_t npos = collections_bfs.size();

  bloom_vector collections_reduced;
  bloom_vector objects_reduced;

  for (auto e : extents) {
    if (e.second == 0) {
      continue;
    }
    uint64_t pos = max(e.first / granularity, prev_pos);
    uint64_t end_pos = 1 + (e.first + e.second - 1) / granularity;
    while (pos != npos && pos < end_pos)  {
        ceph_assert( collections_bfs[pos].element_count() ==
          objects_bfs[pos].element_count());
        if (collections_bfs[pos].element_count()) {
          collections_reduced.push_back(std::move(collections_bfs[pos]));
          objects_reduced.push_back(std::move(objects_bfs[pos]));
        }
        ++pos;
    }
    prev_pos = end_pos;
  }
  collections_reduced.swap(collections_bfs);
  objects_reduced.swap(objects_bfs);
  was_filtered_out = true;
  return collections_bfs.size();
}

bool BlueStoreRepairer::remove_key(KeyValueDB *db,
				   const string& prefix,
				   const string& key)
{
  std::lock_guard l(lock);
  if (!remove_key_txn) {
    remove_key_txn = db->get_transaction();
  }
  ++to_repair_cnt;
  remove_key_txn->rmkey(prefix, key);

  return true;
}

void BlueStoreRepairer::fix_per_pool_omap(KeyValueDB *db, int val)
{
  std::lock_guard l(lock); // possibly redundant
  ceph_assert(fix_per_pool_omap_txn == nullptr);
  fix_per_pool_omap_txn = db->get_transaction();
  ++to_repair_cnt;
  bufferlist bl;
  bl.append(stringify(val));
  fix_per_pool_omap_txn->set(PREFIX_SUPER, "per_pool_omap", bl);
}

bool BlueStoreRepairer::fix_shared_blob(
  KeyValueDB::Transaction txn,
  uint64_t sbid,
  bluestore_extent_ref_map_t* ref_map,
  size_t repaired)
{
  string key;
  get_shared_blob_key(sbid, &key);
  if (ref_map) {
    bluestore_shared_blob_t persistent(sbid, std::move(*ref_map));
    bufferlist bl;
    encode(persistent, bl);
    txn->set(PREFIX_SHARED_BLOB, key, bl);
  } else {
    txn->rmkey(PREFIX_SHARED_BLOB, key);
  }
  to_repair_cnt += repaired;
  return true;
}

bool BlueStoreRepairer::fix_statfs(KeyValueDB *db,
				   const string& key,
				   const store_statfs_t& new_statfs)
{
  std::lock_guard l(lock);
  if (!fix_statfs_txn) {
    fix_statfs_txn = db->get_transaction();
  }
  BlueStore::volatile_statfs vstatfs;
  vstatfs = new_statfs;
  bufferlist bl;
  vstatfs.encode(bl);
  ++to_repair_cnt;
  fix_statfs_txn->set(PREFIX_STAT, key, bl);
  return true;
}

bool BlueStoreRepairer::fix_leaked(KeyValueDB *db,
				   FreelistManager* fm,
				   uint64_t offset, uint64_t len)
{
  std::lock_guard l(lock);
  ceph_assert(!fm->is_null_manager());

  if (!fix_fm_leaked_txn) {
    fix_fm_leaked_txn = db->get_transaction();
  }
  ++to_repair_cnt;
  fm->release(offset, len, fix_fm_leaked_txn);
  return true;
}
bool BlueStoreRepairer::fix_false_free(KeyValueDB *db,
				       FreelistManager* fm,
				       uint64_t offset, uint64_t len)
{
  std::lock_guard l(lock);
  ceph_assert(!fm->is_null_manager());

  if (!fix_fm_false_free_txn) {
    fix_fm_false_free_txn = db->get_transaction();
  }
  ++to_repair_cnt;
  fm->allocate(offset, len, fix_fm_false_free_txn);
  return true;
}

bool BlueStoreRepairer::fix_spanning_blobs(
  KeyValueDB* db,
  std::function<void(KeyValueDB::Transaction)> f)
{
  std::lock_guard l(lock);
  if (!fix_onode_txn) {
    fix_onode_txn = db->get_transaction();
  }
  f(fix_onode_txn);
  ++to_repair_cnt;
  return true;
}

bool BlueStoreRepairer::preprocess_misreference(KeyValueDB *db)
{
  //NB: not for use in multithreading mode!!!
  if (misreferenced_extents.size()) {
    size_t n = space_usage_tracker.filter_out(misreferenced_extents);
    ceph_assert(n > 0);
    if (!fix_misreferences_txn) {
      fix_misreferences_txn = db->get_transaction();
    }
    return true;
  }
  return false;
}

unsigned BlueStoreRepairer::apply(KeyValueDB* db)
{
  //NB: not for use in multithreading mode!!!
  if (fix_per_pool_omap_txn) {
    auto ok = db->submit_transaction_sync(fix_per_pool_omap_txn) == 0;
    ceph_assert(ok);
    fix_per_pool_omap_txn = nullptr;
  }
  if (fix_fm_leaked_txn) {
    auto ok = db->submit_transaction_sync(fix_fm_leaked_txn) == 0;
    ceph_assert(ok);
    fix_fm_leaked_txn = nullptr;
  }
  if (fix_fm_false_free_txn) {
    auto ok = db->submit_transaction_sync(fix_fm_false_free_txn) == 0;
    ceph_assert(ok);
    fix_fm_false_free_txn = nullptr;
  }
  if (remove_key_txn) {
    auto ok = db->submit_transaction_sync(remove_key_txn) == 0;
    ceph_assert(ok);
    remove_key_txn = nullptr;
  }
  if (fix_misreferences_txn) {
    auto ok = db->submit_transaction_sync(fix_misreferences_txn) == 0;
    ceph_assert(ok);
    fix_misreferences_txn = nullptr;
  }
  if (fix_onode_txn) {
    auto ok = db->submit_transaction_sync(fix_onode_txn) == 0;
    ceph_assert(ok);
    fix_onode_txn = nullptr;
  }
  if (fix_shared_blob_txn) {
    auto ok = db->submit_transaction_sync(fix_shared_blob_txn) == 0;
    ceph_assert(ok);
    fix_shared_blob_txn = nullptr;
  }
  if (fix_statfs_txn) {
    auto ok = db->submit_transaction_sync(fix_statfs_txn) == 0;
    ceph_assert(ok);
    fix_statfs_txn = nullptr;
  }
  if (need_compact) {
    db->compact();
    need_compact = false;
  }
  unsigned repaired = to_repair_cnt;
  to_repair_cnt = 0;
  return repaired;
}

// =======================================================
// RocksDBBlueFSVolumeSelector

uint8_t RocksDBBlueFSVolumeSelector::select_prefer_bdev(void* h) {
  ceph_assert(h != nullptr);
  uint64_t hint = reinterpret_cast<uint64_t>(h);
  uint8_t res;
  switch (hint) {
  case LEVEL_SLOW:
    res = BlueFS::BDEV_SLOW;
    if (db_avail4slow > 0) {
      // considering statically available db space vs.
      // - observed maximums on DB dev for DB/WAL/UNSORTED data
      // - observed maximum spillovers
      uint64_t max_db_use = 0; // max db usage we potentially observed
      max_db_use += per_level_per_dev_max.at(BlueFS::BDEV_DB, LEVEL_LOG - LEVEL_FIRST);
      max_db_use += per_level_per_dev_max.at(BlueFS::BDEV_DB, LEVEL_WAL - LEVEL_FIRST);
      max_db_use += per_level_per_dev_max.at(BlueFS::BDEV_DB, LEVEL_DB - LEVEL_FIRST);
      // this could go to db hence using it in the estimation
      max_db_use += per_level_per_dev_max.at(BlueFS::BDEV_SLOW, LEVEL_DB - LEVEL_FIRST);

      auto db_total = l_totals[LEVEL_DB - LEVEL_FIRST];
      uint64_t avail = min(
        db_avail4slow,
        max_db_use < db_total ? db_total - max_db_use : 0);

      // considering current DB dev usage for SLOW data
      if (avail > per_level_per_dev_usage.at(BlueFS::BDEV_DB, LEVEL_SLOW - LEVEL_FIRST)) {
        res = BlueFS::BDEV_DB;
      }
    }
    break;
  case LEVEL_LOG:
  case LEVEL_WAL:
    res = BlueFS::BDEV_WAL;
    break;
  case LEVEL_DB:
  default:
    res = BlueFS::BDEV_DB;
    break;
  }
  return res;
}

void RocksDBBlueFSVolumeSelector::get_paths(const std::string& base, paths& res) const
{
  auto db_size = l_totals[LEVEL_DB - LEVEL_FIRST];
  res.emplace_back(base, db_size);
  auto slow_size = l_totals[LEVEL_SLOW - LEVEL_FIRST];
  if (slow_size == 0) {
    slow_size = db_size;
  }
  res.emplace_back(base + ".slow", slow_size);
}

void* RocksDBBlueFSVolumeSelector::get_hint_by_dir(std::string_view dirname) const {
  uint8_t res = LEVEL_DB;
  if (dirname.length() > 5) {
    // the "db.slow" and "db.wal" directory names are hard-coded at
    // match up with bluestore.  the slow device is always the second
    // one (when a dedicated block.db device is present and used at
    // bdev 0).  the wal device is always last.
    if (boost::algorithm::ends_with(dirname, ".slow")) {
      res = LEVEL_SLOW;
    }
    else if (boost::algorithm::ends_with(dirname, ".wal")) {
      res = LEVEL_WAL;
    }
  }
  return reinterpret_cast<void*>(res);
}

void RocksDBBlueFSVolumeSelector::dump(ostream& sout) {
  auto max_x = per_level_per_dev_usage.get_max_x();
  auto max_y = per_level_per_dev_usage.get_max_y();

  sout << "RocksDBBlueFSVolumeSelector " << std::endl;
  sout << ">>Settings<<"
       << " extra=" << byte_u_t(db_avail4slow)
       << ", l0_size=" << byte_u_t(level0_size)
       << ", l_base=" << byte_u_t(level_base)
       << ", l_multi=" << byte_u_t(level_multiplier)
       << std::endl;
  constexpr std::array<const char*, 8> names{ {
    "DEV/LEV",
    "WAL",
    "DB",
    "SLOW",
    "*",
    "*",
    "REAL",
    "FILES",
  } };
  const size_t width = 12;
  for (size_t i = 0; i < names.size(); ++i) {
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    sout << names[i];
  }
  sout << std::endl;
  for (size_t l = 0; l < max_y; l++) {
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    switch (l + LEVEL_FIRST) {
    case LEVEL_LOG:
      sout << "LOG"; break;
    case LEVEL_WAL:
      sout << "WAL"; break;
    case LEVEL_DB:
      sout << "DB"; break;
    case LEVEL_SLOW:
      sout << "SLOW"; break;
    case LEVEL_MAX:
      sout << "TOTAL"; break;
    }
    for (size_t d = 0; d < max_x; d++) {
      sout.setf(std::ios::left, std::ios::adjustfield);
      sout.width(width);
      sout << stringify(byte_u_t(per_level_per_dev_usage.at(d, l)));
    }
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    sout << stringify(per_level_files[l]) << std::endl;
  }
  ceph_assert(max_x == per_level_per_dev_max.get_max_x());
  ceph_assert(max_y == per_level_per_dev_max.get_max_y());
  sout << "MAXIMUMS:" << std::endl;
  for (size_t l = 0; l < max_y; l++) {
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    switch (l + LEVEL_FIRST) {
    case LEVEL_LOG:
      sout << "LOG"; break;
    case LEVEL_WAL:
      sout << "WAL"; break;
    case LEVEL_DB:
      sout << "DB"; break;
    case LEVEL_SLOW:
      sout << "SLOW"; break;
    case LEVEL_MAX:
      sout << "TOTAL"; break;
    }
    for (size_t d = 0; d < max_x - 1; d++) {
      sout.setf(std::ios::left, std::ios::adjustfield);
      sout.width(width);
      sout << stringify(byte_u_t(per_level_per_dev_max.at(d, l)));
    }
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    sout << stringify(byte_u_t(per_level_per_dev_max.at(max_x - 1, l)));
    sout << std::endl;
  }
  string sizes[] = {
    ">> SIZE <<",
    stringify(byte_u_t(l_totals[LEVEL_WAL - LEVEL_FIRST])),
    stringify(byte_u_t(l_totals[LEVEL_DB - LEVEL_FIRST])),
    stringify(byte_u_t(l_totals[LEVEL_SLOW - LEVEL_FIRST])),
  };
  for (size_t i = 0; i < (sizeof(sizes) / sizeof(sizes[0])); i++) {
    sout.setf(std::ios::left, std::ios::adjustfield);
    sout.width(width);
    sout << sizes[i];
  }
  sout << std::endl;
}

BlueFSVolumeSelector* RocksDBBlueFSVolumeSelector::clone_empty() const {
  RocksDBBlueFSVolumeSelector* ns =
    new RocksDBBlueFSVolumeSelector(0, 0, 0,
				    0, 0, 0,
				    0, 0, false);
  return ns;
}

bool RocksDBBlueFSVolumeSelector::compare(BlueFSVolumeSelector* other) {
  RocksDBBlueFSVolumeSelector* o = dynamic_cast<RocksDBBlueFSVolumeSelector*>(other);
  ceph_assert(o);
  bool equal = true;
  for (size_t x = 0; x < BlueFS::MAX_BDEV + 1; x++) {
    for (size_t y = 0; y <LEVEL_MAX - LEVEL_FIRST + 1; y++) {
      equal &= (per_level_per_dev_usage.at(x, y) == o->per_level_per_dev_usage.at(x, y));
    }
  }
  for (size_t t = 0; t < LEVEL_MAX - LEVEL_FIRST + 1; t++) {
    equal &= (per_level_files[t] == o->per_level_files[t]);
  }
  return equal;
}

// =======================================================

//================================================================================================================
// BlueStore is committing all allocation information (alloc/release) into RocksDB before the client Write is performed.
// This cause a delay in write path and add significant load to the CPU/Memory/Disk.
// The reason for the RocksDB updates is that it allows Ceph to survive any failure without losing the allocation state.
//
// We changed the code skiping RocksDB updates on allocation time and instead performing a full desatge of the allocator object
// with all the OSD allocation state in a single step during umount().
// This change leads to a 25% increase in IOPS and reduced latency in small random-write workload, but exposes the system
// to losing allocation info in failure cases where we don't call umount.
// We add code to perform a full allocation-map rebuild from information stored inside the ONode which is used in failure cases.
// When we perform a graceful shutdown there is no need for recovery and we simply read the allocation-map from a flat file
// where we store the allocation-map during umount().
//================================================================================================================

#undef dout_prefix
#define dout_prefix *_dout << "bluestore::NCB::" << __func__ << "::"

static const std::string allocator_dir    = "ALLOCATOR_NCB_DIR";
static const std::string allocator_file   = "ALLOCATOR_NCB_FILE";
static uint32_t    s_format_version = 0x01; // support future changes to allocator-map file
static uint32_t    s_serial         = 0x01;

#if 1
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64
#else
// help debug the encode/decode by forcing alien format
#define CEPHTOH_32 be32toh
#define CEPHTOH_64 be64toh
#define HTOCEPH_32 htobe32
#define HTOCEPH_64 htobe64
#endif

// 48 Bytes header for on-disk alloator image
const uint64_t ALLOCATOR_IMAGE_VALID_SIGNATURE = 0x1FACE0FF;
struct allocator_image_header {
  uint32_t format_version;	// 0x00
  uint32_t valid_signature;	// 0x04
  utime_t  timestamp;		// 0x08
  uint32_t serial;		// 0x10
  uint32_t pad[0x7];		// 0x14

  allocator_image_header() {
    memset((char*)this, 0, sizeof(allocator_image_header));
  }

  // create header in CEPH format
  allocator_image_header(utime_t timestamp, uint32_t format_version, uint32_t serial) {
    this->format_version  = format_version;
    this->timestamp       = timestamp;
    this->valid_signature = ALLOCATOR_IMAGE_VALID_SIGNATURE;
    this->serial          = serial;
    memset(this->pad, 0, sizeof(this->pad));
  }

  friend std::ostream& operator<<(std::ostream& out, const allocator_image_header& header) {
    out << "format_version  = " << header.format_version << std::endl;
    out << "valid_signature = " << header.valid_signature << "/" << ALLOCATOR_IMAGE_VALID_SIGNATURE << std::endl;
    out << "timestamp       = " << header.timestamp << std::endl;
    out << "serial          = " << header.serial << std::endl;
    for (unsigned i = 0; i < sizeof(header.pad)/sizeof(uint32_t); i++) {
      if (header.pad[i]) {
	out << "header.pad[" << i << "] = " << header.pad[i] << std::endl;
      }
    }
    return out;
  }

  DENC(allocator_image_header, v, p) {
    denc(v.format_version, p);
    denc(v.valid_signature, p);
    denc(v.timestamp.tv.tv_sec, p);
    denc(v.timestamp.tv.tv_nsec, p);
    denc(v.serial, p);
    for (auto& pad: v.pad) {
      denc(pad, p);
    }
  }


  int verify(CephContext* cct, const std::string &path) {
    if (valid_signature == ALLOCATOR_IMAGE_VALID_SIGNATURE) {
      for (unsigned i = 0; i < (sizeof(pad) / sizeof(uint32_t)); i++) {
	if (this->pad[i]) {
	  derr << "Illegal Header - pad[" << i << "]="<< pad[i] << dendl;
	  return -1;
	}
      }
      return 0;
    }
    else {
      derr << "Illegal Header - signature="<< valid_signature << "(" << ALLOCATOR_IMAGE_VALID_SIGNATURE << ")" << dendl;
      return -1;
    }
  }
};
WRITE_CLASS_DENC(allocator_image_header)

// 56 Bytes trailer for on-disk alloator image
struct allocator_image_trailer {
  extent_t null_extent;         // 0x00

  uint32_t format_version;	// 0x10
  uint32_t valid_signature;	// 0x14

  utime_t  timestamp;		// 0x18

  uint32_t serial;		// 0x20
  uint32_t pad;		// 0x24
  uint64_t entries_count;	// 0x28
  uint64_t allocation_size;	// 0x30

  // trailer is created in CEPH format
  allocator_image_trailer(utime_t timestamp, uint32_t format_version, uint32_t serial, uint64_t entries_count, uint64_t allocation_size) {
    memset((char*)&(this->null_extent), 0, sizeof(this->null_extent));
    this->format_version  = format_version;
    this->valid_signature = ALLOCATOR_IMAGE_VALID_SIGNATURE;
    this->timestamp       = timestamp;
    this->serial          = serial;
    this->pad             = 0;
    this->entries_count   = entries_count;
    this->allocation_size = allocation_size;
  }

  allocator_image_trailer() {
    memset((char*)this, 0, sizeof(allocator_image_trailer));
  }

  friend std::ostream& operator<<(std::ostream& out, const allocator_image_trailer& trailer) {
    if (trailer.null_extent.offset || trailer.null_extent.length) {
      out << "trailer.null_extent.offset = " << trailer.null_extent.offset << std::endl;
      out << "trailer.null_extent.length = " << trailer.null_extent.length << std::endl;
    }
    out << "format_version  = " << trailer.format_version << std::endl;
    out << "valid_signature = " << trailer.valid_signature << "/" << ALLOCATOR_IMAGE_VALID_SIGNATURE << std::endl;
    out << "timestamp       = " << trailer.timestamp << std::endl;
    out << "serial          = " << trailer.serial << std::endl;
    if (trailer.pad) {
      out << "trailer.pad= " << trailer.pad << std::endl;
    }
    out << "entries_count   = " << trailer.entries_count   << std::endl;
    out << "allocation_size = " << trailer.allocation_size << std::endl;
    return out;
  }

  int verify(CephContext* cct, const std::string &path, const allocator_image_header *p_header, uint64_t entries_count, uint64_t allocation_size) {
    if (valid_signature == ALLOCATOR_IMAGE_VALID_SIGNATURE) {

      // trailer must starts with null extents (both fields set to zero) [no need to convert formats for zero)
      if (null_extent.offset || null_extent.length) {
	derr << "illegal trailer - null_extent = [" << null_extent.offset << "," << null_extent.length << "]"<< dendl;
	return -1;
      }

      if (serial != p_header->serial) {
	derr << "Illegal trailer: header->serial(" << p_header->serial << ") != trailer->serial(" << serial << ")" << dendl;
	return -1;
      }

      if (format_version != p_header->format_version) {
	derr << "Illegal trailer: header->format_version(" << p_header->format_version
	     << ") != trailer->format_version(" << format_version << ")" << dendl;
	return -1;
      }

      if (timestamp != p_header->timestamp) {
	derr << "Illegal trailer: header->timestamp(" << p_header->timestamp
	     << ") != trailer->timestamp(" << timestamp << ")" << dendl;
	return -1;
      }

      if (this->entries_count != entries_count) {
	derr << "Illegal trailer: entries_count(" << entries_count << ") != trailer->entries_count("
	     << this->entries_count << ")" << dendl;
	return -1;
      }

      if (this->allocation_size != allocation_size) {
	derr << "Illegal trailer: allocation_size(" << allocation_size << ") != trailer->allocation_size("
	     << this->allocation_size << ")" << dendl;
	return -1;
      }

      if (pad) {
	derr << "Illegal Trailer - pad="<< pad << dendl;
	return -1;
      }

      // if arrived here -> trailer is valid !!
      return 0;
    } else {
      derr << "Illegal Trailer - signature="<< valid_signature << "(" << ALLOCATOR_IMAGE_VALID_SIGNATURE << ")" << dendl;
      return -1;
    }
  }

  DENC(allocator_image_trailer, v, p) {
    denc(v.null_extent.offset, p);
    denc(v.null_extent.length, p);
    denc(v.format_version, p);
    denc(v.valid_signature, p);
    denc(v.timestamp.tv.tv_sec, p);
    denc(v.timestamp.tv.tv_nsec, p);
    denc(v.serial, p);
    denc(v.pad, p);
    denc(v.entries_count, p);
    denc(v.allocation_size, p);
  }
};
WRITE_CLASS_DENC(allocator_image_trailer)


//-------------------------------------------------------------------------------------
// invalidate old allocation file if exists so will go directly to recovery after failure
// we can safely ignore non-existing file
int BlueStore::invalidate_allocation_file_on_bluefs()
{
  // mark that allocation-file was invalidated and we should destage a new copy whne closing db
  need_to_destage_allocation_file = true;
  dout(10) << __func__ << " need_to_destage_allocation_file was set" << dendl;

  BlueFS::FileWriter *p_handle = nullptr;
  if (!bluefs->dir_exists(allocator_dir)) {
    dout(5) << "allocator_dir(" << allocator_dir << ") doesn't exist" << dendl;
    // nothing to do -> return
    return 0;
  }

  int ret = bluefs->stat(allocator_dir, allocator_file, nullptr, nullptr);
  if (ret != 0) {
    dout(5) << __func__ << " allocator_file(" << allocator_file << ") doesn't exist" << dendl;
    // nothing to do -> return
    return 0;
  }


  ret = bluefs->open_for_write(allocator_dir, allocator_file, &p_handle, true);
  if (ret != 0) {
    derr << __func__ << "::NCB:: Failed open_for_write with error-code "
         << ret << dendl;
    return -1;
  }

  dout(5) << "invalidate using bluefs->truncate(p_handle, 0)" << dendl;
  ret = bluefs->truncate(p_handle, 0);
  if (ret != 0) {
    derr << __func__ << "::NCB:: Failed truncaste with error-code "
         << ret << dendl;
    bluefs->close_writer(p_handle);
    return -1;
  }

  bluefs->fsync(p_handle);
  bluefs->close_writer(p_handle);

  return 0;
}

//-----------------------------------------------------------------------------------
int BlueStore::copy_allocator(Allocator* src_alloc, Allocator* dest_alloc, uint64_t* p_num_entries)
{
  *p_num_entries = 0;
  auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    (*p_num_entries)++;
  };
  src_alloc->foreach(count_entries);

  dout(5) << "count num_entries=" << *p_num_entries << dendl;

  // add 16K extra entries in case new allocation happened
  (*p_num_entries) += 16*1024;
  unique_ptr<extent_t[]> arr;
  try {
    arr = make_unique<extent_t[]>(*p_num_entries);
  } catch (std::bad_alloc&) {
    derr << "****Failed dynamic allocation, num_entries=" << *p_num_entries << dendl;
    return -1;
  }

  uint64_t idx         = 0;
  auto copy_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    if (extent_length > 0) {
      if (idx < *p_num_entries) {
	arr[idx] = {extent_offset, extent_length};
      }
      idx++;
    }
    else {
      derr << "zero length extent!!! offset=" << extent_offset << ", index=" << idx << dendl;
    }
  };
  src_alloc->foreach(copy_entries);

  dout(5) << "copy num_entries=" << idx << dendl;
  if (idx > *p_num_entries) {
    derr << "****spillover, num_entries=" << *p_num_entries << ", spillover=" << (idx - *p_num_entries) << dendl;
    ceph_assert(idx <= *p_num_entries);
  }

  *p_num_entries = idx;

  for (idx = 0; idx < *p_num_entries; idx++) {
    const extent_t *p_extent = &arr[idx];
    dest_alloc->init_add_free(p_extent->offset, p_extent->length);
  }

  return 0;
}

//-----------------------------------------------------------------------------------
static uint32_t flush_extent_buffer_with_crc(BlueFS::FileWriter *p_handle, const char* buffer, const char *p_curr, uint32_t crc)
{
  std::ptrdiff_t length = p_curr - buffer;
  p_handle->append(buffer, length);

  crc = ceph_crc32c(crc, (const uint8_t*)buffer, length);
  uint32_t encoded_crc = HTOCEPH_32(crc);
  p_handle->append((byte*)&encoded_crc, sizeof(encoded_crc));

  return crc;
}

const unsigned MAX_EXTENTS_IN_BUFFER = 4 * 1024; // 4K extents = 64KB of data
// write the allocator to a flat bluefs file - 4K extents at a time
//-----------------------------------------------------------------------------------
int BlueStore::store_allocator(Allocator* src_allocator)
{
  // when storing allocations to file we must be sure there is no background compactions
  // the easiest way to achieve it is to make sure db is closed
  ceph_assert(db == nullptr);
  utime_t  start_time = ceph_clock_now();
  int ret = 0;

  // create dir if doesn't exist already
  if (!bluefs->dir_exists(allocator_dir) ) {
    ret = bluefs->mkdir(allocator_dir);
    if (ret != 0) {
      derr << "Failed mkdir with error-code " << ret << dendl;
      return -1;
    }
  }
  bluefs->compact_log();
  // reuse previous file-allocation if exists
  ret = bluefs->stat(allocator_dir, allocator_file, nullptr, nullptr);
  bool overwrite_file = (ret == 0);
  BlueFS::FileWriter *p_handle = nullptr;
  ret = bluefs->open_for_write(allocator_dir, allocator_file, &p_handle, overwrite_file);
  if (ret != 0) {
    derr <<  __func__ << "Failed open_for_write with error-code " << ret << dendl;
    return -1;
  }

  uint64_t file_size = p_handle->file->fnode.size;
  uint64_t allocated = p_handle->file->fnode.get_allocated();
  dout(10) << "file_size=" << file_size << ", allocated=" << allocated << dendl;

  bluefs->sync_metadata(false);
  unique_ptr<Allocator> allocator(clone_allocator_without_bluefs(src_allocator));
  if (!allocator) {
    bluefs->close_writer(p_handle);
    return -1;
  }

  // store all extents (except for the bluefs extents we removed) in a single flat file
  utime_t                 timestamp = ceph_clock_now();
  uint32_t                crc       = -1;
  {
    allocator_image_header  header(timestamp, s_format_version, s_serial);
    bufferlist              header_bl;
    encode(header, header_bl);
    crc = header_bl.crc32c(crc);
    encode(crc, header_bl);
    p_handle->append(header_bl);
  }

  crc = -1;					 // reset crc
  extent_t        buffer[MAX_EXTENTS_IN_BUFFER]; // 64KB
  extent_t       *p_curr          = buffer;
  const extent_t *p_end           = buffer + MAX_EXTENTS_IN_BUFFER;
  uint64_t        extent_count    = 0;
  uint64_t        allocation_size = 0;
  auto iterated_allocation = [&](uint64_t extent_offset, uint64_t extent_length) {
    if (extent_length == 0) {
      derr <<  __func__ << "" << extent_count << "::[" << extent_offset << "," << extent_length << "]" << dendl;
      ret = -1;
      return;
    }
    p_curr->offset = HTOCEPH_64(extent_offset);
    p_curr->length = HTOCEPH_64(extent_length);
    extent_count++;
    allocation_size += extent_length;
    p_curr++;

    if (p_curr == p_end) {
      crc = flush_extent_buffer_with_crc(p_handle, (const char*)buffer, (const char*)p_curr, crc);
      p_curr = buffer; // recycle the buffer
    }
  };
  allocator->foreach(iterated_allocation);
  // if got null extent -> fail the operation
  if (ret != 0) {
    derr << "Illegal extent, fail store operation" << dendl;
    derr << "invalidate using bluefs->truncate(p_handle, 0)" << dendl;
    bluefs->truncate(p_handle, 0);
    bluefs->close_writer(p_handle);
    return -1;
  }

  // if we got any leftovers -> add crc and append to file
  if (p_curr > buffer) {
    crc = flush_extent_buffer_with_crc(p_handle, (const char*)buffer, (const char*)p_curr, crc);
  }

  {
    allocator_image_trailer trailer(timestamp, s_format_version, s_serial, extent_count, allocation_size);
    bufferlist trailer_bl;
    encode(trailer, trailer_bl);
    uint32_t crc = -1;
    crc = trailer_bl.crc32c(crc);
    encode(crc, trailer_bl);
    p_handle->append(trailer_bl);
  }

  bluefs->fsync(p_handle);
  bluefs->truncate(p_handle, p_handle->pos);
  bluefs->fsync(p_handle);

  utime_t duration = ceph_clock_now() - start_time;
  dout(5) <<"WRITE-extent_count=" << extent_count << ", allocation_size=" << allocation_size << ", serial=" << s_serial << dendl;
  dout(5) <<"p_handle->pos=" << p_handle->pos << " WRITE-duration=" << duration << " seconds" << dendl;

  bluefs->close_writer(p_handle);
  need_to_destage_allocation_file = false;
  return 0;
}

//-----------------------------------------------------------------------------------
Allocator* BlueStore::create_bitmap_allocator(uint64_t bdev_size) {
  // create allocator
  uint64_t alloc_size = min_alloc_size;
  Allocator* alloc = Allocator::create(cct, "bitmap", bdev_size, alloc_size,
				       "recovery");
  if (alloc) {
    return alloc;
  } else {
    derr << "Failed Allocator Creation" << dendl;
    return nullptr;
  }
}

//-----------------------------------------------------------------------------------
size_t calc_allocator_image_header_size()
{
  utime_t                 timestamp = ceph_clock_now();
  allocator_image_header  header(timestamp, s_format_version, s_serial);
  bufferlist              header_bl;
  encode(header, header_bl);
  uint32_t crc = -1;
  crc = header_bl.crc32c(crc);
  encode(crc, header_bl);

  return header_bl.length();
}

//-----------------------------------------------------------------------------------
int calc_allocator_image_trailer_size()
{
  utime_t                 timestamp       = ceph_clock_now();
  uint64_t                extent_count    = -1;
  uint64_t                allocation_size = -1;
  uint32_t                crc             = -1;
  bufferlist              trailer_bl;
  allocator_image_trailer trailer(timestamp, s_format_version, s_serial, extent_count, allocation_size);

  encode(trailer, trailer_bl);
  crc = trailer_bl.crc32c(crc);
  encode(crc, trailer_bl);
  return trailer_bl.length();
}

//-----------------------------------------------------------------------------------
int BlueStore::__restore_allocator(Allocator* allocator, uint64_t *num, uint64_t *bytes)
{
  if (cct->_conf->bluestore_debug_inject_allocation_from_file_failure > 0) {
     boost::mt11213b rng(time(NULL));
    boost::uniform_real<> ur(0, 1);
    if (ur(rng) < cct->_conf->bluestore_debug_inject_allocation_from_file_failure) {
      derr << __func__ << " failure injected." << dendl;
      return -1;
    }
  }
  utime_t start_time = ceph_clock_now();
  BlueFS::FileReader *p_temp_handle = nullptr;
  int ret = bluefs->open_for_read(allocator_dir, allocator_file, &p_temp_handle, false);
  if (ret != 0) {
    dout(1) << "Failed open_for_read with error-code " << ret << dendl;
    return -1;
  }
  unique_ptr<BlueFS::FileReader> p_handle(p_temp_handle);
  uint64_t read_alloc_size = 0;
  uint64_t file_size = p_handle->file->fnode.size;
  dout(5) << "file_size=" << file_size << ",sizeof(extent_t)=" << sizeof(extent_t) << dendl;

  // make sure we were able to store a valid copy
  if (file_size == 0) {
    dout(1) << "No Valid allocation info on disk (empty file)" << dendl;
    return -1;
  }

  // first read the header
  size_t                 offset = 0;
  allocator_image_header header;
  int                    header_size = calc_allocator_image_header_size();
  {
    bufferlist header_bl,temp_bl;
    int        read_bytes = bluefs->read(p_handle.get(), offset, header_size, &temp_bl, nullptr);
    if (read_bytes != header_size) {
      derr << "Failed bluefs->read() for header::read_bytes=" << read_bytes << ", req_bytes=" << header_size << dendl;
      return -1;
    }

    offset += read_bytes;

    header_bl.claim_append(temp_bl);
    auto p = header_bl.cbegin();
    decode(header, p);
    if (header.verify(cct, path) != 0 ) {
      derr << "header = \n" << header << dendl;
      return -1;
    }

    uint32_t crc_calc = -1, crc;
    crc_calc = header_bl.cbegin().crc32c(p.get_off(), crc_calc); //crc from begin to current pos
    decode(crc, p);
    if (crc != crc_calc) {
      derr << "crc mismatch!!! crc=" << crc << ", crc_calc=" << crc_calc << dendl;
      derr << "header = \n" << header << dendl;
      return -1;
    }

    // increment version for next store
    s_serial = header.serial + 1;
  }

  // then read the payload (extents list) using a recycled buffer
  extent_t        buffer[MAX_EXTENTS_IN_BUFFER]; // 64KB
  uint32_t        crc                = -1;
  int             trailer_size       = calc_allocator_image_trailer_size();
  uint64_t        extent_count       = 0;
  uint64_t        extents_bytes_left = file_size - (header_size + trailer_size + sizeof(crc));
  while (extents_bytes_left) {
    int req_bytes  = std::min(extents_bytes_left, static_cast<uint64_t>(sizeof(buffer)));
    int read_bytes = bluefs->read(p_handle.get(), offset, req_bytes, nullptr, (char*)buffer);
    if (read_bytes != req_bytes) {
      derr << "Failed bluefs->read()::read_bytes=" << read_bytes << ", req_bytes=" << req_bytes << dendl;
      return -1;
    }

    offset             += read_bytes;
    extents_bytes_left -= read_bytes;

    const unsigned  num_extent_in_buffer = read_bytes/sizeof(extent_t);
    const extent_t *p_end                = buffer + num_extent_in_buffer;
    for (const extent_t *p_ext = buffer; p_ext < p_end; p_ext++) {
      uint64_t offset = CEPHTOH_64(p_ext->offset);
      uint64_t length = CEPHTOH_64(p_ext->length);
      read_alloc_size += length;

      if (length > 0) {
	allocator->init_add_free(offset, length);
	extent_count ++;
      } else {
	derr << "extent with zero length at idx=" << extent_count << dendl;
	return -1;
      }
    }

    uint32_t calc_crc = ceph_crc32c(crc, (const uint8_t*)buffer, read_bytes);
    read_bytes        = bluefs->read(p_handle.get(), offset, sizeof(crc), nullptr, (char*)&crc);
    if (read_bytes == sizeof(crc) ) {
      crc     = CEPHTOH_32(crc);
      if (crc != calc_crc) {
	derr << "data crc mismatch!!! crc=" << crc << ", calc_crc=" << calc_crc << dendl;
	derr << "extents_bytes_left=" << extents_bytes_left << ", offset=" << offset << ", extent_count=" << extent_count << dendl;
	return -1;
      }

      offset += read_bytes;
      if (extents_bytes_left) {
	extents_bytes_left -= read_bytes;
      }
    } else {
      derr << "Failed bluefs->read() for crc::read_bytes=" << read_bytes << ", req_bytes=" << sizeof(crc) << dendl;
      return -1;
    }

  }

  // finally, read the trailer and verify it is in good shape and that we got all the extents
  {
    bufferlist trailer_bl,temp_bl;
    int        read_bytes = bluefs->read(p_handle.get(), offset, trailer_size, &temp_bl, nullptr);
    if (read_bytes != trailer_size) {
      derr << "Failed bluefs->read() for trailer::read_bytes=" << read_bytes << ", req_bytes=" << trailer_size << dendl;
      return -1;
    }
    offset += read_bytes;

    trailer_bl.claim_append(temp_bl);
    uint32_t crc_calc = -1;
    uint32_t crc;
    allocator_image_trailer trailer;
    auto p = trailer_bl.cbegin();
    decode(trailer, p);
    if (trailer.verify(cct, path, &header, extent_count, read_alloc_size) != 0 ) {
      derr << "trailer=\n" << trailer << dendl;
      return -1;
    }

    crc_calc = trailer_bl.cbegin().crc32c(p.get_off(), crc_calc); //crc from begin to current pos
    decode(crc, p);
    if (crc != crc_calc) {
      derr << "trailer crc mismatch!::crc=" << crc << ", crc_calc=" << crc_calc << dendl;
      derr << "trailer=\n" << trailer << dendl;
      return -1;
    }
  }

  utime_t duration = ceph_clock_now() - start_time;
  dout(5) << "READ--extent_count=" << extent_count << ", read_alloc_size=  "
	    << read_alloc_size << ", file_size=" << file_size << dendl;
  dout(5) << "READ duration=" << duration << " seconds, s_serial=" << header.serial << dendl;
  *num   = extent_count;
  *bytes = read_alloc_size;
  return 0;
}

//-----------------------------------------------------------------------------------
int BlueStore::restore_allocator(Allocator* dest_allocator, uint64_t *num, uint64_t *bytes)
{
  utime_t    start = ceph_clock_now();
  auto temp_allocator = unique_ptr<Allocator>(create_bitmap_allocator(bdev->get_size()));
  int ret = __restore_allocator(temp_allocator.get(), num, bytes);
  if (ret != 0) {
    return ret;
  }

  uint64_t num_entries = 0;
  dout(5) << " calling copy_allocator(bitmap_allocator -> shared_alloc.a)" << dendl;
  copy_allocator(temp_allocator.get(), dest_allocator, &num_entries);
  utime_t duration = ceph_clock_now() - start;
  dout(5) << "restored in " << duration << " seconds, num_entries=" << num_entries << dendl;
  return ret;
}

//-----------------------------------------------------------------------------------
void BlueStore::set_allocation_in_simple_bmap(SimpleBitmap* sbmap, uint64_t offset, uint64_t length)
{
  dout(30) << __func__ << " 0x" << std::hex
           << offset << "~" << length
           << " " << min_alloc_size_mask
           << dendl;
  ceph_assert((offset & min_alloc_size_mask) == 0);
  ceph_assert((length & min_alloc_size_mask) == 0);
  sbmap->set(offset >> min_alloc_size_order, length >> min_alloc_size_order);
}

void BlueStore::ExtentDecoderPartial::_consume_new_blob(bool spanning,
                                                        uint64_t extent_no,
                                                        uint64_t sbid,
                                                        BlobRef b)
{
  [[maybe_unused]] auto cct = store.cct;
  ceph_assert(per_pool_statfs);
  ceph_assert(oid != ghobject_t());

  auto &blob = b->get_blob();
  if(spanning) {
    dout(20) << __func__ << " " << spanning << " " << b->id << dendl;
    ceph_assert(b->id >= 0);
    spanning_blobs[b->id] = b;
    ++stats.spanning_blob_count;
  } else {
    dout(20) << __func__ << " " << spanning << " " << extent_no << dendl;
    blobs[extent_no] = b;
  }
  bool compressed = blob.is_compressed();
  if (!blob.is_shared()) {
    for (auto& pe : blob.get_extents()) {
      if (pe.offset == bluestore_pextent_t::INVALID_OFFSET) {
        ++stats.skipped_illegal_extent;
        continue;
      }
      store.set_allocation_in_simple_bmap(&sbmap, pe.offset, pe.length);

      per_pool_statfs->allocated() += pe.length;
      if (compressed) {
        per_pool_statfs->compressed_allocated() += pe.length;
      }
    }
    if (compressed) {
      per_pool_statfs->compressed() +=
        blob.get_compressed_payload_length();
      ++stats.compressed_blob_count;
    }
  } else {
    auto it = sb_info.find(sbid);
    if (it == sb_info.end()) {
      derr << __func__ << " shared blob not found:" << sbid
           << dendl;
    }
    auto &sbi = *it;
    auto pool_id = oid.hobj.get_logical_pool();
    if (sbi.pool_id == sb_info_t::INVALID_POOL_ID) {
      sbi.pool_id = pool_id;
      size_t alloc_delta = sbi.allocated_chunks << min_alloc_size_order;
      per_pool_statfs->allocated() += alloc_delta;
      if (compressed) {
        per_pool_statfs->compressed_allocated() += alloc_delta;
        ++stats.compressed_blob_count;
      }
    }
    if (compressed) {
      per_pool_statfs->compressed() +=
        blob.get_compressed_payload_length();
    }
  }
}

void BlueStore::ExtentDecoderPartial::consume_blobid(Extent* le,
                                                     bool spanning,
                                                     uint64_t blobid)
{
  [[maybe_unused]] auto cct = store.cct;
  dout(20) << __func__ << " " << spanning << " " << blobid << dendl;
  auto &map = spanning ? spanning_blobs : blobs;
  auto it = map.find(blobid);
  ceph_assert(it != map.end());
  per_pool_statfs->stored() += le->length;
  if (it->second->get_blob().is_compressed()) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void BlueStore::ExtentDecoderPartial::consume_blob(Extent* le,
                                                   uint64_t extent_no,
                                                   uint64_t sbid,
                                                   BlobRef b)
{
  _consume_new_blob(false, extent_no, sbid, b);
  per_pool_statfs->stored() += le->length;
  if (b->get_blob().is_compressed()) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void BlueStore::ExtentDecoderPartial::consume_spanning_blob(uint64_t sbid,
                                                            BlobRef b)
{
  _consume_new_blob(true, 0/*doesn't matter*/, sbid, b);
}

void BlueStore::ExtentDecoderPartial::reset(const ghobject_t _oid,
                                            volatile_statfs* _per_pool_statfs)
{
  oid = _oid;
  per_pool_statfs = _per_pool_statfs;
  blob_map_t empty;
  blob_map_t empty2;
  std::swap(blobs, empty);
  std::swap(spanning_blobs, empty2);
}

int BlueStore::read_allocation_from_onodes(SimpleBitmap *sbmap, read_alloc_stats_t& stats)
{
  sb_info_space_efficient_map_t sb_info;
  // iterate over all shared blobs
  auto it = db->get_iterator(PREFIX_SHARED_BLOB, KeyValueDB::ITERATOR_NOCACHE);
  if (!it) {
    derr << "failed getting shared blob's iterator" << dendl;
    return -ENOENT;
  }
  if (it) {
    for (it->lower_bound(string()); it->valid(); it->next()) {
      const auto& key = it->key();
      dout(20) << __func__ << " decode sb " << pretty_binary_string(key) << dendl;
      uint64_t sbid = 0;
      if (get_key_shared_blob(key, &sbid) != 0) {
	derr << __func__ << " bad shared blob key '" << pretty_binary_string(key)
	     << "'" << dendl;
      }
      bluestore_shared_blob_t shared_blob(sbid);
      bufferlist bl = it->value();
      auto blp = bl.cbegin();
      try {
        decode(shared_blob, blp);
      }
      catch (ceph::buffer::error& e) {
	derr << __func__ << " failed to decode Shared Blob"
	     << pretty_binary_string(key) << dendl;
	continue;
      }
      dout(20) << __func__ << "  " << shared_blob << dendl;
      uint64_t allocated = 0;
      for (auto& r : shared_blob.ref_map.ref_map) {
        ceph_assert(r.first != bluestore_pextent_t::INVALID_OFFSET);
        set_allocation_in_simple_bmap(sbmap, r.first, r.second.length);
        allocated += r.second.length;
      }
      auto &sbi = sb_info.add_or_adopt(sbid);
      ceph_assert(p2phase(allocated, min_alloc_size) == 0);
      sbi.allocated_chunks += (allocated >> min_alloc_size_order);
      ++stats.shared_blob_count;
    }
  }

  it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
  if (!it) {
    derr << "failed getting onode's iterator" << dendl;
    return -ENOENT;
  }

  uint64_t            kv_count       = 0;
  uint64_t            count_interval = 1'000'000;
  ExtentDecoderPartial edecoder(*this,
                                stats,
                                *sbmap,
                                sb_info,
                                min_alloc_size_order);

  // iterate over all ONodes stored in RocksDB
  for (it->lower_bound(string()); it->valid(); it->next(), kv_count++) {
    // trace an even after every million processed objects (typically every 5-10 seconds)
    if (kv_count && (kv_count % count_interval == 0) ) {
      dout(5) << __func__ << " processed objects count = " << kv_count << dendl;
    }

    auto key = it->key();
    auto okey = key;
    dout(20) << __func__ << " decode onode " << pretty_binary_string(key) << dendl;
    ghobject_t oid;
    if (!is_extent_shard_key(it->key())) {
      int r = get_key_object(okey, &oid);
      if (r != 0) {
        derr << __func__ << " failed to decode onode key = "
             << pretty_binary_string(okey) << dendl;
        return -EIO;
      }
      edecoder.reset(oid,
        &stats.actual_pool_vstatfs[oid.hobj.get_logical_pool()]);
      Onode dummy_on(cct);
      Onode::decode_raw(&dummy_on,
        it->value(),
        edecoder);
      ++stats.onode_count;
    } else {
      uint32_t offset;
      int r = get_key_extent_shard(key, &okey, &offset);
      if (r != 0) {
        derr << __func__ << " failed to decode onode extent key = "
             << pretty_binary_string(key) << dendl;
        return -EIO;
      }
      r = get_key_object(okey, &oid);
      if (r != 0) {
        derr << __func__
             << " failed to decode onode key= " << pretty_binary_string(okey)
             << " from extent key= " << pretty_binary_string(key)
             << dendl;
        return -EIO;
      }
      ceph_assert(oid == edecoder.get_oid());
      edecoder.decode_some(it->value(), nullptr);
      ++stats.shard_count;
    }
  }

  std::lock_guard l(vstatfs_lock);
  store_statfs_t s;
  osd_pools.clear();
  for (auto& p : stats.actual_pool_vstatfs) {
    if (per_pool_stat_collection) {
      osd_pools[p.first] = p.second;
    }
    stats.actual_store_vstatfs += p.second;
    p.second.publish(&s);
    dout(5) << __func__ << " recovered pool "
            << std::hex
            << p.first << "->" << s
            << std::dec
            << " per-pool:" << per_pool_stat_collection
            << dendl;
  }
  vstatfs = stats.actual_store_vstatfs;
  vstatfs.publish(&s);
  dout(5) << __func__ << " recovered " << s
          << dendl;
  return 0;
}

//---------------------------------------------------------
int BlueStore::reconstruct_allocations(SimpleBitmap *sbmap, read_alloc_stats_t &stats)
{
  // first set space used by superblock
  auto super_length = std::max<uint64_t>(min_alloc_size, DB_SUPER_RESERVED);
  set_allocation_in_simple_bmap(sbmap, 0, super_length);
  stats.extent_count++;

  // then set all space taken by Objects
  int ret = read_allocation_from_onodes(sbmap, stats);
  if (ret < 0) {
    derr << "failed read_allocation_from_onodes()" << dendl;
    return ret;
  }

  return 0;
}

//-----------------------------------------------------------------------------------
static void copy_simple_bitmap_to_allocator(SimpleBitmap* sbmap, Allocator* dest_alloc, uint64_t alloc_size)
{
  int alloc_size_shift = std::countr_zero(alloc_size);
  uint64_t offset = 0;
  extent_t ext    = sbmap->get_next_clr_extent(offset);
  while (ext.length != 0) {
    dest_alloc->init_add_free(ext.offset << alloc_size_shift, ext.length << alloc_size_shift);
    offset = ext.offset + ext.length;
    ext = sbmap->get_next_clr_extent(offset);
  }
}

//---------------------------------------------------------
int BlueStore::read_allocation_from_drive_on_startup()
{
  int ret = 0;

  ret = _open_collections();
  if (ret < 0) {
    return ret;
  }
  auto shutdown_cache = make_scope_guard([&] {
    _shutdown_cache();
  });

  utime_t            start = ceph_clock_now();
  read_alloc_stats_t stats = {};
  SimpleBitmap sbmap(cct, (bdev->get_size()/ min_alloc_size));
  ret = reconstruct_allocations(&sbmap, stats);
  if (ret != 0) {
    return ret;
  }

  copy_simple_bitmap_to_allocator(&sbmap, alloc, min_alloc_size);

  utime_t duration = ceph_clock_now() - start;
  dout(1) << "::Allocation Recovery was completed in " << duration << " seconds, extent_count=" << stats.extent_count << dendl;
  return ret;
}




// Only used for debugging purposes - we build a secondary allocator from the Onodes and compare it to the existing one
// Not meant to be run by customers
#ifdef CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

//---------------------------------------------------------
int cmpfunc (const void * a, const void * b)
{
  if ( ((extent_t*)a)->offset > ((extent_t*)b)->offset ) {
    return 1;
  }
  else if( ((extent_t*)a)->offset < ((extent_t*)b)->offset ) {
    return -1;
  }
  else {
    return 0;
  }
}

// compare the allocator built from Onodes with the system allocator (CF-B)
//---------------------------------------------------------
int BlueStore::compare_allocators(Allocator* alloc1, Allocator* alloc2, uint64_t req_extent_count, uint64_t memory_target)
{
  uint64_t allocation_size = std::min((req_extent_count) * sizeof(extent_t), memory_target / 3);
  uint64_t extent_count    = allocation_size/sizeof(extent_t);
  dout(5) << "req_extent_count=" << req_extent_count << ", granted extent_count="<< extent_count << dendl;

  unique_ptr<extent_t[]> arr1;
  unique_ptr<extent_t[]> arr2;
  try {
    arr1 = make_unique<extent_t[]>(extent_count);
    arr2 = make_unique<extent_t[]>(extent_count);
  } catch (std::bad_alloc&) {
    derr << "****Failed dynamic allocation, extent_count=" << extent_count << dendl;
    return -1;
  }

  // copy the extents from the allocators into simple array and then compare them
  uint64_t size1 = 0, size2 = 0;
  uint64_t idx1  = 0, idx2  = 0;
  auto iterated_mapper1 = [&](uint64_t offset, uint64_t length) {
    size1 += length;
    if (idx1 < extent_count) {
      arr1[idx1++] = {offset, length};
    }
    else if (idx1 == extent_count) {
      derr << "(2)compare_allocators:: spillover"  << dendl;
      idx1 ++;
    }

  };

  auto iterated_mapper2 = [&](uint64_t offset, uint64_t length) {
    size2 += length;
    if (idx2 < extent_count) {
      arr2[idx2++] = {offset, length};
    }
    else if (idx2 == extent_count) {
      derr << "(2)compare_allocators:: spillover"  << dendl;
      idx2 ++;
    }
  };

  alloc1->foreach(iterated_mapper1);
  alloc2->foreach(iterated_mapper2);

  qsort(arr1.get(), std::min(idx1, extent_count), sizeof(extent_t), cmpfunc);
  qsort(arr2.get(), std::min(idx2, extent_count), sizeof(extent_t), cmpfunc);

  if (idx1 == idx2) {
    idx1 = idx2 = std::min(idx1, extent_count);
    if (memcmp(arr1.get(), arr2.get(), sizeof(extent_t) * idx2) == 0) {
      return 0;
    }
    derr << "Failed memcmp(arr1, arr2, sizeof(extent_t)*idx2)"  << dendl;
    for (uint64_t i = 0; i < idx1; i++) {
      if (memcmp(arr1.get()+i, arr2.get()+i, sizeof(extent_t)) != 0) {
	derr << "!!!![" << i << "] arr1::<" << arr1[i].offset << "," << arr1[i].length << ">" << dendl;
	derr << "!!!![" << i << "] arr2::<" << arr2[i].offset << "," << arr2[i].length << ">" << dendl;
	return -1;
      }
    }
    return 0;
  } else {
    derr << "mismatch:: idx1=" << idx1 << " idx2=" << idx2 << dendl;
    return -1;
  }
}

//---------------------------------------------------------
int BlueStore::add_existing_bluefs_allocation(Allocator* allocator, read_alloc_stats_t &stats)
{
  // then add space used by bluefs to store rocksdb
  unsigned extent_count = 0;
  if (bluefs) {
    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&](uint64_t start, uint32_t len) {
        allocator->init_rm_free(start, len);
        stats.extent_count++;
      }
    );
  }

  dout(5) << "bluefs extent_count=" << extent_count << dendl;
  return 0;
}

//---------------------------------------------------------
int BlueStore::read_allocation_from_drive_for_bluestore_tool()
{
  dout(5) << __func__ << dendl;
  int ret = 0;
  uint64_t memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  ret = _open_db_and_around(true, false);
  if (ret < 0) {
    return ret;
  }

  ret = _open_collections();
  if (ret < 0) {
    _close_db_and_around();
    return ret;
  }

  utime_t            duration;
  read_alloc_stats_t stats = {};
  utime_t            start = ceph_clock_now();

  auto shutdown_cache = make_scope_guard([&] {
    dout(1) << "Allocation Recovery was completed in " << duration
	    << " seconds; insert_count=" << stats.insert_count
	    << "; extent_count=" << stats.extent_count << dendl;
    _shutdown_cache();
    _close_db_and_around();
  });

  {
    auto allocator = unique_ptr<Allocator>(create_bitmap_allocator(bdev->get_size()));
    //reconstruct allocations into a temp simple-bitmap and copy into allocator
    {
      SimpleBitmap sbmap(cct, (bdev->get_size()/ min_alloc_size));
      ret = reconstruct_allocations(&sbmap, stats);
      if (ret != 0) {
	return ret;
      }
      copy_simple_bitmap_to_allocator(&sbmap, allocator.get(), min_alloc_size);
    }

    // add allocation space used by the bluefs itself
    ret = add_existing_bluefs_allocation(allocator.get(), stats);
    if (ret < 0) {
      return ret;
    }

    duration = ceph_clock_now() - start;
    stats.insert_count = 0;
    auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
      stats.insert_count++;
    };
    allocator->foreach(count_entries);
    ret = compare_allocators(allocator.get(), alloc, stats.insert_count, memory_target);
    if (ret == 0) {
      dout(5) << "Allocator drive - file integrity check OK" << dendl;
    } else {
      derr << "FAILURE. Allocator from file and allocator from metadata differ::ret=" << ret << dendl;
    }
  }

  dout(1) << stats << dendl;
  return ret;
}

//---------------------------------------------------------
Allocator* BlueStore::clone_allocator_without_bluefs(Allocator *src_allocator)
{
  uint64_t   bdev_size = bdev->get_size();
  Allocator* allocator = create_bitmap_allocator(bdev_size);
  if (allocator) {
    dout(5) << "bitmap-allocator=" << allocator << dendl;
  } else {
    derr << "****failed create_bitmap_allocator()" << dendl;
    return nullptr;
  }

  uint64_t num_entries = 0;
  copy_allocator(src_allocator, allocator, &num_entries);

  // BlueFS stores its internal allocation outside RocksDB (FM) so we should not destage them to the allcoator-file
  // we are going to hide bluefs allocation during allocator-destage as they are stored elsewhere
  {
    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&] (uint64_t start, uint32_t len) {
        allocator->init_add_free(start, len);
      }
    );
  }

  return allocator;
}

//---------------------------------------------------------
static void clear_allocation_objects_from_rocksdb(KeyValueDB *db, CephContext *cct, const std::string &path)
{
  dout(5) << "t->rmkeys_by_prefix(PREFIX_ALLOC_BITMAP)" << dendl;
  KeyValueDB::Transaction t = db->get_transaction();
  t->rmkeys_by_prefix(PREFIX_ALLOC_BITMAP);
  db->submit_transaction_sync(t);
}

//---------------------------------------------------------
void BlueStore::copy_allocator_content_to_fm(Allocator *allocator, FreelistManager *real_fm)
{
  unsigned max_txn = 1024;
  dout(5) << "max_transaction_submit=" << max_txn << dendl;
  uint64_t size = 0, idx = 0;
  KeyValueDB::Transaction txn = db->get_transaction();
  auto iterated_insert = [&](uint64_t offset, uint64_t length) {
    size += length;
    real_fm->release(offset, length, txn);
    if ((++idx % max_txn) == 0) {
      db->submit_transaction_sync(txn);
      txn = db->get_transaction();
    }
  };
  allocator->foreach(iterated_insert);
  if (idx % max_txn != 0) {
    db->submit_transaction_sync(txn);
  }
  dout(5) << "size=" << size << ", num extents=" << idx  << dendl;
}

//---------------------------------------------------------
Allocator* BlueStore::initialize_allocator_from_freelist(FreelistManager *real_fm)
{
  dout(5) << "real_fm->enumerate_next" << dendl;
  Allocator* allocator2 = create_bitmap_allocator(bdev->get_size());
  if (allocator2) {
    dout(5) << "bitmap-allocator=" << allocator2 << dendl;
  } else {
    return nullptr;
  }

  uint64_t size2 = 0, idx2 = 0;
  real_fm->enumerate_reset();
  uint64_t offset, length;
  while (real_fm->enumerate_next(db, &offset, &length)) {
    allocator2->init_add_free(offset, length);
    ++idx2;
    size2 += length;
  }
  real_fm->enumerate_reset();

  dout(5) << "size2=" << size2 << ", num2=" << idx2 << dendl;
  return allocator2;
}

//---------------------------------------------------------
// close the active fm and open it in a new mode like makefs()
// but make sure to mark the full device space as allocated
// later we will mark all exetents from the allocator as free
int BlueStore::reset_fm_for_restore()
{
  dout(5) << "<<==>> fm->clear_null_manager()" << dendl;
  fm->shutdown();
  delete fm;
  fm = nullptr;
  freelist_type = "bitmap";
  KeyValueDB::Transaction t = db->get_transaction();
  // call _open_fm() with fm_restore set to TRUE
  // this will mark the full device space as allocated (and not just the reserved space)
  _open_fm(t, true, true, true);
  if (fm == nullptr) {
    derr << "Failed _open_fm()" << dendl;
    return -1;
  }
  db->submit_transaction_sync(t);
  ceph_assert(!fm->is_null_manager());
  dout(5) << "fm was reactivated in full mode" << dendl;
  return 0;
}


//---------------------------------------------------------
// create a temp allocator filled with allocation state from the fm
// and compare it to the base allocator passed in
int BlueStore::verify_rocksdb_allocations(Allocator *allocator)
{
  dout(5) << "verify that alloc content is identical to FM" << dendl;
  // initialize from freelist
  Allocator* temp_allocator = initialize_allocator_from_freelist(fm);
  if (temp_allocator == nullptr) {
    return -1;
  }

  uint64_t insert_count = 0;
  auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    insert_count++;
  };
  temp_allocator->foreach(count_entries);
  uint64_t memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  int ret = compare_allocators(allocator, temp_allocator, insert_count, memory_target);

  delete temp_allocator;

  if (ret == 0) {
    dout(5) << "SUCCESS!!! compare(allocator, temp_allocator)" << dendl;
    return 0;
  } else {
    derr << "**** FAILURE compare(allocator, temp_allocator)::ret=" << ret << dendl;
    return -1;
  }
}

//---------------------------------------------------------
int BlueStore::db_cleanup(int ret)
{
  _shutdown_cache();
  _close_db_and_around();
  return ret;
}

//---------------------------------------------------------
// convert back the system from null-allocator to using rocksdb to store allocation
int BlueStore::push_allocation_to_rocksdb()
{
  if (cct->_conf->bluestore_allocation_from_file) {
    derr << "cct->_conf->bluestore_allocation_from_file must be cleared first" << dendl;
    derr << "please change default to false in ceph.conf file>" << dendl;
    return -1;
  }

  dout(5) << "calling open_db_and_around() in read/write mode" << dendl;
  int ret = _open_db_and_around(false);
  if (ret < 0) {
    return ret;
  }

  if (!fm->is_null_manager()) {
    derr << "This is not a NULL-MANAGER -> nothing to do..." << dendl;
    return db_cleanup(0);
  }

  // start by creating a clone copy of the shared-allocator
  unique_ptr<Allocator> allocator(clone_allocator_without_bluefs(alloc));
  if (!allocator) {
    return db_cleanup(-1);
  }

  // remove all objects of PREFIX_ALLOC_BITMAP from RocksDB to guarantee a clean start
  clear_allocation_objects_from_rocksdb(db, cct, path);

  // then open fm in new mode with the full devie marked as alloctaed
  if (reset_fm_for_restore() != 0) {
    return db_cleanup(-1);
  }

  // push the free-space from the allocator (shared-alloc without bfs) to rocksdb
  copy_allocator_content_to_fm(allocator.get(), fm);

  // compare the allocator info with the info stored in the fm/rocksdb
  if (verify_rocksdb_allocations(allocator.get()) == 0) {
    // all is good -> we can commit to rocksdb allocator
    commit_to_real_manager();
  } else {
    return db_cleanup(-1);
  }

  // can't be too paranoid :-)
  dout(5) << "Running full scale verification..." << dendl;
  // close db/fm/allocator and start fresh
  db_cleanup(0);
  dout(5) << "calling open_db_and_around() in read-only mode" << dendl;
  ret = _open_db_and_around(true);
  if (ret < 0) {
    return db_cleanup(ret);
  }
  ceph_assert(!fm->is_null_manager());
  ceph_assert(verify_rocksdb_allocations(allocator.get()) == 0);

  return db_cleanup(ret);
}

#endif // CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

//-------------------------------------------------------------------------------------
int BlueStore::commit_freelist_type()
{
  // When freelist_type to "bitmap" we will store allocation in RocksDB
  // When allocation-info is stored in a single file we set freelist_type to "null"
  // This will direct the startup code to read allocation from file and not RocksDB
  KeyValueDB::Transaction t = db->get_transaction();
  if (t == nullptr) {
    derr << "db->get_transaction() failed!!!" << dendl;
    return -1;
  }

  bufferlist bl;
  bl.append(freelist_type);
  t->set(PREFIX_SUPER, "freelist_type", bl);

  int ret = db->submit_transaction_sync(t);
  if (ret != 0) {
    derr << "Failed db->submit_transaction_sync(t)" << dendl;
  }
  return ret;
}

//-------------------------------------------------------------------------------------
int BlueStore::commit_to_null_manager()
{
  dout(5) << __func__ << " Set FreelistManager to NULL FM..." << dendl;
  fm->set_null_manager();
  freelist_type = "null";
#if 1
  return commit_freelist_type();
#else
  // should check how long this step take on a big configuration as deletes are expensive
  if (commit_freelist_type() == 0) {
    // remove all objects of PREFIX_ALLOC_BITMAP from RocksDB to guarantee a clean start
    clear_allocation_objects_from_rocksdb(db, cct, path);
  }
#endif
}


//-------------------------------------------------------------------------------------
int BlueStore::commit_to_real_manager()
{
  dout(5) << "Set FreelistManager to Real FM..." << dendl;
  ceph_assert(!fm->is_null_manager());
  freelist_type = "bitmap";
  int ret = commit_freelist_type();
  if (ret == 0) {
    //remove the allocation_file
    invalidate_allocation_file_on_bluefs();
    ret = bluefs->unlink(allocator_dir, allocator_file);
    bluefs->sync_metadata(false);
    if (ret == 0) {
      dout(5) << "Remove Allocation File successfully" << dendl;
    }
    else {
      derr << "Remove Allocation File ret_code=" << ret << dendl;
    }
  }

  return ret;
}

//================================================================================================================
//================================================================================================================
