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

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "BlueStore.h"
#include "kv.h"
#include "include/compat.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "BlueFS.h"
#include "BlueRocksEnv.h"

#define dout_subsys ceph_subsys_bluestore

const string PREFIX_SUPER = "S";   // field -> value
const string PREFIX_STAT = "T";    // field -> value(int64 array)
const string PREFIX_COLL = "C";    // collection name -> cnode_t
const string PREFIX_OBJ = "O";     // object name -> onode_t
const string PREFIX_OVERLAY = "V"; // u64 + offset -> data
const string PREFIX_OMAP = "M";    // u64 + keyname -> value
const string PREFIX_WAL = "L";     // id -> wal_transaction_t
const string PREFIX_ALLOC = "B";   // u64 offset -> u64 length (freelist)

// write a label in the first block.  always use this size.  note that
// bluefs makes a matching assumption about the location of its
// superblock (always the second block of the device).
#define BDEV_LABEL_BLOCK_SIZE  4096

// for bluefs, label (4k) + bluefs super (4k), means we start at 8k.
#define BLUEFS_START  8192

/*
 * object name key structure
 *
 * 2 chars: shard (-- for none, or hex digit, so that we sort properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * 1 char: '.'
 *
 * escaped string: namespace
 *
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are followed just by the key.  otherwise, we are followed by
 *         the key and then the object name.
 * escaped string: key
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 */

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
 */

static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') {
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
  out->push_back('!');
}

static int decode_escaped(const char *p, string *out)
{
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex;
      int r = sscanf(++p, "%2x", &hex);
      if (r < 1)
	return -EINVAL;
      out->push_back((char)hex);
      p += 2;
    } else {
      out->push_back(*p++);
    }
  }
  return p - orig_p;
}

// some things we encode in binary (as le32 or le64); print the
// resulting key strings nicely
static string pretty_binary_string(const string& in)
{
  char buf[10];
  string out;
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	(mode == HEX && in.length() - i >= 4 &&
	 ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	  (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
	  (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
	  (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
	out.append(in.substr(from, i - from));
	out.push_back('\'');
      }
      if (mode != HEX) {
	out.append("0x");
	mode = HEX;
      }
      if (in.length() - i >= 4) {
	// print a whole u32 at once
	snprintf(buf, sizeof(buf), "%08x",
		 (uint32_t)(((unsigned char)in[i] << 24) |
			    ((unsigned char)in[i+1] << 16) |
			    ((unsigned char)in[i+2] << 8) |
			    ((unsigned char)in[i+3] << 0)));
	i += 3;
      } else {
	snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }
      out.append(buf);
    } else {
      if (mode != STRING) {
	out.push_back('\'');
	mode = STRING;
	from = i;
      }
    }
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out;
}

static void _key_encode_shard(shard_id_t shard, string *key)
{
  // make field ordering match with ghobject_t compare operations
  if (shard == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    key->append("--");
  } else {
    char buf[32];
    snprintf(buf, sizeof(buf), "%02x", (int)shard);
    key->append(buf);
  }
}
static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  if (key[0] == '-') {
    *pshard = shard_id_t::NO_SHARD;
  } else {
    unsigned shard;
    int r = sscanf(key, "%x", &shard);
    if (r < 1)
      return NULL;
    *pshard = shard_id_t(shard);
  }
  return key + 2;
}

static void get_coll_key_range(const coll_t& cid, int bits,
			       string *temp_start, string *temp_end,
			       string *start, string *end)
{
  temp_start->clear();
  temp_end->clear();
  start->clear();
  end->clear();

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    _key_encode_shard(pgid.shard, start);
    *end = *start;
    *temp_start = *start;
    *temp_end = *start;

    _key_encode_u64(pgid.pool() + 0x8000000000000000ull, start);
    _key_encode_u64((-2ll - pgid.pool()) + 0x8000000000000000ull, temp_start);
    _key_encode_u32(hobject_t::_reverse_bits(pgid.ps()), start);
    _key_encode_u32(hobject_t::_reverse_bits(pgid.ps()), temp_start);
    start->append(".");
    temp_start->append(".");

    _key_encode_u64(pgid.pool() + 0x8000000000000000ull, end);
    _key_encode_u64((-2ll - pgid.pool()) + 0x8000000000000000ull, temp_end);

    uint64_t end_hash =
      hobject_t::_reverse_bits(pgid.ps()) + (1ull << (32-bits));
    if (end_hash <= 0xffffffffull) {
      _key_encode_u32(end_hash, end);
      _key_encode_u32(end_hash, temp_end);
      end->append(".");
      temp_end->append(".");
    } else {
      _key_encode_u32(0xffffffff, end);
      _key_encode_u32(0xffffffff, temp_end);
      end->append(":");
      temp_end->append(":");
    }
  } else {
    _key_encode_shard(shard_id_t::NO_SHARD, start);
    _key_encode_u64(-1ull + 0x8000000000000000ull, start);
    *end = *start;
    _key_encode_u32(0, start);
    start->append(".");
    _key_encode_u32(0xffffffff, end);
    end->append(":");

    // no separate temp section
    *temp_start = *end;
    *temp_end = *end;
  }
}

static bool is_bnode_key(const string& key)
{
  if (key.size() == 2 + 8 + 4)
    return true;
  return false;
}

static void get_bnode_key(shard_id_t shard, int64_t pool, uint32_t hash,
			  string *key)
{
  key->clear();
  _key_encode_shard(shard, key);
  _key_encode_u64(pool + 0x8000000000000000ull, key);
  _key_encode_u32(hobject_t::_reverse_bits(hash), key);
}

static int get_key_bnode(const string& key, shard_id_t *shard,
			 int64_t *pool, uint32_t *hash)
{
  const char *p = key.c_str();
  if (key.length() < 2 + 8 + 4)
    return -2;
  p = _key_decode_shard(p, shard);
  p = _key_decode_u64(p, (uint64_t*)pool);
  p = _key_decode_u32(p, hash);
  return 0;
}

static int get_key_object(const string& key, ghobject_t *oid);

static void get_object_key(const ghobject_t& oid, string *key)
{
  key->clear();

  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
  key->append(".");

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    // (ASCII chars < = and > sort in that order, yay)
    if (oid.hobj.get_key() < oid.hobj.oid.name) {
      key->append("<");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else if (oid.hobj.get_key() > oid.hobj.oid.name) {
      key->append(">");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
      append_escaped(oid.hobj.oid.name, key);
    }
  } else {
    // no key
    key->append("=");
    append_escaped(oid.hobj.oid.name, key);
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      assert(t == oid);
    }
  }
}

static int get_key_object(const string& key, ghobject_t *oid)
{
  int r;
  const char *p = key.c_str();

  if (key.length() < 2 + 8 + 4)
    return -2;
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);

  oid->hobj.set_bitwise_key_u32(hash);
  if (*p != '.')
    return -5;
  ++p;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -6;
  p += r + 1;

  if (*p == '=') {
    // no key
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -7;
    p += r + 1;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    string okey;
    r = decode_escaped(p, &okey);
    if (r < 0)
      return -8;
    p += r + 1;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -9;
    p += r + 1;
    oid->hobj.set_key(okey);
  } else {
    // malformed
    return -10;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);
  if (*p) {
    // if we get something other than a null terminator here, 
    // something goes wrong.
    return -12;
  }  

  return 0;
}


static void get_overlay_key(uint64_t nid, uint64_t offset, string *out)
{
  _key_encode_u64(nid, out);
  _key_encode_u64(offset, out);
}

// '-' < '.' < '~'
static void get_omap_header(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('-');
}

// hmm, I don't think there's any need to escape the user key since we
// have a clean prefix.
static void get_omap_key(uint64_t id, const string& key, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('.');
  out->append(key);
}

static void rewrite_omap_key(uint64_t id, string old, string *out)
{
  _key_encode_u64(id, out);
  out->append(old.substr(out->length()));
}

static void decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(sizeof(uint64_t) + 1);
}

static void get_omap_tail(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('~');
}

static void get_wal_key(uint64_t seq, string *out)
{
  _key_encode_u64(seq, out);
}


// Buffer

ostream& operator<<(ostream& out, const BlueStore::Buffer& b)
{
  out << "buffer(space " << b.space << " 0x" << std::hex
      << b.offset << "~" << b.length << std::dec
      << " " << BlueStore::Buffer::get_state_name(b.state);
  if (b.flags)
    out << " " << BlueStore::Buffer::get_flag_name(b.flags);
  return out << ")";
}


// Cache
#undef dout_prefix
#define dout_prefix *_dout << "bluestore.Cache(" << this << ") "

void BlueStore::Cache::_touch_onode(OnodeRef& o)
{
  auto p = onode_lru.iterator_to(*o);
  onode_lru.erase(p);
  onode_lru.push_front(*o);
}

void BlueStore::Cache::trim(uint64_t onode_max, uint64_t buffer_max)
{
  std::lock_guard<std::mutex> l(lock);

  dout(20) << __func__ << " onodes " << onode_lru.size() << " / " << onode_max
	   << " buffers " << buffer_size << " / " << buffer_max
	   << dendl;

  _audit_lru("trim start");

  // buffers
  auto i = buffer_lru.end();
  if (buffer_size) {
    assert(i != buffer_lru.begin());
    --i;
  }
  while (buffer_size > buffer_max) {
    Buffer *b = &*i;
    if (b->is_clean()) {
      auto p = b->space->buffer_map.find(b->offset);
      if (i != buffer_lru.begin()) {
	--i;
      }
      dout(20) << __func__ << " rm " << *b << dendl;
      b->space->_rm_buffer(p);
    } else {
      if (i != buffer_lru.begin()) {
	--i;
	continue;
      } else {
	break;
      }
    }
  }

  // onodes
  int num = onode_lru.size() - onode_max;
  if (num <= 0)
    return; // don't even try

  auto p = onode_lru.end();
  if (num)
    --p;
  while (num > 0) {
    Onode *o = &*p;
    int refs = o->nref.load();
    if (refs > 1) {
      dout(20) << __func__ << "  " << o->oid << " has " << refs
	       << " refs; stopping with " << num << " left to trim" << dendl;
      break;
    }
    dout(30) << __func__ << "  trim " << o->oid << dendl;
    if (p != onode_lru.begin()) {
      onode_lru.erase(p--);
    } else {
      onode_lru.erase(p);
      assert(num == 1);
    }
    o->get();  // paranoia
    o->space->onode_map.erase(o->oid);
    o->blob_map._clear();    // clear blobs and their buffers, too
    o->put();
    --num;
  }
}

#ifdef DEBUG_CACHE
void BlueStore::Cache::_audit_lru(const char *when)
{
  if (true) {
    dout(10) << __func__ << " " << when << " start" << dendl;
    uint64_t s = 0;
    for (auto i = buffer_lru.begin(); i != buffer_lru.end(); ++i) {
      s += i->length;
    }
    if (s != buffer_size) {
      derr << __func__ << " buffer_size " << buffer_size << " actual " << s
	   << dendl;
      for (auto i = buffer_lru.begin(); i != buffer_lru.end(); ++i) {
	derr << __func__ << " " << *i << dendl;
      }
      assert(s == buffer_size);
    }
    dout(20) << __func__ << " " << when << " buffer_size " << buffer_size
	     << " ok" << dendl;
  }
  if (false) {
    uint64_t lc = 0, oc = 0;
    set<OnodeSpace*> spaces;
    for (auto i = onode_lru.begin(); i != onode_lru.end(); ++i) {
      assert(i->space->onode_map.count(i->oid));
      if (spaces.count(i->space) == 0) {
	spaces.insert(i->space);
	oc += i->space->onode_map.size();
      }
      ++lc;
    }
    if (lc != oc) {
      derr << " lc " << lc << " oc " << oc << dendl;
    }
  }
}
#endif

struct Int64ArrayMergeOperator : public KeyValueDB::MergeOperator {
  virtual void merge_nonexistant(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  virtual void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) {
    assert(llen == rlen);
    assert((rlen % 8) == 0);
    new_value->resize(rlen);
    const __le64* lv = (const __le64*)ldata;
    const __le64* rv = (const __le64*)rdata;
    __le64* nv = &(__le64&)new_value->at(0);
    for (size_t i = 0; i < rlen >> 3; ++i) {
      nv[i] = lv[i] + rv[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  virtual string name() const {
    return "int64_array";
  }
};

// BufferSpace

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.BufferSpace(" << this << " in " << cache << ") "

void BlueStore::BufferSpace::_clear()
{
  // note: we already hold cache->lock
  dout(10) << __func__ << dendl;
  while (!buffer_map.empty()) {
    _rm_buffer(buffer_map.begin());
  }
}

void BlueStore::BufferSpace::_discard(uint64_t offset, uint64_t length)
{
  cache->_audit_lru("discard start");
  auto i = _data_lower_bound(offset);
  uint64_t end = offset + length;
  while (i != buffer_map.end()) {
    Buffer *b = i->second.get();
    if (b->offset >= offset + length) {
      break;
    }
    if (b->offset < offset) {
      uint64_t front = offset - b->offset;
      if (b->offset + b->length > offset + length) {
	// drop middle (split)
	uint64_t tail = b->offset + b->length - (offset + length);
	if (b->data.length()) {
	  bufferlist bl;
	  bl.substr_of(b->data, b->length - tail, tail);
	  _add_buffer(new Buffer(this, b->state, b->seq, end, bl));
	} else {
	  _add_buffer(new Buffer(this, b->state, b->seq, end, tail));
	}
	cache->buffer_size -= b->length - front;
	b->truncate(front);
	cache->_audit_lru("discard end 1");
	return;
      } else {
	// drop tail
	cache->buffer_size -= b->length - front;
	b->truncate(front);
	++i;
	continue;
      }
    }
    if (b->end() <= end) {
      // drop entire buffer
      _rm_buffer(i++);
      continue;
    }
    // drop front
    uint64_t keep = b->end() - end;
    if (b->data.length()) {
      bufferlist bl;
      bl.substr_of(b->data, b->length - keep, keep);
      _add_buffer(new Buffer(this, b->state, b->seq, end, bl));
      _rm_buffer(i);
    } else {
      _add_buffer(new Buffer(this, b->state, b->seq, end, keep));
      _rm_buffer(i);
    }
    cache->_audit_lru("discard end 2");
    return;
  }
}

void BlueStore::BufferSpace::read(
  uint64_t offset, uint64_t length,
  BlueStore::ready_regions_t& res,
  interval_set<uint64_t>& res_intervals)
{
  std::lock_guard<std::mutex> l(cache->lock);
  res.clear();
  uint64_t end = offset + length;
  for (auto i = _data_lower_bound(offset);
       i != buffer_map.end() && offset < end && i->first < end;
       ++i) {
    Buffer *b = i->second.get();
    assert(b->end() > offset);
    if (b->is_writing() || b->is_clean()) {
      if (b->offset < offset) {
	uint64_t skip = offset - b->offset;
	uint64_t l = MIN(length, b->length - skip);
	res[offset].substr_of(b->data, skip, l);
	res_intervals.insert(offset, l);
	offset += l;
	length -= l;
	cache->_touch_buffer(b);
	continue;
      }
      if (b->offset > offset) {
	uint64_t gap = b->offset - offset;
	if (length <= gap) {
	  break;
	}
	offset += gap;
	length -= gap;
      }
      if (b->length > length) {
	uint64_t l = MIN(length, b->length);
	res[offset].substr_of(b->data, 0, l);
	res_intervals.insert(offset, l);
	offset += l;
	length -= l;
      } else {
	res[offset].append(b->data);
	res_intervals.insert(offset, b->length);
	offset += b->length;
	length -= b->length;
      }
      cache->_touch_buffer(b);
    }
  }
}

void BlueStore::BufferSpace::finish_write(uint64_t seq)
{
  std::lock_guard<std::mutex> l(cache->lock);
  auto i = writing.begin();
  while (i != writing.end()) {
    Buffer *b = &*i;
    dout(20) << __func__ << " " << *b << dendl;
    assert(b->is_writing());
    if (b->seq <= seq) {
      if (b->flags & Buffer::FLAG_NOCACHE) {
	++i;
	_rm_buffer(b);
      } else {
	b->state = Buffer::STATE_CLEAN;
	writing.erase(i++);
      }
    } else {
      ++i;
    }
  }
  cache->_audit_lru("finish_write end");
}

// OnodeSpace

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.OnodeSpace(" << this << " in " << cache << ") "

void BlueStore::OnodeSpace::add(const ghobject_t& oid, OnodeRef o)
{
  std::lock_guard<std::mutex> l(cache->lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  cache->onode_lru.push_front(*o);
}

BlueStore::OnodeRef BlueStore::OnodeSpace::lookup(const ghobject_t& oid)
{
  std::lock_guard<std::mutex> l(cache->lock);
  dout(30) << __func__ << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
  if (p == onode_map.end()) {
    dout(30) << __func__ << " " << oid << " miss" << dendl;
    return OnodeRef();
  }
  dout(30) << __func__ << " " << oid << " hit " << p->second << dendl;
  cache->_touch_onode(p->second);
  return p->second;
}

void BlueStore::OnodeSpace::clear()
{
  std::lock_guard<std::mutex> l(cache->lock);
  dout(10) << __func__ << dendl;
  for (auto &p : onode_map) {
    auto q = cache->onode_lru.iterator_to(*p.second);
    cache->onode_lru.erase(q);

    // clear blobs and their buffers too, while we have cache->lock
    p.second->blob_map._clear();
  }
  onode_map.clear();
}

void BlueStore::OnodeSpace::rename(OnodeRef& oldo,
				     const ghobject_t& old_oid,
				     const ghobject_t& new_oid)
{
  std::lock_guard<std::mutex> l(cache->lock);
  dout(30) << __func__ << " " << old_oid << " -> " << new_oid << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);
  assert(po != pn);

  assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    dout(30) << __func__ << "  removing target " << pn->second << dendl;
    auto p = cache->onode_lru.iterator_to(*pn->second);
    cache->onode_lru.erase(p);
    onode_map.erase(pn);
  }
  OnodeRef o = po->second;

  // install a non-existent onode at old location
  oldo.reset(new Onode(this, old_oid, o->key));
  po->second = oldo;
  cache->onode_lru.push_back(*po->second);

  // add at new position and fix oid, key
  onode_map.insert(make_pair(new_oid, o));
  cache->_touch_onode(o);
  o->oid = new_oid;
  get_object_key(new_oid, &o->key);
}

bool BlueStore::OnodeSpace::map_any(std::function<bool(OnodeRef)> f)
{
  std::lock_guard<std::mutex> l(cache->lock);
  dout(20) << __func__ << dendl;
  for (auto& i : onode_map) {
    if (f(i.second)) {
      return true;
    }
  }
  return false;
}


// BlobMap

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.blobmap(" << this << ") "

void BlueStore::BlobMap::encode(bufferlist& bl) const
{
  uint32_t n = blob_map.size();
  ::encode(n, bl);
  for (auto p = blob_map.begin(); n--; ++p) {
    ::encode(p->id, bl);
    ::encode(p->blob, bl);
  }
}

void BlueStore::BlobMap::decode(bufferlist::iterator& p, Cache *c)
{
  assert(blob_map.empty());
  uint32_t n;
  ::decode(n, p);
  while (n--) {
    int64_t id;
    ::decode(id, p);
    Blob *b = new Blob(id, c);
    ::decode(b->blob, p);
    blob_map.insert(*b);
  }
}

// Bnode

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.bnode(" << this << ") "

void BlueStore::Bnode::put()
{
  if (--nref == 0) {
    dout(20) << __func__ << " removing self from set " << bnode_set << dendl;
    bnode_set->uset.erase(*this);
    delete this;
  }
}

// Onode

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.onode(" << this << ") "

void BlueStore::Onode::flush()
{
  std::unique_lock<std::mutex> l(flush_lock);
  dout(20) << __func__ << " " << flush_txns << dendl;
  while (!flush_txns.empty())
    flush_cond.wait(l);
  dout(20) << __func__ << " done" << dendl;
}



// =======================================================

// Collection

#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << store->path << ").collection(" << cid << ") "

BlueStore::Collection::Collection(BlueStore *ns, Cache *cs, coll_t c)
  : store(ns),
    cache(cs),
    cid(c),
    lock("BlueStore::Collection::lock", true, false),
    exists(true),
    bnode_set(MAX(16, g_conf->bluestore_onode_cache_size / 128)),
    onode_map(cs)
{
}

BlueStore::BnodeRef BlueStore::Collection::get_bnode(
  uint32_t hash
  )
{
  Bnode dummy(hash, string(), NULL);
  auto p = bnode_set.uset.find(dummy);
  if (p == bnode_set.uset.end()) {
    spg_t pgid;
    if (!cid.is_pg(&pgid))
      pgid = spg_t();  // meta
    string key;
    get_bnode_key(pgid.shard, pgid.pool(), hash, &key);
    BnodeRef e = new Bnode(hash, key, &bnode_set);
    dout(10) << __func__ << " hash " << std::hex << hash << std::dec
	     << " created " << e << dendl;

    bufferlist v;
    int r = store->db->get(PREFIX_OBJ, key, &v);
    if (r >= 0) {
      assert(v.length() > 0);
      bufferlist::iterator p = v.begin();
      e->blob_map.decode(p, cache);
      dout(10) << __func__ << " hash " << std::hex << hash << std::dec
	       << " loaded blob_map " << e->blob_map << dendl;
    } else {
      dout(10) << __func__ << " hash " <<std::hex << hash << std::dec
	       << " missed, new blob_map" << dendl;
    }
    bnode_set.uset.insert(*e);
    return e;
  } else {
    dout(10) << __func__ << " hash " << std::hex << hash << std::dec
	     << " had " << &*p << dendl;
    return &*p;
  }
}

BlueStore::OnodeRef BlueStore::Collection::get_onode(
  const ghobject_t& oid,
  bool create)
{
  assert(create ? lock.is_wlocked() : lock.is_locked());

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    if (!oid.match(cnode.bits, pgid.ps())) {
      derr << __func__ << " oid " << oid << " not part of " << pgid
	   << " bits " << cnode.bits << dendl;
      assert(0);
    }
  }

  OnodeRef o = onode_map.lookup(oid);
  if (o)
    return o;

  string key;
  get_object_key(oid, &key);

  dout(20) << __func__ << " oid " << oid << " key "
	   << pretty_binary_string(key) << dendl;

  bufferlist v;
  int r = store->db->get(PREFIX_OBJ, key, &v);
  dout(20) << " r " << r << " v.len " << v.length() << dendl;
  Onode *on;
  if (v.length() == 0) {
    assert(r == -ENOENT);
    if (!g_conf->bluestore_debug_misc &&
	!create)
      return OnodeRef();

    // new
    on = new Onode(&onode_map, oid, key);
  } else {
    // loaded
    assert(r >=0);
    on = new Onode(&onode_map, oid, key);
    on->exists = true;
    bufferlist::iterator p = v.begin();
    ::decode(on->onode, p);
    on->blob_map.decode(p, cache);
  }
  o.reset(on);
  onode_map.add(oid, o);
  return o;
}



// =======================================================

#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << path << ") "


static void aio_cb(void *priv, void *priv2)
{
  BlueStore *store = static_cast<BlueStore*>(priv);
  store->_txc_aio_finish(priv2);
}

BlueStore::BlueStore(CephContext *cct, const string& path)
  : ObjectStore(path),
    cct(cct),
    bluefs(NULL),
    bluefs_shared_bdev(0),
    db(NULL),
    bdev(NULL),
    fm(NULL),
    alloc(NULL),
    path_fd(-1),
    fsid_fd(-1),
    mounted(false),
    coll_lock("BlueStore::coll_lock"),
    nid_last(0),
    nid_max(0),
    throttle_ops(cct, "bluestore_max_ops", cct->_conf->bluestore_max_ops),
    throttle_bytes(cct, "bluestore_max_bytes", cct->_conf->bluestore_max_bytes),
    throttle_wal_ops(cct, "bluestore_wal_max_ops",
		     cct->_conf->bluestore_max_ops +
		     cct->_conf->bluestore_wal_max_ops),
    throttle_wal_bytes(cct, "bluestore_wal_max_bytes",
		       cct->_conf->bluestore_max_bytes +
		       cct->_conf->bluestore_wal_max_bytes),
    wal_seq(0),
    wal_tp(cct,
	   "BlueStore::wal_tp",
           "tp_wal",
	   cct->_conf->bluestore_sync_wal_apply ? 0 : cct->_conf->bluestore_wal_threads,
	   "bluestore_wal_threads"),
    wal_wq(this,
	     cct->_conf->bluestore_wal_thread_timeout,
	     cct->_conf->bluestore_wal_thread_suicide_timeout,
	     &wal_tp),
    finisher(cct),
    kv_sync_thread(this),
    kv_stop(false),
    logger(NULL),
    csum_type(bluestore_blob_t::CSUM_CRC32C),
    sync_wal_apply(cct->_conf->bluestore_sync_wal_apply)
{
  _init_logger();
  g_ceph_context->_conf->add_observer(this);
  set_cache_shards(1);
}

BlueStore::~BlueStore()
{
  g_ceph_context->_conf->remove_observer(this);
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(bluefs == NULL);
  assert(fsid_fd < 0);
  for (auto i : cache_shards) {
    delete i;
  }
  cache_shards.clear();
}

const char **BlueStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "bluestore_csum",
    "bluestore_csum_type",
    "bluestore_compression",
    "bluestore_compression_algorithm",
    "bluestore_compression_min_blob_size",
    "bluestore_compression_max_blob_size",
    NULL
  };
  return KEYS;
}

void BlueStore::handle_conf_change(const struct md_config_t *conf,
				   const std::set<std::string> &changed)
{
  if (changed.count("bluestore_csum_type") ||
      changed.count("bluestore_csum")) {
    _set_csum();
  }
  if (changed.count("bluestore_compression") ||
      changed.count("bluestore_compression_algorithm") ||
      changed.count("bluestore_compression_min_blob_size") ||
      changed.count("bluestore_compression_max_blob_size")) {
    _set_compression();
  }
}

void BlueStore::_set_compression()
{
  comp_min_blob_size = g_conf->bluestore_compression_min_blob_size;
  comp_max_blob_size = g_conf->bluestore_compression_max_blob_size;

  const char *alg = 0;
  if (g_conf->bluestore_compression_algorithm == "snappy") {
    alg = "snappy";
  } else if (g_conf->bluestore_compression_algorithm == "zlib") {
    alg = "zlib";
  } else if (g_conf->bluestore_compression_algorithm.length()) {
    derr << __func__ << " unrecognized compression algorithm '"
	 << g_conf->bluestore_compression_algorithm << "'" << dendl;
  }
  if (alg) {
    compressor = Compressor::create(cct, alg);
    if (!compressor) {
      derr << __func__ << " unable to initialize " << alg << " compressor"
	   << dendl;
    }
  } else {
    compressor = nullptr;
  }
  CompressionMode m = COMP_NONE;
  if (compressor) {
    if (g_conf->bluestore_compression == "force") {
      m = COMP_FORCE;
    } else if (g_conf->bluestore_compression == "aggressive") {
      m = COMP_AGGRESSIVE;
    } else if (g_conf->bluestore_compression == "passive") {
      m = COMP_PASSIVE;
    } else if (g_conf->bluestore_compression == "none") {
      m = COMP_NONE;
    } else {
      derr << __func__ << " unrecognized value '"
	   << g_conf->bluestore_compression
	   << "' for bluestore_compression, reverting to 'none'" << dendl;
      m = COMP_NONE;
    }
  }
  comp_mode = m;
  dout(10) << __func__ << " mode " << get_comp_mode_name(comp_mode)
	   << " alg " << (compressor ? compressor->get_type() : "(none)")
	   << dendl;
}

void BlueStore::_set_csum()
{
  int t = bluestore_blob_t::get_csum_string_type(
    g_conf->bluestore_csum_type);
  if (t < 0 || !g_conf->bluestore_csum) {
    t = bluestore_blob_t::CSUM_NONE;
  }
  csum_type = t;
  dout(10) << __func__ << " csum_type "
	   << bluestore_blob_t::get_csum_type_string(csum_type)
	   << dendl;
}

void BlueStore::_init_logger()
{
  PerfCountersBuilder b(g_ceph_context, "BlueStore",
                        l_bluestore_first, l_bluestore_last);
  b.add_time_avg(l_bluestore_state_prepare_lat, "state_prepare_lat", "Average prepare state latency");
  b.add_time_avg(l_bluestore_state_aio_wait_lat, "state_aio_wait_lat", "Average aio_wait state latency");
  b.add_time_avg(l_bluestore_state_io_done_lat, "state_io_done_lat", "Average io_done state latency");
  b.add_time_avg(l_bluestore_state_kv_queued_lat, "state_kv_queued_lat", "Average kv_queued state latency");
  b.add_time_avg(l_bluestore_state_kv_committing_lat, "state_kv_commiting_lat", "Average kv_commiting state latency");
  b.add_time_avg(l_bluestore_state_kv_done_lat, "state_kv_done_lat", "Average kv_done state latency");
  b.add_time_avg(l_bluestore_state_wal_queued_lat, "state_wal_queued_lat", "Average wal_queued state latency");
  b.add_time_avg(l_bluestore_state_wal_applying_lat, "state_wal_applying_lat", "Average wal_applying state latency");
  b.add_time_avg(l_bluestore_state_wal_aio_wait_lat, "state_wal_aio_wait_lat", "Average aio_wait state latency");
  b.add_time_avg(l_bluestore_state_wal_cleanup_lat, "state_wal_cleanup_lat", "Average cleanup state latency");
  b.add_time_avg(l_bluestore_state_finishing_lat, "state_finishing_lat", "Average finishing state latency");
  b.add_time_avg(l_bluestore_state_done_lat, "state_done_lat", "Average done state latency");

  b.add_u64(l_bluestore_write_pad_bytes, "write_pad_bytes", "Sum for write-op padded bytes");
  b.add_u64(l_bluestore_wal_write_ops, "wal_write_ops", "Sum for wal write op");
  b.add_u64(l_bluestore_wal_write_bytes, "wal_write_bytes", "Sum for wal write bytes");
  b.add_u64(l_bluestore_write_penalty_read_ops, " write_penalty_read_ops", "Sum for write penalty read ops");
  logger = b.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void BlueStore::_shutdown_logger()
{
  g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
}

int BlueStore::get_block_device_fsid(const string& path, uuid_d *fsid)
{
  bluestore_bdev_label_t label;
  int r = _read_bdev_label(path, &label);
  if (r < 0)
    return r;
  *fsid = label.osd_uuid;
  return 0;
}

int BlueStore::_open_path()
{
  assert(path_fd < 0);
  path_fd = ::open(path.c_str(), O_DIRECTORY);
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

int BlueStore::_write_bdev_label(string path, bluestore_bdev_label_t label)
{
  dout(10) << __func__ << " path " << path << " label " << label << dendl;
  bufferlist bl;
  ::encode(label, bl);
  uint32_t crc = bl.crc32c(-1);
  ::encode(crc, bl);
  assert(bl.length() <= BDEV_LABEL_BLOCK_SIZE);
  bufferptr z(BDEV_LABEL_BLOCK_SIZE - bl.length());
  z.zero();
  bl.append(std::move(z));

  int fd = ::open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    fd = -errno;
    derr << __func__ << " failed to open " << path << ": " << cpp_strerror(fd)
	 << dendl;
    return fd;
  }
  int r = bl.write_fd(fd);
  if (r < 0) {
    derr << __func__ << " failed to write to " << path
	 << ": " << cpp_strerror(r) << dendl;
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return r;
}

int BlueStore::_read_bdev_label(string path, bluestore_bdev_label_t *label)
{
  dout(10) << __func__ << dendl;
  int fd = ::open(path.c_str(), O_RDONLY);
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
  bufferlist::iterator p = bl.begin();
  try {
    ::decode(*label, p);
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);
    ::decode(expected_crc, p);
  }
  catch (buffer::error& e) {
    derr << __func__ << " unable to decode label at offset " << p.get_off()
	 << ": " << e.what()
	 << dendl;
    return -EINVAL;
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
    label.btime = ceph_clock_now(NULL);
    label.description = desc;
    int r = _write_bdev_label(path, label);
    if (r < 0)
      return r;
  } else {
    int r = _read_bdev_label(path, &label);
    if (r < 0)
      return r;
    if (label.osd_uuid != fsid) {
      derr << __func__ << " bdev " << path << " fsid " << label.osd_uuid
	   << " does not match our fsid " << fsid << dendl;
      return -EIO;
    }
  }
  return 0;
}

void BlueStore::_set_alloc_sizes(void)
{
  /*
   * Set device block size according to its media
   */
  if (g_conf->bluestore_min_alloc_size) {
    min_alloc_size = g_conf->bluestore_min_alloc_size;
  } else {
    assert(bdev);
    if (bdev->is_rotational()) {
      min_alloc_size = g_conf->bluestore_min_alloc_size_hdd;
    } else {
      min_alloc_size = g_conf->bluestore_min_alloc_size_ssd;
    }
  }
  min_alloc_size_order = ctz(min_alloc_size);
  assert(min_alloc_size == 1u << min_alloc_size_order);

  max_alloc_size = g_conf->bluestore_max_alloc_size;

  dout(10) << __func__ << " min_alloc_size 0x" << std::hex << min_alloc_size
	   << std::dec << " order " << min_alloc_size_order
	   << " max_alloc_size 0x" << std::hex << max_alloc_size
	   << std::dec << dendl;
}

int BlueStore::_open_bdev(bool create)
{
  bluestore_bdev_label_t label;
  assert(bdev == NULL);
  string p = path + "/block";
  bdev = BlockDevice::create(p, aio_cb, static_cast<void*>(this));
  int r = bdev->open(p);
  if (r < 0)
    goto fail;

  if (bdev->supported_bdev_label()) {
    r = _check_or_set_bdev_label(p, bdev->get_size(), "main", create);
    if (r < 0)
      goto fail_close;
  }

  // initialize global block parameters
  block_size = bdev->get_block_size();
  block_mask = ~(block_size - 1);
  block_size_order = ctz(block_size);
  assert(block_size == 1u << block_size_order);

  _set_alloc_sizes();
  max_alloc_size = g_conf->bluestore_max_alloc_size;
  return 0;

 fail_close:
  bdev->close();
 fail:
  delete bdev;
  bdev = NULL;
  return r;
}

void BlueStore::_close_bdev()
{
  assert(bdev);
  bdev->close();
  delete bdev;
  bdev = NULL;
}

int BlueStore::_open_fm(bool create)
{
  assert(fm == NULL);
  fm = FreelistManager::create(freelist_type, db, PREFIX_ALLOC);

  if (create) {
    // initialize freespace
    dout(20) << __func__ << " initializing freespace" << dendl;
    KeyValueDB::Transaction t = db->get_transaction();
    {
      bufferlist bl;
      bl.append(freelist_type);
      t->set(PREFIX_SUPER, "freelist_type", bl);
    }
    fm->create(bdev->get_size(), t);

    uint64_t reserved = 0;
    if (g_conf->bluestore_bluefs) {
      assert(bluefs_extents.num_intervals() == 1);
      interval_set<uint64_t>::iterator p = bluefs_extents.begin();
      reserved = p.get_start() + p.get_len();
      dout(20) << __func__ << " reserved 0x" << std::hex << reserved << std::dec
	       << " for bluefs" << dendl;
      bufferlist bl;
      ::encode(bluefs_extents, bl);
      t->set(PREFIX_SUPER, "bluefs_extents", bl);
      dout(20) << __func__ << " bluefs_extents 0x" << std::hex << bluefs_extents
	       << std::dec << dendl;
    } else {
      reserved = BLUEFS_START;
    }
    fm->allocate(0, reserved, t);

    if (g_conf->bluestore_debug_prefill > 0) {
      uint64_t end = bdev->get_size() - reserved;
      dout(1) << __func__ << " pre-fragmenting freespace, using "
	      << g_conf->bluestore_debug_prefill << " with max free extent "
	      << g_conf->bluestore_debug_prefragment_max << dendl;
      uint64_t start = ROUND_UP_TO(reserved, min_alloc_size);
      uint64_t max_b = g_conf->bluestore_debug_prefragment_max / min_alloc_size;
      float r = g_conf->bluestore_debug_prefill;
      while (start < end) {
	uint64_t l = (rand() % max_b + 1) * min_alloc_size;
	if (start + l > end)
	  l = end - start;
	l = ROUND_UP_TO(l, min_alloc_size);
	uint64_t u = 1 + (uint64_t)(r * (double)l / (1.0 - r));
	u = ROUND_UP_TO(u, min_alloc_size);
	dout(20) << "  free 0x" << std::hex << start << "~" << l
		 << " use 0x" << u << std::dec << dendl;
	fm->allocate(start + l, u, t);
	start += l + u;
      }
    }
    db->submit_transaction_sync(t);
  }

  int r = fm->init();
  if (r < 0) {
    derr << __func__ << " freelist init failed: " << cpp_strerror(r) << dendl;
    delete fm;
    fm = NULL;
    return r;
  }
  return 0;
}

void BlueStore::_close_fm()
{
  dout(10) << __func__ << dendl;
  assert(fm);
  fm->shutdown();
  delete fm;
  fm = NULL;
}

int BlueStore::_open_alloc()
{
  assert(alloc == NULL);
  assert(bdev->get_size());
  alloc = Allocator::create(g_conf->bluestore_allocator,
                            bdev->get_size(),
                            min_alloc_size);
  uint64_t num = 0, bytes = 0;
  fm->enumerate_reset();
  uint64_t offset, length;
  while (fm->enumerate_next(&offset, &length)) {
    alloc->init_add_free(offset, length);
    ++num;
    bytes += length;
  }
  dout(10) << __func__ << " loaded " << pretty_si_t(bytes)
	   << " in " << num << " extents"
	   << dendl;
  return 0;
}

void BlueStore::_close_alloc()
{
  assert(alloc);
  alloc->shutdown();
  delete alloc;
  alloc = NULL;
}

int BlueStore::_open_fsid(bool create)
{
  assert(fsid_fd < 0);
  int flags = O_RDWR;
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
  l.l_start = 0;
  l.l_len = 0;
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

int BlueStore::_open_db(bool create)
{
  int r;
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());
  string options;
  stringstream err;
  ceph::shared_ptr<Int64ArrayMergeOperator> merge_op(new Int64ArrayMergeOperator);

  string kv_backend;
  if (create) {
    kv_backend = g_conf->bluestore_kvbackend;
  } else {
    r = read_meta("kv_backend", &kv_backend);
    if (r < 0) {
      derr << __func__ << " unable to read 'kv_backend' meta" << dendl;
      return -EIO;
    }
  }
  dout(10) << __func__ << " kv_backend = " << kv_backend << dendl;

  bool do_bluefs;
  if (create) {
    do_bluefs = g_conf->bluestore_bluefs;
  } else {
    string s;
    r = read_meta("bluefs", &s);
    if (r < 0) {
      derr << __func__ << " unable to read 'bluefs' meta" << dendl;
      return -EIO;
    }
    if (s == "1") {
      do_bluefs = true;
    } else if (s == "0") {
      do_bluefs = false;
    } else {
      derr << __func__ << " bluefs = " << s << " : not 0 or 1, aborting"
	   << dendl;
      return -EIO;
    }
  }
  dout(10) << __func__ << " bluefs = " << bluefs << dendl;

  rocksdb::Env *env = NULL;
  if (do_bluefs) {
    dout(10) << __func__ << " initializing bluefs" << dendl;
    if (kv_backend != "rocksdb") {
      derr << " backend must be rocksdb to use bluefs" << dendl;
      return -EINVAL;
    }
    bluefs = new BlueFS;

    char bfn[PATH_MAX];
    struct stat st;

    snprintf(bfn, sizeof(bfn), "%s/block.db", path.c_str());
    if (::stat(bfn, &st) == 0) {
      r = bluefs->add_block_device(BlueFS::BDEV_DB, bfn);
      if (r < 0) {
        derr << __func__ << " add block device(" << bfn << ") returned: " 
             << cpp_strerror(r) << dendl;
        goto free_bluefs;
      }
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
      if (create) {
	bluefs->add_block_extent(
	  BlueFS::BDEV_DB,
	  BLUEFS_START,
	  bluefs->get_block_device_size(BlueFS::BDEV_DB) - BLUEFS_START);
      }
      bluefs_shared_bdev = BlueFS::BDEV_SLOW;
    } else {
      bluefs_shared_bdev = BlueFS::BDEV_DB;
    }

    // shared device
    snprintf(bfn, sizeof(bfn), "%s/block", path.c_str());
    r = bluefs->add_block_device(bluefs_shared_bdev, bfn);
    if (r < 0) {
      derr << __func__ << " add block device(" << bfn << ") returned: " 
	   << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }
    if (create) {
      // note: we might waste a 4k block here if block.db is used, but it's
      // simpler.
      uint64_t initial =
	bdev->get_size() * (g_conf->bluestore_bluefs_min_ratio +
			    g_conf->bluestore_bluefs_gift_ratio);
      initial = MAX(initial, g_conf->bluestore_bluefs_min);
      // align to bluefs's alloc_size
      initial = ROUND_UP_TO(initial, g_conf->bluefs_alloc_size);
      initial += g_conf->bluefs_alloc_size - BLUEFS_START;
      bluefs->add_block_extent(bluefs_shared_bdev, BLUEFS_START, initial);
      bluefs_extents.insert(BLUEFS_START, initial);
    }

    snprintf(bfn, sizeof(bfn), "%s/block.wal", path.c_str());
    if (::stat(bfn, &st) == 0) {
      r = bluefs->add_block_device(BlueFS::BDEV_WAL, bfn);
      if (r < 0) {
        derr << __func__ << " add block device(" << bfn << ") returned: " 
	     << cpp_strerror(r) << dendl;
        goto free_bluefs;			
      }
      r = _check_or_set_bdev_label(
	bfn,
	bluefs->get_block_device_size(BlueFS::BDEV_WAL),
        "bluefs wal", create);
      if (r < 0) {
        derr << __func__ << " check block device(" << bfn << ") label returned: "
	     << cpp_strerror(r) << dendl;
        goto free_bluefs;
      }
      if (create) {
	bluefs->add_block_extent(
	  BlueFS::BDEV_WAL, BDEV_LABEL_BLOCK_SIZE,
	  bluefs->get_block_device_size(BlueFS::BDEV_WAL) -
	   BDEV_LABEL_BLOCK_SIZE);
      }
      g_conf->set_val("rocksdb_separate_wal_dir", "true");
    } else {
      g_conf->set_val("rocksdb_separate_wal_dir", "false");
    }

    if (create) {
      bluefs->mkfs(fsid);
    }
    r = bluefs->mount();
    if (r < 0) {
      derr << __func__ << " failed bluefs mount: " << cpp_strerror(r) << dendl;
      goto free_bluefs;
    }
    if (g_conf->bluestore_bluefs_env_mirror) {
      rocksdb::Env *a = new BlueRocksEnv(bluefs);
      unique_ptr<rocksdb::Directory> dir;
      rocksdb::Env *b = rocksdb::Env::Default();
      if (create) {
	string cmd = "rm -rf " + path + "/db " +
	  path + "/db.slow" +
	  path + "/db.wal";
	int r = system(cmd.c_str());
	(void)r;
      }
      env = new rocksdb::EnvMirror(b, a);
    } else {
      env = new BlueRocksEnv(bluefs);

      // simplify the dir names, too, as "seen" by rocksdb
      strcpy(fn, "db");
    }

    if (bluefs_shared_bdev == BlueFS::BDEV_SLOW) {
      // we have both block.db and block; tell rocksdb!
      // note: the second (last) size value doesn't really matter
      char db_paths[PATH_MAX*3];
      snprintf(
	db_paths, sizeof(db_paths), "%s,%lld %s.slow,%lld",
	fn,
	(unsigned long long)bluefs->get_block_device_size(BlueFS::BDEV_DB) *
	 95 / 100,
	fn,
	(unsigned long long)bluefs->get_block_device_size(BlueFS::BDEV_SLOW) *
	 95 / 100);
      g_conf->set_val("rocksdb_db_paths", db_paths, false, false);
      dout(10) << __func__ << " set rocksdb_db_paths to "
	       << g_conf->rocksdb_db_paths << dendl;
    }

    if (create) {
      env->CreateDir(fn);
      if (g_conf->rocksdb_separate_wal_dir)
	env->CreateDir(string(fn) + ".wal");
      if (g_conf->rocksdb_db_paths.length())
	env->CreateDir(string(fn) + ".slow");
    }
  } else if (create) {
    int r = ::mkdir(fn, 0755);
    if (r < 0)
      r = -errno;
    if (r < 0 && r != -EEXIST) {
      derr << __func__ << " failed to create " << fn << ": " << cpp_strerror(r)
	   << dendl;
      return r;
    }

    // wal_dir, too!
    if (g_conf->rocksdb_separate_wal_dir) {
      char walfn[PATH_MAX];
      snprintf(walfn, sizeof(walfn), "%s/db.wal", path.c_str());
      r = ::mkdir(walfn, 0755);
      if (r < 0)
	r = -errno;
      if (r < 0 && r != -EEXIST) {
	derr << __func__ << " failed to create " << walfn
	  << ": " << cpp_strerror(r)
	  << dendl;
	return r;
      }
    }
  }

  db = KeyValueDB::create(g_ceph_context,
			  kv_backend,
			  fn,
			  static_cast<void*>(env));
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    if (bluefs) {
      bluefs->umount();
      delete bluefs;
      bluefs = NULL;
    }
    // delete env manually here since we can't depend on db to do this
    // under this case
    delete env;
    env = NULL;
    return -EIO;
  }

  FreelistManager::setup_merge_operators(db);
  db->set_merge_operator(PREFIX_STAT, merge_op);

  if (kv_backend == "rocksdb")
    options = g_conf->bluestore_rocksdb_options;
  db->init(options);
  if (create)
    r = db->create_and_open(err);
  else
    r = db->open(err);
  if (r) {
    derr << __func__ << " erroring opening db: " << err.str() << dendl;
    if (bluefs) {
      bluefs->umount();
      delete bluefs;
      bluefs = NULL;
    }
    delete db;
    db = NULL;
    return -EIO;
  }
  dout(1) << __func__ << " opened " << kv_backend
	  << " path " << fn << " options " << options << dendl;
  return 0;

free_bluefs:
  assert(bluefs);
  delete bluefs;
  bluefs = NULL;
  return r;
}

void BlueStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
  if (bluefs) {
    bluefs->umount();
    delete bluefs;
    bluefs = NULL;
  }
}

int BlueStore::_reconcile_bluefs_freespace()
{
  dout(10) << __func__ << dendl;
  interval_set<uint64_t> bset;
  int r = bluefs->get_block_extents(bluefs_shared_bdev, &bset);
  assert(r == 0);
  if (bset == bluefs_extents) {
    dout(10) << __func__ << " we agree bluefs has " << bset << dendl;
    return 0;
  }
  dout(10) << __func__ << " bluefs says " << bset << dendl;
  dout(10) << __func__ << " super says  " << bluefs_extents << dendl;

  interval_set<uint64_t> overlap;
  overlap.intersection_of(bset, bluefs_extents);

  bset.subtract(overlap);
  if (!bset.empty()) {
    derr << __func__ << " bluefs extra " << bset << dendl;
    return -EIO;
  }

  interval_set<uint64_t> super_extra;
  super_extra = bluefs_extents;
  super_extra.subtract(overlap);
  if (!super_extra.empty()) {
    // This is normal: it can happen if we commit to give extents to
    // bluefs and we crash before bluefs commits that it owns them.
    dout(10) << __func__ << " super extra " << super_extra << dendl;
    for (interval_set<uint64_t>::iterator p = super_extra.begin();
	 p != super_extra.end();
	 ++p) {
      bluefs->add_block_extent(bluefs_shared_bdev, p.get_start(), p.get_len());
    }
  }

  return 0;
}

int BlueStore::_balance_bluefs_freespace(vector<bluestore_pextent_t> *extents,
					 KeyValueDB::Transaction t)
{
  int ret = 0;
  assert(bluefs);

  vector<pair<uint64_t,uint64_t>> bluefs_usage;  // <free, total> ...
  bluefs->get_usage(&bluefs_usage);
  assert(bluefs_usage.size() > bluefs_shared_bdev);

  // fixme: look at primary bdev only for now
  uint64_t bluefs_free = bluefs_usage[bluefs_shared_bdev].first;
  uint64_t bluefs_total = bluefs_usage[bluefs_shared_bdev].second;
  float bluefs_free_ratio = (float)bluefs_free / (float)bluefs_total;

  uint64_t my_free = alloc->get_free();
  uint64_t total = bdev->get_size();
  float my_free_ratio = (float)my_free / (float)total;

  uint64_t total_free = bluefs_free + my_free;

  float bluefs_ratio = (float)bluefs_free / (float)total_free;

  dout(10) << __func__
	   << " bluefs " << pretty_si_t(bluefs_free)
	   << " free (" << bluefs_free_ratio
	   << ") bluestore " << pretty_si_t(my_free)
	   << " free (" << my_free_ratio
	   << "), bluefs_ratio " << bluefs_ratio
	   << dendl;

  uint64_t gift = 0;
  uint64_t reclaim = 0;
  if (bluefs_ratio < g_conf->bluestore_bluefs_min_ratio) {
    gift = g_conf->bluestore_bluefs_gift_ratio * total_free;
    dout(10) << __func__ << " bluefs_ratio " << bluefs_ratio
	     << " < min_ratio " << g_conf->bluestore_bluefs_min_ratio
	     << ", should gift " << pretty_si_t(gift) << dendl;
  } else if (bluefs_ratio > g_conf->bluestore_bluefs_max_ratio) {
    reclaim = g_conf->bluestore_bluefs_reclaim_ratio * total_free;
    if (bluefs_total - reclaim < g_conf->bluestore_bluefs_min)
      reclaim = bluefs_total - g_conf->bluestore_bluefs_min;
    dout(10) << __func__ << " bluefs_ratio " << bluefs_ratio
	     << " > max_ratio " << g_conf->bluestore_bluefs_max_ratio
	     << ", should reclaim " << pretty_si_t(reclaim) << dendl;
  }
  if (bluefs_total < g_conf->bluestore_bluefs_min) {
    uint64_t g = g_conf->bluestore_bluefs_min;
    dout(10) << __func__ << " bluefs_total " << bluefs_total
	     << " < min " << g_conf->bluestore_bluefs_min
	     << ", should gift " << pretty_si_t(g) << dendl;
    if (g > gift)
      gift = g;
    reclaim = 0;
  }
  if (gift) {
    float new_bluefs_ratio = (float)(bluefs_free + gift) / (float)total_free;
    if (new_bluefs_ratio >= g_conf->bluestore_bluefs_max_ratio) {
      dout(10) << __func__ << " gift would push us past the max_ratio,"
	       << " doing nothing" << dendl;
      gift = 0;
    }
  }

  if (gift) {
    // round up to alloc size
    gift = ROUND_UP_TO(gift, min_alloc_size);

    // hard cap to fit into 32 bits
    gift = MIN(gift, 1ull<<31);
    dout(10) << __func__ << " gifting " << gift
	     << " (" << pretty_si_t(gift) << ")" << dendl;

    // fixme: just do one allocation to start...
    int r = alloc->reserve(gift);
    assert(r == 0);

    uint64_t eoffset;
    uint32_t elength;
    r = alloc->allocate(gift, min_alloc_size, 0, &eoffset, &elength);
    if (r < 0) {
      assert(0 == "allocate failed, wtf");
      return r;
    }
    if (elength < gift) {
      alloc->unreserve(gift - elength);
    }

    bluestore_pextent_t e(eoffset, elength);
    dout(1) << __func__ << " gifting " << e << " to bluefs" << dendl;
    extents->push_back(e);
    ret = 1;
  }

  // reclaim from bluefs?
  if (reclaim) {
    // round up to alloc size
    reclaim = ROUND_UP_TO(reclaim, min_alloc_size);

    // hard cap to fit into 32 bits
    reclaim = MIN(reclaim, 1ull<<31);
    dout(10) << __func__ << " reclaiming " << reclaim
	     << " (" << pretty_si_t(reclaim) << ")" << dendl;

    uint64_t offset = 0;
    uint32_t length = 0;

    // NOTE: this will block and do IO.
    int r = bluefs->reclaim_blocks(bluefs_shared_bdev, reclaim,
				   &offset, &length);
    assert(r >= 0);

    bluefs_extents.erase(offset, length);

    fm->release(offset, length, t);
    alloc->release(offset, length);
    ret = 1;
  }

  return ret;
}

void BlueStore::_commit_bluefs_freespace(
  const vector<bluestore_pextent_t>& bluefs_gift_extents)
{
  dout(10) << __func__ << dendl;
  for (auto& p : bluefs_gift_extents) {
    bluefs->add_block_extent(bluefs_shared_bdev, p.offset, p.length);
  }
}

int BlueStore::_open_collections(int *errors)
{
  assert(coll_map.empty());
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL);
  for (it->upper_bound(string());
       it->valid();
       it->next()) {
    coll_t cid;
    if (cid.parse(it->key())) {
      CollectionRef c(
	new Collection(
	  this,
	  cache_shards[cid.hash_to_shard(cache_shards.size())],
	  cid));
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      try {
        ::decode(c->cnode, p);
      } catch (buffer::error& e) {
        derr << __func__ << " failed to decode cnode, key:"
             << pretty_binary_string(it->key()) << dendl;
        return -EIO;
      }   
      dout(20) << __func__ << " opened " << cid << dendl;
      coll_map[cid] = c;
    } else {
      derr << __func__ << " unrecognized collection " << it->key() << dendl;
      if (errors)
	(*errors)++;
    }
  }
  return 0;
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
  int flags = O_RDWR;
  if (create)
    flags |= O_CREAT;
  if (epath.length()) {
    if (!epath.compare(0, sizeof(SPDK_PREFIX)-1, SPDK_PREFIX)) {
      r = ::symlinkat(epath.c_str(), path_fd, name.c_str());
      if (r < 0) {
        r = -errno;
        derr << __func__ << " failed to create " << name << " symlink to "
    	     << epath << ": " << cpp_strerror(r) << dendl;
        return r;
      }
      int fd = ::openat(path_fd, epath.c_str(), flags, 0644);
      if (fd < 0) {
	r = -errno;
	derr << __func__ << " failed to open " << epath << " file: "
	     << cpp_strerror(r) << dendl;
	return r;
      }
      string serial_number = epath.substr(sizeof(SPDK_PREFIX)-1);
      r = ::write(fd, serial_number.c_str(), serial_number.size());
      assert(r == (int)serial_number.size());
      dout(1) << __func__ << " created " << name << " file with " << dendl;
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    } else {
      r = ::symlinkat(epath.c_str(), path_fd, name.c_str());
      if (r < 0) {
        r = -errno;
        derr << __func__ << " failed to create " << name << " symlink to "
    	   << epath << ": " << cpp_strerror(r) << dendl;
        return r;
      }
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

	if (g_conf->bluestore_block_preallocate_file) {
#ifdef HAVE_POSIX_FALLOCATE
	  r = ::posix_fallocate(fd, 0, size);
	  if (r < 0) {
	    r = -errno;
	    derr << __func__ << " failed to prefallocate " << name << " file to "
	      << size << ": " << cpp_strerror(r) << dendl;
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	    return r;
	  }
#else
	  char data[1024*128];
	  for (uint64_t off = 0; off < size; off += sizeof(data)) {
	    if (off + sizeof(data) > size)
	      r = ::write(fd, data, size - off);
	    else
	      r = ::write(fd, data, sizeof(data));
	    if (r < 0) {
	      r = -errno;
	      derr << __func__ << " failed to prefallocate w/ write " << name << " file to "
		<< size << ": " << cpp_strerror(r) << dendl;
	      VOID_TEMP_FAILURE_RETRY(::close(fd));
	      return r;
	    }
	  }
#endif
	}
	dout(1) << __func__ << " resized " << name << " file to "
		<< pretty_si_t(size) << "B" << dendl;
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

  {
    string done;
    r = read_meta("mkfs_done", &done);
    if (r == 0) {
      dout(1) << __func__ << " already created" << dendl;
      return 0; // idempotent
    }
  }

  {
    string type;
    r = read_meta("type", &type);
    if (r == 0) {
      if (type != "bluestore") {
	dout(1) << __func__ << " expected bluestore, but type is " << type << dendl;
	return -EIO;
      }
    }
    r = write_meta("type", "bluestore");
    if (r < 0)
      return r;
  }

  freelist_type = g_conf->bluestore_freelist_type;

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

  r = _setup_block_symlink_or_file("block", g_conf->bluestore_block_path,
				   g_conf->bluestore_block_size,
				   g_conf->bluestore_block_create);
  if (r < 0)
    goto out_close_fsid;
  if (g_conf->bluestore_bluefs) {
    r = _setup_block_symlink_or_file("block.wal", g_conf->bluestore_block_wal_path,
	g_conf->bluestore_block_wal_size,
	g_conf->bluestore_block_wal_create);
    if (r < 0)
      goto out_close_fsid;
    r = _setup_block_symlink_or_file("block.db", g_conf->bluestore_block_db_path,
	g_conf->bluestore_block_db_size,
	g_conf->bluestore_block_db_create);
    if (r < 0)
      goto out_close_fsid;
  }

  r = _open_bdev(true);
  if (r < 0)
    goto out_close_fsid;

  r = _open_db(true);
  if (r < 0)
    goto out_close_bdev;

  r = _open_fm(true);
  if (r < 0)
    goto out_close_db;

  r = _open_alloc();
  if (r < 0)
    goto out_close_fm;

  r = write_meta("kv_backend", g_conf->bluestore_kvbackend);
  if (r < 0)
    goto out_close_alloc;
  r = write_meta("bluefs", stringify((int)g_conf->bluestore_bluefs));
  if (r < 0)
    goto out_close_alloc;

  if (fsid != old_fsid) {
    r = _write_fsid();
    if (r < 0) {
      derr << __func__ << " error writing fsid: " << cpp_strerror(r) << dendl;
      goto out_close_alloc;
    }
  }

  // indicate success by writing the 'mkfs_done' file
  r = write_meta("mkfs_done", "yes");
  if (r < 0)
    goto out_close_alloc;
  dout(10) << __func__ << " success" << dendl;

 out_close_alloc:
  _close_alloc();
 out_close_fm:
  _close_fm();
 out_close_db:
  _close_db();
 out_close_bdev:
  _close_bdev();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}

void BlueStore::set_cache_shards(unsigned num)
{
  dout(10) << __func__ << " " << num << dendl;
  size_t old = cache_shards.size();
  assert(num >= old);
  cache_shards.resize(num);
  for (unsigned i = old; i < num; ++i) {
    cache_shards[i] = new Cache;
  }
}

int BlueStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

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

  if (g_conf->bluestore_fsck_on_mount) {
    int rc = fsck();
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      return -EIO;
    }
  }

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

  r = _open_db(false);
  if (r < 0)
    goto out_bdev;

  r = _open_super_meta();
  if (r < 0)
    goto out_db;

  r = _open_fm(false);
  if (r < 0)
    goto out_db;

  r = _open_alloc();
  if (r < 0)
    goto out_fm;

  r = _open_collections();
  if (r < 0)
    goto out_alloc;

  if (bluefs) {
    r = _reconcile_bluefs_freespace();
    if (r < 0)
      goto out_coll;
  }

  finisher.start();
  wal_tp.start();
  kv_sync_thread.create("bstore_kv_sync");

  r = _wal_replay();
  if (r < 0)
    goto out_stop;

  _set_csum();
  _set_compression();

  mounted = true;
  return 0;

 out_stop:
  _kv_stop();
  wal_wq.drain();
  wal_tp.stop();
  finisher.wait_for_empty();
  finisher.stop();
 out_coll:
  coll_map.clear();
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

int BlueStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;

  _sync();
  _reap_collections();
  coll_map.clear();

  dout(20) << __func__ << " stopping kv thread" << dendl;
  _kv_stop();
  dout(20) << __func__ << " draining wal_wq" << dendl;
  wal_wq.drain();
  dout(20) << __func__ << " stopping wal_tp" << dendl;
  wal_tp.stop();
  dout(20) << __func__ << " draining finisher" << dendl;
  finisher.wait_for_empty();
  dout(20) << __func__ << " stopping finisher" << dendl;
  finisher.stop();
  dout(20) << __func__ << " closing" << dendl;

  mounted = false;
  _close_alloc();
  _close_fm();
  _close_db();
  _close_bdev();
  _close_fsid();
  _close_path();

  if (g_conf->bluestore_fsck_on_umount) {
    int rc = fsck();
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      return -EIO;
    }
  }
  return 0;
}

int BlueStore::_fsck_verify_blob_map(
  string what,
  const BlobMap& blob_map,
  map<int64_t,bluestore_extent_ref_map_t>& v,
  interval_set<uint64_t> &used_blocks,
  store_statfs_t& expected_statfs)
{
  int errors = 0;
  dout(20) << __func__ << " " << what << " " << v << dendl;
  for (auto& b : blob_map.blob_map) {
    auto pv = v.find(b.id);
    if (pv == v.end()) {
      derr << " " << what << " blob " << b.id
	   << " has no lextent refs" << dendl;
      ++errors;
    }
    if (pv->second != b.blob.ref_map) {
      derr << " " << what << " blob " << b.id
	   << " ref_map " << b.blob.ref_map
	   << " != expected " << pv->second << dendl;
      ++errors;
    }
    v.erase(pv);
    interval_set<uint64_t> span;
    bool compressed = b.blob.is_compressed();
    if (compressed) {
      expected_statfs.compressed += b.blob.compressed_length;
      for (auto& r : b.blob.ref_map.ref_map) {
        expected_statfs.compressed_original += r.second.refs * r.second.length;
      }
    }
    for (auto& p : b.blob.extents) {
      if (!p.is_valid()) {
        continue;
      }
      interval_set<uint64_t> e, i;
      e.insert(p.offset, p.length);
      i.intersection_of(e, used_blocks);
      expected_statfs.allocated += p.length;
      if (compressed) {
        expected_statfs.compressed_allocated += p.length;
      }
      if (!i.empty()) {
	derr << " " << what << " extent(s) " << i
	     << " already allocated" << dendl;
	++errors;
      } else {
	used_blocks.insert(p.offset, p.length);
	if (p.end() > bdev->get_size()) {
	  derr << " " << what << " blob " << b.id << " extent " << e
	       << " past end of block device" << dendl;
	  ++errors;
	}
      }
    }
  }
  for (auto& p : v) {
    derr << " " << what << " blob " << p.first
	 << " dne, has extent refs " << p.second << dendl;
    ++errors;
  }
  return errors;
}

int BlueStore::fsck()
{
  dout(1) << __func__ << dendl;
  int errors = 0;
  set<uint64_t> used_nids;
  set<uint64_t> used_omap_head;
  interval_set<uint64_t> used_blocks;
  KeyValueDB::Iterator it;
  BnodeRef bnode;
  map<int64_t,bluestore_extent_ref_map_t> hash_shared;
  store_statfs_t expected_statfs, actual_statfs;

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

  r = _open_db(false);
  if (r < 0)
    goto out_bdev;

  r = _open_super_meta();
  if (r < 0)
    goto out_db;

  r = _open_fm(false);
  if (r < 0)
    goto out_db;

  r = _open_alloc();
  if (r < 0)
    goto out_fm;

  r = _open_collections(&errors);
  if (r < 0)
    goto out_alloc;

  used_blocks.insert(0, BLUEFS_START);
  if (bluefs) {
    used_blocks.insert(bluefs_extents);
    r = bluefs->fsck();
    if (r < 0) {
      coll_map.clear();
      goto out_alloc;
    }
    if (r > 0)
      errors += r;
  }

  // walk collections, objects
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(1) << __func__ << " collection " << p->first << dendl;
    CollectionRef c = _get_collection(p->first);
    RWLock::RLocker l(c->lock);
    ghobject_t pos;
    while (true) {
      vector<ghobject_t> ols;
      int r = collection_list(p->first, pos, ghobject_t::get_max(), true,
			      100, &ols, &pos);
      if (r < 0) {
	++errors;
	break;
      }
      if (ols.empty()) {
	break;
      }
      for (auto& oid : ols) {
	OnodeRef o = c->get_onode(oid, false);
	if (!o || !o->exists) {
	  derr << __func__ << "  " << oid << " missing" << dendl;
	  ++errors;
	  continue; // go for next object
	}
	if (!bnode || bnode->hash != o->oid.hobj.get_hash()) {
	  if (bnode)
	    errors += _fsck_verify_blob_map(
	      "hash " + stringify(bnode->hash),
	      bnode->blob_map,
	      hash_shared,
	      used_blocks,
	      expected_statfs);
	  bnode = c->get_bnode(o->oid.hobj.get_hash());
	  hash_shared.clear();
	}
	dout(10) << __func__ << "  " << oid << dendl;
	_dump_onode(o, 30);
	if (o->onode.nid) {
	  if (used_nids.count(o->onode.nid)) {
	    derr << " " << oid << " nid " << o->onode.nid << " already in use"
		 << dendl;
	    ++errors;
	    continue; // go for next object
	  }
	  used_nids.insert(o->onode.nid);
	}
	// lextents
	map<int64_t,bluestore_extent_ref_map_t> local_blobs;
	uint64_t lext_next_offset = 0, lext_prev_offset = 0;
	for (auto& l : o->onode.extent_map) {
	  if(l.first < lext_next_offset) {
	    derr << " " << oid << " lextent at 0x" 
		 << std::hex << l.first
		 << "overlaps with the previous one 0x" 
		 << lext_prev_offset << "~" << (lext_next_offset - lext_prev_offset)
		 << std::dec << dendl;
	    ++errors;
	  }
	  lext_next_offset = l.first + l.second.length;
	  lext_prev_offset = l.first;
	  if (l.second.blob >= 0) {
	    local_blobs[l.second.blob].get(l.second.offset, l.second.length);
	  } else {
	    hash_shared[-l.second.blob].get(l.second.offset, l.second.length);
	  }
	  expected_statfs.stored += l.second.length;
	}
	// blobs
	errors += _fsck_verify_blob_map(
	  "object " + stringify(oid),
	  o->blob_map,
	  local_blobs,
	  used_blocks,
	  expected_statfs);
	// overlays
	set<string> overlay_keys;
	map<uint64_t,int> refs;
	for (auto& v : o->onode.overlay_map) {
	  if (v.first + v.second.length > o->onode.size) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " extends past end of object" << dendl;
	    ++errors;
            continue; // go for next overlay
	  }
	  if (v.second.key > o->onode.last_overlay_key) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " is > last_overlay_key " << o->onode.last_overlay_key
		 << dendl;
	    ++errors;
            continue; // go for next overlay
	  }
	  ++refs[v.second.key];
	  string key;
	  bufferlist val;
	  get_overlay_key(o->onode.nid, v.second.key, &key);
	  if (overlay_keys.count(key)) {
	    derr << " " << oid << " dup overlay key " << key << dendl;
	    ++errors;
	  }
	  overlay_keys.insert(key);
	  int r = db->get(PREFIX_OVERLAY, key, &val);
	  if (r < 0) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " failed to fetch: " << cpp_strerror(r) << dendl;
	    ++errors;
            continue;
	  }
	  if (val.length() < v.second.value_offset + v.second.length) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " too short, " << val.length() << dendl;
	    ++errors;
	  }
	}
	for (auto& vr : o->onode.overlay_refs) {
	  if (refs[vr.first] != vr.second) {
	    derr << " " << oid << " overlay key " << vr.first
		 << " says " << vr.second << " refs but we have "
		 << refs[vr.first] << dendl;
	    ++errors;
	  }
	  refs.erase(vr.first);
	}
	for (auto& p : refs) {
	  if (p.second > 1) {
	    derr << " " << oid << " overlay key " << p.first
		 << " has " << p.second << " refs but they are not recorded"
		 << dendl;
	    ++errors;
	  }
	}
	do {
	  string start;
	  get_overlay_key(o->onode.nid, 0, &start);
	  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OVERLAY);
	  if (!it)
	    break;
	  for (it->lower_bound(start); it->valid(); it->next()) {
	    string k = it->key();
	    const char *p = k.c_str();
	    uint64_t nid;
	    p = _key_decode_u64(p, &nid);
	    if (nid != o->onode.nid)
	      break;
	    if (!overlay_keys.count(k)) {
	      derr << " " << oid << " has stray overlay kv pair for "
		   << k << dendl;
	      ++errors;
	    }
	  }
	} while (false);
	// omap
	while (o->onode.omap_head) {
	  if (used_omap_head.count(o->onode.omap_head)) {
	    derr << " " << oid << " omap_head " << o->onode.omap_head
		 << " already in use" << dendl;
	    ++errors;
	    break;
	  }
	  used_omap_head.insert(o->onode.omap_head);
	  // hrm, scan actual key/value pairs?
	  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
	  if (!it)
	    break;
	  string head, tail;
	  get_omap_header(o->onode.omap_head, &head);
	  get_omap_tail(o->onode.omap_head, &tail);
	  it->lower_bound(head);
	  while (it->valid()) {
	    if (it->key() == head) {
	      dout(30) << __func__ << "  got header" << dendl;
	    } else if (it->key() >= tail) {
	      dout(30) << __func__ << "  reached tail" << dendl;
	      break;
	    } else {
	      string user_key;
	      decode_omap_key(it->key(), &user_key);
	      dout(30) << __func__
		       << "  got " << pretty_binary_string(it->key())
		       << " -> " << user_key << dendl;
	      assert(it->key() < tail);
	    }
	    it->next();
	  }
	  break;
	}
      }
    }
  }
  if (bnode) {
    errors += _fsck_verify_blob_map(
      "hash " + stringify(bnode->hash),
      bnode->blob_map,
      hash_shared,
      used_blocks,
      expected_statfs);
    hash_shared.clear();
    bnode.reset();
  }
  statfs(&actual_statfs);
  //fill unaffected fields to be able to compare structs
  expected_statfs.blocks = actual_statfs.blocks;
  expected_statfs.bsize = actual_statfs.bsize;
  expected_statfs.available = actual_statfs.available;
  if (!(actual_statfs == expected_statfs)) {
    dout(30) << __func__ << "  actual statfs differs from the expected one:"
             << actual_statfs << " vs. "
             << expected_statfs << dendl;
    ++errors;
  }

  dout(1) << __func__ << " checking for stray bnodes and onodes" << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (it) {
    CollectionRef c;
    bool expecting_objects = false;
    shard_id_t expecting_shard;
    int64_t expecting_pool;
    uint32_t expecting_hash;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      ghobject_t oid;
      if (is_bnode_key(it->key())) {
	if (expecting_objects) {
	  dout(30) << __func__ << "  had bnode but no objects for 0x"
		   << std::hex << expecting_hash << std::dec << dendl;
	  ++errors;
	}
	get_key_bnode(it->key(), &expecting_shard, &expecting_pool,
		      &expecting_hash);
	continue;
      }
      int r = get_key_object(it->key(), &oid);
      if (r < 0) {
	dout(30) << __func__ << "  bad object key "
		 << pretty_binary_string(it->key()) << dendl;
	++errors;
	continue;
      }
      if (expecting_objects) {
	if (oid.hobj.get_bitwise_key_u32() != expecting_hash) {
	  dout(30) << __func__ << "  had bnode but no objects for 0x"
		   << std::hex << expecting_hash << std::dec << dendl;
	  ++errors;
	}
	expecting_objects = false;
      }
      if (!c || !c->contains(oid)) {
	c = NULL;
	for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
	       coll_map.begin();
	     p != coll_map.end();
	     ++p) {
	  if (p->second->contains(oid)) {
	    c = p->second;
	    break;
	  }
	}
	if (!c) {
	  dout(30) << __func__ << "  stray object " << oid
		   << " not owned by any collection" << dendl;
	  ++errors;
	  continue;
	}
      }
    }
    if (expecting_objects) {
      dout(30) << __func__ << "  had bnode but no objects for 0x"
	       << std::hex << expecting_hash << std::dec << dendl;
      ++errors;
      expecting_objects = false;
    }
  }

  dout(1) << __func__ << " checking for stray overlay data" << dendl;
  it = db->get_iterator(PREFIX_OVERLAY);
  if (it) {
    for (it->lower_bound(string()); it->valid(); it->next()) {
      string key = it->key();
      const char *p = key.c_str();
      uint64_t nid;
      p = _key_decode_u64(p, &nid);
      if (used_nids.count(nid) == 0) {
	derr << __func__ << " found stray overlay data on nid " << nid << dendl;
	++errors;
      }
    }
  }

  dout(1) << __func__ << " checking for stray omap data" << dendl;
  it = db->get_iterator(PREFIX_OMAP);
  if (it) {
    for (it->lower_bound(string()); it->valid(); it->next()) {
      string key = it->key();
      const char *p = key.c_str();
      uint64_t omap_head;
      p = _key_decode_u64(p, &omap_head);
      if (used_omap_head.count(omap_head) == 0) {
	derr << __func__ << " found stray omap data on omap_head " << omap_head
	     << dendl;
	++errors;
      }
    }
  }

  dout(1) << __func__ << " checking wal events" << dendl;
  {
    it = db->get_iterator(PREFIX_WAL);
    for (it->lower_bound(string()); it->valid(); it->next()) {
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      bluestore_wal_transaction_t wt;
      try {
	::decode(wt, p);
      } catch (buffer::error& e) {
	derr << __func__ << " failed to decode wal txn "
	     << pretty_binary_string(it->key()) << dendl;
	r = -EIO;
        goto out_scan;
      }
      dout(20) << __func__ << "  wal " << wt.seq
	       << " ops " << wt.ops.size()
	       << " released " << wt.released << dendl;
      used_blocks.insert(wt.released);
    }
  }

  dout(1) << __func__ << " checking freelist vs allocated" << dendl;
  {
    fm->enumerate_reset();
    uint64_t offset, length;
    while (fm->enumerate_next(&offset, &length)) {
      if (used_blocks.intersects(offset, length)) {
	derr << __func__ << " free extent 0x" << std::hex << offset
	     << "~" << length << std::dec
	     << " intersects allocated blocks" << dendl;
	interval_set<uint64_t> free, overlap;
	free.insert(offset, length);
	overlap.intersection_of(free, used_blocks);
	derr << __func__ << " overlap: " << overlap << dendl;
	++errors;
	continue;
      }
      used_blocks.insert(offset, length);
    }
    if (!used_blocks.contains(0, bdev->get_size())) {
      derr << __func__ << " leaked some space; free+used = "
	   << used_blocks
	   << " != expected 0~" << bdev->get_size()
	   << dendl;
      ++errors;
    }
  }

 out_scan:
  coll_map.clear();
 out_alloc:
  _close_alloc();
 out_fm:
  _close_fm();
 out_db:
  it.reset();  // before db is closed
  _close_db();
 out_bdev:
  _close_bdev();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();

  // fatal errors take precedence
  if (r < 0)
    return r;

  dout(1) << __func__ << " finish with " << errors << " errors" << dendl;
  return errors;
}

void BlueStore::_sync()
{
  dout(10) << __func__ << dendl;

  // flush aios in flight
  bdev->flush();

  std::unique_lock<std::mutex> l(kv_lock);
  while (!kv_committing.empty() ||
	 !kv_queue.empty()) {
    dout(20) << " waiting for kv to commit" << dendl;
    kv_sync_cond.wait(l);
  }

  dout(10) << __func__ << " done" << dendl;
}

int BlueStore::statfs(struct store_statfs_t *buf)
{
  memset(buf, 0, sizeof(*buf));
  uint64_t bluefs_len = 0;
  for (interval_set<uint64_t>::iterator p = bluefs_extents.begin();
      p != bluefs_extents.end(); p++)
    bluefs_len += p.get_len();

  buf->reset();

  buf->blocks = bdev->get_size() / block_size;
  buf->bsize = block_size;
  buf->available = (alloc->get_free() - bluefs_len);

  bufferlist bl;
  int r = db->get(PREFIX_STAT, "bluestore_statfs", &bl);
  if (r >= 0) {
       TransContext::volatile_statfs vstatfs;
     if (size_t(bl.length()) >= sizeof(vstatfs.values)) {
       auto it = bl.begin();
       vstatfs.decode(it);

       buf->allocated = vstatfs.allocated();
       buf->stored = vstatfs.stored();
       buf->compressed = vstatfs.compressed();
       buf->compressed_original = vstatfs.compressed_original();
       buf->compressed_allocated = vstatfs.compressed_allocated();
     } else {
       dout(10) << __func__ << " store_statfs is corrupt, using empty" << dendl;
     }
  } else {
    dout(10) << __func__ << " store_statfs missed, using empty" << dendl;
  }


  dout(20) << __func__ << *buf << dendl;
  return 0;
}

// ---------------
// cache

BlueStore::CollectionRef BlueStore::_get_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void BlueStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c->cid << dendl;
  std::lock_guard<std::mutex> l(reap_lock);
  removed_collections.push_back(c);
}

void BlueStore::_reap_collections()
{
  list<CollectionRef> removed_colls;
  {
    std::lock_guard<std::mutex> l(reap_lock);
    removed_colls.swap(removed_collections);
  }

  for (list<CollectionRef>::iterator p = removed_colls.begin();
       p != removed_colls.end();
       ++p) {
    CollectionRef c = *p;
    dout(10) << __func__ << " " << c->cid << dendl;
    if (c->onode_map.map_any([&](OnodeRef o) {
	  assert(!o->exists);
	  if (!o->flush_txns.empty()) {
	    dout(10) << __func__ << " " << c->cid << " " << o->oid
		     << " flush_txns " << o->flush_txns << dendl;
	    return false;
	  }
	  return true;
	})) {
      return;
    }
    c->onode_map.clear();
    dout(10) << __func__ << " " << c->cid << " done" << dendl;
  }

  dout(10) << __func__ << " all reaped" << dendl;
}

// ---------------
// read operations

ObjectStore::CollectionHandle BlueStore::open_collection(const coll_t& cid)
{
  return _get_collection(cid);
}

bool BlueStore::exists(const coll_t& cid, const ghobject_t& oid)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return false;
  return exists(c, oid);
}

bool BlueStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return false;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return false;
  return true;
}

int BlueStore::stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return stat(c, oid, st, allow_eio);
}

int BlueStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  if (!c->exists)
    return -ENOENT;
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return -ENOENT;
  st->st_size = o->onode.size;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int BlueStore::read(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return read(c, oid, offset, length, bl, op_flags, allow_eio);
}

int BlueStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  const coll_t &cid = c->get_cid();
  dout(15) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  bl.clear();

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (offset == length && offset == 0)
    length = o->onode.size;

  r = _do_read(c, o, offset, length, bl, op_flags);

 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_read(
  Collection *c,
  OnodeRef o,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags)
{
  map<uint64_t,bluestore_lextent_t>::iterator ep, eend;
  int r = 0;

  // generally, don't buffer anything, unless the client explicitly requests
  // it.
  bool buffered = false;
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered read" << dendl;
    buffered = true;
  } else if (g_conf->bluestore_default_buffered_read &&
	     (op_flags & (CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
			  CEPH_OSD_OP_FLAG_FADVISE_NOCACHE)) == 0) {
    dout(20) << __func__ << " defaulting to buffered read" << dendl;
    buffered = true;
  }

  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " size 0x" << o->onode.size << " (" << std::dec
	   << o->onode.size << ")" << dendl;
  bl.clear();

  if (offset >= o->onode.size) {
    return r;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->flush();
  _dump_onode(o);

  ready_regions_t ready_regions;

  // build blob-wise list to of stuff read (that isn't cached)
  blobs2read_t blobs2read;
  unsigned left = length;
  uint64_t pos = offset;
  auto lp = o->onode.seek_lextent(offset);
  while (left > 0 && lp != o->onode.extent_map.end()) {
    if (pos < lp->first) {
      unsigned hole = lp->first - pos;
      if (hole >= left) {
	break;
      }
      dout(30) << __func__ << "  hole 0x" << std::hex << pos << "~" << hole
	       << std::dec << dendl;
      pos += hole;
      left -= hole;
      continue;
    }
    Blob *bptr = c->get_blob(o, lp->second.blob);
    if (bptr == nullptr) {
      dout(20) << __func__ << "  missed blob " << lp->second.blob << dendl;
    }
    assert(bptr != nullptr);
    unsigned l_off = pos - lp->first;
    unsigned b_off = l_off + lp->second.offset;
    unsigned b_len = std::min(left, lp->second.length - l_off);
    assert(b_len <= bptr->blob.length - b_off);

    ready_regions_t cache_res;
    interval_set<uint64_t> cache_interval;
    bptr->bc.read(b_off, b_len, cache_res, cache_interval);
    dout(20) << __func__ << "  blob " << *bptr << std::hex
	     << " need 0x" << b_off << "~" << b_len
	     << " cache has 0x" << cache_interval
	     << std::dec << dendl;

    auto pc = cache_res.begin();
    while (b_len > 0) {
      unsigned l;
      if (pc != cache_res.end() &&
	  pc->first == b_off) {
	l = pc->second.length();
	ready_regions[pos].claim(pc->second);
	dout(30) << __func__ << "    use cache 0x" << std::hex << pos << ": 0x"
		 << b_off << "~" << l << std::dec << dendl;
	++pc;
      } else {
	l = b_len;
	if (pc != cache_res.end()) {
	  assert(pc->first > b_off);
	  l = pc->first - b_off;
	}
	dout(30) << __func__ << "    will read 0x" << std::hex << pos << ": 0x"
		 << b_off << "~" << l << std::dec << dendl;
	blobs2read[bptr].emplace_back(region_t(pos, b_off, l));
      }
      pos += l;
      b_off += l;
      left -= l;
      b_len -= l;
    }
    ++lp;
  }

  //enumerate and read/decompress desired blobs
  blobs2read_t::iterator b2r_it = blobs2read.begin();
  while (b2r_it != blobs2read.end()) {
    Blob* bptr = b2r_it->first;
    dout(20) << __func__ << "  blob " << *bptr << std::hex
	     << " need 0x" << b2r_it->second << std::dec << dendl;
    if (bptr->blob.has_flag(bluestore_blob_t::FLAG_COMPRESSED)) {
      bufferlist compressed_bl, raw_bl;
      IOContext ioc(NULL);   // FIXME?
      bptr->blob.map(
	0, bptr->blob.get_ondisk_length(),
	[&](uint64_t offset, uint64_t length) {
	  bufferlist t;
	  int r = bdev->read(offset, length, &t, &ioc, false);
	  assert(r == 0);
	  compressed_bl.claim_append(t);
	});
      if (_verify_csum(o, &bptr->blob, 0, compressed_bl) < 0) {
	return -EIO;
      }
      r = _decompress(compressed_bl, &raw_bl);
      if (r < 0)
	return r;
      if (buffered) {
	bptr->bc.did_read(0, raw_bl);
      }
      for (auto& i : b2r_it->second) {
	ready_regions[i.logical_offset].substr_of(
	  raw_bl, i.blob_xoffset, i.length);
      }
    } else {
      for (auto reg : b2r_it->second) {
	// determine how much of the blob to read
	unsigned chunk_size = MAX(block_size, bptr->blob.get_csum_chunk_size());
	uint64_t r_off = reg.blob_xoffset;
	uint64_t r_len = reg.length;
	unsigned front = r_off % chunk_size;
	if (front) {
	  r_off -= front;
	  r_len += front;
	}
	unsigned tail = r_len % chunk_size;
	if (tail) {
	  r_len += chunk_size - tail;
	}
	dout(20) << __func__ << "    region 0x" << std::hex
		 << reg.logical_offset
		 << ": 0x" << reg.blob_xoffset << "~" << reg.length
		 << " reading 0x" << r_off << "~" << r_len
		 << dendl;

	// read it
	IOContext ioc(NULL);  // FIXME?
	bufferlist bl;
	bptr->blob.map(r_off, r_len, [&](uint64_t offset, uint64_t length) {
	    bufferlist t;
	    int r = bdev->read(offset, length, &t, &ioc, false);
	    assert(r == 0);
	    bl.claim_append(t);
	  });
	int r = _verify_csum(o, &bptr->blob, r_off, bl);
	if (r < 0) {
	  return r;
	}
	if (buffered) {
	  bptr->bc.did_read(r_off, bl);
	}

	// prune and keep result
	ready_regions[reg.logical_offset].substr_of(bl, front, reg.length);
      }
    }
    ++b2r_it;
  }

  // generate a resulting buffer
  auto pr = ready_regions.begin();
  auto pr_end = ready_regions.end();
  pos = 0;
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
	l = pr->first - (pos + offset);
      }
      dout(30) << __func__ << " assemble 0x" << std::hex << pos
	       << ": zeros for 0x" << (pos + offset) << "~" << l
	       << std::dec << dendl;
      bl.append_zero(l);
      pos += l;
    }
  }
  assert(bl.length() == length);
  assert(pos == length);
  assert(pr == pr_end);
  r = bl.length();
  return r;
}

int BlueStore::_verify_csum(OnodeRef& o,
			    const bluestore_blob_t* blob, uint64_t blob_xoffset,
			    const bufferlist& bl) const
{
  int bad;
  int r = blob->verify_csum(blob_xoffset, bl, &bad);
  if (r < 0) {
    if (r == -1) {
      vector<bluestore_pextent_t> pex;
      blob->map(
	blob_xoffset,
	blob->get_csum_chunk_size(),
	[&](uint64_t offset, uint64_t length) {
	  pex.emplace_back(bluestore_pextent_t(offset, length));
	});
      derr << __func__ << " bad " << blob->get_csum_type_string(blob->csum_type)
	   << "/0x" << std::hex << blob->get_csum_chunk_size()
	   << " checksum at blob offset 0x" << bad << std::dec
	   << ", device location " << pex
	   << ", object " << o->oid << dendl;
    } else {
      derr << __func__ << " failed with exit code: " << cpp_strerror(r) << dendl;
    }
    return r;
  } else {
    return 0;
  }
}

int BlueStore::_decompress(bufferlist& source, bufferlist* result)
{
  int r = 0;
  bufferlist::iterator i = source.begin();
  bluestore_compression_header_t chdr;
  ::decode(chdr, i);
  CompressorRef compressor = Compressor::create(cct, chdr.type);
  if (!compressor.get()) {
    // if compressor isn't available - error, because cannot return
    // decompressed data?
    derr << __func__ << " can't load decompressor " << chdr.type << dendl;
    r = -EIO;
  } else {
    r = compressor->decompress(i, chdr.length, *result);
    if (r < 0) {
      derr << __func__ << " decompression failed with exit code " << r << dendl;
      r = -EIO;
    }
  }
  return r;
}

int BlueStore::fiemap(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return fiemap(c, oid, offset, len, bl);
}

int BlueStore::fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl)
{
  Collection *c = static_cast<Collection*>(c_.get());
  if (!c->exists)
    return -ENOENT;
  interval_set<uint64_t> m;
  RWLock::RLocker l(c->lock);

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    return -ENOENT;
  }
  _dump_onode(o);

  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " size 0x" << o->onode.size << std::dec << dendl;

  map<uint64_t,bluestore_lextent_t>::iterator ep, eend;
 if (offset > o->onode.size)
    goto out;

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  eend = o->onode.extent_map.end();
  ep = o->onode.extent_map.lower_bound(offset);
  if (ep != o->onode.extent_map.begin()) {
    --ep;
  }
  while (length > 0) {
    dout(20) << __func__ << " offset " << offset << dendl;
    if (ep != eend && ep->first + ep->second.length <= offset) {
      ++ep;
      continue;
    }

    uint64_t x_len = length;
    if (ep != eend && ep->first <= offset) {
      uint64_t x_off = offset - ep->first;
      x_len = MIN(x_len, ep->second.length - x_off);
      dout(30) << __func__ << " lextent 0x" << std::hex << offset << "~"
	       << x_len << std::dec << " blob " << ep->second.blob << dendl;
      m.insert(offset, x_len);
      length -= x_len;
      offset += x_len;
      if (x_off + x_len == ep->second.length)
	++ep;
      continue;
    }
    if (ep != eend &&
	ep->first > offset &&
	ep->first - offset < x_len) {
      x_len = ep->first - offset;
    }
    offset += x_len;
    length -= x_len;
  }

 out:
  ::encode(m, bl);
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " size = 0 (" << m << ")" << std::dec << dendl;
  return 0;
}

int BlueStore::getattr(
  const coll_t& cid,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattr(c, oid, name, value);
}

int BlueStore::getattr(
  CollectionHandle &c_,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;
  string k(name);

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
 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}


int BlueStore::getattrs(
  const coll_t& cid,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattrs(c, oid, aset);
}

int BlueStore::getattrs(
  CollectionHandle &c_,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  aset = o->onode.attrs;
  r = 0;
 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int BlueStore::list_collections(vector<coll_t>& ls)
{
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool BlueStore::collection_exists(const coll_t& c)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(c);
}

bool BlueStore::collection_empty(const coll_t& cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list(cid, ghobject_t(), ghobject_t::get_max(), true, 1,
			  &ls, &next);
  if (r < 0)
    return false;  // fixme?
  bool empty = ls.empty();
  dout(10) << __func__ << " " << cid << " = " << (int)empty << dendl;
  return empty;
}

int BlueStore::collection_bits(const coll_t& cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  dout(10) << __func__ << " " << cid << " = " << c->cnode.bits << dendl;
  return c->cnode.bits;
}

int BlueStore::collection_list(
  const coll_t& cid, ghobject_t start, ghobject_t end,
  bool sort_bitwise, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return collection_list(c, start, end, sort_bitwise, max, ls, pnext);
}

int BlueStore::collection_list(
  CollectionHandle &c_, ghobject_t start, ghobject_t end,
  bool sort_bitwise, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->cid
	   << " start " << start << " end " << end << " max " << max << dendl;
  if (!c->exists)
    return -ENOENT;
  if (!sort_bitwise)
    return -EOPNOTSUPP;
  RWLock::RLocker l(c->lock);
  int r = 0;
  KeyValueDB::Iterator it;
  string temp_start_key, temp_end_key;
  string start_key, end_key;
  bool set_next = false;
  string pend;
  bool temp;

  ghobject_t static_next;
  if (!pnext)
    pnext = &static_next;

  if (start == ghobject_t::get_max() ||
      start.hobj.is_max()) {
    goto out;
  }
  get_coll_key_range(c->cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__
	   << " range " << pretty_binary_string(temp_start_key)
	   << " to " << pretty_binary_string(temp_end_key)
	   << " and " << pretty_binary_string(start_key)
	   << " to " << pretty_binary_string(end_key)
	   << " start " << start << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (start == ghobject_t() ||
      start.hobj == hobject_t() ||
      start == c->cid.get_min_hobj()) {
    it->upper_bound(temp_start_key);
    temp = true;
  } else {
    string k;
    get_object_key(start, &k);
    if (start.hobj.is_temp()) {
      temp = true;
      assert(k >= temp_start_key && k < temp_end_key);
    } else {
      temp = false;
      assert(k >= start_key && k < end_key);
    }
    dout(20) << " start from " << pretty_binary_string(k)
	     << " temp=" << (int)temp << dendl;
    it->lower_bound(k);
  }
  if (end.hobj.is_max()) {
    pend = temp ? temp_end_key : end_key;
  } else {
    get_object_key(end, &end_key);
    if (end.hobj.is_temp()) {
      if (temp)
	pend = end_key;
      else
	goto out;
    } else {
      pend = temp ? temp_end_key : end_key;
    }
  }
  dout(20) << __func__ << " pend " << pretty_binary_string(pend) << dendl;
  while (true) {
    if (!it->valid() || it->key() > pend) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " key " << pretty_binary_string(it->key())
		 << " > " << end << dendl;
      if (temp) {
	if (end.hobj.is_temp()) {
	  break;
	}
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(start_key);
	pend = end_key;
	dout(30) << __func__ << " pend " << pretty_binary_string(pend) << dendl;
	continue;
      }
      break;
    }
    if (is_bnode_key(it->key())) {
      dout(20) << __func__ << " key "
	       << pretty_binary_string(it->key())
	       << " (bnode, skipping)" << dendl;
      it->next();
      continue;
    }
    dout(20) << __func__ << " key " << pretty_binary_string(it->key()) << dendl;
    ghobject_t oid;
    int r = get_key_object(it->key(), &oid);
    assert(r == 0);
    if (ls->size() >= (unsigned)max) {
      dout(20) << __func__ << " reached max " << max << dendl;
      *pnext = oid;
      set_next = true;
      break;
    }
    ls->push_back(oid);
    it->next();
  }
  if (!set_next) {
    *pnext = ghobject_t::get_max();
  }
 out:
  dout(10) << __func__ << " " << c->cid
	   << " start " << start << " end " << end << " max " << max
	   << " = " << r << ", ls.size() = " << ls->size()
	   << ", next = " << *pnext << dendl;
  return r;
}

// omap reads

BlueStore::OmapIteratorImpl::OmapIteratorImpl(
  CollectionRef c, OnodeRef o, KeyValueDB::Iterator it)
  : c(c), o(o), it(it)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    get_omap_key(o->onode.omap_head, string(), &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
  }
}

int BlueStore::OmapIteratorImpl::seek_to_first()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int BlueStore::OmapIteratorImpl::upper_bound(const string& after)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, after, &key);
    it->upper_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int BlueStore::OmapIteratorImpl::lower_bound(const string& to)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, to, &key);
    it->lower_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

bool BlueStore::OmapIteratorImpl::valid()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head && it->valid() && it->raw_key().second <= tail) {
    return true;
  } else {
    return false;
  }
}

int BlueStore::OmapIteratorImpl::next(bool validate)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->next();
    return 0;
  } else {
    return -1;
  }
}

string BlueStore::OmapIteratorImpl::key()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  decode_omap_key(db_key, &user_key);
  return user_key;
}

bufferlist BlueStore::OmapIteratorImpl::value()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->value();
}

int BlueStore::omap_get(
  const coll_t& cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return omap_get(c, oid, header, out);
}

int BlueStore::omap_get(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(o->onode.omap_head, &head);
    get_omap_tail(o->onode.omap_head, &tail);
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
	decode_omap_key(it->key(), &user_key);
	dout(30) << __func__ << "  got " << pretty_binary_string(it->key())
		 << " -> " << user_key << dendl;
	assert(it->key() < tail);
	(*out)[user_key] = it->value();
      }
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::omap_get_header(
  const coll_t& cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return omap_get_header(c, oid, header, allow_eio);
}

int BlueStore::omap_get_header(
  CollectionHandle &c_,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    string head;
    get_omap_header(o->onode.omap_head, &head);
    if (db->get(PREFIX_OMAP, head, header) >= 0) {
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
  const coll_t& cid,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return omap_get_keys(c, oid, keys);
}

int BlueStore::omap_get_keys(
  CollectionHandle &c_,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_key(o->onode.omap_head, string(), &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      }
      string user_key;
      decode_omap_key(it->key(), &user_key);
      dout(30) << __func__ << "  got " << pretty_binary_string(it->key())
	       << " -> " << user_key << dendl;
      assert(it->key() < tail);
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::omap_get_values(
  const coll_t& cid,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return omap_get_values(c, oid, keys, out);
}

int BlueStore::omap_get_values(
  CollectionHandle &c_,        ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  got " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
      out->insert(make_pair(*p, val));
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int BlueStore::omap_check_keys(
  const coll_t& cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return omap_check_keys(c, oid, keys, out);
}

int BlueStore::omap_check_keys(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  have " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
      out->insert(*p);
    } else {
      dout(30) << __func__ << "  miss " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

ObjectMap::ObjectMapIterator BlueStore::get_omap_iterator(
  const coll_t& cid,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  CollectionHandle c = _get_collection(cid);
  if (!c) {
    dout(10) << __func__ << " " << cid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  return get_omap_iterator(c, oid);
}

ObjectMap::ObjectMapIterator BlueStore::get_omap_iterator(
  CollectionHandle &c_,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists) {
    return ObjectMap::ObjectMapIterator();
  }
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  o->flush();
  dout(10) << __func__ << " header = " << o->onode.omap_head <<dendl;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o, it));
}


// -----------------
// write helpers

int BlueStore::_open_super_meta()
{
  // nid
  {
    nid_max = 0;
    bufferlist bl;
    db->get(PREFIX_SUPER, "nid_max", &bl);
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(nid_max, p);
    } catch (buffer::error& e) {
    }
    dout(10) << __func__ << " old nid_max " << nid_max << dendl;
    nid_last = nid_max;
  }

  // freelist
  {
    bufferlist bl;
    db->get(PREFIX_SUPER, "freelist_type", &bl);
    if (bl.length()) {
      freelist_type = std::string(bl.c_str(), bl.length());
      dout(10) << __func__ << " freelist_type " << freelist_type << dendl;
    } else {
      freelist_type = "extent";
      dout(10) << __func__ << " freelist_type " << freelist_type
	       << " (legacy bluestore instance)" << dendl;
    }
  }

  // bluefs alloc
  {
    bluefs_extents.clear();
    bufferlist bl;
    db->get(PREFIX_SUPER, "bluefs_extents", &bl);
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(bluefs_extents, p);
    }
    catch (buffer::error& e) {
    }
    dout(10) << __func__ << " bluefs_extents " << bluefs_extents << dendl;
  }
  return 0;
}

void BlueStore::_assign_nid(TransContext *txc, OnodeRef o)
{
  if (o->onode.nid)
    return;
  std::lock_guard<std::mutex> l(nid_lock);
  o->onode.nid = ++nid_last;
  dout(20) << __func__ << " " << o->onode.nid << dendl;
  if (nid_last > nid_max) {
    nid_max += g_conf->bluestore_nid_prealloc;
    bufferlist bl;
    ::encode(nid_max, bl);
    txc->t->set(PREFIX_SUPER, "nid_max", bl);
    dout(10) << __func__ << " nid_max now " << nid_max << dendl;
  }
}

BlueStore::TransContext *BlueStore::_txc_create(OpSequencer *osr)
{
  TransContext *txc = new TransContext(osr);
  txc->t = db->get_transaction();
  osr->queue_new(txc);
  dout(20) << __func__ << " osr " << osr << " = " << txc
	   << " seq " << txc->seq << dendl;
  return txc;
}

void BlueStore::_txc_update_store_statfs(TransContext *txc)
{
  if (txc->statfs_delta.is_empty())
    return;

  bufferlist bl;
  txc->statfs_delta.encode(bl);

  txc->t->merge(PREFIX_STAT, "bluestore_statfs", bl);
  txc->statfs_delta.reset();
}

void BlueStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    dout(10) << __func__ << " txc " << txc
	     << " " << txc->get_state_name() << dendl;
    switch (txc->state) {
    case TransContext::STATE_PREPARE:
      txc->log_state_latency(logger, l_bluestore_state_prepare_lat);
      if (txc->ioc.has_aios()) {
	txc->state = TransContext::STATE_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_AIO_WAIT:
      txc->log_state_latency(logger, l_bluestore_state_aio_wait_lat);
      _txc_finish_io(txc);  // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      //assert(txc->osr->qlock.is_locked());  // see _txc_finish_io
      txc->log_state_latency(logger, l_bluestore_state_io_done_lat);
      txc->state = TransContext::STATE_KV_QUEUED;
      // FIXME: use a per-txc dirty blob list?
      for (auto& o : txc->onodes) {
	for (auto& p : o->blob_map.blob_map) {
	  p.bc.finish_write(txc->seq);
	}
      }
      if (!g_conf->bluestore_sync_transaction) {
	if (g_conf->bluestore_sync_submit_transaction) {
	  _txc_finalize_kv(txc, txc->t);
	  int r = db->submit_transaction(txc->t);
	  assert(r == 0);
	}
      } else {
	_txc_finalize_kv(txc, txc->t);
	int r = db->submit_transaction_sync(txc->t);
	assert(r == 0);
      }
      {
	std::lock_guard<std::mutex> l(kv_lock);
	kv_queue.push_back(txc);
	kv_cond.notify_one();
      }
      return;
    case TransContext::STATE_KV_QUEUED:
      txc->log_state_latency(logger, l_bluestore_state_kv_queued_lat);
      txc->state = TransContext::STATE_KV_DONE;
      _txc_finish_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
      txc->log_state_latency(logger, l_bluestore_state_kv_done_lat);
      if (txc->wal_txn) {
	txc->state = TransContext::STATE_WAL_QUEUED;
	if (sync_wal_apply) {
	  _wal_apply(txc);
	} else {
	  wal_wq.queue(txc);
	}
	return;
      }
      txc->state = TransContext::STATE_FINISHING;
      break;

    case TransContext::STATE_WAL_APPLYING:
      txc->log_state_latency(logger, l_bluestore_state_wal_applying_lat);
      if (txc->ioc.has_aios()) {
	txc->state = TransContext::STATE_WAL_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_WAL_AIO_WAIT:
      txc->log_state_latency(logger, l_bluestore_state_wal_aio_wait_lat);
      _wal_finish(txc);
      return;

    case TransContext::STATE_WAL_CLEANUP:
      txc->log_state_latency(logger, l_bluestore_state_wal_cleanup_lat);
      txc->state = TransContext::STATE_FINISHING;
      // ** fall-thru **

    case TransContext::TransContext::STATE_FINISHING:
      txc->log_state_latency(logger, l_bluestore_state_finishing_lat);
      _txc_finish(txc);
      return;

    default:
      derr << __func__ << " unexpected txc " << txc
	   << " state " << txc->get_state_name() << dendl;
      assert(0 == "unexpected txc state");
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
  std::lock_guard<std::mutex> l(osr->qlock);
  txc->state = TransContext::STATE_IO_DONE;

  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin()) {
    --p;
    if (p->state < TransContext::STATE_IO_DONE) {
      dout(20) << __func__ << " " << txc << " blocked by " << &*p << " "
	       << p->get_state_name() << dendl;
      return;
    }
    if (p->state > TransContext::STATE_IO_DONE) {
      ++p;
      break;
    }
  }
  do {
    _txc_state_proc(&*p++);
  } while (p != osr->q.end() &&
	   p->state == TransContext::STATE_IO_DONE);
}

void BlueStore::_txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t)
{
  dout(20) << __func__ << " txc " << txc
	   << " onodes " << txc->onodes
	   << " bnodes " << txc->bnodes
	   << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
    (*p)->blob_map.encode(bl);
    dout(20) << "  onode " << (*p)->oid << " is " << bl.length() << dendl;
    t->set(PREFIX_OBJ, (*p)->key, bl);

    std::lock_guard<std::mutex> l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }

  // finalize bnodes
  for (set<BnodeRef>::iterator p = txc->bnodes.begin();
       p != txc->bnodes.end();
       ++p) {
    if ((*p)->blob_map.empty()) {
      dout(20) << "  bnode " << std::hex << (*p)->hash << std::dec
	       << " blob_map is empty" << dendl;
      t->rmkey(PREFIX_OBJ, (*p)->key);
    } else {
      bufferlist bl;
      (*p)->blob_map.encode(bl);
      dout(20) << "  bnode " << std::hex << (*p)->hash << std::dec
	       << " blob_map is " << bl.length() << dendl;
      t->set(PREFIX_OBJ, (*p)->key, bl);
    }
  }
}

void BlueStore::_txc_finish_kv(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;

  // warning: we're calling onreadable_sync inside the sequencer lock
  if (txc->onreadable_sync) {
    txc->onreadable_sync->complete(0);
    txc->onreadable_sync = NULL;
  }
  if (txc->onreadable) {
    finisher.queue(txc->onreadable);
    txc->onreadable = NULL;
  }
  if (txc->oncommit) {
    finisher.queue(txc->oncommit);
    txc->oncommit = NULL;
  }
  while (!txc->oncommits.empty()) {
    finisher.queue(txc->oncommits.front());
    txc->oncommits.pop_front();
  }

  throttle_ops.put(txc->ops);
  throttle_bytes.put(txc->bytes);
}

void BlueStore::_txc_finish(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
  assert(txc->state == TransContext::STATE_FINISHING);

  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    std::lock_guard<std::mutex> l((*p)->flush_lock);
    dout(20) << __func__ << " onode " << *p << " had " << (*p)->flush_txns
	     << dendl;
    assert((*p)->flush_txns.count(txc));
    (*p)->flush_txns.erase(txc);
    if ((*p)->flush_txns.empty())
      (*p)->flush_cond.notify_all();
  }

  // clear out refs
  txc->onodes.clear();

  while (!txc->removed_collections.empty()) {
    _queue_reap_collection(txc->removed_collections.front());
    txc->removed_collections.pop_front();
  }

  throttle_wal_ops.put(txc->ops);
  throttle_wal_bytes.put(txc->bytes);

  OpSequencerRef osr = txc->osr;
  {
    std::lock_guard<std::mutex> l(osr->qlock);
    txc->state = TransContext::STATE_DONE;
  }

  _osr_reap_done(osr.get());
}

void BlueStore::_osr_reap_done(OpSequencer *osr)
{
  std::lock_guard<std::mutex> l(osr->qlock);
  dout(20) << __func__ << " osr " << osr << dendl;
  CollectionRef c;
  while (!osr->q.empty()) {
    TransContext *txc = &osr->q.front();
    dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	     << dendl;
    if (txc->state != TransContext::STATE_DONE) {
      break;
    }

    if (!c && txc->first_collection) {
      c = txc->first_collection;
    }

    osr->q.pop_front();
    txc->log_state_latency(logger, l_bluestore_state_done_lat);
    delete txc;
    osr->qcond.notify_all();
    if (osr->q.empty())
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
  }
  if (c) {
    c->cache->trim(
      g_conf->bluestore_onode_cache_size,
      g_conf->bluestore_buffer_cache_size);
  }
}

void BlueStore::_txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t)
{
  dout(20) << __func__ << " txc " << txc << std::hex
	   << " allocated 0x" << txc->allocated
	   << " released 0x" << txc->released
	   << std::dec << dendl;

  // We have to handle the case where we allocate *and* deallocate the
  // same region in this transaction.  The freelist doesn't like that.
  // (Actually, the only thing that cares is the BitmapFreelistManager
  // debug check. But that's important.)
  interval_set<uint64_t> overlap;
  interval_set<uint64_t> tmp_allocated, tmp_released;
  interval_set<uint64_t> *pallocated = &txc->allocated;
  interval_set<uint64_t> *preleased = &txc->released;
  if (!txc->allocated.empty() && !txc->released.empty()) {
    overlap.intersection_of(txc->allocated, txc->released);
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

  // update allocator with full released set
  if (!g_conf->bluestore_debug_no_reuse_blocks) {
    for (interval_set<uint64_t>::iterator p = txc->released.begin();
	 p != txc->released.end();
	 ++p) {
      alloc->release(p.get_start(), p.get_len());
    }
  }

  txc->allocated.clear();
  txc->released.clear();
  _txc_update_store_statfs(txc);
}

void BlueStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock<std::mutex> l(kv_lock);
  while (true) {
    assert(kv_committing.empty());
    assert(wal_cleaning.empty());
    if (kv_queue.empty() && wal_cleanup_queue.empty()) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_sync_cond.notify_all();
      kv_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      dout(20) << __func__ << " committing " << kv_queue.size()
	       << " cleaning " << wal_cleanup_queue.size() << dendl;
      kv_committing.swap(kv_queue);
      wal_cleaning.swap(wal_cleanup_queue);
      utime_t start = ceph_clock_now(NULL);
      l.unlock();

      dout(30) << __func__ << " committing txc " << kv_committing << dendl;
      dout(30) << __func__ << " wal_cleaning txc " << wal_cleaning << dendl;

      alloc->commit_start();

      // flush/barrier on block device
      bdev->flush();

      if (!g_conf->bluestore_sync_transaction &&
	  !g_conf->bluestore_sync_submit_transaction) {
	for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	     it != kv_committing.end();
	     ++it) {
	  _txc_finalize_kv((*it), (*it)->t);
	  int r = db->submit_transaction((*it)->t);
	  assert(r == 0);
	}
      }

      // one final transaction to force a sync
      KeyValueDB::Transaction t = db->get_transaction();

      vector<bluestore_pextent_t> bluefs_gift_extents;
      if (bluefs) {
	int r = _balance_bluefs_freespace(&bluefs_gift_extents, t);
	assert(r >= 0);
	if (r > 0) {
	  for (auto& p : bluefs_gift_extents) {
	    fm->allocate(p.offset, p.length, t);
	    bluefs_extents.insert(p.offset, p.length);
	  }
	  bufferlist bl;
	  ::encode(bluefs_extents, bl);
	  dout(10) << __func__ << " bluefs_extents now " << bluefs_extents
		   << dendl;
	  t->set(PREFIX_SUPER, "bluefs_extents", bl);
	}
      }

      // cleanup sync wal keys
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	    it != wal_cleaning.end();
	    ++it) {
	bluestore_wal_transaction_t& wt =*(*it)->wal_txn;
	// kv metadata updates
	_txc_finalize_kv(*it, t);
	// cleanup the data in overlays
	for (auto& p : wt.ops) {
	  for (auto q : p.removed_overlays) {
            string key;
            get_overlay_key(p.nid, q, &key);
	    t->rm_single_key(PREFIX_OVERLAY, key);
	  }
	}
	// cleanup the wal
	string key;
	get_wal_key(wt.seq, &key);
	t->rm_single_key(PREFIX_WAL, key);
      }
      int r = db->submit_transaction_sync(t);
      assert(r == 0);

      utime_t finish = ceph_clock_now(NULL);
      utime_t dur = finish - start;
      dout(20) << __func__ << " committed " << kv_committing.size()
	       << " cleaned " << wal_cleaning.size()
	       << " in " << dur << dendl;
      while (!kv_committing.empty()) {
	TransContext *txc = kv_committing.front();
	_txc_state_proc(txc);
	kv_committing.pop_front();
      }
      while (!wal_cleaning.empty()) {
	TransContext *txc = wal_cleaning.front();
	_txc_state_proc(txc);
	wal_cleaning.pop_front();
      }

      alloc->commit_finish();

      // this is as good a place as any ...
      _reap_collections();

      if (bluefs) {
	if (!bluefs_gift_extents.empty()) {
	  _commit_bluefs_freespace(bluefs_gift_extents);
	}
      }

      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
}

bluestore_wal_op_t *BlueStore::_get_wal_op(TransContext *txc, OnodeRef o)
{
  if (!txc->wal_txn) {
    txc->wal_txn = new bluestore_wal_transaction_t;
  }
  txc->wal_txn->ops.push_back(bluestore_wal_op_t());
  txc->wal_op_onodes.push_back(o);
  return &txc->wal_txn->ops.back();
}

int BlueStore::_wal_apply(TransContext *txc)
{
  bluestore_wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << txc << " seq " << wt.seq << dendl;
  txc->log_state_latency(logger, l_bluestore_state_wal_queued_lat);
  txc->state = TransContext::STATE_WAL_APPLYING;

  if (g_conf->bluestore_inject_wal_apply_delay) {
    dout(20) << __func__ << " bluestore_inject_wal_apply_delay "
	     << g_conf->bluestore_inject_wal_apply_delay
	     << dendl;
    utime_t t;
    t.set_from_double(g_conf->bluestore_inject_wal_apply_delay);
    t.sleep();
    dout(20) << __func__ << " finished sleep" << dendl;
  }

  assert(txc->ioc.pending_aios.empty());
  vector<OnodeRef>::iterator q = txc->wal_op_onodes.begin();
  for (list<bluestore_wal_op_t>::iterator p = wt.ops.begin();
       p != wt.ops.end();
       ++p, ++q) {
    int r = _do_wal_op(txc, *p);
    assert(r == 0);
  }

  _txc_state_proc(txc);
  return 0;
}

int BlueStore::_wal_finish(TransContext *txc)
{
  bluestore_wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << " seq " << wt.seq << txc << dendl;

  // move released back to txc
  txc->wal_txn->released.swap(txc->released);
  assert(txc->wal_txn->released.empty());

  std::lock_guard<std::mutex> l2(txc->osr->qlock);
  std::lock_guard<std::mutex> l(kv_lock);
  txc->state = TransContext::STATE_WAL_CLEANUP;
  txc->osr->qcond.notify_all();
  wal_cleanup_queue.push_back(txc);
  kv_cond.notify_one();
  return 0;
}

int BlueStore::_do_wal_op(TransContext *txc, bluestore_wal_op_t& wo)
{
  // read all the overlay data first for apply
  _do_read_all_overlays(wo);

  switch (wo.op) {
  case bluestore_wal_op_t::OP_WRITE:
    {
      dout(20) << __func__ << " write " << wo.extents << dendl;
      logger->inc(l_bluestore_wal_write_ops);
      logger->inc(l_bluestore_wal_write_bytes, wo.data.length());
      bufferlist::iterator p = wo.data.begin();
      for (auto& e : wo.extents) {
	bufferlist bl;
	p.copy(e.length, bl);
	int r = bdev->aio_write(e.offset, bl, &txc->ioc, false);
	assert(r == 0);
      }
    }
    break;

  default:
    assert(0 == "unrecognized wal op");
  }

  return 0;
}

int BlueStore::_wal_replay()
{
  dout(10) << __func__ << " start" << dendl;
  OpSequencerRef osr = new OpSequencer;
  int count = 0;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_WAL);
  for (it->lower_bound(string()); it->valid(); it->next(), ++count) {
    dout(20) << __func__ << " replay " << pretty_binary_string(it->key())
	     << dendl;
    bluestore_wal_transaction_t *wal_txn = new bluestore_wal_transaction_t;
    bufferlist bl = it->value();
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(*wal_txn, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode wal txn "
	   << pretty_binary_string(it->key()) << dendl;
      delete wal_txn;
      return -EIO;
    }
    TransContext *txc = _txc_create(osr.get());
    txc->wal_txn = wal_txn;
    txc->state = TransContext::STATE_KV_DONE;
    _txc_state_proc(txc);
  }
  dout(20) << __func__ << " flushing osr" << dendl;
  osr->flush();
  dout(10) << __func__ << " completed " << count << " events" << dendl;
  return 0;
}

// ---------------------------
// transactions

int BlueStore::queue_transactions(
    Sequencer *posr,
    vector<Transaction>& tls,
    TrackedOpRef op,
    ThreadPool::TPHandle *handle)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);

  // set up the sequencer
  OpSequencer *osr;
  assert(posr);
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(10) << __func__ << " existing " << osr << " " << *osr << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(10) << __func__ << " new " << osr << " " << *osr << dendl;
  }

  // prepare
  TransContext *txc = _txc_create(osr);
  txc->onreadable = onreadable;
  txc->onreadable_sync = onreadable_sync;
  txc->oncommit = ondisk;

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    (*p).set_osr(osr);
    txc->ops += (*p).get_num_ops();
    txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }

  // delayed csum calculation?
  for (auto& d : txc->deferred_csum) {
    Blob *b = d.onode->get_blob(d.blob);
    dout(20) << __func__ << "  deferred csum calc blob " << d.blob
	     << " b_off 0x" << std::hex << d.b_off << std::dec
	     << " on " << d.onode->oid << dendl;
    b->blob.calc_csum(d.b_off, d.data);
  }

  _txc_write_nodes(txc, txc->t);

  // journal wal items
  if (txc->wal_txn) {
    // move releases to after wal
    txc->wal_txn->released.swap(txc->released);
    assert(txc->released.empty());

    txc->wal_txn->seq = wal_seq.inc();
    bufferlist bl;
    ::encode(*txc->wal_txn, bl);
    string key;
    get_wal_key(txc->wal_txn->seq, &key);
    txc->t->set(PREFIX_WAL, key, bl);
  }

  throttle_ops.get(txc->ops);
  throttle_bytes.get(txc->bytes);
  throttle_wal_ops.get(txc->ops);
  throttle_wal_bytes.get(txc->bytes);

  // execute (start)
  _txc_state_proc(txc);
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

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t->dump(&f);
  f.close_section();
  f.flush(*_dout);
  *_dout << dendl;

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);

    // note first collection we reference
    if (!j && !txc->first_collection)
      txc->first_collection = cvec[j];
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
    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	assert(!c);
	coll_t cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, op->split_bits, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
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

    case Transaction::OP_COLL_HINT:
      {
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
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
      assert(0 == "not implemented");
      break;
    }
    if (r < 0) {
      dout(0) << " error " << cpp_strerror(r)
	      << " not handled on operation " << op->op
	      << " (op " << pos << ", counting from 0)" << dendl;
      dout(0) << " transaction dump:\n";
      JSONFormatter f(true);
      f.open_object_section("transaction");
      t->dump(&f);
      f.close_section();
      f.flush(*_dout);
      *_dout << dendl;
      assert(0 == "unexpected error");
    }

    // object operations
    RWLock::WLocker l(c->lock);
    OnodeRef &o = ovec[op->oid];
    if (!o) {
      // these operations implicity create the object
      bool create = false;
      if (op->op == Transaction::OP_TOUCH ||
	  op->op == Transaction::OP_WRITE ||
	  op->op == Transaction::OP_ZERO) {
	create = true;
      }
      ghobject_t oid = i.get_oid(op->oid);
      o = c->get_onode(oid, create);
      if (!create) {
	if (!o || !o->exists) {
	  dout(10) << __func__ << " op " << op->op << " got ENOENT on "
		   << oid << dendl;
	  r = -ENOENT;
	  goto endop;
	}
      }
    }

    switch (op->op) {
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
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(txc, c, o, to_set);
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
        const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  no = c->get_onode(noid, true);
	}
	r = _clone(txc, c, o, no);
      }
      break;

    case Transaction::OP_CLONERANGE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  no = c->get_onode(noid, true);
	}
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_COLL_ADD:
      assert(0 == "not implemented");
      break;

    case Transaction::OP_COLL_REMOVE:
      assert(0 == "not implemented");
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
    case Transaction::OP_TRY_RENAME:
    {
	assert(op->cid == op->dest_cid);
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  no = c->get_onode(noid, false);
	}
	r = _rename(txc, c, o, no, noid);
	if (r == -ENOENT && op->op == Transaction::OP_TRY_RENAME)
	  r = 0;
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
			    op->alloc_hint_flags);
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

  endop:
    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
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
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }
  }
}



// -----------------
// write operations

int BlueStore::_touch(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef &o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  o->exists = true;
  _assign_nid(txc, o);
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_do_overlay_trim(TransContext *txc,
			       OnodeRef o,
			       uint64_t offset,
			       uint64_t length)
{
  dout(10) << __func__ << " " << o->oid << " 0x"
	   << std::hex << offset << "~" << length << std::dec << dendl;
  int changed = 0;

  map<uint64_t,bluestore_overlay_t>::iterator p =
    o->onode.overlay_map.lower_bound(offset);
  if (p != o->onode.overlay_map.begin()) {
    --p;
  }
  while (p != o->onode.overlay_map.end()) {
    if (p->first >= offset + length) {
      dout(20) << __func__ << " stop at " << p->first << " " << p->second
	       << dendl;
      break;
    }
    if (p->first + p->second.length <= offset) {
      dout(20) << __func__ << " skip " << p->first << " " << p->second
	       << dendl;
      ++p;
      continue;
    }
    if (p->first >= offset &&
	p->first + p->second.length <= offset + length) {
      dout(20) << __func__ << " rm " << p->first << " " << p->second
	       << dendl;
      if (o->onode.put_overlay_ref(p->second.key)) {
	string key;
	get_overlay_key(o->onode.nid, p->second.key, &key);
	txc->t->rm_single_key(PREFIX_OVERLAY, key);
      }
      o->onode.overlay_map.erase(p++);
      ++changed;
      continue;
    }
    if (p->first >= offset) {
      dout(20) << __func__ << " trim_front " << p->first << " " << p->second
	       << dendl;
      bluestore_overlay_t& ov = o->onode.overlay_map[offset + length] = p->second;
      uint64_t by = offset + length - p->first;
      ov.value_offset += by;
      ov.length -= by;
      o->onode.overlay_map.erase(p++);
      ++changed;
      continue;
    }
    if (p->first < offset &&
	p->first + p->second.length <= offset + length) {
      dout(20) << __func__ << " trim_tail " << p->first << " " << p->second
	       << dendl;
      p->second.length = offset - p->first;
      ++p;
      ++changed;
      continue;
    }
    dout(20) << __func__ << " split " << p->first << " " << p->second
	     << dendl;
    assert(p->first < offset);
    assert(p->first + p->second.length > offset + length);
    bluestore_overlay_t& nov = o->onode.overlay_map[offset + length] = p->second;
    p->second.length = offset - p->first;
    uint64_t by = offset + length - p->first;
    nov.value_offset += by;
    nov.length -= by;
    o->onode.get_overlay_ref(p->second.key);
    ++p;
    ++changed;
  }
  return changed;
}

int BlueStore::_do_overlay_write(TransContext *txc,
				OnodeRef o,
				uint64_t offset,
				uint64_t length,
				const bufferlist& bl)
{
  _do_overlay_trim(txc, o, offset, length);

  dout(10) << __func__ << " " << o->oid << " 0x"
	   << std::hex << offset << "~" << length << std::dec << dendl;
  bluestore_overlay_t& ov = o->onode.overlay_map[offset] =
    bluestore_overlay_t(++o->onode.last_overlay_key, 0, length);
  dout(20) << __func__ << " added 0x" << std::hex << offset << std::dec
	   << " " << ov << dendl;
  string key;
  get_overlay_key(o->onode.nid, o->onode.last_overlay_key, &key);
  txc->t->set(PREFIX_OVERLAY, key, bl);
  return 0;
}

int BlueStore::_do_write_overlays(TransContext *txc,
				  CollectionRef& c,
				 OnodeRef o,
				 uint64_t orig_offset,
				 uint64_t orig_length)
{
  if (o->onode.overlay_map.empty())
    return 0;

  assert(0 == "this is all broken");

  txc->write_onode(o);
  return 0;
}

void BlueStore::_do_read_all_overlays(bluestore_wal_op_t& wo)
{
  for (vector<bluestore_overlay_t>::iterator q = wo.overlays.begin();
       q != wo.overlays.end(); ++q) {
    string key;
    get_overlay_key(wo.nid, q->key, &key);
    bufferlist bl, bl_data;
    int r = db->get(PREFIX_OVERLAY, key, &bl);
    assert(r >= 0); 
    bl_data.substr_of(bl, q->value_offset, q->length);
    wo.data.claim_append(bl_data);
  }
  return;
}

void BlueStore::_dump_onode(OnodeRef o, int log_level)
{
  if (!g_conf->subsys.should_gather(ceph_subsys_bluestore, log_level))
    return;
  dout(log_level) << __func__ << " " << o << " " << o->oid
		  << " nid " << o->onode.nid
		  << " size 0x" << std::hex << o->onode.size
		  << " (" << std::dec << o->onode.size << ")"
		  << " expected_object_size " << o->onode.expected_object_size
		  << " expected_write_size " << o->onode.expected_write_size
		  << dendl;
  for (map<string,bufferptr>::iterator p = o->onode.attrs.begin();
       p != o->onode.attrs.end();
       ++p) {
    dout(log_level) << __func__ << "  attr " << p->first
		    << " len " << p->second.length() << dendl;
  }
  uint64_t pos = 0;
  for (auto& p : o->onode.extent_map) {
    dout(log_level) << __func__ << "  lextent 0x" << std::hex << p.first
		    << std::dec << ": " << p.second
		    << dendl;
    assert(p.first >= pos);
    pos = p.first + p.second.length;
  }
  pos = 0;
  for (auto& v : o->onode.overlay_map) {
    dout(log_level) << __func__ << "  overlay 0x" << std::hex << v.first
		    << std::dec << ": " << v.second
		    << dendl;
    assert(v.first >= pos);
    pos = v.first + v.second.length;
  }
  if (!o->onode.overlay_refs.empty()) {
    dout(log_level) << __func__ << "  overlay_refs " << o->onode.overlay_refs
		    << dendl;
  }
  _dump_blob_map(o->blob_map, log_level);
  if (o->bnode) {
    _dump_bnode(o->bnode, log_level);
  }
}

void BlueStore::_dump_bnode(BnodeRef b, int log_level)
{
  if (!g_conf->subsys.should_gather(ceph_subsys_bluestore, log_level))
    return;
  dout(log_level) << __func__ << " " << b
		  << " " << std::hex << b->hash << std::dec << dendl;
  _dump_blob_map(b->blob_map, log_level);
}

void BlueStore::_dump_blob_map(BlobMap &bm, int log_level)
{
  for (auto& b : bm.blob_map) {
    dout(log_level) << __func__ << "  " << b << dendl;
    if (b.blob.has_csum_data()) {
      vector<uint64_t> v;
      unsigned n = b.blob.get_csum_count();
      for (unsigned i = 0; i < n; ++i)
	v.push_back(b.blob.get_csum_item(i));
      dout(log_level) << __func__ << "       csum: " << std::hex << v << std::dec
		      << dendl;
    }
    if (!b.bc.empty()) {
      for (auto& i : b.bc.buffer_map) {
	dout(log_level) << __func__ << "       0x" << std::hex << i.first
			<< "~" << i.second->length << std::dec
			<< " " << *i.second << dendl;
      }
    }
  }
}

void BlueStore::_pad_zeros(
  bufferlist *bl, uint64_t *offset, uint64_t *length,
  uint64_t chunk_size)
{
  dout(30) << __func__ << " 0x" << std::hex << *offset << "~" << *length
	   << " chunk_size 0x" << chunk_size << std::dec << dendl;
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  // front
  size_t front_pad = *offset % chunk_size;
  size_t back_pad = 0;
  size_t pad_count = 0;
  if (front_pad) {
    size_t front_copy = MIN(chunk_size - front_pad, *length);
    bufferptr z = buffer::create_page_aligned(chunk_size);
    memset(z.c_str(), 0, front_pad);
    pad_count += front_pad;
    memcpy(z.c_str() + front_pad, bl->get_contiguous(0, front_copy), front_copy);
    if (front_copy + front_pad < chunk_size) {
      back_pad = chunk_size - (*length + front_pad);
      memset(z.c_str() + front_pad + *length, 0, back_pad);
      pad_count += back_pad;
    }
    bufferlist old, t;
    old.swap(*bl);
    t.substr_of(old, front_copy, *length - front_copy);
    bl->append(z);
    bl->claim_append(t);
    *offset -= front_pad;
    *length += front_pad + back_pad;
  }

  // back
  uint64_t end = *offset + *length;
  unsigned back_copy = end % chunk_size;
  if (back_copy) {
    assert(back_pad == 0);
    back_pad = chunk_size - back_copy;
    assert(back_copy <= *length);
    bufferptr tail(chunk_size);
    memcpy(tail.c_str(), bl->get_contiguous(*length - back_copy, back_copy),
	   back_copy);
    memset(tail.c_str() + back_copy, 0, back_pad);
    bufferlist old;
    old.swap(*bl);
    bl->substr_of(old, 0, *length - back_copy);
    bl->append(tail);
    *length += back_pad;
    pad_count += back_pad;
  }
  dout(20) << __func__ << " pad 0x" << std::hex << front_pad << " + 0x"
	   << back_pad << " on front/back, now 0x" << *offset << "~"
	   << *length << std::dec << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  if (pad_count)
    logger->inc(l_bluestore_write_pad_bytes, pad_count);
}

bool BlueStore::_can_overlay_write(OnodeRef o, uint64_t length)
{
  return
    (int)o->onode.overlay_map.size() < g_conf->bluestore_overlay_max &&
    (int)length <= g_conf->bluestore_overlay_max_length;
}

void BlueStore::_do_write_small(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  assert(length < min_alloc_size);

  bufferlist bl;
  blp.copy(length, bl);

  // look for an existing mutable blob we can use
  Blob *b = 0;
  map<uint64_t,bluestore_lextent_t>::iterator ep = o->onode.seek_lextent(offset);
  if (ep != o->onode.extent_map.begin()) {
    --ep;
    b = c->get_blob(o, ep->second.blob);
    if (ep->first + b->blob.get_max_length() <= offset) {
      ++ep;
    }
  }
  while (ep != o->onode.extent_map.end()) {
    if (ep->first >= ep->second.offset + offset + length) {
      break;
    }
    int64_t blob = ep->second.blob;
    b = c->get_blob(o, ep->second.blob);
    if (!b->blob.is_mutable()) {
      dout(20) << __func__ << " ignoring immutable " << blob << ": " << *b
	       << dendl;
      ++ep;
      continue;
    }
    if (ep->first % min_alloc_size != ep->second.offset % min_alloc_size) {
      dout(20) << __func__ << " ignoring offset-skewed " << blob << ": " << *b
	       << dendl;
      ++ep;
      continue;
    }
    uint64_t bstart = ep->first - ep->second.offset;
    dout(20) << __func__ << " considering " << blob << ": " << *b
	     << " bstart 0x" << std::hex << bstart << std::dec << dendl;

    // can we pad our head/tail out with zeros?
    uint64_t chunk_size = MAX(block_size, b->blob.get_csum_chunk_size());
    uint64_t head_pad = offset % chunk_size;
    if (head_pad && o->onode.has_any_lextents(offset - head_pad, chunk_size)) {
      head_pad = 0;
    }
    uint64_t tail_pad =
      ROUND_UP_TO(offset + length, chunk_size) - (offset + length);
    if (o->onode.has_any_lextents(offset + length, tail_pad)) {
      tail_pad = 0;
    }
    bufferlist padded = bl;
    if (head_pad) {
      bufferlist z;
      z.append_zero(head_pad);
      z.claim_append(padded);
      padded.claim(z);
      logger->inc(l_bluestore_write_pad_bytes, head_pad);
    }
    if (tail_pad) {
      padded.append_zero(tail_pad);
      logger->inc(l_bluestore_write_pad_bytes, tail_pad);
    }
    if (head_pad || tail_pad) {
      dout(20) << __func__ << "  can pad head 0x" << std::hex << head_pad
	       << " tail 0x" << tail_pad << std::dec << dendl;
    }

    // direct write into unused blocks of an existing mutable blob?
    uint64_t b_off = offset - head_pad - bstart;
    uint64_t b_len = length + head_pad + tail_pad;
    if (b->blob.get_ondisk_length() >= b_off + b_len &&
	b->blob.is_unused(b_off, b_len) &&
	b->blob.is_allocated(b_off, b_len) &&
	(b_off % chunk_size == 0 && b_len % chunk_size == 0)) {
      dout(20) << __func__ << "  write to unused 0x" << std::hex
	       << b_off << "~" << b_len
	       << " pad 0x" << head_pad << " + 0x" << tail_pad
	       << std::dec << " of mutable " << blob << ": " << b << dendl;
      assert(b->blob.is_unreferenced(b_off, b_len));
      b->bc.write(txc->seq, b_off, padded,
		  wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);
      b->blob.map_bl(
	b_off, padded,
	[&](uint64_t offset, uint64_t length, bufferlist& t) {
	  bdev->aio_write(offset, t,
			  &txc->ioc, wctx->buffered);
	});
      b->blob.calc_csum(b_off, padded);
      o->onode.punch_hole(offset, length, &wctx->lex_old);
      dout(20) << __func__ << "  lexold 0x" << std::hex << offset << std::dec
	       << ": " << ep->second << dendl;
      bluestore_lextent_t& lex = o->onode.extent_map[offset] =
	bluestore_lextent_t(blob, b_off + head_pad, length);
      b->blob.ref_map.get(lex.offset, lex.length);
      b->blob.mark_used(lex.offset, lex.length);
      txc->statfs_delta.stored() += lex.length;
      dout(20) << __func__ << "  lex 0x" << std::hex << offset << std::dec
	       << ": " << lex << dendl;
      dout(20) << __func__ << "  old " << blob << ": " << *b << dendl;
      return;
    }

    // read some data to fill out the chunk?
    uint64_t head_read = b_off % chunk_size;
    uint64_t tail_read =
      ROUND_UP_TO(b_off + b_len, chunk_size) - (b_off + b_len);
    if ((head_read || tail_read) &&
	(b->blob.get_ondisk_length() >= b_off + b_len + tail_read)) {
      dout(20) << __func__ << "  reading head 0x" << std::hex << head_read
	       << " and tail 0x" << tail_read << std::dec << dendl;
      if (head_read) {
	bufferlist head_bl;
	int r = _do_read(c.get(), o, offset - head_pad - head_read, head_read,
			 head_bl, 0);
	assert(r >= 0 && r <= (int)head_read);
	size_t zlen = head_read - r;
	if (zlen) {
	  head_bl.append_zero(zlen);
	  logger->inc(l_bluestore_write_pad_bytes, zlen);
	}
	b_off -= head_read;
	b_len += head_read;
	head_bl.claim_append(padded);
	padded.swap(head_bl);
	logger->inc(l_bluestore_write_penalty_read_ops);
      }
      if (tail_read) {
	bufferlist tail_bl;
	int r = _do_read(c.get(), o, offset + length + tail_pad, tail_read,
			 tail_bl, 0);
	assert(r >= 0 && r <= (int)tail_read);
	b_len += tail_read;
	padded.claim_append(tail_bl);
	size_t zlen = tail_read - r;
	if (zlen) {
	  padded.append_zero(zlen);
	  logger->inc(l_bluestore_write_pad_bytes, zlen);
	}
	logger->inc(l_bluestore_write_penalty_read_ops);
      }
    }

    // chunk-aligned wal overwrite?
    if (b->blob.get_ondisk_length() >= b_off + b_len &&
	b_off % chunk_size == 0 &&
	b_len % chunk_size == 0 &&
	b->blob.is_allocated(b_off, b_len)) {
      bluestore_wal_op_t *op = _get_wal_op(txc, o);
      op->op = bluestore_wal_op_t::OP_WRITE;
      b->bc.write(txc->seq, b_off, padded,
		  wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);
      b->blob.map(
	b_off, b_len,
	[&](uint64_t offset, uint64_t length) {
	  op->extents.emplace_back(bluestore_pextent_t(offset, length));
	});
      if (b->blob.csum_type) {
	txc->add_deferred_csum(o, blob, b_off, padded);
      }
      op->data.claim(padded);
      dout(20) << __func__ << "  wal write 0x" << std::hex << b_off << "~"
	       << b_len << std::dec << " of mutable " << blob << ": " << *b
	       << " at " << op->extents << dendl;
      o->onode.punch_hole(offset, length, &wctx->lex_old);
      bluestore_lextent_t& lex = o->onode.extent_map[offset] =
	bluestore_lextent_t(blob, offset - bstart, length);
      b->blob.ref_map.get(lex.offset, lex.length);
      b->blob.mark_used(lex.offset, lex.length);
      txc->statfs_delta.stored() += lex.length;
      dout(20) << __func__ << "  lex 0x" << std::hex << offset
	       << std::dec << ": " << lex << dendl;
      dout(20) << __func__ << "  old " << blob << ": " << *b << dendl;
      return;
    }

    ++ep;
  }

  // new blob.
  b = o->blob_map.new_blob(c->cache);
  b->blob.length = min_alloc_size;
  uint64_t b_off = offset % min_alloc_size;
  uint64_t b_len = length;
  b->bc.write(txc->seq, b_off, bl, wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);
  _pad_zeros(&bl, &b_off, &b_len, block_size);
  if (b_off)
    b->blob.add_unused(0, b_off);
  if (b_off + b_len < b->blob.length)
    b->blob.add_unused(b_off + b_len, b->blob.length - (b_off + b_len));
  o->onode.punch_hole(offset, length, &wctx->lex_old);
  bluestore_lextent_t& lex = o->onode.extent_map[offset] =
    bluestore_lextent_t(b->id, offset % min_alloc_size, length);
  b->blob.ref_map.get(lex.offset, lex.length);
  txc->statfs_delta.stored() += lex.length;
  dout(20) << __func__ << "  lex 0x" << std::hex << offset << std::dec
	   << ": " << lex << dendl;
  dout(20) << __func__ << "  new " << b->id << ": " << *b << dendl;
  wctx->write(b, b_off, bl);
  return;
}

void BlueStore::_do_write_big(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx)
{
  uint64_t max_blob_len = length;
  if (wctx->compress) {
    max_blob_len = MIN(length, wctx->comp_blob_size);
  }
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " max_blob_len 0x" << max_blob_len
	   << " compress " << (int)wctx->compress
	   << std::dec << dendl;
  while (length > 0) {
    Blob *b = o->blob_map.new_blob(c->cache);
    auto l = b->blob.length = MIN(max_blob_len, length);
    bufferlist t;
    blp.copy(l, t);
    b->bc.write(txc->seq, 0, t, wctx->buffered ? 0 : Buffer::FLAG_NOCACHE);
    wctx->write(b, 0, t);
    o->onode.punch_hole(offset, l, &wctx->lex_old);
    o->onode.extent_map[offset] = bluestore_lextent_t(b->id, 0, l);
    b->blob.ref_map.get(0, l);
    txc->statfs_delta.stored() += l;
    dout(20) << __func__ << "  lex 0x" << std::hex << offset << std::dec << ": "
	     << o->onode.extent_map[offset] << dendl;
    dout(20) << __func__ << "  blob " << *b << dendl;
    offset += l;
    length -= l;
  }
}

int BlueStore::_do_alloc_write(
  TransContext *txc,
  WriteContext *wctx)
{
  dout(20) << __func__ << " txc " << txc
	   << " " << wctx->writes.size() << " blobs"
	   << dendl;

  uint64_t need = 0;
  for (auto &wi : wctx->writes) {
    need += wi.b->blob.length;
  }
  int r = alloc->reserve(need);
  if (r < 0) {
    derr << __func__ << " failed to reserve 0x" << std::hex << need << std::dec
	 << dendl;
    return r;
  }

  uint64_t hint = 0;
  for (auto& wi : wctx->writes) {
    Blob *b = wi.b;
    uint64_t b_off = wi.b_off;
    bufferlist *l = &wi.bl;
    uint64_t final_length = b->blob.length;
    uint64_t csum_length = b->blob.length;
    unsigned csum_order;
    bufferlist compressed_bl;
    CompressorRef c;
    bool compressed = false;
    if (wctx->compress &&
	b->blob.length > min_alloc_size &&
	(c = compressor) != nullptr) {
      // compress
      assert(b_off == 0);
      assert(b->blob.length == l->length());
      bluestore_compression_header_t chdr;
      chdr.type = c->get_type();
      // FIXME: memory alignment here is bad
      bufferlist t;
      c->compress(*l, t);
      chdr.length = t.length();
      ::encode(chdr, compressed_bl);
      compressed_bl.claim_append(t);
      uint64_t rawlen = compressed_bl.length();
      uint64_t newlen = ROUND_UP_TO(rawlen, min_alloc_size);
      if (newlen < final_length) {
	// pad out to min_alloc_size
	compressed_bl.append_zero(newlen - rawlen);
	logger->inc(l_bluestore_write_pad_bytes, newlen - rawlen);
	dout(20) << __func__ << hex << "  compressed 0x" << b->blob.length
		 << " -> 0x" << rawlen << " => 0x" << newlen
		 << " with " << chdr.type
		 << dec << dendl;
	txc->statfs_delta.compressed() += rawlen;
	txc->statfs_delta.compressed_original() += l->length();
	txc->statfs_delta.compressed_allocated() += newlen;
	l = &compressed_bl;
	final_length = newlen;
	csum_length = newlen;
	csum_order = ctz(newlen);
	b->blob.set_compressed(rawlen);
	compressed = true;
      } else {
	dout(20) << __func__ << hex << "  compressed 0x" << l->length()
		 << " -> 0x" << rawlen << " with " << chdr.type
		 << ", leaving uncompressed"
		 << dec << dendl;
      }
    }
    if (!compressed) {
      b->blob.set_flag(bluestore_blob_t::FLAG_MUTABLE);
      if (l->length() != b->blob.length) {
	// hrm, maybe we could do better here, but let's not bother.
	dout(20) << __func__ << " forcing csum_order to block_size_order "
		 << block_size_order << dendl;
	csum_order = block_size_order;
      } else {
	assert(b_off == 0);
	csum_order = std::min(wctx->csum_order, ctz(l->length()));
      }
    }
    while (final_length > 0) {
      bluestore_pextent_t e;
      uint32_t l;
      uint64_t want = max_alloc_size ? MIN(final_length, max_alloc_size) : final_length;
      int r = alloc->allocate(want, min_alloc_size, hint,
			      &e.offset, &l);
      assert(r == 0);
      need -= l;
      e.length = l;
      txc->allocated.insert(e.offset, e.length);
      txc->statfs_delta.allocated() += e.length;
      b->blob.extents.push_back(e);
      final_length -= e.length;
      hint = e.end();
    }
    dout(20) << __func__ << " blob " << *b
	     << " csum_order " << csum_order
	     << " csum_length 0x" << std::hex << csum_length << std::dec
	     << dendl;

    // checksum
    if (csum_type) {
      b->blob.init_csum(csum_type, csum_order, csum_length);
      b->blob.calc_csum(b_off, *l);
    }

    // queue io
    b->blob.map_bl(
      b_off, *l,
      [&](uint64_t offset, uint64_t length, bufferlist& t) {
	bdev->aio_write(offset, t, &txc->ioc, false);
      });
  }
  if (need > 0) {
    alloc->unreserve(need);
  }
  return 0;
}

void BlueStore::_wctx_finish(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o,
  WriteContext *wctx)
{
  dout(10) << __func__ << " lex_old " << wctx->lex_old << dendl;
  for (auto &l : wctx->lex_old) {
    Blob *b = c->get_blob(o, l.blob);
    vector<bluestore_pextent_t> r;
    bool compressed = b->blob.is_compressed();
    b->blob.put_ref(l.offset, l.length, min_alloc_size, &r);
    txc->statfs_delta.stored() -= l.length;
    if (compressed) {
      txc->statfs_delta.compressed_original() -= l.length;
    }
    for (auto e : r) {
      dout(20) << __func__ << " release " << e << dendl;
      txc->released.insert(e.offset, e.length);
      txc->statfs_delta.allocated() -= e.length;
      if (compressed) {
        txc->statfs_delta.compressed_allocated() -= e.length;
      }
    }
    if (b->blob.ref_map.empty()) {
      dout(20) << __func__ << " rm blob " << *b << dendl;
      if (compressed) {
        txc->statfs_delta.compressed() -= b->blob.get_payload_length();
      }
      if (l.blob >= 0) {
	o->blob_map.erase(b);
      } else {
	o->bnode->blob_map.erase(b);
      }
    } else {
      dout(20) << __func__ << " keep blob " << *b << dendl;
    }
    if (l.blob < 0) {
      txc->write_bnode(o->bnode);
    }
  }

  o->onode.compress_extent_map();
}

int BlueStore::_do_write(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o,
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
	   << " bytes in " << o->onode.extent_map.size() << " lextents"
	   << dendl;
  _dump_onode(o);

  if (length == 0) {
    return 0;
  }

  uint64_t end = offset + length;

  WriteContext wctx;
  wctx.fadvise_flags = fadvise_flags;
  if (wctx.fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered write" << dendl;
    wctx.buffered = true;
  }
  wctx.csum_order = MAX(block_size_order, o->onode.get_preferred_csum_order());

  // compression parameters
  unsigned alloc_hints = o->onode.alloc_hint_flags;
  wctx.compress =
    (comp_mode == COMP_FORCE) ||
    (comp_mode == COMP_AGGRESSIVE &&
     (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE) == 0) ||
    (comp_mode == COMP_PASSIVE &&
     (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE));

  if ((alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ) == 0 &&
      (alloc_hints & (CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE|
			CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY)) &&
      (alloc_hints & CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE) == 0) {
    dout(20) << __func__ << " will prefer large blob and csum sizes" << dendl;
    wctx.comp_blob_size = comp_max_blob_size;
    wctx.csum_order = min_alloc_size_order;
  } else {
    wctx.comp_blob_size = comp_min_blob_size;
  }
  dout(20) << __func__ << " prefer csum_order " << wctx.csum_order
	   << " comp_blob_size 0x" << std::hex << wctx.comp_blob_size
	   << std::dec << dendl;

  bufferlist::iterator p = bl.begin();
  if (offset / min_alloc_size == (end - 1) / min_alloc_size &&
      (length != min_alloc_size)) {
    // we fall within the same block
    _do_write_small(txc, c, o, offset, length, p, &wctx);
  } else {
    uint64_t head_offset = 0, head_length = 0;
    uint64_t middle_offset = 0, middle_length = 0;
    uint64_t tail_offset = 0, tail_length = 0;
    if (offset % min_alloc_size) {
      head_offset = offset;
      head_length = min_alloc_size - (offset % min_alloc_size);
      assert(head_length < length);
      _do_write_small(txc, c, o, head_offset, head_length, p, &wctx);
      middle_offset = offset + head_length;
      middle_length = length - head_length;
    } else {
      middle_offset = offset;
      middle_length = length;
    }
    if (end % min_alloc_size) {
      tail_length = end % min_alloc_size;
      tail_offset = end - tail_length;
      middle_length -= tail_length;
    }
    if (middle_length) {
      _do_write_big(txc, c, o, middle_offset, middle_length, p, &wctx);
    }
    if (tail_length) {
      _do_write_small(txc, c, o, tail_offset, tail_length, p, &wctx);
    }
  }

  r = _do_alloc_write(txc, &wctx);
  if (r < 0) {
    derr << __func__ << " _do_alloc_write failed with " << cpp_strerror(r)
	 << dendl;
    goto out;
  }

  _wctx_finish(txc, c, o, &wctx);

  if (end > o->onode.size) {
    dout(20) << __func__ << " extending size to 0x" << std::hex << end
	     << std::dec << dendl;
    o->onode.size = end;
  }
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
  o->exists = true;
  _assign_nid(txc, o);
  int r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

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
  int r = _do_zero(txc, c, o, offset, length);
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
  o->exists = true;

  _dump_onode(o);

  // ensure any wal IO has completed before we truncate off any extents
  // they may touch.
  o->flush();

  WriteContext wctx;
  o->onode.punch_hole(offset, length, &wctx.lex_old);
  _wctx_finish(txc, c, o, &wctx);

  if (offset + length > o->onode.size) {
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

int BlueStore::_do_truncate(
  TransContext *txc, CollectionRef& c, OnodeRef o, uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec << dendl;

  if (offset < o->onode.size) {
    // ensure any wal IO has completed before we truncate off any extents
    // they may touch.
    o->flush();

    WriteContext wctx;
    o->onode.punch_hole(offset, o->onode.size, &wctx.lex_old);
    _wctx_finish(txc, c, o, &wctx);
  }

  o->onode.size = offset;

  txc->write_onode(o);
  return 0;
}

int BlueStore::_truncate(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << dendl;
  int r = _do_truncate(txc, c, o, offset);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_remove(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o)
{
  int r = _do_truncate(txc, c, o, 0);
  if (r < 0)
    return r;
  if (o->onode.omap_head) {
    _do_omap_clear(txc, o->onode.omap_head);
  }
  o->exists = false;
  o->onode = bluestore_onode_t();
  txc->onodes.erase(o);
  txc->t->rmkey(PREFIX_OBJ, o->key);
  return 0;
}

int BlueStore::_remove(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef &o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = _do_remove(txc, c, o);
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
  o->onode.attrs[name] = val;
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
    if (p->second.is_partial())
      o->onode.attrs[p->first] = bufferptr(p->second.c_str(), p->second.length());
    else
      o->onode.attrs[p->first] = p->second;
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
  map<string, bufferptr>::iterator it = o->onode.attrs.find(name);
  if (it == o->onode.attrs.end())
    goto out;

  o->onode.attrs.erase(name);
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

void BlueStore::_do_omap_clear(TransContext *txc, uint64_t id)
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  string prefix, tail;
  get_omap_header(id, &prefix);
  get_omap_tail(id, &tail);
  it->lower_bound(prefix);
  while (it->valid()) {
    if (it->key() >= tail) {
      dout(30) << __func__ << "  stop at " << pretty_binary_string(tail)
	       << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << pretty_binary_string(it->key()) << dendl;
    it->next();
  }
}

int BlueStore::_omap_clear(TransContext *txc,
			   CollectionRef& c,
			   OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  if (o->onode.omap_head != 0) {
    _do_omap_clear(txc, o->onode.omap_head);
    o->onode.omap_head = 0;
    txc->write_onode(o);
  }
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
  bufferlist::iterator p = bl.begin();
  __u32 num;
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  ::decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    ::decode(key, p);
    ::decode(value, p);
    string final_key;
    get_omap_key(o->onode.omap_head, key, &final_key);
    dout(30) << __func__ << "  " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->set(PREFIX_OMAP, final_key, value);
  }
  r = 0;
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_setheader(TransContext *txc,
			       CollectionRef& c,
			       OnodeRef &o,
			       bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r;
  string key;
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  get_omap_header(o->onode.omap_head, &key);
  txc->t->set(PREFIX_OMAP, key, bl);
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
  bufferlist::iterator p = bl.begin();
  __u32 num;

  if (!o->onode.omap_head) {
    r = 0;
    goto out;
  }
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    string final_key;
    get_omap_key(o->onode.omap_head, key, &final_key);
    dout(30) << __func__ << "  rm " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->rmkey(PREFIX_OMAP, final_key);
  }
  r = 0;

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
  KeyValueDB::Iterator it;
  string key_first, key_last;
  int r = 0;
  if (!o->onode.omap_head) {
    goto out;
  }
  it = db->get_iterator(PREFIX_OMAP);
  get_omap_key(o->onode.omap_head, first, &key_first);
  get_omap_key(o->onode.omap_head, last, &key_last);
  it->lower_bound(key_first);
  while (it->valid()) {
    if (it->key() >= key_last) {
      dout(30) << __func__ << "  stop at " << pretty_binary_string(key_last)
	       << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << pretty_binary_string(it->key()) << dendl;
    it->next();
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
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

  bufferlist bl;
  newo->exists = true;
  _assign_nid(txc, newo);

  // data
  oldo->flush();

  r = _do_truncate(txc, c, newo, 0);
  if (r < 0)
    goto out;

  if (g_conf->bluestore_clone_cow) {
    if (!oldo->onode.extent_map.empty()) {
      if (!oldo->bnode) {
	oldo->bnode = c->get_bnode(newo->oid.hobj.get_hash());
      }
      if (!newo->bnode) {
	newo->bnode = oldo->bnode;
      }
      assert(newo->bnode == oldo->bnode);
      // move blobs
      map<int64_t,int64_t> moved_blobs;
      for (auto& p : oldo->onode.extent_map) {
	if (!p.second.is_shared() && moved_blobs.count(p.second.blob) == 0) {
	  Blob *b = oldo->blob_map.get(p.second.blob);
	  oldo->blob_map.erase(b);
	  newo->bnode->blob_map.claim(b);
	  moved_blobs[p.second.blob] = b->id;
	  dout(30) << __func__ << "  moving old onode blob " << p.second.blob
		   << " to bnode blob " << b->id << dendl;
	  b->blob.clear_flag(bluestore_blob_t::FLAG_MUTABLE);
	}
      }
      // update lextents
      for (auto& p : oldo->onode.extent_map) {
	if (moved_blobs.count(p.second.blob)) {
	  p.second.blob = -moved_blobs[p.second.blob];
	}
	newo->onode.extent_map[p.first] = p.second;
	newo->bnode->blob_map.get(-p.second.blob)->blob.ref_map.get(
	  p.second.offset,
	  p.second.length);
	txc->statfs_delta.stored() += p.second.length;
      }
      _dump_onode(newo);
      txc->write_bnode(newo->bnode);
      if (!moved_blobs.empty()) {
	txc->write_onode(oldo);
      }
    }
    newo->onode.size = oldo->onode.size;
  } else {
    // read + write
    r = _do_read(c.get(), oldo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
      goto out;

    r = _do_write(txc, c, newo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
      goto out;
  }

  // attrs
  newo->onode.attrs = oldo->onode.attrs;

  // clone omap
  if (newo->onode.omap_head) {
    dout(20) << __func__ << " clearing old omap data" << dendl;
    _do_omap_clear(txc, newo->onode.omap_head);
  }
  if (oldo->onode.omap_head) {
    dout(20) << __func__ << " copying omap data" << dendl;
    if (!newo->onode.omap_head) {
      newo->onode.omap_head = newo->onode.nid;
    }
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(oldo->onode.omap_head, &head);
    get_omap_tail(oldo->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      string key;
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	dout(30) << __func__ << "  got header/data "
		 << pretty_binary_string(it->key()) << dendl;
	assert(it->key() < tail);
	rewrite_omap_key(newo->onode.omap_head, it->key(), &key);
	txc->t->set(PREFIX_OMAP, key, it->value());
      }
      it->next();
    }
  }

  txc->write_onode(newo);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << " = " << r << dendl;
  return r;
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

  bufferlist bl;
  newo->exists = true;
  _assign_nid(txc, newo);

  r = _do_read(c.get(), oldo, srcoff, length, bl, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, c, newo, dstoff, bl.length(), bl, 0);
  if (r < 0)
    goto out;

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

  if (newo) {
    if (newo->exists) {
      r = -EEXIST;
      goto out;
    }
    assert(txc->onodes.count(newo) == 0);
  }

  txc->t->rmkey(PREFIX_OBJ, oldo->key);
  txc->write_onode(oldo);
  newo = oldo;

  // this adjusts oldo->{oid,key}, and reset oldo to a fresh empty
  // Onode in the old slot
  c->onode_map.rename(oldo, old_oid, new_oid);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

// collections

int BlueStore::_create_collection(
  TransContext *txc,
  coll_t cid,
  unsigned bits,
  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << " bits " << bits << dendl;
  int r;
  bufferlist bl;

  {
    RWLock::WLocker l(coll_lock);
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    c->reset(
      new Collection(
	this,
	cache_shards[cid.hash_to_shard(cache_shards.size())],
	cid));
    (*c)->cnode.bits = bits;
    coll_map[cid] = *c;
  }
  ::encode((*c)->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(cid), bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " bits " << bits << " = " << r << dendl;
  return r;
}

int BlueStore::_remove_collection(TransContext *txc, coll_t cid,
				  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;

  {
    RWLock::WLocker l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    assert((*c)->exists);
    if ((*c)->onode_map.map_any([&](OnodeRef o) {
	  if (o->exists) {
	    dout(10) << __func__ << " " << o->oid << " " << o
		     << " exists in onode_map" << dendl;
	    return true;
	  }
	  return false;
	})) {
      r = -ENOTEMPTY;
      goto out;
    }
    coll_map.erase(cid);
    txc->removed_collections.push_back(*c);
    (*c)->exists = false;
    c->reset();
  }
  txc->t->rmkey(PREFIX_COLL, stringify(cid));
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

int BlueStore::_split_collection(TransContext *txc,
				CollectionRef& c,
				CollectionRef& d,
				unsigned bits, int rem)
{
  dout(15) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  RWLock::WLocker l2(d->lock);

  // blow away the caches.  FIXME.
  c->onode_map.clear();
  d->onode_map.clear();

  c->cnode.bits = bits;
  assert(d->cnode.bits == bits);
  r = 0;

  bufferlist bl;
  ::encode(c->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(c->cid), bl);

  dout(10) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

// ===========================================
