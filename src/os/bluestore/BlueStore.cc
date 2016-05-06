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
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "BlueFS.h"
#include "BlueRocksEnv.h"

#define dout_subsys ceph_subsys_bluestore

const string PREFIX_SUPER = "S";   // field -> value
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

static bool is_enode_key(const string& key)
{
  if (key.size() == 2 + 8 + 4)
    return true;
  return false;
}

static void get_enode_key(shard_id_t shard, int64_t pool, uint32_t hash,
			  string *key)
{
  key->clear();
  _key_encode_shard(shard, key);
  _key_encode_u64(pool + 0x8000000000000000ull, key);
  _key_encode_u32(hobject_t::_reverse_bits(hash), key);
}

static int get_key_enode(const string& key, shard_id_t *shard,
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

// Enode

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.enode(" << this << ") "

void BlueStore::Enode::put()
{
  if (--nref == 0) {
    dout(20) << __func__ << " removing self from set " << enode_set << dendl;
    enode_set->uset.erase(*this);
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

// OnodeHashLRU

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.lru(" << this << ") "

void BlueStore::OnodeHashLRU::_touch(OnodeRef o)
{
  lru_list_t::iterator p = lru.iterator_to(*o);
  lru.erase(p);
  lru.push_front(*o);
}

void BlueStore::OnodeHashLRU::add(const ghobject_t& oid, OnodeRef o)
{
  std::lock_guard<std::mutex> l(lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  lru.push_front(*o);
  _trim(max_size);
}

BlueStore::OnodeRef BlueStore::OnodeHashLRU::lookup(const ghobject_t& oid)
{
  std::lock_guard<std::mutex> l(lock);
  dout(30) << __func__ << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
  if (p == onode_map.end()) {
    dout(30) << __func__ << " " << oid << " miss" << dendl;
    return OnodeRef();
  }
  dout(30) << __func__ << " " << oid << " hit " << p->second << dendl;
  _touch(p->second);
  return p->second;
}

void BlueStore::OnodeHashLRU::clear()
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << dendl;
  lru.clear();
  onode_map.clear();
}

void BlueStore::OnodeHashLRU::rename(OnodeRef& oldo,
				     const ghobject_t& old_oid,
				     const ghobject_t& new_oid)
{
  std::lock_guard<std::mutex> l(lock);
  dout(30) << __func__ << " " << old_oid << " -> " << new_oid << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);
  assert(po != pn);

  assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    dout(30) << __func__ << "  removing target " << pn->second << dendl;
    lru_list_t::iterator p = lru.iterator_to(*pn->second);
    lru.erase(p);
    onode_map.erase(pn);
  }
  OnodeRef o = po->second;

  // install a non-existent onode at old location
  oldo.reset(new Onode(old_oid, o->key));
  po->second = oldo;
  lru.push_back(*po->second);

  // add at new position and fix oid, key
  onode_map.insert(make_pair(new_oid, o));
  _touch(o);
  o->oid = new_oid;
  get_object_key(new_oid, &o->key);
}

bool BlueStore::OnodeHashLRU::get_next(
  const ghobject_t& after,
  pair<ghobject_t,OnodeRef> *next)
{
  std::lock_guard<std::mutex> l(lock);
  dout(20) << __func__ << " after " << after << dendl;

  if (after == ghobject_t()) {
    if (lru.empty()) {
      return false;
    }
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.begin();
    assert(p != onode_map.end());
    next->first = p->first;
    next->second = p->second;
    return true;
  }

  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(after);
  assert(p != onode_map.end()); // for now
  lru_list_t::iterator pi = lru.iterator_to(*p->second);
  ++pi;
  if (pi == lru.end()) {
    return false;
  }
  next->first = pi->oid;
  next->second = onode_map[pi->oid];
  return true;
}

int BlueStore::OnodeHashLRU::trim(int max)
{
  std::lock_guard<std::mutex> l(lock);
  if (max < 0) {
    max = max_size;
  }
  return _trim(max);
}

int BlueStore::OnodeHashLRU::_trim(int max)
{
  dout(20) << __func__ << " max " << max << " size " << onode_map.size() << dendl;
  int trimmed = 0;
  int num = onode_map.size() - max;
  if (onode_map.size() == 0 || num <= 0)
    return 0; // don't even try
  
  lru_list_t::iterator p = lru.end();
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
    if (p != lru.begin()) {
      lru.erase(p--);
    } else {
      lru.erase(p);
      assert(num == 1);
    }
    o->get();  // paranoia
    onode_map.erase(o->oid);
    o->put();
    --num;
    ++trimmed;
  }
  return trimmed;
}

// =======================================================

// Collection

#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << store->path << ").collection(" << cid << ") "

BlueStore::Collection::Collection(BlueStore *ns, coll_t c)
  : store(ns),
    cid(c),
    lock("BlueStore::Collection::lock", true, false),
    exists(true),
    enode_set(g_conf->bluestore_onode_map_size),
    onode_map(g_conf->bluestore_onode_map_size)
{
}

BlueStore::EnodeRef BlueStore::Collection::get_enode(
  uint32_t hash
  )
{
  Enode dummy(hash, string(), NULL);
  auto p = enode_set.uset.find(dummy);
  if (p == enode_set.uset.end()) {
    spg_t pgid;
    if (!cid.is_pg(&pgid))
      pgid = spg_t();  // meta
    string key;
    get_enode_key(pgid.shard, pgid.pool(), hash, &key);
    EnodeRef e = new Enode(hash, key, &enode_set);
    dout(10) << __func__ << " hash " << std::hex << hash << std::dec
	     << " created " << e << dendl;

    bufferlist v;
    int r = store->db->get(PREFIX_OBJ, key, &v);
    if (r >= 0) {
      assert(v.length() > 0);
      bufferlist::iterator p = v.begin();
      ::decode(e->ref_map, p);
      dout(10) << __func__ << " hash " << std::hex << hash << std::dec
	       << " loaded ref_map " << e->ref_map << dendl;
    } else {
      dout(10) << __func__ << " hash " <<std::hex << hash << std::dec
	       << " missed, new ref_map" << dendl;
    }
    enode_set.uset.insert(*e);
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
    on = new Onode(oid, key);
  } else {
    // loaded
    assert(r >=0);
    on = new Onode(oid, key);
    on->exists = true;
    bufferlist::iterator p = v.begin();
    ::decode(on->onode, p);
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
	   cct->_conf->bluestore_wal_threads,
	   "bluestore_wal_threads"),
    wal_wq(this,
	     cct->_conf->bluestore_wal_thread_timeout,
	     cct->_conf->bluestore_wal_thread_suicide_timeout,
	     &wal_tp),
    finisher(cct),
    kv_sync_thread(this),
    kv_stop(false),
    logger(NULL)
{
  _init_logger();
}

BlueStore::~BlueStore()
{
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(bluefs == NULL);
  assert(fsid_fd < 0);
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
  b.add_time_avg(l_bluestore_state_wal_done_lat, "state_wal_done_lat", "Average wal_done state latency");
  b.add_time_avg(l_bluestore_state_finishing_lat, "state_finishing_lat", "Average finishing state latency");
  b.add_time_avg(l_bluestore_state_done_lat, "state_done_lat", "Average done state latency");
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

int BlueStore::_open_alloc()
{
  assert(fm == NULL);
  assert(alloc == NULL);
  fm = new FreelistManager();
  int r = fm->init(db, PREFIX_ALLOC);
  if (r < 0) {
    delete fm;
    fm = NULL;
    return r;
  }

  alloc = Allocator::create("stupid");
  uint64_t num = 0, bytes = 0;
  const auto& fl = fm->get_freelist();
  for (auto& p : fl) {
    alloc->init_add_free(p.first, p.second);
    ++num;
    bytes += p.second;
  }
  dout(10) << __func__ << " loaded " << pretty_si_t(bytes)
	   << " in " << num << " extents"
	   << dendl;
  return r;
}

void BlueStore::_close_alloc()
{
  assert(fm);
  assert(alloc);
  alloc->shutdown();
  delete alloc;
  alloc = NULL;
  fm->shutdown();
  delete fm;
  fm = NULL;
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

int BlueStore::_balance_bluefs_freespace(vector<bluestore_extent_t> *extents,
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
    uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
    gift = ROUND_UP_TO(gift, min_alloc_size);

    // hard cap to fit into 32 bits
    gift = MIN(gift, 1ull<<31);
    dout(10) << __func__ << " gifting " << gift
	     << " (" << pretty_si_t(gift) << ")" << dendl;

    // fixme: just do one allocation to start...
    int r = alloc->reserve(gift);
    assert(r == 0);

    bluestore_extent_t e;
    r = alloc->allocate(gift, min_alloc_size, 0, &e.offset, &e.length);
    if (r < 0) {
      assert(0 == "allocate failed, wtf");
      return r;
    }
    if (e.length < gift) {
      alloc->unreserve(gift - e.length);
    }

    dout(1) << __func__ << " gifting " << e << " to bluefs" << dendl;
    extents->push_back(e);
    ret = 1;
  }

  // reclaim from bluefs?
  if (reclaim) {
    // round up to alloc size
    uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
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
  const vector<bluestore_extent_t>& bluefs_gift_extents)
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
      CollectionRef c(new Collection(this, cid));
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

  r = _open_alloc();
  if (r < 0)
    goto out_close_db;

  // initialize freespace
  {
    dout(20) << __func__ << " initializing freespace" << dendl;
    KeyValueDB::Transaction t = db->get_transaction();
    uint64_t reserved = 0;
    if (g_conf->bluestore_bluefs) {
      assert(bluefs_extents.num_intervals() == 1);
      interval_set<uint64_t>::iterator p = bluefs_extents.begin();
      reserved = p.get_start() + p.get_len();
      dout(20) << __func__ << " reserved " << reserved << " for bluefs" << dendl;
      bufferlist bl;
      ::encode(bluefs_extents, bl);
      t->set(PREFIX_SUPER, "bluefs_extents", bl);
      dout(20) << __func__ << " bluefs_extents " << bluefs_extents << dendl;
    } else {
      reserved = BLUEFS_START;
    }
    uint64_t end = bdev->get_size() - reserved;
    if (g_conf->bluestore_debug_prefill > 0) {
      dout(1) << __func__ << " pre-fragmenting freespace, using "
	      << g_conf->bluestore_debug_prefill << " with max free extent "
	      << g_conf->bluestore_debug_prefragment_max << dendl;
      uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
      uint64_t start = ROUND_UP_TO(reserved, min_alloc_size);
      uint64_t max_b = g_conf->bluestore_debug_prefragment_max / min_alloc_size;
      float r = g_conf->bluestore_debug_prefill;
      while (start < end) {
	uint64_t l = (rand() % max_b + 1) * min_alloc_size;
	if (start + l > end)
	  l = end - start;
	l = ROUND_UP_TO(l, min_alloc_size);
	fm->release(start, l, t);
	uint64_t u = 1 + (uint64_t)(r * (double)l / (1.0 - r));
	u = ROUND_UP_TO(u, min_alloc_size);
	dout(20) << "  free " << start << "~" << l << " use " << u << dendl;
	start += l + u;
      }
    } else {
      fm->release(reserved, end, t);
    }
    assert(0 == db->submit_transaction_sync(t));
  }

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

int BlueStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

  {
    string type;
    int r = read_meta("type", &type);
    if (r < 0) {
      derr << __func__ << " failed to load os-type: " << cpp_strerror(r) << dendl;
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

  r = _open_alloc();
  if (r < 0)
    goto out_db;

  r = _open_super_meta();
  if (r < 0)
    goto out_alloc;

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

int BlueStore::_verify_enode_shared(
  EnodeRef enode,
  map<int64_t,vector<bluestore_pextent_t>>& v,
  interval_set<uint64_t> &used_blocks)
{
  int errors = 0;
  dout(10) << __func__ << " hash " << enode->hash << " v " << v << dendl;
  for (auto& b : enode->blob_map) {
    auto pv = v.find(b.first);
    if (pv == v.end()) {
      derr << " hash " << enode->hash << " blob " << b.first
	   << " exists in bnode but has no refs" << dendl;
      ++errors;
    }
    bluestore_extent_ref_map_t ref_map;
    interval_set<uint64_t> span;
    for (auto& p : pv->second) {
      interval_set<uint64_t> t, i;
      t.insert(p.offset, p.length);
      i.intersection_of(t, span);
      t.subtract(i);
      dout(20) << __func__ << "  extent " << p << " t " << t << " i " << i
	       << dendl;
      for (interval_set<uint64_t>::iterator q = t.begin(); q != t.end(); ++q) {
	ref_map.add(q.get_start(), q.get_len(), 1);
      }
      for (interval_set<uint64_t>::iterator q = i.begin(); q != i.end(); ++q) {
	ref_map.get(q.get_start(), q.get_len());
      }
      span.insert(t);
    }
    if (b.second.ref_map != ref_map) {
      derr << " hash " << enode->hash << " blob " << b.first
	   << " ref_map " << b.second.ref_map
	   << " != expected " << ref_map << dendl;
      ++errors;
    }
    for (auto& p : b.second.extents) {
      interval_set<uint64_t> e, i;
      e.insert(p.offset, p.length);
      i.intersection_of(e, used_blocks);
      if (!i.empty()) {
	derr << " hash " << enode->hash << " extent(s) " << i
	     << " already allocated" << dendl;
	++errors;
      } else {
	used_blocks.insert(p.offset, p.length);
      }
    }
    v.erase(b.first);
  }
  for (auto& p : v) {
    derr << " hash " << enode->hash << " blob " << p.first
	 << " dne, has extent refs "
	 << p.second << dendl;
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
  EnodeRef enode;
  map<int64_t,vector<bluestore_pextent_t>> hash_shared;

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

  r = _open_alloc();
  if (r < 0)
    goto out_db;

  r = _open_super_meta();
  if (r < 0)
    goto out_alloc;

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
	dout(10) << __func__ << "  " << oid << dendl;
	OnodeRef o = c->get_onode(oid, false);
	if (!o || !o->exists) {
	  ++errors;
	  continue; // go for next object
	}
	if (!enode || enode->hash != o->oid.hobj.get_hash()) {
	  if (enode)
	    errors += _verify_enode_shared(enode, hash_shared, used_blocks);
	  enode = c->get_enode(o->oid.hobj.get_hash());
	  hash_shared.clear();
	}
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
	set<uint64_t> local_blobs;
	for (auto& l : o->onode.extent_map) {
	  if (l.second.blob >= 0) {
	    local_blobs.insert(l.second.blob);
	    // fixme: make sure offset,length are valid
	  } else {
	    hash_shared[-l.second.blob].push_back(
	      bluestore_pextent_t(l.second.offset, l.second.length));
	  }
	}
	// blobs
	for (auto& b : o->onode.blob_map) {
	  for (auto& e : b.second.extents) {
	    if (used_blocks.intersects(e.offset, e.length)) {
	      derr << " " << oid << " blob " << b.first << " extent " << e
		   << " already allocated" << dendl;
	      ++errors;
	      continue;
	    }
	    used_blocks.insert(e.offset, e.length);
	    if (e.end() > bdev->get_size()) {
	      derr << " " << oid << " blob " << b.first << " extent " << e
		   << " past end of block device" << dendl;
	      ++errors;
	    }
	  }
	  if (local_blobs.count(b.first)) {
	    local_blobs.erase(b.first);
	  } else {
	    derr << " " << oid << " blob " << b.first << " has no lextent refs"
		 << dendl;
	    ++errors;
	  }
	}
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
  if (enode) {
    errors += _verify_enode_shared(enode, hash_shared, used_blocks);
    hash_shared.clear();
    enode.reset();
  }

  dout(1) << __func__ << " checking for stray enodes and onodes" << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (it) {
    CollectionRef c;
    bool expecting_objects = false;
    shard_id_t expecting_shard;
    int64_t expecting_pool;
    uint32_t expecting_hash;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      ghobject_t oid;
      if (is_enode_key(it->key())) {
	if (expecting_objects) {
	  dout(30) << __func__ << "  had enode but no objects for "
		   << std::hex << expecting_hash << std::dec << dendl;
	  ++errors;
	}
	get_key_enode(it->key(), &expecting_shard, &expecting_pool,
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
	  dout(30) << __func__ << "  had enode but no objects for "
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
      dout(30) << __func__ << "  had enode but no objects for "
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
    const auto& free = fm->get_freelist();
    for (auto p = free.begin();
	 p != free.end(); ++p) {
      if (used_blocks.intersects(p->first, p->second)) {
	derr << __func__ << " free extent " << p->first << "~" << p->second
	     << " intersects allocated blocks" << dendl;
	interval_set<uint64_t> free, overlap;
	free.insert(p->first, p->second);
	overlap.intersection_of(free, used_blocks);
	derr << __func__ << " overlap: " << overlap << dendl;
	++errors;
	continue;
      }
      used_blocks.insert(p->first, p->second);
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

int BlueStore::statfs(struct statfs *buf)
{
  memset(buf, 0, sizeof(*buf));
  buf->f_blocks = bdev->get_size() / bdev->get_block_size();
  buf->f_bsize = bdev->get_block_size();
  buf->f_bfree = fm->get_total_free() / bdev->get_block_size();
  buf->f_bavail = buf->f_bfree;
  dout(20) << __func__ << " free " << pretty_si_t(buf->f_bfree * buf->f_bsize)
	   << " / " << pretty_si_t(buf->f_blocks * buf->f_bsize) << dendl;
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
    {
      pair<ghobject_t,OnodeRef> next;
      while (c->onode_map.get_next(next.first, &next)) {
	assert(!next.second->exists);
	if (!next.second->flush_txns.empty()) {
	  dout(10) << __func__ << " " << c->cid << " " << next.second->oid
		   << " flush_txns " << next.second->flush_txns << dendl;
	  return;
	}
      }
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
	   << " " << offset << "~" << length
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
	   << " " << offset << "~" << length
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
  map<uint64_t,bluestore_lextent_t>::iterator bp, bend;
  map<uint64_t,bluestore_overlay_t>::iterator op, oend;
  uint64_t block_size = bdev->get_block_size();
  int r = 0;
  IOContext ioc(NULL);   // FIXME?
  EnodeRef enode;

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

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << dendl;
  bl.clear();
  _dump_onode(o);

  if (offset > o->onode.size) {
    goto out;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->flush();

  // loop over overlays and data fragments.  overlays take precedence.
  bend = o->onode.extent_map.end();
  bp = o->onode.extent_map.lower_bound(offset);
  if (bp != o->onode.extent_map.begin()) {
    --bp;
  }
  oend = o->onode.overlay_map.end();
  op = o->onode.overlay_map.lower_bound(offset);
  if (op != o->onode.overlay_map.begin()) {
    --op;
  }
  while (length > 0) {
    if (op != oend && op->first + op->second.length < offset) {
      dout(20) << __func__ << " skip overlay " << op->first << " " << op->second
	       << dendl;
      ++op;
      continue;
    }
    if (bp != bend && bp->first + bp->second.length <= offset) {
      dout(30) << __func__ << " skip lextent " << bp->first << " " << bp->second
	       << dendl;
      ++bp;
      continue;
    }

    // overlay?
    if (op != oend && op->first <= offset) {
      uint64_t x_off = offset - op->first + op->second.value_offset;
      uint64_t x_len = MIN(op->first + op->second.length - offset, length);
      dout(20) << __func__ << "  overlay " << op->first << " " << op->second
	       << " use " << x_off << "~" << x_len << dendl;
      bufferlist v;
      string key;
      get_overlay_key(o->onode.nid, op->second.key, &key);
      r = db->get(PREFIX_OVERLAY, key, &v);
      if (r < 0) {
        derr << " failed to fetch overlay(nid = " << o->onode.nid
             << ", key = " << key 
             << "): " << cpp_strerror(r) << dendl;
        goto out;
      }
      bufferlist frag;
      frag.substr_of(v, x_off, x_len);
      bl.claim_append(frag);
      ++op;
      length -= x_len;
      offset += x_len;
      continue;
    }
    unsigned x_len = length;
    if (op != oend &&
	op->first > offset &&
	op->first - offset < x_len) {
      x_len = op->first - offset;
    }

    // extent?
    if (bp != bend && bp->first <= offset) {
      uint64_t x_off = offset - bp->first;
      x_len = MIN(x_len, bp->second.length - x_off);
      uint64_t p_off = x_off + bp->second.offset;
      bluestore_blob_t *b = c->get_blob_ptr(o, bp->second.blob);
      dout(30) << __func__ << " lextent 0x" << std::hex << bp->first << std::dec
	       << ": " << bp->second
	       << " use 0x" << std::hex << p_off << "~0x" << x_len << std::dec
	       << " blob " << bp->second.blob << " " << *b
	       << dendl;
      vector<bluestore_pextent_t>::iterator p = b->extents.begin();
      while (x_len > 0) {
	assert(p != b->extents.end());
	if (p_off >= p->length) {
	  p_off -= p->length;
	  ++p;
	  continue;
	}
	uint64_t p_len = MIN(p->length - p_off, x_len);
	uint64_t front_extra = p_off % block_size;
	uint64_t r_off = p_off - front_extra;
	uint64_t r_len = ROUND_UP_TO(p_len + front_extra, block_size);
	dout(30) << __func__ << "  reading 0x" << std::hex << r_off << "~0x"
		 << r_len << std::dec
		 << " from " << *p << dendl;
	bufferlist t;
	r = bdev->read(r_off + p->offset, r_len, &t, &ioc, buffered);
	if (r < 0) {
	  goto out;
	}
	r = r_len;
	bufferlist u;
	u.substr_of(t, front_extra, p_len);
	bl.claim_append(u);
	offset += p_len;
	length -= p_len;
	p_off = 0;
	x_off += p_len;
	x_len -= p_len;
	++p;
      }
      if (x_off == bp->second.length) {
	++bp;
      }
      continue;
    }
    if (bp != bend &&
	bp->first > offset &&
	bp->first - offset < x_len) {
      x_len = bp->first - offset;
    }

    // zero.
    dout(30) << __func__ << " zero " << offset << "~" << x_len << dendl;
    bl.append_zero(x_len);
    offset += x_len;
    length -= x_len;
  }
  r = bl.length();

 out:
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
  size_t len,
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

  dout(20) << __func__ << " " << offset << "~" << len << " size "
	   << o->onode.size << dendl;

  map<uint64_t,bluestore_lextent_t>::iterator bp, bend;
  map<uint64_t,bluestore_overlay_t>::iterator op, oend;

  if (offset > o->onode.size)
    goto out;

  if (offset + len > o->onode.size) {
    len = o->onode.size - offset;
  }

  // loop over overlays and data fragments.  overlays take precedence.
  bend = o->onode.extent_map.end();
  bp = o->onode.extent_map.lower_bound(offset);
  if (bp != o->onode.extent_map.begin()) {
    --bp;
  }
  oend = o->onode.overlay_map.end();
  op = o->onode.overlay_map.lower_bound(offset);
  if (op != o->onode.overlay_map.begin()) {
    --op;
  }
  while (len > 0) {
    dout(20) << __func__ << " offset " << offset << dendl;
    if (op != oend && op->first + op->second.length < offset) {
      ++op;
      continue;
    }
    if (bp != bend && bp->first + bp->second.length <= offset) {
      ++bp;
      continue;
    }

    // overlay?
    if (op != oend && op->first <= offset) {
      uint64_t x_len = MIN(op->first + op->second.length - offset, len);
      dout(30) << __func__ << " overlay " << offset << "~" << x_len << dendl;
      m.insert(offset, x_len);
      len -= x_len;
      offset += x_len;
      ++op;
      continue;
    }

    uint64_t x_len = len;
    if (op != oend &&
	op->first > offset &&
	op->first - offset < x_len) {
      x_len = op->first - offset;
    }

    // extent?
    if (bp != bend && bp->first <= offset) {
      uint64_t x_off = offset - bp->first;
      x_len = MIN(x_len, bp->second.length - x_off);
      dout(30) << __func__ << " lextent " << offset << "~" << x_len
	       << " blob " << bp->second.blob << dendl;
      m.insert(offset, x_len);
      len -= x_len;
      offset += x_len;
      if (x_off + x_len == bp->second.length)
	++bp;
      continue;
    }
    if (bp != bend &&
	bp->first > offset &&
	bp->first - offset < x_len) {
      x_len = bp->first - offset;
    }
    offset += x_len;
    len -= x_len;
  }

 out:
  ::encode(m, bl);
  dout(20) << __func__ << " " << offset << "~" << len
	   << " size = 0 (" << m << ")" << dendl;
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
      start.hobj == hobject_t::get_max()) {
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
    if (is_enode_key(it->key())) {
      dout(20) << __func__ << " key "
	       << pretty_binary_string(it->key())
	       << " (enode, skipping)" << dendl;
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
  dout(20) << __func__ << " osr " << osr << " = " << txc << dendl;
  return txc;
}

void BlueStore::_txc_release(
  TransContext *txc, CollectionRef& c, OnodeRef& o,
  uint64_t offset, uint64_t length,
  bool shared)
{
  if (shared) {
    vector<bluestore_pextent_t> release;
    if (!o->enode)
      o->enode = c->get_enode(o->oid.hobj.get_hash());
    o->enode->ref_map.put(offset, length, &release);
    dout(10) << __func__ << " " << offset << "~" << length
	     << " shared: ref_map now " << o->enode->ref_map
	     << " releasing " << release << dendl;
    txc->write_enode(o->enode);
    for (auto& p : release) {
      txc->released.insert(p.offset, p.length);
    }
  } else {
    dout(10) << __func__ << " " << offset << "~" << length << dendl;
    txc->released.insert(offset, length);
  }
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
      if (!g_conf->bluestore_sync_transaction) {
	if (g_conf->bluestore_sync_submit_transaction) {
	  _txc_update_fm(txc);
	  assert(0 == db->submit_transaction(txc->t));
	}
      } else {
	_txc_update_fm(txc);
	assert(0 == db->submit_transaction_sync(txc->t));
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
	if (g_conf->bluestore_sync_wal_apply) {
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

void BlueStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc
	   << " onodes " << txc->onodes << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
    dout(20) << "  onode " << (*p)->oid << " is " << bl.length() << dendl;
    txc->t->set(PREFIX_OBJ, (*p)->key, bl);

    std::lock_guard<std::mutex> l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }

  // finalize enodes
  for (set<EnodeRef>::iterator p = txc->enodes.begin();
       p != txc->enodes.end();
       ++p) {
    if ((*p)->ref_map.empty()) {
      dout(20) << "  enode " << std::hex << (*p)->hash << std::dec
	       << " ref_map is empty" << dendl;
      txc->t->rmkey(PREFIX_OBJ, (*p)->key);
    } else {
      bufferlist bl;
      ::encode((*p)->ref_map, bl);
      dout(20) << "  enode " << std::hex << (*p)->hash << std::dec
	       << " ref_map is " << bl.length() << dendl;
      txc->t->set(PREFIX_OBJ, (*p)->key, bl);
    }
  }

  // journal wal items
  if (txc->wal_txn) {
    txc->wal_txn->released.swap(txc->released);
    assert(txc->released.empty());

    txc->wal_txn->seq = wal_seq.inc();
    bufferlist bl;
    ::encode(*txc->wal_txn, bl);
    string key;
    get_wal_key(txc->wal_txn->seq, &key);
    txc->t->set(PREFIX_WAL, key, bl);
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
  while (!osr->q.empty()) {
    TransContext *txc = &osr->q.front();
    dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	     << dendl;
    if (txc->state != TransContext::STATE_DONE) {
      break;
    }

    if (txc->first_collection) {
      txc->first_collection->onode_map.trim();
    }

    osr->q.pop_front();
    txc->log_state_latency(logger, l_bluestore_state_done_lat);
    delete txc;
    osr->qcond.notify_all();
    if (osr->q.empty())
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
  }
}

void BlueStore::_txc_update_fm(TransContext *txc)
{
  if (txc->wal_txn)
    dout(20) << __func__ << " txc " << txc
      << " allocated " << txc->allocated
      << " (will release " << txc->released << " after wal)"
      << dendl;
  else
    dout(20) << __func__ << " txc " << txc
      << " allocated " << txc->allocated
      << " released " << txc->released
      << dendl;

  for (interval_set<uint64_t>::iterator p = txc->allocated.begin();
      p != txc->allocated.end();
      ++p) {
    fm->allocate(p.get_start(), p.get_len(), txc->t);
  }

  for (interval_set<uint64_t>::iterator p = txc->released.begin();
      p != txc->released.end();
      ++p) {
    dout(20) << __func__ << " release " << p.get_start()
      << "~" << p.get_len() << dendl;
    fm->release(p.get_start(), p.get_len(), txc->t);

    if (!g_conf->bluestore_debug_no_reuse_blocks)
      alloc->release(p.get_start(), p.get_len());
  }
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
	  _txc_update_fm((*it));
	  assert(0 == db->submit_transaction((*it)->t));
	}
      }

      // one final transaction to force a sync
      KeyValueDB::Transaction t = db->get_transaction();

      vector<bluestore_extent_t> bluefs_gift_extents;
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

      // allocations and deallocations
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	  it != wal_cleaning.end();
	  ++it) {
	TransContext *txc = *it;
	if (!txc->wal_txn->released.empty()) {
	  dout(20) << __func__ << " txc " << txc
	    << " (post-wal) released " << txc->wal_txn->released
	    << dendl;
	  for (interval_set<uint64_t>::iterator p =
	      txc->wal_txn->released.begin();
	      p != txc->wal_txn->released.end();
	      ++p) {
	    dout(20) << __func__ << " release " << p.get_start()
	      << "~" << p.get_len() << dendl;
	    fm->release(p.get_start(), p.get_len(), t);
	    if (!g_conf->bluestore_debug_no_reuse_blocks)
	      alloc->release(p.get_start(), p.get_len());
	  }
	}
      }

      // cleanup sync wal keys
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	    it != wal_cleaning.end();
	    ++it) {
	bluestore_wal_transaction_t& wt =*(*it)->wal_txn;
	// cleanup the data in overlays
	for (list<bluestore_wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
	  for (vector<uint64_t>::iterator q = p->removed_overlays.begin();
	       q != p->removed_overlays.end();
	       ++q) {
            string key;
            get_overlay_key(p->nid, *q, &key);
	    t->rm_single_key(PREFIX_OVERLAY, key);
	  }
	}
	// cleanup the wal
	string key;
	get_wal_key(wt.seq, &key);
	t->rm_single_key(PREFIX_WAL, key);
      }
      assert(0 == db->submit_transaction_sync(t));

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
    int r = _do_wal_op(*p, &txc->ioc);
    assert(r == 0);
  }

  _txc_state_proc(txc);
  return 0;
}

int BlueStore::_wal_finish(TransContext *txc)
{
  bluestore_wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << " seq " << wt.seq << txc << dendl;

  std::lock_guard<std::mutex> l2(txc->osr->qlock);
  std::lock_guard<std::mutex> l(kv_lock);
  txc->state = TransContext::STATE_WAL_CLEANUP;
  txc->osr->qcond.notify_all();
  wal_cleanup_queue.push_back(txc);
  kv_cond.notify_one();
  return 0;
}

int BlueStore::_do_wal_op(bluestore_wal_op_t& wo, IOContext *ioc)
{
  const uint64_t block_size = bdev->get_block_size();
  const uint64_t block_mask = ~(block_size - 1);
  int r = 0;

  // read all the overlay data first for apply
  _do_read_all_overlays(wo);

  // NOTE: we are doing all reads and writes buffered so that we can
  // avoid worrying about multiple RMW cycles over the same blocks.

  switch (wo.op) {
  case bluestore_wal_op_t::OP_WRITE:
  {
    dout(20) << __func__ << " write " << wo.extent << dendl;
    // FIXME: do the reads async?
    bufferlist bl;
    bl.claim(wo.data);
    uint64_t offset = wo.extent.offset;
    bufferlist first;
    uint64_t first_len = offset & ~block_mask;
    if (first_len) {
      uint64_t src_offset;
      if (wo.src_rmw_head)
	src_offset = wo.src_rmw_head & block_mask;
      else
	src_offset = wo.extent.offset & block_mask;
      offset = offset & block_mask;
      dout(20) << __func__ << "  reading initial partial block "
	       << src_offset << "~" << block_size << dendl;
      r = bdev->read(src_offset, block_size, &first, ioc, true);
      assert(r == 0);
      bufferlist t;
      t.substr_of(first, 0, first_len);
      t.claim_append(bl);
      bl.swap(t);
    }
    if (wo.extent.end() & ~block_mask) {
      uint64_t last_offset;
      if (wo.src_rmw_tail)
	last_offset = wo.src_rmw_tail & block_mask;
      else
	last_offset = wo.extent.end() & block_mask;
      bufferlist last;
      if (last_offset == offset && first.length()) {
	last.claim(first);   // same block we read above
      } else {
	dout(20) << __func__ << "  reading trailing partial block "
		 << last_offset << "~" << block_size << dendl;
	r = bdev->read(last_offset, block_size, &last, ioc, true);
        assert(r == 0);
      }
      bufferlist t;
      uint64_t endoff = wo.extent.end() & ~block_mask;
      t.substr_of(last, endoff, block_size - endoff);
      bl.claim_append(t);
    }
    assert((bl.length() & ~block_mask) == 0);
    r = bdev->aio_write(offset, bl, ioc, true);
    assert(r == 0);
  }
  break;

  case bluestore_wal_op_t::OP_COPY:
  {
    dout(20) << __func__ << " copy " << wo.extent << " from " << wo.src_extent
	     << dendl;
    assert((wo.extent.offset & ~block_mask) == 0);
    assert((wo.extent.length & ~block_mask) == 0);
    assert(wo.extent.length == wo.src_extent.length);
    assert((wo.src_extent.offset & ~block_mask) == 0);
    bufferlist bl;
    r = bdev->read(wo.src_extent.offset, wo.src_extent.length, &bl, ioc,
		       true);
    assert(r == 0);
    assert(bl.length() == wo.extent.length);
    r = bdev->aio_write(wo.extent.offset, bl, ioc, true);
    assert(r == 0);
  }
  break;

  case bluestore_wal_op_t::OP_ZERO:
  {
    dout(20) << __func__ << " zero " << wo.extent << dendl;
    uint64_t offset = wo.extent.offset;
    uint64_t length = wo.extent.length;
    bufferlist first;
    uint64_t first_len = offset & ~block_mask;
    if (first_len) {
      uint64_t first_offset = offset & block_mask;
      dout(20) << __func__ << "  reading initial partial block "
	       << first_offset << "~" << block_size << dendl;
      r = bdev->read(first_offset, block_size, &first, ioc, true);
      assert(r == 0);
      size_t z_len = MIN(block_size - first_len, length);
      memset(first.c_str() + first_len, 0, z_len);
      r = bdev->aio_write(first_offset, first, ioc, true);
      assert(r == 0);
      offset += block_size - first_len;
      length -= z_len;
    }
    assert(offset % block_size == 0);
    if (length >= block_size) {
      uint64_t middle_len = length & block_mask;
      dout(20) << __func__ << "  zero " << offset << "~" << length << dendl;
      r = bdev->aio_zero(offset, middle_len, ioc);
      assert(r == 0);
      offset += middle_len;
      length -= middle_len;
    }
    assert(offset % block_size == 0);
    if (length > 0) {
      assert(length < block_size);
      bufferlist last;
      dout(20) << __func__ << "  reading trailing partial block "
	       << offset << "~" << block_size << dendl;
      r = bdev->read(offset, block_size, &last, ioc, true);
      assert(r == 0);
      memset(last.c_str(), 0, length);
      r = bdev->aio_write(offset, last, ioc, true);
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

  _txc_finalize(osr, txc);

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
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
	r = _setallochint(txc, c, o,
			  expected_object_size,
			  expected_write_size);
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
  dout(10) << __func__ << " " << o->oid << " "
	   << offset << "~" << length << dendl;
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

  // let's avoid considering how overlay interacts with cached tail
  // blocks for now.
  o->clear_tail();

  dout(10) << __func__ << " " << o->oid << " "
	   << offset << "~" << length << dendl;
  bluestore_overlay_t& ov = o->onode.overlay_map[offset] =
    bluestore_overlay_t(++o->onode.last_overlay_key, 0, length);
  dout(20) << __func__ << " added " << offset << " " << ov << dendl;
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
  dout(log_level) << __func__ << " " << o
	   << " nid " << o->onode.nid
	   << " size " << o->onode.size
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
    dout(log_level) << __func__ << "  lextent " << p.first << " " << p.second
		    << dendl;
    assert(p.first >= pos);
    pos = p.first + p.second.length;
  }
  for (auto& b : o->onode.blob_map) {
    dout(log_level) << __func__ << "  blob " << b.first << " " << b.second
		    << dendl;
  }
  pos = 0;
  for (auto& v : o->onode.overlay_map) {
    dout(log_level) << __func__ << "  overlay " << v.first << " " << v.second
		    << dendl;
    assert(v.first >= pos);
    pos = v.first + v.second.length;
  }
  if (!o->onode.overlay_refs.empty()) {
    dout(log_level) << __func__ << "  overlay_refs " << o->onode.overlay_refs
		    << dendl;
  }
}

void BlueStore::_pad_zeros(
  TransContext *txc,
  OnodeRef o,
  bufferlist *bl, uint64_t *offset, uint64_t *length,
  uint64_t block_size)
{
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  // front
  size_t front_pad = *offset % block_size;
  size_t back_pad = 0;
  if (front_pad) {
    size_t front_copy = MIN(block_size - front_pad, *length);
    bufferptr z = buffer::create_page_aligned(block_size);
    memset(z.c_str(), 0, front_pad);
    memcpy(z.c_str() + front_pad, bl->get_contiguous(0, front_copy), front_copy);
    if (front_copy + front_pad < block_size) {
      back_pad = block_size - (*length + front_pad);
      memset(z.c_str() + front_pad + *length, 0, back_pad);
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
  unsigned back_copy = end % block_size;
  if (back_copy) {
    assert(back_pad == 0);
    back_pad = block_size - back_copy;
    assert(back_copy <= *length);
    bufferptr tail(block_size);
    memcpy(tail.c_str(), bl->get_contiguous(*length - back_copy, back_copy),
	   back_copy);
    memset(tail.c_str() + back_copy, 0, back_pad);
    bufferlist old;
    old.swap(*bl);
    bl->substr_of(old, 0, *length - back_copy);
    bl->append(tail);
    *length += back_pad;
    if (end >= o->onode.size && g_conf->bluestore_cache_tails) {
      o->tail_bl.clear();
      o->tail_bl.append(tail, 0, back_copy);
      o->tail_offset = end - back_copy;
      o->tail_txc_seq = txc->seq;
      dout(20) << __func__ << " cached "<< back_copy << " of tail block at "
	       << o->tail_offset << dendl;
    }
  }
  dout(20) << __func__ << " pad " << front_pad << " + " << back_pad
	   << " on front/back, now " << *offset << "~" << *length << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
}

void BlueStore::_pad_zeros_head(
  OnodeRef o,
  bufferlist *bl, uint64_t *offset, uint64_t *length,
  uint64_t block_size)
{
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  size_t front_pad = *offset % block_size;
  assert(front_pad);  // or we wouldn't have been called
  size_t front_copy = MIN(block_size - front_pad, *length);
  bufferptr z;
  if (front_copy + front_pad < block_size)
    z = buffer::create(front_copy + front_pad);
  else
    z = buffer::create_page_aligned(block_size);
  memset(z.c_str(), 0, front_pad);
  memcpy(z.c_str() + front_pad, bl->get_contiguous(0, front_copy), front_copy);
  bufferlist old, t;
  old.swap(*bl);
  bl->append(z);
  if (front_copy < *length) {
    t.substr_of(old, front_copy, *length - front_copy);
    bl->claim_append(t);
  }
  *offset -= front_pad;
  *length += front_pad;
  dout(20) << __func__ << " pad " << front_pad
	   << " on front, now " << *offset << "~" << *length << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
}

void BlueStore::_pad_zeros_tail(
  TransContext *txc,
  OnodeRef o,
  bufferlist *bl, uint64_t offset, uint64_t *length,
  uint64_t block_size)
{
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;

  // back
  uint64_t end = offset + *length;
  unsigned back_copy = end % block_size;
  assert(back_copy);  // or we wouldn't have been called
  uint64_t tail_len;
  if (back_copy <= *length) {
    // we start at or before the block boundary
    tail_len = block_size;
  } else {
    // we start partway into the tail block
    back_copy = *length;
    tail_len = block_size - (offset % block_size);
  }
  uint64_t back_pad = tail_len - back_copy;
  bufferptr tail(tail_len);
  memcpy(tail.c_str(), bl->get_contiguous(*length - back_copy, back_copy),
	 back_copy);
  memset(tail.c_str() + back_copy, 0, back_pad);
  bufferlist old;
  old.swap(*bl);
  bl->substr_of(old, 0, *length - back_copy);
  bl->append(tail);
  *length += back_pad;
  if (tail_len == block_size &&
      end >= o->onode.size && g_conf->bluestore_cache_tails) {
    o->tail_bl.clear();
    o->tail_bl.append(tail, 0, back_copy);
    o->tail_offset = end - back_copy;
    o->tail_txc_seq = txc->seq;
    dout(20) << __func__ << " cached "<< back_copy << " of tail block at "
	     << o->tail_offset << dendl;
  }
  dout(20) << __func__ << " pad " << back_pad
	   << " on back, now " << offset << "~" << *length << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
}

/*
 * Allocate extents for the given range.  In general, allocate new space
 * for any min_alloc_size blocks that we overwrite.  For the head/tail and/or
 * small writes that can be captured by overlay, or small writes that we will
 * WAL, do not bother.
 *
 * If we are doing WAL over a shared extent, allocate a new extent, and queue
 * WAL OP_COPY operations for any head/tail portions (rounded down/up to the
 * nearest block--the overwrite will to read/modify/write on the first or last
 * block as needed using the src_rmw_{head,tail} fields).
 */
int BlueStore::_do_allocate(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o,
  uint64_t orig_offset, uint64_t orig_length,
  uint32_t fadvise_flags,
  bool allow_overlay,
  uint64_t *alloc_offset,
  uint64_t *alloc_length,
  uint64_t *cow_head_extent,
  uint64_t *cow_tail_extent,
  uint64_t *cow_rmw_head,
  uint64_t *cow_rmw_tail)
{
  dout(20) << __func__
	   << " " << o->oid << " " << orig_offset << "~" << orig_length
	   << " - have " << o->onode.size
	   << " bytes in " << o->onode.extent_map.size()
	   << " lextents" << dendl;
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
  uint64_t block_size = bdev->get_block_size();
  uint64_t block_mask = ~(block_size - 1);

  // start with any full blocks we will write
  uint64_t offset = orig_offset;
  uint64_t length = orig_length;

  map<uint64_t, bluestore_lextent_t>::iterator bp;
  bool shared_head = false;
  bool shared_tail = false;
  uint64_t orig_end = orig_offset + orig_length;
  if (orig_offset / min_alloc_size == (orig_end - 1) / min_alloc_size &&
      (orig_length != min_alloc_size)) {
    // we fall within the same block
    offset = orig_offset - orig_offset % min_alloc_size;
    length = 0;
    assert(offset <= orig_offset);
    dout(20) << "  io falls within " << offset << "~" << min_alloc_size << dendl;
    if (allow_overlay && _can_overlay_write(o, orig_length)) {
      dout(20) << "  entire write will be captured by overlay" << dendl;
    } else {
      bp = o->onode.find_lextent(offset);
      if (bp == o->onode.extent_map.end()) {
	dout(20) << "  not yet allocated" << dendl;
	length = min_alloc_size;
      } else if (bp->second.is_shared()) {
	dout(20) << "  shared" << dendl;
	length = min_alloc_size;
	shared_head = shared_tail = true;
      } else {
	dout(20) << "  will presumably WAL" << dendl;
      }
    }
  } else {
    uint64_t head = 0;
    uint64_t tail = 0;
    if (offset % min_alloc_size) {
      head = min_alloc_size - (offset % min_alloc_size);
      offset += head;
      if (length >= head)
	length -= head;
    }
    if ((offset + length) % min_alloc_size) {
      tail = (offset + length) % min_alloc_size;
      if (length >= tail)
	length -= tail;
    }

    dout(20) << "  initial full " << offset << "~" << length
	     << ", head " << head << " tail " << tail << dendl;

    // include tail?
    if (tail) {
      if (allow_overlay && _can_overlay_write(o, tail)) {
	dout(20) << "  tail " << head << " will be captured by overlay" << dendl;
      } else {
	bp = o->onode.find_lextent(orig_offset + orig_length - 1);
	if (bp == o->onode.extent_map.end()) {
	  dout(20) << "  tail " << tail << " not yet allocated" << dendl;
	  length += min_alloc_size;
	} else if (bp->second.is_shared()) {
	  dout(20) << "  tail shared" << dendl;
	  length += min_alloc_size;
	  shared_tail = true;
	} else {
	  dout(20) << "  tail " << tail << " will presumably WAL" << dendl;
	}
      }
    }

    // include head?
    if (head) {
      if (allow_overlay && _can_overlay_write(o, head)) {
	dout(20) << "  head " << head << " will be captured by overlay" << dendl;
      } else {
        bp = o->onode.find_lextent(orig_offset);
        if (bp == o->onode.extent_map.end()) {
          dout(20) << "  head " << head << " not yet allocated" << dendl;
          offset -= min_alloc_size;
          length += min_alloc_size;
        } else if (bp->second.is_shared()) {
          dout(20) << "  head " << head << " shared" << dendl;
          offset -= min_alloc_size;
          length += min_alloc_size;
          shared_head = true;
        } else {
          dout(20) << "  head " << head << " will presumably WAL" << dendl;
        }
      }
    }
  }

  // COW head and/or tail?
  bluestore_wal_op_t *cow_head_op = nullptr;
  bluestore_wal_op_t *cow_tail_op = nullptr;
  if ((shared_head || shared_tail) && !o->enode)
    o->enode = c->get_enode(o->oid.hobj.get_hash());
  if (shared_head) {
    uint64_t cow_offset = offset;
    uint64_t cow_end = MIN(orig_offset & block_mask,
			   ROUND_UP_TO(o->onode.size, block_size));
    bp = o->onode.find_lextent(cow_offset);
    bluestore_blob_t *b = o->get_blob_ptr(bp->second.blob);
    if (cow_end > cow_offset) {
      uint64_t cow_length = cow_end - cow_offset;
      uint64_t x_off = cow_offset - bp->first;
      dout(20) << "  head shared, will COW "
	       << x_off << "~" << cow_length << " of " << bp->second
	       << " blob " << *b << dendl;
      while (cow_length > 0) {
	cow_head_op = _get_wal_op(txc, o);
	cow_head_op->op = bluestore_wal_op_t::OP_COPY;
	uint64_t c_len;
	cow_head_op->src_extent.offset =
	  b->calc_offset(x_off + bp->second.offset, &c_len);
	c_len = MIN(c_len, cow_length);
	cow_head_op->src_extent.length = c_len;
	cow_head_op->extent.offset = 0;   // will reset this below
	cow_head_op->extent.length = c_len;
	x_off += c_len;
	cow_length -= c_len;
	assert(cow_length == 0);  // fixme ... until we fix this loop
      }
    } else {
      dout(20) << "  head shared, but no COW needed" << dendl;
    }
    if (orig_offset & ~block_mask) {
      *cow_rmw_head = b->calc_offset(orig_offset - bp->first, nullptr);
      dout(20) << "  cow_rmw_head " << *cow_rmw_head
	       << " from " << bp->second
	       << " blob " << *b << dendl;
    }
  }
  if (shared_tail) {
    uint64_t cow_offset_raw = orig_offset + orig_length;
    uint64_t cow_offset = ROUND_UP_TO(cow_offset_raw, block_size);
    uint64_t cow_end_raw = MIN(offset + length, o->onode.size);
    uint64_t cow_end = ROUND_UP_TO(cow_end_raw, block_size);
    bp = o->onode.find_lextent(cow_offset_raw);
    bluestore_blob_t *b = o->get_blob_ptr(bp->second.blob);
    if (cow_end > cow_offset) {
      uint64_t cow_length = cow_end - cow_offset;
      uint64_t x_off = cow_offset - bp->first;
      dout(20) << "  tail shared, will COW "
	       << x_off << "~" << cow_length << " from " << bp->second
	       << " blob " << *b << dendl;
      while (cow_length > 0) {
	cow_tail_op = _get_wal_op(txc, o);
	cow_tail_op->op = bluestore_wal_op_t::OP_COPY;
	uint64_t c_len;
	cow_tail_op->src_extent.offset =
	  b->calc_offset(x_off + bp->second.offset, &c_len);
	c_len = MIN(c_len, cow_length);
	cow_tail_op->src_extent.length = c_len;
	// will adjust logical offset -> final bdev offset below
	cow_tail_op->extent.offset = cow_offset;
	cow_tail_op->extent.length = c_len;
	cow_offset += c_len;
	cow_length -= c_len;
	x_off += c_len;
	assert(cow_length == 0);  // fixme ... until we fix this loop
      }
    } else {
      dout(20) << "  tail shared, but no COW needed" << dendl;
    }
    if (cow_offset_raw < o->onode.size &&
	(cow_offset_raw & ~block_mask)) {
      *cow_rmw_tail = b->calc_offset(cow_offset_raw - bp->first, nullptr);
      dout(20) << "  cow_rmw_tail " << *cow_rmw_tail
	       << " from " << bp->second
	       << " blob " << *b << dendl;
    }
  }

  if (length) {
    dout(20) << "  must alloc " << offset << "~" << length << dendl;

    // positional hint
    uint64_t hint = 0;

    int r = alloc->reserve(length);
    if (r < 0) {
      derr << __func__ << " failed to reserve " << length << dendl;
      return r;
    }

    // deallocate existing extents
    bp = o->onode.seek_lextent(offset);
    while (bp != o->onode.extent_map.end() &&
	   bp->first < offset + length &&
	   bp->first + bp->second.length > offset) {
      bluestore_blob_t *b = o->get_blob_ptr(bp->second.blob);
      dout(30) << "   bp " << bp->first << ": " << bp->second
	       << " blob " << *b << dendl;
      if (bp->first < offset) {
	uint64_t left = offset - bp->first;
	if (bp->first + bp->second.length <= offset + length) {
	  dout(20) << "  trim tail " << bp->first << ": " << bp->second << dendl;
	  // fixme: deref part of blob
	  bp->second.length = left;
	  dout(20) << "        now " << bp->first << ": " << bp->second << dendl;
	  hint = bp->first + bp->second.length;
	  ++bp;
	} else {
	  dout(20) << "      split " << bp->first << ": " << bp->second << dendl;
	  // fixme: deref part of blob
	  o->onode.extent_map[offset + length] =
	    bluestore_lextent_t(
	      bp->second.blob,
	      bp->second.offset + left + length,
	      bp->second.length - (left + length),
	      bp->second.flags);
	  ++b->num_refs;
	  bp->second.length = left;
	  dout(20) << "       left " << bp->first << ": " << bp->second << dendl;
	  ++bp;
	  dout(20) << "      right " << bp->first << ": " << bp->second << dendl;
	  assert(bp->first == offset + length);
	  hint = bp->first + bp->second.length;
	}
      } else {
	assert(bp->first >= offset);
	if (bp->first + bp->second.length > offset + length) {
	  uint64_t overlap = offset + length - bp->first;
	  dout(20) << "  trim head " << bp->first << ": " << bp->second
		   << " (overlap " << overlap << ")" << dendl;
	  // fixme: deref part of blob
	  o->onode.extent_map[bp->first + overlap] =
	    bluestore_lextent_t(
	      bp->second.blob,
	      bp->second.offset + overlap,
	      bp->second.length - overlap,
	      bp->second.flags);
	  o->onode.extent_map.erase(bp++);
	  dout(20) << "        now " << bp->first << ": " << bp->second << dendl;
	  assert(bp->first == offset + length);
	  hint = bp->first;
	} else {
	  dout(20) << "    dealloc " << bp->first << ": " << bp->second
		   << " " << *b << dendl;
	  if (--b->num_refs == 0) {
	    for (auto& v : b->extents)
	      txc->released.insert(v.offset, v.length);
	    if (bp->second.blob >= 0)
	      o->onode.blob_map.erase(bp->second.blob);
	    else
	      o->enode->blob_map.erase(bp->second.blob);
	  }
	  hint = bp->first + bp->second.length;
	  o->onode.extent_map.erase(bp++);
	}
      }
    }

    *alloc_offset = offset;
    *alloc_length = length;

    // allocate our new extent(s). one blob for now. FIXME.
    int64_t blob = 0;
    bluestore_blob_t *b = o->onode.add_blob(&blob);
    uint64_t alloc_start = offset;
    while (length > 0) {
      bluestore_pextent_t e;
      uint32_t l;
      int r = alloc->allocate(length, min_alloc_size, hint,
			      &e.offset, &l);
      e.length = l;
      assert(r == 0);
      assert(e.length <= length);  // bc length is a multiple of min_alloc_size
      if (offset == alloc_start && cow_head_op) {
        // we set cow_head_extent to indicate that all or part of this
        // new extent will be copied from the previous allocation.
	*cow_head_extent = e.offset;
        cow_head_op->extent.offset = e.offset;
        dout(10) << __func__ << "  final head cow op extent "
                 << cow_head_op->extent << dendl;
      }
      if (e.length == length && cow_tail_op) {
        // we set cow_tail_extent to indicate that all or part of this
        // new extent will be copied from the previous allocation.
	*cow_tail_extent = e.offset;
        // extent.offset is the logical object offset
        assert(cow_tail_op->extent.offset >= offset);
        assert(cow_tail_op->extent.end() <= offset + length);
        cow_tail_op->extent.offset += e.offset - offset;
        dout(10) << __func__ << "  final tail cow op extent "
                 << cow_tail_op->extent << dendl;
      }
      txc->allocated.insert(e.offset, e.length);
      b->extents.push_back(e);
      offset += e.length;
      length -= e.length;
      hint = e.end();
    }
    bluestore_lextent_t& le =
      o->onode.extent_map[*alloc_offset] = bluestore_lextent_t(
	blob,
	0,
	*alloc_length,
	0);
    dout(10) << __func__ << "  alloc 0x" << std::hex << offset << std::dec
	     << ": " << le << dendl;
    b->length = *alloc_length;
    dout(10) << __func__ << "   blob " << blob << " " << *b << dendl;
  }

  return 0;
}

bool BlueStore::_can_overlay_write(OnodeRef o, uint64_t length)
{
  return
    (int)o->onode.overlay_map.size() < g_conf->bluestore_overlay_max &&
    (int)length <= g_conf->bluestore_overlay_max_length;
}

int BlueStore::_do_write(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o,
  uint64_t orig_offset,
  uint64_t orig_length,
  bufferlist& orig_bl,
  uint32_t fadvise_flags)
{
  int r = 0;

  dout(20) << __func__
	   << " " << o->oid << " " << orig_offset << "~" << orig_length
	   << " - have " << o->onode.size
	   << " bytes in " << o->onode.extent_map.size()
	   << " lextents" << dendl;
  _dump_onode(o);

  if (orig_length == 0) {
    return 0;
  }

  bool buffered = false;
  if (fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_WILLNEED) {
    dout(20) << __func__ << " will do buffered write" << dendl;
    buffered = true;
  }

  uint64_t block_size = bdev->get_block_size();
  const uint64_t block_mask = ~(block_size - 1);
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
  map<uint64_t, bluestore_lextent_t>::iterator bp;
  uint64_t length;
  uint64_t alloc_offset = 0, alloc_length = 0;
  uint64_t cow_head_extent = 0;
  uint64_t cow_tail_extent = 0;
  uint64_t cow_rmw_head = 0;
  uint64_t cow_rmw_tail = 0;

  if (orig_offset > o->onode.size) {
    // zero tail of previous existing extent?
    _do_zero_tail_extent(txc, c, o, orig_offset);
  }

  r = _do_allocate(txc, c, o, orig_offset, orig_length, fadvise_flags, true,
		   &alloc_offset, &alloc_length,
		   &cow_head_extent, &cow_tail_extent,
		   &cow_rmw_head, &cow_rmw_tail);
  if (r < 0) {
    derr << __func__ << " allocate failed, " << cpp_strerror(r) << dendl;
    goto out;
  }

  bp = o->onode.seek_lextent(orig_offset);

  for (uint64_t offset = orig_offset;
       offset < orig_offset + orig_length;
       offset += length) {
    // cut to extent
    length = orig_offset + orig_length - offset;
    if (bp == o->onode.extent_map.end() ||
	bp->first > offset) {
      // no allocation; crop at alloc boundary (this will be an overlay write)
      uint64_t end = ROUND_UP_TO(offset + 1, min_alloc_size);
      if (offset + length > end)
	length = end - offset;
    } else {
      // we are inside this extent; don't go past it
      if (bp->first + bp->second.length < offset + length) {
	assert(bp->first <= offset);
	length = bp->first + bp->second.length - offset;
      }
    }
    bufferlist bl;
    bl.substr_of(orig_bl, offset - orig_offset, length);
    if (bp == o->onode.extent_map.end())
      dout(20) << __func__ << "  chunk " << offset << "~" << length
	       << " (no extent)" << dendl;
    else
      dout(20) << __func__ << "  chunk " << offset << "~" << length
	       << " extent " << bp->first << ": " << bp->second << dendl;

    if (_can_overlay_write(o, length)) {
      r = _do_overlay_write(txc, o, offset, length, bl);
      if (r < 0)
	goto out;
      if (bp != o->onode.extent_map.end() &&
	  bp->first < offset + length)
	++bp;
      continue;
    }

    assert(bp != o->onode.extent_map.end());
    assert(offset >= bp->first);
    assert(offset + length <= bp->first + bp->second.length);

    bluestore_blob_t *b = c->get_blob_ptr(o, bp->second.blob);
    dout(20) << __func__ << "   blob " << *b << dendl;

    // overwrite unused portion of extent for an append?
    if (offset > bp->first &&
	offset >= o->onode.size &&                  // past eof +
	(o->onode.size & ~block_mask) == 0) {       // eof was aligned
      dout(20) << __func__ << " append after aligned eof" << dendl;
      _pad_zeros(txc, o, &bl, &offset, &length, block_size);
      assert(offset % block_size == 0);
      assert(length % block_size == 0);
      uint64_t x_off = offset - bp->first;
      if (bp->first >= alloc_offset &&
	  bp->first + bp->second.length <= alloc_offset + alloc_length &&
	  cow_head_extent != b->calc_offset(bp->second.offset, NULL)) {
	if (x_off > 0) {
	  // extent is unwritten; zero up until x_off
	  dout(20) << __func__ << " zero " << bp->second.offset << "~" << x_off
		   << " in blob" << dendl;
	  b->map(
	    0,
	    x_off,
	    [&](uint64_t offset, uint64_t length) {
	      bdev->aio_zero(offset, length, &txc->ioc);
	    });
	}
      } else {
	// the trailing block is zeroed from EOF to the end
	uint64_t from = MAX(ROUND_UP_TO(o->onode.size, block_size), bp->first);
	if (offset > from) {
	  uint64_t zx_off = from - bp->first;
	  uint64_t z_len = offset - from;
	  dout(20) << __func__ << " zero " << from << "~" << z_len
		   << " x_off " << zx_off << " in blob" << dendl;
	  b->map(
	    zx_off,
	    z_len,
	    [&](uint64_t offset, uint64_t length) {
	      bdev->aio_zero(offset, length, &txc->ioc);
	    });
	}
      }
      dout(20) << __func__ << " write " << offset << "~" << length
	       << " x_off " << x_off << " into blob " << *b << dendl;
      b->map_bl(
	x_off, bl,
	[&](uint64_t offset, uint64_t length, bufferlist& t) {
	  bdev->aio_write(offset, t, &txc->ioc, buffered);
	});
      ++bp;
      continue;
    }

    // use cached tail block?
    uint64_t tail_start = o->onode.size - o->onode.size % block_size;
    if (offset >= bp->first &&
	offset > tail_start &&
	offset + length >= o->onode.size &&
	o->tail_offset == tail_start &&
	o->tail_bl.length() &&
	(offset / block_size == (o->onode.size - 1) / block_size)) {
      dout(20) << __func__ << " using cached tail" << dendl;
      assert((offset & block_mask) == (o->onode.size & block_mask));
      // wait for any related wal writes to commit
      txc->osr->wait_for_wal_on_seq(o->tail_txc_seq);
      uint64_t tail_off = offset % block_size;
      if (tail_off >= o->tail_bl.length()) {
	bufferlist t;
	t = o->tail_bl;
	if (tail_off > t.length()) {
	  bufferptr z(tail_off - t.length());
	  z.zero();
	  t.append(z);
	}
	offset -= t.length();
	length += t.length();
	t.claim_append(bl);
	bl.swap(t);
      } else {
	bufferlist t;
	t.substr_of(o->tail_bl, 0, tail_off);
	offset -= t.length();
	length += t.length();
	t.claim_append(bl);
	bl.swap(t);
      }
      assert(offset == tail_start);
      assert((bp->first + bp->second.length <= alloc_offset ||
	      bp->first >= alloc_offset + alloc_length) ||
	     cow_head_extent == b->calc_offset(bp->second.offset, NULL) ||
	     offset == bp->first);
      _pad_zeros(txc, o, &bl, &offset, &length, block_size);
      uint64_t x_off = offset - bp->first;
      dout(20) << __func__ << " write " << offset << "~" << length
	       << " x_off " << x_off << " into blob " << *b << dendl;
      b->map_bl(x_off, bl,
		[&](uint64_t offset, uint64_t length, bufferlist& t) {
		  bdev->aio_write(offset, t,
				  &txc->ioc, buffered);
		});
      ++bp;
      continue;
    }

    if (offset + length > (o->onode.size & block_mask) &&
	o->tail_bl.length()) {
      dout(20) << __func__ << " clearing cached tail" << dendl;
      o->clear_tail();
    }

    if (offset % min_alloc_size == 0 &&
	length % min_alloc_size == 0) {
      assert(bp->first >= alloc_offset &&
	     bp->first + bp->second.length <= alloc_offset + alloc_length);
    }

    if (bp->first >= alloc_offset &&
	bp->first + bp->second.length <= alloc_offset + alloc_length) {
      // NOTE: we may need to zero before or after our write if the
      // prior extent wasn't allocated but we are still doing some COW.
      uint64_t z_end = offset & block_mask;
      if (z_end > bp->first &&
	  cow_head_extent != b->calc_offset(bp->second.offset, NULL)) {
	uint64_t z_off = bp->second.offset;
	uint64_t z_len = z_end - bp->first;
	dout(20) << __func__ << " zero 0x" << std::hex << z_off << "~0x"
		 << z_len << std::dec << dendl;
	b->map(z_off, z_len,
	       [&](uint64_t offset, uint64_t length) {
		 bdev->aio_zero(offset, length, &txc->ioc);
	       });
      }
      uint64_t end = ROUND_UP_TO(offset + length, block_size);
      if (end < bp->first + bp->second.length &&
	  end <= o->onode.size &&
	  cow_tail_extent != b->calc_offset(bp->second.offset, NULL)) {
	uint64_t z_len = bp->first + bp->second.length - end;
	uint64_t z_off = end - bp->first + bp->second.offset;
	dout(20) << __func__ << " zero 0x" << std::hex << z_off << "~0x" << z_len
		 << std::dec << dendl;
	b->map(z_off, z_len,
	       [&](uint64_t offset, uint64_t length) {
		 bdev->aio_zero(offset, length, &txc->ioc);
	       });
      }
      if ((offset & ~block_mask) != 0 && !cow_rmw_head) {
	_pad_zeros_head(o, &bl, &offset, &length, block_size);
      }
      if (((offset + length) & ~block_mask) != 0 && !cow_rmw_tail) {
	_pad_zeros_tail(txc, o, &bl, offset, &length, block_size);
      }
      if ((offset & ~block_mask) == 0 && (length & ~block_mask) == 0) {
	uint64_t x_off = offset - bp->first;
	dout(20) << __func__ << " write " << offset << "~" << length
		 << " x_off " << x_off << " into blob " << *b << dendl;
	_do_overlay_trim(txc, o, offset, length);
	b->map_bl(x_off, bl,
		  [&](uint64_t offset, uint64_t length, bufferlist& t) {
		    bdev->aio_write(offset, t, &txc->ioc, buffered);
		  });
	++bp;
	continue;
      }
    }

    // WAL.
    r = _do_write_overlays(txc, c, o, bp->first, bp->second.length);
    if (r < 0)
      goto out;
    assert(bp->first <= offset);
    assert(offset + length <= bp->first + bp->second.length);
    bool is_orig_offset = offset == orig_offset;
    if (offset > o->onode.size &&
	o->onode.size > bp->first) {
      uint64_t zlen = offset - o->onode.size;
      dout(20) << __func__ << " padding " << zlen << " zeroes from eof "
	       << o->onode.size << " to " << offset << dendl;
      bufferlist z;
      z.append_zero(zlen);
      z.claim_append(bl);
      bl.swap(z);
      offset -= zlen;
      length += zlen;
      if (cow_rmw_head)
	cow_rmw_head -= zlen;
    }
    b->map_bl(
      offset - bp->first + bp->second.offset, bl,
      [&](uint64_t dev_offset, uint64_t dev_length, bufferlist& t) {
	bluestore_wal_op_t *op = _get_wal_op(txc, o);
	op->op = bluestore_wal_op_t::OP_WRITE;
	if (is_orig_offset && cow_rmw_head) {
	  op->src_rmw_head = cow_rmw_head;
	  dout(20) << __func__ << " src_rmw_head " << op->src_rmw_head << dendl;
	}
	if (offset + dev_length == orig_offset + orig_length && cow_rmw_tail) {
	  op->src_rmw_tail = cow_rmw_tail;
	  dout(20) << __func__ << " src_rmw_tail " << op->src_rmw_tail << dendl;
	} else if (((offset + dev_length) & ~block_mask) &&
		   offset + dev_length > o->onode.size) {
	  dout(20) << __func__ << " past eof, padding out tail block" << dendl;
	  _pad_zeros_tail(txc, o, &bl, offset, &length, block_size);
	}
	op->extent.offset = dev_offset;
	op->extent.length = dev_length;
	op->data = t;
	dout(20) << __func__ << " wal write "
		 << offset << "~" << dev_length << " to " << op->extent
		 << dendl;
	offset += dev_length;
	length -= dev_length;
      });
    ++bp;
    continue;
  }
  r = 0;

  if (orig_offset + orig_length > o->onode.size) {
    dout(20) << __func__ << " extending size to " << orig_offset + orig_length
	     << dendl;
    o->onode.size = orig_offset + orig_length;
  }

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
	   << " " << offset << "~" << length
	   << dendl;
  o->exists = true;
  _assign_nid(txc, o);
  int r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_write_zero(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef o,
  uint64_t offset,
  uint64_t length)
{
  bufferlist zl;
  zl.append_zero(length);
  uint64_t old_size = o->onode.size;
  int r = _do_write(txc, c, o, offset, length, zl, 0);
  // we do not modify onode size
  o->onode.size = old_size;
  return r;
}

int BlueStore::_zero(TransContext *txc,
		     CollectionRef& c,
		     OnodeRef& o,
		     uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << dendl;
  int r = _do_zero(txc, c, o, offset, length);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

void BlueStore::_do_zero_tail_extent(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset)
{
  const uint64_t block_size = bdev->get_block_size();
  const uint64_t block_mask = ~(block_size - 1);

  map<uint64_t, bluestore_lextent_t>::iterator bp, pp;
  bp = o->onode.seek_lextent(offset);
  pp = o->onode.find_lextent(o->onode.size);

  dout(10) << __func__ << " offset " << offset << " extent "
	   << pp->first << ": " << pp->second << dendl;
  assert(offset > o->onode.size);

  // we assume the caller will handle any partial block they start with
  offset &= block_mask;
  if (offset <= o->onode.size)
    return;

  if (pp != o->onode.extent_map.end() &&
      pp != bp) {
    if (pp->second.is_shared()) {
      dout(10) << __func__ << " shared tail lextent; doing _do_write_zero"
	       << dendl;
      uint64_t old_size = o->onode.size;
      uint64_t end = pp->first + pp->second.length;
      uint64_t zlen = end - old_size;
      _do_write_zero(txc, c, o, old_size, zlen);
    } else {
      uint64_t end_block = ROUND_UP_TO(o->onode.size, block_size);

      if (end_block > o->onode.size) {
	// end was in a partial block, do wal r/m/w.
	bluestore_blob_t *b = o->get_blob_ptr(pp->second.blob);
	bluestore_wal_op_t *op = _get_wal_op(txc, o);
	op->op = bluestore_wal_op_t::OP_ZERO;
	uint64_t x_off = o->onode.size;
	uint64_t x_len = end_block - x_off;
	op->extent.offset = b->calc_offset(x_off - pp->first, NULL);
	op->extent.length = x_len;
	dout(10) << __func__ << " wal zero tail partial block "
		 << x_off << "~" << x_len
		 << " blob " << *b
		 << " at " << op->extent
		 << dendl;
      }
      if (offset > end_block) {
	// end was block-aligned.  zero the rest of the extent now.
	uint64_t x_off = end_block - pp->first;
	uint64_t x_len = pp->second.length - x_off;
	bluestore_blob_t *b = o->get_blob_ptr(pp->second.blob);
	if (x_len > 0) {
	  dout(10) << __func__ << " zero tail " << x_off << "~" << x_len
		   << " of tail extent " << pp->first << ": " << pp->second
		   << " blob " << *b
		   << dendl;
	  b->map(x_off, x_len,
		 [&](uint64_t offset, uint64_t length) {
		   bdev->aio_zero(offset, length, &txc->ioc);
		 });
	}
      }
    }
  }
}

int BlueStore::_do_zero(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << dendl;
  int r = 0;
  o->exists = true;

  _dump_onode(o);
  _assign_nid(txc, o);

  if (offset > o->onode.size) {
    // we are past eof; just truncate up.
    return _do_truncate(txc, c, o, offset + length);
  }

  // overlay
  _do_overlay_trim(txc, o, offset, length);

  uint64_t block_size = bdev->get_block_size();

  map<uint64_t,bluestore_lextent_t>::iterator bp = o->onode.seek_lextent(offset);
  while (bp != o->onode.extent_map.end()) {
    if (bp->first >= offset + length)
      break;

    bluestore_blob_t *b = c->get_blob_ptr(o, bp->second.blob);

    if (offset <= bp->first &&
	(offset + length >= bp->first + bp->second.length ||
	 offset >= o->onode.size)) {
      // remove fragment
      dout(20) << __func__ << " dealloc " << bp->first << ": "
	       << bp->second << " blob " << *b << dendl;
      if (--b->num_refs == 0) {
	for (auto& v : b->extents)
	  txc->released.insert(v.offset, v.length);
	if (bp->second.blob >= 0)
	  o->onode.blob_map.erase(bp->second.blob);
	else
	  o->enode->blob_map.erase(-bp->second.blob);
      }
      o->onode.extent_map.erase(bp++);
      continue;
    }

    // start,end are offsets in the extent
    uint64_t x_off = 0;
    if (offset > bp->first) {
      if (offset > o->onode.size &&
	  o->onode.size >= bp->first) {
	uint64_t zlen = offset - o->onode.size;
	dout(10) << __func__ << " extending range by " << zlen
		 << " to start from eof " << o->onode.size << dendl;
	offset -= zlen;
	length += zlen;
      }
      x_off = offset - bp->first;
    }
    uint64_t x_len = MIN(offset + length - bp->first,
			 bp->second.length) - x_off;
    if (bp->second.is_shared()) {
      uint64_t end = bp->first + x_off + x_len;
      _do_write_zero(txc, c, o, bp->first + x_off, x_len);
      // we probably invalidated bp.  move past the extent we just
      // reallocated.
      bp = o->onode.seek_lextent(end - 1);
    } else {
      // WAL
      uint64_t end = bp->first + x_off + x_len;
      if (end >= o->onode.size && end % block_size) {
	dout(20) << __func__ << " past eof, padding out tail block" << dendl;
	x_len += block_size - (end % block_size);
      }
      dout(20) << __func__ << "  wal zero 0x" << std::hex
	       << x_off + bp->second.offset << "~0x"
	       << x_len << std::dec << " blob " << *b << dendl;
      b->map(x_off + bp->second.offset, x_len,
	     [&](uint64_t offset, uint64_t length) {
	       bluestore_wal_op_t *op = _get_wal_op(txc, o);
	       op->op = bluestore_wal_op_t::OP_ZERO;
	       op->extent.offset = offset;
	       op->extent.length = length;
	     });
    }
    ++bp;
  }

  if (offset + length > o->onode.size) {
    o->onode.size = offset + length;
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
  }
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_truncate(
  TransContext *txc, CollectionRef& c, OnodeRef o, uint64_t offset)
{
  uint64_t block_size = bdev->get_block_size();
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
  uint64_t alloc_end = ROUND_UP_TO(offset, min_alloc_size);

  // ensure any wal IO has completed before we truncate off any extents
  // they may touch.
  o->flush();

  // trim down cached tail
  if (o->tail_bl.length()) {
    // we could adjust this if we truncate down within the same
    // block...
    dout(20) << __func__ << " clear cached tail" << dendl;
    o->clear_tail();
  }

  // trim down fragments
  map<uint64_t,bluestore_lextent_t>::iterator bp = o->onode.extent_map.end();
  if (bp != o->onode.extent_map.begin())
    --bp;
  while (bp != o->onode.extent_map.end()) {
    if (bp->first + bp->second.length <= alloc_end) {
      break;
    }

    bluestore_blob_t *b = c->get_blob_ptr(o, bp->second.blob);

    if (bp->first >= alloc_end) {
      dout(20) << __func__ << " dealloc " << bp->first << ": "
	       << bp->second << " blob " << *b << dendl;
      if (--b->num_refs == 0) {
	for (auto& v : b->extents)
	  txc->released.insert(v.offset, v.length);
	if (bp->second.blob >= 0)
	  o->onode.blob_map.erase(bp->second.blob);
	else
	  o->enode->blob_map.erase(-bp->second.blob);
      }
      if (bp != o->onode.extent_map.begin()) {
	o->onode.extent_map.erase(bp--);
	continue;
      } else {
	o->onode.extent_map.erase(bp);
	break;
      }
    } else {
      assert(bp->first + bp->second.length > alloc_end);
      assert(bp->first < alloc_end);
      uint64_t newlen = alloc_end - bp->first;
      assert(newlen % min_alloc_size == 0);
      dout(20) << __func__ << " trunc " << bp->first << ": " << bp->second
	       << " to " << newlen << dendl;
      // fixme: prune blob
      bp->second.length = newlen;
      break;
    }
  }

  // adjust size now, in case we need to call _do_write_zero below.
  uint64_t old_size = o->onode.size;
  o->onode.size = offset;

  // zero extent if trimming up?
  if (offset > old_size) {
    map<uint64_t,bluestore_lextent_t>::iterator bp = o->onode.extent_map.end();
    if (bp != o->onode.extent_map.begin())
      --bp;
    uint64_t x_end = bp->first + bp->second.length;
    if (bp != o->onode.extent_map.end() &&
	x_end > old_size) {
      // we need to zero from old_size to (end of extent or offset)
      assert(offset > bp->first);    // else we would have trimmed it above
      assert(old_size > bp->first);  // we do no preallocation (yet)
      uint64_t x_off = old_size - bp->first + bp->second.offset;
      uint64_t x_len = MIN(ROUND_UP_TO(offset, block_size), x_end) - old_size;
      if (bp->second.is_shared()) {
        int r = _do_write_zero(txc, c, o, old_size, x_len);
        if (r < 0)
          return r;
      } else {
	bluestore_blob_t *b = o->get_blob_ptr(bp->second.blob);
	dout(20) << __func__ << "  wal zero " << x_off << "~" << x_len
		 << " in blob " << *b << dendl;
	b->map(
	  x_off, x_len,
	  [&](uint64_t offset, uint64_t length) {
	    bluestore_wal_op_t *op = _get_wal_op(txc, o);
	    op->op = bluestore_wal_op_t::OP_ZERO;
	    op->extent.offset = offset;
	    op->extent.length = length;
	    dout(20) << "  wal zero 0x" << op->extent << dendl;
	  });
      }
    }
  }

  // trim down overlays
  map<uint64_t,bluestore_overlay_t>::iterator op = o->onode.overlay_map.end();
  if (op != o->onode.overlay_map.begin())
    --op;
  while (op != o->onode.overlay_map.end()) {
    if (op->first + op->second.length <= offset) {
      break;
    }
    if (op->first >= offset) {
      if (o->onode.put_overlay_ref(op->second.key)) {
	dout(20) << __func__ << " rm overlay " << op->first << " "
		 << op->second << dendl;
	string key;
	get_overlay_key(o->onode.nid, op->second.key, &key);
	txc->t->rm_single_key(PREFIX_OVERLAY, key);
      } else {
	dout(20) << __func__ << " rm overlay " << op->first << " "
		 << op->second << " (put ref)" << dendl;
      }
      if (op != o->onode.overlay_map.begin()) {
	o->onode.overlay_map.erase(op--);
	continue;
      } else {
	o->onode.overlay_map.erase(op);
	break;
      }
    } else {
      assert(op->first + op->second.length > offset);
      assert(op->first < offset);
      uint64_t newlen = offset - op->first;
      dout(20) << __func__ << " truncate overlay " << op->first << " "
	       << op->second << " to " << newlen << dendl;
      bluestore_overlay_t& ov = op->second;
      ov.length = newlen;
      break;
    }
  }

  txc->write_onode(o);
  return 0;
}

int BlueStore::_truncate(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset
	   << dendl;
  int r = _do_truncate(txc, c, o, offset);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset
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

int BlueStore::_setallochint(TransContext *txc,
			     CollectionRef& c,
			     OnodeRef& o,
			     uint64_t expected_object_size,
			     uint64_t expected_write_size)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << dendl;
  int r = 0;
  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
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
      EnodeRef e = c->get_enode(newo->oid.hobj.get_hash());
      map<int64_t,int64_t> moved_blobs;
      for (auto& p : oldo->onode.extent_map) {
	if (p.second.is_shared()) {
	  e->blob_map[-p.second.blob].num_refs++;
	} else if (moved_blobs.count(p.second.blob)) {
	  e->blob_map[moved_blobs[p.second.blob]].num_refs++;
	} else {
	  int64_t id = e->get_new_blob_id();
	  moved_blobs[p.second.blob] = id;
	  e->blob_map[id] = oldo->onode.blob_map[p.second.blob];
	  e->blob_map[id].num_refs++;
	  oldo->onode.blob_map.erase(p.second.blob);
	}
      }
      if (!moved_blobs.empty()) {
	for (auto& p : oldo->onode.extent_map) {
	  if (moved_blobs.count(p.second.blob)) {
	    p.second.blob = -moved_blobs[p.second.blob];
	  }
	}
      }
      newo->onode.extent_map = oldo->onode.extent_map;
      newo->enode = e;
      dout(20) << __func__ << " extent_map " << newo->onode.extent_map << dendl;
      dout(20) << __func__ << " blob_map " << e->blob_map << dendl;
      txc->write_enode(e);
      if (!moved_blobs.empty())
	txc->write_onode(oldo);
    }

    //don't care _can_overlay_write()
    for (auto& v : oldo->onode.overlay_map) {
      string key;
      bufferlist val;
      get_overlay_key(oldo->onode.nid, v.second.key, &key);
      int r  = db->get(PREFIX_OVERLAY, key, &val);
      if (r < 0) {
	derr << __func__ << " get oid " << oldo->oid << " overlay value(key=" << v.second.key
	  << ")" << "failed: " << cpp_strerror(r) << dendl;
	goto out;
      }

      newo->onode.overlay_map[v.first] = bluestore_overlay_t(++newo->onode.last_overlay_key, 0, v.second.length);
      dout(20) << __func__ << " added " << v.first << " " << v.second.length << dendl;
      key.clear();
      get_overlay_key(newo->onode.nid, newo->onode.last_overlay_key, &key);
      txc->t->set(PREFIX_OVERLAY, key, val);
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
	   << newo->oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff << dendl;
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
	   << newo->oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff
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
    c->reset(new Collection(this, cid));
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
    pair<ghobject_t,OnodeRef> next;
    while ((*c)->onode_map.get_next(next.first, &next)) {
      if (next.second->exists) {
	dout(10) << __func__ << " " << next.first << " " << next.second
		 << " exists in onode_map" << dendl;
	r = -ENOTEMPTY;
	goto out;
      }
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
