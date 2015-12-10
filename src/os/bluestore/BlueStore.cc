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

#define dout_subsys ceph_subsys_bluestore

/*

  TODO:

  * superblock, features
  * statfs reports on block device only
  * bdev: smarter zeroing
  * zero overlay in onode?
  * discard
  * aio read?
  * read uses local ioc
  * refcounted extents (for efficient clone)
  * overlay does inefficient zeroing on unwritten extent

 */

/*
 * Some invariants:
 *
 * - If the end of the object is a partial block, and is not an overlay,
 *   the remainder of that block will always be zeroed.  (It has to be written
 *   anyway, so we may as well have written zeros.)
 *
 */

const string PREFIX_SUPER = "S"; // field -> value
const string PREFIX_COLL = "C"; // collection name -> (nothing)
const string PREFIX_OBJ = "O";  // object name -> onode
const string PREFIX_OVERLAY = "V"; // u64 + offset -> value
const string PREFIX_OMAP = "M"; // u64 + keyname -> value
const string PREFIX_WAL = "L";  // write ahead log
const string PREFIX_ALLOC = "B";  // block allocator

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
 * ghobject_t does.  We do this by escaping anything <= to '%' with %
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < %
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
string pretty_binary_string(const string& in)
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

  p = _key_decode_shard(p, &oid->shard_id);
  if (!p)
    return -2;

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  if (!p)
    return -3;
  oid->hobj.pool = pool - 0x8000000000000000;

  unsigned hash;
  p = _key_decode_u32(p, &hash);
  if (!p)
    return -4;
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
      return -8;
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
  } else {
    // malformed
    return -7;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  if (!p)
    return -10;
  p = _key_decode_u64(p, &oid->generation);
  if (!p)
    return -11;
  return 0;
}


void get_overlay_key(uint64_t nid, uint64_t offset, string *out)
{
  _key_encode_u64(nid, out);
  _key_encode_u64(offset, out);
}

// '-' < '.' < '~'
void get_omap_header(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('-');
}

// hmm, I don't think there's any need to escape the user key since we
// have a clean prefix.
void get_omap_key(uint64_t id, const string& key, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('.');
  out->append(key);
}

void rewrite_omap_key(uint64_t id, string old, string *out)
{
  _key_encode_u64(id, out);
  out->append(old.substr(out->length()));
}

void decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(sizeof(uint64_t) + 1);
}

void get_omap_tail(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('~');
}

void get_wal_key(uint64_t seq, string *out)
{
  _key_encode_u64(seq, out);
}


// Onode

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.onode(" << this << ") "

BlueStore::Onode::Onode(const ghobject_t& o, const string& k)
  : nref(0),
    oid(o),
    key(k),
    dirty(false),
    exists(true),
    flush_lock("BlueStore::Onode::flush_lock") {
}

void BlueStore::Onode::flush()
{
  Mutex::Locker l(flush_lock);
  dout(20) << __func__ << " " << flush_txns << dendl;
  while (!flush_txns.empty())
    flush_cond.Wait(flush_lock);
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
  Mutex::Locker l(lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  lru.push_back(*o);
}

BlueStore::OnodeRef BlueStore::OnodeHashLRU::lookup(const ghobject_t& oid)
{
  Mutex::Locker l(lock);
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
  Mutex::Locker l(lock);
  dout(10) << __func__ << dendl;
  lru.clear();
  onode_map.clear();
}

void BlueStore::OnodeHashLRU::remove(const ghobject_t& oid)
{
  Mutex::Locker l(lock);
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
  if (p == onode_map.end()) {
    dout(30) << __func__ << " " << oid << " miss" << dendl;
    return;
  }
  dout(30) << __func__ << " " << oid << " hit " << p->second << dendl;
  lru_list_t::iterator pi = lru.iterator_to(*p->second);
  lru.erase(pi);
  onode_map.erase(p);
}

void BlueStore::OnodeHashLRU::rename(const ghobject_t& old_oid,
				    const ghobject_t& new_oid)
{
  Mutex::Locker l(lock);
  dout(30) << __func__ << " " << old_oid << " -> " << new_oid << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);

  assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    lru_list_t::iterator p = lru.iterator_to(*pn->second);
    lru.erase(p);
    onode_map.erase(pn);
  }
  onode_map.insert(make_pair(new_oid, po->second));
  _touch(po->second);
  onode_map.erase(po);
}

bool BlueStore::OnodeHashLRU::get_next(
  const ghobject_t& after,
  pair<ghobject_t,OnodeRef> *next)
{
  Mutex::Locker l(lock);
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
  Mutex::Locker l(lock);
  dout(20) << __func__ << " max " << max
	   << " size " << onode_map.size() << dendl;
  int trimmed = 0;
  int num = onode_map.size() - max;
  lru_list_t::iterator p = lru.end();
  if (num)
    --p;
  while (num > 0) {
    Onode *o = &*p;
    int refs = o->nref.read();
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
    lock("BlueStore::Collection::lock"),
    onode_map()
{
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
    if (!create)
      return OnodeRef();

    // new
    on = new Onode(oid, key);
    on->dirty = true;
  } else {
    // loaded
    assert(r >=0);
    on = new Onode(oid, key);
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


void aio_cb(void *priv, void *priv2)
{
  BlueStore *store = static_cast<BlueStore*>(priv);
  store->_txc_aio_finish(priv2);
}

BlueStore::BlueStore(CephContext *cct, const string& path)
  : ObjectStore(path),
    cct(cct),
    db(NULL),
    fs(NULL),
    bdev(NULL),
    fm(NULL),
    alloc(NULL),
    path_fd(-1),
    fsid_fd(-1),
    mounted(false),
    coll_lock("BlueStore::coll_lock"),
    nid_lock("BlueStore::nid_lock"),
    nid_max(0),
    throttle_ops(cct, "bluestore_max_ops", cct->_conf->bluestore_max_ops),
    throttle_bytes(cct, "bluestore_max_bytes", cct->_conf->bluestore_max_bytes),
    throttle_wal_ops(cct, "bluestore_wal_max_ops",
		     cct->_conf->bluestore_max_ops +
		     cct->_conf->bluestore_wal_max_ops),
    throttle_wal_bytes(cct, "bluestore_wal_max_bytes",
		       cct->_conf->bluestore_max_bytes +
		       cct->_conf->bluestore_wal_max_bytes),
    wal_lock("BlueStore::wal_lock"),
    wal_seq(0),
    wal_tp(cct,
	   "BlueStore::wal_tp",
	   cct->_conf->bluestore_wal_threads,
	   "bluestore_wal_threads"),
    wal_wq(this,
	     cct->_conf->bluestore_wal_thread_timeout,
	     cct->_conf->bluestore_wal_thread_suicide_timeout,
	     &wal_tp),
    finisher(cct),
    kv_sync_thread(this),
    kv_lock("BlueStore::kv_lock"),
    kv_stop(false),
    logger(NULL),
    reap_lock("BlueStore::reap_lock")
{
  _init_logger();
}

BlueStore::~BlueStore()
{
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(fsid_fd < 0);
}

void BlueStore::_init_logger()
{
  // XXX
}

void BlueStore::_shutdown_logger()
{
  // XXX
}

int BlueStore::peek_journal_fsid(uuid_d *fsid)
{
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
  assert(fs == NULL);
  fs = FS::create(path_fd);
  dout(1) << __func__ << " using fs driver '" << fs->get_name() << "'" << dendl;
  return 0;
}

void BlueStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
  delete fs;
  fs = NULL;
}

int BlueStore::_open_bdev()
{
  assert(bdev == NULL);
  bdev = new BlockDevice(aio_cb, static_cast<void*>(this));
  string p = path + "/block";
  int r = bdev->open(p);
  if (r < 0) {
    delete bdev;
    bdev = NULL;
  }
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
  const map<uint64_t,uint64_t>& fl = fm->get_freelist();
  for (auto p : fl) {
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
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret > 36)
    fsid_str[36] = 0;
  else
    fsid_str[ret] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
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
    ret = true; // if we can't lock, it is in used
  _close_fsid();
 out_path:
  _close_path();
  return ret;
}

int BlueStore::_open_db(bool create)
{
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());
  if (create) {
    int r = ::mkdir(fn, 0755);
    if (r < 0)
      r = -errno;
    if (r < 0 && r != -EEXIST) {
      derr << __func__ << " failed to create " << fn << ": " << cpp_strerror(r)
	   << dendl;
      return r;
    }

    // wal_dir, too!
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
  db = KeyValueDB::create(g_ceph_context,
			  g_conf->bluestore_backend,
			  fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  string options;
  if (g_conf->bluestore_backend == "rocksdb")
    options = g_conf->bluestore_rocksdb_options;
  db->init(options);
  stringstream err;
  int r;
  if (create)
    r = db->create_and_open(err);
  else
    r = db->open(err);
  if (r) {
    derr << __func__ << " erroring opening db: " << err.str() << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  dout(1) << __func__ << " opened " << g_conf->bluestore_backend
	  << " path " << fn << " options " << options << dendl;

  if (create) {
    // blow it away
    dout(1) << __func__ << " wiping by prefix" << dendl;
    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkeys_by_prefix(PREFIX_SUPER);
    t->rmkeys_by_prefix(PREFIX_COLL);
    t->rmkeys_by_prefix(PREFIX_OBJ);
    t->rmkeys_by_prefix(PREFIX_OVERLAY);
    t->rmkeys_by_prefix(PREFIX_OMAP);
    t->rmkeys_by_prefix(PREFIX_WAL);
    t->rmkeys_by_prefix(PREFIX_ALLOC);
    db->submit_transaction_sync(t);
  }
  return 0;
}

void BlueStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

int BlueStore::_open_collections(int *errors)
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL);
  for (it->upper_bound(string());
       it->valid();
       it->next()) {
    coll_t cid;
    if (cid.parse(it->key())) {
      CollectionRef c(new Collection(this, cid));
      bufferlist bl;
      db->get(PREFIX_COLL, it->key(), &bl);
      bufferlist::iterator p = bl.begin();
      ::decode(c->cnode, p);
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

int BlueStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;

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
  if (r < 0 && old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    r = _write_fsid();
    if (r < 0)
      goto out_close_fsid;
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
	   << " != provided " << fsid << dendl;
      r = -EINVAL;
      goto out_close_fsid;
    }
    fsid = old_fsid;
    dout(1) << __func__ << " fsid is already set to " << fsid << dendl;
  }

  // block device
  if (g_conf->bluestore_block_path.length()) {
    int r = ::symlinkat(g_conf->bluestore_block_path.c_str(), path_fd, "block");
    if (r < 0) {
      r = -errno;
      derr << __func__ << " failed to create block symlink to "
	   << g_conf->bluestore_block_path << ": " << cpp_strerror(r) << dendl;
      goto out_close_fsid;
    }
  } else if (g_conf->bluestore_block_size) {
    struct stat st;
    int r = ::fstatat(path_fd, "block", &st, 0);
    if (r < 0)
      r = -errno;
    if (r == -ENOENT) {
      int fd = ::openat(path_fd, "block", O_CREAT|O_RDWR, 0644);
      if (fd < 0) {
	int r = -errno;
	derr << __func__ << " faile to create block file: " << cpp_strerror(r)
	     << dendl;
	goto out_close_fsid;
      }
      int r = ::ftruncate(fd, g_conf->bluestore_block_size);
      assert(r == 0);
      dout(1) << __func__ << " created block file with size "
	      << pretty_si_t(g_conf->bluestore_block_size) << "B" << dendl;
    }
  }

  r = _open_bdev();
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
    fm->release(0, bdev->get_size(), t);
    db->submit_transaction_sync(t);
  }

  // FIXME: superblock

  dout(10) << __func__ << " success" << dendl;
  r = 0;

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

  if (g_conf->bluestore_fsck_on_mount) {
    int rc = fsck();
    if (rc < 0)
      return rc;
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

  r = _open_bdev();
  if (r < 0)
    goto out_fsid;

  r = _open_db(false);
  if (r < 0)
    goto out_bdev;

  r = _open_alloc();
  if (r < 0)
    goto out_db;

  r = _recover_next_nid();
  if (r < 0)
    goto out_alloc;

  r = _open_collections();
  if (r < 0)
    goto out_alloc;

  finisher.start();
  wal_tp.start();
  kv_sync_thread.create();

  r = _wal_replay();
  if (r < 0)
    goto out_stop;

  mounted = true;
  return 0;

 out_stop:
  _kv_stop();
  wal_tp.stop();
  finisher.wait_for_empty();
  finisher.stop();
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
  return 0;
}

int BlueStore::fsck()
{
  dout(1) << __func__ << dendl;
  int errors = 0;
  set<uint64_t> used_nids;
  set<uint64_t> used_omap_head;
  interval_set<uint64_t> used_blocks;
  KeyValueDB::Iterator it;

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

  r = _open_bdev();
  if (r < 0)
    goto out_fsid;

  r = _open_db(false);
  if (r < 0)
    goto out_bdev;

  r = _open_alloc();
  if (r < 0)
    goto out_db;

  r = _open_collections(&errors);
  if (r < 0)
    goto out_alloc;

  // walk collections, objects
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end() && !errors;
       ++p) {
    dout(1) << __func__ << " collection " << p->first << dendl;
    CollectionRef c = _get_collection(p->first);
    RWLock::RLocker l(c->lock);
    ghobject_t pos;
    while (!errors) {
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
      for (auto oid : ols) {
	dout(10) << __func__ << "  " << oid << dendl;
	OnodeRef o = c->get_onode(oid, false);
	if (!o || !o->exists) {
	  ++errors;
	  break;
	}
	if (o->onode.nid) {
	  if (used_nids.count(o->onode.nid)) {
	    derr << " " << oid << " nid " << o->onode.nid << " already in use"
		 << dendl;
	    ++errors;
	    break;
	  }
	  used_nids.insert(o->onode.nid);
	}
	// blocks
	for (auto b : o->onode.block_map) {
	  if (used_blocks.contains(b.second.offset, b.second.length)) {
	    derr << " " << oid << " extent " << b.first << ": " << b.second
		 << " already allocated" << dendl;
	    ++errors;
	    continue;
	  }
	  used_blocks.insert(b.second.offset, b.second.length);
	  if (b.second.end() > bdev->get_size()) {
	    derr << " " << oid << " extent " << b.first << ": " << b.second
		 << " past end of block device" << dendl;
	    ++errors;
	  }
	}
	// overlays
	set<string> overlay_keys;
	map<uint64_t,int> refs;
	for (auto v : o->onode.overlay_map) {
	  if (v.first + v.second.length > o->onode.size) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " extends past end of object" << dendl;
	    ++errors;
	  }
	  if (v.second.key > o->onode.last_overlay_key) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " is > last_overlay_key " << o->onode.last_overlay_key
		 << dendl;
	    ++errors;
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
	  }
	  if (val.length() < v.second.value_offset + v.second.length) {
	    derr << " " << oid << " overlay " << v.first << " " << v.second
		 << " too short, " << val.length() << dendl;
	    ++errors;
	  }
	}
	for (auto vr : o->onode.overlay_refs) {
	  if (refs[vr.first] != vr.second) {
	    derr << " " << oid << " overlay key " << vr.first
		 << " says " << vr.second << " refs but we have "
		 << refs[vr.first] << dendl;
	    ++errors;
	  }
	  refs.erase(vr.first);
	}
	for (auto p : refs) {
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

  dout(1) << __func__ << " checking for stray objects" << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (it) {
    CollectionRef c;
    for (it->lower_bound(string()); it->valid(); it->next()) {
      ghobject_t oid;
      int r = get_key_object(it->key(), &oid);
      if (r < 0) {
	dout(30) << __func__ << "  bad object key "
		 << pretty_binary_string(it->key()) << dendl;
	++errors;
	continue;
      }
      if (!c || !c->contains(oid)) {
	c = NULL;
	for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
	       coll_map.begin();
	     p != coll_map.end() && !errors;
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

  dout(1) << __func__ << " checking freelist vs allocated" << dendl;
  {
    const map<uint64_t,uint64_t>& free = fm->get_freelist();
    for (map<uint64_t,uint64_t>::const_iterator p = free.begin();
	 p != free.end(); ++p) {
      if (used_blocks.contains(p->first, p->second)) {
	derr << __func__ << " free extent " << p->first << "~" << p->second
	     << " intersects allocated blocks" << dendl;
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

  dout(1) << __func__ << " finish with " << errors << " errors" << dendl;
  return errors;
}

void BlueStore::_sync()
{
  dout(10) << __func__ << dendl;

  // flush aios in flght
  bdev->flush();

  kv_lock.Lock();
  while (!kv_committing.empty() ||
	 !kv_queue.empty()) {
    dout(20) << " waiting for kv to commit" << dendl;
    kv_sync_cond.Wait(kv_lock);
  }
  kv_lock.Unlock();

  dout(10) << __func__ << " done" << dendl;
}

int BlueStore::statfs(struct statfs *buf)
{
  memset(buf, 0, sizeof(*buf));
  buf->f_blocks = bdev->get_size() / bdev->get_block_size();
  buf->f_bsize = bdev->get_block_size();
  buf->f_bfree = fm->get_total_free() / bdev->get_block_size();
  buf->f_bavail = buf->f_bfree;

  /*
  struct statfs fs;
  if (::statfs(path.c_str(), &fs) < 0) {
    int r = -errno;
    assert(!g_conf->bluestore_fail_eio || r != -EIO);
    return r;
  }
  */

  return 0;
}

// ---------------
// cache

BlueStore::CollectionRef BlueStore::_get_collection(coll_t cid)
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
  Mutex::Locker l(reap_lock);
  removed_collections.push_back(c);
}

void BlueStore::_reap_collections()
{
  reap_lock.Lock();

  list<CollectionRef> removed_colls;
  removed_colls.swap(removed_collections);
  reap_lock.Unlock();

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
  reap_cond.Signal();
}

// ---------------
// read operations

bool BlueStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return false;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return false;
  return true;
}

int BlueStore::stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
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
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  dout(15) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  bl.clear();
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (offset == length && offset == 0)
    length = o->onode.size;

  r = _do_read(o, offset, length, bl, op_flags);

 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint32_t op_flags)
{
  map<uint64_t,extent_t>::iterator bp, bend;
  map<uint64_t,overlay_t>::iterator op, oend;
  uint64_t block_size = bdev->get_block_size();
  int r;
  IOContext ioc(NULL);   // FIXME?

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << dendl;
  bl.clear();

  if (offset > o->onode.size) {
    r = 0;
    goto out;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->flush();

  r = 0;

  // loop over overlays and data fragments.  overlays take precedence.
  bend = o->onode.block_map.end();
  bp = o->onode.block_map.lower_bound(offset);
  if (bp != o->onode.block_map.begin()) {
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
      dout(30) << __func__ << " skip frag " << bp->first << "~" << bp->second
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
      db->get(PREFIX_OVERLAY, key, &v);
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
      x_len = MIN(x_len, bp->second.length);
      if (!bp->second.has_flag(extent_t::FLAG_UNWRITTEN)) {
	dout(30) << __func__ << " data " << bp->first << ": " << bp->second
		 << " use " << x_off << "~" << x_len
		 << " final offset " << x_off + bp->second.offset
		 << dendl;
	uint64_t front_extra = x_off % block_size;
	uint64_t r_off = x_off - front_extra;
	uint64_t r_len = ROUND_UP_TO(x_len + front_extra, block_size);
	dout(30) << __func__ << "  reading " << r_off << "~" << r_len << dendl;
	bufferlist t;
	r = bdev->read(r_off + bp->second.offset, r_len, &t, &ioc);
	if (r < 0) {
	  goto out;
	}
	r = r_len;
	bufferlist u;
	u.substr_of(t, front_extra, x_len);
	bl.claim_append(u);
      } else {
	// unwritten (zero) extent
	dout(30) << __func__ << " data " << bp->first << ": " << bp->second
		 << ", use " << x_len << " zeros" << dendl;
	bufferptr bp(x_len);
	bp.zero();
	bl.push_back(bp);
      }
      offset += x_len;
      length -= x_len;
      if (x_off + x_len == bp->second.length) {
	++bp;
      }
      continue;
    }

    // zero.
    dout(30) << __func__ << " zero " << offset << "~" << x_len << dendl;
    bufferptr bp(x_len);
    bp.zero();
    bl.push_back(bp);
    offset += x_len;
    length -= x_len;
    continue;
  }
  r = bl.length();

 out:
  return r;
}

int BlueStore::fiemap(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{
  map<uint64_t, uint64_t> m;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    return -ENOENT;
  }

  if (offset == len && offset == 0)
    len = o->onode.size;

  if (offset > o->onode.size)
    return 0;

  if (offset + len > o->onode.size) {
    len = o->onode.size - offset;
  }

  dout(20) << __func__ << " " << offset << "~" << len << " size "
	   << o->onode.size << dendl;

  map<uint64_t,extent_t>::iterator bp, bend;
  map<uint64_t,overlay_t>::iterator op, oend;

  // loop over overlays and data fragments.  overlays take precedence.
  bend = o->onode.block_map.end();
  bp = o->onode.block_map.lower_bound(offset);
  if (bp != o->onode.block_map.begin()) {
    --bp;
  }
  oend = o->onode.overlay_map.end();
  op = o->onode.overlay_map.lower_bound(offset);
  if (op != o->onode.overlay_map.begin()) {
    --op;
  }
  uint64_t start = offset;
  while (len > 0) {
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
      //m[offset] = x_len;
      dout(30) << __func__ << " overlay " << offset << "~" << x_len << dendl;
      len -= x_len;
      offset += x_len;
      ++op;
      continue;
    }

    unsigned x_len = len;
    if (op != oend &&
	op->first > offset &&
	op->first - offset < x_len) {
      x_len = op->first - offset;
    }

    // extent?
    if (bp != bend && bp->first <= offset) {
      uint64_t x_off = offset - bp->first;
      x_len = MIN(x_len, bp->second.length - x_off);
      dout(30) << __func__ << " extent " << offset << "~" << x_len << dendl;
      len -= x_len;
      offset += x_len;
      if (x_off + x_len == bp->second.length)
	++bp;
      continue;
    }
    // we are seeing a hole, time to add an entry to fiemap.
    m[start] = offset - start;
    dout(20) << __func__ << " out " << start << "~" << m[start] << dendl;
    offset += x_len;
    start = offset;
    len -= x_len;
    continue;
  }
  // add tailing
  if (offset - start != 0) {
    m[start] = offset - start;
    dout(20) << __func__ << " out " << start << "~" << m[start] << dendl;
  }

  ::encode(m, bl);
  dout(20) << __func__ << " " << offset << "~" << len
	   << " size = 0 (" << m << ")" << dendl;
  return 0;
}

int BlueStore::getattr(
  coll_t cid,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  dout(15) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int BlueStore::getattrs(
  coll_t cid,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " " << oid
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

bool BlueStore::collection_exists(coll_t c)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(c);
}

bool BlueStore::collection_empty(coll_t cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list(cid, ghobject_t(), ghobject_t::get_max(), true, 5,
			  &ls, &next);
  if (r < 0)
    return false;  // fixme?
  bool empty = ls.empty();
  dout(10) << __func__ << " " << cid << " = " << (int)empty << dendl;
  return empty;
}

int BlueStore::collection_list(
  coll_t cid, ghobject_t start, ghobject_t end,
  bool sort_bitwise, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " end " << end << " max " << max << dendl;
  if (!sort_bitwise)
    return -EOPNOTSUPP;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
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

  if (start == ghobject_t::get_max())
    goto out;
  get_coll_key_range(cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__
	   << " range " << pretty_binary_string(temp_start_key)
	   << " to " << pretty_binary_string(temp_end_key)
	   << " and " << pretty_binary_string(start_key)
	   << " to " << pretty_binary_string(end_key)
	   << " start " << start << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (start == ghobject_t() || start == cid.get_min_hobj()) {
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
  dout(10) << __func__ << " " << cid
	   << " start " << start << " end " << end << " max " << max
	   << " = " << r << ", ls.size() = " << ls->size()
	   << ", next = " << *pnext << dendl;
  return r;
}

// omap reads

BlueStore::OmapIteratorImpl::OmapIteratorImpl(CollectionRef c, OnodeRef o, KeyValueDB::Iterator it)
  : c(c), o(o), it(it)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    get_omap_header(o->onode.omap_head, &head);
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
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::omap_get_header(
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::omap_get_keys(
  coll_t cid,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
	dout(30) << __func__ << "  skipping head" << dendl;
	it->next();
	continue;
      }
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::omap_get_values(
  coll_t cid,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::omap_check_keys(
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

ObjectMap::ObjectMapIterator BlueStore::get_omap_iterator(
  coll_t cid,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{

  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c) {
    dout(10) << __func__ << " " << cid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o) {
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

int BlueStore::_recover_next_nid()
{
  nid_max = 0;
  bufferlist bl;
  db->get(PREFIX_SUPER, "nid_max", &bl);
  try {
    ::decode(nid_max, bl);
  } catch (buffer::error& e) {
  }
  dout(1) << __func__ << " old nid_max " << nid_max << dendl;
  nid_last = nid_max;
  return 0;
}

void BlueStore::_assign_nid(TransContext *txc, OnodeRef o)
{
  if (o->onode.nid)
    return;
  Mutex::Locker l(nid_lock);
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

void BlueStore::_txc_release(TransContext *txc, uint64_t offset, uint64_t length)
{
  txc->released.insert(offset, length);
}

void BlueStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    dout(10) << __func__ << " txc " << txc
	     << " " << txc->get_state_name() << dendl;
    switch (txc->state) {
    case TransContext::STATE_PREPARE:
      if (txc->ioc.has_aios()) {
	txc->state = TransContext::STATE_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_AIO_WAIT:
      _txc_finish_io(txc);  // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      assert(txc->osr->qlock.is_locked());  // see _txc_finish_io
      txc->state = TransContext::STATE_KV_QUEUED;
      if (!g_conf->bluestore_sync_transaction) {
	Mutex::Locker l(kv_lock);
	if (g_conf->bluestore_sync_submit_transaction) {
	  db->submit_transaction(txc->t);
	}
	kv_queue.push_back(txc);
	kv_cond.SignalOne();
	return;
      }
      db->submit_transaction_sync(txc->t);
      break;

    case TransContext::STATE_KV_QUEUED:
      txc->state = TransContext::STATE_KV_DONE;
      _txc_finish_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
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
      if (txc->ioc.has_aios()) {
	txc->state = TransContext::STATE_WAL_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_WAL_AIO_WAIT:
      _wal_finish(txc);
      return;

    case TransContext::STATE_WAL_CLEANUP:
      txc->state = TransContext::STATE_FINISHING;
      // ** fall-thru **

    case TransContext::TransContext::STATE_FINISHING:
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
  Mutex::Locker l(osr->qlock);
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

int BlueStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc
	   << " onodes " << txc->onodes << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
    dout(20) << " onode size is " << bl.length() << dendl;
    txc->t->set(PREFIX_OBJ, (*p)->key, bl);

    Mutex::Locker l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }

  // journal wal items
  if (txc->wal_txn) {
    txc->wal_txn->seq = wal_seq.inc();
    bufferlist bl;
    ::encode(*txc->wal_txn, bl);
    string key;
    get_wal_key(txc->wal_txn->seq, &key);
    txc->t->set(PREFIX_WAL, key, bl);
  }

  return 0;
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
    Mutex::Locker l((*p)->flush_lock);
    dout(20) << __func__ << " onode " << *p << " had " << (*p)->flush_txns
	     << dendl;
    assert((*p)->flush_txns.count(txc));
    (*p)->flush_txns.erase(txc);
    if ((*p)->flush_txns.empty())
      (*p)->flush_cond.Signal();
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
  osr->qlock.Lock();
  txc->state = TransContext::STATE_DONE;
  osr->qlock.Unlock();

  _osr_reap_done(osr.get());
}

void BlueStore::_osr_reap_done(OpSequencer *osr)
{
  Mutex::Locker l(osr->qlock);
  dout(20) << __func__ << " osr " << osr << dendl;
  while (!osr->q.empty()) {
    TransContext *txc = &osr->q.front();
    dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	     << dendl;
    if (txc->state != TransContext::STATE_DONE) {
      break;
    }

    if (txc->first_collection) {
      txc->first_collection->onode_map.trim(g_conf->bluestore_onode_map_size);
    }

    osr->q.pop_front();
    delete txc;
    osr->qcond.Signal();
    if (osr->q.empty())
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
  }
}

void BlueStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  kv_lock.Lock();
  while (true) {
    assert(kv_committing.empty());
    assert(wal_cleaning.empty());
    if (kv_queue.empty() && wal_cleanup_queue.empty()) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_sync_cond.Signal();
      kv_cond.Wait(kv_lock);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      dout(20) << __func__ << " committing " << kv_queue.size()
	       << " cleaning " << wal_cleanup_queue.size() << dendl;
      kv_committing.swap(kv_queue);
      wal_cleaning.swap(wal_cleanup_queue);
      utime_t start = ceph_clock_now(NULL);
      kv_lock.Unlock();

      dout(30) << __func__ << " committing txc " << kv_committing << dendl;
      dout(30) << __func__ << " wal_cleaning txc " << wal_cleaning << dendl;

      // one transaction to force a sync
      KeyValueDB::Transaction t = db->get_transaction();

      // allocations.  consolidate before submitting to freelist so that
      // we avoid redundant kv ops.
      interval_set<uint64_t> allocated, released;
      for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	   it != kv_committing.end();
	   ++it) {
	TransContext *txc = *it;
	allocated.insert(txc->allocated);
	if (txc->wal_txn) {
	  dout(20) << __func__ << " txc " << txc
		   << " allocated " << txc->allocated
		   << " (will release " << txc->released << " after wal)"
		   << dendl;
	  txc->wal_txn->released.swap(txc->released);
	  assert(txc->released.empty());
	} else {
	  dout(20) << __func__ << " txc " << *it
		   << " allocated " << txc->allocated
		   << " released " << txc->released
		   << dendl;
	  released.insert((*it)->released);
	}
      }
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	   it != wal_cleaning.end();
	   ++it) {
	TransContext *txc = *it;
	if (!txc->wal_txn->released.empty()) {
	  dout(20) << __func__ << " txc " << txc
		   << " (post-wal) released " << txc->wal_txn->released
		   << dendl;
	  released.insert(txc->wal_txn->released);
	}
      }
      for (interval_set<uint64_t>::iterator p = allocated.begin();
	   p != allocated.end();
	   ++p) {
	dout(20) << __func__ << " alloc " << p.get_start() << "~" << p.get_len()
		 << dendl;
	fm->allocate(p.get_start(), p.get_len(), t);
      }
      for (interval_set<uint64_t>::iterator p = released.begin();
	   p != released.end();
	   ++p) {
	dout(20) << __func__ << " release " << p.get_start()
		 << "~" << p.get_len() << dendl;
	fm->release(p.get_start(), p.get_len(), t);
	alloc->release(p.get_start(), p.get_len());
      }

      alloc->commit_start();

      // flush/barrier on block device
      bdev->flush();

      if (!g_conf->bluestore_sync_submit_transaction) {
	for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	     it != kv_committing.end();
	     ++it) {
	  db->submit_transaction((*it)->t);
	}
      }

      // cleanup sync wal keys
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	    it != wal_cleaning.end();
	    ++it) {
	wal_transaction_t& wt =*(*it)->wal_txn;
	// cleanup the data in overlays
	for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
	  for (vector<uint64_t>::iterator q = p->removed_overlays.begin();
	       q != p->removed_overlays.end();
	       ++q) {
            string key;
            get_overlay_key(p->nid, *q, &key);
	    t->rmkey(PREFIX_OVERLAY, key);
	  }
	}
	// cleanup the wal
	string key;
	get_wal_key(wt.seq, &key);
	t->rmkey(PREFIX_WAL, key);
      }
      db->submit_transaction_sync(t);
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

      kv_lock.Lock();
    }
  }
  kv_lock.Unlock();
  dout(10) << __func__ << " finish" << dendl;
}

wal_op_t *BlueStore::_get_wal_op(TransContext *txc, OnodeRef o)
{
  if (!txc->wal_txn) {
    txc->wal_txn = new wal_transaction_t;
  }
  txc->wal_txn->ops.push_back(wal_op_t());
  txc->wal_op_onodes.push_back(o);
  return &txc->wal_txn->ops.back();
}

int BlueStore::_wal_apply(TransContext *txc)
{
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << txc << " seq " << wt.seq << dendl;
  txc->state = TransContext::STATE_WAL_APPLYING;

  assert(txc->ioc.pending_aios.empty());
  vector<OnodeRef>::iterator q = txc->wal_op_onodes.begin();
  for (list<wal_op_t>::iterator p = wt.ops.begin();
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
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << " seq " << wt.seq << txc << dendl;

  Mutex::Locker l(kv_lock);
  txc->state = TransContext::STATE_WAL_CLEANUP;
  wal_cleanup_queue.push_back(txc);
  kv_cond.SignalOne();
  return 0;
}

int BlueStore::_do_wal_op(wal_op_t& wo, IOContext *ioc)
{
  const uint64_t block_size = bdev->get_block_size();
  const uint64_t block_mask = ~(block_size - 1);

  // read all the overlay data first for apply
  _do_read_all_overlays(wo);

  switch (wo.op) {
  case wal_op_t::OP_WRITE:
  {
    dout(20) << __func__ << " write " << wo.extent << dendl;
    // FIXME: do the reads async?
    bufferlist bl;
    bl.claim(wo.data);
    uint64_t offset = wo.extent.offset;
    bufferlist first;
    uint64_t first_len = offset & ~block_mask;
    if (first_len) {
      offset = offset & block_mask;
      dout(20) << __func__ << "  reading initial partial block "
	       << offset << "~" << block_size << dendl;
      bdev->read(offset, block_size, &first, ioc);
      bufferlist t;
      t.substr_of(first, 0, first_len);
      t.claim_append(bl);
      bl.swap(t);
    }
    if (wo.extent.end() & ~block_mask) {
      uint64_t last_offset = wo.extent.end() & block_mask;
      bufferlist last;
      if (last_offset == offset && first.length()) {
	last.claim(first);   // same block we read above
      } else {
	dout(20) << __func__ << "  reading trailing partial block "
		 << last_offset << "~" << block_size << dendl;
	bdev->read(last_offset, block_size, &last, ioc);
      }
      bufferlist t;
      uint64_t endoff = wo.extent.end() & ~block_mask;
      t.substr_of(last, endoff, block_size - endoff);
      bl.claim_append(t);
    }
    assert((bl.length() & ~block_mask) == 0);
    bdev->aio_write(offset, bl, ioc);
  }
  break;

  case wal_op_t::OP_ZERO:
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
      bdev->read(first_offset, block_size, &first, ioc);
      size_t z_len = MIN(block_size - first_len, length);
      memset(first.c_str() + first_len, 0, z_len);
      bdev->aio_write(first_offset, first, ioc);
      offset += block_size - first_len;
      length -= z_len;
    }
    assert(offset % block_size == 0);
    if (length >= block_size) {
      uint64_t middle_len = length & block_mask;
      dout(20) << __func__ << "  zero " << offset << "~" << length << dendl;
      bdev->aio_zero(offset, middle_len, ioc);
      offset += middle_len;
      length -= middle_len;
    }
    assert(offset % block_size == 0);
    if (length > 0) {
      assert(length < block_size);
      bufferlist last;
      dout(20) << __func__ << "  reading trailing partial block "
	       << offset << "~" << block_size << dendl;
      bdev->read(offset, block_size, &last, ioc);
      memset(last.c_str(), 0, length);
      bdev->aio_write(offset, last, ioc);
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
    TransContext *txc = _txc_create(osr.get());
    txc->wal_txn = new wal_transaction_t;
    bufferlist bl = it->value();
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(*txc->wal_txn, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode wal txn "
	   << pretty_binary_string(it->key()) << dendl;
      return -EIO;
    }
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
    list<Transaction*>& tls,
    TrackedOpRef op,
    ThreadPool::TPHandle *handle)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);
  int r;

  // set up the sequencer
  OpSequencer *osr;
  assert(posr);
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(5) << __func__ << " existing " << osr << " " << *osr << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << __func__ << " new " << osr << " " << *osr << dendl;
  }

  // prepare
  TransContext *txc = _txc_create(osr);
  txc->onreadable = onreadable;
  txc->onreadable_sync = onreadable_sync;
  txc->oncommit = ondisk;

  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    (*p)->set_osr(osr);
    txc->ops += (*p)->get_num_ops();
    txc->bytes += (*p)->get_num_bytes();
    _txc_add_transaction(txc, *p);
  }

  r = _txc_finalize(osr, txc);
  assert(r == 0);

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

int BlueStore::_txc_add_transaction(TransContext *txc, Transaction *t)
{
  Transaction::iterator i = t->begin();
  int pos = 0;

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);

    // note first collection we reference
    if (!j && !txc->first_collection)
      txc->first_collection = cvec[j];
  }

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;
    CollectionRef &c = cvec[op->cid];

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _touch(txc, c, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, oid, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        uint64_t off = op->off;
	r = _truncate(txc, c, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
	r = _remove(txc, c, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(txc, c, oid, to_set);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	string name = i.decode_string();
	r = _rmattr(txc, c, oid, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _rmattrs(txc, c, oid);
      }
      break;

    case Transaction::OP_CLONE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        const ghobject_t& noid = i.get_oid(op->dest_oid);
	r = _clone(txc, c, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	assert(!c);
        coll_t cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, op->split_bits, &c);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
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
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
      }
      break;

    case Transaction::OP_COLL_ADD:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_REMOVE:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	assert(op->cid == op->dest_cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _rename(txc, c, oldoid, newoid);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      assert(0 == "not implmeneted");
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        ghobject_t oid = i.get_oid(op->oid);
	r = _omap_clear(txc, c, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        ghobject_t oid = i.get_oid(op->oid);
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(txc, c, oid, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        ghobject_t oid = i.get_oid(op->oid);
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(txc, c, oid, keys_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkey_range(txc, c, oid, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(txc, c, oid, bl);
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
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
	r = _setallochint(txc, c, oid,
			  expected_object_size,
			  expected_write_size);
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

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

    ++pos;
  }

  return 0;
}



// -----------------
// write operations

int BlueStore::_touch(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  assert(o);
  o->exists = true;
  _assign_nid(txc, o);
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_do_overlay_clear(TransContext *txc,
				OnodeRef o)
{
  dout(10) << __func__ << " " << o->oid << dendl;

  map<uint64_t,overlay_t>::iterator p = o->onode.overlay_map.begin();
  while (p != o->onode.overlay_map.end()) {
    dout(20) << __func__ << " rm " << p->first << " " << p->second << dendl;
    string key;
    get_overlay_key(o->onode.nid, p->first, &key);
    txc->t->rmkey(PREFIX_OVERLAY, key);
    o->onode.overlay_map.erase(p++);
  }
  o->onode.overlay_refs.clear();
  return 0;
}

int BlueStore::_do_overlay_trim(TransContext *txc,
			       OnodeRef o,
			       uint64_t offset,
			       uint64_t length)
{
  dout(10) << __func__ << " " << o->oid << " "
	   << offset << "~" << length << dendl;
  int changed = 0;

  map<uint64_t,overlay_t>::iterator p =
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
	get_overlay_key(o->onode.nid, p->first, &key);
	txc->t->rmkey(PREFIX_OVERLAY, key);
      }
      o->onode.overlay_map.erase(p++);
      ++changed;
      continue;
    }
    if (p->first >= offset) {
      dout(20) << __func__ << " trim_front " << p->first << " " << p->second
	       << dendl;
      overlay_t& ov = o->onode.overlay_map[offset + length] = p->second;
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
    overlay_t& nov = o->onode.overlay_map[offset + length] = p->second;
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
  overlay_t& ov = o->onode.overlay_map[offset] =
    overlay_t(++o->onode.last_overlay_key, 0, length);
  dout(20) << __func__ << " added " << offset << " " << ov << dendl;
  string key;
  get_overlay_key(o->onode.nid, o->onode.last_overlay_key, &key);
  txc->t->set(PREFIX_OVERLAY, key, bl);
  return 0;
}

int BlueStore::_do_write_overlays(TransContext *txc,
				 OnodeRef o,
				 uint64_t orig_offset,
				 uint64_t orig_length)
{
  if (o->onode.overlay_map.empty())
    return 0;

  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;

  uint64_t offset = 0;
  uint64_t length = 0;
  wal_op_t *op = NULL;

  map<uint64_t,overlay_t>::iterator p =
    o->onode.overlay_map.lower_bound(orig_offset);
  while (true) {
    if (p != o->onode.overlay_map.end() && p->first < orig_offset + orig_length) {
      if (!op) {
	dout(10) << __func__ << " overlay " << p->first
		 << "~" << p->second.length << " " << p->second
		 << " (first)" << dendl;
	op = _get_wal_op(txc, o);
	op->nid = o->onode.nid;
	op->op = wal_op_t::OP_WRITE;
	op->overlays.push_back(p->second);
	offset = p->first;
	length = p->second.length;

	if (o->onode.put_overlay_ref(p->second.key)) {
	  string key;
	  get_overlay_key(o->onode.nid, p->first, &key);
	  txc->t->rmkey(PREFIX_OVERLAY, key);
	}
	o->onode.overlay_map.erase(p++);
	continue;
      }

      // contiguous?  and in the same allocation unit?
      if (offset + length == p->first &&
	  p->first % min_alloc_size) {
	dout(10) << __func__ << " overlay " << p->first
		 << "~" << p->second.length << " " << p->second
		 << " (contiguous)" << dendl;
	op->overlays.push_back(p->second);
	length += p->second.length;

	if (o->onode.put_overlay_ref(p->second.key)) {
	  string key;
	  get_overlay_key(o->onode.nid, p->first, &key);
	  txc->t->rmkey(PREFIX_OVERLAY, key);
	}
	o->onode.overlay_map.erase(p++);
	continue;
      }
    }
    if (!op) {
      break;
    }
    assert(length <= min_alloc_size);

    // emit
    map<uint64_t, extent_t>::iterator bp = o->onode.find_extent(offset);
    if (bp == o->onode.block_map.end() ||
	length == min_alloc_size) {
      int r = _do_allocate(txc, o, offset, length, 0, false);
      if (r < 0)
	return r;
      bp = o->onode.find_extent(offset);
      if (bp->second.has_flag(extent_t::FLAG_UNWRITTEN)) {
	dout(10) << __func__ << " zero new allocation " << bp->second << dendl;
	bdev->aio_zero(bp->second.offset, bp->second.length, &txc->ioc);
	bp->second.clear_flag(extent_t::FLAG_UNWRITTEN);
      }
    }
    uint64_t x_off = offset - bp->first;
    dout(10) << __func__ << " wal write " << offset << "~" << length
	     << " to extent " << bp->first << ": " << bp->second
	     << " x_off " << x_off << " overlay data from "
	     << offset << "~" << length << dendl;
    op->extent.offset = bp->second.offset + x_off;
    op->extent.length = length;
    op = NULL;

    if (p == o->onode.overlay_map.end() || p->first >= orig_offset + orig_length) {
      break;
    }
    ++p;
  }

  txc->write_onode(o);
  return 0;
}

void BlueStore::_do_read_all_overlays(wal_op_t& wo)
{
  for (vector<overlay_t>::iterator q = wo.overlays.begin();
       q != wo.overlays.end(); ++q) {
    string key;
    get_overlay_key(wo.nid, q->key, &key);
    bufferlist bl, bl_data;
    db->get(PREFIX_OVERLAY, key, &bl);
    bl_data.substr_of(bl, q->value_offset, q->length);
    wo.data.claim_append(bl_data);
  }
  return;
}

void BlueStore::_dump_onode(OnodeRef o)
{
  dout(30) << __func__ << " " << o
	   << " nid " << o->onode.nid
	   << " size " << o->onode.size
	   << " expected_object_size " << o->onode.expected_object_size
	   << " expected_write_size " << o->onode.expected_write_size
	   << dendl;
  for (map<string,bufferptr>::iterator p = o->onode.attrs.begin();
       p != o->onode.attrs.end();
       ++p) {
    dout(30) << __func__ << "  attr " << p->first
	     << " len " << p->second.length() << dendl;
  }
  uint64_t pos = 0;
  for (map<uint64_t,extent_t>::iterator p = o->onode.block_map.begin();
       p != o->onode.block_map.end();
       ++p) {
    dout(30) << __func__ << "  extent " << p->first << " " << p->second
	     << dendl;
    assert(p->first >= pos);
    pos = p->first + p->second.length;
  }
  pos = 0;
  for (map<uint64_t,overlay_t>::iterator p = o->onode.overlay_map.begin();
       p != o->onode.overlay_map.end();
       ++p) {
    dout(30) << __func__ << "  overlay " << p->first << " " << p->second
	     << dendl;
    assert(p->first >= pos);
    pos = p->first + p->second.length;
  }
  if (!o->onode.overlay_refs.empty()) {
    dout(30) << __func__ << "  overlay_refs " << o->onode.overlay_refs << dendl;
  }
}

void BlueStore::_pad_zeros(
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
    if (end > o->onode.size && g_conf->bluestore_cache_tails) {
      o->tail_bl.clear();
      o->tail_bl.append(tail, 0, back_copy);
      o->tail_offset = end - back_copy;
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

int BlueStore::_do_allocate(TransContext *txc,
			   OnodeRef o,
			   uint64_t orig_offset, uint64_t orig_length,
			   uint32_t fadvise_flags,
			   bool allow_overlay)
{
  dout(20) << __func__
	   << " " << o->oid << " " << orig_offset << "~" << orig_length
	   << " - have " << o->onode.size
	   << " bytes in " << o->onode.block_map.size()
	   << " extents" << dendl;
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;

  // start with any full blocks we will write
  uint64_t offset = orig_offset;
  uint64_t length = orig_length;
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

  map<uint64_t, extent_t>::iterator bp;

  uint64_t orig_end = orig_offset + orig_length;
  if (orig_offset / min_alloc_size == orig_end / min_alloc_size) {
    // we fall within the same block
    offset = orig_offset - orig_offset % min_alloc_size;
    length = 0;
    assert(offset <= orig_offset);
    dout(20) << "  io falls within " << offset << "~" << min_alloc_size << dendl;
    if (allow_overlay && _can_overlay_write(o, orig_length)) {
      dout(20) << "  entire write will be captured by overlay" << dendl;
    } else {
      bp = o->onode.find_extent(offset);
      if (bp == o->onode.block_map.end()) {
	dout(20) << "  not yet allocated" << dendl;
	length = min_alloc_size;
      } else {
	dout(20) << "  will presumably WAL" << dendl;
      }
    }
  } else {
    dout(20) << "  initial full " << offset << "~" << length
	     << ", head " << head << " tail " << tail << dendl;

    // include tail?
    if (tail) {
      if (allow_overlay && _can_overlay_write(o, tail)) {
	dout(20) << "  tail " << head << " will be captured by overlay" << dendl;
      } else {
	bp = o->onode.find_extent(orig_offset + orig_length - 1);
	if (bp == o->onode.block_map.end()) {
	  dout(20) << "  tail " << tail << " not yet allocated" << dendl;
	  length += min_alloc_size;
	} else {
	  dout(20) << "  tail " << tail << " will presumably WAL" << dendl;
	}
      }
    }

    // include head?
    bp = o->onode.find_extent(orig_offset);
    if (head) {
      if (allow_overlay && _can_overlay_write(o, head)) {
	dout(20) << "  head " << head << " will be captured by overlay" << dendl;
      } else if (bp == o->onode.block_map.end()) {
	dout(20) << "  head " << head << " not yet allocated" << dendl;
	offset -= min_alloc_size;
	length += min_alloc_size;
      } else {
	dout(20) << "  head " << head << " will presumably WAL" << dendl;
      }
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
    bp = o->onode.seek_extent(offset);
    while (bp != o->onode.block_map.end() &&
	   bp->first < offset + length &&
	   bp->first + bp->second.length > offset) {
      dout(30) << "   bp " << bp->first << ": " << bp->second << dendl;
      if (bp->first < offset) {
	uint64_t left = offset - bp->first;
	if (bp->first + bp->second.length <= offset + length) {
	  dout(20) << "  trim tail " << bp->first << ": " << bp->second << dendl;
	  _txc_release(txc,
		       bp->second.offset + left,
		       bp->second.length - left);
	  bp->second.length = left;
	  dout(20) << "        now " << bp->first << ": " << bp->second << dendl;
	  hint = bp->first + bp->second.length;
	  ++bp;
	} else {
	  dout(20) << "      split " << bp->first << ": " << bp->second << dendl;
	  _txc_release(txc, bp->second.offset + left, length);
	  o->onode.block_map[offset + length] =
	    extent_t(bp->second.offset + left + length,
		     bp->second.length - (left + length));
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
	  _txc_release(txc, bp->second.offset, overlap);
	  o->onode.block_map[bp->first + overlap] =
	    extent_t(bp->second.offset + overlap,
		     bp->second.length - overlap);
	  o->onode.block_map.erase(bp++);
	  dout(20) << "        now " << bp->first << ": " << bp->second << dendl;
	  assert(bp->first == offset + length);
	  hint = bp->first;
	} else {
	  dout(20) << "    dealloc " << bp->first << ": " << bp->second << dendl;
	  _txc_release(txc, bp->second.offset, bp->second.length);
	  hint = bp->first + bp->second.length;
	  o->onode.block_map.erase(bp++);
	}
      }
    }

    // allocate our new extent(s)
    while (length > 0) {
      extent_t e;
      // for safety, set the UNWRITTEN flag here.  We should clear this in
      // _do_write or else we likely have problems.
      e.flags |= extent_t::FLAG_UNWRITTEN;
      int r = alloc->allocate(length, min_alloc_size, hint,
			      &e.offset, &e.length);
      assert(r == 0);
      assert(e.length <= length);  // bc length is a multiple of min_alloc_size
      txc->allocated.insert(e.offset, e.length);
      o->onode.block_map[offset] = e;
      dout(10) << __func__ << "  alloc " << offset << ": " << e << dendl;
      length -= e.length;
      offset += e.length;
      hint = e.end();
    }
  }

  return 0;
}

bool BlueStore::_can_overlay_write(OnodeRef o, uint64_t length)
{
  return
    (int)o->onode.overlay_map.size() < g_conf->bluestore_overlay_max &&
    (int)length <= g_conf->bluestore_overlay_max_length;
}

int BlueStore::_do_write(TransContext *txc,
			OnodeRef o,
			uint64_t orig_offset, uint64_t orig_length,
			bufferlist& orig_bl,
			uint32_t fadvise_flags)
{
  int r = 0;

  dout(20) << __func__
	   << " " << o->oid << " " << orig_offset << "~" << orig_length
	   << " - have " << o->onode.size
	   << " bytes in " << o->onode.block_map.size()
	   << " extents" << dendl;
  _dump_onode(o);
  o->exists = true;

  if (orig_length == 0) {
    return 0;
  }

  uint64_t block_size = bdev->get_block_size();
  const uint64_t block_mask = ~(block_size - 1);
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
  map<uint64_t, extent_t>::iterator bp;
  uint64_t length;

  r = _do_allocate(txc, o, orig_offset, orig_length, fadvise_flags, true);
  if (r < 0) {
    derr << __func__ << " allocate failed, " << cpp_strerror(r) << dendl;
    goto out;
  }

  bp = o->onode.seek_extent(orig_offset);
  for (uint64_t offset = orig_offset;
       offset < orig_offset + orig_length;
       offset += length) {
    // cut to extent
    length = orig_offset + orig_length - offset;
    if (bp == o->onode.block_map.end() ||
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
    if (bp == o->onode.block_map.end())
      dout(20) << __func__ << "  chunk " << offset << "~" << length
	       << " (no extent)" << dendl;
    else
      dout(20) << __func__ << "  chunk " << offset << "~" << length
	       << " extent " << bp->first << ": " << bp->second << dendl;

    if (_can_overlay_write(o, length)) {
      r = _do_overlay_write(txc, o, offset, length, bl);
      if (r < 0)
	goto out;
      if (bp != o->onode.block_map.end() &&
	  bp->first < offset + length)
	++bp;
      continue;
    }

    assert(bp != o->onode.block_map.end());
    assert(offset >= bp->first);
    assert(offset + length <= bp->first + bp->second.length);

    // (pad and) overwrite unused portion of extent for an append?
    if (offset > bp->first &&
	offset >= o->onode.size &&                                  // past eof +
	(offset / block_size != (o->onode.size - 1) / block_size)) {// diff block
      dout(20) << __func__ << " append" << dendl;
      _pad_zeros(o, &bl, &offset, &length, block_size);
      assert(offset % block_size == 0);
      assert(length % block_size == 0);
      // the trailing block is zeroed to the end
      uint64_t from = MAX(ROUND_UP_TO(o->onode.size, block_size), bp->first);
      if (offset > from) {
	uint64_t x_off = from - bp->first;
	uint64_t z_len = offset - from;
	dout(20) << __func__ << " zero " << from << "~" << z_len
		 << " x_off " << x_off << dendl;
	bdev->aio_zero(bp->second.offset + x_off, z_len, &txc->ioc);
      }
      uint64_t x_off = offset - bp->first;
      dout(20) << __func__ << " write " << offset << "~" << length
	       << " x_off " << x_off << dendl;
      bdev->aio_write(bp->second.offset + x_off, bl, &txc->ioc);
      bp->second.clear_flag(extent_t::FLAG_UNWRITTEN);
      ++bp;
      continue;
    }

    // use cached tail block?
    uint64_t tail_start = o->onode.size - o->onode.size % block_size;
    if (offset >= bp->first &&
	offset > tail_start &&
	offset + length >= o->onode.size &&
	o->tail_bl.length() &&
	(offset / block_size == (o->onode.size - 1) / block_size)) {
      dout(20) << __func__ << " using cached tail" << dendl;
      assert((offset & block_mask) == (o->onode.size & block_mask));
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
      assert(!bp->second.has_flag(extent_t::FLAG_UNWRITTEN));
      _pad_zeros(o, &bl, &offset, &length, block_size);
      uint64_t x_off = offset - bp->first;
      dout(20) << __func__ << " write " << offset << "~" << length
	       << " x_off " << x_off << dendl;
      bdev->aio_write(bp->second.offset + x_off, bl, &txc->ioc);
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
      assert(bp->second.has_flag(extent_t::FLAG_UNWRITTEN));
    }

    if (bp->second.has_flag(extent_t::FLAG_UNWRITTEN)) {
      _pad_zeros(o, &bl, &offset, &length, block_size);
      if (offset > bp->first) {
	uint64_t z_len = offset - bp->first;
	dout(20) << __func__ << " zero " << bp->first << "~" << z_len << dendl;
	bdev->aio_zero(bp->second.offset, z_len, &txc->ioc);
      }
      uint64_t x_off = offset - bp->first;
      dout(20) << __func__ << " write " << offset << "~" << length
	       << " x_off " << x_off << dendl;
      bdev->aio_write(bp->second.offset + x_off, bl, &txc->ioc);
      if (offset + length < bp->first + bp->second.length &&
	  offset + length <= o->onode.size) {
	uint64_t end = offset + length;
	uint64_t z_len = bp->first + bp->second.length - end;
	uint64_t x_off = end - bp->first;
	dout(20) << __func__ << " zero " << end << "~" << z_len
		 << " x_off " << x_off << dendl;
	bdev->aio_zero(bp->second.offset + x_off, z_len, &txc->ioc);
      }
      bp->second.clear_flag(extent_t::FLAG_UNWRITTEN);
      ++bp;
      continue;
    }

    // WAL.
    r = _do_write_overlays(txc, o, bp->first, bp->second.length);
    if (r < 0)
      goto out;
    assert(bp->first <= offset);
    assert(offset + length <= bp->first + bp->second.length);
    wal_op_t *op = _get_wal_op(txc, o);
    op->op = wal_op_t::OP_WRITE;
    op->extent.offset = bp->second.offset + offset - bp->first;
    op->extent.length = length;
    op->data = bl;
    if (offset + length - bp->first > bp->second.length) {
      op->extent.length = offset + length - bp->first;
    }
    dout(20) << __func__ << " wal write "
	     << offset << "~" << length << " to " << op->extent
	     << dendl;
    ++bp;
    continue;
  }
  r = 0;

  if (orig_offset + orig_length > o->onode.size) {
    dout(20) << __func__ << " extending size to " << orig_offset + orig_length
	     << dendl;
    o->onode.size = orig_offset + orig_length;
  }

  // make sure we didn't leave unwritten extents behind
  for (map<uint64_t,extent_t>::iterator p = o->onode.block_map.begin();
       p != o->onode.block_map.end();
       ++p) {
    if (p->second.has_flag(extent_t::FLAG_UNWRITTEN)) {
      derr << __func__ << " left behind an unwritten extent, out of sync with "
	   << "_do_allocate" << dendl;
      _dump_onode(o);
      assert(0 == "leaked unwritten extent");
    }
  }

 out:
  return r;
}

int BlueStore::_write(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid,
		     uint64_t offset, size_t length,
		     bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  _assign_nid(txc, o);
  int r = _do_write(txc, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_zero(TransContext *txc,
		    CollectionRef& c,
		    const ghobject_t& oid,
		    uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  _assign_nid(txc, o);

  // overlay
  _do_overlay_trim(txc, o, offset, length);

  map<uint64_t,extent_t>::iterator bp = o->onode.seek_extent(offset);
  while (bp != o->onode.block_map.end()) {
    if (bp->first >= offset + length)
      break;

    if (offset <= bp->first &&
	(offset + length >= bp->first + bp->second.length ||
	 offset >= o->onode.size)) {
      // remove fragment
      dout(20) << __func__ << " dealloc " << bp->first << ": "
	       << bp->second << dendl;
      _txc_release(txc, bp->second.offset, bp->second.length);
      o->onode.block_map.erase(bp++);
      continue;
    }

    // start,end are offsets in the extent
    uint64_t x_off = 0;
    if (offset > bp->first) {
      x_off = offset - bp->first;
    }
    uint64_t x_len = MIN(length, bp->second.length - x_off);

    // WAL
    wal_op_t *op = _get_wal_op(txc, o);
    op->op = wal_op_t::OP_ZERO;
    op->extent.offset = bp->second.offset + x_off;
    op->extent.length = x_len;
    dout(20) << __func__ << "  wal zero " << x_off << "~" << x_len
	     << " " << op->extent << dendl;

    bp++;
  }

  if (offset + length > o->onode.size) {
    o->onode.size = offset + length;
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
  }
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_truncate(TransContext *txc, OnodeRef o, uint64_t offset)
{
  uint64_t block_size = bdev->get_block_size();
  uint64_t min_alloc_size = g_conf->bluestore_min_alloc_size;
  uint64_t alloc_end = ROUND_UP_TO(offset, min_alloc_size);

  // ensure any wal IO has completed before we truncate off any extents
  // they may touch.
  o->flush();

  // trim down fragments
  map<uint64_t,extent_t>::iterator bp = o->onode.block_map.end();
  if (bp != o->onode.block_map.begin())
    --bp;
  while (bp != o->onode.block_map.end()) {
    if (bp->first + bp->second.length <= alloc_end) {
      break;
    }
    if (bp->first >= alloc_end) {
      dout(20) << __func__ << " dealloc " << bp->first << ": "
	       << bp->second << dendl;
      _txc_release(txc, bp->second.offset, bp->second.length);
      if (bp != o->onode.block_map.begin()) {
	o->onode.block_map.erase(bp--);
	continue;
      } else {
	o->onode.block_map.erase(bp);
	break;
      }
    } else {
      assert(bp->first + bp->second.length > alloc_end);
      assert(bp->first < alloc_end);
      uint64_t newlen = alloc_end - bp->first;
      assert(newlen % min_alloc_size == 0);
      dout(20) << __func__ << " trunc " << bp->first << ": " << bp->second
	       << " to " << newlen << dendl;
      _txc_release(txc, bp->second.offset + newlen, bp->second.length - newlen);
      bp->second.length = newlen;
      break;
    }
  }

  // zero extent if trimming up?
  if (offset > o->onode.size) {
    map<uint64_t,extent_t>::iterator bp = o->onode.block_map.end();
    if (bp != o->onode.block_map.begin())
      --bp;
    if (bp != o->onode.block_map.end() &&
	bp->first + bp->second.length > o->onode.size) {
      // we need to zero from onode.size to offset.
      assert(offset > bp->first);  // else we would have trimmed it above
      assert(o->onode.size > bp->first);  // we do no preallocation (yet)
      uint64_t x_off = o->onode.size - bp->first;
      uint64_t x_len = ROUND_UP_TO(offset, block_size) - o->onode.size;
      wal_op_t *op = _get_wal_op(txc, o);
      op->op = wal_op_t::OP_ZERO;
      op->extent.offset = bp->second.offset + x_off;
      op->extent.length = x_len;
      dout(20) << __func__ << "  wal zero " << x_off << "~" << x_len
	       << " " << op->extent << dendl;
    }
  } else if (offset < o->onode.size &&
	     offset % block_size != 0) {
    // zero trailing block?
    map<uint64_t,extent_t>::iterator bp = o->onode.find_extent(offset);
    if (bp != o->onode.block_map.end()) {
      wal_op_t *op = _get_wal_op(txc, o);
      op->op = wal_op_t::OP_ZERO;
      uint64_t z_len = block_size - offset % block_size;
      op->extent.offset = bp->second.offset + offset - bp->first;
      op->extent.length = block_size - offset % block_size;
      dout(20) << __func__ << " wal zero tail " << offset << "~" << z_len
	       << " at " << op->extent << dendl;
    }
  }

  // trim down overlays
  map<uint64_t,overlay_t>::iterator op = o->onode.overlay_map.end();
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
	txc->t->rmkey(PREFIX_OVERLAY, key);
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
      overlay_t& ov = op->second;
      ov.length = newlen;
      break;
    }
  }

  // trim down cached tail
  if (o->tail_bl.length()) {
    if (offset / block_size != o->onode.size / block_size) {
      dout(20) << __func__ << " clear cached tail" << dendl;
      o->clear_tail();
    }
  }

  o->onode.size = offset;
  txc->write_onode(o);
  return 0;
}

int BlueStore::_truncate(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _do_truncate(txc, o, offset);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_do_remove(TransContext *txc,
			 OnodeRef o)
{
  string key;
  o->exists = false;
  if (!o->onode.block_map.empty()) {
    for (map<uint64_t,extent_t>::iterator p = o->onode.block_map.begin();
	 p != o->onode.block_map.end();
	 ++p) {
      dout(20) << __func__ << " dealloc " << p->second << dendl;
      _txc_release(txc, p->second.offset, p->second.length);
    }
  }
  o->onode.block_map.clear();
  _do_overlay_clear(txc, o);
  o->onode.size = 0;
  if (o->onode.omap_head) {
    _do_omap_clear(txc, o->onode.omap_head);
  }

  get_object_key(o->oid, &key);
  txc->t->rmkey(PREFIX_OBJ, key);
  return 0;
}

int BlueStore::_remove(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _do_remove(txc, o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_setattr(TransContext *txc,
		       CollectionRef& c,
		       const ghobject_t& oid,
		       const string& name,
		       bufferptr& val)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs[name] = val;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_setattrs(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			const map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end(); ++p)
    o->onode.attrs[p->first] = p->second;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << " = " << r << dendl;
  return r;
}


int BlueStore::_rmattr(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid,
		      const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.erase(name);
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int BlueStore::_rmattrs(TransContext *txc,
		       CollectionRef& c,
		       const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.clear();
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
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
      dout(30) << __func__ << "  stop at " << tail << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << pretty_binary_string(it->key()) << dendl;
    it->next();
  }
}

int BlueStore::_omap_clear(TransContext *txc,
			  CollectionRef& c,
			  const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (o->onode.omap_head != 0) {
    _do_omap_clear(txc, o->onode.omap_head);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_setkeys(TransContext *txc,
			    CollectionRef& c,
			    const ghobject_t& oid,
			    bufferlist &bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = bl.begin();
  __u32 num;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
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

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_setheader(TransContext *txc,
			      CollectionRef& c,
			      const ghobject_t& oid,
			      bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  string key;
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  get_omap_header(o->onode.omap_head, &key);
  txc->t->set(PREFIX_OMAP, key, bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_rmkeys(TransContext *txc,
			   CollectionRef& c,
			   const ghobject_t& oid,
			   bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = bl.begin();
  __u32 num;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    r = 0;
    goto out;
  }
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
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
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_omap_rmkey_range(TransContext *txc,
				CollectionRef& c,
				const ghobject_t& oid,
				const string& first, const string& last)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  KeyValueDB::Iterator it;
  string key_first, key_last;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    r = 0;
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
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int BlueStore::_setallochint(TransContext *txc,
			    CollectionRef& c,
			    const ghobject_t& oid,
			    uint64_t expected_object_size,
			    uint64_t expected_write_size)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_clone(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& old_oid,
		     const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;
  _assign_nid(txc, newo);

  r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

  // truncate any old data
  r = _do_truncate(txc, newo, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);

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
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

int BlueStore::_clone_range(TransContext *txc,
			   CollectionRef& c,
			   const ghobject_t& old_oid,
			   const ghobject_t& new_oid,
			   uint64_t srcoff, uint64_t length, uint64_t dstoff)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;

  r = _do_read(oldo, srcoff, length, bl, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff
	   << " = " << r << dendl;
  return r;
}

int BlueStore::_rename(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& old_oid,
		      const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << dendl;
  int r;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  string old_key, new_key;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);

  if (newo->exists) {
    r = _do_remove(txc, newo);
    if (r < 0)
      return r;
  }

  get_object_key(old_oid, &old_key);
  get_object_key(new_oid, &new_key);

  c->onode_map.rename(old_oid, new_oid);
  oldo->oid = new_oid;
  oldo->key = new_key;

  txc->t->rmkey(PREFIX_OBJ, old_key);
  txc->write_onode(oldo);
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
  bufferlist empty;

  {
    RWLock::WLocker l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    pair<ghobject_t,OnodeRef> next;
    while ((*c)->onode_map.get_next(next.first, &next)) {
      if (next.second->exists) {
	r = -ENOTEMPTY;
	goto out;
      }
    }
    coll_map.erase(cid);
    txc->removed_collections.push_back(*c);
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

  dout(10) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

// ===========================================
