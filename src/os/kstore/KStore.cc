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

#include "KStore.h"
#include "kv.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Formatter.h"


#define dout_subsys ceph_subsys_kstore

/*

  TODO:

  * superblock, features
  * refcounted extents (for efficient clone)

 */

const string PREFIX_SUPER = "S"; // field -> value
const string PREFIX_COLL = "C"; // collection name -> (nothing)
const string PREFIX_OBJ = "O";  // object name -> onode
const string PREFIX_DATA = "D"; // nid + offset -> data
const string PREFIX_OMAP = "M"; // u64 + keyname -> value

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


static void get_data_key(uint64_t nid, uint64_t offset, string *out)
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



// Onode

#undef dout_prefix
#define dout_prefix *_dout << "kstore.onode(" << this << ") "

void KStore::Onode::flush()
{
  std::unique_lock<std::mutex> l(flush_lock);
  dout(20) << __func__ << " " << flush_txns << dendl;
  while (!flush_txns.empty())
    flush_cond.wait(l);
  dout(20) << __func__ << " done" << dendl;
}

// OnodeHashLRU

#undef dout_prefix
#define dout_prefix *_dout << "kstore.lru(" << this << ") "

void KStore::OnodeHashLRU::_touch(OnodeRef o)
{
  lru_list_t::iterator p = lru.iterator_to(*o);
  lru.erase(p);
  lru.push_front(*o);
}

void KStore::OnodeHashLRU::add(const ghobject_t& oid, OnodeRef o)
{
  std::lock_guard<std::mutex> l(lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  lru.push_front(*o);
}

KStore::OnodeRef KStore::OnodeHashLRU::lookup(const ghobject_t& oid)
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

void KStore::OnodeHashLRU::clear()
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << dendl;
  lru.clear();
  onode_map.clear();
}

void KStore::OnodeHashLRU::rename(const ghobject_t& old_oid,
				    const ghobject_t& new_oid)
{
  std::lock_guard<std::mutex> l(lock);
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
  OnodeRef o = po->second;

  // install a non-existent onode it its place
  po->second.reset(new Onode(old_oid, o->key));
  lru.push_back(*po->second);

  // fix oid, key
  onode_map.insert(make_pair(new_oid, o));
  _touch(o);
  o->oid = new_oid;
  get_object_key(new_oid, &o->key);
}

bool KStore::OnodeHashLRU::get_next(
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

int KStore::OnodeHashLRU::trim(int max)
{
  std::lock_guard<std::mutex> l(lock);
  dout(20) << __func__ << " max " << max
	   << " size " << onode_map.size() << dendl;
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
#define dout_prefix *_dout << "kstore(" << store->path << ").collection(" << cid << ") "

KStore::Collection::Collection(KStore *ns, coll_t c)
  : store(ns),
    cid(c),
    lock("KStore::Collection::lock", true, false),
    onode_map()
{
}

KStore::OnodeRef KStore::Collection::get_onode(
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
#define dout_prefix *_dout << "kstore(" << path << ") "

KStore::KStore(CephContext *cct, const string& path)
  : ObjectStore(path),
    cct(cct),
    db(NULL),
    path_fd(-1),
    fsid_fd(-1),
    mounted(false),
    coll_lock("KStore::coll_lock"),
    nid_max(0),
    throttle_ops(cct, "kstore_max_ops", cct->_conf->kstore_max_ops),
    throttle_bytes(cct, "kstore_max_bytes", cct->_conf->kstore_max_bytes),
    finisher(cct),
    kv_sync_thread(this),
    kv_stop(false),
    logger(NULL)
{
  _init_logger();
}

KStore::~KStore()
{
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(fsid_fd < 0);
}

void KStore::_init_logger()
{
  // XXX
  PerfCountersBuilder b(g_ceph_context, "KStore",
                        l_kstore_first, l_kstore_last);
  b.add_time_avg(l_kstore_state_prepare_lat, "state_prepare_lat", "Average prepare state latency");
  b.add_time_avg(l_kstore_state_kv_queued_lat, "state_kv_queued_lat", "Average kv_queued state latency");
  b.add_time_avg(l_kstore_state_kv_done_lat, "state_kv_done_lat", "Average kv_done state latency");
  b.add_time_avg(l_kstore_state_finishing_lat, "state_finishing_lat", "Average finishing state latency");
  b.add_time_avg(l_kstore_state_done_lat, "state_done_lat", "Average done state latency");
  logger = b.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void KStore::_shutdown_logger()
{
  // XXX
  g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
}

int KStore::_open_path()
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

void KStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
}

int KStore::_open_fsid(bool create)
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

int KStore::_read_fsid(uuid_d *uuid)
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

int KStore::_write_fsid()
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

void KStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int KStore::_lock_fsid()
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

bool KStore::test_mount_in_use()
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

int KStore::_open_db(bool create)
{
  int r;
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());

  string kv_backend;
  if (create) {
    kv_backend = g_conf->kstore_backend;
  } else {
    r = read_meta("kv_backend", &kv_backend);
    if (r < 0) {
      derr << __func__ << " uanble to read 'kv_backend' meta" << dendl;
      return -EIO;
    }
  }
  dout(10) << __func__ << " kv_backend = " << kv_backend << dendl;

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
			  kv_backend,
			  fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    return -EIO;
  }
  string options;
  if (kv_backend == "rocksdb")
    options = g_conf->kstore_rocksdb_options;
  db->init(options);
  stringstream err;
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
  dout(1) << __func__ << " opened " << kv_backend
	  << " path " << fn << " options " << options << dendl;
  return 0;
}

void KStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

int KStore::_open_collections(int *errors)
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

int KStore::mkfs()
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
  if (r < 0 || old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    // we'll write it last.
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
	   << " != provided " << fsid << dendl;
      r = -EINVAL;
      goto out_close_fsid;
    }
    fsid = old_fsid;
    dout(1) << __func__ << " already created, fsid is " << fsid << dendl;
    goto out_close_fsid;
  }

  r = _open_db(true);
  if (r < 0)
    goto out_close_fsid;

  r = write_meta("kv_backend", g_conf->kstore_backend);
  if (r < 0)
    goto out_close_db;

  r = write_meta("type", "kstore");
  if (r < 0)
    goto out_close_db;

  // indicate mkfs completion/success by writing the fsid file
  r = _write_fsid();
  if (r == 0)
    dout(10) << __func__ << " success" << dendl;
  else
    derr << __func__ << " error writing fsid: " << cpp_strerror(r) << dendl;

 out_close_db:
  _close_db();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}

int KStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

  if (g_conf->kstore_fsck_on_mount) {
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

  r = _open_db(false);
  if (r < 0)
    goto out_fsid;

  r = _open_super_meta();
  if (r < 0)
    goto out_db;

  r = _open_collections();
  if (r < 0)
    goto out_db;

  finisher.start();
  kv_sync_thread.create("kstore_kv_sync");

  mounted = true;
  return 0;

 out_db:
  _close_db();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  return r;
}

int KStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;

  _sync();
  _reap_collections();
  coll_map.clear();

  dout(20) << __func__ << " stopping kv thread" << dendl;
  _kv_stop();
  dout(20) << __func__ << " draining finisher" << dendl;
  finisher.wait_for_empty();
  dout(20) << __func__ << " stopping finisher" << dendl;
  finisher.stop();
  dout(20) << __func__ << " closing" << dendl;

  mounted = false;
  _close_db();
  _close_fsid();
  _close_path();
  return 0;
}

int KStore::fsck()
{
  dout(1) << __func__ << dendl;
  int errors = 0;
#if 0
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

  if (bluefs) {
    used_blocks.insert(0, BLUEFS_START);
    used_blocks.insert(bluefs_extents);
    r = bluefs->fsck();
    if (r < 0)
      goto out_alloc;
    if (r > 0)
      errors += r;
  }

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
      for (auto& oid : ols) {
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
	for (auto& b : o->onode.block_map) {
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
	for (auto& v : o->onode.overlay_map) {
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

#endif
  dout(1) << __func__ << " finish with " << errors << " errors" << dendl;
  return errors;
}

void KStore::_sync()
{
  dout(10) << __func__ << dendl;

  std::unique_lock<std::mutex> l(kv_lock);
  while (!kv_committing.empty() ||
	 !kv_queue.empty()) {
    dout(20) << " waiting for kv to commit" << dendl;
    kv_sync_cond.wait(l);
  }

  dout(10) << __func__ << " done" << dendl;
}

int KStore::statfs(struct statfs *buf)
{
  return db->get_statfs(buf);
}

// ---------------
// cache

KStore::CollectionRef KStore::_get_collection(coll_t cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void KStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c->cid << dendl;
  std::lock_guard<std::mutex> l(reap_lock);
  removed_collections.push_back(c);
}

void KStore::_reap_collections()
{
  list<CollectionRef> removed_colls;
  std::lock_guard<std::mutex> l(reap_lock);
  removed_colls.swap(removed_collections);

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

bool KStore::exists(const coll_t& cid, const ghobject_t& oid)
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

int KStore::stat(
    const coll_t& cid,
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

int KStore::read(
  const coll_t& cid,
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

int KStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint32_t op_flags)
{
  int r = 0;
  uint64_t stripe_size = o->onode.stripe_size;
  uint64_t stripe_off;

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << " nid " << o->onode.nid << dendl;
  bl.clear();

  if (offset > o->onode.size) {
    goto out;
  }
  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }
  if (stripe_size == 0) {
    bl.append_zero(length);
    r = length;
    goto out;
  }

  o->flush();

  stripe_off = offset % stripe_size;
  while (length > 0) {
    bufferlist stripe;
    _do_read_stripe(o, offset - stripe_off, &stripe);
    dout(30) << __func__ << " stripe " << offset - stripe_off << " got "
	     << stripe.length() << dendl;
    unsigned swant = MIN(stripe_size - stripe_off, length);
    if (stripe.length()) {
      if (swant == stripe.length()) {
	bl.claim_append(stripe);
	dout(30) << __func__ << " taking full stripe" << dendl;
      } else {
	unsigned l = 0;
	if (stripe_off < stripe.length()) {
	  l = MIN(stripe.length() - stripe_off, swant);
	  bufferlist t;
	  t.substr_of(stripe, stripe_off, l);
	  bl.claim_append(t);
	  dout(30) << __func__ << " taking " << stripe_off << "~" << l << dendl;
	}
	if (l < swant) {
	  bl.append_zero(swant - l);
	  dout(30) << __func__ << " adding " << swant - l << " zeros" << dendl;
	}
      }
    } else {
      dout(30) << __func__ << " generating " << swant << " zeros" << dendl;
      bl.append_zero(swant);
    }
    offset += swant;
    length -= swant;
    stripe_off = 0;
  }
  r = bl.length();
  dout(30) << " result:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

 out:
  return r;
}

int KStore::fiemap(
  const coll_t& cid,
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

  if (offset > o->onode.size)
    goto out;

  if (offset + len > o->onode.size) {
    len = o->onode.size - offset;
  }

  dout(20) << __func__ << " " << offset << "~" << len << " size "
	   << o->onode.size << dendl;

  // FIXME: do something smarter here
  m[0] = o->onode.size;

 out:
  ::encode(m, bl);
  dout(20) << __func__ << " " << offset << "~" << len
	   << " size = 0 (" << m << ")" << dendl;
  return 0;
}

int KStore::getattr(
  const coll_t& cid,
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

int KStore::getattrs(
  const coll_t& cid,
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

int KStore::list_collections(vector<coll_t>& ls)
{
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool KStore::collection_exists(const coll_t& c)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(c);
}

bool KStore::collection_empty(const coll_t& cid)
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

int KStore::collection_list(
  const coll_t& cid, ghobject_t start, ghobject_t end,
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

KStore::OmapIteratorImpl::OmapIteratorImpl(
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

int KStore::OmapIteratorImpl::seek_to_first()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int KStore::OmapIteratorImpl::upper_bound(const string& after)
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

int KStore::OmapIteratorImpl::lower_bound(const string& to)
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

bool KStore::OmapIteratorImpl::valid()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head && it->valid() && it->raw_key().second <= tail) {
    return true;
  } else {
    return false;
  }
}

int KStore::OmapIteratorImpl::next(bool validate)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->next();
    return 0;
  } else {
    return -1;
  }
}

string KStore::OmapIteratorImpl::key()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  decode_omap_key(db_key, &user_key);
  return user_key;
}

bufferlist KStore::OmapIteratorImpl::value()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->value();
}

int KStore::omap_get(
  const coll_t& cid,                ///< [in] Collection containing oid
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

int KStore::omap_get_header(
  const coll_t& cid,                ///< [in] Collection containing oid
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

int KStore::omap_get_keys(
  const coll_t& cid,              ///< [in] Collection containing oid
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
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int KStore::omap_get_values(
  const coll_t& cid,                    ///< [in] Collection containing oid
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

int KStore::omap_check_keys(
  const coll_t& cid,                ///< [in] Collection containing oid
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

ObjectMap::ObjectMapIterator KStore::get_omap_iterator(
  const coll_t& cid,              ///< [in] collection
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

int KStore::_open_super_meta()
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
  return 0;
}

void KStore::_assign_nid(TransContext *txc, OnodeRef o)
{
  if (o->onode.nid)
    return;
  std::lock_guard<std::mutex> l(nid_lock);
  o->onode.nid = ++nid_last;
  dout(20) << __func__ << " " << o->oid << " nid " << o->onode.nid << dendl;
  if (nid_last > nid_max) {
    nid_max += g_conf->kstore_nid_prealloc;
    bufferlist bl;
    ::encode(nid_max, bl);
    txc->t->set(PREFIX_SUPER, "nid_max", bl);
    dout(10) << __func__ << " nid_max now " << nid_max << dendl;
  }
}

KStore::TransContext *KStore::_txc_create(OpSequencer *osr)
{
  TransContext *txc = new TransContext(osr);
  txc->t = db->get_transaction();
  osr->queue_new(txc);
  dout(20) << __func__ << " osr " << osr << " = " << txc << dendl;
  return txc;
}

void KStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    dout(10) << __func__ << " txc " << txc
	     << " " << txc->get_state_name() << dendl;
    switch (txc->state) {
    case TransContext::STATE_PREPARE:
      txc->log_state_latency(logger, l_kstore_state_prepare_lat);
      txc->state = TransContext::STATE_KV_QUEUED;
      if (!g_conf->kstore_sync_transaction) {
	std::lock_guard<std::mutex> l(kv_lock);
	if (g_conf->kstore_sync_submit_transaction) {
	  db->submit_transaction(txc->t);
	}
	kv_queue.push_back(txc);
	kv_cond.notify_one();
	return;
      }
      db->submit_transaction_sync(txc->t);
      break;

    case TransContext::STATE_KV_QUEUED:
      txc->log_state_latency(logger, l_kstore_state_kv_queued_lat);
      txc->state = TransContext::STATE_KV_DONE;
      _txc_finish_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
      txc->log_state_latency(logger, l_kstore_state_kv_done_lat);
      txc->state = TransContext::STATE_FINISHING;
      break;

    case TransContext::TransContext::STATE_FINISHING:
      txc->log_state_latency(logger, l_kstore_state_finishing_lat);
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

int KStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
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

    std::lock_guard<std::mutex> l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }

  return 0;
}

void KStore::_txc_finish_kv(TransContext *txc)
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

void KStore::_txc_finish(TransContext *txc)
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
    if ((*p)->flush_txns.empty()) {
      (*p)->flush_cond.notify_all();
      (*p)->clear_pending_stripes();
    }
  }

  // clear out refs
  txc->onodes.clear();

  while (!txc->removed_collections.empty()) {
    _queue_reap_collection(txc->removed_collections.front());
    txc->removed_collections.pop_front();
  }

  OpSequencerRef osr = txc->osr;
  {
    std::lock_guard<std::mutex> l(osr->qlock);
    txc->state = TransContext::STATE_DONE;
  }

  _osr_reap_done(osr.get());
}

void KStore::_osr_reap_done(OpSequencer *osr)
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
      txc->first_collection->onode_map.trim(g_conf->kstore_onode_map_size);
    }

    osr->q.pop_front();
    txc->log_state_latency(logger, l_kstore_state_done_lat);
    delete txc;
    osr->qcond.notify_all();
    if (osr->q.empty())
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
  }
}

void KStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock<std::mutex> l(kv_lock);
  while (true) {
    assert(kv_committing.empty());
    if (kv_queue.empty()) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_sync_cond.notify_all();
      kv_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      dout(20) << __func__ << " committing " << kv_queue.size() << dendl;
      kv_committing.swap(kv_queue);
      utime_t start = ceph_clock_now(NULL);
      l.unlock();

      dout(30) << __func__ << " committing txc " << kv_committing << dendl;

      // one transaction to force a sync
      KeyValueDB::Transaction t = db->get_transaction();
      if (!g_conf->kstore_sync_submit_transaction) {
	for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	     it != kv_committing.end();
	     ++it) {
	  db->submit_transaction((*it)->t);
	}
      }
      db->submit_transaction_sync(t);
      utime_t finish = ceph_clock_now(NULL);
      utime_t dur = finish - start;
      dout(20) << __func__ << " committed " << kv_committing.size()
	       << " in " << dur << dendl;
      while (!kv_committing.empty()) {
	TransContext *txc = kv_committing.front();
	_txc_state_proc(txc);
	kv_committing.pop_front();
      }

      // this is as good a place as any ...
      _reap_collections();

      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
}


// ---------------------------
// transactions

int KStore::queue_transactions(
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
  int r;

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

  r = _txc_finalize(osr, txc);
  assert(r == 0);

  throttle_ops.get(txc->ops);
  throttle_bytes.get(txc->bytes);

  // execute (start)
  _txc_state_proc(txc);
  return 0;
}

void KStore::_txc_add_transaction(TransContext *txc, Transaction *t)
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
	r = _remove(txc, c, o);
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
	OnodeRef no = c->get_onode(noid, true);
	r = _clone(txc, c, o, no);
      }
      break;

    case Transaction::OP_CLONERANGE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
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
      {
	assert(op->cid == op->dest_cid);
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
	r = _rename(txc, c, o, no, noid);
	o.reset();
      }
      break;

    case Transaction::OP_TRY_RENAME:
      {
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
	r = _rename(txc, c, o, no, noid);
	if (r == -ENOENT)
	  r = 0;
	o.reset();
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

int KStore::_touch(TransContext *txc,
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

void KStore::_dump_onode(OnodeRef o)
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
}

void KStore::_do_read_stripe(OnodeRef o, uint64_t offset, bufferlist *pbl)
{
  map<uint64_t,bufferlist>::iterator p = o->pending_stripes.find(offset);
  if (p == o->pending_stripes.end()) {
    string key;
    get_data_key(o->onode.nid, offset, &key);
    db->get(PREFIX_DATA, key, pbl);
    o->pending_stripes[offset] = *pbl;
  } else {
    *pbl = p->second;
  }
}

void KStore::_do_write_stripe(TransContext *txc, OnodeRef o,
			      uint64_t offset, bufferlist& bl)
{
  o->pending_stripes[offset] = bl;
  string key;
  get_data_key(o->onode.nid, offset, &key);
  txc->t->set(PREFIX_DATA, key, bl);
}

void KStore::_do_remove_stripe(TransContext *txc, OnodeRef o, uint64_t offset)
{
  o->pending_stripes.erase(offset);
  string key;
  get_data_key(o->onode.nid, offset, &key);
  txc->t->rmkey(PREFIX_DATA, key);
}

int KStore::_do_write(TransContext *txc,
		      OnodeRef o,
		      uint64_t offset, uint64_t length,
		      bufferlist& orig_bl,
		      uint32_t fadvise_flags)
{
  int r = 0;

  dout(20) << __func__
	   << " " << o->oid << " " << offset << "~" << length
	   << " - have " << o->onode.size
	   << " bytes, nid " << o->onode.nid << dendl;
  _dump_onode(o);
  o->exists = true;

  if (length == 0) {
    return 0;
  }

  uint64_t stripe_size = o->onode.stripe_size;
  if (!stripe_size) {
    o->onode.stripe_size = g_conf->kstore_default_stripe_size;
    stripe_size = o->onode.stripe_size;
  }

  unsigned bl_off = 0;
  while (length > 0) {
    uint64_t offset_rem = offset % stripe_size;
    uint64_t end_rem = (offset + length) % stripe_size;
    if (offset_rem == 0 && end_rem == 0) {
      bufferlist bl;
      bl.substr_of(orig_bl, bl_off, stripe_size);
      dout(30) << __func__ << " full stripe " << offset << dendl;
      _do_write_stripe(txc, o, offset, bl);
      offset += stripe_size;
      length -= stripe_size;
      bl_off += stripe_size;
      continue;
    }
    uint64_t stripe_off = offset - offset_rem;
    bufferlist prev;
    _do_read_stripe(o, stripe_off, &prev);
    dout(20) << __func__ << " read previous stripe " << stripe_off
	     << ", got " << prev.length() << dendl;
    bufferlist bl;
    if (offset_rem) {
      unsigned p = MIN(prev.length(), offset_rem);
      if (p) {
	dout(20) << __func__ << " reuse leading " << p << " bytes" << dendl;
	bl.substr_of(prev, 0, p);
      }
      if (p < offset_rem) {
	dout(20) << __func__ << " add leading " << offset_rem - p << " zeros" << dendl;
	bl.append_zero(offset_rem - p);
      }
    }
    unsigned use = stripe_size - offset_rem;
    if (use > length)
      use -= stripe_size - end_rem;
    dout(20) << __func__ << " using " << use << " for this stripe" << dendl;
    bufferlist t;
    t.substr_of(orig_bl, bl_off, use);
    bl.claim_append(t);
    bl_off += use;
    if (end_rem) {
      if (end_rem < prev.length()) {
	unsigned l = prev.length() - end_rem;
	dout(20) << __func__ << " reuse trailing " << l << " bytes" << dendl;
	bufferlist t;
	t.substr_of(prev, end_rem, l);
	bl.claim_append(t);
      }
    }
    dout(30) << " writing:\n";
    bl.hexdump(*_dout);
    *_dout << dendl;
    _do_write_stripe(txc, o, stripe_off, bl);
    offset += use;
    length -= use;
  }

  if (offset > o->onode.size) {
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
    o->onode.size = offset;
  }

  return r;
}

int KStore::_write(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   uint64_t offset, size_t length,
		   bufferlist& bl,
		   uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << dendl;
  _assign_nid(txc, o);
  int r = _do_write(txc, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int KStore::_zero(TransContext *txc,
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

  uint64_t stripe_size = o->onode.stripe_size;
  if (stripe_size) {
    uint64_t end = offset + length;
    uint64_t pos = offset;
    uint64_t stripe_off = pos % stripe_size;
    while (pos < offset + length) {
      if (stripe_off || end - pos < stripe_size) {
	bufferlist stripe;
	_do_read_stripe(o, pos - stripe_off, &stripe);
	dout(30) << __func__ << " stripe " << pos - stripe_off << " got "
		 << stripe.length() << dendl;
	bufferlist bl;
	bl.substr_of(stripe, 0, MIN(stripe.length(), stripe_off));
	if (end >= pos - stripe_off + stripe_size ||
	    end >= o->onode.size) {
	  dout(20) << __func__ << " truncated stripe " << pos - stripe_off
		   << " to " << bl.length() << dendl;
	} else {
          auto len = end - (pos - stripe_off + bl.length());
	  bl.append_zero(len);
	  dout(20) << __func__ << " adding " << len << " of zeros" << dendl;
	  if (stripe.length() > bl.length()) {
	    unsigned l = stripe.length() - bl.length();
	    bufferlist t;
	    t.substr_of(stripe, stripe.length() - l, l);
	    dout(20) << __func__ << " keeping tail " << l << " of stripe" << dendl;
	    bl.claim_append(t);
	  }
	}
	_do_write_stripe(txc, o, pos - stripe_off, bl);
	pos += stripe_size - stripe_off;
	stripe_off = 0;
      } else {
	dout(20) << __func__ << " rm stripe " << pos << dendl;
	_do_remove_stripe(txc, o, pos - stripe_off);
	pos += stripe_size;
      }
    }
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

int KStore::_do_truncate(TransContext *txc, OnodeRef o, uint64_t offset)
{
  uint64_t stripe_size = o->onode.stripe_size;

  o->flush();

  // trim down stripes
  if (stripe_size) {
    uint64_t pos = offset;
    uint64_t stripe_off = pos % stripe_size;
    while (pos < o->onode.size) {
      if (stripe_off) {
	bufferlist stripe;
	_do_read_stripe(o, pos - stripe_off, &stripe);
	dout(30) << __func__ << " stripe " << pos - stripe_off << " got "
		 << stripe.length() << dendl;
	bufferlist t;
	t.substr_of(stripe, 0, MIN(stripe_off, stripe.length()));
	_do_write_stripe(txc, o, pos - stripe_off, t);
	dout(20) << __func__ << " truncated stripe " << pos - stripe_off
		 << " to " << t.length() << dendl;
	pos += stripe_size - stripe_off;
	stripe_off = 0;
      } else {
	dout(20) << __func__ << " rm stripe " << pos << dendl;
	_do_remove_stripe(txc, o, pos - stripe_off);
	pos += stripe_size;
      }
    }

    // trim down cached tail
    if (o->tail_bl.length()) {
      if (offset / stripe_size != o->onode.size / stripe_size) {
	dout(20) << __func__ << " clear cached tail" << dendl;
	o->clear_tail();
      }
    }
  }

  o->onode.size = offset;
  dout(10) << __func__ << " truncate size to " << offset << dendl;

  txc->write_onode(o);
  return 0;
}

int KStore::_truncate(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset
	   << dendl;
  int r = _do_truncate(txc, o, offset);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << offset
	   << " = " << r << dendl;
  return r;
}

int KStore::_do_remove(TransContext *txc,
		       OnodeRef o)
{
  string key;

  _do_truncate(txc, o, 0);

  o->onode.size = 0;
  if (o->onode.omap_head) {
    _do_omap_clear(txc, o->onode.omap_head);
  }
  o->exists = false;
  o->onode = kstore_onode_t();
  txc->onodes.erase(o);
  get_object_key(o->oid, &key);
  txc->t->rmkey(PREFIX_OBJ, key);
  return 0;
}

int KStore::_remove(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef &o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = _do_remove(txc, o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int KStore::_setattr(TransContext *txc,
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

int KStore::_setattrs(TransContext *txc,
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


int KStore::_rmattr(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << dendl;
  int r = 0;
  o->onode.attrs.erase(name);
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int KStore::_rmattrs(TransContext *txc,
		     CollectionRef& c,
		     OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  o->onode.attrs.clear();
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

void KStore::_do_omap_clear(TransContext *txc, uint64_t id)
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

int KStore::_omap_clear(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  if (o->onode.omap_head != 0) {
    _do_omap_clear(txc, o->onode.omap_head);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int KStore::_omap_setkeys(TransContext *txc,
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

int KStore::_omap_setheader(TransContext *txc,
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

int KStore::_omap_rmkeys(TransContext *txc,
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

int KStore::_omap_rmkey_range(TransContext *txc,
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

int KStore::_setallochint(TransContext *txc,
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

int KStore::_clone(TransContext *txc,
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

  r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

  // truncate any old data
  r = _do_truncate(txc, newo, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

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

int KStore::_clone_range(TransContext *txc,
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

  r = _do_read(oldo, srcoff, length, bl, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << newo->oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff
	   << " = " << r << dendl;
  return r;
}

int KStore::_rename(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& oldo,
		    OnodeRef& newo,
		    const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << new_oid << dendl;
  int r;
  ghobject_t old_oid = oldo->oid;
  bufferlist bl;
  string old_key, new_key;

  if (newo && newo->exists) {
    // destination object already exists, remove it first
    r = _do_remove(txc, newo);
    if (r < 0)
      goto out;
  }

  txc->t->rmkey(PREFIX_OBJ, oldo->key);
  txc->write_onode(oldo);
  c->onode_map.rename(old_oid, new_oid);  // this adjusts oldo->{oid,key}
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

// collections

int KStore::_create_collection(
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

int KStore::_remove_collection(TransContext *txc, coll_t cid,
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

int KStore::_split_collection(TransContext *txc,
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
