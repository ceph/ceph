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
#if defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include "KStore.h"
#include "osd/osd_types.h"
#include "os/kv.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Formatter.h"
#include "common/pretty_binary.h"

#define dout_context cct
#define dout_subsys ceph_subsys_kstore

/*

  TODO:

  * superblock, features
  * refcounted extents (for efficient clone)

 */

using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;
using ceph::JSONFormatter;

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
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (uint8_t)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (uint8_t)*i);
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

static void get_object_key(CephContext* cct, const ghobject_t& oid,
			   string *key)
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
      ceph_assert(t == oid);
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
  ceph_assert(onode_map.count(oid) == 0);
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

  ceph_assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    lru_list_t::iterator p = lru.iterator_to(*pn->second);
    lru.erase(p);
    onode_map.erase(pn);
  }
  OnodeRef o = po->second;

  // install a non-existent onode it its place
  po->second.reset(new Onode(cct, old_oid, o->key));
  lru.push_back(*po->second);

  // fix oid, key
  onode_map.insert(make_pair(new_oid, o));
  _touch(o);
  o->oid = new_oid;
  get_object_key(cct, new_oid, &o->key);
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
    ceph_assert(p != onode_map.end());
    next->first = p->first;
    next->second = p->second;
    return true;
  }

  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(after);
  ceph_assert(p != onode_map.end()); // for now
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
      ceph_assert(num == 1);
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

KStore::Collection::Collection(KStore *ns, coll_t cid)
  : CollectionImpl(ns->cct, cid),
    store(ns),
    osr(new OpSequencer()),
    onode_map(store->cct)
{
}

void KStore::Collection::flush()
{
  osr->flush();
}

bool KStore::Collection::flush_commit(Context *c)
{
  return osr->flush_commit(c);
}


KStore::OnodeRef KStore::Collection::get_onode(
  const ghobject_t& oid,
  bool create)
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

  OnodeRef o = onode_map.lookup(oid);
  if (o)
    return o;

  string key;
  get_object_key(store->cct, oid, &key);

  ldout(store->cct, 20) << __func__ << " oid " << oid << " key "
			<< pretty_binary_string(key) << dendl;

  bufferlist v;
  int r = store->db->get(PREFIX_OBJ, key, &v);
  ldout(store->cct, 20) << " r " << r << " v.len " << v.length() << dendl;
  Onode *on;
  if (v.length() == 0) {
    ceph_assert(r == -ENOENT);
    if (!create)
      return OnodeRef();

    // new
    on = new Onode(store->cct, oid, key);
    on->dirty = true;
  } else {
    // loaded
    ceph_assert(r >=0);
    on = new Onode(store->cct, oid, key);
    on->exists = true;
    auto p = v.cbegin();
    decode(on->onode, p);
  }
  o.reset(on);
  onode_map.add(oid, o);
  return o;
}



// =======================================================

#undef dout_prefix
#define dout_prefix *_dout << "kstore(" << path << ") "

KStore::KStore(CephContext *cct, const string& path)
  : ObjectStore(cct, path),
    db(NULL),
    basedir(path),
    path_fd(-1),
    fsid_fd(-1),
    mounted(false),
    nid_last(0),
    nid_max(0),
    throttle_ops(cct, "kstore_max_ops", cct->_conf->kstore_max_ops),
    throttle_bytes(cct, "kstore_max_bytes", cct->_conf->kstore_max_bytes),
    finisher(cct),
    kv_sync_thread(this),
    kv_stop(false),
    logger(nullptr)
{
  _init_logger();
}

KStore::~KStore()
{
  _shutdown_logger();
  ceph_assert(!mounted);
  ceph_assert(db == NULL);
  ceph_assert(fsid_fd < 0);
}

void KStore::_init_logger()
{
  // XXX
  PerfCountersBuilder b(cct, "KStore",
                        l_kstore_first, l_kstore_last);
  b.add_time_avg(l_kstore_state_prepare_lat, "state_prepare_lat", "Average prepare state latency");
  b.add_time_avg(l_kstore_state_kv_queued_lat, "state_kv_queued_lat", "Average kv_queued state latency");
  b.add_time_avg(l_kstore_state_kv_done_lat, "state_kv_done_lat", "Average kv_done state latency");
  b.add_time_avg(l_kstore_state_finishing_lat, "state_finishing_lat", "Average finishing state latency");
  b.add_time_avg(l_kstore_state_done_lat, "state_done_lat", "Average done state latency");
  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

void KStore::_shutdown_logger()
{
  // XXX
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

int KStore::_open_path()
{
  ceph_assert(path_fd < 0);
  path_fd = ::open(path.c_str(), O_DIRECTORY|O_CLOEXEC);
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
  ceph_assert(fsid_fd < 0);
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
  ceph_assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());

  string kv_backend;
  if (create) {
    kv_backend = cct->_conf->kstore_backend;
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

  db = KeyValueDB::create(cct, kv_backend, fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    return -EIO;
  }
  string options;
  if (kv_backend == "rocksdb")
    options = cct->_conf->kstore_rocksdb_options;
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
  ceph_assert(db);
  delete db;
  db = NULL;
}

int KStore::_open_collections(int *errors)
{
  ceph_assert(coll_map.empty());
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL);
  for (it->upper_bound(string());
       it->valid();
       it->next()) {
    coll_t cid;
    if (cid.parse(it->key())) {
      auto c = ceph::make_ref<Collection>(this, cid);
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      try {
        decode(c->cnode, p);
      } catch (ceph::buffer::error& e) {
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

  r = write_meta("kv_backend", cct->_conf->kstore_backend);
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

  if (cct->_conf->kstore_fsck_on_mount) {
    int rc = fsck(cct->_conf->kstore_fsck_on_mount_deep);
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
  ceph_assert(mounted);
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

int KStore::fsck(bool deep)
{
  dout(1) << __func__ << dendl;
  int errors = 0;
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

int KStore::statfs(struct store_statfs_t* buf0, osd_alert_list_t* alerts)
{
  struct statfs buf;
  buf0->reset();
  if (alerts) {
    alerts->clear(); // returns nothing for now
  }
  if (::statfs(basedir.c_str(), &buf) < 0) {
    int r = -errno;
    ceph_assert(r != -ENOENT);
    return r;
  }

  buf0->total = buf.f_blocks * buf.f_bsize;
  buf0->available = buf.f_bavail * buf.f_bsize;

  return 0;
}

ObjectStore::CollectionHandle KStore::open_collection(const coll_t& cid)
{
  return _get_collection(cid);
}

ObjectStore::CollectionHandle KStore::create_new_collection(const coll_t& cid)
{
  auto c = ceph::make_ref<Collection>(this, cid);
  std::unique_lock l{coll_lock};
  new_coll_map[cid] = c;
  return c;
}

int KStore::pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
			bool *per_pool_omap)
{
  return -ENOTSUP;
}

// ---------------
// cache

KStore::CollectionRef KStore::_get_collection(coll_t cid)
{
  std::shared_lock l{coll_lock};
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
	ceph_assert(!next.second->exists);
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

bool KStore::exists(CollectionHandle& ch, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return false;
  return true;
}

int KStore::stat(
  CollectionHandle& ch,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return -ENOENT;
  st->st_size = o->onode.size;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int KStore::set_collection_opts(
  CollectionHandle& ch,
  const pool_opts_t& opts)
{
  return -EOPNOTSUPP;
}

int KStore::read(
  CollectionHandle& ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags)
{
  dout(15) << __func__ << " " << ch->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  bl.clear();
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (offset == length && offset == 0)
    length = o->onode.size;

  r = _do_read(o, offset, length, bl, false, op_flags);

 out:
  dout(10) << __func__ << " " << ch->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int KStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    bool do_cache,
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
    _do_read_stripe(o, offset - stripe_off, &stripe, do_cache);
    dout(30) << __func__ << " stripe " << offset - stripe_off << " got "
	     << stripe.length() << dendl;
    unsigned swant = std::min<unsigned>(stripe_size - stripe_off, length);
    if (stripe.length()) {
      if (swant == stripe.length()) {
	bl.claim_append(stripe);
	dout(30) << __func__ << " taking full stripe" << dendl;
      } else {
	unsigned l = 0;
	if (stripe_off < stripe.length()) {
	  l = std::min<uint64_t>(stripe.length() - stripe_off, swant);
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
  CollectionHandle& ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{
  map<uint64_t, uint64_t> m;
  int r = fiemap(ch, oid, offset, len, m);
  if (r >= 0) {
    encode(m, bl);
  }
  return r;
}

int KStore::fiemap(
  CollectionHandle& ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  map<uint64_t, uint64_t>& destmap)
{
  CollectionRef c = static_cast<Collection*>(ch.get());
  if (!c)
    return -ENOENT;
  std::shared_lock l{c->lock};

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
  destmap[0] = o->onode.size;

 out:
  dout(20) << __func__ << " " << offset << "~" << len
	   << " size = 0 (" << destmap << ")" << dendl;
  return 0;
}

int KStore::getattr(
  CollectionHandle& ch,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  dout(15) << __func__ << " " << ch->cid << " " << oid << " " << name << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
  dout(10) << __func__ << " " << ch->cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int KStore::getattrs(
  CollectionHandle& ch,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  aset = o->onode.attrs;
  r = 0;
 out:
  dout(10) << __func__ << " " << ch->cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int KStore::list_collections(vector<coll_t>& ls)
{
  std::shared_lock l{coll_lock};
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool KStore::collection_exists(const coll_t& c)
{
  std::shared_lock l{coll_lock};
  return coll_map.count(c);
}

int KStore::collection_empty(CollectionHandle& ch, bool *empty)
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

int KStore::collection_bits(CollectionHandle& ch)
{
  dout(15) << __func__ << " " << ch->cid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
  dout(10) << __func__ << " " << ch->cid << " = " << c->cnode.bits << dendl;
  return c->cnode.bits;
}

int KStore::collection_list(
  CollectionHandle &c_, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)

{
  Collection *c = static_cast<Collection*>(c_.get());
  c->flush();
  dout(15) << __func__ << " " << c->cid
           << " start " << start << " end " << end << " max " << max << dendl;
  int r;
  {
    std::shared_lock l{c->lock};
    r = _collection_list(c, start, end, max, ls, pnext);
  }

  dout(10) << __func__ << " " << c->cid
    << " start " << start << " end " << end << " max " << max
    << " = " << r << ", ls.size() = " << ls->size()
    << ", next = " << (pnext ? *pnext : ghobject_t())  << dendl;
  return r;
}

int KStore::_collection_list(
  Collection* c, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
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
  if (start == ghobject_t() || start == c->cid.get_min_hobj()) {
    it->upper_bound(temp_start_key);
    temp = true;
  } else {
    string k;
    get_object_key(cct, start, &k);
    if (start.hobj.is_temp()) {
      temp = true;
      ceph_assert(k >= temp_start_key && k < temp_end_key);
    } else {
      temp = false;
      ceph_assert(k >= start_key && k < end_key);
    }
    dout(20) << " start from " << pretty_binary_string(k)
	     << " temp=" << (int)temp << dendl;
    it->lower_bound(k);
  }
  if (end.hobj.is_max()) {
    pend = temp ? temp_end_key : end_key;
  } else {
    get_object_key(cct, end, &end_key);
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
    if (!it->valid() || it->key() >= pend) {
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
    ceph_assert(r == 0);
    if (ls->size() >= (unsigned)max) {
      dout(20) << __func__ << " reached max " << max << dendl;
      *pnext = oid;
      set_next = true;
      break;
    }
    ls->push_back(oid);
    it->next();
  }
out:
  if (!set_next) {
    *pnext = ghobject_t::get_max();
  }
  return r;
}

// omap reads

KStore::OmapIteratorImpl::OmapIteratorImpl(
  CollectionRef c, OnodeRef o, KeyValueDB::Iterator it)
  : c(c), o(o), it(it)
{
  std::shared_lock l{c->lock};
  if (o->onode.omap_head) {
    get_omap_key(o->onode.omap_head, string(), &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
  }
}

int KStore::OmapIteratorImpl::seek_to_first()
{
  std::shared_lock l{c->lock};
  if (o->onode.omap_head) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int KStore::OmapIteratorImpl::upper_bound(const string& after)
{
  std::shared_lock l{c->lock};
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
  std::shared_lock l{c->lock};
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
  std::shared_lock l{c->lock};
  if (o->onode.omap_head && it->valid() && it->raw_key().second <= tail) {
    return true;
  } else {
    return false;
  }
}

int KStore::OmapIteratorImpl::next()
{
  std::shared_lock l{c->lock};
  if (o->onode.omap_head) {
    it->next();
    return 0;
  } else {
    return -1;
  }
}

string KStore::OmapIteratorImpl::key()
{
  std::shared_lock l{c->lock};
  ceph_assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  decode_omap_key(db_key, &user_key);
  return user_key;
}

bufferlist KStore::OmapIteratorImpl::value()
{
  std::shared_lock l{c->lock};
  ceph_assert(it->valid());
  return it->value();
}

int KStore::omap_get(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  dout(15) << __func__ << " " << ch->cid << " oid " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
	ceph_assert(it->key() < tail);
	(*out)[user_key] = it->value();
      }
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << ch->cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int KStore::omap_get_header(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  dout(15) << __func__ << " " << ch->cid << " oid " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
  dout(10) << __func__ << " " << ch->cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int KStore::omap_get_keys(
  CollectionHandle& ch,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  dout(15) << __func__ << " " << ch->cid << " oid " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
      ceph_assert(it->key() < tail);
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << ch->cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int KStore::omap_get_values(
  CollectionHandle& ch,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  dout(15) << __func__ << " " << ch->cid << " oid " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
  dout(10) << __func__ << " " << ch->cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int KStore::omap_check_keys(
  CollectionHandle& ch,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  dout(15) << __func__ << " " << ch->cid << " oid " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
  dout(10) << __func__ << " " << ch->cid << " oid " << oid << " = " << r << dendl;
  return r;
}

ObjectMap::ObjectMapIterator KStore::get_omap_iterator(
  CollectionHandle& ch,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{

  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  std::shared_lock l{c->lock};
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
    auto p = bl.cbegin();
    try {
      decode(nid_max, p);
    } catch (ceph::buffer::error& e) {
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
    nid_max += cct->_conf->kstore_nid_prealloc;
    bufferlist bl;
    encode(nid_max, bl);
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
      if (!cct->_conf->kstore_sync_transaction) {
	std::lock_guard<std::mutex> l(kv_lock);
	if (cct->_conf->kstore_sync_submit_transaction) {
          int r = db->submit_transaction(txc->t);
	  ceph_assert(r == 0);
	}
	kv_queue.push_back(txc);
	kv_cond.notify_one();
	return;
      }
      {
	int r = db->submit_transaction_sync(txc->t);
	ceph_assert(r == 0);
      }
      break;

    case TransContext::STATE_KV_QUEUED:
      txc->log_state_latency(logger, l_kstore_state_kv_queued_lat);
      txc->state = TransContext::STATE_KV_DONE;
      _txc_finish_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
      txc->log_state_latency(logger, l_kstore_state_kv_done_lat);
      txc->state = TransContext::STATE_FINISHING;
      // ** fall-thru **

    case TransContext::TransContext::STATE_FINISHING:
      txc->log_state_latency(logger, l_kstore_state_finishing_lat);
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

void KStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc
	   << " onodes " << txc->onodes << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    encode((*p)->onode, bl);
    dout(20) << " onode size is " << bl.length() << dendl;
    txc->t->set(PREFIX_OBJ, (*p)->key, bl);

    std::lock_guard<std::mutex> l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }
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
  if (!txc->oncommits.empty()) {
    finisher.queue(txc->oncommits);
  }

  throttle_ops.put(txc->ops);
  throttle_bytes.put(txc->bytes);
}

void KStore::_txc_finish(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
  ceph_assert(txc->state == TransContext::STATE_FINISHING);

  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    std::lock_guard<std::mutex> l((*p)->flush_lock);
    dout(20) << __func__ << " onode " << *p << " had " << (*p)->flush_txns
	     << dendl;
    ceph_assert((*p)->flush_txns.count(txc));
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
      txc->first_collection->onode_map.trim(cct->_conf->kstore_onode_map_size);
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
    ceph_assert(kv_committing.empty());
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
      utime_t start = ceph_clock_now();
      l.unlock();

      dout(30) << __func__ << " committing txc " << kv_committing << dendl;

      // one transaction to force a sync
      KeyValueDB::Transaction t = db->get_transaction();
      if (!cct->_conf->kstore_sync_submit_transaction) {
	for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	     it != kv_committing.end();
	     ++it) {
	  int r = db->submit_transaction((*it)->t);
	  ceph_assert(r == 0);
	}
      }
      int r = db->submit_transaction_sync(t);
      ceph_assert(r == 0);
      utime_t finish = ceph_clock_now();
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
  CollectionHandle& ch,
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
  Collection *c = static_cast<Collection*>(ch.get());
  OpSequencer *osr = c->osr.get();
  dout(10) << __func__ << " ch " << ch.get() << " " << c->cid << dendl;

  // prepare
  TransContext *txc = _txc_create(osr);
  txc->onreadable = onreadable;
  txc->onreadable_sync = onreadable_sync;
  txc->oncommit = ondisk;

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    txc->ops += (*p).get_num_ops();
    txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }

  _txc_finalize(osr, txc);

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
	ceph_assert(!c);
        coll_t cid = i.get_cid(op->cid);
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
        uint32_t type = op->hint_type;
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
      derr << " error " << cpp_strerror(r)
	   << " not handled on operation " << op->op
	   << " (op " << pos << ", counting from 0)" << dendl;
      dout(0) << " transaction dump:\n";
      JSONFormatter f(true);
      f.open_object_section("transaction");
      t->dump(&f);
      f.close_section();
      f.flush(*_dout);
      *_dout << dendl;
      ceph_abort_msg("unexpected error");
    }

    // object operations
    std::unique_lock l{c->lock};
    OnodeRef &o = ovec[op->oid];
    if (!o) {
      // these operations implicity create the object
      bool create = false;
      if (op->op == Transaction::OP_TOUCH ||
	  op->op == Transaction::OP_CREATE ||
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
    case Transaction::OP_CREATE:
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
      ceph_abort_msg("deprecated");
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
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_REMOVE:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_MOVE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	ceph_assert(op->cid == op->dest_cid);
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
	uint32_t flags = op->alloc_hint_flags;
	r = _setallochint(txc, c, o,
			  expected_object_size,
			  expected_write_size,
			  flags);
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
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
	  msg = "ENOSPC from key value store, misconfigured cluster";

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
	ceph_abort_msg("unexpected error");
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

void KStore::_do_read_stripe(OnodeRef o, uint64_t offset, bufferlist *pbl, bool do_cache)
{
  if (!do_cache) {
    string key;
    get_data_key(o->onode.nid, offset, &key);
    db->get(PREFIX_DATA, key, pbl);
    return;
  }
 
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
    o->onode.stripe_size = cct->_conf->kstore_default_stripe_size;
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
    _do_read_stripe(o, stripe_off, &prev, true);
    dout(20) << __func__ << " read previous stripe " << stripe_off
	     << ", got " << prev.length() << dendl;
    bufferlist bl;
    if (offset_rem) {
      unsigned p = std::min<uint64_t>(prev.length(), offset_rem);
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
	_do_read_stripe(o, pos - stripe_off, &stripe, true);
	dout(30) << __func__ << " stripe " << pos - stripe_off << " got "
		 << stripe.length() << dendl;
	bufferlist bl;
	bl.substr_of(stripe, 0, std::min<uint64_t>(stripe.length(), stripe_off));
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
	_do_read_stripe(o, pos - stripe_off, &stripe, true);
	dout(30) << __func__ << " stripe " << pos - stripe_off << " got "
		 << stripe.length() << dendl;
	bufferlist t;
	t.substr_of(stripe, 0, std::min<uint64_t>(stripe_off, stripe.length()));
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
  get_object_key(cct, o->oid, &key);
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
  auto p = bl.cbegin();
  __u32 num;
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    decode(key, p);
    decode(value, p);
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
			 const bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  auto p = bl.cbegin();
  __u32 num;

  if (!o->onode.omap_head) {
    r = 0;
    goto out;
  }
  decode(num, p);
  while (num--) {
    string key;
    decode(key, p);
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
			  uint64_t expected_write_size,
			  uint32_t flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " flags " << flags
	   << dendl;
  int r = 0;
  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  o->onode.alloc_hint_flags = flags;

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

  r = _do_read(oldo, 0, oldo->onode.size, bl, true, 0);
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
	ceph_assert(it->key() < tail);
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

  r = _do_read(oldo, srcoff, length, bl, true, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);
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
    std::unique_lock l{coll_lock};
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    auto p = new_coll_map.find(cid);
    ceph_assert(p != new_coll_map.end());
    *c = p->second;
    ceph_assert((*c)->cid == cid);
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

int KStore::_remove_collection(TransContext *txc, coll_t cid,
				 CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;

  {
    std::unique_lock l{coll_lock};
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    size_t nonexistent_count = 0;
    pair<ghobject_t,OnodeRef> next_onode;
    while ((*c)->onode_map.get_next(next_onode.first, &next_onode)) {
      if (next_onode.second->exists) {
	r = -ENOTEMPTY;
	goto out;
      }
      ++nonexistent_count;
    }
    vector<ghobject_t> ls;
    ghobject_t next;
    // Enumerate onodes in db, up to nonexistent_count + 1
    // then check if all of them are marked as non-existent.
    // Bypass the check if returned number is greater than nonexistent_count
    r = _collection_list(c->get(), ghobject_t(), ghobject_t::get_max(),
                         nonexistent_count + 1, &ls, &next);
    if (r >= 0) {
      bool exists = false; //ls.size() > nonexistent_count;
      for (auto it = ls.begin(); !exists && it < ls.end(); ++it) {
        dout(10) << __func__ << " oid " << *it << dendl;
        auto onode = (*c)->onode_map.lookup(*it);
        exists = !onode || onode->exists;
        if (exists) {
          dout(10) << __func__ << " " << *it
                   << " exists in db" << dendl;
        }
      }
      if (!exists) {
        coll_map.erase(cid);
        txc->removed_collections.push_back(*c);
        c->reset();
        txc->t->rmkey(PREFIX_COLL, stringify(cid));
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

int KStore::_split_collection(TransContext *txc,
				CollectionRef& c,
				CollectionRef& d,
				unsigned bits, int rem)
{
  dout(15) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << dendl;
  int r;
  std::unique_lock l{c->lock};
  std::unique_lock l2{d->lock};
  c->onode_map.clear();
  d->onode_map.clear();
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

int KStore::_merge_collection(TransContext *txc,
			      CollectionRef *c,
			      CollectionRef& d,
			      unsigned bits)
{
  dout(15) << __func__ << " " << (*c)->cid << " to " << d->cid << " "
	   << " bits " << bits << dendl;
  int r;
  std::scoped_lock l{(*c)->lock, d->lock};
  (*c)->onode_map.clear();
  d->onode_map.clear();
  d->cnode.bits = bits;
  r = 0;

  coll_t cid = (*c)->cid;

  bufferlist bl;
  encode(d->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(d->cid), bl);

  coll_map.erase((*c)->cid);
  txc->removed_collections.push_back(*c);
  c->reset();
  txc->t->rmkey(PREFIX_COLL, stringify(cid));

  dout(10) << __func__ << " " << cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

// ===========================================
