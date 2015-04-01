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

#include "NewStore.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#define dout_subsys ceph_subsys_newstore

/*

  TODO:

  * hobject sorting
      - backfill
      - scrub
      - pgnls
      - tiering agent position
      - ObjectStore::collection_list_range
      - ObjectStore::collection_list_partial
      - DBObjectMap::clone lock ordering
      - HashIndex::get_path_contents_by_hash
      - HashIndex::list_by_hash
  * open-by-handle
  * use work queue for wal fsyncs and kv record removals
  * avoid mtime updates when doing open-by-handle
  * abstract out fs specifics
  * fid xattr backpointer
  * kill collection_list_range
  * inline first fsync_item in TransContext to void allocation?

 */

const string PREFIX_SUPER = "S"; // field -> value
const string PREFIX_COLL = "C"; // collection name -> (nothing)
const string PREFIX_OBJ = "O";  // object name -> onode
const string PREFIX_OMAP = "M"; // u64 + keyname -> value
const string PREFIX_WAL = "L";  // write ahead log


/*
 * key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '%' with %
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a separator for strings; this works because it is < %
 * and will get escaped if it is present in the string.
 *
 * For the fixed length numeric fields, we just use hex and '.' as a
 * convenient visual separator.  Two oddities here:
 *
 *   1. for the -1 shard value we use --; it's the only negative value
 *      and it sorts < 0 that way.
 *
 *   2. for the pool value, we add 2^63 so that it sorts correctly
 *
 * We could do something much more compact here, but it would be less
 * readable by humans.  :/
 */

const string KEY_SEP_S = "!";

static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '%') {
      snprintf(hexbyte, sizeof(hexbyte), "%%%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= 126) {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
}

static int decode_escaped(const char *p, string *out)
{
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '%' || *p == '~') {
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

// here is a sample (large) key
// --.7fffffffffffffff.B9FA767A.!0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!fffffffffffffffe.ffffffffffffffff

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
    char buf[PATH_MAX];

    // make field ordering match with ghobject_t compare operations
    if (pgid.shard == shard_id_t::NO_SHARD) {
      // otherwise ff will sort *after* 0, not before.
      *start = "--";
    } else {
      snprintf(buf, sizeof(buf), "%02x", (int)pgid.shard);
      start->append(buf);
    }
    *end = *start;
    *temp_start = *start;
    *temp_end = *start;

    snprintf(buf, sizeof(buf), ".%016llx.%08x.",
	     (unsigned long long)(pgid.pool() + 0x8000000000000000ull),
	     (unsigned)hobject_t::_reverse_nibbles(pgid.ps()));
    start->append(buf);
    snprintf(buf, sizeof(buf), ".%016llx.%08x.",
	     (unsigned long long)((-1ll - pgid.pool()) + 0x8000000000000000ull),
	     (unsigned)hobject_t::_reverse_nibbles(pgid.ps()));
    temp_start->append(buf);

    uint64_t end_hash = hobject_t::_reverse_nibbles(pgid.ps());
    end_hash += (1ull << (32-bits));
    if (end_hash > 0xffffffff) {
      snprintf(buf, sizeof(buf), ".%016llx.gggggggg.",
	       (unsigned long long)(pgid.pool() + 0x8000000000000000ull));
      end->append(buf);
      snprintf(buf, sizeof(buf), ".%016llx.gggggggg.",
	       (unsigned long long)((-1ll - pgid.pool()) + 0x8000000000000000ull));
      temp_end->append(buf);
    } else {
      snprintf(buf, sizeof(buf), ".%016llx.%08x.",
	       (unsigned long long)(pgid.pool() + 0x8000000000000000ull),
	       (unsigned)end_hash);
      end->append(buf);
      snprintf(buf, sizeof(buf), ".%016llx.%08x.",
	       (unsigned long long)((-1ll - pgid.pool()) + 0x8000000000000000ull),
	       (unsigned)end_hash);
      temp_end->append(buf);
    }
  } else if (cid.is_meta()) {
    *start = "--.7fffffffffffffff.00000000.";
    *end =   "--.7fffffffffffffff.gggggggg.";
    // no separate temp section
    *temp_start = *end;
    *temp_end = *end;
  } else {
    assert(0);
  }
}

static int get_key_object(const string& key, ghobject_t *oid);

static void get_object_key(const ghobject_t& oid, string *key)
{
  char buf[PATH_MAX];
  char *t = buf;
  char *end = t + sizeof(buf);

  key->clear();

  // make field ordering match with ghobject_t compare operations
  if (oid.shard_id == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    *key = "--";
  } else {
    snprintf(buf, sizeof(buf), "%02x", (int)oid.shard_id);
    key->append(buf);
  }

  t += snprintf(t, end - t, ".%016llx.%.*x.",
		(unsigned long long)(oid.hobj.pool + 0x8000000000000000ull),
		(int)(sizeof(oid.hobj.get_hash())*2),
		(uint32_t)oid.get_filestore_key_u32());
  key->append(buf);

  append_escaped(oid.hobj.nspace, key);
  key->append(KEY_SEP_S);

  append_escaped(oid.hobj.get_effective_key(), key);
  key->append(KEY_SEP_S);

  append_escaped(oid.hobj.oid.name, key);
  key->append(KEY_SEP_S);

  t = buf;
  t += snprintf(t, end - t, "%016llx.%016llx",
		(long long unsigned)oid.hobj.snap,
		(long long unsigned)oid.generation);
  key->append(buf);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << *key << dendl;
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

  if (key[0] == '-') {
    oid->shard_id = shard_id_t::NO_SHARD;
  } else {
    unsigned shard;
    r = sscanf(p, "%x", &shard);
    if (r < 1)
      return -1;
    oid->shard_id = shard_id_t(shard);
  }
  if (p[2] != '.' || p[19] != '.' || p[28] != '.')
    return -2;

  unsigned hash;
  uint64_t pool;
  r = sscanf(p + 3, "%llx.%x", (unsigned long long*)&pool, &hash);
  if (r < 2)
    return -3;
  oid->hobj.pool = pool - 0x8000000000000000;
  oid->hobj.set_filestore_key_u32(hash);
  p += 3 + 2 + 16 + 8;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -4;
  p += r + 1;
  string okey;
  r = decode_escaped(p, &okey);
  if (r < 0)
    return -5;
  p += r + 1;
  r = decode_escaped(p, &oid->hobj.oid.name);
  if (r < 0)
    return -6;
  p += r + 1;

  oid->hobj.set_key(okey);

  r = sscanf(p, "%llx.%llx", (unsigned long long*)&oid->hobj.snap,
	     (unsigned long long*)&oid->generation);
  if (r < 2)
    return -7;
  return 0;
}

// '-' < '.' < '~'
void get_omap_header(uint64_t id, string *out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx-", (unsigned long long)id);
  *out = buf;
}

// hmm, I don't think there's any need to escape the user key since we
// have a clean prefix.
void get_omap_key(uint64_t id, const string& key, string *out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx.", (unsigned long long)id);
  *out = buf;
  out->append(key);
}

void rewrite_omap_key(uint64_t id, string old, string *out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)id);
  *out = buf;
  out->append(old.substr(16));
}

void decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(17);
}

void get_omap_tail(uint64_t id, string *out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx~", (unsigned long long)id);
  *out = buf;
}

void get_wal_key(uint64_t seq, string *out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)seq);
  *out = buf;
}

// Onode

NewStore::Onode::Onode(const ghobject_t& o, const string& k)
  : nref(0),
    oid(o),
    key(k),
    dirty(false),
    exists(true),
    flush_lock("NewStore::Onode::flush_lock") {
}

// OnodeHashLRU

#undef dout_prefix
#define dout_prefix *_dout << "newstore.lru(" << this << ") "

void NewStore::OnodeHashLRU::_touch(OnodeRef o)
{
  lru_list_t::iterator p = lru.iterator_to(*o);
  lru.erase(p);
  lru.push_front(*o);
}

void NewStore::OnodeHashLRU::add(const ghobject_t& oid, OnodeRef o)
{
  Mutex::Locker l(lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  lru.push_back(*o);
}

NewStore::OnodeRef NewStore::OnodeHashLRU::lookup(const ghobject_t& oid)
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

void NewStore::OnodeHashLRU::clear()
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << dendl;
  lru.clear();
  onode_map.clear();
}

void NewStore::OnodeHashLRU::remove(const ghobject_t& oid)
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

void NewStore::OnodeHashLRU::rename(const ghobject_t& old_oid,
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

bool NewStore::OnodeHashLRU::get_next(
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

int NewStore::OnodeHashLRU::trim(int max)
{
  Mutex::Locker l(lock);
  dout(20) << __func__ << " max " << max
	   << " size " << onode_map.size() << dendl;
  int trimmed = 0;
  int num = onode_map.size() - max;
  lru_list_t::iterator p = lru.end();
  if (num)
    p--;
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
#define dout_prefix *_dout << "newstore(" << store->path << ").collection(" << cid << ") "

NewStore::Collection::Collection(NewStore *ns, coll_t c)
  : store(ns),
    cid(c),
    lock("NewStore::Collection::lock"),
    onode_map()
{
}

NewStore::OnodeRef NewStore::Collection::get_onode(
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

  dout(20) << __func__ << " oid " << oid << " key '" << key << "'" << dendl;

  bufferlist v;
  int r = store->db->get(PREFIX_OBJ, key, &v);
  dout(20) << " r " << r << " v.len " << v.length() << dendl;
  Onode *on;
  assert(r >= 0);
  if (v.length() == 0) {
    if (!create)
      return OnodeRef();

    // new
    on = new Onode(oid, key);
    on->dirty = true;
  } else {
    // loaded
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
#define dout_prefix *_dout << "newstore(" << path << ") "


NewStore::NewStore(CephContext *cct, const string& path)
  : ObjectStore(path),
    cct(cct),
    db(NULL),
    path_fd(-1),
    fsid_fd(-1),
    frag_fd(-1),
    fset_fd(-1),
    mounted(false),
    coll_lock("NewStore::coll_lock"),
    fid_lock("NewStore::fid_lock"),
    wal_lock("NewStore::wal_lock"),
    wal_seq(0),
    finisher(cct),
    fsync_tp(cct,
	     "NewStore::fsync_tp",
	     cct->_conf->newstore_fsync_threads,
	     "newstore_fsync_threads"),
    fsync_wq(this,
	     cct->_conf->newstore_fsync_thread_timeout,
	     cct->_conf->newstore_fsync_thread_suicide_timeout,
	     &fsync_tp),
    kv_sync_thread(this),
    kv_lock("NewStore::kv_lock"),
    kv_stop(false),
    logger(NULL),
    default_osr("default"),
    reap_lock("NewStore::reap_lock")
{
  _init_logger();
}

NewStore::~NewStore()
{
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(fsid_fd < 0);
  assert(frag_fd < 0);
}

void NewStore::_init_logger()
{
  // XXX
}

void NewStore::_shutdown_logger()
{
  // XXX
}

int NewStore::peek_journal_fsid(uuid_d *fsid)
{
  return 0;
}

int NewStore::_open_path()
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

void NewStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
}

int NewStore::_open_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int NewStore::_create_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0 && errno == ENOENT) {
    int r = ::mkdirat(path_fd, "fragments", 0755);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " cannot create " << path << "/fragments: "
	   << cpp_strerror(r) << dendl;
      return r;
    }
    frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  }
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open created " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void NewStore::_close_frag()
{
  if (fset_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
    fset_fd = -1;
  }
  VOID_TEMP_FAILURE_RETRY(::close(frag_fd));
  frag_fd = -1;
}

int NewStore::_open_fsid(bool create)
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

int NewStore::_read_fsid(uuid_d *uuid)
{
  char fsid_str[40];
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret > 36)
    fsid_str[36] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int NewStore::_write_fsid()
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

void NewStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int NewStore::_lock_fsid()
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

bool NewStore::test_mount_in_use()
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

int NewStore::_open_db()
{
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());
  db = KeyValueDB::create(g_ceph_context,
			  g_conf->newstore_backend,
			  fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  db->init();
  stringstream err;
  if (db->create_and_open(err)) {
    derr << __func__ << " erroring opening db: " << err << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  return 0;
}

void NewStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

int NewStore::_open_collections()
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
      dout(20) << __func__ << " unrecognized collection " << it->key() << dendl;
    }
  }
  return 0;
}

int NewStore::mkfs()
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

  r = _create_frag();
  if (r < 0)
    goto out_close_fsid;

  r = _open_db();
  if (r < 0)
    goto out_close_frag;

  // FIXME: superblock

  dout(10) << __func__ << " success" << dendl;
  r = 0;
  _close_db();

 out_close_frag:
  _close_frag();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}

int NewStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

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

  r = _open_frag();
  if (r < 0)
    goto out_fsid;

  // FIXME: superblock, features

  r = _open_db();
  if (r < 0)
    goto out_frag;

  r = _recover_next_fid();
  if (r < 0)
    goto out_db;

  r = _recover_next_omap_id();
  if (r < 0)
    goto out_db;

  r = _open_collections();
  if (r < 0)
    goto out_db;

  r = _replay_wal();
  if (r < 0)
    goto out_db;

  finisher.start();
  fsync_tp.start();
  kv_sync_thread.create();

  mounted = true;
  return 0;

 out_db:
  _close_db();
 out_frag:
  _close_frag();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  return r;
}

int NewStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;

  sync_and_flush();
  _reap_collections();

  dout(20) << __func__ << " stopping fsync_wq" << dendl;
  fsync_tp.stop();
  dout(20) << __func__ << " stopping kv thread" << dendl;
  _kv_stop();
  dout(20) << __func__ << " draining finisher" << dendl;
  finisher.wait_for_empty();
  dout(20) << __func__ << " stopping finisher" << dendl;
  finisher.stop();
  dout(20) << __func__ << " closing" << dendl;

  mounted = false;
  if (fset_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
  _close_db();
  _close_frag();
  _close_fsid();
  _close_path();
  return 0;
}

void NewStore::sync(Context *onsync)
{
#warning write sync
}

void NewStore::sync()
{
#warning write sync
}

void NewStore::flush()
{
#warning write flush
}

void NewStore::sync_and_flush()
{
  dout(10) << __func__ << dendl;

  dout(20) << " flushing fsync wq" << dendl;
  fsync_wq.flush();

  kv_lock.Lock();
  while (!kv_committing.empty() ||
	 !kv_queue.empty()) {
    dout(20) << " waiting for kv to commit" << dendl;
    kv_sync_cond.Wait(kv_lock);
  }
  kv_lock.Unlock();

  dout(10) << __func__ << " done" << dendl;
}

int NewStore::statfs(struct statfs *buf)
{
  if (::statfs(path.c_str(), buf) < 0) {
    int r = -errno;
    assert(!g_conf->newstore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

// ---------------
// cache

NewStore::CollectionRef NewStore::_get_collection(coll_t cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void NewStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c->cid << dendl;
  Mutex::Locker l(reap_lock);
  removed_collections.push_back(c);
}

void NewStore::_reap_collections()
{
  Mutex::Locker l(reap_lock);
  for (list<CollectionRef>::iterator p = removed_collections.begin();
       p != removed_collections.end();
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

bool NewStore::exists(coll_t cid, const ghobject_t& oid)
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

int NewStore::stat(
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

int NewStore::read(
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

int NewStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint32_t op_flags)
{
  map<uint64_t,fragment_t>::iterator p;
  int r;
  int fd = -1;
  fid_t cur_fid;

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << dendl;

  if (offset > o->onode.size) {
    r = 0;
    goto out;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->flush();

  r = 0;

  p = o->onode.data_map.begin();   // fixme
  if (p->first > offset && p != o->onode.data_map.begin()) {
    --p;
  }
  for ( ; length > 0 && p != o->onode.data_map.end(); ++p) {
    assert(p->first == 0);
    assert(p->second.offset == 0);
    assert(p->second.length == o->onode.size);
    dout(30) << __func__ << " x " << p->first << "~" << p->second.length
	     << " in " << p->second.fid << dendl;
    if (p->first + p->second.length <= offset) {
      dout(30) << __func__ << " skipping " << p->first << "~" << p->second.length
	       << dendl;
      continue;
    }
    if (p->first > offset) {
      unsigned l = p->first - offset;
      dout(30) << __func__ << " zero " << offset << "~" << l << dendl;
      bufferptr bp(l);
      bp.zero();
      bl.append(bp);
      length = length - l;
    }
    if (p->second.fid != cur_fid) {
      cur_fid = p->second.fid;
      if (fd >= 0) {
	VOID_TEMP_FAILURE_RETRY(::close(fd));
      }
      fd = _open_fid(cur_fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
    }
    unsigned x_off;
    if (p->first < offset) {
      x_off = offset - p->first;
    } else {
      x_off = 0;
    }
    unsigned x_len = MIN(length, p->second.length - x_off);
    dout(30) << __func__ << " data " << offset << "~" << x_len
	     << " fid " << cur_fid << " offset " << x_off + p->second.offset
	     << dendl;
    r = ::lseek64(fd, p->second.offset + x_off, SEEK_SET);
    if (r < 0) {
      r = -errno;
      goto out;
    }
    bufferlist t;
    r = t.read_fd(fd, x_len);
    if (r < 0) {
      goto out;
    }
    bl.claim_append(t);
    if ((unsigned)r < x_len) {
      dout(10) << __func__ << "   short read " << r << " < " << x_len
	       << " from " << cur_fid << " offset " << p->second.offset + x_off
	       << dendl;
      bufferptr z(x_len - r);
      z.zero();
      bl.append(z);
    }
    offset += x_len;
    length -= x_len;
  }
  if (length > 0 && p == o->onode.data_map.end()) {
    dout(30) << __func__ << " trailing zero " << offset << "~" << length << dendl;
    bufferptr bp(length);
    bp.zero();
    bl.push_back(bp);
  }
  r = bl.length();
 out:
  if (fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  return r;
}


int NewStore::fiemap(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{
  assert(0);
}

int NewStore::getattr(
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

int NewStore::getattrs(
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

int NewStore::list_collections(vector<coll_t>& ls)
{
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool NewStore::collection_exists(coll_t c)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(c);
}

bool NewStore::collection_empty(coll_t cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list_partial(cid, ghobject_t(), 5, 5, CEPH_NOSNAP,
				  &ls, &next);
  if (r < 0)
    return false;  // fixme?
  bool empty = ls.empty();
  dout(10) << __func__ << " " << cid << " = " << (int)empty << dendl;
  return empty;
}

int NewStore::collection_list(coll_t cid, vector<ghobject_t>& o)
{
  dout(15) << __func__ << " " << cid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OBJ);
  string temp_start_key, temp_end_key;
  string start_key, end_key;
  bool temp = true;
  const char *end;
  get_coll_key_range(cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__ << " range " << temp_start_key << " to " << temp_end_key
	   << " and " << start_key << " to " << end_key << dendl;
  end = temp_start_key.c_str();
  it->upper_bound(temp_start_key);
  while (true) {
    if (!it->valid() || strcmp(it->key().c_str(), end) > 0) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " key " << it->key() << " > " << end << dendl;
      if (temp) {
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(start_key);
	end = end_key.c_str();
	continue;
      }
      break;
    }
    dout(20) << __func__ << " key " << it->key() << dendl;
    ghobject_t oid;
    int r = get_key_object(it->key(), &oid);
    assert(r == 0);
    o.push_back(oid);
    it->next();
  }
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

int NewStore::collection_list_partial(
  coll_t cid, ghobject_t start,
  int min, int max, snapid_t snap,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " min/max " << min << "/" << max
	   << " snap " << snap << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  KeyValueDB::Iterator it;
  string temp_start_key, temp_end_key;
  string start_key, end_key;
  bool set_next = false;
  const char *end;
  bool temp;

  if (start == ghobject_t::get_max())
    goto out;
  get_coll_key_range(cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__ << " range " << temp_start_key << " to "
	   << temp_end_key << " and " << start_key << " to " << end_key
	   << " start " << start << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (start == ghobject_t()) {
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
    it->upper_bound(k);
  }
  end = temp ? temp_end_key.c_str() : end_key.c_str();
  while (true) {
    if (!it->valid() || strcmp(it->key().c_str(), end) > 0) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " key " << it->key() << " > " << end << dendl;
      if (temp) {
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(start_key);
	end = end_key.c_str();
	continue;
      }
      break;
    }
    dout(20) << __func__ << " key " << it->key() << dendl;
    ghobject_t oid;
    int r = get_key_object(it->key(), &oid);
    assert(r == 0);
    ls->push_back(oid);
    if (ls->size() >= (unsigned)max) {
      *pnext = oid;
      set_next = true;
      break;
    }
    it->next();
  }
  if (!set_next) {
    *pnext = ghobject_t::get_max();
  }
 out:
  dout(10) << __func__ << " " << cid
	   << " start " << start << " min/max " << min << "/" << max
	   << " snap " << snap << " = " << r << ", ls.size() = " << ls->size()
	   << ", next = " << *pnext << dendl;
  return r;
}

int NewStore::collection_list_range(
  coll_t cid, ghobject_t start, ghobject_t end,
  snapid_t seq, vector<ghobject_t> *ls)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " end " << end
	   << " snap " << seq << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  KeyValueDB::Iterator it;
  string temp_start_key, temp_end_key;
  string start_key, end_key;
  string end_str;
  const char *pend;
  bool temp;

  if (start == ghobject_t::get_max() || end == ghobject_t())
    goto out;
  get_coll_key_range(cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__ << " range " << temp_start_key << " to "
	   << temp_end_key << " and " << start_key << " to " << end_key
	   << " start " << start << " end " << end << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (start == ghobject_t()) {
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
    it->upper_bound(k);
  }
  get_object_key(end, &end_str);
  if (end.hobj.is_temp()) {
    if (temp)
      pend = end_str.c_str();
    else
      goto out;
  } else {
    pend = temp ? temp_end_key.c_str() : end_str.c_str();
  }
  while (true) {
    if (!it->valid() || strcmp(it->key().c_str(), pend) > 0) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " key " << it->key() << " > " << pend << dendl;
      if (temp) {
	if (end.hobj.is_temp()) {
	  break;
	}
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(start_key);
	pend = end_str.c_str();
	continue;
      }
      break;
    }
    dout(20) << __func__ << " key " << it->key() << dendl;
    ghobject_t oid;
    int r = get_key_object(it->key(), &oid);
    assert(r == 0);
    ls->push_back(oid);
    it->next();
  }
 out:
  dout(10) << __func__ << " " << cid
	   << " start " << start << " end " << end
	   << " snap " << seq << " = " << r << dendl;
  return r;
}

// omap reads

NewStore::OmapIteratorImpl::OmapIteratorImpl(CollectionRef c, OnodeRef o)
  : c(c), o(o)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    get_omap_header(o->onode.omap_head, &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->upper_bound(head);
  }
}

int NewStore::OmapIteratorImpl::seek_to_first()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->upper_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int NewStore::OmapIteratorImpl::upper_bound(const string& after)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, after, &key);
    it->upper_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int NewStore::OmapIteratorImpl::lower_bound(const string& to)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, to, &key);
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

bool NewStore::OmapIteratorImpl::valid()
{
  RWLock::RLocker l(c->lock);
  return it->valid();
}

int NewStore::OmapIteratorImpl::next()
{
  RWLock::RLocker l(c->lock);
  it->next();
  if (!it->valid() || it->key() == tail) {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

string NewStore::OmapIteratorImpl::key()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->key();
}

bufferlist NewStore::OmapIteratorImpl::value()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->value();
}

int NewStore::omap_get(
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
      } else if (it->key() == tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	string user_key;
	decode_omap_key(it->key(), &user_key);
	dout(30) << __func__ << "  got " << it->key() << " -> " << user_key << dendl;
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

int NewStore::omap_get_header(
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

int NewStore::omap_get_keys(
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
      if (it->key() == tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      }
      string user_key;
      decode_omap_key(it->key(), &user_key);
      dout(30) << __func__ << "  got " << it->key() << " -> " << user_key << dendl;
      assert(it->key() < tail);
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_get_values(
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
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  got " << key << " -> " << *p << dendl;
      out->insert(make_pair(*p, val));
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_check_keys(
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
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  have " << key << " -> " << *p << dendl;
      out->insert(*p);
    } else {
      dout(30) << __func__ << "  miss " << key << " -> " << *p << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

ObjectMap::ObjectMapIterator NewStore::get_omap_iterator(
  coll_t cid,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  assert(0);
}


// -----------------
// write helpers

int NewStore::_recover_next_omap_id()
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  it->lower_bound("GGGGGGGG");
  if (!it->valid()) {
    omap_id.set(1);
    dout(10) << __func__ << " no omap keys, starting at 1" << dendl;
    return 0;
  }
  dout(20) << __func__ << " last key is " << it->key() << dendl;
  uint64_t id;
  int r = sscanf(it->key().c_str(), "%llx", (unsigned long long*)&id);
  if (r < 0) {
    derr << "unable to parse " << it->key() << dendl;
    return -EIO;
  }
  omap_id.set(id);
  return 0;
}

void NewStore::_get_omap_id(TransContext *txc, OnodeRef o)
{
  if (o->onode.omap_head)
    return;

  o->onode.omap_head = omap_id.inc();
  dout(10) << __func__ << " assigned " << o->oid
	   << " id " << o->onode.omap_head << dendl;
  string tail;
  get_omap_tail(o->onode.omap_head, &tail);
  bufferlist empty;
  txc->t->set(PREFIX_OMAP, tail, empty);
}

int NewStore::_recover_next_fid()
{
  bufferlist bl;
  db->get(PREFIX_SUPER, "fid_max", &bl);
  try {
    ::decode(fid_max, bl);
  } catch (buffer::error& e) {
  }
  dout(1) << __func__ << " old fid_max " << fid_max << dendl;
  fid_last = fid_max;

  if (fid_last.fset > 0) {
    char s[32];
    snprintf(s, sizeof(s), "%u", fid_last.fset);
    assert(fset_fd < 0);
    fset_fd = ::openat(frag_fd, s, O_DIRECTORY, 0644);
    if (fset_fd < 0) {
      int r = -errno;
      derr << __func__ << " cannot open created " << path << "/fragments/"
	 << s << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return 0;
}

int NewStore::_open_fid(fid_t fid)
{
  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int fd = ::openat(frag_fd, fn, O_RDWR);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " on " << fid << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  dout(30) << __func__ << " " << fid << " = " << fd << dendl;
  return fd;
}

int NewStore::_create_fid(TransContext *txc, fid_t *fid)
{
  {
    Mutex::Locker l(fid_lock);
    if (fid_last.fset > 0 &&
	fid_last.fno > 0 &&
	fid_last.fset == fid_max.fset &&
	fid_last.fno < g_conf->newstore_max_dir_size) {
      ++fid_last.fno;
      if (fid_last.fno >= fid_max.fno) {
	// raise fid_max, same fset
	fid_max.fno += g_conf->newstore_fid_prealloc;
	assert(fid_max.fno >= fid_last.fno);
	bufferlist bl;
	::encode(fid_max, bl);
	txc->t->set(PREFIX_SUPER, "fid_max", bl);
	dout(10) << __func__ << " fid_max now " << fid_max << dendl;
      }
    } else {
      // new fset
      ++fid_last.fset;
      fid_last.fno = 1;
      dout(10) << __func__ << " creating " << fid_last.fset << dendl;
      char s[32];
      snprintf(s, sizeof(s), "%u", fid_last.fset);
      int r = ::mkdirat(frag_fd, s, 0755);
      if (r < 0) {
	r = -errno;
	derr << __func__ << " cannot create " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
	return r;
      }
      if (fset_fd >= 0)
	VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
      fset_fd = ::openat(frag_fd, s, O_DIRECTORY, 0644);
      if (fset_fd < 0) {
	r = -errno;
	derr << __func__ << " cannot open created " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
      }

      fid_max = fid_last;
      fid_max.fno = g_conf->newstore_fid_prealloc;
      bufferlist bl;
      ::encode(fid_max, bl);
      txc->t->set(PREFIX_SUPER, "fid_max", bl);
      dout(10) << __func__ << " fid_max now " << fid_max << dendl;
    }
    *fid = fid_last;
  }

  dout(10) << __func__ << " " << fid_last << dendl;
  char s[32];
  snprintf(s, sizeof(s), "%u", fid->fno);
  int fd = ::openat(fset_fd, s, O_RDWR|O_CREAT, 0644);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot create " << path << "/fragments/"
	 << *fid << ": " << cpp_strerror(r) << dendl;
    return r;
  }

#if 0
  // store a handle, too
  void *hp;
  size_t hlen;
  int r = fd_to_handle(fd, &hp, &hlen);
  if (r >= 0) {
    fid->handle = string((char *)hp, hlen);
  }
#endif

  dout(30) << __func__ << " " << *fid << " = " << fd << dendl;
  return fd;
}

int NewStore::_remove_fid(fid_t fid)
{
  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int r = ::unlinkat(frag_fd, fn, 0);
  if (r < 0)
    return -errno;
  return 0;
}

NewStore::TransContext *NewStore::_txc_create(OpSequencer *osr)
{
  TransContext *txc = new TransContext(osr);
  txc->t = db->get_transaction();
  osr->queue_new(txc);
  dout(20) << __func__ << " osr " << osr << " = " << txc << dendl;
  return txc;
}

void NewStore::_txc_process_fsync(fsync_item *i)
{
  dout(20) << __func__ << " txc " << i->txc << dendl;
  int r = ::fdatasync(i->fd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " error from fdatasync on " << i->fd
	 << " txc " << i->txc
	 << ": " << cpp_strerror(r) << dendl;
    assert(0 == "error from fdatasync");
  }
  VOID_TEMP_FAILURE_RETRY(::close(i->fd));
  if (i->txc->finish_fsync()) {
    _txc_finish_fsync(i->txc);
  }
  dout(20) << __func__ << " txc " << i->txc << " done" << dendl;
}

void NewStore::_txc_finish_fsync(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << dendl;

  /*
   * we need to preserve the order of kv transactions,
   * even though fsyncs will complete in any order.
   */

  OpSequencer *osr = txc->osr.get();
  Mutex::Locker l(osr->qlock);
  txc->state = TransContext::STATE_FSYNC_DONE;

  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin()) {
    --p;
    if (p->state < TransContext::STATE_FSYNC_DONE) {
      dout(20) << __func__ << " " << txc << " blocked by " << &*p << " "
	       << p->get_state_name() << dendl;
      return;
    }
    if (p->state > TransContext::STATE_FSYNC_DONE) {
      ++p;
      break;
    }
  }
  do {
    _txc_submit_kv(&*p++);
  } while (p != osr->q.end() &&
	   p->state == TransContext::STATE_FSYNC_DONE);
}

int NewStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc
	   << " onodes " << txc->onodes << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
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

void NewStore::_txc_queue_fsync(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  txc->state = TransContext::STATE_FSYNC_QUEUED;
  fsync_wq.lock();
  for (list<fsync_item>::iterator p = txc->fds.begin();
       p != txc->fds.end();
       ++p) {
    fsync_wq._enqueue(&*p);
    fsync_wq._wake();
  }
  fsync_wq.unlock();
}

void NewStore::_txc_submit_kv(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  txc->state = TransContext::STATE_KV_QUEUED;

  Mutex::Locker l(kv_lock);
  db->submit_transaction(txc->t);
  kv_queue.push_back(txc);
  kv_cond.SignalOne();
}

struct C_ApplyWAL : public Context {
  NewStore *store;
  NewStore::TransContext *txc;
  C_ApplyWAL(NewStore *s, NewStore::TransContext *t) : store(s), txc(t) {}
  void finish(int r) {
    store->_apply_wal_transaction(txc);
  }
};

void NewStore::_txc_finish_kv(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  txc->osr->qlock.Lock();
  txc->state = TransContext::STATE_KV_DONE;

  // loop in case we race with OpSequencer::flush_commit()
  do {
    txc->osr->qlock.Unlock();
    if (txc->onreadable_sync) {
      txc->onreadable_sync->complete(0);
      txc->onreadable_sync = NULL;
    }
    if (txc->onreadable) {
      finisher.queue(txc->onreadable);
      txc->onreadable = NULL;
    }
    if (txc->oncommit) {
      txc->oncommit->complete(0);
      txc->oncommit = NULL;
    }
    while (!txc->oncommits.empty()) {
      txc->oncommits.front()->complete(0);
      txc->oncommits.pop_front();
    }
    txc->osr->qlock.Lock();
  } while (txc->oncommit || !txc->oncommits.empty());

  if (txc->wal_txn) {
    dout(20) << __func__ << " starting wal apply" << dendl;
    txc->state = TransContext::STATE_WAL_QUEUED;
    txc->osr->qlock.Unlock();
    finisher.queue(new C_ApplyWAL(this, txc));
  } else {
    txc->state = TransContext::STATE_FINISHING;
    txc->osr->qlock.Unlock();
    _txc_finish_apply(txc);
  }
}

void NewStore::_txc_finish_apply(TransContext *txc)
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

  OpSequencerRef osr = txc->osr;
  osr->qlock.Lock();
  txc->state = TransContext::STATE_DONE;
  osr->qlock.Unlock();

  _osr_reap_done(osr.get());
}

void NewStore::_osr_reap_done(OpSequencer *osr)
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
      txc->first_collection->onode_map.trim(g_conf->newstore_onode_map_size);
    }

    osr->q.pop_front();
    delete txc;
    osr->qcond.Signal();
  }
}

void NewStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  kv_lock.Lock();
  while (true) {
    assert(kv_committing.empty());
    if (kv_queue.empty()) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_sync_cond.Signal();
      kv_cond.Wait(kv_lock);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      dout(20) << __func__ << " committing " << kv_queue.size() << dendl;
      kv_committing.swap(kv_queue);
      utime_t start = ceph_clock_now(NULL);
      kv_lock.Unlock();
      db->submit_transaction_sync(db->get_transaction());
      utime_t finish = ceph_clock_now(NULL);
      utime_t dur = finish - start;
      dout(20) << __func__ << " committed " << kv_committing.size()
	       << " in " << dur << dendl;
      while (!kv_committing.empty()) {
	_txc_finish_kv(kv_committing.front());
	kv_committing.pop_front();
      }

      // this is as good a place as any ...
      _reap_collections();

      kv_lock.Lock();
    }
  }
  kv_lock.Unlock();
  dout(10) << __func__ << " finish" << dendl;
}

wal_op_t *NewStore::_get_wal_op(TransContext *txc)
{
  if (!txc->wal_txn) {
    txc->wal_txn = new wal_transaction_t;
  }
  txc->wal_txn->ops.push_back(wal_op_t());
  return &txc->wal_txn->ops.back();
}

int NewStore::_apply_wal_transaction(TransContext *txc)
{
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << txc << " seq " << wt.seq << dendl;
  txc->state = TransContext::STATE_WAL_APPLYING;

  int r = _do_wal_transaction(wt);
  if (r < 0)
    return r;

  string key;
  get_wal_key(wt.seq, &key);
  KeyValueDB::Transaction cleanup = db->get_transaction();
  cleanup->rmkey(PREFIX_WAL, key);
  db->submit_transaction_sync(cleanup);

  txc->osr->qlock.Lock();
  txc->state = TransContext::STATE_FINISHING;
  txc->osr->qlock.Unlock();

  _txc_finish_apply(txc);
  return 0;
}

int NewStore::_do_wal_transaction(wal_transaction_t& wt)
{
  vector<int> sync_fds;
  sync_fds.reserve(wt.ops.size());

  for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
    switch (p->op) {
    case wal_op_t::OP_WRITE:
      {
	dout(20) << __func__ << " write " << p->fid << " "
		 << p->offset << "~" << p->length << dendl;
	int fd = _open_fid(p->fid);
	if (fd < 0)
	  return fd;
	int r = ::lseek64(fd, p->offset, SEEK_SET);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " lseek64 on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	p->data.write_fd(fd);
	sync_fds.push_back(fd);
      }
      break;
    case wal_op_t::OP_ZERO:
      {
	dout(20) << __func__ << " zero " << p->fid << " "
		 << p->offset << "~" << p->length << dendl;
	int fd = _open_fid(p->fid);
	if (fd < 0)
	  return fd;
	int r = ::lseek64(fd, p->offset, SEEK_SET);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " lseek64 on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
#warning use hole punch ioctl to zero when available
	bufferlist bl;
	bufferptr bp(p->length);
	bp.zero();
	bl.append(bp);
	bl.write_fd(fd);
	sync_fds.push_back(fd);
      }
      break;
    case wal_op_t::OP_TRUNCATE:
      {
	dout(20) << __func__ << " truncate " << p->fid << " "
		 << p->offset << dendl;
	int fd = _open_fid(p->fid);
	if (fd < 0)
	  return fd;
	int r = ::ftruncate(fd, p->offset);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " truncate on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	//sync_fds.push_back(fd);  // do we care?
      }
      break;

    case wal_op_t::OP_REMOVE:
      dout(20) << __func__ << " remove " << p->fid << dendl;
      _remove_fid(p->fid);
      break;

    default:
      assert(0 == "unrecognized wal op");
    }
  }

  for (vector<int>::iterator p = sync_fds.begin();
       p != sync_fds.end();
       ++p) {
    int r = ::fsync(*p);
    assert(r == 0);
    VOID_TEMP_FAILURE_RETRY(::close(*p));
  }

  return 0;
}

int NewStore::_replay_wal()
{
  dout(10) << __func__ << " start" << dendl;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_WAL);
  it->lower_bound(string());
  KeyValueDB::Transaction cleanup = db->get_transaction();
  int count = 0;
  while (it->valid()) {
    bufferlist bl = it->value();
    bufferlist::iterator p = bl.begin();
    wal_transaction_t wt;
    try {
      ::decode(wt, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode wal txn " << it->key() << dendl;
      return -EIO;
    }
    dout(20) << __func__ << " replay " << it->key() << dendl;
    int r = _do_wal_transaction(wt);
    if (r < 0)
      return r;
    cleanup->rmkey(PREFIX_WAL, it->key());
    ++count;
    it->next();
  }
  if (count) {
    dout(10) << __func__ << " cleanup" << dendl;
    db->submit_transaction_sync(cleanup);
  }
  dout(10) << __func__ << " completed " << count << " events" << dendl;
  return 0;
}

// ---------------------------
// transactions

int NewStore::queue_transactions(
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
  if (!posr)
    posr = &default_osr;
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(5) << __func__ << " existing " << *osr << "/" << osr->parent << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << __func__ << " new " << *osr << "/" << osr->parent << dendl;
  }

  TransContext *txc = _txc_create(osr);

  // XXX do it sync for now; this is not crash safe
  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    (*p)->set_osr(osr);
    _do_transaction(*p, txc, handle);
  }

  txc->onreadable = onreadable;
  txc->onreadable_sync = onreadable_sync;
  txc->oncommit = ondisk;

  r = _txc_finalize(osr, txc);
  assert(r == 0);

  if (g_conf->newstore_sync_queue_transaction) {
    // do it syncrhonously.  for example, if we have a *very* fast backend.

    // sync
    txc->state = TransContext::STATE_FSYNC_FSYNCING;
    for (list<fsync_item>::iterator p = txc->fds.begin();
	 p != txc->fds.end(); ++p) {
      dout(30) << __func__ << " fsync " << p->fd << dendl;
      int r = ::fsync(p->fd);
      if (r < 0) {
	r = -errno;
	derr << __func__ << " fsync: " << cpp_strerror(r) << dendl;
	return r;
      }
      VOID_TEMP_FAILURE_RETRY(::close(p->fd));
    }

    txc->state = TransContext::STATE_KV_COMMITTING;
    db->submit_transaction_sync(txc->t);

    _txc_finish_kv(txc);
  } else {
    // async path
    if (!txc->fds.empty()) {
      _txc_queue_fsync(txc);
    } else {
      _txc_finish_fsync(txc);
    }
  }

  return 0;
}

int NewStore::_do_transaction(Transaction *t,
			      TransContext *txc,
			      ThreadPool::TPHandle *handle)
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
        map<string, bufferlist> aset;
        i.decode_attrset(aset);
	r = _omap_setkeys(txc, c, oid, aset);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        ghobject_t oid = i.get_oid(op->oid);
        set<string> keys;
        i.decode_keyset(keys);
	r = _omap_rmkeys(txc, c, oid, keys);
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

int NewStore::_touch(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  assert(o);
  o->exists = true;
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_do_write(TransContext *txc,
			OnodeRef o,
			uint64_t offset, uint64_t length,
			const bufferlist& bl,
			uint32_t fadvise_flags)
{
  int fd = -1;
  int r = 0;

  dout(20) << __func__ << " have " << o->onode.size
	   << " bytes in " << o->onode.data_map.size()
	   << " fragments" << dendl;

  o->exists = true;

  if (length == 0) {
    dout(20) << __func__ << " zero-length write" << dendl;
    goto out;
  }
  if (o->onode.size == offset ||
      o->onode.size == 0 ||
      o->onode.data_map.empty()) {
    if (o->onode.data_map.empty()) {
      // create
      fragment_t &f = o->onode.data_map[0];
      f.offset = 0;
      f.length = MAX(offset + length, o->onode.size);
      fd = _create_fid(txc, &f.fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      ::lseek64(fd, offset, SEEK_SET);
      dout(20) << __func__ << " create " << f.fid << " writing "
	       << offset << "~" << length << dendl;
    } else {
      // append (possibly with gap)
      assert(o->onode.data_map.size() == 1);
      fragment_t &f = o->onode.data_map.rbegin()->second;
      fd = _open_fid(f.fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      ::ftruncate(fd, f.length);  // in case there is trailing crap
      f.length = (offset + length) - f.offset;
      ::lseek64(fd, offset - f.offset, SEEK_SET);
      dout(20) << __func__ << " append " << f.fid << " writing "
	       << (offset - f.offset) << "~" << length << dendl;
    }
    if (offset + length > o->onode.size) {
      o->onode.size = offset + length;
    }
    r = bl.write_fd(fd);
    if (r < 0) {
      derr << __func__ << " bl.write_fd error: " << cpp_strerror(r) << dendl;
      goto out;
    }
    txc->sync_fd(fd);
  } else if (offset == 0 &&
	     length >= o->onode.size) {
    // overwrite to new fid
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    assert(f.length == o->onode.size);

    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_REMOVE;
    op->fid = f.fid;

    f.length = length;
    o->onode.size = length;
    fd = _create_fid(txc, &f.fid);
    if (fd < 0) {
      r = fd;
      goto out;
    }
    dout(20) << __func__ << " replace old fid " << op->fid
	     << " with new fid " << f.fid
	     << ", writing " << offset << "~" << length << dendl;
    r = bl.write_fd(fd);
    if (r < 0) {
      derr << __func__ << " bl.write_fd error: " << cpp_strerror(r) << dendl;
      goto out;
    }
    txc->sync_fd(fd);
  } else {
    // WAL
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    assert(f.length == o->onode.size);
    r = _clean_fid_tail(txc, f);
    if (r < 0)
      goto out;
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_WRITE;
    op->offset = offset - f.offset;
    op->length = length;
    op->fid = f.fid;
    op->data = bl;
    if (offset + length > o->onode.size) {
      o->onode.size = offset + length;
    }
    if (offset + length - f.offset > f.length) {
      f.length = offset + length - f.offset;
    }
    dout(20) << __func__ << " wal " << f.fid << " write "
	     << (offset - f.offset) << "~" << length << dendl;
  }
  r = 0;

 out:
  return r;
}

int NewStore::_clean_fid_tail(TransContext *txc, const fragment_t& f)
{
  int fd = _open_fid(f.fid);
  if (fd < 0) {
    return fd;
  }
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " failed to fstat " << f.fid << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  if (st.st_size > f.length) {
    dout(20) << __func__ << " frag " << f.fid << " is long, truncating"
	     << dendl;
    r = ::ftruncate(fd, f.length);
    if (r < 0) {
      derr << __func__ << " failed to ftruncate " << f.fid << ": "
	   << cpp_strerror(r) << dendl;
      return r;
    }
    txc->sync_fd(fd);
  } else {
    // all good!
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  return 0;
}


int NewStore::_write(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid,
		     uint64_t offset, size_t length,
		     const bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  int r = _do_write(txc, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_zero(TransContext *txc,
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

  if (o->onode.data_map.empty()) {
    // we're already a big hole
    if (offset + length > o->onode.size) {
      o->onode.size = offset + length;
      txc->write_onode(o);
    }
  } else {
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    assert(f.length == o->onode.size);

    r = _clean_fid_tail(txc, f);
    if (r < 0)
      goto out;

    if (offset >= o->onode.size) {
      // after tail
      int fd = _open_fid(f.fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      f.length = (offset + length) - f.offset;
      ::ftruncate(fd, f.length);
      dout(20) << __func__ << " tail " << f.fid << " truncating up to "
	       << f.length << dendl;
      o->onode.size = offset + length;
      txc->write_onode(o);
    } else {
      // WAL
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_ZERO;
      op->offset = offset - f.offset;
      op->length = length;
      op->fid = f.fid;
      if (offset + length > o->onode.size) {
	f.length = offset + length - f.offset;
	o->onode.size = offset + length;
	txc->write_onode(o);
      }
    }
  }

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_do_truncate(TransContext *txc, OnodeRef o, uint64_t offset)
{
  if (o->onode.data_map.empty()) {
    o->onode.size = offset;
  } else if (offset == 0) {
    while (!o->onode.data_map.empty()) {
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = o->onode.data_map.rbegin()->second.fid;
      o->onode.data_map.erase(o->onode.data_map.rbegin()->first);
    }
  } else if (offset < o->onode.size) {
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    assert(f.length == o->onode.size);
    f.length = offset;
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_TRUNCATE;
    op->offset = offset;
    op->fid = f.fid;
    assert(f.offset == 0);
  } else if (offset > o->onode.size) {
    // resize file up.  make sure we don't have trailing bytes
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    assert(f.length == o->onode.size);
    int r = _clean_fid_tail(txc, f);
    if (r < 0)
      return r;
    if (false) {  // hmm don't bother!!
      // truncate up.  don't bother to fsync since it's all zeros.
      int fd = _open_fid(f.fid);
      if (fd < 0) {
	return fd;
      }
      r = ::ftruncate(fd, offset);
      if (r < 0) {
	r = -errno;
	derr << "error from ftruncate on " << f.fid << " to " << offset << ": "
	     << cpp_strerror(r) << dendl;
	VOID_TEMP_FAILURE_RETRY(::close(fd));
	return r;
      }
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
    f.length = offset;
  }
  o->onode.size = offset;
  txc->write_onode(o);
  return 0;
}

int NewStore::_truncate(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
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

int NewStore::_do_remove(TransContext *txc,
			 OnodeRef o)
{
  string key;
  o->exists = false;
  if (!o->onode.data_map.empty()) {
    for (map<uint64_t,fragment_t>::iterator p = o->onode.data_map.begin();
	 p != o->onode.data_map.end();
	 ++p) {
      dout(20) << __func__ << " will wal remove " << p->second.fid << dendl;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = p->second.fid;
    }
  }
  o->onode.data_map.clear();
  o->onode.size = 0;
  if (o->onode.omap_head) {
    _do_omap_clear(txc, o->onode.omap_head, true);
  }

  get_object_key(o->oid, &key);
  txc->t->rmkey(PREFIX_OBJ, key);
  return 0;
}

int NewStore::_remove(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _do_remove(txc, o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_setattr(TransContext *txc,
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

int NewStore::_setattrs(TransContext *txc,
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


int NewStore::_rmattr(TransContext *txc,
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

int NewStore::_rmattrs(TransContext *txc,
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

void NewStore::_do_omap_clear(TransContext *txc, uint64_t id,
			      bool remove_tail)
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  string prefix, tail;
  get_omap_header(id, &prefix);
  get_omap_tail(id, &tail);
  it->lower_bound(prefix);
  while (it->valid()) {
    if (it->key() == tail) {
      if (remove_tail) {
	txc->t->rmkey(PREFIX_OMAP, it->key());
      }
      dout(30) << __func__ << "  stop at " << tail << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << it->key() << dendl;
    it->next();
  }
}

int NewStore::_omap_clear(TransContext *txc,
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
    _do_omap_clear(txc, o->onode.omap_head, false);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_setkeys(TransContext *txc,
			    CollectionRef& c,
			    const ghobject_t& oid,
			    const map<string,bufferlist>& m)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  _get_omap_id(txc, o);
  for (map<string,bufferlist>::const_iterator p = m.begin(); p != m.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, p->first, &key);
    dout(30) << __func__ << "  " << key << " <- " << p->first << dendl;
    txc->t->set(PREFIX_OMAP, key, p->second);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_setheader(TransContext *txc,
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
  _get_omap_id(txc, o);
  get_omap_header(o->onode.omap_head, &key);
  txc->t->set(PREFIX_OMAP, key, bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_rmkeys(TransContext *txc,
			   CollectionRef& c,
			   const ghobject_t& oid,
			   const set<string>& m)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

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
  _get_omap_id(txc, o);
  for (set<string>::const_iterator p = m.begin(); p != m.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    dout(30) << __func__ << "  rm " << key << " <- " << *p << dendl;
    txc->t->rmkey(PREFIX_OMAP, key);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_rmkey_range(TransContext *txc,
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
      dout(30) << __func__ << "  stop at " << key_last << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << it->key() << dendl;
    it->next();
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_setallochint(TransContext *txc,
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

int NewStore::_clone(TransContext *txc,
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

  r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

  // truncate any old data
  while (!newo->onode.data_map.empty()) {
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_REMOVE;
    op->fid = newo->onode.data_map.rbegin()->second.fid;
    newo->onode.data_map.erase(newo->onode.data_map.rbegin()->first);
  }

  r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);

  newo->onode.attrs = oldo->onode.attrs;

  // clone omap
  if (newo->onode.omap_head) {
    dout(20) << __func__ << " clearing old omap data" << dendl;
    _do_omap_clear(txc, newo->onode.omap_head, true);
  }
  if (oldo->onode.omap_head) {
    dout(20) << __func__ << " copying omap data" << dendl;
    _get_omap_id(txc, newo);
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(oldo->onode.omap_head, &head);
    get_omap_tail(oldo->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      string key;
      if (it->key() == tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	dout(30) << __func__ << "  got header/data " << it->key() << dendl;
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

int NewStore::_clone_range(TransContext *txc,
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

int NewStore::_rename(TransContext *txc,
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

int NewStore::_create_collection(
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

int NewStore::_remove_collection(TransContext *txc, coll_t cid,
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

int NewStore::_split_collection(TransContext *txc,
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
