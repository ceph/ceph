// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/int_types.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <errno.h>
#include <dirent.h>

#include <iostream>
#include <map>

#include "include/compat.h"

#include <fstream>
#include <sstream>

#include "KeyValueStore.h"
#include "common/BackTrace.h"
#include "include/types.h"

#include "osd/osd_types.h"
#include "include/color.h"
#include "include/buffer.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/perf_counters.h"
#include "common/sync_filesystem.h"

#ifdef HAVE_KINETIC
#include "KineticStore.h"
#endif

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;

#include "include/assert.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_keyvaluestore

const string KeyValueStore::OBJECT_STRIP_PREFIX = "_STRIP_";
const string KeyValueStore::OBJECT_XATTR = "__OBJATTR__";
const string KeyValueStore::OBJECT_OMAP = "__OBJOMAP__";
const string KeyValueStore::OBJECT_OMAP_HEADER = "__OBJOMAP_HEADER__";
const string KeyValueStore::OBJECT_OMAP_HEADER_KEY = "__OBJOMAP_HEADER__KEY_";
const string KeyValueStore::COLLECTION = "__COLLECTION__";
const string KeyValueStore::COLLECTION_ATTR = "__COLL_ATTR__";


//Initial features in new superblock.
static CompatSet get_kv_initial_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

//Features are added here that this KeyValueStore supports.
static CompatSet get_kv_supported_compat_set() {
  CompatSet compat =  get_kv_initial_compat_set();
  //Any features here can be set in code, but not in initial superblock
  return compat;
}


// ============== StripObjectMap Implementation =================

int StripObjectMap::save_strip_header(StripObjectHeaderRef strip_header,
                                      KeyValueDB::Transaction t)
{
  if (strip_header->updated) {
    strip_header->header->data.clear();
    ::encode(*strip_header, strip_header->header->data);

    set_header(strip_header->cid, strip_header->oid, *(strip_header->header), t);
    strip_header->updated = false;
  }
  return 0;
}

int StripObjectMap::create_strip_header(const coll_t &cid,
                                        const ghobject_t &oid,
                                        StripObjectHeaderRef *strip_header,
                                        KeyValueDB::Transaction t)
{
  Header header = generate_new_header(cid, oid, Header(), t);
  if (!header)
    return -EINVAL;

  StripObjectHeaderRef tmp = StripObjectHeaderRef(new StripObjectHeader());
  tmp->oid = oid;
  tmp->cid = cid;
  tmp->header = header;
  tmp->updated = true;
  if (strip_header)
    *strip_header = tmp;

  return 0;
}

int StripObjectMap::lookup_strip_header(const coll_t &cid,
                                        const ghobject_t &oid,
                                        StripObjectHeaderRef *strip_header)
{
  {
    Mutex::Locker l(lock);
    pair<coll_t, StripObjectHeaderRef> p;
    if (caches.lookup(oid, &p)) {
      if (p.first == cid) {
        *strip_header = p.second;
        return 0;
      }
    }
  }
  Header header = lookup_header(cid, oid);

  if (!header) {
    dout(20) << "lookup_strip_header failed to get strip_header "
             << " cid " << cid <<" oid " << oid << dendl;
    return -ENOENT;
  }


  StripObjectHeaderRef tmp = StripObjectHeaderRef(new StripObjectHeader());
  if (header->data.length()) {
    bufferlist::iterator bliter = header->data.begin();
    ::decode(*tmp, bliter);
  }

  if (tmp->strip_size == 0) {
    tmp->strip_size = default_strip_size;
    tmp->updated = true;
  }

  tmp->oid = oid;
  tmp->cid = cid;
  tmp->header = header;

  {
    Mutex::Locker l(lock);
    caches.add(oid, make_pair(cid, tmp));
  }
  *strip_header = tmp;
  dout(10) << "lookup_strip_header done " << " cid " << cid << " oid "
           << oid << dendl;
  return 0;
}

int StripObjectMap::file_to_extents(uint64_t offset, size_t len,
                                    uint64_t strip_size,
                                    vector<StripExtent> &extents)
{
  if (len == 0)
    return 0;

  uint64_t start, end, strip_offset;
  start = offset / strip_size;
  end = (offset + len) / strip_size;
  strip_offset = start * strip_size;

  // "offset" may in the middle of first strip object
  if (offset > strip_offset) {
    uint64_t extent_offset, extent_len;
    extent_offset = offset - strip_offset;
    if (extent_offset + len <= strip_size)
      extent_len = len;
    else
      extent_len = strip_size - extent_offset;
    extents.push_back(StripExtent(start, extent_offset, extent_len));
    start++;
    strip_offset += strip_size;
  }

  for (; start < end; ++start) {
    extents.push_back(StripExtent(start, 0, strip_size));
    strip_offset += strip_size;
  }

  // The end of strip object may be partial
  if (offset + len > strip_offset)
    extents.push_back(StripExtent(start, 0, offset+len-strip_offset));

  assert(extents.size());
  dout(10) << "file_to_extents done " << dendl;
  return 0;
}

void StripObjectMap::clone_wrap(StripObjectHeaderRef old_header,
                                const coll_t &cid, const ghobject_t &oid,
                                KeyValueDB::Transaction t,
                                StripObjectHeaderRef *target_header)
{
  Header new_origin_header;
  StripObjectHeaderRef tmp = StripObjectHeaderRef(new StripObjectHeader());

  clone(old_header->header, cid, oid, t, &new_origin_header,
        &tmp->header);

  tmp->oid = oid;
  tmp->cid = cid;
  tmp->strip_size = old_header->strip_size;
  tmp->max_size = old_header->max_size;
  tmp->bits = old_header->bits;
  tmp->updated = true;
  old_header->header = new_origin_header;
  old_header->updated = true;

  if (target_header)
    *target_header = tmp;
}

void StripObjectMap::rename_wrap(StripObjectHeaderRef old_header, const coll_t &cid, const ghobject_t &oid,
                                 KeyValueDB::Transaction t,
                                 StripObjectHeaderRef *new_header)
{
  rename(old_header->header, cid, oid, t);

  StripObjectHeaderRef tmp = StripObjectHeaderRef(new StripObjectHeader());
  tmp->strip_size = old_header->strip_size;
  tmp->max_size = old_header->max_size;
  tmp->bits = old_header->bits;
  tmp->header = old_header->header;
  tmp->oid = oid;
  tmp->cid = cid;
  tmp->updated = true;

  if (new_header)
    *new_header = tmp;

  old_header->header = Header();
  old_header->deleted = true;
}

int StripObjectMap::get_values_with_header(const StripObjectHeaderRef header,
                                           const string &prefix,
                                           const set<string> &keys,
                                           map<string, bufferlist> *out)
{
  return scan(header->header, prefix, keys, 0, out);
}

int StripObjectMap::get_keys_with_header(const StripObjectHeaderRef header,
                                         const string &prefix,
                                         set<string> *keys)
{
  ObjectMap::ObjectMapIterator iter = _get_iterator(header->header, prefix);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    assert(!iter->status());
    keys->insert(iter->key());
  }
  return 0;
}

int StripObjectMap::get_with_header(const StripObjectHeaderRef header,
                        const string &prefix, map<string, bufferlist> *out)
{
  ObjectMap::ObjectMapIterator iter = _get_iterator(header->header, prefix);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    assert(!iter->status());
    out->insert(make_pair(iter->key(), iter->value()));
  }

  return 0;
}

// ========= KeyValueStore::BufferTransaction Implementation ============

int KeyValueStore::BufferTransaction::lookup_cached_header(
    const coll_t &cid, const ghobject_t &oid,
    StripObjectMap::StripObjectHeaderRef *strip_header,
    bool create_if_missing)
{
  uniq_id uid = make_pair(cid, oid);
  StripObjectMap::StripObjectHeaderRef header;
  int r = 0;

  StripHeaderMap::iterator it = strip_headers.find(uid);
  if (it != strip_headers.end()) {

    if (!it->second->deleted) {
      if (strip_header)
        *strip_header = it->second;
      return 0;
    } else if (!create_if_missing) {
      return -ENOENT;
    }

    // If (it->second.deleted && create_if_missing) go down
    r = -ENOENT;
  } else {
    r = store->backend->lookup_strip_header(cid, oid, &header);
  }

  if (r == -ENOENT && create_if_missing) {
    r = store->backend->create_strip_header(cid, oid, &header, t);
  }

  if (r < 0) {
    dout(10) << __func__  << " " << cid << "/" << oid << " "
             << " r = " << r << dendl;
    return r;
  }

  strip_headers[uid] = header;
  if (strip_header)
    *strip_header = header;
  return r;
}

int KeyValueStore::BufferTransaction::get_buffer_keys(
    StripObjectMap::StripObjectHeaderRef strip_header, const string &prefix,
    const set<string> &keys, map<string, bufferlist> *out)
{
  set<string> need_lookup;

  uniq_id uid = make_pair(strip_header->cid, strip_header->oid);
  for (set<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
    map< uniq_id, map<pair<string, string>, bufferlist> >::iterator obj_it = buffers.find(uid);
    if ( obj_it != buffers.end() ) {
      map<pair<string, string>, bufferlist>::iterator i =
          obj_it->second.find(make_pair(prefix, *it));
      if (i != obj_it->second.end()) {
        (*out)[*it].swap(i->second);
      } else {
        need_lookup.insert(*it);
      }
    }else {
      need_lookup.insert(*it);
    }
  }

  if (!need_lookup.empty()) {
    int r = store->backend->get_values_with_header(strip_header, prefix,
                                                   need_lookup, out);
    if (r < 0) {
      dout(10) << __func__  << " " << strip_header->cid << "/"
               << strip_header->oid << " " << " r = " << r << dendl;
      return r;
    }
  }

  return 0;
}

void KeyValueStore::BufferTransaction::set_buffer_keys(
     StripObjectMap::StripObjectHeaderRef strip_header,
     const string &prefix, map<string, bufferlist> &values)
{
  store->backend->set_keys(strip_header->header, prefix, values, t);

  uniq_id uid = make_pair(strip_header->cid, strip_header->oid);
  map<pair<string, string>, bufferlist> &uid_buffers = buffers[uid];
  for (map<string, bufferlist>::iterator iter = values.begin();
       iter != values.end(); ++iter) {
    uid_buffers[make_pair(prefix, iter->first)].swap(iter->second);
  }
}

int KeyValueStore::BufferTransaction::remove_buffer_keys(
     StripObjectMap::StripObjectHeaderRef strip_header, const string &prefix,
     const set<string> &keys)
{
  uniq_id uid = make_pair(strip_header->cid, strip_header->oid);
  map< uniq_id, map<pair<string, string>, bufferlist> >::iterator obj_it = buffers.find(uid);
  set<string> buffered_keys;
  if ( obj_it != buffers.end() ) {
    // TODO: Avoid use empty bufferlist to indicate the key is removed
    for (set<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      obj_it->second[make_pair(prefix, *iter)] = bufferlist();
    }
    // TODO: Avoid collect all buffered keys when remove keys
    if (strip_header->header->parent) {
      for (map<pair<string, string>, bufferlist>::iterator iter = obj_it->second.begin();
           iter != obj_it->second.end(); ++iter) {
        buffered_keys.insert(iter->first.second);
      }
    }
  }

  return store->backend->rm_keys(strip_header->header, prefix, buffered_keys, keys, t);
}

void KeyValueStore::BufferTransaction::clear_buffer_keys(
     StripObjectMap::StripObjectHeaderRef strip_header, const string &prefix)
{
  uniq_id uid = make_pair(strip_header->cid, strip_header->oid);
  map< uniq_id, map<pair<string, string>, bufferlist> >::iterator obj_it = buffers.find(uid);
  if ( obj_it != buffers.end() ) {
    for (map<pair<string, string>, bufferlist>::iterator iter = obj_it->second.begin();
         iter != obj_it->second.end(); ++iter) {
      if (iter->first.first == prefix)
        iter->second = bufferlist();
    }
  }
}

int KeyValueStore::BufferTransaction::clear_buffer(
     StripObjectMap::StripObjectHeaderRef strip_header)
{
  strip_header->deleted = true;

  InvalidateCacheContext *c = new InvalidateCacheContext(store, strip_header->cid, strip_header->oid);
  finishes.push_back(c);
  return store->backend->clear(strip_header->header, t);
}

void KeyValueStore::BufferTransaction::clone_buffer(
    StripObjectMap::StripObjectHeaderRef old_header,
    const coll_t &cid, const ghobject_t &oid)
{
  // Remove target ahead to avoid dead lock
  strip_headers.erase(make_pair(cid, oid));

  StripObjectMap::StripObjectHeaderRef new_target_header;

  store->backend->clone_wrap(old_header, cid, oid, t, &new_target_header);

  // FIXME: Lacking of lock for origin header(now become parent), it will
  // cause other operation can get the origin header while submitting
  // transactions
  strip_headers[make_pair(cid, oid)] = new_target_header;
}

void KeyValueStore::BufferTransaction::rename_buffer(
    StripObjectMap::StripObjectHeaderRef old_header,
    const coll_t &cid, const ghobject_t &oid)
{
  // FIXME: Lacking of lock for origin header, it will cause other operation
  // can get the origin header while submitting transactions
  StripObjectMap::StripObjectHeaderRef new_header;
  store->backend->rename_wrap(old_header, cid, oid, t, &new_header);

  InvalidateCacheContext *c = new InvalidateCacheContext(store, old_header->cid, old_header->oid);
  finishes.push_back(c);
  strip_headers[make_pair(cid, oid)] = new_header;
}

int KeyValueStore::BufferTransaction::submit_transaction()
{
  int r = 0;

  for (StripHeaderMap::iterator header_iter = strip_headers.begin();
       header_iter != strip_headers.end(); ++header_iter) {
    StripObjectMap::StripObjectHeaderRef header = header_iter->second;

    if (header->deleted)
      continue;

    if (header->updated) {
      r = store->backend->save_strip_header(header, t);

      if (r < 0) {
        dout(10) << __func__ << " save strip header failed " << dendl;
        goto out;
      }
    }
  }

  r = store->backend->submit_transaction_sync(t);
  for (list<Context*>::iterator it = finishes.begin(); it != finishes.end(); ++it) {
    (*it)->complete(r);
  }

out:
  dout(5) << __func__ << " r = " << r << dendl;
  return r;
}

// =========== KeyValueStore Intern Helper Implementation ==============

ostream& operator<<(ostream& out, const KeyValueStore::OpSequencer& s)
{
  assert(&out);
  return out << *s.parent;
}

int KeyValueStore::_create_current()
{
  struct stat st;
  int ret = ::stat(current_fn.c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "_create_current: current/ exists but is not a directory" << dendl;
      ret = -EINVAL;
    }
  } else {
    ret = ::mkdir(current_fn.c_str(), 0755);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "_create_current: mkdir " << current_fn << " failed: "<< cpp_strerror(ret) << dendl;
    }
  }

  return ret;
}



// =========== KeyValueStore API Implementation ==============

KeyValueStore::KeyValueStore(const std::string &base,
                             const char *name, bool do_update) :
  ObjectStore(base),
  internal_name(name),
  basedir(base),
  fsid_fd(-1), current_fd(-1),
  backend(NULL),
  ondisk_finisher(g_ceph_context),
  collections_lock("KeyValueStore::collections_lock"),
  lock("KeyValueStore::lock"),
  throttle_ops(g_ceph_context, "keyvaluestore_ops", g_conf->keyvaluestore_queue_max_ops),
  throttle_bytes(g_ceph_context, "keyvaluestore_bytes", g_conf->keyvaluestore_queue_max_bytes),
  op_finisher(g_ceph_context),
  op_tp(g_ceph_context, "KeyValueStore::op_tp",
        g_conf->keyvaluestore_op_threads, "keyvaluestore_op_threads"),
  op_wq(this, g_conf->keyvaluestore_op_thread_timeout,
        g_conf->keyvaluestore_op_thread_suicide_timeout, &op_tp),
  perf_logger(NULL),
  m_keyvaluestore_queue_max_ops(g_conf->keyvaluestore_queue_max_ops),
  m_keyvaluestore_queue_max_bytes(g_conf->keyvaluestore_queue_max_bytes),
  m_keyvaluestore_strip_size(g_conf->keyvaluestore_default_strip_size),
  m_keyvaluestore_max_expected_write_size(g_conf->keyvaluestore_max_expected_write_size),
  do_update(do_update),
  m_keyvaluestore_do_dump(false),
  m_keyvaluestore_dump_fmt(true)
{
  ostringstream oss;
  oss << basedir << "/current";
  current_fn = oss.str();

  // initialize perf_logger
  PerfCountersBuilder plb(g_ceph_context, internal_name, l_os_commit_len, l_os_last);

  plb.add_u64(l_os_oq_max_ops, "op_queue_max_ops", "Max operations count in queue");
  plb.add_u64(l_os_oq_ops, "op_queue_ops", "Operations count in queue");
  plb.add_u64_counter(l_os_ops, "ops", "Operations");
  plb.add_u64(l_os_oq_max_bytes, "op_queue_max_bytes", "Max size of queue");
  plb.add_u64(l_os_oq_bytes, "op_queue_bytes", "Size of queue");
  plb.add_u64_counter(l_os_bytes, "bytes", "Data written to store");
  plb.add_time_avg(l_os_commit_lat, "commit_latency", "Commit latency");
  plb.add_time_avg(l_os_apply_lat, "apply_latency", "Apply latency");
  plb.add_time_avg(l_os_queue_lat, "queue_transaction_latency_avg", "Store operation queue latency");

  perf_logger = plb.create_perf_counters();

  g_ceph_context->get_perfcounters_collection()->add(perf_logger);
  g_ceph_context->_conf->add_observer(this);

  superblock.compat_features = get_kv_initial_compat_set();
}

KeyValueStore::~KeyValueStore()
{
  g_ceph_context->_conf->remove_observer(this);
  g_ceph_context->get_perfcounters_collection()->remove(perf_logger);

  delete perf_logger;
  
  if (m_keyvaluestore_do_dump) {
    dump_stop();
  }
}

int KeyValueStore::statfs(struct statfs *buf)
{
  int r = backend->db->get_statfs(buf);
  if (r < 0) {
    if (::statfs(basedir.c_str(), buf) < 0) {
      int r = -errno;
      return r;
    }
  }
  return 0;
}

void KeyValueStore::collect_metadata(map<string,string> *pm)
{
  (*pm)["keyvaluestore_backend"] = superblock.backend;
}

int KeyValueStore::mkfs()
{
  int ret = 0;
  char fsid_fn[PATH_MAX];
  uuid_d old_fsid;

  dout(1) << "mkfs in " << basedir << dendl;

  // open+lock fsid
  snprintf(fsid_fn, sizeof(fsid_fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fsid_fn, O_RDWR|O_CREAT, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "mkfs: failed to open " << fsid_fn << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (lock_fsid() < 0) {
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  if (read_fsid(fsid_fd, &old_fsid) < 0 || old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << "mkfs generated fsid " << fsid << dendl;
    } else {
      dout(1) << "mkfs using provided fsid " << fsid << dendl;
    }

    char fsid_str[40];
    fsid.print(fsid_str);
    strcat(fsid_str, "\n");
    ret = ::ftruncate(fsid_fd, 0);
    if (ret < 0) {
      ret = -errno;
      derr << "mkfs: failed to truncate fsid: " << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    ret = safe_write(fsid_fd, fsid_str, strlen(fsid_str));
    if (ret < 0) {
      derr << "mkfs: failed to write fsid: " << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    if (::fsync(fsid_fd) < 0) {
      ret = errno;
      derr << "mkfs: close failed: can't write fsid: "
           << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    dout(10) << "mkfs fsid is " << fsid << dendl;
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << "mkfs on-disk fsid " << old_fsid << " != provided " << fsid << dendl;
      ret = -EINVAL;
      goto close_fsid_fd;
    }
    fsid = old_fsid;
    dout(1) << "mkfs fsid is already set to " << fsid << dendl;
  }

  // version stamp
  ret = write_version_stamp();
  if (ret < 0) {
    derr << "mkfs: write_version_stamp() failed: "
         << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  ret = _create_current();
  if (ret < 0) {
    derr << "mkfs: failed to create current/ " << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  // superblock
  superblock.backend = g_conf->keyvaluestore_backend;
  ret = write_superblock();
  if (ret < 0) {
    derr << "KeyValueStore::mkfs write_superblock() failed: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  {
    KeyValueDB *store = KeyValueDB::create(g_ceph_context,
					   superblock.backend,
					   current_fn.c_str());
    if (!store) {
      derr << __func__ << " failed to create backend type "
	   << g_conf->keyvaluestore_backend << "." << dendl;
      ret = -1;
      goto close_fsid_fd;
    }

    ostringstream err;
    if (store->create_and_open(err)) {
      derr << __func__  << " failed to create/open backend type "
	   << g_conf->keyvaluestore_backend << "." << dendl;
      ret = -1;
      delete store;
      goto close_fsid_fd;
    }

    bufferlist bl;
    ::encode(collections, bl);
    KeyValueDB::Transaction t = store->get_transaction();
    t->set("meta", "collections", bl);
    store->submit_transaction_sync(t);

    dout(1) << g_conf->keyvaluestore_backend << " backend exists/created" << dendl;
    delete store;
  }

  ret = write_meta("type", "keyvaluestore");
  if (ret < 0)
    goto close_fsid_fd;

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

 close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return ret;
}

int KeyValueStore::read_fsid(int fd, uuid_d *uuid)
{
  char fsid_str[40];
  int ret = safe_read(fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret == 8) {
    // old 64-bit fsid... mirror it.
    *(uint64_t*)&uuid->bytes()[0] = *(uint64_t*)fsid_str;
    *(uint64_t*)&uuid->bytes()[8] = *(uint64_t*)fsid_str;
    return 0;
  }

  if (ret > 36)
    fsid_str[36] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int KeyValueStore::lock_fsid()
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
    dout(0) << "lock_fsid failed to lock " << basedir
            << "/fsid, is another ceph-osd still running? "
            << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool KeyValueStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;   // no fsid, ok.
  bool inuse = lock_fsid() < 0;
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return inuse;
}

int KeyValueStore::write_superblock()
{
  bufferlist bl;
  ::encode(superblock, bl);
  return safe_write_file(basedir.c_str(), "superblock",
      bl.c_str(), bl.length());
}

int KeyValueStore::read_superblock()
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "superblock",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT) {
      // If the file doesn't exist write initial CompatSet
      return write_superblock();
    }
    return ret;
  }

  bufferlist bl;
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(superblock, i);
  return 0;
}



int KeyValueStore::update_version_stamp()
{
  return write_version_stamp();
}

int KeyValueStore::version_stamp_is_valid(uint32_t *version)
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "store_version",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT)
      return 0;
    return ret;
  }
  bufferlist bl;
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(*version, i);
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int KeyValueStore::write_version_stamp()
{
  bufferlist bl;
  ::encode(target_version, bl);

  return safe_write_file(basedir.c_str(), "store_version",
      bl.c_str(), bl.length());
}

int KeyValueStore::mount()
{
  int ret;
  char buf[PATH_MAX];
  CompatSet supported_compat_set = get_kv_supported_compat_set();

  dout(5) << "basedir " << basedir << dendl;

  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "KeyValueStore::mount: unable to access basedir '" << basedir
         << "': " << cpp_strerror(ret) << dendl;
    goto done;
  }

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening '" << buf << "': "
         << cpp_strerror(ret) << dendl;
    goto done;
  }

  ret = read_fsid(fsid_fd, &fsid);
  if (ret < 0) {
    derr << "KeyValueStore::mount: error reading fsid_fd: "
         << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "KeyValueStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;

  uint32_t version_stamp;
  ret = version_stamp_is_valid(&version_stamp);
  if (ret < 0) {
    derr << "KeyValueStore::mount : error in version_stamp_is_valid: "
         << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  } else if (ret == 0) {
    if (do_update) {
      derr << "KeyValueStore::mount : stale version stamp detected: "
           << version_stamp << ". Proceeding, do_update "
           << "is set, performing disk format upgrade." << dendl;
    } else {
      ret = -EINVAL;
      derr << "KeyValueStore::mount : stale version stamp " << version_stamp
           << ". Please run the KeyValueStore update script before starting "
           << "the OSD, or set keyvaluestore_update_to to " << target_version
           << dendl;
      goto close_fsid_fd;
    }
  }
  
  superblock.backend = g_conf->keyvaluestore_backend;
  ret = read_superblock();
  if (ret < 0) {
    ret = -EINVAL;
    goto close_fsid_fd;
  }

  // Check if this KeyValueStore supports all the necessary features to mount
  if (supported_compat_set.compare(superblock.compat_features) == -1) {
    derr << "KeyValueStore::mount : Incompatible features set "
	   << superblock.compat_features << dendl;
    ret = -EINVAL;
    goto close_fsid_fd;
  }

  current_fd = ::open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening: " << current_fn << ": "
         << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  assert(current_fd >= 0);

  {
    if (superblock.backend.empty())
      superblock.backend = g_conf->keyvaluestore_backend;
    KeyValueDB *store = KeyValueDB::create(g_ceph_context,
					   superblock.backend,
					   current_fn.c_str());
    if(!store)
    {
      derr << "KeyValueStore::mount backend type "
	   << superblock.backend << " error" << dendl;
      ret = -1;
      goto close_fsid_fd;

    }

    if (superblock.backend == "rocksdb")
      store->init(g_conf->keyvaluestore_rocksdb_options);
    else
      store->init();
    stringstream err;
    if (store->open(err)) {
      derr << "KeyValueStore::mount Error initializing keyvaluestore backend "
           << superblock.backend << ": " << err.str() << dendl;
      ret = -1;
      delete store;
      goto close_current_fd;
    }

    // get collection list
    set<string> keys;
    keys.insert("collections");
    map<string,bufferlist> values;
    store->get("meta", keys, &values);
    if (values.empty()) {
      ret = -EIO;
      derr << "Error no collection list; old store?" << dendl;
      goto close_current_fd;
    }
    bufferlist::iterator p = values["collections"].begin();
    ::decode(collections, p);
    dout(20) << "collections: " << collections << dendl;

    StripObjectMap *dbomap = new StripObjectMap(store);
    ret = dbomap->init(do_update);
    if (ret < 0) {
      delete dbomap;
      derr << "Error initializing StripObjectMap: " << ret << dendl;
      goto close_current_fd;
    }
    stringstream err2;

    if (g_conf->keyvaluestore_debug_check_backend && !dbomap->check(err2)) {
      derr << err2.str() << dendl;
      delete dbomap;
      ret = -EINVAL;
      goto close_current_fd;
    }

    default_strip_size = m_keyvaluestore_strip_size;
    backend.reset(dbomap);
  }

  op_tp.start();
  op_finisher.start();
  ondisk_finisher.start();

  // all okay.
  return 0;

close_current_fd:
  VOID_TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  return ret;
}

int KeyValueStore::umount()
{
  dout(5) << "umount " << basedir << dendl;

  op_tp.stop();
  op_finisher.stop();
  ondisk_finisher.stop();

  if (fsid_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
  if (current_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(current_fd));
    current_fd = -1;
  }

  backend.reset();

  // nothing
  return 0;
}

int KeyValueStore::queue_transactions(Sequencer *posr, list<Transaction*> &tls,
                                      TrackedOpRef osd_op,
                                      ThreadPool::TPHandle *handle)
{
  utime_t start = ceph_clock_now(g_ceph_context);
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
    dout(5) << "queue_transactions existing " << osr << " " << *osr << "/" << osr->parent
            << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << "queue_transactions new " << osr << " " << *osr << "/" << osr->parent << dendl;
  }

  Op *o = build_op(tls, ondisk, onreadable, onreadable_sync, osd_op);
  op_queue_reserve_throttle(o, handle);
  if (m_keyvaluestore_do_dump)
    dump_transactions(o->tls, o->op, osr);
  dout(5) << "queue_transactions (trailing journal) " << " " << tls <<dendl;
  queue_op(osr, o);

  utime_t end = ceph_clock_now(g_ceph_context);
  perf_logger->tinc(l_os_queue_lat, end - start);
  return 0;
}


// ============== KeyValueStore Op Handler =================

KeyValueStore::Op *KeyValueStore::build_op(list<Transaction*>& tls,
        Context *ondisk, Context *onreadable, Context *onreadable_sync,
        TrackedOpRef osd_op)
{
  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  Op *o = new Op;
  o->start = ceph_clock_now(g_ceph_context);
  o->tls.swap(tls);
  o->ondisk = ondisk;
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ops = ops;
  o->bytes = bytes;
  o->osd_op = osd_op;
  return o;
}

void KeyValueStore::queue_op(OpSequencer *osr, Op *o)
{
  // queue op on sequencer, then queue sequencer for the threadpool,
  // so that regardless of which order the threads pick up the
  // sequencer, the op order will be preserved.

  osr->queue(o);

  perf_logger->inc(l_os_ops);
  perf_logger->inc(l_os_bytes, o->bytes);

  dout(5) << "queue_op " << o << " seq " << o->op << " " << *osr << " "
          << o->bytes << " bytes" << "   (queue has " << throttle_ops.get_current()
          << " ops and " << throttle_bytes.get_current() << " bytes)" << dendl;
  op_wq.queue(osr);
}

void KeyValueStore::op_queue_reserve_throttle(Op *o, ThreadPool::TPHandle *handle)
{
  uint64_t max_ops = m_keyvaluestore_queue_max_ops;
  uint64_t max_bytes = m_keyvaluestore_queue_max_bytes;

  perf_logger->set(l_os_oq_max_ops, max_ops);
  perf_logger->set(l_os_oq_max_bytes, max_bytes);

  if (handle)
    handle->suspend_tp_timeout();
  if (throttle_ops.should_wait(1) ||
    (throttle_bytes.get_current()      // let single large ops through!
    && throttle_bytes.should_wait(o->bytes))) {
    dout(2) << "waiting " << throttle_ops.get_current() + 1 << " > " << max_ops << " ops || " 
      << throttle_bytes.get_current() + o->bytes << " > " << max_bytes << dendl;
  }
  throttle_ops.get();
  throttle_bytes.get(o->bytes);
  if (handle)
    handle->reset_tp_timeout();

  perf_logger->set(l_os_oq_ops, throttle_ops.get_current());
  perf_logger->set(l_os_oq_bytes, throttle_bytes.get_current());
}

void KeyValueStore::op_queue_release_throttle(Op *o)
{
  throttle_ops.put();
  throttle_bytes.put(o->bytes);
  perf_logger->set(l_os_oq_ops, throttle_ops.get_current());
  perf_logger->set(l_os_oq_bytes, throttle_bytes.get_current());
}

void KeyValueStore::_do_op(OpSequencer *osr, ThreadPool::TPHandle &handle)
{
  // FIXME: Suppose the collection of transaction only affect objects in the
  // one PG, so this lock will ensure no other concurrent write operation
  osr->apply_lock.Lock();
  Op *o = osr->peek_queue();
  dout(5) << "_do_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " start" << dendl;
  int r = _do_transactions(o->tls, o->op, &handle);
  dout(10) << "_do_op " << o << " seq " << o->op << " r = " << r
           << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;

  if (o->ondisk) {
    if (r < 0) {
      delete o->ondisk;
      o->ondisk = 0;
    } else {
      ondisk_finisher.queue(o->ondisk, r);
    }
  }
}

void KeyValueStore::_finish_op(OpSequencer *osr)
{
  list<Context*> to_queue;
  Op *o = osr->dequeue(&to_queue);

  utime_t lat = ceph_clock_now(g_ceph_context);
  lat -= o->start;

  dout(10) << "_finish_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " lat " << lat << dendl;
  osr->apply_lock.Unlock();  // locked in _do_op
  op_queue_release_throttle(o);

  perf_logger->tinc(l_os_commit_lat, lat);
  perf_logger->tinc(l_os_apply_lat, lat);

  if (o->onreadable_sync) {
    o->onreadable_sync->complete(0);
  }
  if (o->onreadable)
    op_finisher.queue(o->onreadable);
  if (!to_queue.empty())
    op_finisher.queue(to_queue);
  delete o;
}

// Combine all the ops in the same transaction using "BufferTransaction" and
// cache the middle results in order to make visible to the following ops.
//
// Lock: KeyValueStore use "in_use" in GenericObjectMap to avoid concurrent
// operation on the same object. Not sure ReadWrite lock should be applied to
// improve concurrent performance. In the future, I'd like to remove apply_lock
// on "osr" and introduce PG RWLock.
int KeyValueStore::_do_transactions(list<Transaction*> &tls, uint64_t op_seq,
  ThreadPool::TPHandle *handle)
{
  int r = 0;
  int trans_num = 0;
  BufferTransaction bt(this);

  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++) {
    r = _do_transaction(**p, bt, handle);
    if (r < 0)
      break;
    if (handle)
      handle->reset_tp_timeout();
  }

  r = bt.submit_transaction();
  if (r < 0) {
    assert(0 == "unexpected error");  // FIXME
  }

  return r;
}

unsigned KeyValueStore::_do_transaction(Transaction& transaction,
                                        BufferTransaction &t,
                                        ThreadPool::TPHandle *handle)
{
  dout(10) << "_do_transaction on " << &transaction << dendl;

  Transaction::iterator i = transaction.begin();
  uint64_t op_num = 0;
  bool exist_clone = false;

  while (i.have_op()) {
    if (handle)
      handle->reset_tp_timeout();

    Transaction::Op *op = i.decode_op();
    int r = 0;

    switch (op->op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _touch(cid, oid, t);
      }
      break;

    case Transaction::OP_WRITE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
        r = _write(cid, oid, off, len, bl, t, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        r = _zero(cid, oid, off, len, t);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        r = _truncate(cid, oid, off, t);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _remove(cid, oid, t);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        map<string, bufferptr> to_set;
        to_set[name] = bufferptr(bl.c_str(), bl.length());
        r = _setattrs(cid, oid, to_set, t);
        if (r == -ENOSPC)
          dout(0) << " ENOSPC on setxattr on " << cid << "/" << oid
                  << " name " << name << " size " << bl.length() << dendl;
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
        r = _setattrs(cid, oid, aset, t);
        if (r == -ENOSPC)
          dout(0) << " ENOSPC on setxattrs on " << cid << "/" << oid << dendl;
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
        r = _rmattr(cid, oid, name.c_str(), t);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _rmattrs(cid, oid, t);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        exist_clone = true;
        r = _clone(cid, oid, noid, t);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        exist_clone = true;
        r = _clone_range(cid, oid, noid, off, len, off, t);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
        exist_clone = true;
        r = _clone_range(cid, oid, noid, srcoff, len, dstoff, t);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        r = _create_collection(cid, t);
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
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        r = _destroy_collection(cid, t);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _collection_add(ncid, ocid, oid, t);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _remove(cid, oid, t);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      {
        // WARNING: this is deprecated and buggy; only here to replay old journals.
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _collection_move_rename(ocid, oid, ncid, oid, t);
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
        r = _collection_move_rename(oldcid, oldoid, newcid, newoid, t);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
    case Transaction::OP_COLL_RMATTR:
      assert(0 == "coll attrs no longer supported");
      break;

    case Transaction::OP_STARTSYNC:
      {
        start_sync();
        break;
      }

    case Transaction::OP_COLL_RENAME:
      {
        assert(0 == "not implemented");
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _omap_clear(cid, oid, t);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferlist> aset;
        i.decode_attrset(aset);
        r = _omap_setkeys(cid, oid, aset, t);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        set<string> keys;
        i.decode_keyset(keys);
        r = _omap_rmkeys(cid, oid, keys, t);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        r = _omap_rmkeyrange(cid, oid, first, last, t);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
        r = _omap_setheader(cid, oid, bl, t);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
        r = _split_collection_create(cid, bits, rem, dest, t);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
        r = _split_collection(cid, bits, rem, dest, t);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
        r = _set_alloc_hint(cid, oid, expected_object_size,
                            expected_write_size, t);
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
        // -ENOENT is normally okay
        // ...including on a replayed OP_RMCOLL with checkpoint mode
        ok = true;
      if (r == -ENODATA)
        ok = true;

      if (!ok) {
        const char *msg = "unexpected error code";

        if (exist_clone) {
          dout(0) << "BUG: clone failed will lead to paritial transaction applied" << dendl;
        }

        if (r == -ENOSPC)
          // For now, if we hit _any_ ENOSPC, crash, before we do any damage
          // by partially applying transactions.
          msg = "ENOSPC handling not implemented";

        if (r == -ENOTEMPTY) {
          msg = "ENOTEMPTY suggests garbage data in osd data dir";
        }

        dout(0) << " error " << cpp_strerror(r) << " not handled on operation "
                << op->op << " op " << op_num << ", counting from 0)" << dendl;
        dout(0) << msg << dendl;
        dout(0) << " transaction dump:\n";
        JSONFormatter f(true);
        f.open_object_section("transaction");
        transaction.dump(&f);
        f.close_section();
        f.flush(*_dout);
        *_dout << dendl;
        assert(0 == "unexpected error");

        if (r == -EMFILE) {
          dump_open_fds(g_ceph_context);
        }
      }
    }

    op_num++;
  }

  return 0;  // FIXME count errors
}


// =========== KeyValueStore Op Implementation ==============
// objects

bool KeyValueStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << "collection: " << cid << " object: " << oid
           << dendl;
  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = backend->lookup_strip_header(cid, oid, &header);
  if (r < 0) {
    return false;
  }

  return true;
}

int KeyValueStore::stat(coll_t cid, const ghobject_t& oid,
                        struct stat *st, bool allow_eio)
{
  dout(10) << "stat " << cid << "/" << oid << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = backend->lookup_strip_header(cid, oid, &header);
  if (r < 0) {
    dout(10) << "stat " << cid << "/" << oid << "=" << r << dendl;
    return -ENOENT;
  }

  st->st_blocks = header->max_size / header->strip_size;
  if (header->max_size % header->strip_size)
    st->st_blocks++;
  st->st_nlink = 1;
  st->st_size = header->max_size;
  st->st_blksize = header->strip_size;

  return r;
}

int KeyValueStore::_generic_read(StripObjectMap::StripObjectHeaderRef header,
                                 uint64_t offset, size_t len, bufferlist& bl,
                                 bool allow_eio, BufferTransaction *bt)
{
  if (header->max_size < offset) {
    dout(10) << __func__ << " " << header->cid << "/" << header->oid << ")"
             << " offset exceed the length of bl"<< dendl;
    return 0;
  }

  if (len == 0)
    len = header->max_size - offset;

  if (offset + len > header->max_size)
    len = header->max_size - offset;

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header->strip_size,
                                  extents);
  map<string, bufferlist> out;
  set<string> keys;
  pair<coll_t, ghobject_t> uid = make_pair(header->cid, header->oid);

  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    bufferlist old;
    string key = strip_object_key(iter->no);

    map< pair<coll_t, ghobject_t>, map<pair<string, string>, bufferlist> >::iterator obj_it;
    if ( bt ){
      obj_it = bt->buffers.find(uid);
    }
    if ( bt && obj_it != bt->buffers.end() && obj_it->second.count(make_pair(OBJECT_STRIP_PREFIX, key))) {
      // use strip_header buffer
      assert(header->bits[iter->no]);
      out[key] = obj_it->second[make_pair(OBJECT_STRIP_PREFIX, key)];
    }else if (header->bits[iter->no]) {
      keys.insert(key);
    }
  }


  int r = backend->get_values_with_header(header, OBJECT_STRIP_PREFIX, keys, &out);
  r = check_get_rc(header->cid, header->oid, r, out.size() == keys.size());
  if (r < 0)
    return r;

  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    string key = strip_object_key(iter->no);

    if (header->bits[iter->no]) {
      if (iter->len == header->strip_size) {
        bl.claim_append(out[key]);
      } else {
        out[key].copy(iter->offset, iter->len, bl);
      }
    } else {
      bl.append_zero(iter->len);
    }
  }

  dout(10) << __func__ << " " << header->cid << "/" << header->oid << " "
           << offset << "~" << bl.length() << "/" << len << " r = " << r
           << dendl;

  return bl.length();
}


int KeyValueStore::read(coll_t cid, const ghobject_t& oid, uint64_t offset,
                        size_t len, bufferlist& bl, uint32_t op_flags,
			bool allow_eio)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
           << len << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = backend->lookup_strip_header(cid, oid, &header);

  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
              << len << " header isn't exist: r = " << r << dendl;
    return r;
  }

  return _generic_read(header, offset, len, bl, allow_eio);
}

int KeyValueStore::fiemap(coll_t cid, const ghobject_t& oid,
                          uint64_t offset, size_t len, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
           << len << dendl;
  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = backend->lookup_strip_header(cid, oid, &header);
  if (r < 0) {
    dout(10) << "fiemap " << cid << "/" << oid << " " << offset << "~" << len
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header->strip_size,
                                  extents);

  map<uint64_t, uint64_t> m;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    if (header->bits[iter->no]) {
      uint64_t off = iter->no * header->strip_size + iter->offset;
      m[off] = iter->len;
    }
  }
  ::encode(m, bl);
  return 0;
}

int KeyValueStore::_remove(coll_t cid, const ghobject_t& oid,
                           BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " "
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  header->max_size = 0;
  header->bits.clear();
  header->updated = true;
  r = t.clear_buffer(header);

  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_truncate(coll_t cid, const ghobject_t& oid, uint64_t size,
                             BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " size " << size
           << dendl;

  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << size
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  if (header->max_size == size)
    return 0;

  if (header->max_size > size) {
    vector<StripObjectMap::StripExtent> extents;
    StripObjectMap::file_to_extents(size, header->max_size-size,
                                    header->strip_size, extents);
    assert(extents.size());

    vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
    if (header->bits[iter->no] && iter->offset != 0) {
      bufferlist value;
      map<string, bufferlist> values;
      set<string> lookup_keys;
      string key = strip_object_key(iter->no);

      lookup_keys.insert(key);
      r = t.get_buffer_keys(header, OBJECT_STRIP_PREFIX,
                            lookup_keys, &values);
      r = check_get_rc(cid, oid, r, lookup_keys.size() == values.size());
      if (r < 0)
        return r;

      values[key].copy(0, iter->offset, value);
      value.append_zero(header->strip_size-iter->offset);
      assert(value.length() == header->strip_size);
      value.swap(values[key]);

      t.set_buffer_keys(header, OBJECT_STRIP_PREFIX, values);
      ++iter;
    }

    set<string> keys;
    for (; iter != extents.end(); ++iter) {
      if (header->bits[iter->no]) {
        keys.insert(strip_object_key(iter->no));
        header->bits[iter->no] = 0;
      }
    }
    r = t.remove_buffer_keys(header, OBJECT_STRIP_PREFIX, keys);
    if (r < 0) {
      dout(10) << __func__ << " " << cid << "/" << oid << " "
               << size << " = " << r << dendl;
      return r;
    }
  }

  header->bits.resize(size/header->strip_size+1);
  header->max_size = size;
  header->updated = true;

  dout(10) << __func__ << " " << cid << "/" << oid << " size " << size << " = "
           << r << dendl;
  return r;
}

int KeyValueStore::_touch(coll_t cid, const ghobject_t& oid,
                          BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " "
             << " failed to get header: r = " << r << dendl;
    r = -EINVAL;
    return r;
  }

  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_generic_write(StripObjectMap::StripObjectHeaderRef header,
                                  uint64_t offset, size_t len,
                                  const bufferlist& bl, BufferTransaction &t,
                                  uint32_t fadvise_flags)
{
  if (len > bl.length())
    len = bl.length();

  if (len + offset > header->max_size) {
    header->max_size = len + offset;
    header->bits.resize(header->max_size/header->strip_size+1);
    header->updated = true;
  }

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header->strip_size,
                                  extents);

  map<string, bufferlist> out;
  set<string> keys;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    if (header->bits[iter->no] && !(iter->offset == 0 &&
                                   iter->len == header->strip_size))
      keys.insert(strip_object_key(iter->no));
  }

  int r = t.get_buffer_keys(header, OBJECT_STRIP_PREFIX, keys, &out);
  r = check_get_rc(header->cid, header->oid, r, keys.size() == out.size());
  if (r < 0) 
    return r;

  uint64_t bl_offset = 0;
  map<string, bufferlist> values;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    bufferlist value;
    string key = strip_object_key(iter->no);
    if (header->bits[iter->no]) {
      if (iter->offset == 0 && iter->len == header->strip_size) {
        bl.copy(bl_offset, iter->len, value);
        bl_offset += iter->len;
      } else {
        assert(out[key].length() == header->strip_size);

        out[key].copy(0, iter->offset, value);
        bl.copy(bl_offset, iter->len, value);
        bl_offset += iter->len;

        if (value.length() != header->strip_size)
          out[key].copy(value.length(), header->strip_size-value.length(),
                        value);
      }
    } else {
      if (iter->offset)
        value.append_zero(iter->offset);
      bl.copy(bl_offset, iter->len, value);
      bl_offset += iter->len;

      if (value.length() < header->strip_size)
        value.append_zero(header->strip_size-value.length());

      header->bits[iter->no] = 1;
      header->updated = true;
    }
    assert(value.length() == header->strip_size);
    values[key].swap(value);
  }
  assert(bl_offset == len);

  t.set_buffer_keys(header, OBJECT_STRIP_PREFIX, values);
  dout(10) << __func__ << " " << header->cid << "/" << header->oid << " "
           << offset << "~" << len << " = " << r << dendl;

  return r;
}

int KeyValueStore::_write(coll_t cid, const ghobject_t& oid,
                          uint64_t offset, size_t len, const bufferlist& bl,
                          BufferTransaction &t, uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
           << len << dendl;

  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << offset
             << "~" << len << " failed to get header: r = " << r << dendl;
    return r;
  }

  return _generic_write(header, offset, len, bl, t, fadvise_flags);
}

int KeyValueStore::_zero(coll_t cid, const ghobject_t& oid, uint64_t offset,
                         size_t len, BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~" << len << dendl;
  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << offset
             << "~" << len << " failed to get header: r = " << r << dendl;
    return r;
  }

  if (len + offset > header->max_size) {
    header->max_size = len + offset;
    header->bits.resize(header->max_size/header->strip_size+1);
    header->updated = true;
  }

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header->strip_size,
                                  extents);
  set<string> rm_keys;
  set<string> lookup_keys;
  map<string, bufferlist> values;
  map<string, pair<uint64_t, uint64_t> > off_len;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    string key = strip_object_key(iter->no);
    if (header->bits[iter->no]) {
      if (iter->offset == 0 && iter->len == header->strip_size) {
        rm_keys.insert(key);
        header->bits[iter->no] = 0;
        header->updated = true;
      } else {
        lookup_keys.insert(key);
        off_len[key] = make_pair(iter->offset, iter->len);
      }
    }    
  }
  r = t.get_buffer_keys(header, OBJECT_STRIP_PREFIX,
                        lookup_keys, &values);
  r = check_get_rc(header->cid, header->oid, r, lookup_keys.size() == values.size());
  if (r < 0)
    return r;
  for(set<string>::iterator it = lookup_keys.begin(); it != lookup_keys.end(); ++it)
  {
    pair<uint64_t, uint64_t> p = off_len[*it];
    values[*it].zero(p.first, p.second);
  }
  t.set_buffer_keys(header, OBJECT_STRIP_PREFIX, values);
  t.remove_buffer_keys(header, OBJECT_STRIP_PREFIX, rm_keys);
  dout(10) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
           << len << " = " << r << dendl;
  return r;
}

int KeyValueStore::_clone(coll_t cid, const ghobject_t& oldoid,
                          const ghobject_t& newoid, BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << dendl;

  if (oldoid == newoid)
    return 0;

  int r;
  StripObjectMap::StripObjectHeaderRef old_header;

  r = t.lookup_cached_header(cid, oldoid, &old_header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
             << newoid << " = " << r << dendl;
    return r;
  }

  t.clone_buffer(old_header, cid, newoid);

  dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_clone_range(coll_t cid, const ghobject_t& oldoid,
                                const ghobject_t& newoid, uint64_t srcoff,
                                uint64_t len, uint64_t dstoff,
                                BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << dendl;

  int r;
  bufferlist bl;

  StripObjectMap::StripObjectHeaderRef old_header, new_header;

  r = t.lookup_cached_header(cid, oldoid, &old_header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << " header isn't exist: r = " << r << dendl;
    return r;
  }

  r = t.lookup_cached_header(cid, newoid, &new_header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << " can't create header: r = " << r << dendl;
    return r;
  }

  r = _generic_read(old_header, srcoff, len, bl, &t);
  if (r < 0)
    goto out;

  r = _generic_write(new_header, dstoff, len, bl, t);

 out:
  dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << " = " << r << dendl;
  return r;
}

// attrs

int KeyValueStore::getattr(coll_t cid, const ghobject_t& oid, const char *name,
                           bufferptr &bp)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " '" << name << "'"
           << dendl;

  int r;
  map<string, bufferlist> got;
  set<string> to_get;
  StripObjectMap::StripObjectHeaderRef header;

  to_get.insert(string(name));

  r = backend->lookup_strip_header(cid, oid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_values_with_header(header, OBJECT_XATTR, to_get, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " get_xattrs err r =" << r << dendl;
    goto out;
  }
  if (got.empty()) {
    dout(10) << __func__ << " got.size() is 0" << dendl;
    return -ENODATA;
  }
  bp = bufferptr(got.begin()->second.c_str(),
                 got.begin()->second.length());
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << "/" << oid << " '" << name << "' = "
           << r << dendl;
  return r;
}

int KeyValueStore::getattrs(coll_t cid, const ghobject_t& oid,
                           map<string,bufferptr>& aset)
{
  map<string, bufferlist> attr_aset;
  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = backend->lookup_strip_header(cid, oid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_with_header(header, OBJECT_XATTR, &attr_aset);
  if (r < 0) {
    dout(10) << __func__ << " could not get attrs r = " << r << dendl;
    goto out;
  }

  for (map<string, bufferlist>::iterator i = attr_aset.begin();
       i != attr_aset.end(); ++i) {
    string key;
    key = i->first;
    aset.insert(make_pair(key,
                bufferptr(i->second.c_str(), i->second.length())));
  }

 out:
  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;

  return r;
}

int KeyValueStore::_setattrs(coll_t cid, const ghobject_t& oid,
                             map<string, bufferptr>& aset,
                             BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;

  StripObjectMap::StripObjectHeaderRef header;
  map<string, bufferlist> attrs;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0)
    goto out;

  for (map<string, bufferptr>::iterator it = aset.begin();
       it != aset.end(); ++it) {
    attrs[it->first].push_back(it->second);
  }

  t.set_buffer_keys(header, OBJECT_XATTR, attrs);

out:
  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}


int KeyValueStore::_rmattr(coll_t cid, const ghobject_t& oid, const char *name,
                           BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " '" << name << "'"
           << dendl;

  int r;
  set<string> to_remove;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r
             << dendl;
    return r;
  }

  to_remove.insert(string(name));
  r = t.remove_buffer_keys(header, OBJECT_XATTR, to_remove);

  dout(10) << __func__ << " " << cid << "/" << oid << " '" << name << "' = "
           << r << dendl;
  return r;
}

int KeyValueStore::_rmattrs(coll_t cid, const ghobject_t& oid,
                            BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  set<string> attrs;

  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r
             << dendl;
    return r;
  }

  r = backend->get_keys_with_header(header, OBJECT_XATTR, &attrs);
  if (r < 0) {
    dout(10) << __func__ << " could not get attrs r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(header, OBJECT_XATTR, attrs);
  t.clear_buffer_keys(header, OBJECT_XATTR);

  dout(10) << __func__ <<  " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}


// collections

int KeyValueStore::_create_collection(coll_t c, BufferTransaction &t)
{
  dout(15) << __func__ << " " << c << dendl;
  int r = 0;
  bufferlist bl;

  RWLock::WLocker l(collections_lock);
  if (collections.count(c)) {
    r = -EEXIST;
    goto out;
  }

  collections.insert(c);
  t.set_collections(collections);

 out:
  dout(10) << __func__ << " cid " << c << " r = " << r << dendl;
  return r;
}

int KeyValueStore::_destroy_collection(coll_t c, BufferTransaction &t)
{
  dout(15) << __func__ << " " << c << dendl;

  int r;
  uint64_t modified_object = 0;
  vector<ghobject_t> oids;
  bufferlist bl;

  {
    RWLock::RLocker l(collections_lock);
    if (!collections.count(c)) {
      r = -ENOENT;
      goto out;
    }
  }

  // All modified objects are marked deleted
  for (BufferTransaction::StripHeaderMap::iterator iter = t.strip_headers.begin();
       iter != t.strip_headers.end(); ++iter) {
    // sum the total modified object in this PG
    if (iter->first.first != c)
      continue;

    modified_object++;
    if (!iter->second->deleted) {
      r = -ENOTEMPTY;
      goto out;
    }
  }

  r = backend->list_objects(c, ghobject_t(), ghobject_t::get_max(), modified_object+1, &oids,
                            0);
  // No other object
  if (oids.size() != modified_object && oids.size() != 0) {
    r = -ENOTEMPTY;
    goto out;
  }

  for (vector<ghobject_t>::iterator iter = oids.begin();
      iter != oids.end(); ++iter) {
    if (!t.strip_headers.count(make_pair(c, *iter))) {
      r = -ENOTEMPTY;
      goto out;
    }
  }

  {
    RWLock::WLocker l(collections_lock);
    collections.erase(c);
    t.set_collections(collections);
  }
  r = 0;

out:
  dout(10) << __func__ << " " << c << " = " << r << dendl;
  return r;
}


int KeyValueStore::_collection_add(coll_t c, coll_t oldcid,
                                   const ghobject_t& o,
                                   BufferTransaction &t)
{
  dout(15) << __func__ <<  " " << c << "/" << o << " from " << oldcid << "/"
           << o << dendl;

  bufferlist bl;
  StripObjectMap::StripObjectHeaderRef header, old_header;

  int r = t.lookup_cached_header(oldcid, o, &old_header, false);
  if (r < 0) {
    goto out;
  }

  r = t.lookup_cached_header(c, o, &header, false);
  if (r == 0) {
    r = -EEXIST;
    dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
             << o << " already exist " << dendl;
    goto out;
  }

  r = _generic_read(old_header, 0, old_header->max_size, bl, &t);
  if (r < 0) {
    r = -EINVAL;
    goto out;
  }

  r = _generic_write(header, 0, bl.length(), bl, t);
  if (r < 0) {
    r = -EINVAL;
  }

out:
  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
           << o << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_move_rename(coll_t oldcid,
                                           const ghobject_t& oldoid,
                                           coll_t c, const ghobject_t& o,
                                           BufferTransaction &t)
{
  dout(15) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
           << oldoid << dendl;
  int r;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(c, o, &header, false);
  if (r == 0) {
    dout(10) << __func__ << " " << oldcid << "/" << oldoid << " -> " << c
             << "/" << o << " = " << r << dendl;
    return -EEXIST;
  }

  r = t.lookup_cached_header(oldcid, oldoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << oldcid << "/" << oldoid << " -> " << c
             << "/" << o << " = " << r << dendl;
    return r;
  }

  t.rename_buffer(header, c, o);

  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
           << oldoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_remove_recursive(const coll_t &cid,
                                                BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r = 0;

  {
    RWLock::RLocker l(collections_lock);
    if (collections.count(cid) == 0)
      return -ENOENT;
  }

  vector<ghobject_t> objects;
  ghobject_t max;
  while (!max.is_max()) {
    r = collection_list(cid, max, ghobject_t::get_max(), true, 300, &objects, &max);
    if (r < 0)
      goto out;

    for (vector<ghobject_t>::iterator i = objects.begin();
         i != objects.end(); ++i) {
      r = _remove(cid, *i, t);
      if (r < 0)
	goto out;
    }
  }

  {
    RWLock::WLocker l(collections_lock);
    collections.erase(cid);
    t.set_collections(collections);
  }

 out:
  dout(10) << __func__ << " " << cid  << " r = " << r << dendl;
  return r;
}

int KeyValueStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << " " << dendl;
  RWLock::RLocker l(collections_lock);
  for (set<coll_t>::iterator p = collections.begin(); p != collections.end();
       ++p) {
    ls.push_back(*p);
  }
  return 0;
}

bool KeyValueStore::collection_exists(coll_t c)
{
  dout(10) << __func__ << " " << dendl;
  RWLock::RLocker l(collections_lock);
  return collections.count(c);
}

bool KeyValueStore::collection_empty(coll_t c)
{
  dout(10) << __func__ << " " << dendl;

  vector<ghobject_t> oids;
  backend->list_objects(c, ghobject_t(), ghobject_t::get_max(), 1, &oids, 0);

  return oids.empty();
}

int KeyValueStore::collection_list(coll_t c, ghobject_t start,
				   ghobject_t end, bool sort_bitwise, int max,
				   vector<ghobject_t> *ls, ghobject_t *next)
{
  if (!sort_bitwise)
    return -EOPNOTSUPP;

  if (max < 0)
    return -EINVAL;

  if (start.is_max())
    return 0;

  int r = backend->list_objects(c, start, end, max, ls, next);
  return r;
}

int KeyValueStore::collection_version_current(coll_t c, uint32_t *version)
{
  *version = COLLECTION_VERSION;
  if (*version == target_version)
    return 1;
  else
    return 0;
}

// omap

int KeyValueStore::omap_get(coll_t c, const ghobject_t &hoid,
                            bufferlist *bl, map<string, bufferlist> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = backend->lookup_strip_header(c, hoid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_with_header(header, OBJECT_OMAP, out);
  if (r < 0) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  set<string> keys;
  map<string, bufferlist> got;

  keys.insert(OBJECT_OMAP_HEADER_KEY);
  r = backend->get_values_with_header(header, OBJECT_OMAP_HEADER, keys, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  if (!got.empty()) {
    assert(got.size() == 1);
    bl->swap(got.begin()->second);
  }

  return 0;
}

int KeyValueStore::omap_get_header(coll_t c, const ghobject_t &hoid,
                                   bufferlist *bl, bool allow_eio)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  set<string> keys;
  map<string, bufferlist> got;
  StripObjectMap::StripObjectHeaderRef header;

  int r = backend->lookup_strip_header(c, hoid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  keys.insert(OBJECT_OMAP_HEADER_KEY);
  r = backend->get_values_with_header(header, OBJECT_OMAP_HEADER, keys, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  if (!got.empty()) {
    assert(got.size() == 1);
    bl->swap(got.begin()->second);
  }

  return 0;
}

int KeyValueStore::omap_get_keys(coll_t c, const ghobject_t &hoid, set<string> *keys)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;
  int r = backend->lookup_strip_header(c, hoid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_keys_with_header(header, OBJECT_OMAP, keys);
  if (r < 0) {
    return r;
  }
  return 0;
}

int KeyValueStore::omap_get_values(coll_t c, const ghobject_t &hoid,
                                   const set<string> &keys,
                                   map<string, bufferlist> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;
  int r = backend->lookup_strip_header(c, hoid, &header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_values_with_header(header, OBJECT_OMAP, keys, out);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

int KeyValueStore::omap_check_keys(coll_t c, const ghobject_t &hoid,
                                   const set<string> &keys, set<string> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  int r = backend->check_keys(c, hoid, OBJECT_OMAP, keys, out);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

ObjectMap::ObjectMapIterator KeyValueStore::get_omap_iterator(
    coll_t c, const ghobject_t &hoid)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  return backend->get_iterator(c, hoid, OBJECT_OMAP);
}

int KeyValueStore::_omap_clear(coll_t cid, const ghobject_t &hoid,
                               BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  set<string> keys;
  r = backend->get_keys_with_header(header, OBJECT_OMAP, &keys);
  if (r < 0) {
    dout(10) << __func__ << " could not get omap_keys r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(header, OBJECT_OMAP, keys);
  if (r < 0) {
    dout(10) << __func__ << " could not remove keys r = " << r << dendl;
    return r;
  }

  keys.clear();
  keys.insert(OBJECT_OMAP_HEADER_KEY);
  r = t.remove_buffer_keys(header, OBJECT_OMAP_HEADER, keys);
  if (r < 0) {
    dout(10) << __func__ << " could not remove keys r = " << r << dendl;
    return r;
  }

  t.clear_buffer_keys(header, OBJECT_OMAP_HEADER);

  dout(10) << __func__ << " " << cid << "/" << hoid << " r = " << r << dendl;
  return 0;
}

int KeyValueStore::_omap_setkeys(coll_t cid, const ghobject_t &hoid,
                                 map<string, bufferlist> &aset,
                                 BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  t.set_buffer_keys(header, OBJECT_OMAP, aset);

  return 0;
}

int KeyValueStore::_omap_rmkeys(coll_t cid, const ghobject_t &hoid,
                                const set<string> &keys,
                                BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeaderRef header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(header, OBJECT_OMAP, keys);

  dout(10) << __func__ << " " << cid << "/" << hoid << " r = " << r << dendl;
  return r;
}

int KeyValueStore::_omap_rmkeyrange(coll_t cid, const ghobject_t &hoid,
                                    const string& first, const string& last,
                                    BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << " [" << first << ","
           << last << "]" << dendl;

  set<string> keys;
  {
    ObjectMap::ObjectMapIterator iter = get_omap_iterator(cid, hoid);
    if (!iter)
      return -ENOENT;

    for (iter->lower_bound(first); iter->valid() && iter->key() < last;
         iter->next(false)) {
      keys.insert(iter->key());
    }
  }
  return _omap_rmkeys(cid, hoid, keys, t);
}

int KeyValueStore::_omap_setheader(coll_t cid, const ghobject_t &hoid,
                                   const bufferlist &bl,
                                   BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  map<string, bufferlist> sets;
  StripObjectMap::StripObjectHeaderRef header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  sets[OBJECT_OMAP_HEADER_KEY] = bl;
  t.set_buffer_keys(header, OBJECT_OMAP_HEADER, sets);
  return 0;
}

int KeyValueStore::_split_collection(coll_t cid, uint32_t bits, uint32_t rem,
                                     coll_t dest, BufferTransaction &t)
{
  {
    dout(15) << __func__ << " " << cid << " bits: " << bits << dendl;

    StripObjectMap::StripObjectHeaderRef header;

    {
      RWLock::RLocker l(collections_lock);
      if (collections.count(cid) == 0)
	return -ENOENT;
      if (collections.count(dest) == 0)
	return -ENOENT;
    }

    vector<ghobject_t> objects;
    ghobject_t next, current;
    int move_size = 0;
    while (1) {
      collection_list(cid, current, ghobject_t::get_max(), true,
		      get_ideal_list_max(), &objects, &next);

      dout(20) << __func__ << cid << "objects size: " << objects.size()
              << dendl;

      if (objects.empty())
        break;

      for (vector<ghobject_t>::iterator i = objects.begin();
          i != objects.end(); ++i) {
        if (i->match(bits, rem)) {
          if (_collection_move_rename(cid, *i, dest, *i, t) < 0) {
            return -1;
          }
          move_size++;
        }
      }

      objects.clear();
      current = next;
    }

    dout(20) << __func__ << "move" << move_size << " object from " << cid
             << "to " << dest << dendl;
  }

  if (g_conf->filestore_debug_verify_split) {
    vector<ghobject_t> objects;
    ghobject_t next;
    while (1) {
      collection_list(cid, next, ghobject_t::get_max(), true,
		      get_ideal_list_max(), &objects, &next);
      if (objects.empty())
        break;

      for (vector<ghobject_t>::iterator i = objects.begin();
           i != objects.end(); ++i) {
        dout(20) << __func__ << ": " << *i << " still in source "
                 << cid << dendl;
        assert(!i->match(bits, rem));
      }
      objects.clear();
    }

    next = ghobject_t();
    while (1) {
      collection_list(dest, next, ghobject_t::get_max(), true,
		      get_ideal_list_max(), &objects, &next);
      if (objects.empty())
        break;

      for (vector<ghobject_t>::iterator i = objects.begin();
           i != objects.end(); ++i) {
        dout(20) << __func__ << ": " << *i << " now in dest "
                 << *i << dendl;
        assert(i->match(bits, rem));
      }
      objects.clear();
    }
  }
  return 0;
}

int KeyValueStore::_set_alloc_hint(coll_t cid, const ghobject_t& oid,
                                   uint64_t expected_object_size,
                                   uint64_t expected_write_size,
                                   BufferTransaction &t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " object_size "
           << expected_object_size << " write_size "
           << expected_write_size << dendl;

  int r = 0;
  StripObjectMap::StripObjectHeaderRef header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid
             << " failed to get header: r = " << r << dendl;
    return r;
  }

  bool blank = true;
  for (vector<char>::iterator it = header->bits.begin();
       it != header->bits.end(); ++it) {
    if (*it) {
      blank = false;
      break;
    }
  }

  // Now only consider to change "strip_size" when the object is blank,
  // because set_alloc_hint is expected to be very lightweight<O(1)>
  if (blank) {
    // header->strip_size = MIN(expected_write_size, m_keyvaluestore_max_expected_write_size);
    // dout(20) << __func__ << " hint " << header->strip_size << " success" << dendl;
  }

  dout(10) << __func__ << "" << cid << "/" << oid << " object_size "
           << expected_object_size << " write_size "
           << expected_write_size << " = " << r << dendl;

  return r;
}

const char** KeyValueStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "keyvaluestore_queue_max_ops",
    "keyvaluestore_queue_max_bytes",
    "keyvaluestore_default_strip_size",
    "keyvaluestore_dump_file",
    NULL
  };
  return KEYS;
}

void KeyValueStore::handle_conf_change(const struct md_config_t *conf,
                                       const std::set <std::string> &changed)
{
  if (changed.count("keyvaluestore_queue_max_ops") ||
      changed.count("keyvaluestore_queue_max_bytes") ||
      changed.count("keyvaluestore_max_expected_write_size")) {
    m_keyvaluestore_queue_max_ops = conf->keyvaluestore_queue_max_ops;
    m_keyvaluestore_queue_max_bytes = conf->keyvaluestore_queue_max_bytes;
    m_keyvaluestore_max_expected_write_size = conf->keyvaluestore_max_expected_write_size;
    throttle_ops.reset_max(conf->keyvaluestore_queue_max_ops);
    throttle_bytes.reset_max(conf->keyvaluestore_queue_max_bytes);
  }
  if (changed.count("keyvaluestore_default_strip_size")) {
    m_keyvaluestore_strip_size = conf->keyvaluestore_default_strip_size;
    default_strip_size = m_keyvaluestore_strip_size;
  }
  if (changed.count("keyvaluestore_dump_file")) {
    if (conf->keyvaluestore_dump_file.length() &&
	conf->keyvaluestore_dump_file != "-") {
      dump_start(conf->keyvaluestore_dump_file);
    } else {
      dump_stop();
    }
  }
}

int KeyValueStore::check_get_rc(const coll_t cid, const ghobject_t& oid, int r, bool is_equal_size)
{
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " "
             << " get rc = " <<  r << dendl;
  } else if (!is_equal_size) {
    dout(0) << __func__ << " broken header or missing data in backend "
            << cid << "/" << oid << " get rc = " << r << dendl;
    r = -EBADF;
  }
  return r;
}

void KeyValueStore::dump_start(const std::string &file)
{
  dout(10) << "dump_start " << file << dendl;
  if (m_keyvaluestore_do_dump) {
    dump_stop();
  }
  m_keyvaluestore_dump_fmt.reset();
  m_keyvaluestore_dump_fmt.open_array_section("dump");
  m_keyvaluestore_dump.open(file.c_str());
  m_keyvaluestore_do_dump = true;
}

void KeyValueStore::dump_stop()
{
  dout(10) << "dump_stop" << dendl;
  m_keyvaluestore_do_dump = false;
  if (m_keyvaluestore_dump.is_open()) {
    m_keyvaluestore_dump_fmt.close_section();
    m_keyvaluestore_dump_fmt.flush(m_keyvaluestore_dump);
    m_keyvaluestore_dump.flush();
    m_keyvaluestore_dump.close();
  }
}
void KeyValueStore::dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq, OpSequencer *osr)
{
  m_keyvaluestore_dump_fmt.open_array_section("transactions");
  unsigned trans_num = 0;
  for (list<ObjectStore::Transaction*>::iterator i = ls.begin(); i != ls.end(); ++i, ++trans_num) {
    m_keyvaluestore_dump_fmt.open_object_section("transaction");
    m_keyvaluestore_dump_fmt.dump_string("osr", osr->get_name());
    m_keyvaluestore_dump_fmt.dump_unsigned("seq", seq);
    m_keyvaluestore_dump_fmt.dump_unsigned("trans_num", trans_num);
    (*i)->dump(&m_keyvaluestore_dump_fmt);
    m_keyvaluestore_dump_fmt.close_section();
  }
  m_keyvaluestore_dump_fmt.close_section();
  m_keyvaluestore_dump_fmt.flush(m_keyvaluestore_dump);
  m_keyvaluestore_dump.flush();
}


// -- KVSuperblock --

void KVSuperblock::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  compat_features.encode(bl);
  ::encode(backend, bl);
  ENCODE_FINISH(bl);
}

void KVSuperblock::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  compat_features.decode(bl);
  ::decode(backend, bl);
  DECODE_FINISH(bl);
}

void KVSuperblock::dump(Formatter *f) const
{
  f->open_object_section("compat");
  compat_features.dump(f);
  f->dump_string("backend", backend);
  f->close_section();
}

void KVSuperblock::generate_test_instances(list<KVSuperblock*>& o)
{
  KVSuperblock z;
  o.push_back(new KVSuperblock(z));
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  z.compat_features = CompatSet(feature_compat, feature_ro_compat,
                                feature_incompat);
  o.push_back(new KVSuperblock(z));
  z.backend = "rocksdb";
  o.push_back(new KVSuperblock(z));
}
