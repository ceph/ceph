// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "ECBackend.h"
#include "PGBackend.h"
#include "OSD.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, PGBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

// -- ObjectModDesc --
struct RollbackVisitor : public ObjectModDesc::Visitor {
  const hobject_t &hoid;
  PGBackend *pg;
  ObjectStore::Transaction t;
  RollbackVisitor(
    const hobject_t &hoid,
    PGBackend *pg) : hoid(hoid), pg(pg) {}
  void append(uint64_t old_size) {
    ObjectStore::Transaction temp;
    pg->rollback_append(hoid, old_size, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void setattrs(map<string, boost::optional<bufferlist> > &attrs) {
    ObjectStore::Transaction temp;
    pg->rollback_setattrs(hoid, attrs, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void rmobject(version_t old_version) {
    ObjectStore::Transaction temp;
    pg->rollback_stash(hoid, old_version, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void create() {
    ObjectStore::Transaction temp;
    pg->rollback_create(hoid, &temp);
    temp.append(t);
    temp.swap(t);
  }
  void update_snaps(set<snapid_t> &snaps) {
    // pass
  }
};

void PGBackend::rollback(
  const hobject_t &hoid,
  const ObjectModDesc &desc,
  ObjectStore::Transaction *t)
{
  assert(desc.can_rollback());
  RollbackVisitor vis(hoid, this);
  desc.visit(&vis);
  t->append(vis.t);
}


void PGBackend::on_change(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t>::iterator i = temp_contents.begin();
       i != temp_contents.end();
       ++i) {
    dout(10) << __func__ << ": Removing oid "
	     << *i << " from the temp collection" << dendl;
    t->remove(
      get_temp_coll(t),
      ghobject_t(*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  temp_contents.clear();
  _on_change(t);
}

coll_t PGBackend::get_temp_coll(ObjectStore::Transaction *t)
{
  if (temp_created)
    return temp_coll;
  if (!store->collection_exists(temp_coll))
      t->create_collection(temp_coll);
  temp_created = true;
  return temp_coll;
}

int PGBackend::objects_list_partial(
  const hobject_t &begin,
  int min,
  int max,
  snapid_t seq,
  vector<hobject_t> *ls,
  hobject_t *next)
{
  assert(ls);
  ghobject_t _next(begin);
  ls->reserve(max);
  int r = 0;
  while (!_next.is_max() && ls->size() < (unsigned)min) {
    vector<ghobject_t> objects;
    int r = store->collection_list_partial(
      coll,
      _next,
      min - ls->size(),
      max - ls->size(),
      seq,
      &objects,
      &_next);
    if (r != 0)
      break;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      if (i->is_no_gen()) {
	ls->push_back(i->hobj);
      }
    }
  }
  if (r == 0)
    *next = _next.hobj;
  return r;
}

int PGBackend::objects_list_range(
  const hobject_t &start,
  const hobject_t &end,
  snapid_t seq,
  vector<hobject_t> *ls)
{
  assert(ls);
  vector<ghobject_t> objects;
  int r = store->collection_list_range(
    coll,
    start,
    end,
    seq,
    &objects);
  ls->reserve(objects.size());
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    if (i->is_no_gen()) {
      ls->push_back(i->hobj);
    }
  }
  return r;
}

int PGBackend::objects_get_attr(
  const hobject_t &hoid,
  const string &attr,
  bufferlist *out)
{
  bufferptr bp;
  int r = store->getattr(
    hoid.is_temp() ? temp_coll : coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    attr.c_str(),
    bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(bp);
  }
  return r;
}

int PGBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  return store->getattrs(
    hoid.is_temp() ? temp_coll : coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    *out);
}

void PGBackend::rollback_setattrs(
  const hobject_t &hoid,
  map<string, boost::optional<bufferlist> > &old_attrs,
  ObjectStore::Transaction *t) {
  map<string, bufferlist> to_set;
  set<string> to_remove;
  assert(!hoid.is_temp());
  for (map<string, boost::optional<bufferlist> >::iterator i = old_attrs.begin();
       i != old_attrs.end();
       ++i) {
    if (i->second) {
      to_set[i->first] = i->second.get();
    } else {
      t->rmattr(
	coll,
	ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	i->first);
    }
  }
  t->setattrs(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    to_set);
}

void PGBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_size,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    old_size);
}

void PGBackend::rollback_stash(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->remove(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  t->collection_move_rename(
    coll,
    ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard),
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::rollback_create(
  const hobject_t &hoid,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->remove(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::trim_stashed_object(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->remove(
    coll, ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard));
}

PGBackend *PGBackend::build_pg_backend(
  const pg_pool_t &pool,
  const OSDMapRef curmap,
  Listener *l,
  coll_t coll,
  coll_t temp_coll,
  ObjectStore *store,
  CephContext *cct)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED: {
    return new ReplicatedBackend(l, coll, temp_coll, store, cct);
  }
  case pg_pool_t::TYPE_ERASURE: {
    ErasureCodeInterfaceRef ec_impl;
    const map<string,string> &profile = curmap->get_erasure_code_profile(pool.erasure_code_profile);
    assert(profile.count("plugin"));
    stringstream ss;
    ceph::ErasureCodePluginRegistry::instance().factory(
      profile.find("plugin")->second,
      profile,
      &ec_impl,
      ss);
    assert(ec_impl);
    return new ECBackend(
      l,
      coll,
      temp_coll,
      store,
      cct,
      ec_impl,
      pool.stripe_width);
  }
  default:
    assert(0);
    return NULL;
  }
}

/*
 * pg lock may or may not be held
 */
void PGBackend::be_scan_list(
  ScrubMap &map, const vector<hobject_t> &ls, bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << "_scan_list scanning " << ls.size() << " objects"
           << (deep ? " deeply" : "") << dendl;
  int i = 0;
  for (vector<hobject_t>::const_iterator p = ls.begin();
       p != ls.end();
       ++p, i++) {
    handle.reset_tp_timeout();
    hobject_t poid = *p;

    struct stat st;
    int r = store->stat(
      coll,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &st,
      true);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      assert(!o.negative);
      store->getattrs(
	coll,
	ghobject_t(
	  poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	o.attrs);

      // calculate the CRC32 on deep scrubs
      if (deep) {
	be_deep_scrub(*p, o, handle);
      }

      dout(25) << "_scan_list  " << poid << dendl;
    } else if (r == -ENOENT) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", skipping" << dendl;
    } else if (r == -EIO) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", read_error" << dendl;
      ScrubMap::object &o = map.objects[poid];
      o.read_error = true;
    } else {
      derr << "_scan_list got: " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

enum scrub_error_type PGBackend::be_compare_scrub_objects(
  const ScrubMap::object &auth,
  const ScrubMap::object &candidate,
  ostream &errorstream)
{
  enum scrub_error_type error = CLEAN;
  if (candidate.read_error) {
    // This can occur on stat() of a shallow scrub, but in that case size will
    // be invalid, and this will be over-ridden below.
    error = DEEP_ERROR;
    errorstream << "candidate had a read error";
  }
  if (auth.digest_present && candidate.digest_present) {
    if (auth.digest != candidate.digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = DEEP_ERROR;

      errorstream << "digest " << candidate.digest
                  << " != known digest " << auth.digest;
    }
  }
  if (auth.omap_digest_present && candidate.omap_digest_present) {
    if (auth.omap_digest != candidate.omap_digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = DEEP_ERROR;

      errorstream << "omap_digest " << candidate.omap_digest
                  << " != known omap_digest " << auth.omap_digest;
    }
  }
  // Shallow error takes precendence because this will be seen by
  // both types of scrubs.
  if (auth.size != candidate.size) {
    if (error != CLEAN)
      errorstream << ", ";
    error = SHALLOW_ERROR;
    errorstream << "size " << candidate.size
		<< " != known size " << auth.size;
  }
  for (map<string,bufferptr>::const_iterator i = auth.attrs.begin();
       i != auth.attrs.end();
       ++i) {
    if (!candidate.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "missing attr " << i->first;
    } else if (candidate.attrs.find(i->first)->second.cmp(i->second)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "attr value mismatch " << i->first;
    }
  }
  for (map<string,bufferptr>::const_iterator i = candidate.attrs.begin();
       i != candidate.attrs.end();
       ++i) {
    if (!auth.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "extra attr " << i->first;
    }
  }
  return error;
}

map<pg_shard_t, ScrubMap *>::const_iterator
  PGBackend::be_select_auth_object(
  const hobject_t &obj,
  const map<pg_shard_t,ScrubMap*> &maps)
{
  map<pg_shard_t, ScrubMap *>::const_iterator auth = maps.end();
  for (map<pg_shard_t, ScrubMap *>::const_iterator j = maps.begin();
       j != maps.end();
       ++j) {
    map<hobject_t, ScrubMap::object>::iterator i =
      j->second->objects.find(obj);
    if (i == j->second->objects.end()) {
      continue;
    }
    if (auth == maps.end()) {
      // Something is better than nothing
      // TODO: something is NOT better than nothing, do something like
      // unfound_lost if no valid copies can be found, or just mark unfound
      auth = j;
      dout(10) << __func__ << ": selecting osd " << j->first
	       << " for obj " << obj
	       << ", auth == maps.end()"
	       << dendl;
      continue;
    }
    if (i->second.read_error) {
      // scrub encountered read error, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", read_error"
	       << dendl;
      continue;
    }
    map<string, bufferptr>::iterator k = i->second.attrs.find(OI_ATTR);
    if (k == i->second.attrs.end()) {
      // no object info on object, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", no oi attr"
	       << dendl;
      continue;
    }

    bufferlist bl;
    bl.push_back(k->second);
    object_info_t oi;
    try {
      bufferlist::iterator bliter = bl.begin();
      ::decode(oi, bliter);
    } catch (...) {
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", corrupt oi attr"
	       << dendl;
      // invalid object info, probably corrupt
      continue;
    }
    uint64_t correct_size = be_get_ondisk_size(oi.size);
    if (correct_size != i->second.size) {
      // invalid size, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", size mismatch"
	       << dendl;
      // invalid object info, probably corrupt
      continue;
    }
    dout(10) << __func__ << ": selecting osd " << j->first
	     << " for obj " << obj
	     << dendl;
    auth = j;
  }
  return auth;
}

void PGBackend::be_compare_scrubmaps(
  const map<pg_shard_t,ScrubMap*> &maps,
  map<hobject_t, set<pg_shard_t> > &missing,
  map<hobject_t, set<pg_shard_t> > &inconsistent,
  map<hobject_t, pg_shard_t> &authoritative,
  map<hobject_t, set<pg_shard_t> > &invalid_snapcolls,
  int &shallow_errors, int &deep_errors,
  const spg_t pgid,
  const vector<int> &acting,
  ostream &errorstream)
{
  map<hobject_t,ScrubMap::object>::const_iterator i;
  map<pg_shard_t, ScrubMap *>::const_iterator j;
  set<hobject_t> master_set;

  // Construct master set
  for (j = maps.begin(); j != maps.end(); ++j) {
    for (i = j->second->objects.begin(); i != j->second->objects.end(); ++i) {
      master_set.insert(i->first);
    }
  }

  // Check maps against master set and each other
  for (set<hobject_t>::const_iterator k = master_set.begin();
       k != master_set.end();
       ++k) {
    map<pg_shard_t, ScrubMap *>::const_iterator auth =
      be_select_auth_object(*k, maps);
    assert(auth != maps.end());
    set<pg_shard_t> cur_missing;
    set<pg_shard_t> cur_inconsistent;
    for (j = maps.begin(); j != maps.end(); ++j) {
      if (j == auth)
	continue;
      if (j->second->objects.count(*k)) {
	// Compare
	stringstream ss;
	enum scrub_error_type error = be_compare_scrub_objects(auth->second->objects[*k],
	    j->second->objects[*k],
	    ss);
        if (error != CLEAN) {
	  cur_inconsistent.insert(j->first);
          if (error == SHALLOW_ERROR)
	    ++shallow_errors;
          else
	    ++deep_errors;
	  errorstream << pgid << " shard " << j->first
		      << ": soid " << *k << " " << ss.str() << std::endl;
	}
      } else {
	cur_missing.insert(j->first);
	++shallow_errors;
	errorstream << pgid << " shard " << j->first
		    << " missing " << *k << std::endl;
      }
    }
    assert(auth != maps.end());
    if (!cur_missing.empty()) {
      missing[*k] = cur_missing;
    }
    if (!cur_inconsistent.empty()) {
      inconsistent[*k] = cur_inconsistent;
    }
    if (!cur_inconsistent.empty() || !cur_missing.empty()) {
      authoritative[*k] = auth->first;
    }
  }
}
