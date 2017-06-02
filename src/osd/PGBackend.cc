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
#include "common/scrub_types.h"
#include "ReplicatedBackend.h"
#include "ScrubStore.h"
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
  void try_rmobject(version_t old_version) {
    ObjectStore::Transaction temp;
    pg->rollback_try_stash(hoid, old_version, &temp);
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


void PGBackend::try_stash(
  const hobject_t &hoid,
  version_t v,
  ObjectStore::Transaction *t)
{
  t->try_rename(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    ghobject_t(hoid, v, get_parent()->whoami_shard().shard));
}

void PGBackend::on_change_cleanup(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = temp_contents.begin();
       i != temp_contents.end();
       ++i) {
    dout(10) << __func__ << ": Removing oid "
	     << *i << " from the temp collection" << dendl;
    t->remove(
      coll,
      ghobject_t(*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  temp_contents.clear();
}

int PGBackend::objects_list_partial(
  const hobject_t &begin,
  int min,
  int max,
  vector<hobject_t> *ls,
  hobject_t *next)
{
  assert(ls);
  // Starts with the smallest generation to make sure the result list
  // has the marker object (it might have multiple generations
  // though, which would be filtered).
  ghobject_t _next;
  if (!begin.is_min())
    _next = ghobject_t(begin, 0, get_parent()->whoami_shard().shard);
  ls->reserve(max);
  int r = 0;
  while (!_next.is_max() && ls->size() < (unsigned)min) {
    vector<ghobject_t> objects;
    r = store->collection_list(
      ch,
      _next,
      ghobject_t::get_max(),
      parent->sort_bitwise(),
      max - ls->size(),
      &objects,
      &_next);
    if (r != 0) {
      derr << __func__ << " list collection " << ch << " got: " << cpp_strerror(r) << dendl;
      break;
    }
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      if (i->is_pgmeta() || i->hobj.is_temp()) {
	continue;
      }
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
  vector<hobject_t> *ls,
  vector<ghobject_t> *gen_obs)
{
  assert(ls);
  vector<ghobject_t> objects;
  int r = store->collection_list(
    ch,
    ghobject_t(start, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    ghobject_t(end, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    parent->sort_bitwise(),
    INT_MAX,
    &objects,
    NULL);
  ls->reserve(objects.size());
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    if (i->is_pgmeta() || i->hobj.is_temp()) {
      continue;
    }
    if (i->is_no_gen()) {
      ls->push_back(i->hobj);
    } else if (gen_obs) {
      gen_obs->push_back(*i);
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
    ch,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    attr.c_str(),
    bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(std::move(bp));
  }
  return r;
}

int PGBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  return store->getattrs(
    ch,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    *out);
}

void PGBackend::rollback_setattrs(
  const hobject_t &hoid,
  map<string, boost::optional<bufferlist> > &old_attrs,
  ObjectStore::Transaction *t) {
  map<string, bufferlist> to_set;
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

void PGBackend::rollback_try_stash(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->remove(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  t->try_rename(
    coll,
    ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard),
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
  ObjectStore::CollectionHandle &ch,
  ObjectStore *store,
  CephContext *cct)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED: {
    return new ReplicatedBackend(l, coll, ch, store, cct);
  }
  case pg_pool_t::TYPE_ERASURE: {
    ErasureCodeInterfaceRef ec_impl;
    ErasureCodeProfile profile = curmap->get_erasure_code_profile(pool.erasure_code_profile);
    assert(profile.count("plugin"));
    stringstream ss;
    ceph::ErasureCodePluginRegistry::instance().factory(
      profile.find("plugin")->second,
      g_conf->erasure_code_dir,
      profile,
      &ec_impl,
      &ss);
    assert(ec_impl);
    return new ECBackend(
      l,
      coll,
      ch,
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
  ScrubMap &map, const vector<hobject_t> &ls, bool deep, uint32_t seed,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " scanning " << ls.size() << " objects"
           << (deep ? " deeply" : "") << dendl;
  int i = 0;
  for (vector<hobject_t>::const_iterator p = ls.begin();
       p != ls.end();
       ++p, i++) {
    handle.reset_tp_timeout();
    hobject_t poid = *p;

    struct stat st;
    int r = store->stat(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &st,
      true);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      assert(!o.negative);
      store->getattrs(
	ch,
	ghobject_t(
	  poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	o.attrs);

      // calculate the CRC32 on deep scrubs
      if (deep) {
	be_deep_scrub(*p, seed, o, handle);
      }

      dout(25) << __func__ << "  " << poid << dendl;
    } else if (r == -ENOENT) {
      dout(25) << __func__ << "  " << poid << " got " << r
	       << ", skipping" << dendl;
    } else if (r == -EIO) {
      dout(25) << __func__ << "  " << poid << " got " << r
	       << ", stat_error" << dendl;
      ScrubMap::object &o = map.objects[poid];
      o.stat_error = true;
    } else {
      derr << __func__ << " got: " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

bool PGBackend::be_compare_scrub_objects(
  pg_shard_t auth_shard,
  const ScrubMap::object &auth,
  const object_info_t& auth_oi,
  const ScrubMap::object &candidate,
  shard_info_wrapper &shard_result,
  inconsistent_obj_wrapper &obj_result,
  ostream &errorstream)
{
  enum { CLEAN, FOUND_ERROR } error = CLEAN;
  if (candidate.stat_error) {
    assert(shard_result.has_stat_error());
    error = FOUND_ERROR;
    errorstream << "candidate had a stat error";
  }
  if (candidate.read_error || candidate.ec_hash_mismatch || candidate.ec_size_mismatch) {
    error = FOUND_ERROR;
    errorstream << "candidate had a read error";
  }
  if (auth.digest_present && candidate.digest_present) {
    if (auth.digest != candidate.digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      errorstream << "data_digest 0x" << std::hex << candidate.digest
		  << " != data_digest 0x" << auth.digest << std::dec
		  << " from shard " << auth_shard;
      obj_result.set_data_digest_mismatch();
    }
  }
  if (auth.omap_digest_present && candidate.omap_digest_present) {
    if (auth.omap_digest != candidate.omap_digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      errorstream << "omap_digest 0x" << std::hex << candidate.omap_digest
		  << " != omap_digest 0x" << auth.omap_digest << std::dec
		  << " from shard " << auth_shard;
      obj_result.set_omap_digest_mismatch();
    }
  }
  if (parent->get_pool().is_replicated()) {
    if (auth_oi.is_data_digest() && candidate.digest_present) {
      if (auth_oi.data_digest != candidate.digest) {
        if (error != CLEAN)
          errorstream << ", ";
        error = FOUND_ERROR;
        errorstream << "data_digest 0x" << std::hex << candidate.digest
		    << " != data_digest 0x" << auth_oi.data_digest << std::dec
		    << " from auth oi " << auth_oi;
        shard_result.set_data_digest_mismatch_oi();
      }
    }
    if (auth_oi.is_omap_digest() && candidate.omap_digest_present) {
      if (auth_oi.omap_digest != candidate.omap_digest) {
        if (error != CLEAN)
          errorstream << ", ";
        error = FOUND_ERROR;
        errorstream << "omap_digest 0x" << std::hex << candidate.omap_digest
		    << " != omap_digest 0x" << auth_oi.omap_digest << std::dec
		    << " from auth oi " << auth_oi;
        shard_result.set_omap_digest_mismatch_oi();
      }
    }
  }
  if (candidate.stat_error)
    return error == FOUND_ERROR;
  uint64_t oi_size = be_get_ondisk_size(auth_oi.size);
  if (oi_size != candidate.size) {
    if (error != CLEAN)
      errorstream << ", ";
    error = FOUND_ERROR;
    errorstream << "size " << candidate.size
		<< " != size " << oi_size
		<< " from auth oi " << auth_oi;
    shard_result.set_size_mismatch_oi();
  }
  if (auth.size != candidate.size) {
    if (error != CLEAN)
      errorstream << ", ";
    error = FOUND_ERROR;
    errorstream << "size " << candidate.size
		<< " != size " << auth.size
		<< " from shard " << auth_shard;
    obj_result.set_size_mismatch();
  }
  for (map<string,bufferptr>::const_iterator i = auth.attrs.begin();
       i != auth.attrs.end();
       ++i) {
    if (!candidate.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      errorstream << "attr name mismatch '" << i->first << "'";
      obj_result.set_attr_name_mismatch();
    } else if (candidate.attrs.find(i->first)->second.cmp(i->second)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      errorstream << "attr value mismatch '" << i->first << "'";
      obj_result.set_attr_value_mismatch();
    }
  }
  for (map<string,bufferptr>::const_iterator i = candidate.attrs.begin();
       i != candidate.attrs.end();
       ++i) {
    if (!auth.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      errorstream << "attr name mismatch '" << i->first << "'";
      obj_result.set_attr_name_mismatch();
    }
  }
  return error == FOUND_ERROR;
}

static int dcount(const object_info_t &oi)
{
  int count = 0;
  if (oi.is_data_digest())
    count++;
  if (oi.is_omap_digest())
    count++;
  return count;
}

map<pg_shard_t, ScrubMap *>::const_iterator
  PGBackend::be_select_auth_object(
  const hobject_t &obj,
  const map<pg_shard_t,ScrubMap*> &maps,
  object_info_t *auth_oi,
  map<pg_shard_t, shard_info_wrapper> &shard_map,
  inconsistent_obj_wrapper &object_error)
{
  eversion_t auth_version;
  bufferlist auth_bl;

  map<pg_shard_t, ScrubMap *>::const_iterator auth = maps.end();
  for (map<pg_shard_t, ScrubMap *>::const_iterator j = maps.begin();
       j != maps.end();
       ++j) {
    map<hobject_t, ScrubMap::object, hobject_t::BitwiseComparator>::iterator i =
      j->second->objects.find(obj);
    if (i == j->second->objects.end()) {
      continue;
    }
    string error_string;
    auto& shard_info = shard_map[j->first];
    if (i->second.read_error) {
      shard_info.set_read_error();
      error_string += " read_error";
    }
    if (i->second.ec_hash_mismatch) {
      shard_info.set_ec_hash_mismatch();
      error_string += " ec_hash_mismatch";
    }
    if (i->second.ec_size_mismatch) {
      shard_info.set_ec_size_mismatch();
      error_string += " ec_size_mismatch";
    }

    object_info_t oi;
    bufferlist bl;
    map<string, bufferptr>::iterator k;

    if (i->second.stat_error) {
      shard_info.set_stat_error();
      error_string += " stat_error";
      // With stat_error no further checking
      // We don't need to also see a missing_object_info_attr
      goto out;
    }

    k = i->second.attrs.find(OI_ATTR);
    if (k == i->second.attrs.end()) {
      // no object info on object, probably corrupt
      shard_info.set_oi_attr_missing();
      error_string += " oi_attr_missing";
      goto out;
    }
    bl.push_back(k->second);
    try {
      bufferlist::iterator bliter = bl.begin();
      ::decode(oi, bliter);
    } catch (...) {
      // invalid object info, probably corrupt
      shard_info.set_oi_attr_corrupted();
      error_string += " oi_attr_corrupted";
      goto out;
    }

    if (auth_version != eversion_t()) {
      if (!object_error.has_object_info_inconsistency() && !(bl == auth_bl)) {
	object_error.set_object_info_inconsistency();
	error_string += " object_info_inconsistency";
      }
    }

    // Don't use this particular shard because it won't be able to repair data
    // XXX: For now we can't pick one shard for repair and another's object info
    if (i->second.read_error || i->second.ec_hash_mismatch || i->second.ec_size_mismatch)
      goto out;

    if (auth_version == eversion_t() || oi.version > auth_version ||
        (oi.version == auth_version && dcount(oi) > dcount(*auth_oi))) {
      auth = j;
      *auth_oi = oi;
      auth_version = oi.version;
      auth_bl.clear();
      auth_bl.append(bl);
    }

out:
    // Check error_string because some errors already generated messages
    if (error_string != "") {
      dout(10) << __func__ << ": error(s) osd " << j->first
	       << " for obj " << obj
	       << "," << error_string
	       << dendl;
    }
    // Keep scanning other shards
  }
  dout(10) << __func__ << ": selecting osd " << auth->first
	   << " for obj " << obj
	   << " with oi " << *auth_oi
	   << dendl;
  return auth;
}

void PGBackend::be_compare_scrubmaps(
  const map<pg_shard_t,ScrubMap*> &maps,
  bool repair,
  map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &missing,
  map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &inconsistent,
  map<hobject_t, list<pg_shard_t>, hobject_t::BitwiseComparator> &authoritative,
  map<hobject_t, pair<uint32_t,uint32_t>, hobject_t::BitwiseComparator> &missing_digest,
  int &shallow_errors, int &deep_errors,
  Scrub::Store *store,
  const spg_t& pgid,
  const vector<int> &acting,
  ostream &errorstream)
{
  map<hobject_t,ScrubMap::object, hobject_t::BitwiseComparator>::const_iterator i;
  map<pg_shard_t, ScrubMap *, hobject_t::BitwiseComparator>::const_iterator j;
  set<hobject_t, hobject_t::BitwiseComparator> master_set;
  utime_t now = ceph_clock_now(NULL);

  // Construct master set
  for (j = maps.begin(); j != maps.end(); ++j) {
    for (i = j->second->objects.begin(); i != j->second->objects.end(); ++i) {
      master_set.insert(i->first);
    }
  }

  // Check maps against master set and each other
  for (set<hobject_t, hobject_t::BitwiseComparator>::const_iterator k = master_set.begin();
       k != master_set.end();
       ++k) {
    object_info_t auth_oi;
    map<pg_shard_t, shard_info_wrapper> shard_map;

    inconsistent_obj_wrapper object_error{*k};

    map<pg_shard_t, ScrubMap *>::const_iterator auth =
      be_select_auth_object(*k, maps, &auth_oi, shard_map, object_error);

    list<pg_shard_t> auth_list;
    set<pg_shard_t> object_errors;
    if (auth == maps.end()) {
      object_error.set_version(0);
      object_error.set_auth_missing(*k, maps, shard_map, shallow_errors, deep_errors);
      if (object_error.has_deep_errors())
	++deep_errors;
      else if (object_error.has_shallow_errors())
	++shallow_errors;
      store->add_object_error(k->pool, object_error);
      errorstream << pgid.pgid << " soid " << *k
		  << ": failed to pick suitable object info\n";
      continue;
    }
    object_error.set_version(auth_oi.user_version);
    ScrubMap::object& auth_object = auth->second->objects[*k];
    set<pg_shard_t> cur_missing;
    set<pg_shard_t> cur_inconsistent;

    for (j = maps.begin(); j != maps.end(); ++j) {
      if (j == auth)
	shard_map[auth->first].selected_oi = true;
      if (j->second->objects.count(*k)) {
	shard_map[j->first].set_object(j->second->objects[*k]);
	// Compare
	stringstream ss;
	bool found = be_compare_scrub_objects(auth->first,
				   auth_object,
				   auth_oi,
				   j->second->objects[*k],
				   shard_map[j->first],
				   object_error,
				   ss);
	// Some errors might have already been set in be_select_auth_object()
	if (shard_map[j->first].errors != 0) {
	  cur_inconsistent.insert(j->first);
          if (shard_map[j->first].has_deep_errors())
	    ++deep_errors;
	  else
	    ++shallow_errors;
	  // Only true if be_compare_scrub_objects() found errors and put something
	  // in ss.
	  if (found)
	    errorstream << pgid << " shard " << j->first << ": soid " << *k
		      << " " << ss.str() << "\n";
	} else if (found) {
	  // Track possible shard to use as authoritative, if needed
	  // There are errors, without identifying the shard
	  object_errors.insert(j->first);
	} else {
	  // XXX: The auth shard might get here that we don't know
	  // that it has the "correct" data.
	  auth_list.push_back(j->first);
	}
      } else {
	cur_missing.insert(j->first);
	shard_map[j->first].set_missing();
	// Can't have any other errors if there is no information available
	++shallow_errors;
	errorstream << pgid << " shard " << j->first << " missing " << *k
		    << "\n";
      }
      object_error.add_shard(j->first, shard_map[j->first]);
    }

    if (auth_list.empty()) {
      if (object_errors.empty()) {
        errorstream << pgid.pgid << " soid " << *k
		  << ": failed to pick suitable auth object\n";
        goto out;
      }
      // Object errors exist and we haven't found an authortative shard
      // Prefer the primary shard otherwise take first from list.
      pg_shard_t auth_shard;
      if (object_errors.count(get_parent()->whoami_shard())) {
	auth_shard = get_parent()->whoami_shard();
      } else {
	auth_shard = *(object_errors.begin());
      }
      auth_list.push_back(auth_shard);
      object_errors.erase(auth_shard);
    }
    // At this point auth_list is populated, so we add the object errors shards
    // as inconsistent.
    cur_inconsistent.insert(object_errors.begin(), object_errors.end());
    if (!cur_missing.empty()) {
      missing[*k] = cur_missing;
    }
    if (!cur_inconsistent.empty()) {
      inconsistent[*k] = cur_inconsistent;
    }
    if (!cur_inconsistent.empty() || !cur_missing.empty()) {
      authoritative[*k] = auth_list;
    } else if (parent->get_pool().is_replicated()) {
      enum {
	NO = 0,
	MAYBE = 1,
	FORCE = 2,
      } update = NO;

      if (auth_object.digest_present && auth_object.omap_digest_present &&
	  (!auth_oi.is_data_digest() || !auth_oi.is_omap_digest())) {
	dout(20) << __func__ << " missing digest on " << *k << dendl;
	update = MAYBE;
      }
      if (auth_object.digest_present && auth_object.omap_digest_present &&
	  g_conf->osd_debug_scrub_chance_rewrite_digest &&
	  (((unsigned)rand() % 100) >
	   g_conf->osd_debug_scrub_chance_rewrite_digest)) {
	dout(20) << __func__ << " randomly updating digest on " << *k << dendl;
	update = MAYBE;
      }

      // recorded digest != actual digest?
      if (auth_oi.is_data_digest() && auth_object.digest_present &&
	  auth_oi.data_digest != auth_object.digest) {
        assert(shard_map[auth->first].has_data_digest_mismatch_oi());
	errorstream << pgid << " recorded data digest 0x"
		    << std::hex << auth_oi.data_digest << " != on disk 0x"
		    << auth_object.digest << std::dec << " on " << auth_oi.soid
		    << "\n";
	if (repair)
	  update = FORCE;
      }
      if (auth_oi.is_omap_digest() && auth_object.omap_digest_present &&
	  auth_oi.omap_digest != auth_object.omap_digest) {
        assert(shard_map[auth->first].has_omap_digest_mismatch_oi());
	errorstream << pgid << " recorded omap digest 0x"
		    << std::hex << auth_oi.omap_digest << " != on disk 0x"
		    << auth_object.omap_digest << std::dec
		    << " on " << auth_oi.soid << "\n";
	if (repair)
	  update = FORCE;
      }

      if (update != NO) {
	utime_t age = now - auth_oi.local_mtime;
	if (update == FORCE ||
	    age > g_conf->osd_deep_scrub_update_digest_min_age) {
	  dout(20) << __func__ << " will update digest on " << *k << dendl;
	  missing_digest[*k] = make_pair(auth_object.digest,
					 auth_object.omap_digest);
	} else {
	  dout(20) << __func__ << " missing digest but age " << age
		   << " < " << g_conf->osd_deep_scrub_update_digest_min_age
		   << " on " << *k << dendl;
	}
      }
    }
out:
    if (object_error.has_deep_errors())
      ++deep_errors;
    else if (object_error.has_shallow_errors())
      ++shallow_errors;
    if (object_error.errors || object_error.union_shards.errors) {
      store->add_object_error(k->pool, object_error);
    }
  }
}
