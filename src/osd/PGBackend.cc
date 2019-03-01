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
#include "OSDMap.h"
#include "PGLog.h"
#include "common/LogClient.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, PGBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

void PGBackend::recover_delete_object(const hobject_t &oid, eversion_t v,
				      RecoveryHandle *h)
{
  ceph_assert(get_parent()->get_acting_recovery_backfill_shards().size() > 0);
  for (const auto& shard : get_parent()->get_acting_recovery_backfill_shards()) {
    if (shard == get_parent()->whoami_shard())
      continue;
    if (get_parent()->get_shard_missing(shard).is_missing(oid)) {
      dout(20) << __func__ << " will remove " << oid << " " << v << " from "
	       << shard << dendl;
      h->deletes[shard].push_back(make_pair(oid, v));
      get_parent()->begin_peer_recover(shard, oid);
    }
  }
}

void PGBackend::send_recovery_deletes(int prio,
				      const map<pg_shard_t, vector<pair<hobject_t, eversion_t> > > &deletes)
{
  epoch_t min_epoch = get_parent()->get_last_peering_reset_epoch();
  for (const auto& p : deletes) {
    const auto& shard = p.first;
    const auto& objects = p.second;
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      shard.osd,
      get_osdmap_epoch());
    if (!con)
      continue;
    auto it = objects.begin();
    while (it != objects.end()) {
      uint64_t cost = 0;
      uint64_t deletes = 0;
      spg_t target_pg = spg_t(get_parent()->get_info().pgid.pgid, shard.shard);
      MOSDPGRecoveryDelete *msg =
	new MOSDPGRecoveryDelete(get_parent()->whoami_shard(),
				 target_pg,
				 get_osdmap_epoch(),
				 min_epoch);
      msg->set_priority(prio);

      while (it != objects.end() &&
	     cost < cct->_conf->osd_max_push_cost &&
	     deletes < cct->_conf->osd_max_push_objects) {
	dout(20) << __func__ << ": sending recovery delete << " << it->first
		 << " " << it->second << " to osd." << shard << dendl;
	msg->objects.push_back(*it);
	cost += cct->_conf->osd_push_per_object_cost;
	++deletes;
	++it;
      }

      msg->set_cost(cost);
      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

bool PGBackend::handle_message(OpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_RECOVERY_DELETE:
    handle_recovery_delete(op);
    return true;

  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    handle_recovery_delete_reply(op);
    return true;

  default:
    break;
  }

  return _handle_message(op);
}

void PGBackend::handle_recovery_delete(OpRequestRef op)
{
  const MOSDPGRecoveryDelete *m = static_cast<const MOSDPGRecoveryDelete *>(op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_RECOVERY_DELETE);
  dout(20) << __func__ << " " << op << dendl;

  op->mark_started();

  C_GatherBuilder gather(cct);
  for (const auto &p : m->objects) {
    get_parent()->remove_missing_object(p.first, p.second, gather.new_sub());
  }

  MOSDPGRecoveryDeleteReply *reply = new MOSDPGRecoveryDeleteReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = spg_t(get_parent()->get_info().pgid.pgid, m->from.shard);
  reply->map_epoch = m->map_epoch;
  reply->min_epoch = m->min_epoch;
  reply->objects = m->objects;
  ConnectionRef conn = m->get_connection();

  gather.set_finisher(new FunctionContext(
    [=](int r) {
      if (r != -EAGAIN) {
	get_parent()->send_message_osd_cluster(reply, conn.get());
      } else {
	reply->put();
      }
    }));
  gather.activate();
}

void PGBackend::handle_recovery_delete_reply(OpRequestRef op)
{
  const MOSDPGRecoveryDeleteReply *m = static_cast<const MOSDPGRecoveryDeleteReply *>(op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_RECOVERY_DELETE_REPLY);
  dout(20) << __func__ << " " << op << dendl;

  for (const auto &p : m->objects) {
    ObjectRecoveryInfo recovery_info;
    hobject_t oid = p.first;
    recovery_info.version = p.second;
    get_parent()->on_peer_recover(m->from, oid, recovery_info);
    bool peers_recovered = true;
    for (const auto& shard : get_parent()->get_acting_recovery_backfill_shards()) {
      if (shard == get_parent()->whoami_shard())
	continue;
      if (get_parent()->get_shard_missing(shard).is_missing(oid)) {
	dout(20) << __func__ << " " << oid << " still missing on at least "
		 << shard << dendl;
	peers_recovered = false;
	break;
      }
    }
    if (peers_recovered && !get_parent()->get_local_missing().is_missing(oid)) {
      dout(20) << __func__ << " completed recovery, local_missing = "
	       << get_parent()->get_local_missing() << dendl;
      object_stat_sum_t stat_diff;
      stat_diff.num_objects_recovered = 1;
      get_parent()->on_global_recover(p.first, stat_diff, true);
    }
  }
}

void PGBackend::rollback(
  const pg_log_entry_t &entry,
  ObjectStore::Transaction *t)
{

  struct RollbackVisitor : public ObjectModDesc::Visitor {
    const hobject_t &hoid;
    PGBackend *pg;
    ObjectStore::Transaction t;
    RollbackVisitor(
      const hobject_t &hoid,
      PGBackend *pg) : hoid(hoid), pg(pg) {}
    void append(uint64_t old_size) override {
      ObjectStore::Transaction temp;
      pg->rollback_append(hoid, old_size, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void setattrs(map<string, boost::optional<bufferlist> > &attrs) override {
      ObjectStore::Transaction temp;
      pg->rollback_setattrs(hoid, attrs, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void rmobject(version_t old_version) override {
      ObjectStore::Transaction temp;
      pg->rollback_stash(hoid, old_version, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void try_rmobject(version_t old_version) override {
      ObjectStore::Transaction temp;
      pg->rollback_try_stash(hoid, old_version, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void create() override {
      ObjectStore::Transaction temp;
      pg->rollback_create(hoid, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void update_snaps(const set<snapid_t> &snaps) override {
      ObjectStore::Transaction temp;
      pg->get_parent()->pgb_set_object_snap_mapping(hoid, snaps, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void rollback_extents(
      version_t gen,
      const vector<pair<uint64_t, uint64_t> > &extents) override {
      ObjectStore::Transaction temp;
      pg->rollback_extents(gen, extents, hoid, &temp);
      temp.append(t);
      temp.swap(t);
    }
  };

  ceph_assert(entry.mod_desc.can_rollback());
  RollbackVisitor vis(entry.soid, this);
  entry.mod_desc.visit(&vis);
  t->append(vis.t);
}

struct Trimmer : public ObjectModDesc::Visitor {
  const hobject_t &soid;
  PGBackend *pg;
  ObjectStore::Transaction *t;
  Trimmer(
    const hobject_t &soid,
    PGBackend *pg,
    ObjectStore::Transaction *t)
    : soid(soid), pg(pg), t(t) {}
  void rmobject(version_t old_version) override {
    pg->trim_rollback_object(
      soid,
      old_version,
      t);
  }
  // try_rmobject defaults to rmobject
  void rollback_extents(
    version_t gen,
    const vector<pair<uint64_t, uint64_t> > &extents) override {
    pg->trim_rollback_object(
      soid,
      gen,
      t);
  }
};

void PGBackend::rollforward(
  const pg_log_entry_t &entry,
  ObjectStore::Transaction *t)
{
  auto dpp = get_parent()->get_dpp();
  ldpp_dout(dpp, 20) << __func__ << ": entry=" << entry << dendl;
  if (!entry.can_rollback())
    return;
  Trimmer trimmer(entry.soid, this, t);
  entry.mod_desc.visit(&trimmer);
}

void PGBackend::trim(
  const pg_log_entry_t &entry,
  ObjectStore::Transaction *t)
{
  if (!entry.can_rollback())
    return;
  Trimmer trimmer(entry.soid, this, t);
  entry.mod_desc.visit(&trimmer);
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

void PGBackend::remove(
  const hobject_t &hoid,
  ObjectStore::Transaction *t) {
  ceph_assert(!hoid.is_temp());
  t->remove(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  get_parent()->pgb_clear_object_snap_mapping(hoid, t);
}

void PGBackend::on_change_cleanup(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t>::iterator i = temp_contents.begin();
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
  ceph_assert(ls);
  // Starts with the smallest generation to make sure the result list
  // has the marker object (it might have multiple generations
  // though, which would be filtered).
  ghobject_t _next;
  if (!begin.is_min())
    _next = ghobject_t(begin, 0, get_parent()->whoami_shard().shard);
  ls->reserve(max);
  int r = 0;

  if (min > max)
    min = max;

  while (!_next.is_max() && ls->size() < (unsigned)min) {
    vector<ghobject_t> objects;
    r = store->collection_list(
      ch,
      _next,
      ghobject_t::get_max(),
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
  vector<hobject_t> *ls,
  vector<ghobject_t> *gen_obs)
{
  ceph_assert(ls);
  vector<ghobject_t> objects;
  int r = store->collection_list(
    ch,
    ghobject_t(start, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    ghobject_t(end, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
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
  ceph_assert(!hoid.is_temp());
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
  ceph_assert(!hoid.is_temp());
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    old_size);
}

void PGBackend::rollback_stash(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  ceph_assert(!hoid.is_temp());
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
  ceph_assert(!hoid.is_temp());
  t->remove(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  t->try_rename(
    coll,
    ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard),
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::rollback_extents(
  version_t gen,
  const vector<pair<uint64_t, uint64_t> > &extents,
  const hobject_t &hoid,
  ObjectStore::Transaction *t) {
  auto shard = get_parent()->whoami_shard().shard;
  for (auto &&extent: extents) {
    t->clone_range(
      coll,
      ghobject_t(hoid, gen, shard),
      ghobject_t(hoid, ghobject_t::NO_GEN, shard),
      extent.first,
      extent.second,
      extent.first);
  }
  t->remove(
    coll,
    ghobject_t(hoid, gen, shard));
}

void PGBackend::trim_rollback_object(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  ceph_assert(!hoid.is_temp());
  t->remove(
    coll, ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard));
}

PGBackend *PGBackend::build_pg_backend(
  const pg_pool_t &pool,
  const map<string,string>& profile,
  Listener *l,
  coll_t coll,
  ObjectStore::CollectionHandle &ch,
  ObjectStore *store,
  CephContext *cct)
{
  ErasureCodeProfile ec_profile = profile;
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED: {
    return new ReplicatedBackend(l, coll, ch, store, cct);
  }
  case pg_pool_t::TYPE_ERASURE: {
    ErasureCodeInterfaceRef ec_impl;
    stringstream ss;
    ceph::ErasureCodePluginRegistry::instance().factory(
      profile.find("plugin")->second,
      cct->_conf.get_val<std::string>("erasure_code_dir"),
      ec_profile,
      &ec_impl,
      &ss);
    ceph_assert(ec_impl);
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
    ceph_abort();
    return NULL;
  }
}

int PGBackend::be_scan_list(
  ScrubMap &map,
  ScrubMapBuilder &pos)
{
  dout(10) << __func__ << " " << pos << dendl;
  ceph_assert(!pos.done());
  ceph_assert(pos.pos < pos.ls.size());
  hobject_t& poid = pos.ls[pos.pos];

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
    ceph_assert(!o.negative);
    store->getattrs(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      o.attrs);

    if (pos.deep) {
      r = be_deep_scrub(poid, map, pos, o);
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
    ceph_abort();
  }
  if (r == -EINPROGRESS) {
    return -EINPROGRESS;
  }
  pos.next_object();
  return 0;
}

bool PGBackend::be_compare_scrub_objects(
  pg_shard_t auth_shard,
  const ScrubMap::object &auth,
  const object_info_t& auth_oi,
  const ScrubMap::object &candidate,
  shard_info_wrapper &shard_result,
  inconsistent_obj_wrapper &obj_result,
  ostream &errorstream,
  bool has_snapset)
{
  enum { CLEAN, FOUND_ERROR } error = CLEAN;
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
        shard_result.set_data_digest_mismatch_info();
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
        shard_result.set_omap_digest_mismatch_info();
      }
    }
  }
  if (candidate.stat_error)
    return error == FOUND_ERROR;
  if (!shard_result.has_info_missing()
      && !shard_result.has_info_corrupted()) {
    bufferlist can_bl, auth_bl;
    auto can_attr = candidate.attrs.find(OI_ATTR);
    auto auth_attr = auth.attrs.find(OI_ATTR);

    ceph_assert(auth_attr != auth.attrs.end());
    ceph_assert(can_attr != candidate.attrs.end());

    can_bl.push_back(can_attr->second);
    auth_bl.push_back(auth_attr->second);
    if (!can_bl.contents_equal(auth_bl)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = FOUND_ERROR;
      obj_result.set_object_info_inconsistency();
      errorstream << "object info inconsistent ";
    }
  }
  if (has_snapset) {
    if (!shard_result.has_snapset_missing()
        && !shard_result.has_snapset_corrupted()) {
      bufferlist can_bl, auth_bl;
      auto can_attr = candidate.attrs.find(SS_ATTR);
      auto auth_attr = auth.attrs.find(SS_ATTR);

      ceph_assert(auth_attr != auth.attrs.end());
      ceph_assert(can_attr != candidate.attrs.end());

      can_bl.push_back(can_attr->second);
      auth_bl.push_back(auth_attr->second);
      if (!can_bl.contents_equal(auth_bl)) {
	  if (error != CLEAN)
	    errorstream << ", ";
	  error = FOUND_ERROR;
	  obj_result.set_snapset_inconsistency();
	  errorstream << "snapset inconsistent ";
      }
    }
  }
  if (parent->get_pool().is_erasure()) {
    if (!shard_result.has_hinfo_missing()
        && !shard_result.has_hinfo_corrupted()) {
      bufferlist can_bl, auth_bl;
      auto can_hi = candidate.attrs.find(ECUtil::get_hinfo_key());
      auto auth_hi = auth.attrs.find(ECUtil::get_hinfo_key());

      ceph_assert(auth_hi != auth.attrs.end());
      ceph_assert(can_hi != candidate.attrs.end());

      can_bl.push_back(can_hi->second);
      auth_bl.push_back(auth_hi->second);
      if (!can_bl.contents_equal(auth_bl)) {
        if (error != CLEAN)
	  errorstream << ", ";
	error = FOUND_ERROR;
	obj_result.set_hinfo_inconsistency();
	errorstream << "hinfo inconsistent ";
      }
    }
  }
  uint64_t oi_size = be_get_ondisk_size(auth_oi.size);
  if (oi_size != candidate.size) {
    if (error != CLEAN)
      errorstream << ", ";
    error = FOUND_ERROR;
    errorstream << "size " << candidate.size
		<< " != size " << oi_size
		<< " from auth oi " << auth_oi;
    shard_result.set_size_mismatch_info();
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
    // We check system keys seperately
    if (i->first == OI_ATTR || i->first[0] != '_')
      continue;
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
    // We check system keys seperately
    if (i->first == OI_ATTR || i->first[0] != '_')
      continue;
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
  bool &digest_match,
  spg_t pgid,
  ostream &errorstream)
{
  eversion_t auth_version;

  // Create list of shards with primary first so it will be auth copy all
  // other things being equal.
  list<pg_shard_t> shards;
  for (map<pg_shard_t, ScrubMap *>::const_iterator j = maps.begin();
       j != maps.end();
       ++j) {
    if (j->first == get_parent()->whoami_shard())
      continue;
    shards.push_back(j->first);
  }
  shards.push_front(get_parent()->whoami_shard());

  map<pg_shard_t, ScrubMap *>::const_iterator auth = maps.end();
  digest_match = true;
  for (auto &l : shards) {
    ostringstream shard_errorstream;
    bool error = false;
    map<pg_shard_t, ScrubMap *>::const_iterator j = maps.find(l);
    map<hobject_t, ScrubMap::object>::iterator i =
      j->second->objects.find(obj);
    if (i == j->second->objects.end()) {
      continue;
    }
    auto& shard_info = shard_map[j->first];
    if (j->first == get_parent()->whoami_shard())
      shard_info.primary = true;
    if (i->second.read_error) {
      shard_info.set_read_error();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had a read error";
    }
    if (i->second.ec_hash_mismatch) {
      shard_info.set_ec_hash_mismatch();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had an ec hash mismatch";
    }
    if (i->second.ec_size_mismatch) {
      shard_info.set_ec_size_mismatch();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had an ec size mismatch";
    }

    object_info_t oi;
    bufferlist bl;
    map<string, bufferptr>::iterator k;
    SnapSet ss;
    bufferlist ss_bl, hk_bl;

    if (i->second.stat_error) {
      shard_info.set_stat_error();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had a stat error";
      // With stat_error no further checking
      // We don't need to also see a missing_object_info_attr
      goto out;
    }

    // We won't pick an auth copy if the snapset is missing or won't decode.
    ceph_assert(!obj.is_snapdir());
    if (obj.is_head()) {
      k = i->second.attrs.find(SS_ATTR);
      if (k == i->second.attrs.end()) {
	shard_info.set_snapset_missing();
        if (error)
          shard_errorstream << ", ";
        error = true;
        shard_errorstream << "candidate had a missing snapset key";
      } else {
        ss_bl.push_back(k->second);
        try {
	  auto bliter = ss_bl.cbegin();
	  decode(ss, bliter);
        } catch (...) {
	  // invalid snapset, probably corrupt
	  shard_info.set_snapset_corrupted();
          if (error)
            shard_errorstream << ", ";
          error = true;
          shard_errorstream << "candidate had a corrupt snapset";
        }
      }
    }

    if (parent->get_pool().is_erasure()) {
      ECUtil::HashInfo hi;
      k = i->second.attrs.find(ECUtil::get_hinfo_key());
      if (k == i->second.attrs.end()) {
	shard_info.set_hinfo_missing();
        if (error)
          shard_errorstream << ", ";
        error = true;
        shard_errorstream << "candidate had a missing hinfo key";
      } else {
	hk_bl.push_back(k->second);
        try {
	  auto bliter = hk_bl.cbegin();
	  decode(hi, bliter);
        } catch (...) {
	  // invalid snapset, probably corrupt
	  shard_info.set_hinfo_corrupted();
          if (error)
            shard_errorstream << ", ";
          error = true;
          shard_errorstream << "candidate had a corrupt hinfo";
        }
      }
    }

    k = i->second.attrs.find(OI_ATTR);
    if (k == i->second.attrs.end()) {
      // no object info on object, probably corrupt
      shard_info.set_info_missing();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had a missing info key";
      goto out;
    }
    bl.push_back(k->second);
    try {
      auto bliter = bl.cbegin();
      decode(oi, bliter);
    } catch (...) {
      // invalid object info, probably corrupt
      shard_info.set_info_corrupted();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate had a corrupt info";
      goto out;
    }

    // This is automatically corrected in PG::_repair_oinfo_oid()
    ceph_assert(oi.soid == obj);

    if (i->second.size != be_get_ondisk_size(oi.size)) {
      shard_info.set_obj_size_info_mismatch();
      if (error)
        shard_errorstream << ", ";
      error = true;
      shard_errorstream << "candidate size " << i->second.size << " info size "
			<< oi.size << " mismatch";
    }

    // digest_match will only be true if computed digests are the same
    if (auth_version != eversion_t()
        && auth->second->objects[obj].digest_present
        && i->second.digest_present
        && auth->second->objects[obj].digest != i->second.digest) {
      digest_match = false;
      dout(10) << __func__ << " digest_match = false, " << obj << " data_digest 0x" << std::hex << i->second.digest
		    << " != data_digest 0x" << auth->second->objects[obj].digest << std::dec
		    << dendl;
    }

    // Don't use this particular shard due to previous errors
    // XXX: For now we can't pick one shard for repair and another's object info or snapset
    if (shard_info.errors)
      goto out;

    if (auth_version == eversion_t() || oi.version > auth_version ||
        (oi.version == auth_version && dcount(oi) > dcount(*auth_oi))) {
      auth = j;
      *auth_oi = oi;
      auth_version = oi.version;
    }

out:
    if (error)
        errorstream << pgid.pgid << " shard " << l << " soid " << obj
		    << " : " << shard_errorstream.str() << "\n";
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
  const set<hobject_t> &master_set,
  bool repair,
  map<hobject_t, set<pg_shard_t>> &missing,
  map<hobject_t, set<pg_shard_t>> &inconsistent,
  map<hobject_t, list<pg_shard_t>> &authoritative,
  map<hobject_t, pair<boost::optional<uint32_t>,
                      boost::optional<uint32_t>>> &missing_digest,
  int &shallow_errors, int &deep_errors,
  Scrub::Store *store,
  const spg_t& pgid,
  const vector<int> &acting,
  ostream &errorstream)
{
  utime_t now = ceph_clock_now();

  // Check maps against master set and each other
  for (set<hobject_t>::const_iterator k = master_set.begin();
       k != master_set.end();
       ++k) {
    object_info_t auth_oi;
    map<pg_shard_t, shard_info_wrapper> shard_map;

    inconsistent_obj_wrapper object_error{*k};

    bool digest_match;
    map<pg_shard_t, ScrubMap *>::const_iterator auth =
      be_select_auth_object(*k, maps, &auth_oi, shard_map, digest_match,
			    pgid, errorstream);

    list<pg_shard_t> auth_list;
    set<pg_shard_t> object_errors;
    if (auth == maps.end()) {
      object_error.set_version(0);
      object_error.set_auth_missing(*k, maps, shard_map, shallow_errors,
	deep_errors, get_parent()->whoami_shard());
      if (object_error.has_deep_errors())
	++deep_errors;
      else if (object_error.has_shallow_errors())
	++shallow_errors;
      store->add_object_error(k->pool, object_error);
      errorstream << pgid.pgid << " soid " << *k
		  << " : failed to pick suitable object info\n";
      continue;
    }
    object_error.set_version(auth_oi.user_version);
    ScrubMap::object& auth_object = auth->second->objects[*k];
    set<pg_shard_t> cur_missing;
    set<pg_shard_t> cur_inconsistent;
    bool fix_digest = false;

    for (auto j = maps.cbegin(); j != maps.cend(); ++j) {
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
				   ss,
				   k->has_snapset());

	dout(20) << __func__ << (repair ? " repair " : " ") << (parent->get_pool().is_replicated() ? "replicated " : "")
	 << (j == auth ? "auth" : "") << "shards " << shard_map.size() << (digest_match ? " digest_match " : " ")
	 << (shard_map[j->first].only_data_digest_mismatch_info() ? "'info mismatch info'" : "")
	 << dendl;
	// If all replicas match, but they don't match object_info we can
	// repair it by using missing_digest mechanism
	if (repair && parent->get_pool().is_replicated() && j == auth && shard_map.size() > 1
	    && digest_match && shard_map[j->first].only_data_digest_mismatch_info()
	    && auth_object.digest_present) {
	  // Set in missing_digests
	  fix_digest = true;
	  // Clear the error
	  shard_map[j->first].clear_data_digest_mismatch_info();
	  errorstream << pgid << " soid " << *k << " : repairing object info data_digest" << "\n";
	}
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
	    errorstream << pgid << " shard " << j->first << " soid " << *k
		      << " : " << ss.str() << "\n";
	} else if (found) {
	  // Track possible shard to use as authoritative, if needed
	  // There are errors, without identifying the shard
	  object_errors.insert(j->first);
	  errorstream << pgid << " soid " << *k << " : " << ss.str() << "\n";
	} else {
	  // XXX: The auth shard might get here that we don't know
	  // that it has the "correct" data.
	  auth_list.push_back(j->first);
	}
      } else {
	cur_missing.insert(j->first);
	shard_map[j->first].set_missing();
        shard_map[j->first].primary = (j->first == get_parent()->whoami_shard());
	// Can't have any other errors if there is no information available
	++shallow_errors;
	errorstream << pgid << " shard " << j->first << " " << *k << " : missing\n";
      }
      object_error.add_shard(j->first, shard_map[j->first]);
    }

    if (auth_list.empty()) {
      if (object_errors.empty()) {
        errorstream << pgid.pgid << " soid " << *k
		  << " : failed to pick suitable auth object\n";
        goto out;
      }
      // Object errors exist and nothing in auth_list
      // Prefer the auth shard otherwise take first from list.
      pg_shard_t shard;
      if (object_errors.count(auth->first)) {
	shard = auth->first;
      } else {
	shard = *(object_errors.begin());
      }
      auth_list.push_back(shard);
      object_errors.erase(shard);
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

    if (fix_digest) {
      boost::optional<uint32_t> data_digest, omap_digest;
      ceph_assert(auth_object.digest_present);
      data_digest = auth_object.digest;
      if (auth_object.omap_digest_present) {
        omap_digest = auth_object.omap_digest;
      }
      missing_digest[*k] = make_pair(data_digest, omap_digest);
    }
    if (!cur_inconsistent.empty() || !cur_missing.empty()) {
      authoritative[*k] = auth_list;
    } else if (!fix_digest && parent->get_pool().is_replicated()) {
      enum {
	NO = 0,
	MAYBE = 1,
	FORCE = 2,
      } update = NO;

      if (auth_object.digest_present && !auth_oi.is_data_digest()) {
	dout(20) << __func__ << " missing data digest on " << *k << dendl;
	update = MAYBE;
      }
      if (auth_object.omap_digest_present && !auth_oi.is_omap_digest()) {
	dout(20) << __func__ << " missing omap digest on " << *k << dendl;
	update = MAYBE;
      }

      // recorded digest != actual digest?
      if (auth_oi.is_data_digest() && auth_object.digest_present &&
	  auth_oi.data_digest != auth_object.digest) {
        ceph_assert(shard_map[auth->first].has_data_digest_mismatch_info());
	errorstream << pgid << " recorded data digest 0x"
		    << std::hex << auth_oi.data_digest << " != on disk 0x"
		    << auth_object.digest << std::dec << " on " << auth_oi.soid
		    << "\n";
	if (repair)
	  update = FORCE;
      }
      if (auth_oi.is_omap_digest() && auth_object.omap_digest_present &&
	  auth_oi.omap_digest != auth_object.omap_digest) {
        ceph_assert(shard_map[auth->first].has_omap_digest_mismatch_info());
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
	    age > cct->_conf->osd_deep_scrub_update_digest_min_age) {
          boost::optional<uint32_t> data_digest, omap_digest;
          if (auth_object.digest_present) {
            data_digest = auth_object.digest;
	    dout(20) << __func__ << " will update data digest on " << *k << dendl;
          }
          if (auth_object.omap_digest_present) {
            omap_digest = auth_object.omap_digest;
	    dout(20) << __func__ << " will update omap digest on " << *k << dendl;
          }
	  missing_digest[*k] = make_pair(data_digest, omap_digest);
	} else {
	  dout(20) << __func__ << " missing digest but age " << age
		   << " < " << cct->_conf->osd_deep_scrub_update_digest_min_age
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

void PGBackend::be_omap_checks(const map<pg_shard_t,ScrubMap*> &maps,
  const set<hobject_t> &master_set,
  omap_stat_t& omap_stats,
  ostream &warnstream) const
{
  bool needs_omap_check = false;
  for (const auto& map : maps) {
    if (map.second->has_large_omap_object_errors || map.second->has_omap_keys) {
      needs_omap_check = true;
      break;
    }
  }

  if (!needs_omap_check) {
    return; // Nothing to do
  }

  // Iterate through objects and update omap stats
  for (const auto& k : master_set) {
    for (const auto& map : maps) {
      auto it = map.second->objects.find(k);
      if (it == map.second->objects.end())
        continue;
      ScrubMap::object& obj = it->second;
      omap_stats.omap_bytes += obj.object_omap_bytes;
      omap_stats.omap_keys += obj.object_omap_keys;
      if (obj.large_omap_object_found) {
        omap_stats.large_omap_objects++;
        warnstream << "Large omap object found. Object: " << k << " Key count: "
                   << obj.large_omap_object_key_count << " Size (bytes): "
                   << obj.large_omap_object_value_size << '\n';
        break;
      }
    }
  }
}
