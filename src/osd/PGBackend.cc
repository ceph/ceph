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

#include "PGBackend.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/scrub_types.h"
#include "include/random.h" // for ceph::util::generate_random_number()
#include "ReplicatedBackend.h"
#include "osd/scrubber/ScrubStore.h"
#include "ECBackend.h"
#include "ECSwitch.h"
#include "OSD.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "OSDMap.h"
#include "PGLog.h"
#include "common/LogClient.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::ErasureCodeProfile;
using ceph::ErasureCodeInterfaceRef;

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
  auto m = op->get_req<MOSDPGRecoveryDelete>();
  ceph_assert(m->get_type() == MSG_OSD_PG_RECOVERY_DELETE);
  dout(20) << __func__ << " " << *op->get_req() << dendl;

  op->mark_started();

  C_GatherBuilder gather(cct);
  for (const auto &p : m->objects) {
    get_parent()->remove_missing_object(p.first, p.second, gather.new_sub());
  }

  auto reply = make_message<MOSDPGRecoveryDeleteReply>();
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = spg_t(get_parent()->get_info().pgid.pgid, m->from.shard);
  reply->map_epoch = m->map_epoch;
  reply->min_epoch = m->min_epoch;
  reply->objects = m->objects;
  ConnectionRef conn = m->get_connection();

  gather.set_finisher(new LambdaContext(
    [=, this](int r) {
      if (r != -EAGAIN) {
	get_parent()->send_message_osd_cluster(reply, conn.get());
      }
    }));
  gather.activate();
}

void PGBackend::handle_recovery_delete_reply(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGRecoveryDeleteReply>();
  ceph_assert(m->get_type() == MSG_OSD_PG_RECOVERY_DELETE_REPLY);
  dout(20) << __func__ << " " << *op->get_req() << dendl;

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
    const pg_log_entry_t &entry;
    ObjectStore::Transaction t;
    RollbackVisitor(
      const hobject_t &hoid,
      PGBackend *pg,
      const pg_log_entry_t &entry) : hoid(hoid), pg(pg), entry(entry) {}
    void append(uint64_t old_size) override {
      ObjectStore::Transaction temp;
      auto dpp = pg->get_parent()->get_dpp();
      const uint64_t shard_size = pg->object_size_to_shard_size(old_size,
		       pg->get_parent()->whoami_shard().shard);
      ldpp_dout(dpp, 20) << " entry " << entry.version
			 << " rollback append object_size " << old_size
			 << " shard_size " << shard_size << dendl;
      pg->rollback_append(hoid, shard_size, &temp);
      temp.append(t);
      temp.swap(t);
    }
    void setattrs(map<string, std::optional<bufferlist> > &attrs) override {
      auto dpp = pg->get_parent()->get_dpp();
      const pg_pool_t &pool = pg->get_parent()->get_pool();
      if (pool.is_nonprimary_shard(pg->get_parent()->whoami_shard().shard)) {
        if (entry.is_written_shard(pg->get_parent()->whoami_shard().shard)) {
	  // Written shard - only rollback OI attr
	  ldpp_dout(dpp, 20) << " entry " << entry.version
			     << " written shard OI attr rollback "
			     << pg->get_parent()->whoami_shard().shard
			     << dendl;
	  ObjectStore::Transaction temp;
	  pg->rollback_setattrs(hoid, attrs, &temp, true);
	  temp.append(t);
	  temp.swap(t);
	} else {
	  // Unwritten shard - nothing to rollback
	  ldpp_dout(dpp, 20) << " entry " << entry.version
			     << " unwritten shard skipping attr rollback "
			     << pg->get_parent()->whoami_shard().shard
			     << dendl;
	}
      } else {
	// Primary shard - rollback all attrs
	ldpp_dout(dpp, 20) << " entry " << entry.version
			   << " primary_shard attr rollback "
			   << pg->get_parent()->whoami_shard().shard
			   << dendl;
	ObjectStore::Transaction temp;
	pg->rollback_setattrs(hoid, attrs, &temp, false);
	temp.append(t);
	temp.swap(t);
      }
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
      const version_t gen,
      const std::vector<std::pair<uint64_t, uint64_t>> &extents,
      const uint64_t object_size,
      const std::vector<shard_id_set> &shards) override {
      ObjectStore::Transaction temp;
      const pg_pool_t& pool = pg->get_parent()->get_pool();
      ceph_assert(entry.written_shards.empty() ||
		  pool.allows_ecoptimizations());
      auto dpp = pg->get_parent()->get_dpp();
      bool donework = false;
      ceph_assert(shards.empty() || shards.size() == extents.size());
      for (unsigned int i = 0; i < extents.size(); i++) {
        if (shards.empty() ||
	    shards[i].empty() ||
	    shards[i].contains(pg->get_parent()->whoami_shard().shard)) {
	  // Written shard - rollback extents
	  const uint64_t shard_size = pg->object_size_to_shard_size(
					object_size,
					pg->get_parent()->whoami_shard().shard);
	  ldpp_dout(dpp, 20) << " entry " << entry.version
			     << " written shard rollback_extents "
			     << entry.written_shards
			     << " shards "
			     << (shards.empty() ? shard_id_set() : shards[i])
			     << " " << pg->get_parent()->whoami_shard().shard
			     << " " << object_size
			     << " " << shard_size
			     << dendl;
	  pg->rollback_extents(gen, extents[i].first, extents[i].second,
			       hoid, shard_size, &temp);
	  donework = true;
	} else {
	  // Unwritten shard - nothing to rollback
	  ldpp_dout(dpp, 20) << " entry " << entry.version
			     << " unwritten shard skipping rollback_extents "
			     << entry.written_shards
			     << " " << pg->get_parent()->whoami_shard().shard
			     << dendl;
	}
      }
      if (donework) {
	t.remove(
	  pg->coll,
	  ghobject_t(hoid, gen, pg->get_parent()->whoami_shard().shard));
	temp.append(t);
	temp.swap(t);
      }
    }
  };

  ceph_assert(entry.mod_desc.can_rollback());
  RollbackVisitor vis(entry.soid, this, entry);
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
    const version_t gen,
    const std::vector<std::pair<uint64_t, uint64_t>> &extents,
    const uint64_t object_size,
    const std::vector<shard_id_set> &shards) override {
    auto dpp = pg->get_parent()->get_dpp();
    ceph_assert(shards.empty() || shards.size() == extents.size());
    for (unsigned int i = 0; i < extents.size(); i++) {
      if (shards.empty() ||
	  shards[i].empty() ||
	  shards[i].contains(pg->get_parent()->whoami_shard().shard)) {
        ldpp_dout(dpp, 30) << __func__ << " trim " << shards << " "
			   << pg->get_parent()->whoami_shard().shard << dendl;
        pg->trim_rollback_object(
          soid,
          gen,
          t);
	break;
      } else {
	ldpp_dout(dpp, 20) << __func__ << " skipping trim " << shards << " "
			   << pg->get_parent()->whoami_shard().shard << dendl;
      }
    }
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

void PGBackend::partial_write(
   pg_info_t *info,
   eversion_t previous_version,
   const pg_log_entry_t &entry)
{
  ceph_assert(info != nullptr);
  if (entry.written_shards.empty() && info->partial_writes_last_complete.empty()) {
    return;
  }
  auto dpp = get_parent()->get_dpp();
  ldpp_dout(dpp, 20) << __func__ << " version=" << entry.version
		     << " written_shards=" << entry.written_shards
		     << " present_shards=" << entry.present_shards
		     << " pwlc=" << info->partial_writes_last_complete
		     << " previous_version=" << previous_version
		     << dendl;
  const pg_pool_t &pool = get_parent()->get_pool();
  for (shard_id_t shard : pool.nonprimary_shards) {
    auto pwlc_iter = info->partial_writes_last_complete.find(shard);
    if (!entry.is_written_shard(shard)) {
      if (pwlc_iter == info->partial_writes_last_complete.end()) {
	// 1st partial write since all logs were updated
	info->partial_writes_last_complete[shard] =
	  std::pair(previous_version, entry.version);

	continue;
      }
      auto &&[old_v,  new_v] = pwlc_iter->second;
      if (old_v == new_v) {
	old_v = previous_version;
	new_v = entry.version;
      } else if (new_v == previous_version) {
	// Subsequent partial write, contiguous versions
	new_v = entry.version;
      } else {
	// Subsequent partial write, discontiguous versions
	ldpp_dout(dpp, 20) << __func__ << " cannot update shard " << shard
			   << dendl;
      }
    } else if (pwlc_iter != info->partial_writes_last_complete.end()) {
      auto &&[old_v,  new_v] = pwlc_iter->second;
      // Log updated or shard absent, partial write entry is a no-op
      // FIXME: In a later commit (or later PR) we should look at other ways of
      //        actually clearing the PWLC once all shards have seen the update.
      old_v = new_v = entry.version;
    }
  }
  ldpp_dout(dpp, 20) << __func__ << " after pwlc="
		     << info->partial_writes_last_complete << dendl;
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
    if (HAVE_FEATURE(parent->min_upacting_features(),
                     OSD_FIXED_COLLECTION_LIST)) {
      r = store->collection_list(
        ch,
        _next,
        ghobject_t::get_max(),
        max - ls->size(),
        &objects,
        &_next);
    } else {
      r = store->collection_list_legacy(
        ch,
        _next,
        ghobject_t::get_max(),
        max - ls->size(),
        &objects,
        &_next);
    }
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
  int r;
  if (HAVE_FEATURE(parent->min_upacting_features(),
                   OSD_FIXED_COLLECTION_LIST)) {
    r = store->collection_list(
      ch,
      ghobject_t(start, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      ghobject_t(end, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      INT_MAX,
      &objects,
      NULL);
  } else {
    r = store->collection_list_legacy(
      ch,
      ghobject_t(start, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      ghobject_t(end, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      INT_MAX,
      &objects,
      NULL);
  }
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
  map<string, bufferlist, less<>> *out)
{
  return store->getattrs(
    ch,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    *out);
}

void PGBackend::rollback_setattrs(
  const hobject_t &hoid,
  map<string, std::optional<bufferlist> > &old_attrs,
  ObjectStore::Transaction *t,
  bool only_oi) {
  map<string, bufferlist, less<>> to_set;
  ceph_assert(!hoid.is_temp());
  for (map<string, std::optional<bufferlist> >::iterator i = old_attrs.begin();
       i != old_attrs.end();
       ++i) {
    if (i->second) {
      to_set[i->first] = *(i->second);
    } else if (!only_oi) {
      t->rmattr(
	coll,
	ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	i->first);
    }
  }
  if (only_oi) {
    t->setattr(
      coll,
      ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      OI_ATTR,
      to_set[OI_ATTR]);
  } else {
    t->setattrs(
      coll,
      ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      to_set);
  }
}

void PGBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_shard_size,
  ObjectStore::Transaction *t) {
  ceph_assert(!hoid.is_temp());
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    old_shard_size);
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
  dout(20) << __func__ << " " << hoid << " " << old_version << dendl;
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
  const uint64_t offset,
  uint64_t length,
  const hobject_t &hoid,
  const uint64_t shard_size,
  ObjectStore::Transaction *t) {
  auto shard = get_parent()->whoami_shard().shard;
  if (offset >= shard_size) {
    // extent on this shard is beyond the end of the object - nothing to do
    dout(20) << __func__ << " " << hoid << " "
	     << offset << "~" << length << " is out of range "
	     << shard_size << dendl;
  } else {
    if (offset + length > shard_size) {
      dout(20) << __func__ << " " << length << " is being truncated" << dendl;
      // extent on this shard goes beyond end of the object - truncate length
      length = shard_size - offset;
    }
    dout(20) << __func__ << " " << hoid << " " << offset << "~" << length
	     << dendl;
    t->clone_range(
      coll,
      ghobject_t(hoid, gen, shard),
      ghobject_t(hoid, ghobject_t::NO_GEN, shard),
      offset,
      length,
      offset);
  }
}

void PGBackend::trim_rollback_object(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  ceph_assert(!hoid.is_temp());
  dout(20) << __func__ << " trim " << hoid << " " << old_version << dendl;
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
  CephContext *cct,
  ECExtentCache::LRU &ec_extent_cache_lru)
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
    return new ECSwitch(
      l,
      coll,
      ch,
      store,
      cct,
      ec_impl,
      pool.stripe_width,
      ec_extent_cache_lru);
  }
  default:
    ceph_abort();
    return NULL;
  }
}

int PGBackend::be_scan_list(
  const Scrub::ScrubCounterSet& io_counters,
  ScrubMap &map,
  ScrubMapBuilder &pos)
{
  dout(10) << __func__ << " " << pos << dendl;
  ceph_assert(!pos.done());
  ceph_assert(pos.pos < pos.ls.size());
  hobject_t& poid = pos.ls[pos.pos];
  auto& perf_logger = *(get_parent()->get_logger());

  int r = 0;
  ScrubMap::object &o = map.objects[poid];
  if (!pos.metadata_done) {
    perf_logger.inc(io_counters.stats_cnt);
    struct stat st;
    r = store->stat(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &st,
      true);

    if (r == 0) {
      perf_logger.inc(io_counters.getattr_cnt);
      o.size = st.st_size;
      ceph_assert(!o.negative);
      r = store->getattrs(
	ch,
	ghobject_t(
	  poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	o.attrs);
    }

    if (r == -ENOENT) {
      dout(25) << __func__ << "  " << poid << " got " << r
	       << ", removing from map" << dendl;
      map.objects.erase(poid);
    } else if (r == -EIO) {
      dout(25) << __func__ << "  " << poid << " got " << r
	       << ", stat_error" << dendl;
      o.stat_error = true;
    } else if (r != 0) {
      derr << __func__ << " got: " << cpp_strerror(r) << dendl;
      ceph_abort();
    }

    if (r != 0) {
      dout(25) << __func__ << "  " << poid << " got " << r
	       << ", skipping" << dendl;
      pos.next_object();
      return 0;
    }

    dout(25) << __func__ << "  " << poid << dendl;
    pos.metadata_done = true;
  }

  if (pos.deep) {
    r = be_deep_scrub(io_counters, poid, map, pos, o);
    if (r == -EINPROGRESS) {
      return -EINPROGRESS;
    } else if (r != 0) {
      derr << __func__ << " be_deep_scrub got: " << cpp_strerror(r) << dendl;
      ceph_abort();
    }
  }

  pos.next_object();
  return 0;
}
