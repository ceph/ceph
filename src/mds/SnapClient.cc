// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "MDSMap.h"
#include "MDSRank.h"
#include "msg/Messenger.h"
#include "messages/MMDSTableRequest.h"
#include "SnapClient.h"

#include "common/config.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".snapclient "

void SnapClient::resend_queries()
{
  if (!waiting_for_version.empty() || (!synced && sync_reqid > 0)) {
    version_t want;
    if (!waiting_for_version.empty())
      want = std::max<version_t>(cached_version, waiting_for_version.rbegin()->first);
    else
      want = std::max<version_t>(cached_version, 1);
    refresh(want, NULL);
    if (!synced)
      sync_reqid = last_reqid;
  }
}

void SnapClient::handle_query_result(const MMDSTableRequest::const_ref &m)
{
  dout(10) << __func__ << " " << *m << dendl;

  char type;
  using ceph::decode;
  auto p = m->bl.cbegin();
  decode(type, p);

  switch (type) {
  case 'U': // uptodate
    ceph_assert(cached_version == m->get_tid());
    break;
  case 'F': // full
    {
      decode(cached_snaps, p);
      decode(cached_pending_update, p);
      decode(cached_pending_destroy, p);

      snapid_t last_created, last_destroyed;
      decode(last_created, p);
      decode(last_destroyed, p);

      if (last_created > cached_last_created)
	cached_last_created = last_created;
      if (last_destroyed > cached_last_destroyed)
	cached_last_destroyed = last_destroyed;

      cached_version = m->get_tid();
    }
    break;
  default:
    ceph_abort();
  };

  if (!committing_tids.empty()) {
    for (auto p = committing_tids.begin();
	 p != committing_tids.end() && *p <= cached_version; ) {
      if (cached_pending_update.count(*p)) {
	if (cached_pending_update[*p].snapid > cached_last_created)
	  cached_last_created = cached_pending_update[*p].snapid;
	++p;
      } else if (cached_pending_destroy.count(*p)) {
	if (cached_pending_destroy[*p].second > cached_last_destroyed)
	  cached_last_destroyed = cached_pending_destroy[*p].second;
	++p;
      } else {
	// pending update/destroy have been committed.
	committing_tids.erase(p++);
      }
    }
  }

  if (m->op == TABLESERVER_OP_QUERY_REPLY && m->reqid >= sync_reqid)
    synced = true;

  if (synced && !waiting_for_version.empty()) {
    MDSContext::vec finished;
    while (!waiting_for_version.empty()) {
      auto it = waiting_for_version.begin();
      if (it->first > cached_version)
	break;
      auto& v = it->second;
      finished.insert(finished.end(), v.begin(), v.end());
      waiting_for_version.erase(it);
    }
    if (!finished.empty())
      mds->queue_waiters(finished);
  }
}

void SnapClient::handle_notify_prep(const MMDSTableRequest::const_ref &m)
{
  dout(10) << __func__ << " " << *m << dendl;
  handle_query_result(m);
  auto ack = MMDSTableRequest::create(table, TABLESERVER_OP_NOTIFY_ACK, 0, m->get_tid());
  mds->send_message(ack, m->get_connection());
}

void SnapClient::notify_commit(version_t tid)
{
  dout(10) << __func__ << " tid " << tid << dendl;

  ceph_assert(cached_version == 0 || cached_version >= tid);
  if (cached_version == 0) {
    committing_tids.insert(tid);
  } else if (cached_pending_update.count(tid)) {
    committing_tids.insert(tid);
    if (cached_pending_update[tid].snapid > cached_last_created)
      cached_last_created = cached_pending_update[tid].snapid;
  } else if (cached_pending_destroy.count(tid)) {
    committing_tids.insert(tid);
    if (cached_pending_destroy[tid].second > cached_last_destroyed)
      cached_last_destroyed = cached_pending_destroy[tid].second;
  } else if (cached_version > tid) {
    // no need to record the tid if it has already been committed.
  } else {
    ceph_abort();
  }
}

void SnapClient::refresh(version_t want, MDSContext *onfinish)
{
  dout(10) << __func__ << " want " << want << dendl;

  ceph_assert(want >= cached_version);
  if (onfinish)
    waiting_for_version[want].push_back(onfinish);

  if (!server_ready)
    return;

  mds_rank_t ts = mds->mdsmap->get_tableserver();
  auto req = MMDSTableRequest::create(table, TABLESERVER_OP_QUERY, ++last_reqid, 0);
  using ceph::encode;
  char op = 'F';
  encode(op, req->bl);
  encode(cached_version, req->bl);
  mds->send_message_mds(req, ts);
}

void SnapClient::sync(MDSContext *onfinish)
{
  dout(10) << __func__ << dendl;

  refresh(std::max<version_t>(cached_version, 1), onfinish);
  synced = false;
  if (server_ready)
    sync_reqid = last_reqid;
  else
    sync_reqid = (last_reqid == ~0ULL) ? 1 : last_reqid + 1;
}

void SnapClient::get_snaps(set<snapid_t>& result) const
{
  ceph_assert(cached_version > 0);
  for (auto& p : cached_snaps)
    result.insert(p.first);

  for (auto tid : committing_tids) {
    auto q = cached_pending_update.find(tid);
    if (q != cached_pending_update.end())
      result.insert(q->second.snapid);

    auto r = cached_pending_destroy.find(tid);
    if (r != cached_pending_destroy.end())
      result.erase(r->second.first);
  }
}

set<snapid_t> SnapClient::filter(const set<snapid_t>& snaps) const
{
  ceph_assert(cached_version > 0);
  if (snaps.empty())
    return snaps;

  set<snapid_t> result;

  for (auto p : snaps) {
    if (cached_snaps.count(p))
      result.insert(p);
  }

  for (auto tid : committing_tids) {
    auto q = cached_pending_update.find(tid);
    if (q != cached_pending_update.end()) {
      if (snaps.count(q->second.snapid))
	result.insert(q->second.snapid);
    }

    auto r = cached_pending_destroy.find(tid);
    if (r != cached_pending_destroy.end())
      result.erase(r->second.first);
  }

  dout(10) << __func__ << " " << snaps << " -> " << result <<  dendl;
  return result;
}

const SnapInfo* SnapClient::get_snap_info(snapid_t snapid) const
{
  ceph_assert(cached_version > 0);

  const SnapInfo* result = NULL;
  auto it = cached_snaps.find(snapid);
  if (it != cached_snaps.end())
    result = &it->second;

  for (auto tid : committing_tids) {
    auto q = cached_pending_update.find(tid);
    if (q != cached_pending_update.end() && q->second.snapid == snapid) {
      result = &q->second;
      break;
    }

    auto r = cached_pending_destroy.find(tid);
    if (r != cached_pending_destroy.end() && r->second.first == snapid) {
      result = NULL;
      break;
    }
  }

  dout(10) << __func__ << " snapid " << snapid << " -> " << result <<  dendl;
  return result;
}

void SnapClient::get_snap_infos(map<snapid_t, const SnapInfo*>& infomap,
			        const set<snapid_t>& snaps) const
{
  ceph_assert(cached_version > 0);

  if (snaps.empty())
    return;

  map<snapid_t, const SnapInfo*> result;
  for (auto p : snaps) {
    auto it = cached_snaps.find(p);
    if (it != cached_snaps.end())
      result[p] = &it->second;
  }

  for (auto tid : committing_tids) {
    auto q = cached_pending_update.find(tid);
    if (q != cached_pending_update.end()) {
      if (snaps.count(q->second.snapid))
	result[q->second.snapid] = &q->second;
    }

    auto r = cached_pending_destroy.find(tid);
    if (r != cached_pending_destroy.end())
      result.erase(r->second.first);
  }

  infomap.insert(result.begin(), result.end());
}

int SnapClient::dump_cache(Formatter *f) const
{
  if (!is_synced()) {
    dout(5) << "dump_cache: not synced" << dendl;
    return -EINVAL;
  }

  map<snapid_t, const SnapInfo*> snaps;
  for (auto& p : cached_snaps)
    snaps[p.first] = &p.second;

  for (auto tid : committing_tids) {
    auto q = cached_pending_update.find(tid);
    if (q != cached_pending_update.end())
      snaps[q->second.snapid] = &q->second;

    auto r = cached_pending_destroy.find(tid);
    if (r != cached_pending_destroy.end())
      snaps.erase(r->second.first);
  }

  f->open_object_section("snapclient");

  f->dump_int("last_created", get_last_created());
  f->dump_int("last_destroyed", get_last_destroyed());

  f->open_array_section("snaps");
  for (auto p : snaps) {
    f->open_object_section("snap");
    p.second->dump(f);
    f->close_section();
  }
  f->close_section();

  f->close_section();

  return 0;
}
