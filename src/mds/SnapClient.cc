// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "MDSContext.h"
#include "MDSRank.h"
#include "msg/Messenger.h"
#include "messages/MMDSTableRequest.h"
#include "SnapClient.h"

#include "common/config.h"
#include "common/debug.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".snapclient "

using namespace std;

void SnapClient::resend_queries()
{
  if (!waiting_for_version.empty() || !waiting_for_commit.empty() ||
      (!synced && sync_reqid > 0)) {
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

void SnapClient::handle_query_result(const cref_t<MMDSTableRequest> &m)
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
      const auto prepared = get_prepared_ops();

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

      note_unnotified_commits(prepared);
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

  kick_commit_waiters();

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

void SnapClient::handle_notify_prep(const cref_t<MMDSTableRequest> &m)
{
  dout(10) << __func__ << " " << *m << dendl;
  handle_query_result(m);
  auto ack = make_message<MMDSTableRequest>(table, TABLESERVER_OP_NOTIFY_ACK, 0, m->get_tid());
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

  unnotified_commits.erase(tid);
  kick_commit_waiters();
}

void SnapClient::prepare_create(inodeno_t dirino, std::string_view name, utime_t stamp,
				version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
  bufferlist bl;
  __u32 op = TABLE_OP_CREATE;
  encode(op, bl);
  encode(dirino, bl);
  encode(name, bl);
  encode(stamp, bl);
  _prepare(bl, pstid, pbl, onfinish);
}

void SnapClient::prepare_create_realm(inodeno_t ino, version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
  bufferlist bl;
  __u32 op = TABLE_OP_CREATE;
  encode(op, bl);
  encode(ino, bl);
  _prepare(bl, pstid, pbl, onfinish);
}

void SnapClient::prepare_destroy(inodeno_t ino, snapid_t snapid, version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
  bufferlist bl;
  __u32 op = TABLE_OP_DESTROY;
  encode(op, bl);
  encode(ino, bl);
  encode(snapid, bl);
  _prepare(bl, pstid, pbl, onfinish);
}

void SnapClient::prepare_update(inodeno_t ino, snapid_t snapid, std::string_view name, utime_t stamp,
				version_t *pstid, MDSContext *onfinish) {
  bufferlist bl;
  __u32 op = TABLE_OP_UPDATE;
  encode(op, bl);
  encode(ino, bl);
  encode(snapid, bl);
  encode(name, bl);
  encode(stamp, bl);
  _prepare(bl, pstid, NULL, onfinish);
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
  auto req = make_message<MMDSTableRequest>(table, TABLESERVER_OP_QUERY, ++last_reqid, 0);
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

bool SnapClient::is_pending(version_t tid) const
{
  if (unnotified_commits.count(tid)) {
    return true;
  }
  if (committing_tids.count(tid)) {
    return false;
  }
  return cached_pending_update.count(tid) || cached_pending_destroy.count(tid);
}

version_t SnapClient::get_pending_tid(snapid_t first, snapid_t last) const
{
  version_t tid = 0;
  snapid_t tid_seq = 0;

  /*
   * Do not filter by get_last_seq(): snapids are allocated at prepare, but
   * ops from different ranks can commit in any order, so a snap committed
   * here may have a larger seq than one that is still pending.
   */
  auto consider = [&](version_t t, snapid_t s) {
    if (s < first || s > last || !is_pending(t)) {
      return;
    }
    if (!tid || s < tid_seq) {
      tid = t;
      tid_seq = s;
    }
  };
  for (const auto& [t, info] : cached_pending_update) {
    // a rename (TABLE_OP_UPDATE) carries an existing snapid
    if (cached_snaps.count(info.snapid)) {
      continue;
    }
    consider(t, info.snapid);
  }
  for (const auto& [t, p] : cached_pending_destroy) {
    consider(t, p.second);
  }
  for (const auto& [t, s] : unnotified_commits) {
    consider(t, s);
  }
  return tid;
}

void SnapClient::wait_for_commit(version_t tid, MDSContext *c)
{
  ceph_assert(is_pending(tid));
  auto& waiters = waiting_for_commit[tid];
  if (waiters.empty()) {
    /*
     * The commit normally arrives with MMDSSnapUpdate. A prepare that was
     * rolled back is never committed and stays in the cache until the next
     * refresh, so ask for one. A refresh that shows the op committed does
     * not release the waiters (see note_unnotified_commits()).
     */
    dout(10) << __func__ << " tid " << tid << ", refreshing" << dendl;
    refresh(cached_version, nullptr);
  }
  waiters.push_back(c);
}

void SnapClient::kick_commit_waiters()
{
  MDSContext::vec finished;
  for (auto it = waiting_for_commit.begin(); it != waiting_for_commit.end(); ) {
    if (is_pending(it->first)) {
      ++it;
      continue;
    }
    dout(10) << __func__ << " tid " << it->first << dendl;
    finished.insert(finished.end(), it->second.begin(), it->second.end());
    it = waiting_for_commit.erase(it);
  }
  if (!finished.empty()) {
    mds->queue_waiters(finished);
  }
}

/*
 * creates/destroys in the cache that this rank has not been notified of
 * committing yet
 */
std::vector<SnapClient::prepared_op_t> SnapClient::get_prepared_ops() const
{
  std::vector<prepared_op_t> ops;
  for (const auto& [tid, info] : cached_pending_update) {
    // a rename (TABLE_OP_UPDATE) carries an existing snapid
    if (!committing_tids.count(tid) && !cached_snaps.count(info.snapid)) {
      ops.push_back({tid, info.snapid, info.snapid, false});
    }
  }
  for (const auto& [tid, p] : cached_pending_destroy) {
    if (!committing_tids.count(tid)) {
      ops.push_back({tid, p.first, p.second, true});
    }
  }
  return ops;
}

/*
 * A refresh drops an op from the pending maps as soon as the snapserver
 * commits it. This rank may not have handled the MMDSSnapUpdate for it
 * yet, so its snaprealms do not have the snap, but get_last_seq() already
 * covers it. Keep such ops in unnotified_commits until notify_commit().
 *
 * An op that left the pending maps without being committed was rolled back.
 * (A create that was committed and then destroyed looks the same, which is
 * fine: the snap is gone either way.)
 */
void SnapClient::note_unnotified_commits(const std::vector<prepared_op_t>& ops)
{
  for (const auto& op : ops) {
    if (cached_pending_update.count(op.tid) ||
	cached_pending_destroy.count(op.tid)) {
      continue;
    }
    const bool have_snap = cached_snaps.count(op.snapid);
    const bool committed = op.destroy ? !have_snap : have_snap;
    dout(10) << __func__ << " tid " << op.tid
	     << (op.destroy ? " destroy " : " create ") << op.snapid
	     << " seq " << op.seq
	     << (committed ? " committed, not notified yet" : " rolled back")
	     << dendl;
    if (committed) {
      unnotified_commits[op.tid] = op.seq;
    }
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
