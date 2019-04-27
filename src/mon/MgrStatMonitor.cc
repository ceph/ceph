// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MgrStatMonitor.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMap.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MMonMgrReport.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"
#include "messages/MServiceMap.h"

#include "include/ceph_assert.h"	// re-clobber assert

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon)
static ostream& _prefix(std::ostream *_dout, Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mgrstat ";
}

MgrStatMonitor::MgrStatMonitor(Monitor *mn, Paxos *p, const string& service_name)
  : PaxosService(mn, p, service_name)
{
}

MgrStatMonitor::~MgrStatMonitor() = default;

void MgrStatMonitor::create_initial()
{
  dout(10) << __func__ << dendl;
  version = 0;
  service_map.epoch = 1;
  pending_service_map_bl.clear();
  encode(service_map, pending_service_map_bl, CEPH_FEATURES_ALL);
}

void MgrStatMonitor::update_from_paxos(bool *need_bootstrap)
{
  version = get_last_committed();
  dout(10) << " " << version << dendl;
  load_health();
  bufferlist bl;
  get_version(version, bl);
  if (version) {
    ceph_assert(bl.length());
    try {
      auto p = bl.cbegin();
      decode(digest, p);
      decode(service_map, p);
      if (!p.end()) {
	decode(progress_events, p);
      }
      dout(10) << __func__ << " v" << version
	       << " service_map e" << service_map.epoch
	       << " " << progress_events.size() << " progress events"
	       << dendl;
    }
    catch (buffer::error& e) {
      derr << "failed to decode mgrstat state; luminous dev version? "
	   << e.what() << dendl;
    }
  }
  check_subs();
  update_logger();
}

void MgrStatMonitor::update_logger()
{
  dout(20) << __func__ << dendl;

  mon->cluster_logger->set(l_cluster_osd_bytes, digest.osd_sum.statfs.total);
  mon->cluster_logger->set(l_cluster_osd_bytes_used,
                           digest.osd_sum.statfs.get_used_raw());
  mon->cluster_logger->set(l_cluster_osd_bytes_avail,
                           digest.osd_sum.statfs.available);

  mon->cluster_logger->set(l_cluster_num_pool, digest.pg_pool_sum.size());
  uint64_t num_pg = 0;
  for (auto i : digest.num_pg_by_pool) {
    num_pg += i.second;
  }
  mon->cluster_logger->set(l_cluster_num_pg, num_pg);

  unsigned active = 0, active_clean = 0, peering = 0;
  for (auto p = digest.num_pg_by_state.begin();
       p != digest.num_pg_by_state.end();
       ++p) {
    if (p->first & PG_STATE_ACTIVE) {
      active += p->second;
      if (p->first & PG_STATE_CLEAN)
	active_clean += p->second;
    }
    if (p->first & PG_STATE_PEERING)
      peering += p->second;
  }
  mon->cluster_logger->set(l_cluster_num_pg_active_clean, active_clean);
  mon->cluster_logger->set(l_cluster_num_pg_active, active);
  mon->cluster_logger->set(l_cluster_num_pg_peering, peering);

  mon->cluster_logger->set(l_cluster_num_object, digest.pg_sum.stats.sum.num_objects);
  mon->cluster_logger->set(l_cluster_num_object_degraded, digest.pg_sum.stats.sum.num_objects_degraded);
  mon->cluster_logger->set(l_cluster_num_object_misplaced, digest.pg_sum.stats.sum.num_objects_misplaced);
  mon->cluster_logger->set(l_cluster_num_object_unfound, digest.pg_sum.stats.sum.num_objects_unfound);
  mon->cluster_logger->set(l_cluster_num_bytes, digest.pg_sum.stats.sum.num_bytes);

}

void MgrStatMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending_digest = digest;
  pending_health_checks = get_health_checks();
  pending_service_map_bl.clear();
  encode(service_map, pending_service_map_bl, mon->get_quorum_con_features());
}

void MgrStatMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  dout(10) << " " << version << dendl;
  bufferlist bl;
  encode(pending_digest, bl, mon->get_quorum_con_features());
  ceph_assert(pending_service_map_bl.length());
  bl.append(pending_service_map_bl);
  encode(pending_progress_events, bl);
  put_version(t, version, bl);
  put_last_committed(t, version);

  encode_health(pending_health_checks, t);
}

version_t MgrStatMonitor::get_trim_to() const
{
  // we don't actually need *any* old states, but keep a few.
  if (version > 5) {
    return version - 5;
  }
  return 0;
}

void MgrStatMonitor::on_active()
{
  update_logger();
}

void MgrStatMonitor::tick()
{
}

bool MgrStatMonitor::preprocess_query(MonOpRequestRef op)
{
  auto m = static_cast<PaxosServiceMessage*>(op->get_req());
  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    return preprocess_statfs(op);
  case MSG_MON_MGR_REPORT:
    return preprocess_report(op);
  case MSG_GETPOOLSTATS:
    return preprocess_getpoolstats(op);
  default:
    mon->no_reply(op);
    derr << "Unhandled message type " << m->get_type() << dendl;
    return true;
  }
}

bool MgrStatMonitor::prepare_update(MonOpRequestRef op)
{
  auto m = static_cast<PaxosServiceMessage*>(op->get_req());
  switch (m->get_type()) {
  case MSG_MON_MGR_REPORT:
    return prepare_report(op);
  default:
    mon->no_reply(op);
    derr << "Unhandled message type " << m->get_type() << dendl;
    return true;
  }
}

bool MgrStatMonitor::preprocess_report(MonOpRequestRef op)
{
  mon->no_reply(op);
  return false;
}

bool MgrStatMonitor::prepare_report(MonOpRequestRef op)
{
  auto m = static_cast<MMonMgrReport*>(op->get_req());
  bufferlist bl = m->get_data();
  auto p = bl.cbegin();
  decode(pending_digest, p);
  pending_health_checks.swap(m->health_checks);
  if (m->service_map_bl.length()) {
    pending_service_map_bl.swap(m->service_map_bl);
  }
  pending_progress_events.swap(m->progress_events);
  dout(10) << __func__ << " " << pending_digest << ", "
	   << pending_health_checks.checks.size() << " health checks, "
	   << progress_events.size() << " progress events" << dendl;
  dout(20) << "pending_digest:\n";
  JSONFormatter jf(true);
  jf.open_object_section("pending_digest");
  pending_digest.dump(&jf);
  jf.close_section();
  jf.flush(*_dout);
  *_dout << dendl;
  dout(20) << "health checks:\n";
  JSONFormatter jf(true);
  jf.open_object_section("health_checks");
  pending_health_checks.dump(&jf);
  jf.close_section();
  jf.flush(*_dout);
  *_dout << dendl;
  dout(20) << "progress events:\n";
  JSONFormatter jf(true);
  jf.open_object_section("progress_events");
  for (auto& i : pending_progress_events) {
    jf.dump_object(i.first.c_str(), i.second);
  }
  jf.close_section();
  jf.flush(*_dout);
  *_dout << dendl;
  return true;
}

bool MgrStatMonitor::preprocess_getpoolstats(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  auto m = static_cast<MGetPoolStats*>(op->get_req());
  auto session = op->get_session();
  if (!session)
    return true;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MGetPoolStats received from entity with insufficient caps "
            << session->caps << dendl;
    return true;
  }
  if (m->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " on fsid "
	    << m->fsid << " != " << mon->monmap->fsid << dendl;
    return true;
  }
  epoch_t ver = get_last_committed();
  auto reply = new MGetPoolStatsReply(m->fsid, m->get_tid(), ver);
  for (const auto& pool_name : m->pools) {
    const auto pool_id = mon->osdmon()->osdmap.lookup_pg_pool_name(pool_name);
    if (pool_id == -ENOENT)
      continue;
    auto pool_stat = get_pool_stat(pool_id);
    if (!pool_stat)
      continue;
    reply->pool_stats[pool_name] = *pool_stat;
  }
  mon->send_reply(op, reply);
  return true;
}

bool MgrStatMonitor::preprocess_statfs(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  auto statfs = static_cast<MStatfs*>(op->get_req());
  auto session = op->get_session();

  if (!session)
    return true;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MStatfs received from entity with insufficient privileges "
            << session->caps << dendl;
    return true;
  }
  if (statfs->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " on fsid " << statfs->fsid
            << " != " << mon->monmap->fsid << dendl;
    return true;
  }
  dout(10) << __func__ << " " << *statfs
           << " from " << statfs->get_orig_source() << dendl;
  epoch_t ver = get_last_committed();
  auto reply = new MStatfsReply(statfs->fsid, statfs->get_tid(), ver);
  reply->h.st = get_statfs(mon->osdmon()->osdmap, statfs->data_pool);
  mon->send_reply(op, reply);
  return true;
}

void MgrStatMonitor::check_sub(Subscription *sub)
{
  const auto epoch = mon->monmap->get_epoch();
  dout(10) << __func__
	   << " next " << sub->next
	   << " have " << epoch << dendl;
  if (sub->next <= service_map.epoch) {
    auto m = new MServiceMap(service_map);
    sub->session->con->send_message(m);
    if (sub->onetime) {
      mon->with_session_map([sub](MonSessionMap& session_map) {
	  session_map.remove_sub(sub);
	});
    } else {
      sub->next = epoch + 1;
    }
  }
}

void MgrStatMonitor::check_subs()
{
  dout(10) << __func__ << dendl;
  if (!service_map.epoch) {
    return;
  }
  auto subs = mon->session_map.subs.find("servicemap");
  if (subs == mon->session_map.subs.end()) {
    return;
  }
  auto p = subs->second->begin();
  while (!p.end()) {
    auto sub = *p;
    ++p;
    check_sub(sub);
  }
}
