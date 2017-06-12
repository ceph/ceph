// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MgrStatMonitor.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMap.h"
#include "mon/PGMonitor.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MMonMgrReport.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

class MgrPGStatService : public PGStatService {
  PGMapDigest& digest;
public:
  MgrPGStatService(PGMapDigest& d) : digest(d) {}

  const pool_stat_t* get_pool_stat(int poolid) const override {
    auto i = digest.pg_pool_sum.find(poolid);
    if (i != digest.pg_pool_sum.end()) {
      return &i->second;
    }
    return nullptr;
  }

  ceph_statfs get_statfs() const override {
    return digest.get_statfs();
  }

  void print_summary(Formatter *f, ostream *out) const override {
    digest.print_summary(f, out);
  }
  void dump_info(Formatter *f) const override {
    digest.dump(f);
  }
  void dump_fs_stats(stringstream *ss,
		     Formatter *f,
		     bool verbose) const override {
    digest.dump_fs_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
		       bool verbose) const override {
    digest.dump_pool_stats_full(osdm, ss, f, verbose);
  }
};


#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon)
static ostream& _prefix(std::ostream *_dout, Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mgrstat ";
}

MgrStatMonitor::MgrStatMonitor(Monitor *mn, Paxos *p, const string& service_name)
  : PaxosService(mn, p, service_name),
    pgservice(new MgrPGStatService(digest))
{
}

MgrStatMonitor::~MgrStatMonitor() = default;

PGStatService *MgrStatMonitor::get_pg_stat_service()
{
  return pgservice.get();
}

void MgrStatMonitor::create_initial()
{
  dout(10) << dendl;
  version = 0;
}

void MgrStatMonitor::update_from_paxos(bool *need_bootstrap)
{
  version = get_last_committed();
  dout(10) << " " << version << dendl;
  bufferlist bl;
  get_version(version, bl);
  if (version) {
    assert(bl.length());
    auto p = bl.begin();
    ::decode(digest, p);
    ::decode(health_summary, p);
    ::decode(health_detail, p);
  }
}

void MgrStatMonitor::create_pending()
{
  dout(10) << " " << version << dendl;
  pending_digest = digest;
  pending_health_summary = health_summary;
  pending_health_detail = health_detail;
}

void MgrStatMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  if (version < mon->pgmon()->get_last_committed()) {
    // fast-forward to pgmon version to ensure clients don't see a
    // jump back in time for MGetPoolStats and MStatFs.
    version = mon->pgmon()->get_last_committed() + 1;
  }
  dout(10) << " " << version << dendl;
  bufferlist bl;
  ::encode(pending_digest, bl, mon->get_quorum_con_features());
  ::encode(pending_health_summary, bl);
  ::encode(pending_health_detail, bl);
  put_version(t, version, bl);
  put_last_committed(t, version);
}

version_t MgrStatMonitor::get_trim_to()
{
  // we don't actually need *any* old states, but keep a few.
  if (version > 5) {
    return version - 5;
  }
  return 0;
}

void MgrStatMonitor::on_active()
{
}

void MgrStatMonitor::get_health(list<pair<health_status_t,string> >& summary,
				list<pair<health_status_t,string> > *detail,
				CephContext *cct) const
{
  summary.insert(summary.end(), health_summary.begin(), health_summary.end());
  if (detail) {
    detail->insert(detail->end(), health_detail.begin(), health_detail.end());
  }
}

void MgrStatMonitor::tick()
{
}

void MgrStatMonitor::print_summary(Formatter *f, std::ostream *ss) const
{
  pgservice->print_summary(f, ss);
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
  return false;
}

bool MgrStatMonitor::prepare_report(MonOpRequestRef op)
{
  auto m = static_cast<MMonMgrReport*>(op->get_req());
  bufferlist bl = m->get_data();
  auto p = bl.begin();
  ::decode(pending_digest, p);
  pending_health_summary.swap(m->health_summary);
  pending_health_detail.swap(m->health_detail);
  return true;
}

bool MgrStatMonitor::preprocess_getpoolstats(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  auto m = static_cast<MGetPoolStats*>(op->get_req());
  auto session = m->get_session();
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
  epoch_t ver = 0;
  if (mon->pgservice == get_pg_stat_service()) {
    ver = get_last_committed();
  } else {
    ver = mon->pgmon()->get_last_committed();
  }
  auto reply = new MGetPoolStatsReply(m->fsid, m->get_tid(), ver);
  for (const auto& pool_name : m->pools) {
    const auto pool_id = mon->osdmon()->osdmap.lookup_pg_pool_name(pool_name);
    if (pool_id == -ENOENT)
      continue;
    auto pool_stat = mon->pgservice->get_pool_stat(pool_id);
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
  auto session = statfs->get_session();
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
  epoch_t ver = 0;
  if (mon->pgservice == get_pg_stat_service()) {
    ver = get_last_committed();
  } else {
    ver = mon->pgmon()->get_last_committed();
  }
  auto reply = new MStatfsReply(statfs->fsid, statfs->get_tid(), ver);
  reply->h.st = mon->pgservice->get_statfs();
  mon->send_reply(op, reply);
  return true;
}
