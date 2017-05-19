// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MgrStatMonitor.h"
#include "mon/PGMap.h"
#include "messages/MMonMgrReport.h"

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

  const pool_stat_t& get_pg_sum() const override { return digest.pg_sum; }
  const osd_stat_t& get_osd_sum() const override { return digest.osd_sum; }

  const osd_stat_t *get_osd_stat(int osd) const override {
    auto i = digest.osd_stat.find(osd);
    if (i == digest.osd_stat.end()) {
      return nullptr;
    }
    return &i->second;
  }
  const mempool::pgmap::unordered_map<int32_t,osd_stat_t> &get_osd_stat() const override {
    return digest.osd_stat;
  }

  size_t get_num_pg_by_osd(int osd) const override {
    return digest.get_num_pg_by_osd(osd);
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
  version = 0;
}

void MgrStatMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  bufferlist bl;
  get_version(version, bl);
  if (version) {
    assert(bl.length());
    auto p = bl.begin();
    bufferlist digestbl;
    ::decode(digestbl, p);
    auto q = digestbl.begin();
    ::decode(digest, q);
    ::decode(health_summary, p);
    ::decode(health_detail, p);
  }
}

void MgrStatMonitor::create_pending()
{
  pending_digest = digest;
  pending_health_summary = health_summary;
  pending_health_detail = health_detail;
}

void MgrStatMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  dout(10) << __func__ << " " << version << dendl;
  bufferlist digestbl, bl;
  ::encode(pending_digest, digestbl, mon->get_quorum_con_features());
  ::encode(digestbl, bl);
  ::encode(pending_health_summary, bl);
  ::encode(pending_health_detail, bl);
  put_version(t, version, bl);
  put_last_committed(t, version);
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
  case MSG_MON_MGR_REPORT:
    return preprocess_report(op);
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
