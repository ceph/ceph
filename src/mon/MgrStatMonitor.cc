// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MgrStatMonitor.h"
#include "mon/PGMap.h"
#include "messages/MMonMgrReport.h"

class MgrPGStatService : public PGStatService {
  PGMapDigest digest;
public:
  void decode_digest(bufferlist& bl) {
    auto p = bl.begin();
    ::decode(digest, p);
  }
  void encode_digest(bufferlist& bl, uint64_t features) {
    ::encode(digest, bl, features);
  }

  const pool_stat_t* get_pool_stat(int poolid) const {
    auto i = digest.pg_pool_sum.find(poolid);
    if (i != digest.pg_pool_sum.end()) {
      return &i->second;
    }
    return NULL;
  }

  const pool_stat_t& get_pg_sum() const { return digest.pg_sum; }
  const osd_stat_t& get_osd_sum() const { return digest.osd_sum; }

  const osd_stat_t *get_osd_stat(int osd) const {
    auto i = digest.osd_stat.find(osd);
    if (i == digest.osd_stat.end()) {
      return NULL;
    }
    return &i->second;
  }
  const ceph::unordered_map<int32_t,osd_stat_t> *get_osd_stat() const {
    return &digest.osd_stat;
  }

  size_t get_num_pg_by_osd(int osd) const {
    return digest.get_num_pg_by_osd(osd);
  }

  void print_summary(Formatter *f, ostream *out) const {
    digest.print_summary(f, out);
  }
  void dump_fs_stats(stringstream *ss, Formatter *f, bool verbose) const {
    digest.dump_fs_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
		       bool verbose) const {
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
    pgservice(new MgrPGStatService())
{
}

MgrStatMonitor::~MgrStatMonitor()
{
}

PGStatService *MgrStatMonitor::get_pg_stat_service()
{
  return pgservice.get();
}

void MgrStatMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  bufferlist bl;
  get_version(version, bl);
  if (version) {
    assert(bl.length());
    pgservice->decode_digest(bl);
  }
}

void MgrStatMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  ++version;
  dout(10) << __func__ << " " << version << dendl;
  bufferlist bl;
  pgservice->encode_digest(bl, mon->get_quorum_con_features());
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
}

void MgrStatMonitor::tick()
{
}

void MgrStatMonitor::print_summary(Formatter *f, std::ostream *ss) const
{
}

bool MgrStatMonitor::preprocess_query(MonOpRequestRef op)
{
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
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
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
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
  MMonMgrReport *m = static_cast<MMonMgrReport*>(op->get_req());
  pgservice->decode_digest(m->get_data());
  return true;
}
