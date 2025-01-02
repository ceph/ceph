// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "messages/MMDSMetrics.h"

#include "MDSRank.h"
#include "SessionMap.h"
#include "MetricsHandler.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": mds.metrics"

MetricsHandler::MetricsHandler(CephContext *cct, MDSRank *mds)
  : Dispatcher(cct),
    mds(mds) {
}

bool MetricsHandler::ms_can_fast_dispatch2(const cref_t<Message> &m) const {
  return m->get_type() == CEPH_MSG_CLIENT_METRICS || m->get_type() == MSG_MDS_PING;
}

void MetricsHandler::ms_fast_dispatch2(const ref_t<Message> &m) {
  bool handled = ms_dispatch2(m);
  ceph_assert(handled);
}

bool MetricsHandler::ms_dispatch2(const ref_t<Message> &m) {
  if (m->get_type() == CEPH_MSG_CLIENT_METRICS &&
      m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_CLIENT) {
    handle_client_metrics(ref_cast<MClientMetrics>(m));
    return true;
  } else if (m->get_type() == MSG_MDS_PING &&
             m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MDS) {
    const Message *msg = m.get();
    const MMDSOp *op = dynamic_cast<const MMDSOp*>(msg);
    if (!op)
      dout(0) << typeid(*msg).name() << " is not an MMDSOp type" << dendl;
    ceph_assert(op);
    handle_mds_ping(ref_cast<MMDSPing>(m));
    return true;
  }
  return false;
}

void MetricsHandler::init() {
  dout(10) << dendl;

  updater = std::thread([this]() {
      std::unique_lock locker(lock);
      while (!stopping) {
        double after = g_conf().get_val<std::chrono::seconds>("mds_metrics_update_interval").count();
        locker.unlock();
        sleep(after);
        locker.lock();
        update_rank0();
      }
    });
}

void MetricsHandler::shutdown() {
  dout(10) << dendl;

  {
    std::scoped_lock locker(lock);
    ceph_assert(!stopping);
    stopping = true;
  }

  if (updater.joinable()) {
    updater.join();
  }
}


void MetricsHandler::add_session(Session *session) {
  ceph_assert(session != nullptr);

  auto &client = session->info.inst;
  dout(10) << ": session=" << session << ", client=" << client << dendl;

  std::scoped_lock locker(lock);

  auto p = client_metrics_map.emplace(client, std::pair(last_updated_seq, Metrics())).first;
  auto &metrics = p->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  dout(20) << ": metrics=" << metrics << dendl;
}

void MetricsHandler::remove_session(Session *session) {
  ceph_assert(session != nullptr);

  auto &client = session->info.inst;
  dout(10) << ": session=" << session << ", client=" << client << dendl;

  std::scoped_lock locker(lock);

  auto it = client_metrics_map.find(client);
  if (it == client_metrics_map.end()) {
    return;
  }

  // if a session got removed before rank 0 saw at least one refresh
  // update from us or if we will send a remove type update as the
  // the first "real" update (with an incoming sequence number), then
  // cut short the update as rank 0 has not witnessed this client session
  // update this rank.
  auto lus = it->second.first;
  if (lus == last_updated_seq) {
    dout(10) << ": metric lus=" << lus << ", last_updated_seq=" << last_updated_seq
             << dendl;
    client_metrics_map.erase(it);
    return;
  }

  // zero out all metrics
  auto &metrics = it->second.second;
  metrics.cap_hit_metric = { };
  metrics.read_latency_metric = { };
  metrics.write_latency_metric = { };
  metrics.metadata_latency_metric = { };
  metrics.dentry_lease_metric = { };
  metrics.opened_files_metric = { };
  metrics.pinned_icaps_metric = { };
  metrics.opened_inodes_metric = { };
  metrics.read_io_sizes_metric = { };
  metrics.write_io_sizes_metric = { };
  metrics.update_type = UPDATE_TYPE_REMOVE;
}

void MetricsHandler::set_next_seq(version_t seq) {
  dout(20) << ": current sequence number " << next_seq << ", setting next sequence number "
           << seq << dendl;
  next_seq = seq;
}

void MetricsHandler::reset_seq() {
  dout(10) << ": last_updated_seq=" << last_updated_seq << dendl;

  set_next_seq(0);
  for (auto &[client, metrics_v] : client_metrics_map) {
    dout(10) << ": reset last updated seq for client addr=" << client << dendl;
    metrics_v.first = last_updated_seq;
  }
}

void MetricsHandler::handle_payload(Session *session, const CapInfoPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
	   << ", session=" << session << ", hits=" << payload.cap_hits << ", misses="
	   << payload.cap_misses << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.cap_hit_metric.hits = payload.cap_hits;
  metrics.cap_hit_metric.misses = payload.cap_misses;
}

void MetricsHandler::handle_payload(Session *session, const ReadLatencyPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", latency=" << payload.lat
           << ", avg=" << payload.mean << ", sq_sum=" << payload.sq_sum
           << ", count=" << payload.count << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.read_latency_metric.lat = payload.lat;
  metrics.read_latency_metric.mean = payload.mean;
  metrics.read_latency_metric.sq_sum = payload.sq_sum;
  metrics.read_latency_metric.count = payload.count;
  metrics.read_latency_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const WriteLatencyPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", latency=" << payload.lat
           << ", avg=" << payload.mean << ", sq_sum=" << payload.sq_sum
           << ", count=" << payload.count << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.write_latency_metric.lat = payload.lat;
  metrics.write_latency_metric.mean = payload.mean;
  metrics.write_latency_metric.sq_sum = payload.sq_sum;
  metrics.write_latency_metric.count = payload.count;
  metrics.write_latency_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const MetadataLatencyPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", latency=" << payload.lat
           << ", avg=" << payload.mean << ", sq_sum=" << payload.sq_sum
           << ", count=" << payload.count << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.metadata_latency_metric.lat = payload.lat;
  metrics.metadata_latency_metric.mean = payload.mean;
  metrics.metadata_latency_metric.sq_sum = payload.sq_sum;
  metrics.metadata_latency_metric.count = payload.count;
  metrics.metadata_latency_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const DentryLeasePayload &payload) {
  dout(20) << ": type=" << payload.get_type()
	   << ", session=" << session << ", hits=" << payload.dlease_hits << ", misses="
	   << payload.dlease_misses << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.dentry_lease_metric.hits = payload.dlease_hits;
  metrics.dentry_lease_metric.misses = payload.dlease_misses;
  metrics.dentry_lease_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const OpenedFilesPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", opened_files=" << payload.opened_files
           << ", total_inodes=" << payload.total_inodes << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.opened_files_metric.opened_files = payload.opened_files;
  metrics.opened_files_metric.total_inodes = payload.total_inodes;
  metrics.opened_files_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const PinnedIcapsPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", pinned_icaps=" << payload.pinned_icaps
           << ", total_inodes=" << payload.total_inodes << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.pinned_icaps_metric.pinned_icaps = payload.pinned_icaps;
  metrics.pinned_icaps_metric.total_inodes = payload.total_inodes;
  metrics.pinned_icaps_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const OpenedInodesPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", opened_inodes=" << payload.opened_inodes
           << ", total_inodes=" << payload.total_inodes << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.opened_inodes_metric.opened_inodes = payload.opened_inodes;
  metrics.opened_inodes_metric.total_inodes = payload.total_inodes;
  metrics.opened_inodes_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const ReadIoSizesPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", total_ops=" << payload.total_ops
           << ", total_size=" << payload.total_size << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.read_io_sizes_metric.total_ops = payload.total_ops;
  metrics.read_io_sizes_metric.total_size = payload.total_size;
  metrics.read_io_sizes_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const WriteIoSizesPayload &payload) {
  dout(20) << ": type=" << payload.get_type()
           << ", session=" << session << ", total_ops=" << payload.total_ops
           << ", total_size=" << payload.total_size << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.write_io_sizes_metric.total_ops = payload.total_ops;
  metrics.write_io_sizes_metric.total_size = payload.total_size;
  metrics.write_io_sizes_metric.updated = true;
}

void MetricsHandler::handle_payload(Session *session, const UnknownPayload &payload) {
  dout(5) << ": type=Unknown, session=" << session << ", ignoring unknown payload" << dendl;
}

void MetricsHandler::handle_client_metrics(const cref_t<MClientMetrics> &m) {
  if (!mds->is_active_lockless()) {
    dout(20) << ": dropping metrics message during recovery" << dendl;
    return;
  }

  std::scoped_lock locker(lock);

  Session *session = mds->get_session(m);
  dout(20) << ": session=" << session << dendl;

  if (session == nullptr) {
    dout(10) << ": ignoring session less message" << dendl;
    return;
  }

  for (auto &metric : m->updates) {
    boost::apply_visitor(HandlePayloadVisitor(this, session), metric.payload);
  }
}

void MetricsHandler::handle_mds_ping(const cref_t<MMDSPing> &m) {
  std::scoped_lock locker(lock);
  set_next_seq(m->seq);
}

void MetricsHandler::notify_mdsmap(const MDSMap &mdsmap) {
  dout(10) << dendl;

  std::set<mds_rank_t> active_set;

  std::scoped_lock locker(lock);

  // reset sequence number when rank0 is unavailable or a new
  // rank0 mds is chosen -- new rank0 will assign a starting
  // sequence number when it is ready to process metric updates.
  // this also allows to cut-short metric remove operations to
  // be satisfied locally in many cases.

  // update new rank0 address
  mdsmap.get_active_mds_set(active_set);
  if (!active_set.count((mds_rank_t)0)) {
    dout(10) << ": rank0 is unavailable" << dendl;
    addr_rank0 = boost::none;
    reset_seq();
    return;
  }

  dout(10) << ": rank0 is mds." << mdsmap.get_mds_info((mds_rank_t)0).name << dendl;

  auto new_rank0_addr = mdsmap.get_addrs((mds_rank_t)0);
  if (addr_rank0 != new_rank0_addr) {
    dout(10) << ": rank0 addr is now " << new_rank0_addr << dendl;
    addr_rank0 = new_rank0_addr;
    reset_seq();
  }
}

void MetricsHandler::update_rank0() {
  dout(20) << dendl;

  if (!addr_rank0) {
    dout(20) << ": not yet notified with rank0 address, ignoring" << dendl;
    return;
  }

  metrics_message_t metrics_message;
  auto &update_client_metrics_map = metrics_message.client_metrics_map;

  metrics_message.seq = next_seq;
  metrics_message.rank = mds->get_nodeid();

  for (auto p = client_metrics_map.begin(); p != client_metrics_map.end();) {
    // copy metrics and update local metrics map as required
    auto &metrics = p->second.second;
    update_client_metrics_map.emplace(p->first, metrics);
    if (metrics.update_type == UPDATE_TYPE_REFRESH) {
      metrics = {};
      ++p;
    } else {
      p = client_metrics_map.erase(p);
    }
  }

  // only start incrementing when its kicked via set_next_seq()
  if (next_seq != 0) {
    ++last_updated_seq;
  }

  dout(20) << ": sending metric updates for " << update_client_metrics_map.size()
           << " clients to rank 0 (address: " << *addr_rank0 << ") with sequence number "
           << next_seq << ", last updated sequence number " << last_updated_seq << dendl;

  mds->send_message_mds(make_message<MMDSMetrics>(std::move(metrics_message)), *addr_rank0);
}
