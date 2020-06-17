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
  }
  if (m->get_type() == MSG_MDS_PING &&
      m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MDS) {
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
  metrics.update_type = UPDATE_TYPE_REMOVE;
}

void MetricsHandler::set_next_seq(version_t seq) {
  dout(20) << ": current sequence number " << next_seq << ", setting next sequence number "
           << seq << dendl;
  next_seq = seq;
}

void MetricsHandler::handle_payload(Session *session, const CapInfoPayload &payload) {
  dout(20) << ": session=" << session << ", hits=" << payload.cap_hits << ", misses="
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
  dout(20) << ": session=" << session << ", latency=" << payload.lat << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.read_latency_metric.lat = payload.lat;
}

void MetricsHandler::handle_payload(Session *session, const WriteLatencyPayload &payload) {
  dout(20) << ": session=" << session << ", latency=" << payload.lat << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.write_latency_metric.lat = payload.lat;
}

void MetricsHandler::handle_payload(Session *session, const MetadataLatencyPayload &payload) {
  dout(20) << ": session=" << session << ", latency=" << payload.lat << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  if (it == client_metrics_map.end()) {
    return;
  }

  auto &metrics = it->second.second;
  metrics.update_type = UPDATE_TYPE_REFRESH;
  metrics.metadata_latency_metric.lat = payload.lat;
}

void MetricsHandler::handle_payload(Session *session, const UnknownPayload &payload) {
  dout(5) << ": session=" << session << ", ignoring unknown payload" << dendl;
}

void MetricsHandler::handle_client_metrics(const cref_t<MClientMetrics> &m) {
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
  // reset the sequence number so that last_updated_seq starts
  // updating when the new rank0 mds pings us.
  set_next_seq(0);

  // update new rank0 address
  mdsmap.get_active_mds_set(active_set);
  if (!active_set.count((mds_rank_t)0)) {
    dout(10) << ": rank0 is unavailable" << dendl;
    addr_rank0 = boost::none;
    return;
  }

  dout(10) << ": rank0 is mds." << mdsmap.get_mds_info((mds_rank_t)0).name << dendl;

  auto new_rank0_addr = mdsmap.get_addrs((mds_rank_t)0);
  if (addr_rank0 != new_rank0_addr) {
    dout(10) << ": rank0 addr is now " << new_rank0_addr << dendl;
    addr_rank0 = new_rank0_addr;
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
