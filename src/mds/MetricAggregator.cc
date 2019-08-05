// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

#include "MDSRank.h"
#include "MetricAggregator.h"
#include "mgr/MgrClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.metric.aggregator" << " " << __func__

MetricAggregator::MetricAggregator(CephContext *cct, MDSRank *mds, MgrClient *mgrc)
  : Dispatcher(cct),
    mds(mds),
    mgrc(mgrc),
    mds_pinger(mds) {
}

void MetricAggregator::ping_all_active_ranks() {
  dout(10) << ": pinging " << active_rank_addrs.size() << " active mds(s)" << dendl;

  for (const auto &[rank, addr] : active_rank_addrs) {
    dout(20) << ": pinging rank=" << rank << " addr=" << addr << dendl;
    mds_pinger.send_ping(rank, addr);
  }
}

int MetricAggregator::init() {
  dout(10) << dendl;

  pinger = std::thread([this]() {
      std::unique_lock locker(lock);
      while (!stopping) {
        ping_all_active_ranks();
        locker.unlock();
        double timo = g_conf().get_val<std::chrono::seconds>("mds_ping_interval").count();
        sleep(timo);
        locker.lock();
      }
    });

  return 0;
}

void MetricAggregator::shutdown() {
  dout(10) << dendl;

  {
    std::scoped_lock locker(lock);
    ceph_assert(!stopping);
    stopping = true;
  }

  if (pinger.joinable()) {
    pinger.join();
  }
}

bool MetricAggregator::ms_can_fast_dispatch2(const cref_t<Message> &m) const {
  return m->get_type() == MSG_MDS_METRICS;
}

void MetricAggregator::ms_fast_dispatch2(const ref_t<Message> &m) {
  bool handled = ms_dispatch2(m);
  ceph_assert(handled);
}

bool MetricAggregator::ms_dispatch2(const ref_t<Message> &m) {
  if (m->get_type() == MSG_MDS_METRICS) {
    if (m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MDS) {
      handle_mds_metrics(ref_cast<MMDSMetrics>(m));
    }
    return true;
  }
  return false;
}

void MetricAggregator::refresh_metrics_for_rank(const entity_inst_t &client,
                                                mds_rank_t rank, const Metrics &metrics) {
  dout(20) << ": client=" << client << ", rank=" << rank << ", metrics="
           << metrics << dendl;

  auto &p = clients_by_rank.at(rank);
  bool ins = p.insert(client).second;
  if (ins) {
    dout(20) << ": rank=" << rank << " has " << p.size() << " connected"
             << " client(s)" << dendl;
  }

  auto update_counter_func = [&metrics](const MDSPerformanceCounterDescriptor &d,
                                        PerformanceCounter *c) {
    ceph_assert(d.is_supported());

    dout(20) << ": performance_counter_descriptor=" << d << dendl;

    switch (d.type) {
    case MDSPerformanceCounterType::CAP_HIT_METRIC:
      c->first = metrics.cap_hit_metric.hits;
      c->second = metrics.cap_hit_metric.misses;
      break;
    case MDSPerformanceCounterType::READ_LATENCY_METRIC:
      c->first = metrics.read_latency_metric.lat.tv.tv_sec;
      c->second = metrics.read_latency_metric.lat.tv.tv_nsec;
      break;
    case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
      c->first = metrics.write_latency_metric.lat.tv.tv_sec;
      c->second = metrics.write_latency_metric.lat.tv.tv_nsec;
      break;
    case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
      c->first = metrics.metadata_latency_metric.lat.tv.tv_sec;
      c->second = metrics.metadata_latency_metric.lat.tv.tv_nsec;
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }
  };

  auto sub_key_func = [client, rank](const MDSPerfMetricSubKeyDescriptor &d,
                                     MDSPerfMetricSubKey *sub_key) {
    ceph_assert(d.is_supported());

    dout(20) << ": sub_key_descriptor=" << d << dendl;

    std::string match_string;
    switch (d.type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
      match_string = stringify(rank);
      break;
    case MDSPerfMetricSubKeyType::CLIENT_ID:
      match_string = stringify(client);
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }

    dout(20) << ": match_string=" << match_string << dendl;

    std::smatch match;
    if (!std::regex_search(match_string, match, d.regex)) {
      return false;
    }
    if (match.size() <= 1) {
      return false;
    }
    for (size_t i = 1; i < match.size(); ++i) {
      sub_key->push_back(match[i].str());
    }
    return true;
  };

  for (auto& [query, perf_key_map] : query_metrics_map) {
    MDSPerfMetricKey key;
    if (query.get_key(sub_key_func, &key)) {
      query.update_counters(update_counter_func, &perf_key_map[key]);
    }
  }
}

void MetricAggregator::remove_metrics_for_rank(const entity_inst_t &client,
                                               mds_rank_t rank, bool remove) {
  dout(20) << ": client=" << client << ", rank=" << rank << dendl;

  if (remove) {
    auto &p = clients_by_rank.at(rank);
    bool rm = p.erase(client) != 0;
    ceph_assert(rm);
    dout(20) << ": rank=" << rank << " has " << p.size() << " connected"
             << " client(s)" << dendl;
  }

  auto sub_key_func = [client, rank](const MDSPerfMetricSubKeyDescriptor &d,
                                     MDSPerfMetricSubKey *sub_key) {
    ceph_assert(d.is_supported());
    dout(20) << ": sub_key_descriptor=" << d << dendl;

    std::string match_string;
    switch (d.type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
      match_string = stringify(rank);
      break;
    case MDSPerfMetricSubKeyType::CLIENT_ID:
      match_string = stringify(client);
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }

    dout(20) << ": match_string=" << match_string << dendl;

    std::smatch match;
    if (!std::regex_search(match_string, match, d.regex)) {
      return false;
    }
    if (match.size() <= 1) {
      return false;
    }
    for (size_t i = 1; i < match.size(); ++i) {
      sub_key->push_back(match[i].str());
    }
    return true;
  };

  for (auto& [query, perf_key_map] : query_metrics_map) {
    MDSPerfMetricKey key;
    if (query.get_key(sub_key_func, &key)) {
      if (perf_key_map.erase(key)) {
        dout(10) << ": removed metric for key=" << key << dendl;
      }
    }
  }
}

void MetricAggregator::handle_mds_metrics(const cref_t<MMDSMetrics> &m) {
  const metrics_message_t &metrics_message = m->metrics_message;

  auto seq = metrics_message.seq;
  auto rank = metrics_message.rank;
  auto &client_metrics_map = metrics_message.client_metrics_map;

  dout(20) << ": applying " << client_metrics_map.size() << " updates for rank="
           << rank << " with sequence number " << seq << dendl;

  std::scoped_lock locker(lock);
  if (!mds_pinger.pong_received(rank, seq)) {
    return;
  }

  for (auto& [client, metrics] : client_metrics_map) {
    switch (metrics.update_type) {
    case UpdateType::UPDATE_TYPE_REFRESH:
      refresh_metrics_for_rank(client, rank, metrics);
      break;
    case UpdateType::UPDATE_TYPE_REMOVE:
      remove_metrics_for_rank(client, rank, true);
      break;
    default:
      ceph_abort();
    }
  }
}

void MetricAggregator::cull_metrics_for_rank(mds_rank_t rank) {
  dout(20) << ": rank=" << rank << dendl;

  auto &p = clients_by_rank.at(rank);
  for (auto &client : p) {
    remove_metrics_for_rank(client, rank, false);
  }

  dout(10) << ": culled " << p.size() << " clients" << dendl;
  clients_by_rank.erase(rank);
}

void MetricAggregator::notify_mdsmap(const MDSMap &mdsmap) {
  dout(10) << dendl;

  std::scoped_lock locker(lock);
  std::set<mds_rank_t> current_active;
  mdsmap.get_active_mds_set(current_active);

  std::set<mds_rank_t> active_set;
  boost::copy(active_rank_addrs | boost::adaptors::map_keys,
              std::inserter(active_set, active_set.begin()));

  std::set<mds_rank_t> diff;
  std::set_difference(active_set.begin(), active_set.end(),
                      current_active.begin(), current_active.end(),
                      std::inserter(diff, diff.end()));

  for (auto &rank : diff) {
    dout(10) << ": mds rank=" << rank << " removed from mdsmap" << dendl;
    active_rank_addrs.erase(rank);
    cull_metrics_for_rank(rank);
    mds_pinger.reset_ping(rank);
  }

  diff.clear();
  std::set_difference(current_active.begin(), current_active.end(),
                      active_set.begin(), active_set.end(),
                      std::inserter(diff, diff.end()));

  for (auto &rank : diff) {
    auto rank_addr = mdsmap.get_addrs(rank);
    dout(10) << ": active rank=" << rank << " has addr=" << rank_addr << dendl;
    active_rank_addrs.emplace(rank, rank_addr);
    clients_by_rank.emplace(rank, std::unordered_set<entity_inst_t>{});
  }

  dout(10) << ": active set=["  << active_rank_addrs << "]" << dendl;
}
