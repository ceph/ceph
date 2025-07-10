// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MetricAggregator.h"
#include "MDSMap.h"
#include "MDSRank.h"
#include "mgr/MgrClient.h"

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/perf_counters_key.h"

#include "messages/MMDSMetrics.h"

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.metric.aggregator" << " " << __func__

// Performance Counters
 enum {
  l_mds_client_metrics_start = 10000,
  l_mds_client_metrics_num_clients,
  l_mds_client_metrics_last
 };

enum {
  l_mds_per_client_metrics_start = 20000,
  l_mds_per_client_metrics_cap_hits,
  l_mds_per_client_metrics_cap_misses,
  l_mds_per_client_metrics_avg_read_latency,
  l_mds_per_client_metrics_avg_write_latency,
  l_mds_per_client_metrics_avg_metadata_latency,
  l_mds_per_client_metrics_dentry_lease_hits,
  l_mds_per_client_metrics_dentry_lease_misses,
  l_mds_per_client_metrics_opened_files,
  l_mds_per_client_metrics_opened_inodes,
  l_mds_per_client_metrics_pinned_icaps,
  l_mds_per_client_metrics_total_inodes,
  l_mds_per_client_metrics_total_read_ops,
  l_mds_per_client_metrics_total_read_size,
  l_mds_per_client_metrics_total_write_ops,
  l_mds_per_client_metrics_total_write_size,
  l_mds_per_client_metrics_last
 };

enum {
  l_subvolume_metrics_first = 30000,
  l_subvolume_metrics_read_iops,
  l_subvolume_metrics_read_tp_Bps,
  l_subvolume_metrics_avg_read_latency,
  l_subvolume_metrics_write_iops,
  l_subvolume_metrics_write_tp_Bps,
  l_subvolume_metrics_avg_write_latency,
  l_subvolume_metrics_last_window_end,
  l_subvolume_metrics_last_window,
  l_subvolume_metrics_last
};

MetricAggregator::MetricAggregator(CephContext *cct, MDSRank *mds, MgrClient *mgrc)
  : Dispatcher(cct),
    m_cct(cct),
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

  std::string labels = ceph::perf_counters::key_create("mds_client_metrics",
						       {{"fs_name", mds->mdsmap->get_fs_name()},
							{"id", stringify(mds->get_global_id())}});
  PerfCountersBuilder plb(m_cct, labels, l_mds_client_metrics_start, l_mds_client_metrics_last);
  plb.add_u64(l_mds_client_metrics_num_clients,
	      "num_clients", "Number of client sessions", "mcli", PerfCountersBuilder::PRIO_CRITICAL);
  m_perf_counters = plb.create_perf_counters();
  m_cct->get_perfcounters_collection()->add(m_perf_counters);

  pinger = std::thread([this]() {
      ceph_pthread_setname("mds-ping");
      std::unique_lock locker(lock);
      while (!stopping) {
        ping_all_active_ranks();
        locker.unlock();
        double timo = g_conf().get_val<std::chrono::seconds>("mds_ping_interval").count();
        sleep(timo);
        locker.lock();
      }
    });

  mgrc->set_perf_metric_query_cb(
    [this](const ConfigPayload &config_payload) {
      set_perf_queries(config_payload);
    },
    [this]() {
      return get_perf_reports();
    });

  subv_window_sec = g_conf().get_val<std::chrono::seconds>("subv_metrics_window_interval").count();
  if (!subv_window_sec)
    return -EINVAL;

  return 0;
}

void MetricAggregator::shutdown() {
  dout(10) << dendl;

  {
    std::scoped_lock locker(lock);
    ceph_assert(!stopping);
    stopping = true;

    // dealloc per-client perf counter
    for (auto [crpair, pc] : client_perf_counters) {
      PerfCounters *perf_counters = nullptr;
      std::swap(perf_counters, pc);
      if (perf_counters != nullptr) {
	m_cct->get_perfcounters_collection()->remove(perf_counters);
	delete perf_counters;
      }
    }
    client_perf_counters.clear();

    PerfCounters *perf_counters = nullptr;
    std::swap(perf_counters, m_perf_counters);
    if (perf_counters != nullptr) {
      m_cct->get_perfcounters_collection()->remove(perf_counters);
      delete perf_counters;
    }
  }

  if (pinger.joinable()) {
    pinger.join();
  }
}

Dispatcher::dispatch_result_t MetricAggregator::ms_dispatch2(const ref_t<Message> &m) {
  dout(25) << " processing " << m << dendl;
  if (m->get_type() == MSG_MDS_METRICS &&
      m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MDS) {
    const Message *msg = m.get();
    const MMDSOp *op = dynamic_cast<const MMDSOp*>(msg);
    if (!op)
      dout(0) << typeid(*msg).name() << " is not an MMDSOp type" << dendl;
    ceph_assert(op);
    handle_mds_metrics(ref_cast<MMDSMetrics>(m));
    return true;
  }
  return false;
}

void MetricAggregator::refresh_subvolume_metrics_for_rank(
        mds_rank_t rank, const std::vector<SubvolumeMetric> &metrics) {
  for (const auto &m : metrics) {
    // Register labeled PerfCounters if needed
    if (!subvolume_perf_counters.contains(m.subvolume_path)) {
      std::string labels = ceph::perf_counters::key_create(
              "mds_subvolume_metrics",
              {{"subvolume_path", m.subvolume_path},
               {"fs_name", std::string(mds->mdsmap->get_fs_name())}});
      PerfCountersBuilder plb(m_cct, labels,
                              l_subvolume_metrics_first,
                              l_subvolume_metrics_last);
      plb.add_u64(l_subvolume_metrics_read_iops, "avg_read_iops",
                  "Average read IOPS", "rops", PerfCountersBuilder::PRIO_CRITICAL);
      plb.add_u64(l_subvolume_metrics_read_tp_Bps, "avg_read_tp_Bps",
                  "Average read throughput (Bps)", "rbps", PerfCountersBuilder::PRIO_CRITICAL);
      plb.add_u64(l_subvolume_metrics_avg_read_latency, "avg_read_lat_msec",
                  "Average read latency (ms)", "rlav", PerfCountersBuilder::PRIO_CRITICAL);
      plb.add_u64(l_subvolume_metrics_write_iops, "avg_write_iops",
                  "Average write IOPS", "wops", PerfCountersBuilder::PRIO_CRITICAL);
      plb.add_u64(l_subvolume_metrics_write_tp_Bps, "avg_write_tp_Bps",
                  "Average write throughput (Bps)", "wbps", PerfCountersBuilder::PRIO_CRITICAL);
      plb.add_u64(l_subvolume_metrics_avg_write_latency, "avg_write_lat_msec",
                  "Average write latency (ms)", "wlav", PerfCountersBuilder::PRIO_CRITICAL);

      auto perf_counter = plb.create_perf_counters();
      subvolume_perf_counters[m.subvolume_path] = perf_counter;
      m_cct->get_perfcounters_collection()->add(perf_counter);

      subvolume_aggregated_metrics.try_emplace(m.subvolume_path, subv_window_sec);
    }

    // Update sliding window
    auto &tracker = subvolume_aggregated_metrics.at(m.subvolume_path);
    tracker.add_value(m);
  }

  // Aggregate, update metrics, and clean stale subvolumes
  for (auto it = subvolume_aggregated_metrics.begin(); it != subvolume_aggregated_metrics.end(); ) {
    const std::string &path = it->first;
    auto &tracker = it->second;
    tracker.update();

    if (tracker.is_empty()) {
      dout(10) << "Removing stale subv_metric for path=" << path  << ", window size:=" << subv_window_sec << dendl;

      // Remove PerfCounters
      auto counter_it = subvolume_perf_counters.find(path);
      if (counter_it != subvolume_perf_counters.end()) {
        m_cct->get_perfcounters_collection()->remove(counter_it->second);
        delete counter_it->second;
        subvolume_perf_counters.erase(counter_it);
      }

      // Remove PerfQuery entries
      for (auto &[query, perf_key_map] : query_metrics_map) {
        MDSPerfMetricKey key;
        auto sub_key_func_cleanup = [this, &path](const MDSPerfMetricSubKeyDescriptor &desc,
                                                  MDSPerfMetricSubKey *sub_key) {
            if (desc.type == MDSPerfMetricSubKeyType::SUBVOLUME_PATH) {
              std::smatch match;
              if (std::regex_search(path, match, desc.regex) && match.size() > 1) {
                for (size_t i = 1; i < match.size(); ++i) {
                  sub_key->push_back(match[i].str());
                }
                return true;
              }
            } else if (desc.type == MDSPerfMetricSubKeyType::MDS_RANK) {
              sub_key->push_back(std::to_string(mds->get_nodeid()));
              return true;
            }
            return false;
        };

        if (query.get_key(sub_key_func_cleanup, &key)) {
          if (perf_key_map.erase(key)) {
            dout(15) << __func__ << ": Removed PerfQuery entry for subv_metric=" << path << dendl;
          }
        }
      }

      it = subvolume_aggregated_metrics.erase(it);
      // removed stale, continue to the next one, no need to increment the iterator since erase returns the next one
      continue;
    } else {
      tracker.update();

      AggregatedSubvolumeMetric aggr_metric;
      aggr_metric.subvolume_path = path;
      aggr_metric.time_window_last_dur_sec = tracker.get_current_window_duration_sec();
      aggr_metric.time_window_last_end_sec = tracker.get_time_from_last_sample();
      if (aggr_metric.time_window_last_dur_sec == 0)
        aggr_metric.time_window_last_dur_sec = 1; // avoid div-by-zero

      uint64_t total_read_ops = 0, total_write_ops = 0;
      uint64_t total_read_bytes = 0, total_write_bytes = 0;
      uint64_t weighted_read_latency_sum = 0, weighted_write_latency_sum = 0;

      tracker.for_each_value([&](const SubvolumeMetric &m) {
          total_read_ops += m.read_ops;
          total_write_ops += m.write_ops;
          total_read_bytes += m.read_size;
          total_write_bytes += m.write_size;
          weighted_read_latency_sum += m.avg_read_latency * m.read_ops;
          weighted_write_latency_sum += m.avg_write_latency * m.write_ops;
      });

      aggr_metric.read_iops = total_read_ops / aggr_metric.time_window_last_dur_sec;
      aggr_metric.write_iops = total_write_ops / aggr_metric.time_window_last_dur_sec;
      aggr_metric.read_tpBs = total_read_bytes / aggr_metric.time_window_last_dur_sec;
      aggr_metric.write_tBps = total_write_bytes / aggr_metric.time_window_last_dur_sec;

      aggr_metric.avg_read_latency = (total_read_ops > 0)
                                     ? (weighted_read_latency_sum / total_read_ops) / 1000
                                     : 0;
      aggr_metric.avg_write_latency = (total_write_ops > 0)
                                      ? (weighted_write_latency_sum / total_write_ops) / 1000
                                      : 0;

      // update PerfCounters
      auto counter = subvolume_perf_counters[path];
      ceph_assert(counter);
      counter->set(l_subvolume_metrics_read_iops, aggr_metric.read_iops);
      counter->set(l_subvolume_metrics_read_tp_Bps, aggr_metric.read_tpBs);
      counter->set(l_subvolume_metrics_avg_read_latency, aggr_metric.avg_read_latency);
      counter->set(l_subvolume_metrics_write_iops, aggr_metric.write_iops);
      counter->set(l_subvolume_metrics_write_tp_Bps, aggr_metric.write_tBps);
      counter->set(l_subvolume_metrics_avg_write_latency, aggr_metric.avg_write_latency);
      counter->set(l_subvolume_metrics_last_window_end, aggr_metric.time_window_last_end_sec);
      counter->set(l_subvolume_metrics_last_window, aggr_metric.time_window_last_dur_sec);

      // Update query_metrics_map
      auto sub_key_func_subvolume = [this, &path](const MDSPerfMetricSubKeyDescriptor &desc,
                                                  MDSPerfMetricSubKey *sub_key) {
          if (desc.type == MDSPerfMetricSubKeyType::SUBVOLUME_PATH) {
            std::smatch match;
            if (std::regex_search(path, match, desc.regex) && match.size() > 1) {
              for (size_t i = 1; i < match.size(); ++i) {
                sub_key->push_back(match[i].str());
              }
              return true;
            }
          } else if (desc.type == MDSPerfMetricSubKeyType::MDS_RANK) {
            sub_key->push_back(std::to_string(mds->get_nodeid()));
            return true;
          }
          return false;
      };

      for (auto &[query, perf_key_map] : query_metrics_map) {
        MDSPerfMetricKey key;
        bool matched = query.get_key(sub_key_func_subvolume, &key);
        if (!matched)
          continue;

        auto &perf_counters = perf_key_map[key];
        if (perf_counters.empty()) {
          perf_counters.resize(query.performance_counter_descriptors.size());
        }

        query.update_counters(
                [&](const MDSPerformanceCounterDescriptor &desc, PerformanceCounter *counter) {
                    switch (desc.type) {
                      case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
                        counter->first = aggr_metric.read_iops;
                        break;
                      case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
                        counter->first = aggr_metric.write_iops;
                        break;
                      case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
                        counter->first = aggr_metric.read_tpBs;
                        break;
                      case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
                        counter->first = aggr_metric.write_tBps;
                        break;
                      case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
                        counter->first = aggr_metric.avg_read_latency;
                        break;
                      case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
                        counter->first = aggr_metric.avg_write_latency;
                        break;
                      default:
                        break;
                    }
                },
                &perf_counters);
      }

      // non stale metric, continue to the next one
      ++it;
    }
  }
}

void MetricAggregator::refresh_metrics_for_rank(const entity_inst_t &client,
                                                mds_rank_t rank, const Metrics &metrics) {
  dout(20) << ": client=" << client << ", rank=" << rank << ", metrics="
           << metrics << dendl;

  auto &p = clients_by_rank.at(rank);
  auto crpair = std::make_pair(client, rank);
  bool ins = p.insert(client).second;
  if (ins) {
    dout(20) << ": rank=" << rank << " has " << p.size() << " connected"
             << " client(s)" << dendl;
    if (m_perf_counters) {
      m_perf_counters->inc(l_mds_client_metrics_num_clients);
    }

    std::string labels = ceph::perf_counters::key_create("mds_client_metrics-" + std::string(mds->mdsmap->get_fs_name()),
							 {{"client", stringify(client.name)},
							  {"rank", stringify(rank)}});
    PerfCountersBuilder plb(m_cct, labels, l_mds_per_client_metrics_start, l_mds_per_client_metrics_last);
    plb.add_u64(l_mds_per_client_metrics_cap_hits,
		"cap_hits", "Capability hits", "hcap", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_cap_misses,
		"cap_miss", "Capability misses", "mcap", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_time(l_mds_per_client_metrics_avg_read_latency,
		 "avg_read_latency", "Average Read Latency", "arlt", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_time(l_mds_per_client_metrics_avg_write_latency,
		 "avg_write_latency", "Average Write Latency", "awlt", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_time(l_mds_per_client_metrics_avg_metadata_latency,
		 "avg_metadata_latency", "Average Metadata Latency", "amlt", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_dentry_lease_hits,
		"dentry_lease_hits", "Dentry Lease Hits", "hden", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_dentry_lease_misses,
		"dentry_lease_miss", "Dentry Lease Misses", "mden", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_opened_files,
		"opened_files", "Open Files", "ofil", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_opened_inodes,
		"opened_inodes", "Open Inodes", "oino", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_pinned_icaps,
		"pinned_icaps", "Pinned Inode Caps", "pino", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_total_inodes,
		"total_inodes", "Total Inodes", "tino", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_total_read_ops,
		"total_read_ops", "Total Read Operations", "rops", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_total_read_size,
		"total_read_size", "Total Read Size", "rsiz", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_total_write_ops,
		"total_write_ops", "Total Write Operations", "wops", PerfCountersBuilder::PRIO_CRITICAL);
    plb.add_u64(l_mds_per_client_metrics_total_write_size,
		"total_write_size", "Total Write Size", "wsiz", PerfCountersBuilder::PRIO_CRITICAL);
    client_perf_counters[crpair] = plb.create_perf_counters();
    m_cct->get_perfcounters_collection()->add(client_perf_counters[crpair]);
  }

  // update perf counters
  PerfCounters *perf_counter_ptr = nullptr;
  if (client_perf_counters.contains(crpair)) {
    perf_counter_ptr = client_perf_counters[crpair];
  }

  if (perf_counter_ptr) {
    // client capability hit ratio
    perf_counter_ptr->set(l_mds_per_client_metrics_cap_hits, metrics.cap_hit_metric.hits);
    perf_counter_ptr->set(l_mds_per_client_metrics_cap_misses, metrics.cap_hit_metric.misses);

    // some averages
    if (metrics.read_latency_metric.updated) {
      utime_t ravg(metrics.read_latency_metric.mean.tv.tv_sec * 100,
		   metrics.read_latency_metric.mean.tv.tv_nsec / 1000000);
      perf_counter_ptr->tset(l_mds_per_client_metrics_avg_read_latency, ravg);
    }
    if (metrics.write_latency_metric.updated) {
      utime_t wavg(metrics.write_latency_metric.mean.tv.tv_sec * 100,
		   metrics.write_latency_metric.mean.tv.tv_nsec / 1000000);
      perf_counter_ptr->set(l_mds_per_client_metrics_avg_write_latency, wavg);
    }
    if (metrics.metadata_latency_metric.updated) {
      utime_t mavg(metrics.metadata_latency_metric.mean.tv.tv_sec * 100,
		   metrics.metadata_latency_metric.mean.tv.tv_nsec / 1000000);
      perf_counter_ptr->set(l_mds_per_client_metrics_avg_metadata_latency, mavg);
    }

    // dentry leases
    if (metrics.dentry_lease_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_dentry_lease_hits, metrics.dentry_lease_metric.hits);
      perf_counter_ptr->set(l_mds_per_client_metrics_dentry_lease_misses, metrics.dentry_lease_metric.misses);
    }

    // file+inode opens, pinned inode caps
    if (metrics.opened_files_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_opened_files, metrics.opened_files_metric.opened_files);
      perf_counter_ptr->set(l_mds_per_client_metrics_total_inodes, metrics.opened_files_metric.total_inodes);
    }
    if (metrics.opened_inodes_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_opened_inodes, metrics.opened_inodes_metric.total_inodes);
      perf_counter_ptr->set(l_mds_per_client_metrics_total_inodes, metrics.opened_inodes_metric.total_inodes);
    }
    if (metrics.pinned_icaps_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_pinned_icaps, metrics.pinned_icaps_metric.pinned_icaps);
      perf_counter_ptr->set(l_mds_per_client_metrics_total_inodes, metrics.pinned_icaps_metric.total_inodes);
    }

    // read+write io metrics
    if (metrics.read_io_sizes_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_total_read_ops, metrics.read_io_sizes_metric.total_ops);
      perf_counter_ptr->set(l_mds_per_client_metrics_total_read_size, metrics.read_io_sizes_metric.total_size);
    }
    if (metrics.write_io_sizes_metric.updated) {
      perf_counter_ptr->set(l_mds_per_client_metrics_total_write_ops, metrics.write_io_sizes_metric.total_ops);
      perf_counter_ptr->set(l_mds_per_client_metrics_total_write_size, metrics.write_io_sizes_metric.total_size);
    }
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
      if (metrics.read_latency_metric.updated) {
        c->first = metrics.read_latency_metric.lat.tv.tv_sec;
        c->second = metrics.read_latency_metric.lat.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
      if (metrics.write_latency_metric.updated) {
        c->first = metrics.write_latency_metric.lat.tv.tv_sec;
        c->second = metrics.write_latency_metric.lat.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
      if (metrics.metadata_latency_metric.updated) {
        c->first = metrics.metadata_latency_metric.lat.tv.tv_sec;
        c->second = metrics.metadata_latency_metric.lat.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::DENTRY_LEASE_METRIC:
      if (metrics.dentry_lease_metric.updated) {
        c->first = metrics.dentry_lease_metric.hits;
        c->second = metrics.dentry_lease_metric.misses;
      }
      break;
    case MDSPerformanceCounterType::OPENED_FILES_METRIC:
      if (metrics.opened_files_metric.updated) {
        c->first = metrics.opened_files_metric.opened_files;
        c->second = metrics.opened_files_metric.total_inodes;
      }
      break;
    case MDSPerformanceCounterType::PINNED_ICAPS_METRIC:
      if (metrics.pinned_icaps_metric.updated) {
        c->first = metrics.pinned_icaps_metric.pinned_icaps;
        c->second = metrics.pinned_icaps_metric.total_inodes;
      }
      break;
    case MDSPerformanceCounterType::OPENED_INODES_METRIC:
      if (metrics.opened_inodes_metric.updated) {
        c->first = metrics.opened_inodes_metric.opened_inodes;
        c->second = metrics.opened_inodes_metric.total_inodes;
      }
      break;
    case MDSPerformanceCounterType::READ_IO_SIZES_METRIC:
      if (metrics.read_io_sizes_metric.updated) {
        c->first = metrics.read_io_sizes_metric.total_ops;
        c->second = metrics.read_io_sizes_metric.total_size;
      }
      break;
    case MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC:
      if (metrics.write_io_sizes_metric.updated) {
        c->first = metrics.write_io_sizes_metric.total_ops;
        c->second = metrics.write_io_sizes_metric.total_size;
      }
      break;
    case MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC:
      if (metrics.read_latency_metric.updated) {
        c->first = metrics.read_latency_metric.mean.tv.tv_sec;
        c->second = metrics.read_latency_metric.mean.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC:
      if (metrics.read_latency_metric.updated) {
        c->first = metrics.read_latency_metric.sq_sum;
        c->second = metrics.read_latency_metric.count;
      }
      break;
    case MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC:
      if (metrics.write_latency_metric.updated) {
        c->first = metrics.write_latency_metric.mean.tv.tv_sec;
        c->second = metrics.write_latency_metric.mean.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC:
      if (metrics.write_latency_metric.updated) {
        c->first = metrics.write_latency_metric.sq_sum;
        c->second = metrics.write_latency_metric.count;
      }
      break;
    case MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC:
      if (metrics.metadata_latency_metric.updated) {
        c->first = metrics.metadata_latency_metric.mean.tv.tv_sec;
        c->second = metrics.metadata_latency_metric.mean.tv.tv_nsec;
      }
      break;
    case MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC:
      if (metrics.metadata_latency_metric.updated) {
        c->first = metrics.metadata_latency_metric.sq_sum;
        c->second = metrics.metadata_latency_metric.count;
      }
      break;
    // subvolume metrics are handled in refresh_subvolume_metrics_for_rank()
    case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
    case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
    case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
    case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
    case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
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
    // subvolumes metrics are handled in refresh_subvolume_metrics_for_rank()
    case MDSPerfMetricSubKeyType::SUBVOLUME_PATH:
      return false;
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
    auto crpair = std::make_pair(client, rank);
    m_cct->get_perfcounters_collection()->remove(client_perf_counters[crpair]);
    delete client_perf_counters[crpair];
    client_perf_counters.erase(crpair);
  }
  if (m_perf_counters) {
    m_perf_counters->dec(l_mds_client_metrics_num_clients);
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
        // subvolume metrics are handled in refresh_subvolume_metrics_for_rank()
    case MDSPerfMetricSubKeyType::SUBVOLUME_PATH:
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
  if (stopping) {
    dout(10) << ": stopping" << dendl;
    return;
  }
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

  refresh_subvolume_metrics_for_rank(rank, metrics_message.subvolume_metrics);
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
    dout(10) << ": active rank=" << rank << " (mds." << mdsmap.get_mds_info(rank).name
             << ") has addr=" << rank_addr << dendl;
    active_rank_addrs.emplace(rank, rank_addr);
    clients_by_rank.emplace(rank, std::unordered_set<entity_inst_t>{});
  }

  dout(10) << ": active set=["  << active_rank_addrs << "]" << dendl;
}

void MetricAggregator::set_perf_queries(const ConfigPayload &config_payload) {
  const MDSConfigPayload &mds_config_payload = std::get<MDSConfigPayload>(config_payload);
  const std::map<MDSPerfMetricQuery, MDSPerfMetricLimits> &queries = mds_config_payload.config;

  dout(10) << ": setting " << queries.size() << " perf_queries" << dendl;

  std::scoped_lock locker(lock);
  std::map<MDSPerfMetricQuery, std::map<MDSPerfMetricKey, PerformanceCounters>> new_data;
  for (auto &p : queries) {
    dout(10) << ": perf_query " << p << dendl;
    std::swap(new_data[p.first], query_metrics_map[p.first]);
  }
  std::swap(query_metrics_map, new_data);
}

MetricPayload MetricAggregator::get_perf_reports() {
  MDSMetricPayload payload;
  MDSPerfMetricReport &metric_report = payload.metric_report;
  std::map<MDSPerfMetricQuery, MDSPerfMetrics> &reports = metric_report.reports;

  std::scoped_lock locker(lock);

  for (auto& [query, counters] : query_metrics_map) {
    auto &report = reports[query];

    query.get_performance_counter_descriptors(&report.performance_counter_descriptors);

    auto &descriptors = report.performance_counter_descriptors;

    dout(20) << ": descriptors=" << descriptors << dendl;

    for (auto &p : counters) {
      dout(20) << ": packing perf_metric_key=" << p.first << ", perf_counter="
               << p.second << dendl;
      auto &bl = report.group_packed_performance_counters[p.first];
      query.pack_counters(p.second, &bl);
    }
  }

  // stash a copy of dealyed and failed ranks. mgr culls out metrics
  // for failed ranks and tags metrics for delayed ranks as "stale".
  for (auto &p : active_rank_addrs) {
    auto rank = p.first;
    if (mds_pinger.is_rank_lagging(rank)) {
      metric_report.rank_metrics_delayed.insert(rank);
    }
  }

  return payload;
}
