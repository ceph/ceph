// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MetricsHandler.h"

#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>
#include <variant>
#include <vector>
#include <unistd.h>

#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"

#include "include/fs_types.h"
#include "include/stringify.h"
#include "messages/MClientMetrics.h"
#include "messages/MMDSMetrics.h"
#include "messages/MMDSPing.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "CInode.h"
#include "SessionMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": mds.metrics"

MetricsHandler::MetricsHandler(CephContext *cct, MDSRank *mds)
  : Dispatcher(cct),
    mds(mds) {
  clk_tck = sysconf(_SC_CLK_TCK);
  if (clk_tck <= 0) {
    dout(1) << "failed to determine clock ticks per second, cpu usage metric disabled" << dendl;
    clk_tck = 0;
  }
}

Dispatcher::dispatch_result_t MetricsHandler::ms_dispatch2(const ref_t<Message> &m) {
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

  if (mds->get_nodeid() != 0 && !rank_perf_counters) {
    std::string labels = ceph::perf_counters::key_create(
      "mds_rank_perf",
      {{"rank", stringify(mds->get_nodeid())}});

    PerfCountersBuilder plb(cct, labels,
                            l_mds_rank_perf_start, l_mds_rank_perf_last);
    plb.add_u64(l_mds_rank_perf_cpu_usage,
                "cpu_usage",
                "Sum of per-core CPU utilisation for this MDS (100 == one full core)",
                "cpu%",
                PerfCountersBuilder::PRIO_USEFUL);
    plb.add_u64(l_mds_rank_perf_open_requests,
                "open_requests",
                "Number of metadata requests currently in flight",
                "req",
                PerfCountersBuilder::PRIO_USEFUL);
    rank_perf_counters = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(rank_perf_counters);
  }

  subv_window_sec = g_conf().get_val<std::chrono::seconds>("subv_metrics_window_interval").count();
  if (!subv_window_sec) {
    dout(0) << "subv_metrics_window_interval is not set, setting to 300 seconds" << dendl;
    subv_window_sec = 300;
  }

  updater = std::thread([this]() {
      ceph_pthread_setname("mds-metrics");
      std::unique_lock locker(lock);
      while (!stopping) {
        double after = g_conf().get_val<std::chrono::seconds>("mds_metrics_update_interval").count();
        locker.unlock();
        sleep(after);
        locker.lock();
        update_rank0(locker);
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

  if (rank_perf_counters) {
    cct->get_perfcounters_collection()->remove(rank_perf_counters);
    delete rank_perf_counters;
    rank_perf_counters = nullptr;
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

void MetricsHandler::handle_payload(Session* session,  const SubvolumeMetricsPayload& payload, std::unique_lock<ceph::mutex>& lk) {
  dout(20) << ": type=" << payload.get_type() << ", session=" << session
      << " , subv_metrics count=" << payload.subvolume_metrics.size() << dendl;

  ceph_assert(lk.owns_lock()); // caller must hold the lock

  std::vector<std::string> resolved_paths;
  resolved_paths.reserve(payload.subvolume_metrics.size());

  // RAII guard: unlock on construction, re-lock on destruction (even on exceptions)
  UnlockGuard unlock_guard{lk};

  // unlocked: resolve paths, no contention with mds lock
  for (const auto& metric : payload.subvolume_metrics) {
    std::string path = mds->get_path(metric.subvolume_id);
    if (path.empty()) {
      dout(10) << " path not found for " << metric.subvolume_id << dendl;
    }
    resolved_paths.emplace_back(std::move(path));
  }

  // locked again (via UnlockGuard dtor): update metrics map
  const auto now_ms = static_cast<int64_t>(
  std::chrono::duration_cast<std::chrono::milliseconds>(
std::chrono::steady_clock::now().time_since_epoch()).count());

  // Keep index pairing but avoid double map lookup
  for (size_t i = 0; i < resolved_paths.size(); ++i) {
    const auto& path = resolved_paths[i];
    if (path.empty()) continue;

    auto& vec = subvolume_metrics_map[path];

    dout(20) << " accumulating subv_metric " << payload.subvolume_metrics[i] << dendl;
    // std::move of the const expression of the trivially-copyable type 'const value_type'
    // (aka 'const AggregatedIOMetrics') has no effect; remove std::move()
    vec.emplace_back(payload.subvolume_metrics[i]);
    vec.back().time_stamp = now_ms;
  }
}

void MetricsHandler::handle_payload(Session *session, const UnknownPayload &payload) {
  dout(5) << ": type=Unknown, session=" << session << ", ignoring unknown payload" << dendl;
}

void MetricsHandler::handle_client_metrics(const cref_t<MClientMetrics> &m) {
  if (!mds->is_active_lockless()) {
    dout(20) << ": dropping metrics message during recovery" << dendl;
    return;
  }

  std::unique_lock locker(lock);

  Session *session = mds->get_session(m);
  dout(20) << ": session=" << session << dendl;

  if (session == nullptr) {
    dout(10) << ": ignoring session less message" << dendl;
    return;
  }

  for (auto &metric : m->updates) {
    // Special handling for SubvolumeMetricsPayload to avoid lock contention
    if (auto* subv_payload = std::get_if<SubvolumeMetricsPayload>(&metric.payload)) {
      // this handles the subvolume metrics payload without acquiring the mds lock
      // should not call the visitor pattern here
      handle_payload(session, *subv_payload, locker);
    } else {
      std::visit(HandlePayloadVisitor(this, session), metric.payload);
    }
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

void MetricsHandler::update_rank0(std::unique_lock<ceph::mutex>& locker) {
  dout(20) << dendl;

  sample_cpu_usage();
  sample_open_requests();

  if (!addr_rank0) {
    dout(20) << ": not yet notified with rank0 address, ignoring" << dendl;
    return;
  }

  metrics_message_t metrics_message;
  auto &update_client_metrics_map = metrics_message.client_metrics_map;

  metrics_message.seq = next_seq;
  metrics_message.rank = mds->get_nodeid();
  metrics_message.rank_metrics = rank_telemetry.metrics;

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

  // Resolve used_bytes for all subvolumes without holding the metrics lock
  // (same unlock pattern used for path resolution in handle_payload).
  // Step 1 (locked): collect unique subvolume ids
  std::vector<inodeno_t> subvol_ids;
  subvol_ids.reserve(subvolume_metrics_map.size());
  for (const auto &[path, aggregated_metrics] : subvolume_metrics_map) {
    if (!aggregated_metrics.empty()) {
      subvol_ids.push_back(aggregated_metrics.front().subvolume_id);
    }
  }

  // Step 2 (unlocked): fetch rbytes under mds_lock via helper, release metric log
  // it allows to proceed another update in metrics handler
  UnlockGuard unlock_guard{locker};

  // here the metrics handler lock witll be retaken via the UnlockGuard dtor
  std::unordered_map<inodeno_t, uint64_t> subvol_used_bytes;
  for (inodeno_t subvol_id : subvol_ids) {
    if (subvol_used_bytes.count(subvol_id) == 0) {
      uint64_t rbytes = mds->get_inode_rbytes(subvol_id);
      subvol_used_bytes[subvol_id] = rbytes;
      dout(20) << "resolved used_bytes for subvol " << subvol_id << " = " << rbytes << dendl;
    }
  }

  // subvolume metrics, reserve 100 entries per subvolume ? good enough?
  metrics_message.subvolume_metrics.reserve(subvolume_metrics_map.size()* 100);
  for (auto &[path, aggregated_metrics] : subvolume_metrics_map) {
    metrics_message.subvolume_metrics.emplace_back();
    aggregate_subvolume_metrics(path, aggregated_metrics, subvol_used_bytes,
                                metrics_message.subvolume_metrics.back());
  }
  // if we need to show local MDS metrics, we need to save a last copy...
  subvolume_metrics_map.clear();

  // Evict stale subvolume quota entries
  if (subv_window_sec > 0) {
    auto now = std::chrono::steady_clock::now();
    auto threshold = std::chrono::seconds(subv_window_sec) * 2;
    for (auto it = subvolume_quota.begin(); it != subvolume_quota.end(); ) {
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_activity);
      if (elapsed > threshold) {
        dout(15) << "evicting stale subvolume quota entry " << it->first
                 << " (inactive for " << elapsed.count() << "s)" << dendl;
        it = subvolume_quota.erase(it);
      } else {
        ++it;
      }
    }
  }

  // only start incrementing when its kicked via set_next_seq()
  if (next_seq != 0) {
    ++last_updated_seq;
  }

  dout(20) << ": sending " << metrics_message.subvolume_metrics.size() << " subv_metrics to aggregator"
	   << dendl;

  mds->send_message_mds(make_message<MMDSMetrics>(std::move(metrics_message)), *addr_rank0);
}

void MetricsHandler::aggregate_subvolume_metrics(const std::string& subvolume_path,
                                 const std::vector<AggregatedIOMetrics>& metrics_list,
                                 const std::unordered_map<inodeno_t, uint64_t>& subvol_used_bytes,
                                 SubvolumeMetric &res) {
  dout(20) << ": aggregating " << metrics_list.size() << " subv_metrics" << dendl;
  res.subvolume_path = subvolume_path;

  uint64_t weighted_read_latency_sum = 0;
  uint64_t weighted_write_latency_sum = 0;

  res.read_ops = 0;
  res.write_ops = 0;
  res.read_size = 0;
  res.write_size = 0;
  res.avg_read_latency = 0;
  res.avg_write_latency = 0;
  res.time_stamp = 0;
  res.quota_bytes = 0;
  res.used_bytes = 0;

  for (const auto& m : metrics_list) {
    res.read_ops += m.read_count;
    res.write_ops += m.write_count;
    res.read_size += m.read_bytes;
    res.write_size += m.write_bytes;
    // we want to have more metrics in the sliding window (on the aggregator),
    // so we set the latest timestamp of all received metrics
    res.time_stamp = std::max(res.time_stamp, m.time_stamp);

    if (m.read_count > 0) {
      weighted_read_latency_sum += m.read_latency_us * m.read_count;
    }

    if (m.write_count > 0) {
      weighted_write_latency_sum += m.write_latency_us * m.write_count;
    }
  }

  // Lookup quota and used_bytes after aggregating I/O metrics
  if (!metrics_list.empty()) {
    inodeno_t subvolume_id = metrics_list.front().subvolume_id;
    
    // Get quota/used bytes from cache and update last activity time
    auto it = subvolume_quota.find(subvolume_id);
    if (it != subvolume_quota.end()) {
      res.quota_bytes = it->second.quota_bytes;
      res.used_bytes = it->second.used_bytes;
      it->second.last_activity = std::chrono::steady_clock::now();
    }
    
    // Fallback: if cache didn't have used_bytes, use the pre-fetched map
    if (res.used_bytes == 0) {
      auto used_it = subvol_used_bytes.find(subvolume_id);
      if (used_it != subvol_used_bytes.end()) {
        res.used_bytes = used_it->second;
      }
    }
  }

  // normalize latencies
  res.avg_read_latency = (res.read_ops > 0)
                         ? (weighted_read_latency_sum / res.read_ops)
                         : 0;
  res.avg_write_latency = (res.write_ops > 0)
                          ? (weighted_write_latency_sum / res.write_ops)
                          : 0;
}

void MetricsHandler::maybe_update_subvolume_quota(inodeno_t subvol_id, uint64_t quota_bytes, uint64_t used_bytes, bool force_zero) {
  std::lock_guard l(lock);
  
  auto it = subvolume_quota.find(subvol_id);
  if (it == subvolume_quota.end()) {
    // If the subvolume was not registered yet, insert it now so we don't lose
    // the first quota update (e.g., when broadcast happens before caps/metadata).
    it = subvolume_quota.emplace(subvol_id, SubvolumeQuotaInfo{}).first;
    dout(20) << __func__ << " inserted subvolume_quota for " << subvol_id << dendl;
  }

  // Only update quota_bytes if this inode has quota enabled (avoid overwriting
  // a good value from a quota-enabled child with 0 from the subvolume root).
  // Exception: force_zero=true means quota was explicitly removed (set to unlimited).
  if (quota_bytes > 0 || force_zero) {
    it->second.quota_bytes = quota_bytes;
  }
  it->second.used_bytes = used_bytes;
  it->second.last_activity = std::chrono::steady_clock::now();

  dout(20) << __func__ << " subvol " << subvol_id
           << " quota=" << it->second.quota_bytes
           << " used=" << it->second.used_bytes
           << " (input: quota=" << quota_bytes << ", used=" << used_bytes << ")" << dendl;
}

void MetricsHandler::sample_cpu_usage() {
  uint64_t current_ticks = 0;
  ceph::mds::proc_stat_error err;

  if (clk_tck <= 0) {
    rank_telemetry.metrics.cpu_usage_percent = 0;
    if (rank_perf_counters) {
      rank_perf_counters->set(l_mds_rank_perf_cpu_usage, 0);
    }
    return;
  }

  if (!ceph::mds::read_process_cpu_ticks(&current_ticks, &err)) {
    rank_telemetry.metrics.cpu_usage_percent = 0;
    if (rank_perf_counters) {
      rank_perf_counters->set(l_mds_rank_perf_cpu_usage, 0);
    }
    constexpr const char* stat_path = PROCPREFIX "/proc/self/stat";
    if (err == ceph::mds::proc_stat_error::not_resolvable) {
      dout(5) << "input file '" << stat_path << "' not resolvable" << dendl;
    } else if (err == ceph::mds::proc_stat_error::not_found) {
      dout(5) << "input file '" << stat_path << "' not found" << dendl;
    }
    return;
  }

  auto now = std::chrono::steady_clock::now();
  if (!rank_telemetry.cpu_sample_initialized) {
    rank_telemetry.last_cpu_total_ticks = current_ticks;
    rank_telemetry.last_cpu_sample_time = now;
    rank_telemetry.cpu_sample_initialized = true;
    rank_telemetry.metrics.cpu_usage_percent = 0;
    if (rank_perf_counters) {
      rank_perf_counters->set(l_mds_rank_perf_cpu_usage, 0);
    }
    return;
  }

  if (current_ticks < rank_telemetry.last_cpu_total_ticks) {
    rank_telemetry.last_cpu_total_ticks = current_ticks;
    rank_telemetry.last_cpu_sample_time = now;
    rank_telemetry.metrics.cpu_usage_percent = 0;
    if (rank_perf_counters) {
      rank_perf_counters->set(l_mds_rank_perf_cpu_usage, 0);
    }
    return;
  }

  double elapsed = std::chrono::duration<double>(now - rank_telemetry.last_cpu_sample_time).count();
  if (elapsed <= 0.0) {
    rank_telemetry.metrics.cpu_usage_percent = 0;
    if (rank_perf_counters) {
      rank_perf_counters->set(l_mds_rank_perf_cpu_usage, 0);
    }
    return;
  }

  uint64_t delta_ticks = current_ticks - rank_telemetry.last_cpu_total_ticks;
  rank_telemetry.last_cpu_total_ticks = current_ticks;
  rank_telemetry.last_cpu_sample_time = now;

  double cpu_seconds = static_cast<double>(delta_ticks) / static_cast<double>(clk_tck);
  double cores_used = cpu_seconds / elapsed;
  double usage_percent = cores_used * 100.0;
  if (usage_percent < 0.0) {
    usage_percent = 0.0;
  }

  uint64_t stored = static_cast<uint64_t>(std::llround(usage_percent));
  if (rank_perf_counters) {
    rank_perf_counters->set(l_mds_rank_perf_cpu_usage, stored);
  }
  rank_telemetry.metrics.cpu_usage_percent = stored;
}

void MetricsHandler::sample_open_requests() {
  uint64_t open = 0;
  if (mds->op_tracker.is_tracking()) {
    open = mds->op_tracker.get_num_ops_in_flight();
  }
  rank_telemetry.metrics.open_requests = open;
  if (rank_perf_counters) {
    rank_perf_counters->set(l_mds_rank_perf_open_requests, open);
  }
}