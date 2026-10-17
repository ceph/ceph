// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/LogClient.h"
#include "include/common_fwd.h"
#include "osd/oob_executor.h"

#include "common/ofi_rma_fwd.h"

namespace ceph::oob {
class Arena;
struct location_t;
}

/**
 * Out-of-band delivery over libfabric.
 *
 * Serves delivery tokens of the form ceph::ofi::token_t describes
 * ("<base>:<size>:ofi1:<wire>:<endpoint>:<key>[:<auth>]") whose wire is
 * the one this OSD's endpoint speaks. The provider (oob_cluster_transport
 * or oob_client_transports) picks the wire: tcp or shm need no special hardware, verbs runs RC
 * under the rxm utility provider ("rxm.1"), efa runs SRD, and any
 * vendor's UET provider runs Ultra Ethernet ("uet.<version>"), so a
 * token from one vendor's UET NIC is served on another's. The window owner never inserts this OSD into its address
 * vector, so any OSD holding the token can write, as with cuObject's DC
 * transport.
 *
 * When its provider is the cluster transport (oob_cluster_transport),
 * the executor also lends the gather windows: a pool of windows open to
 * remote writes, which shards of an erasure-coded read push into. A
 * window gets a new memory key when it is released, so that no write
 * meant for an earlier gather, a provider's late duplicate included,
 * lands in it once it is lent again.
 *
 * The endpoint also asks the provider for RMA reads, and the executor
 * pulls the payload of a write out of the client's window (an
 * OSD-direct PUT): see OSDOobExecutor::execute_pull(). A provider
 * without them serves deliveries only.
 *
 * When a cut-off fails, writes may land in a client's window after the
 * client was told they would not (see ceph::ofi::Endpoint::write()). The
 * executor then stops: it serves no token and lends no window, logs to
 * the cluster log once, and raises the OOB_DELIVERY_UNSAFE health alert.
 * With osd_oob_cutoff_policy set to abort, the OSD exits instead, which
 * ends the writes of a software provider and makes a device drop the
 * queue pair's. A quick cut-off that ends later than
 * osd_oob_cutoff_late_tolerance fails only when osd_oob_cutoff_policy is
 * disable or abort. With warn, the executor logs it to the cluster log,
 * raises OOB_CUTOFF_PAST_TOLERANCE, and goes on.
 */
class OSDOfi : public OSDOobExecutor {
public:
  OSDOfi(CephContext* cct, LogChannelRef clog);
  ~OSDOfi() override;

  OSDOfi(const OSDOfi&) = delete;
  OSDOfi& operator=(const OSDOfi&) = delete;

  /// open the endpoint of provider at the daemon's out-of-band location,
  /// staging each transfer in a buffer of arena, which must outlive the
  /// executor; and when this executor lends the gather windows, the
  /// window pool, in a run that it reserves in the arena, and the
  /// progress thread that places data in it
  int init(const std::string& provider, const ceph::oob::location_t& loc,
	   bool lend_windows, ceph::oob::Arena* arena);

  bool is_available() const override;
  bool handles(const std::string& token) const override;
  // A provider that offers only FI_THREAD_DOMAIN stops the endpoint
  // while it adds a new client's address, for seconds perhaps. The op
  // threads that call the methods below wait for that only briefly
  // (ceph::ofi::config_t::insert_wait): a read for any client, the new
  // one's included, is delivered inline meanwhile, and declined, a
  // shard's push is delivered inline, a gather lends no window, so that
  // its shards reply inline, and a returned window gets its new key from
  // a later gather. The insert first lets the shards' pushes into
  // windows lent before it land, so that they do not wait for it
  // (ceph::ofi::config_t::lent_wait).
  ssize_t execute_plan(const std::string& key,
		       const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override;
  /// with a provider that reads: the payload of a write, pulled out of
  /// the client's window through a staging buffer
  bool pulls(const std::string& token) const override;
  ssize_t execute_pull(const std::string& key,
		       const std::string& token,
		       const ceph::osd::oob::placement_plan& plan,
		       uint64_t total,
		       ceph::buffer::list* out,
		       std::chrono::milliseconds budget,
		       bool* started) override;
  void dump_stats(ceph::Formatter* f) const override;
  std::string_view name() const override { return "ofi"; }
  std::string_view status_help() const override {
    return "libfabric out-of-band delivery statistics";
  }
  std::optional<window_t> acquire_window(size_t size) override;
  void release_window(uint64_t id, uint64_t quarantine_ms) override;
  void window_sync() override;
  void get_alerts(std::map<std::string, std::string>& alerts) const override;
  bool delivery_complete() const override;

private:
  CephContext* cct;
  LogChannelRef clog;
  /// the OSD's out-of-band memory, which the endpoint stages in
  ceph::oob::Arena* arena = nullptr;
  std::unique_ptr<ceph::ofi::Endpoint> ep;
  /// the endpoint went unsafe and this executor stopped; reported once
  std::atomic<bool> stopped{false};
  /// osd_oob_cutoff_policy is abort, as init() read it
  bool abort_on_unsafe = false;
  /// stop for good once the endpoint is unsafe; true when stopped
  bool check_unsafe();
  /// report, once each, the cut-offs that ended beyond the tolerance
  /// though the endpoint stayed clean
  void check_past_tolerance();
  std::atomic<uint64_t> past_reported{0};

  /// the gather windows, in a run of the arena; null when gathers stay
  /// inline
  std::unique_ptr<ceph::ofi::WindowPool> windows;

  std::atomic<uint64_t> plans_started{0};
  std::atomic<uint64_t> plans_completed{0};
  std::atomic<uint64_t> plans_failed{0};
  std::atomic<uint64_t> bytes_pushed{0};
  std::atomic<uint64_t> pulls_started{0};
  std::atomic<uint64_t> pulls_completed{0};
  std::atomic<uint64_t> pulls_failed{0};
  std::atomic<uint64_t> bytes_pulled{0};

};
