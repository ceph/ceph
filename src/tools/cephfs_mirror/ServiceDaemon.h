// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPHFS_MIRROR_SERVICE_DAEMON_H
#define CEPHFS_MIRROR_SERVICE_DAEMON_H

#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "mds/FSMap.h"
#include "Types.h"

namespace cephfs {
namespace mirror {

const std::string SERVICE_DAEMON_MIRROR_ENABLE_FAILED_KEY("mirroring_failed");
const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";

class ServiceDaemon {
public:
  ServiceDaemon(CephContext *cct, RadosRef rados);
  ~ServiceDaemon();

  int init();

  void add_filesystem(fs_cluster_id_t fscid, std::string_view fs_name);
  void remove_filesystem(fs_cluster_id_t fscid);

  void add_peer(fs_cluster_id_t fscid, const Peer &peer);
  void remove_peer(fs_cluster_id_t fscid, const Peer &peer);

  void add_or_update_fs_attribute(fs_cluster_id_t fscid, std::string_view key,
                                  AttributeValue value);
  void add_or_update_peer_attribute(fs_cluster_id_t fscid, const Peer &peer,
                                    std::string_view key, AttributeValue value);
  void update_mirror_health(std::vector<DaemonHealthMetric>& health_metrics);
  void schedule_health_tick();
private:
  struct Filesystem {
    std::string fs_name;
    Attributes fs_attributes;
    std::map<Peer, Attributes> peer_attributes;

    Filesystem(std::string_view fs_name)
      : fs_name(fs_name) {
    }
  };

  const std::string CEPHFS_MIRROR_AUTH_ID_PREFIX = "cephfs-mirror.";

  CephContext *m_cct;
  RadosRef m_rados;
  SafeTimer *m_timer, *h_timer;
  ceph::mutex m_timer_lock = ceph::make_mutex("cephfs::mirror::ServiceDaemon");
  ceph::mutex h_timer_lock = ceph::make_mutex("cephfs::mirror::ServiceDaemon");
  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::service_daemon");
  Context *m_timer_ctx = nullptr;
  Context *h_timer_ctx = nullptr;
  std::map<fs_cluster_id_t, Filesystem> m_filesystems;
  std::vector<DaemonHealthMetric> m_health_metrics;

  void schedule_update_status();
  void update_status();
  std::vector<DaemonHealthMetric> get_health_metrics();
  void health_tick();

};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_SERVICE_DAEMON_H
