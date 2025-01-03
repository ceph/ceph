// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_SERVICE_DAEMON_H
#define CEPHFS_MIRROR_SERVICE_DAEMON_H

#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "mds/FSMap.h"
#include "Types.h"

namespace cephfs {
namespace mirror {

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
  SafeTimer *m_timer;
  ceph::mutex m_timer_lock = ceph::make_mutex("cephfs::mirror::ServiceDaemon");

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::service_daemon");
  Context *m_timer_ctx = nullptr;
  std::map<fs_cluster_id_t, Filesystem> m_filesystems;

  void schedule_update_status();
  void update_status();
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_SERVICE_DAEMON_H
