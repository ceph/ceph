// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H
#define CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H

#include "include/rados/librados_fwd.hpp"
#include "tools/rbd/ArgumentTypes.h"

#include <iosfwd>
#include <list>
#include <map>
#include <string>

namespace rbd {

enum MirrorHealth {
  MIRROR_HEALTH_OK      = 0,
  MIRROR_HEALTH_UNKNOWN = 1,
  MIRROR_HEALTH_WARNING = 2,
  MIRROR_HEALTH_ERROR   = 3
};

std::ostream& operator<<(std::ostream& os, MirrorHealth mirror_health);

struct MirrorService {
  MirrorService() {}
  explicit MirrorService(const std::string& service_id)
    : service_id(service_id) {
  }

  std::string service_id;
  std::string instance_id;
  bool leader = false;
  std::string client_id;
  std::string ceph_version;
  std::string hostname;
  std::list<std::string> callouts;

  MirrorHealth health = MIRROR_HEALTH_UNKNOWN;

  std::string get_image_description() const;
  void dump_image(argument_types::Format::Formatter formatter) const;
};

typedef std::list<MirrorService> MirrorServices;

class MirrorDaemonServiceInfo {
public:
  MirrorDaemonServiceInfo(librados::IoCtx &io_ctx) : m_io_ctx(io_ctx) {
  }

  int init();

  const MirrorService* get_by_service_id(const std::string& service_id) const;
  const MirrorService* get_by_instance_id(const std::string& instance_id) const;

  MirrorServices get_mirror_services() const;
  MirrorHealth get_daemon_health() const {
    return m_daemon_health;
  }

private:
  librados::IoCtx &m_io_ctx;

  std::map<std::string, MirrorService> m_mirror_services;
  std::map<std::string, std::string> m_instance_to_service_ids;

  MirrorHealth m_daemon_health = MIRROR_HEALTH_UNKNOWN;

  int get_mirror_service_dump();
  int get_mirror_service_status();

};

} // namespace rbd

#endif // CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H
