// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H
#define CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H

#include "include/rados/librados_fwd.hpp"
#include "tools/rbd/ArgumentTypes.h"

#include <string>
#include <map>

namespace rbd {

class MirrorDaemonServiceInfo {
public:
  MirrorDaemonServiceInfo(librados::IoCtx &io_ctx) : m_io_ctx(io_ctx) {
  }

  int init();

  std::string get_description(const std::string &instance_id) const;
  void dump(const std::string &instance_id,
            argument_types::Format::Formatter formatter) const;

private:
  librados::IoCtx &m_io_ctx;
  std::map<std::string, std::string> m_instance_id_to_service_id;
  std::map<std::string, std::map<std::string, std::string>> m_daemons_metadata;
};

} // namespace rbd

#endif // CEPH_RBD_MIRROR_DAEMON_SERVICE_INFO_H
