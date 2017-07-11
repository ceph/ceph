
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/ServiceDaemon.h"
#include "include/stringify.h"
#include "common/ceph_context.h"
#include "common/config.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ServiceDaemon: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {

namespace {

const std::string RBD_MIRROR_AUTH_ID_PREFIX("rbd-mirror.");

} // anonymous namespace

template <typename I>
int ServiceDaemon<I>::init() {
  std::string name = m_cct->_conf->name.get_id();
  if (name.find(RBD_MIRROR_AUTH_ID_PREFIX) == 0) {
    name = name.substr(RBD_MIRROR_AUTH_ID_PREFIX.size());
  }

  std::map<std::string, std::string> service_metadata = {
      {"instance_id", stringify(m_rados->get_instance_id())}
    };
  int r = m_rados->service_daemon_register("rbd-mirror", name,
                                           service_metadata);
  if (r < 0) {
    return r;
  }

  return 0;
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ServiceDaemon<librbd::ImageCtx>;
