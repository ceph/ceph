// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AsioEngine.h"
#include "include/stringify.h"
#include "include/neorados/RADOS.hpp"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "librbd/asio/ContextWQ.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AsioEngine: " \
                           << this << " " << __func__ << ": "

namespace librbd {

AsioEngine::AsioEngine(std::shared_ptr<librados::Rados> rados)
  : m_rados_api(std::make_shared<neorados::RADOS>(
      neorados::RADOS::make_with_librados(*rados))),
    m_cct(m_rados_api->cct()),
    m_io_context(m_rados_api->get_io_context()),
    m_api_strand(m_io_context),
    m_context_wq(std::make_unique<asio::ContextWQ>(m_io_context)) {
  ldout(m_cct, 20) << dendl;

  auto rados_threads = m_cct->_conf.get_val<uint64_t>("librados_thread_count");
  auto rbd_threads = m_cct->_conf.get_val<uint64_t>("rbd_op_threads");
  if (rbd_threads > rados_threads) {
    // inherit the librados thread count -- but increase it if librbd wants to
    // utilize more threads
    m_cct->_conf.set_val("librados_thread_count", stringify(rbd_threads));
  }
}

AsioEngine::AsioEngine(librados::IoCtx& io_ctx)
  : AsioEngine(std::make_shared<librados::Rados>(io_ctx)) {
}

AsioEngine::~AsioEngine() {
  ldout(m_cct, 20) << dendl;
}

} // namespace librbd
