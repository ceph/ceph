// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AsioEngine.h"
#include "common/dout.h"
#include "librbd/asio/ContextWQ.h"
#include <boost/system/error_code.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AsioEngine: " \
                           << this << " " << __func__ << ": "

namespace librbd {

AsioEngine::AsioEngine(CephContext* cct)
  : m_cct(cct) {
  init();
}

AsioEngine::~AsioEngine() {
  shut_down();
}

void AsioEngine::init() {
  auto thread_count = m_cct->_conf.get_val<uint64_t>("rbd_op_threads");
  m_threads.reserve(thread_count);

  // prevent IO context from exiting if no work is currently scheduled
  m_work_guard.emplace(boost::asio::make_work_guard(m_io_context));

  ldout(m_cct, 5) << "spawning " << thread_count << " threads" << dendl;
  for (auto i = 0U; i < thread_count; i++) {
    m_threads.emplace_back([=] {
      boost::system::error_code ec;
      m_io_context.run(ec);
    });
  }

  m_work_queue = std::make_unique<asio::ContextWQ>(m_io_context);
}

void AsioEngine::shut_down() {
  ldout(m_cct, 5) << "joining threads" << dendl;

  m_work_guard.reset();
  for (auto& thread : m_threads) {
    thread.join();
  }
  m_threads.clear();

  ldout(m_cct, 5) << "done" << dendl;
}

} // namespace librbd
