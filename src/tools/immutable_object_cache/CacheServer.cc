// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind/bind.hpp>
#include "common/debug.h"
#include "common/ceph_context.h"
#include "CacheServer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheServer: " << this << " " \
                           << __func__ << ": "


namespace ceph {
namespace immutable_obj_cache {

CacheServer::CacheServer(CephContext* cct, const std::string& file,
                         ProcessMsg processmsg)
  : cct(cct), m_server_process_msg(processmsg),
    m_local_path(file), m_acceptor(m_io_service) {}

CacheServer::~CacheServer() {
  stop();
}

int CacheServer::run() {
  ldout(cct, 20) << dendl;

  int ret = start_accept();
  if (ret != 0) {
    return ret;
  }

  boost::system::error_code ec;
  ret = m_io_service.run(ec);
  if (ec) {
    ldout(cct, 1) << "m_io_service run fails: " << ec.message() << dendl;
    return -1;
  }
  return 0;
}

int CacheServer::stop() {
  m_io_service.stop();
  return 0;
}

int CacheServer::start_accept() {
  ldout(cct, 20) << dendl;

  boost::system::error_code ec;
  m_acceptor.open(m_local_path.protocol(), ec);
  if (ec) {
    lderr(cct) << "failed to open domain socket: " << ec.message() << dendl;
    return -ec.value();
  }

  m_acceptor.bind(m_local_path, ec);
  if (ec) {
    lderr(cct) << "failed to bind to domain socket '"
               << m_local_path << "': " << ec.message() << dendl;
    return -ec.value();
  }

  m_acceptor.listen(boost::asio::socket_base::max_connections, ec);
  if (ec) {
    lderr(cct) << "failed to listen on domain socket: " << ec.message()
               << dendl;
    return -ec.value();
  }

  accept();
  return 0;
}

void CacheServer::accept() {
  CacheSessionPtr new_session = nullptr;

  new_session.reset(new CacheSession(m_io_service,
                    m_server_process_msg, cct));

  m_acceptor.async_accept(new_session->socket(),
      boost::bind(&CacheServer::handle_accept, this, new_session,
        boost::asio::placeholders::error));
}

void CacheServer::handle_accept(CacheSessionPtr new_session,
                                const boost::system::error_code& error) {
  ldout(cct, 20) << dendl;
  if (error) {
    // operation_absort
    lderr(cct) << "async accept fails : " << error.message() << dendl;
    return;
  }

  // TODO(dehao) : session setting
  new_session->start();

  // lanuch next accept
  accept();
}

}  // namespace immutable_obj_cache
}  // namespace ceph
