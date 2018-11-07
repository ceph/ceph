// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_CLIENT_H
#define CEPH_CACHE_CLIENT_H

#include <atomic>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>
#include "librbd/ImageCtx.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "SocketCommon.h"


using boost::asio::local::stream_protocol;

namespace ceph {
namespace immutable_obj_cache {

class CacheClient {
public:
  CacheClient(const std::string& file, CephContext* ceph_ctx);
  ~CacheClient();
  void run();
  bool is_session_work();

  void close();
  int stop();
  int connect();

  int register_volume(std::string pool_name, std::string vol_name, uint64_t vol_size, Context* on_finish);
  int lookup_object(std::string pool_name, std::string vol_name, std::string object_id, Context* on_finish);
  void get_result(Context* on_finish);

private:
  boost::asio::io_service m_io_service;
  boost::asio::io_service::work m_io_service_work;
  stream_protocol::socket m_dm_socket;
  ClientProcessMsg m_client_process_msg;
  stream_protocol::endpoint m_ep;
  char m_recv_buffer[1024];
  std::shared_ptr<std::thread> m_io_thread;

  // atomic modfiy for this variable.
  // thread 1 : asio callback thread modify it.
  // thread 2 : librbd read it.
  std::atomic<bool> m_session_work;
  CephContext* cct;
};

} // namespace immutable_obj_cache
} // namespace ceph
#endif
