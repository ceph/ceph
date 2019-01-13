// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SESSION_H
#define CEPH_CACHE_SESSION_H

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "Types.h"
#include "SocketCommon.h"

using boost::asio::local::stream_protocol;
using boost::asio::io_service;

namespace ceph {
namespace immutable_obj_cache {

class CacheSession : public std::enable_shared_from_this<CacheSession> {
public:
  CacheSession(uint64_t session_id, io_service& io_service, ProcessMsg process_msg, CephContext* ctx);
  ~CacheSession();
  stream_protocol::socket& socket();
  void close();
  void start();
  void read_request_header();
  void handle_request_header(const boost::system::error_code& err, size_t bytes_transferred);
  void read_request_data(uint64_t data_len);
  void handle_request_data(bufferptr bp, uint64_t data_len,
                          const boost::system::error_code& err, size_t bytes_transferred);
  void process(ObjectCacheRequest* req);
  void fault();
  void send(ObjectCacheRequest* msg);

private:
  uint64_t m_session_id;
  stream_protocol::socket m_dm_socket;
  char* m_head_buffer;
  ProcessMsg m_server_process_msg;
  CephContext* cct;
};

typedef std::shared_ptr<CacheSession> CacheSessionPtr;

} // namespace immutable_obj_cache
} // namespace ceph

#endif
