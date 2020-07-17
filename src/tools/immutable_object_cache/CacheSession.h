// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SESSION_H
#define CEPH_CACHE_SESSION_H

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
  CacheSession(io_service& io_service, ProcessMsg process_msg,
                CephContext* ctx);
  ~CacheSession();
  stream_protocol::socket& socket();
  void close();
  void start();
  void read_request_header();
  void handle_request_header(const boost::system::error_code& err,
                             size_t bytes_transferred);
  void read_request_data(uint64_t data_len);
  void handle_request_data(bufferptr bp, uint64_t data_len,
                          const boost::system::error_code& err,
                          size_t bytes_transferred);
  void process(ObjectCacheRequest* req);
  void fault(const boost::system::error_code& ec);
  void send(ObjectCacheRequest* msg);

 private:
  stream_protocol::socket m_dm_socket;
  ProcessMsg m_server_process_msg;
  CephContext* m_cct;

  bufferptr m_bp_header;
};

typedef std::shared_ptr<CacheSession> CacheSessionPtr;

}  // namespace immutable_obj_cache
}  // namespace ceph

#endif  // CEPH_CACHE_SESSION_H
