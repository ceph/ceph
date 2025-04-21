// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SESSION_H
#define CEPH_CACHE_SESSION_H

#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>

#include "Types.h"
#include "SocketCommon.h"

namespace ceph {
namespace immutable_obj_cache {

using boost::asio::local::stream_protocol;
using boost::asio::io_context;

class CacheSession : public std::enable_shared_from_this<CacheSession> {
 public:
  CacheSession(io_context& io_service, ProcessMsg process_msg,
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

  void set_client_version(const std::string &version);
  const std::string &client_version() const;

 private:
  stream_protocol::socket m_dm_socket;
  ProcessMsg m_server_process_msg;
  CephContext* m_cct;

  std::string m_client_version;

  bufferptr m_bp_header;
};

typedef std::shared_ptr<CacheSession> CacheSessionPtr;

}  // namespace immutable_obj_cache
}  // namespace ceph

#endif  // CEPH_CACHE_SESSION_H
