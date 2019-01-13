// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_CACHE_CLIENT_H
#define CEPH_CACHE_CACHE_CLIENT_H

#include <atomic>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "Types.h"
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

  void lookup_object(ObjectCacheRequest* req);
  void send_message();
  void try_send();
  void fault(const int err_type, const boost::system::error_code& err);
  void try_receive();
  void receive_message();
  int register_client(Context* on_finish);

private:
  boost::asio::io_service m_io_service;
  boost::asio::io_service::work m_io_service_work;
  stream_protocol::socket m_dm_socket;
  ClientProcessMsg m_client_process_msg;
  stream_protocol::endpoint m_ep;
  std::shared_ptr<std::thread> m_io_thread;
  std::atomic<bool> m_session_work;
  CephContext* cct;

  bool m_use_dedicated_worker;
  int m_worker_thread_num;
  boost::asio::io_service* m_worker;
  std::vector<std::thread*> m_worker_threads;
  boost::asio::io_service::work* m_worker_io_service_work;

  char* m_header_buffer;
  std::atomic<bool> m_writing;
  std::atomic<bool> m_reading;
  std::atomic<uint64_t> m_sequence_id;
  Mutex m_lock;
  bufferlist m_outcoming_bl;
  Mutex m_map_lock;
  std::map<uint64_t, ObjectCacheRequest*> m_seq_to_req;
};

} // namespace immutable_obj_cache
} // namespace ceph
#endif
