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
  void lookup_object(std::string pool_nspace, uint64_t pool_id,
                     uint64_t snap_id, std::string oid,
                     GenContext<ObjectCacheRequest*>* on_finish);
  int register_client(Context* on_finish);

 private:
  void send_message();
  void try_send();
  void fault(const int err_type, const boost::system::error_code& err);
  void try_receive();
  void receive_message();
  void process(ObjectCacheRequest* reply, uint64_t seq_id);
  void read_reply_header();
  void handle_reply_header(bufferptr bp_head,
                           const boost::system::error_code& ec,
                           size_t bytes_transferred);
  void read_reply_data(bufferptr&& bp_head, bufferptr&& bp_data,
                       const uint64_t data_len);
  void handle_reply_data(bufferptr bp_head, bufferptr bp_data,
                        const uint64_t data_len,
                        const boost::system::error_code& ec,
                        size_t bytes_transferred);

 private:
  CephContext* m_cct;
  boost::asio::io_service m_io_service;
  boost::asio::io_service::work m_io_service_work;
  stream_protocol::socket m_dm_socket;
  stream_protocol::endpoint m_ep;
  std::shared_ptr<std::thread> m_io_thread;
  std::atomic<bool> m_session_work;

  uint64_t m_worker_thread_num;
  boost::asio::io_service* m_worker;
  std::vector<std::thread*> m_worker_threads;
  boost::asio::io_service::work* m_worker_io_service_work;

  std::atomic<bool> m_writing;
  std::atomic<bool> m_reading;
  std::atomic<uint64_t> m_sequence_id;
  Mutex m_lock;
  std::map<uint64_t, ObjectCacheRequest*> m_seq_to_req;
  bufferlist m_outcoming_bl;
  bufferptr m_bp_header;
};

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif  // CEPH_CACHE_CACHE_CLIENT_H
