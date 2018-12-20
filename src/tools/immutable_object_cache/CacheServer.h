// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SERVER_H
#define CEPH_CACHE_SERVER_H

#include <cstdio>
#include <iostream>
#include <array>
#include <memory>
#include <string>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>

#include "include/ceph_assert.h"
#include "SocketCommon.h"
#include "CacheSession.h"


using boost::asio::local::stream_protocol;

namespace ceph {
namespace immutable_obj_cache {

class CacheServer {

 public:
  CacheServer(CephContext* cct, const std::string& file, ProcessMsg processmsg);
  ~CacheServer();

  int run();
  void send(uint64_t session_id, std::string msg);
  int start_accept();
  int stop();

 private:
  void accept();
  void handle_accept(CacheSessionPtr new_session, const boost::system::error_code& error);

 private:
  CephContext* cct;
  boost::asio::io_service m_io_service; // TODO wrapper it.
  ProcessMsg m_server_process_msg;
  stream_protocol::endpoint m_local_path;
  stream_protocol::acceptor m_acceptor;
  uint64_t m_session_id = 1;
  std::map<uint64_t, CacheSessionPtr> m_session_map;
};

} // namespace immutable_obj_cache
} // namespace ceph

#endif
