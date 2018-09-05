// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CACHE_CONTROLLER_SOCKET_H
#define CACHE_CONTROLLER_SOCKET_H

#include <cstdio>
#include <iostream>
#include <array>
#include <memory>
#include <string>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>
#include "CacheControllerSocketCommon.h"


using boost::asio::local::stream_protocol;

namespace rbd {
namespace cache {

class session : public std::enable_shared_from_this<session> {
public:
  session(uint64_t session_id, boost::asio::io_service& io_service, ProcessMsg processmsg)
    : m_session_id(session_id), m_dm_socket(io_service), process_msg(processmsg) {}

  stream_protocol::socket& socket() {
    return m_dm_socket;
  }

  void start() {
    if(true) {
      serial_handing_request();
    } else {
      parallel_handing_request();
    }
  }
  // flow:
  //
  // recv request --> process request --> reply ack
  //   |                                      |
  //   --------------<-------------------------
  void serial_handing_request() {
    boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer, RBDSC_MSG_LEN),
                            boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                            boost::bind(&session::handle_read,
                                        shared_from_this(),
                                        boost::asio::placeholders::error,
                                        boost::asio::placeholders::bytes_transferred));
  }

  // flow :
  //
  //              --> thread 1: process request
  // recv request --> thread 2: process request --> reply ack
  //              --> thread n: process request
  //
  void parallel_handing_request() {
    // TODO
  }

private:

  void handle_read(const boost::system::error_code& error, size_t bytes_transferred) {
    // when recv eof, the most proble is that client side close socket.
    // so, server side need to end handing_request
    if(error == boost::asio::error::eof) {
      std::cout<<"session: async_read : " << error.message() << std::endl;
      return;
    }

    if(error) {
      std::cout<<"session: async_read fails: " << error.message() << std::endl;
      assert(0);
    }

    if(bytes_transferred != RBDSC_MSG_LEN) {
      std::cout<<"session : request in-complete. "<<std::endl;
      assert(0);
    }

    // TODO async_process can increse coding readable.
    // process_msg_callback call handle async_send
    process_msg(m_session_id, std::string(m_buffer, bytes_transferred));
  }

  void handle_write(const boost::system::error_code& error, size_t bytes_transferred) {
    if (error) {
      std::cout<<"session: async_write fails: " << error.message() << std::endl;
      assert(0);
    }

    if(bytes_transferred != RBDSC_MSG_LEN) {
      std::cout<<"session : reply in-complete. "<<std::endl;
      assert(0);
    }

    boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer),
                            boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                            boost::bind(&session::handle_read,
                            shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));

  }

public:
  void send(std::string msg) {
      boost::asio::async_write(m_dm_socket,
          boost::asio::buffer(msg.c_str(), msg.size()),
          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
          boost::bind(&session::handle_write,
                      shared_from_this(),
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred));

  }

private:
  uint64_t m_session_id;
  stream_protocol::socket m_dm_socket;
  ProcessMsg process_msg;

  // Buffer used to store data received from the client.
  //std::array<char, 1024> data_;
  char m_buffer[1024];
};

typedef std::shared_ptr<session> session_ptr;

class CacheServer {
public:
  CacheServer(const std::string& file, ProcessMsg processmsg, CephContext* cct)
    : m_cct(cct), m_server_process_msg(processmsg),
      m_local_path(file),
      m_acceptor(m_io_service)
  {}

  void run() {
    bool ret;
    ret = start_accept();
    if(!ret) {
      return;
    }
    m_io_service.run();
  }

  // TODO : use callback to replace this function.
  void send(uint64_t session_id, std::string msg) {
    auto it = m_session_map.find(session_id);
    if (it != m_session_map.end()) {
      it->second->send(msg);
    } else {
      // TODO : why don't find existing session id ?
      std::cout<<"don't find session id..."<<std::endl;
      assert(0);
    }
  }

private:
  // when creating one acceptor, can control every step in this way.
  bool start_accept() {
    boost::system::error_code ec;
    m_acceptor.open(m_local_path.protocol(), ec);
    if(ec) {
      std::cout << "m_acceptor open fails: " << ec.message() << std::endl;
      return false;
    }

    // TODO control acceptor attribute.

    m_acceptor.bind(m_local_path, ec);
    if(ec) {
      std::cout << "m_acceptor bind fails: " << ec.message() << std::endl;
      return false;
    }

    m_acceptor.listen(boost::asio::socket_base::max_connections, ec);
    if(ec) {
      std::cout << "m_acceptor listen fails: " << ec.message() << std::endl;
      return false;
    }

    accept();
    return true;
  }

  void accept() {
    session_ptr new_session(new session(m_session_id, m_io_service, m_server_process_msg));
    m_acceptor.async_accept(new_session->socket(),
        boost::bind(&CacheServer::handle_accept, this, new_session,
          boost::asio::placeholders::error));
  }

 void handle_accept(session_ptr new_session, const boost::system::error_code& error) {
    //TODO(): open librbd snap ... yuan

    if(error) {
      std::cout << "async accept fails : " << error.message() << std::endl;
      assert(0); // TODO
    }

      // must put session into m_session_map at the front of session.start()
    m_session_map.emplace(m_session_id, new_session);
    // TODO : session setting
    new_session->start();
    m_session_id++;

    // lanuch next accept
    accept();
  }

private:
  CephContext* m_cct;
  boost::asio::io_service m_io_service; // TODO wrapper it.
  ProcessMsg m_server_process_msg;
  stream_protocol::endpoint m_local_path;
  stream_protocol::acceptor m_acceptor;
  uint64_t m_session_id = 1;
  std::map<uint64_t, session_ptr> m_session_map;
};

} // namespace cache
} // namespace rbd

#endif
