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
#include <boost/algorithm/string.hpp>
#include "CacheControllerSocketCommon.h"


using boost::asio::local::stream_protocol;

class session : public std::enable_shared_from_this<session> {
public:
  session(uint64_t session_id, boost::asio::io_service& io_service, ProcessMsg processmsg)
    : session_id(session_id), socket_(io_service), process_msg(processmsg) {}

  stream_protocol::socket& socket() {
    return socket_;
  }

  void start() {

    boost::asio::async_read(socket_, boost::asio::buffer(data_),
                            boost::asio::transfer_exactly(544),
                            boost::bind(&session::handle_read,
                            shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));

  }

  void handle_read(const boost::system::error_code& error, size_t bytes_transferred) {

    if (!error) {
     
      process_msg(session_id, std::string(data_, bytes_transferred));

    }
  }

  void handle_write(const boost::system::error_code& error) {
    if (!error) {
      socket_.async_read_some(boost::asio::buffer(data_),
          boost::bind(&session::handle_read,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
    }
  }

  void send(std::string msg) {

      boost::asio::async_write(socket_,
          boost::asio::buffer(msg.c_str(), msg.size()),
          boost::bind(&session::handle_write,
            shared_from_this(),
            boost::asio::placeholders::error));

  }

private:
  uint64_t session_id;
  stream_protocol::socket socket_;
  ProcessMsg process_msg;

  // Buffer used to store data received from the client.
  //std::array<char, 1024> data_;
  char data_[1024];
};

typedef std::shared_ptr<session> session_ptr;

class CacheServer {
public:
  CacheServer(boost::asio::io_service& io_service,
         const std::string& file, ProcessMsg processmsg)
    : io_service_(io_service),
      server_process_msg(processmsg),
      acceptor_(io_service, stream_protocol::endpoint(file))
  {
    session_ptr new_session(new session(session_id, io_service_, server_process_msg));
    acceptor_.async_accept(new_session->socket(),
        boost::bind(&CacheServer::handle_accept, this, new_session,
          boost::asio::placeholders::error));
  }

  void handle_accept(session_ptr new_session,
      const boost::system::error_code& error)
  {
    //TODO(): open librbd snap
    if (!error) {
      new_session->start();
      session_map.emplace(session_id, new_session);
      session_id++;
      new_session.reset(new session(session_id, io_service_, server_process_msg));
      acceptor_.async_accept(new_session->socket(),
          boost::bind(&CacheServer::handle_accept, this, new_session,
            boost::asio::placeholders::error));
    }
  }

  void send(uint64_t session_id, std::string msg) {
    auto it = session_map.find(session_id);
    if (it != session_map.end()) {
      it->second->send(msg);
    }
  }

private:
  boost::asio::io_service& io_service_;
  ProcessMsg server_process_msg;
  stream_protocol::acceptor acceptor_;
  uint64_t session_id = 1;
  std::map<uint64_t, session_ptr> session_map;
};

#endif
