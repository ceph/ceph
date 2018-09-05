// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CACHE_CONTROLLER_SOCKET_CLIENT_H
#define CACHE_CONTROLLER_SOCKET_CLIENT_H

#include <atomic>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>
#include "librbd/ImageCtx.h"
#include "include/assert.h"
#include "include/Context.h"
#include "CacheControllerSocketCommon.h"


using boost::asio::local::stream_protocol;

namespace rbd {
namespace cache {

class CacheClient {
public:
  CacheClient(const std::string& file, ClientProcessMsg processmsg, CephContext* ceph_ctx)
    : m_io_service_work(m_io_service),
      m_dm_socket(m_io_service),
      m_client_process_msg(processmsg),
      m_ep(stream_protocol::endpoint(file)),
      m_session_work(false),
      cct(ceph_ctx)
  {
     // TODO wrapper io_service
     std::thread thd([this](){
                      m_io_service.run();});
     thd.detach();
  }

  void run(){
  }

  bool is_session_work() {
    return m_session_work.load() == true;
  }

  // just when error occur, call this method.
  void close() {
    m_session_work.store(false);
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if(close_ec) {
       std::cout << "close: " << close_ec.message() << std::endl;
    }
    std::cout << "session don't work, later all request will be dispatched to rados layer" << std::endl;
  }

  int connect() {
    boost::system::error_code ec;
    m_dm_socket.connect(m_ep, ec);
    if(ec) {
      if(ec == boost::asio::error::connection_refused) {
        std::cout << ec.message() << " : maybe rbd-cache Controller don't startup. "
                  << "Now data will be read from ceph cluster " << std::endl;
      } else {
        std::cout << "connect: " << ec.message() << std::endl;
      }

      if(m_dm_socket.is_open()) {
        // Set to indicate what error occurred, if any.
        // Note that, even if the function indicates an error,
        // the underlying descriptor is closed.
        boost::system::error_code close_ec;
        m_dm_socket.close(close_ec);
        if(close_ec) {
          std::cout << "close: " << close_ec.message() << std::endl;
        }
      }
      return -1;
    }

    std::cout<<"connect success"<<std::endl;

    return 0;
  }

  int register_volume(std::string pool_name, std::string vol_name, uint64_t vol_size) {
    // cache controller will init layout
    rbdsc_req_type_t *message = new rbdsc_req_type_t();
    message->type = RBDSC_REGISTER;
    memcpy(message->pool_name, pool_name.c_str(), pool_name.size());
    memcpy(message->vol_name, vol_name.c_str(), vol_name.size());
    message->vol_size = vol_size;
    message->offset = 0;
    message->length = 0;

    uint64_t ret;
    boost::system::error_code ec;

    ret = boost::asio::write(m_dm_socket, boost::asio::buffer((char*)message, message->size()), ec);
    if(ec) {
      std::cout << "write fails : " << ec.message() << std::endl;
      return -1;
    }

    if(ret != message->size()) {
      std::cout << "write fails : ret != send_bytes "<< std::endl;
      return -1;
    }

    // hard code TODO
    ret = boost::asio::read(m_dm_socket, boost::asio::buffer(m_recv_buffer, RBDSC_MSG_LEN), ec);
    if(ec == boost::asio::error::eof) {
      std::cout<< "recv eof"<<std::endl;
      return -1;
    }

    if(ec) {
      std::cout << "write fails : " << ec.message() << std::endl;
      return -1;
    }

    if(ret != RBDSC_MSG_LEN) {
      std::cout << "write fails : ret != receive bytes " << std::endl;
      return -1;
    }

    m_client_process_msg(std::string(m_recv_buffer, ret));

    delete message;

    std::cout << "register volume success" << std::endl;

    // TODO
    m_session_work.store(true);

    return 0;
  }

  // if occur any error, we just return false. Then read from rados.
  int lookup_object(std::string pool_name, std::string vol_name, std::string object_id, Context* on_finish) {
    rbdsc_req_type_t *message = new rbdsc_req_type_t();
    message->type = RBDSC_READ;
    memcpy(message->pool_name, pool_name.c_str(), pool_name.size());
    memcpy(message->vol_name, object_id.c_str(), object_id.size());
    message->vol_size = 0;
    message->offset = 0;
    message->length = 0;

    boost::asio::async_write(m_dm_socket,
                             boost::asio::buffer((char*)message, message->size()),
                             boost::asio::transfer_exactly(RBDSC_MSG_LEN),
        [this, on_finish, message](const boost::system::error_code& err, size_t cb) {
          delete message;
          if(err) {
            std::cout<< "lookup_object: async_write fails." << err.message() << std::endl;
            close();
            on_finish->complete(false);
            return;
          }
          if(cb != RBDSC_MSG_LEN) {
            std::cout<< "lookup_object: async_write fails. in-complete request" <<std::endl;
            close();
            on_finish->complete(false);
            return;
          }
          get_result(on_finish);
    });

    return 0;
  }

  void get_result(Context* on_finish) {
    boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_recv_buffer, RBDSC_MSG_LEN),
                            boost::asio::transfer_exactly(RBDSC_MSG_LEN),
        [this, on_finish](const boost::system::error_code& err, size_t cb) {
          if(err == boost::asio::error::eof) {
            std::cout<<"get_result: ack is EOF." << std::endl;
            close();
            on_finish->complete(false);
            return;
          }
          if(err) {
            std::cout<< "get_result: async_read fails:" << err.message() << std::endl;
            close();
            on_finish->complete(false); // TODO replace this assert with some metohds.
            return;
          }
          if (cb != RBDSC_MSG_LEN) {
            close();
            std::cout << "get_result: in-complete ack." << std::endl;
	    on_finish->complete(false); // TODO: replace this assert with some methods.
          }

	  rbdsc_req_type_t *io_ctx = (rbdsc_req_type_t*)(m_recv_buffer);

          // TODO: re-occur yuan's bug
          if(io_ctx->type == RBDSC_READ) {
            std::cout << "get rbdsc_read... " << std::endl;
            assert(0);
          }

          if (io_ctx->type == RBDSC_READ_REPLY) {
	    on_finish->complete(true);
            return;
          } else {
	    on_finish->complete(false);
            return;
          }
    });
  }

private:
  boost::asio::io_service m_io_service;
  boost::asio::io_service::work m_io_service_work;
  stream_protocol::socket m_dm_socket;
  ClientProcessMsg m_client_process_msg;
  stream_protocol::endpoint m_ep;
  char m_recv_buffer[1024];

  // atomic modfiy for this variable.
  // thread 1 : asio callback thread modify it.
  // thread 2 : librbd read it.
  std::atomic<bool> m_session_work;
  CephContext* cct;
};

} // namespace cache
} // namespace rbd
#endif
