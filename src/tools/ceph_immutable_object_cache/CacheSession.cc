// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/ceph_context.h"
#include "CacheSession.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheSession: " << this << " " \
                           << __func__ << ": "


namespace ceph {
namespace immutable_obj_cache {

CacheSession::CacheSession(uint64_t session_id, boost::asio::io_service& io_service, ProcessMsg processmsg, CephContext* cct)
    : m_session_id(session_id), m_dm_socket(io_service), process_msg(processmsg), cct(cct)
    {}

CacheSession::~CacheSession(){}

stream_protocol::socket& CacheSession::socket() {
  return m_dm_socket;
}

void CacheSession::start() {
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
void CacheSession::serial_handing_request() {
  boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer, RBDSC_MSG_LEN),
                          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                          boost::bind(&CacheSession::handle_read,
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
void CacheSession::parallel_handing_request() {
  // TODO
}

void CacheSession::handle_read(const boost::system::error_code& error, size_t bytes_transferred) {
  // when recv eof, the most proble is that client side close socket.
  // so, server side need to end handing_request
  if(error == boost::asio::error::eof) {
    ldout(cct, 20) << "session: async_read : " << error.message() << dendl;
    return;
  }

  if(error) {
    ldout(cct, 20) << "session: async_read fails: " << error.message() << dendl;
    assert(0);
  }

  if(bytes_transferred != RBDSC_MSG_LEN) {
    ldout(cct, 20) << "session : request in-complete. "<<dendl;
    assert(0);
  }

  // TODO async_process can increse coding readable.
  // process_msg_callback call handle async_send
  process_msg(m_session_id, std::string(m_buffer, bytes_transferred));
}

void CacheSession::handle_write(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    ldout(cct, 20) << "session: async_write fails: " << error.message() << dendl;
    assert(0);
  }

  if(bytes_transferred != RBDSC_MSG_LEN) {
    ldout(cct, 20) << "session : reply in-complete. "<<dendl;
    assert(0);
  }

  boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer),
                          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                          boost::bind(&CacheSession::handle_read,
                          shared_from_this(),
                          boost::asio::placeholders::error,
                          boost::asio::placeholders::bytes_transferred));

}

void CacheSession::send(std::string msg) {
    boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(msg.c_str(), msg.size()),
        boost::asio::transfer_exactly(RBDSC_MSG_LEN),
        boost::bind(&CacheSession::handle_write,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));

}

} // namespace immutable_obj_cache
} // namespace ceph

