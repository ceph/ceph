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

CacheSession::~CacheSession() {
  close();
}

stream_protocol::socket& CacheSession::socket() {
  return m_dm_socket;
}

void CacheSession::close() {
  if(m_dm_socket.is_open()) {
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if(close_ec) {
       ldout(cct, 20) << "close: " << close_ec.message() << dendl;
    }
  }
}

void CacheSession::start() {
  handing_request();
}

void CacheSession::handing_request() {
  boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer, RBDSC_MSG_LEN),
                          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                          boost::bind(&CacheSession::handle_read,
                                      shared_from_this(),
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_read(const boost::system::error_code& err, size_t bytes_transferred) {
  if (err == boost::asio::error::eof ||
     err == boost::asio::error::connection_reset ||
     err == boost::asio::error::operation_aborted ||
     err == boost::asio::error::bad_descriptor) {
    ldout(cct, 20) << "fail to handle read : " << err.message() << dendl;
    close();
    return;
  }

  if(err) {
    ldout(cct, 1) << "faile to handle read: " << err.message() << dendl;
    return;
  }

  if(bytes_transferred != RBDSC_MSG_LEN) {
    ldout(cct, 1) << "incomplete read" << dendl;
    return;
  }

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
