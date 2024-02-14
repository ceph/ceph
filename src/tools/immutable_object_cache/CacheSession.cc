// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/asio/error.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind/bind.hpp>
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

CacheSession::CacheSession(io_context& io_service,
                           ProcessMsg processmsg,
                           CephContext* cct)
    : m_dm_socket(io_service),
      m_server_process_msg(processmsg), m_cct(cct) {
  m_bp_header = buffer::create(get_header_size());
}

CacheSession::~CacheSession() {
  close();
}

stream_protocol::socket& CacheSession::socket() {
  return m_dm_socket;
}

void CacheSession::set_client_version(const std::string &version) {
  m_client_version = version;
}

const std::string &CacheSession::client_version() const {
  return m_client_version;
}

void CacheSession::close() {
  if (m_dm_socket.is_open()) {
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if (close_ec) {
       ldout(m_cct, 20) << "close: " << close_ec.message() << dendl;
    }
  }
}

void CacheSession::start() {
  read_request_header();
}

void CacheSession::read_request_header() {
  ldout(m_cct, 20) << dendl;
  boost::asio::async_read(m_dm_socket,
    boost::asio::buffer(m_bp_header.c_str(), get_header_size()),
    boost::asio::transfer_exactly(get_header_size()),
    boost::bind(&CacheSession::handle_request_header,
     shared_from_this(), boost::asio::placeholders::error,
     boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_request_header(const boost::system::error_code& err,
                                         size_t bytes_transferred) {
  ldout(m_cct, 20) << dendl;
  if (err || bytes_transferred != get_header_size()) {
    fault(err);
    return;
  }

  read_request_data(get_data_len(m_bp_header.c_str()));
}

void CacheSession::read_request_data(uint64_t data_len) {
  ldout(m_cct, 20) << dendl;
  bufferptr bp_data(buffer::create(data_len));
  boost::asio::async_read(m_dm_socket,
    boost::asio::buffer(bp_data.c_str(), bp_data.length()),
    boost::asio::transfer_exactly(data_len),
    boost::bind(&CacheSession::handle_request_data,
      shared_from_this(), bp_data, data_len,
      boost::asio::placeholders::error,
      boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_request_data(bufferptr bp, uint64_t data_len,
                                      const boost::system::error_code& err,
                                      size_t bytes_transferred) {
  ldout(m_cct, 20) << dendl;
  if (err || bytes_transferred != data_len) {
    fault(err);
    return;
  }

  bufferlist bl_data;

  bl_data.append(m_bp_header);
  bl_data.append(std::move(bp));

  ObjectCacheRequest* req = decode_object_cache_request(bl_data);

  process(req);
  delete req;
  read_request_header();
}

void CacheSession::process(ObjectCacheRequest* req) {
  ldout(m_cct, 20) << dendl;
  m_server_process_msg(this, req);
}

void CacheSession::send(ObjectCacheRequest* reply) {
  ldout(m_cct, 20) << dendl;
  bufferlist bl;
  reply->encode();
  bl.append(reply->get_payload_bufferlist());

  boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(bl.c_str(), bl.length()),
        boost::asio::transfer_exactly(bl.length()),
        [this, bl, reply](const boost::system::error_code& err,
          size_t bytes_transferred) {
          delete reply;
          if (err || bytes_transferred != bl.length()) {
            fault(err);
            return;
          }
        });
}

void CacheSession::fault(const boost::system::error_code& ec) {
  ldout(m_cct, 20) << "session fault : " << ec.message() << dendl;
}

}  // namespace immutable_obj_cache
}  // namespace ceph
