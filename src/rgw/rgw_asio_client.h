// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_ASIO_CLIENT_H
#define RGW_ASIO_CLIENT_H

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include "include/assert.h"

#include "rgw_client_io.h"

namespace rgw {
namespace asio {

namespace beast = boost::beast;
using parser_type = beast::http::request_parser<beast::http::buffer_body>;

class ClientIO : public io::RestfulClient,
                 public io::BuffererSink {
 private:
  using tcp = boost::asio::ip::tcp;
  tcp::socket& socket;
  parser_type& parser;
  beast::flat_buffer& buffer; //< parse buffer

  RGWEnv env;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t write_data(const char *buf, size_t len) override;
  size_t read_data(char *buf, size_t max);

 public:
  ClientIO(tcp::socket& socket, parser_type& parser,
           beast::flat_buffer& buffer);
  ~ClientIO() override;

  int init_env(CephContext *cct) override;
  size_t complete_request() override;
  void flush() override;
  size_t send_status(int status, const char *status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const boost::string_ref& name,
                     const boost::string_ref& value) override;
  size_t send_content_length(uint64_t len) override;
  size_t complete_header() override;

  size_t recv_body(char* buf, size_t max) override {
    return read_data(buf, max);
  }

  size_t send_body(const char* buf, size_t len) override {
    return write_data(buf, len);
  }

  RGWEnv& get_env() noexcept override {
    return env;
  }
};

} // namespace asio
} // namespace rgw

#endif // RGW_ASIO_CLIENT_H
