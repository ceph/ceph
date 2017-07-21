// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_ASIO_CLIENT_H
#define RGW_ASIO_CLIENT_H

#include <boost/asio/ip/tcp.hpp>
#include <beast/http/message.hpp>
#include <beast/http/message_parser.hpp>
#include <beast/core/flat_streambuf.hpp>
#include "include/assert.h"

#include "rgw_client_io.h"

namespace rgw {
namespace asio {

/// streaming message body interface
struct streaming_body {
  using value_type = boost::asio::mutable_buffer;

  class reader {
    value_type& buffer;
   public:
    using mutable_buffers_type = boost::asio::mutable_buffers_1;

    static const bool is_direct{true}; // reads directly into user buffer

    template<bool isRequest, class Fields>
    explicit reader(beast::http::message<isRequest, streaming_body, Fields>& m)
      : buffer(m.body)
    {}

    void init() {}
    void init(uint64_t content_length) {}
    void finish() {}

    mutable_buffers_type prepare(size_t n) {
      n = std::min(n, boost::asio::buffer_size(buffer));
      auto position = boost::asio::buffer_cast<char*>(buffer);
      return {position, n};
    }

    void commit(size_t n) {
      buffer = buffer + n;
    }
  };
};

using header_type = beast::http::fields;
using parser_type = beast::http::message_parser<true, streaming_body, header_type>;

class ClientIO : public io::RestfulClient,
                 public io::BuffererSink {
 private:
  using tcp = boost::asio::ip::tcp;
  tcp::socket& socket;
  parser_type& parser;
  beast::flat_streambuf& buffer; //< parse buffer

  bool conn_keepalive{false};
  bool conn_close{false};
  RGWEnv env;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t write_data(const char *buf, size_t len) override;
  size_t read_data(char *buf, size_t max);

 public:
  ClientIO(tcp::socket& socket, parser_type& parser,
           beast::flat_streambuf& buffer);
  ~ClientIO() override;

  bool get_conn_close() const { return conn_close; }

  void init_env(CephContext *cct) override;
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
