// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_ASIO_CLIENT_H
#define RGW_ASIO_CLIENT_H

#include <boost/asio/ip/tcp.hpp>
#include <beast/http/body_type.hpp>
#include <beast/http/concepts.hpp>
#include <beast/http/message_v1.hpp>
#include "include/assert.h"

#include "rgw_client_io.h"

// bufferlist to represent the message body
class RGWBufferlistBody {
 public:
  using value_type = ceph::bufferlist;

  class reader;
  class writer;

  template <bool isRequest, typename Headers>
  using message_type = beast::http::message<isRequest, RGWBufferlistBody,
                                            Headers>;
};

class RGWAsioClientIO : public rgw::io::RestfulClient,
                        public rgw::io::BuffererSink {
  using tcp = boost::asio::ip::tcp;
  tcp::socket socket;

  using body_type = RGWBufferlistBody;
  using request_type = beast::http::request_v1<body_type>;
  request_type request;

  bufferlist::const_iterator body_iter;

  bool conn_keepalive{false};
  bool conn_close{false};
  RGWEnv env;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t write_data(const char *buf, size_t len) override;
  size_t read_data(char *buf, size_t max);

 public:
  RGWAsioClientIO(tcp::socket&& socket, request_type&& request);
  ~RGWAsioClientIO();

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

// used by beast::http::read() to read the body into a bufferlist
class RGWBufferlistBody::reader {
  value_type& bl;
 public:
  template<bool isRequest, typename Headers>
  explicit reader(message_type<isRequest, Headers>& m) : bl(m.body) {}

  void write(const char* data, size_t size, boost::system::error_code&) {
    bl.append(data, size);
  }
};

// used by beast::http::write() to write the buffered body
class RGWBufferlistBody::writer {
  const value_type& bl;
 public:
  template<bool isRequest, typename Headers>
  explicit writer(const message_type<isRequest, Headers>& msg)
    : bl(msg.body) {}

  void init(boost::system::error_code& ec) {}
  uint64_t content_length() const { return bl.length(); }

  template<typename Write>
  boost::tribool operator()(beast::http::resume_context&&,
                            boost::system::error_code&, Write&& write) {
    // translate from bufferlist to a ConstBufferSequence for beast
    std::vector<boost::asio::const_buffer> buffers;
    buffers.reserve(bl.get_num_buffers());
    for (auto& ptr : bl.buffers()) {
      buffers.emplace_back(ptr.c_str(), ptr.length());
    }
    write(buffers);
    return true;
  }
};
static_assert(beast::http::is_ReadableBody<RGWBufferlistBody>{},
              "RGWBufferlistBody does not satisfy ReadableBody");
static_assert(beast::http::is_WritableBody<RGWBufferlistBody>{},
              "RGWBufferlistBody does not satisfy WritableBody");

#endif // RGW_ASIO_CLIENT_H
