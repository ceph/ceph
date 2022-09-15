// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/write.hpp>

#include "rgw_asio_client.h"
#include "rgw_perf_counters.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw::asio;

ClientIO::ClientIO(parser_type& parser, bool is_ssl,
                   const endpoint_type& local_endpoint,
                   const endpoint_type& remote_endpoint)
  : parser(parser), response(), serializer(response), is_ssl(is_ssl),
    local_endpoint(local_endpoint),
    remote_endpoint(remote_endpoint)
{
  response.version(11);
  response.keep_alive(parser.keep_alive());
}

ClientIO::~ClientIO() = default;

int ClientIO::init_env(CephContext *cct)
{
  env.init(cct);

  perfcounter->inc(l_rgw_qlen);
  perfcounter->inc(l_rgw_qactive);

  const auto& request = parser.get();
  const auto& headers = request;
  for (auto header = headers.begin(); header != headers.end(); ++header) {
    const auto& field = header->name(); // enum type for known headers
    const auto& name = header->name_string();
    const auto& value = header->value();

    if (field == beast::http::field::content_length) {
      env.set("CONTENT_LENGTH", value.to_string());
      continue;
    }
    if (field == beast::http::field::content_type) {
      env.set("CONTENT_TYPE", value.to_string());
      continue;
    }

    static const std::string_view HTTP_{"HTTP_"};

    char buf[name.size() + HTTP_.size() + 1];
    auto dest = std::copy(std::begin(HTTP_), std::end(HTTP_), buf);
    for (auto src = name.begin(); src != name.end(); ++src, ++dest) {
      if (*src == '-') {
        *dest = '_';
      } else if (*src == '_') {
        *dest = '-';
      } else {
        *dest = std::toupper(*src);
      }
    }
    *dest = '\0';

    env.set(buf, value.to_string());
  }

  int major = request.version() / 10;
  int minor = request.version() % 10;
  env.set("HTTP_VERSION", std::to_string(major) + '.' + std::to_string(minor));

  env.set("REQUEST_METHOD", request.method_string().to_string());

  // split uri from query
  auto uri = request.target();
  auto pos = uri.find('?');
  if (pos != uri.npos) {
    auto query = uri.substr(pos + 1);
    env.set("QUERY_STRING", query.to_string());
    uri = uri.substr(0, pos);
  }
  env.set("SCRIPT_URI", uri.to_string());

  env.set("REQUEST_URI", request.target().to_string());

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", local_endpoint.port());
  env.set("SERVER_PORT", port_buf);
  if (is_ssl) {
    env.set("SERVER_PORT_SECURE", port_buf);
  }
  env.set("REMOTE_ADDR", remote_endpoint.address().to_string());
  // TODO: set REMOTE_USER if authenticated
  return 0;
}

size_t ClientIO::send_status(int status, const char* status_name)
{
  response.result(status);
  return 0; // accounted for later in complete_header()
}

size_t ClientIO::send_header(const std::string_view& name,
                             const std::string_view& value)
{
  response.insert(beast::string_view{name.data(), name.size()},
                  beast::string_view{value.data(), value.size()});
  return 0; // accounted for later in complete_header()
}

size_t ClientIO::send_content_length(uint64_t len)
{
  response.content_length(len);
  return 0; // accounted for later in complete_header()
}

size_t ClientIO::send_chunked_transfer_encoding()
{
  response.chunked(true);
  return 0; // accounted for later in complete_header()
}
