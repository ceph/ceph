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
  : parser(parser), is_ssl(is_ssl),
    local_endpoint(local_endpoint),
    remote_endpoint(remote_endpoint),
    txbuf(*this)
{
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

size_t ClientIO::complete_request()
{
  perfcounter->inc(l_rgw_qlen, -1);
  perfcounter->inc(l_rgw_qactive, -1);
  return 0;
}

void ClientIO::flush()
{
  txbuf.pubsync();
}

size_t ClientIO::send_status(int status, const char* status_name)
{
  static constexpr size_t STATUS_BUF_SIZE = 128;

  char statusbuf[STATUS_BUF_SIZE];
  const auto statuslen = snprintf(statusbuf, sizeof(statusbuf),
                                  "HTTP/1.1 %d %s\r\n", status, status_name);

  return txbuf.sputn(statusbuf, statuslen);
}

size_t ClientIO::send_100_continue()
{
  const char HTTTP_100_CONTINUE[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";
  const size_t sent = txbuf.sputn(HTTTP_100_CONTINUE,
                                  sizeof(HTTTP_100_CONTINUE) - 1);
  flush();
  return sent;
}

static constexpr size_t TIME_BUF_SIZE = 128;
static size_t dump_date_header(char (&timestr)[TIME_BUF_SIZE])
{
  const time_t gtime = time(nullptr);
  struct tm result;
  struct tm const * const tmp = gmtime_r(&gtime, &result);
  if (tmp == nullptr) {
    return 0;
  }
  return strftime(timestr, sizeof(timestr),
                  "Date: %a, %d %b %Y %H:%M:%S %Z\r\n", tmp);
}

size_t ClientIO::complete_header()
{
  size_t sent = 0;

  char timestr[TIME_BUF_SIZE];
  if (dump_date_header(timestr)) {
    sent += txbuf.sputn(timestr, strlen(timestr));
  }

  if (parser.keep_alive()) {
    constexpr char CONN_KEEP_ALIVE[] = "Connection: Keep-Alive\r\n";
    sent += txbuf.sputn(CONN_KEEP_ALIVE, sizeof(CONN_KEEP_ALIVE) - 1);
  } else {
    constexpr char CONN_KEEP_CLOSE[] = "Connection: close\r\n";
    sent += txbuf.sputn(CONN_KEEP_CLOSE, sizeof(CONN_KEEP_CLOSE) - 1);
  }

  constexpr char HEADER_END[] = "\r\n";
  sent += txbuf.sputn(HEADER_END, sizeof(HEADER_END) - 1);

  flush();
  return sent;
}

size_t ClientIO::send_header(const std::string_view& name,
                             const std::string_view& value)
{
  static constexpr char HEADER_SEP[] = ": ";
  static constexpr char HEADER_END[] = "\r\n";

  size_t sent = 0;

  sent += txbuf.sputn(name.data(), name.length());
  sent += txbuf.sputn(HEADER_SEP, sizeof(HEADER_SEP) - 1);
  sent += txbuf.sputn(value.data(), value.length());
  sent += txbuf.sputn(HEADER_END, sizeof(HEADER_END) - 1);

  return sent;
}

size_t ClientIO::send_content_length(uint64_t len)
{
  static constexpr size_t CONLEN_BUF_SIZE = 128;

  char sizebuf[CONLEN_BUF_SIZE];
  const auto sizelen = snprintf(sizebuf, sizeof(sizebuf),
                                "Content-Length: %" PRIu64 "\r\n", len);

  return txbuf.sputn(sizebuf, sizelen);
}
