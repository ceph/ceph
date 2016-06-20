// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/write.hpp>

#include "rgw_asio_client.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "asio: ")


RGWAsioClientIO::RGWAsioClientIO(tcp::socket&& socket, request_type&& request)
  : socket(std::move(socket)), request(std::move(request))
{}

RGWAsioClientIO::~RGWAsioClientIO() = default;

void RGWAsioClientIO::init_env(CephContext *cct)
{
  env.init(cct);
  body_iter = request.body.begin();

  const auto& headers = request.headers;
  for (auto header = headers.begin(); header != headers.end(); ++header) {
    const auto& name = header->name();
    const auto& value = header->value();

    if (boost::algorithm::iequals(name, "content-length")) {
      env.set("CONTENT_LENGTH", value);
      continue;
    }
    if (boost::algorithm::iequals(name, "content-type")) {
      env.set("CONTENT_TYPE", value);
      continue;
    }
    if (boost::algorithm::iequals(name, "connection")) {
      conn_keepalive = boost::algorithm::iequals(value, "keep-alive");
      conn_close = boost::algorithm::iequals(value, "close");
    }

    static const boost::string_ref HTTP_{"HTTP_"};

    char buf[name.size() + HTTP_.size()];
    auto dest = std::copy(std::begin(HTTP_), std::end(HTTP_), buf);
    for (auto src = name.begin(); src != name.end(); ++src, ++dest) {
      if (*src == '-') {
        *dest = '_';
      } else {
        *dest = std::toupper(*src);
      }
    }
    *dest = '\0';

    env.set(buf, value);
  }

  env.set("REQUEST_METHOD", request.method);

  // split uri from query
  auto url = boost::string_ref{request.url};
  auto pos = url.find('?');
  auto query = url.substr(pos + 1);
  url = url.substr(0, pos);

  env.set("REQUEST_URI", url);
  env.set("QUERY_STRING", query);
  env.set("SCRIPT_URI", url); /* FIXME */

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", socket.local_endpoint().port());
  env.set("SERVER_PORT", port_buf);
  // TODO: set SERVER_PORT_SECURE if using ssl
  // TODO: set REMOTE_USER if authenticated
}

int RGWAsioClientIO::write_data(const char *buf, int len)
{
  boost::system::error_code ec;
  auto bytes = boost::asio::write(socket, boost::asio::buffer(buf, len), ec);
  if (ec) {
    derr << "write_data failed with " << ec.message() << dendl;
    return -ec.value();
  }
  return bytes;
}

int RGWAsioClientIO::read_data(char *buf, int max)
{
  // read data from the body's bufferlist
  auto bytes = std::min<unsigned>(max, body_iter.get_remaining());
  body_iter.copy(bytes, buf);
  return bytes;
}

int RGWAsioClientIO::complete_request()
{
  return 0;
}

void RGWAsioClientIO::flush()
{
}

int RGWAsioClientIO::send_status(int status, const char *status_name)
{
  return print("HTTP/1.1 %d %s\r\n", status, status_name);
}

int RGWAsioClientIO::send_100_continue()
{
  return print("HTTP/1.1 100 CONTINUE\r\n\r\n");
}

static constexpr size_t TIME_BUF_SIZE = 128;
static int dump_date_header(char *timestr, size_t size)
{
  const time_t gtime = time(nullptr);
  struct tm result;
  struct tm const * const tmp = gmtime_r(&gtime, &result);
  if (tmp == nullptr)
    return 0;
  return strftime(timestr, size, "Date: %a, %d %b %Y %H:%M:%S %Z\r\n", tmp);
}

int RGWAsioClientIO::complete_header()
{
  char timestr[TIME_BUF_SIZE];
  if (dump_date_header(timestr, sizeof(timestr))) {
    print(timestr);
  }

  if (conn_keepalive) {
    print("Connection: Keep-Alive\r\n");
  } else if (conn_close) {
    print("Connection: close\r\n");
  }
  print("\r\n");
  return 0;
}

int RGWAsioClientIO::send_content_length(uint64_t len)
{
  return print("Content-Length: %" PRIu64 "\r\n", len);
}
