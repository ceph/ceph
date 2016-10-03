// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/utility/string_ref.hpp>

#include "civetweb/civetweb.h"
#include "rgw_civetweb.h"


#define dout_subsys ceph_subsys_rgw

std::size_t RGWCivetWeb::write_data(const char *buf, std::size_t len)
{
  const int ret = mg_write(conn, buf, len);
  if (ret == 0) {
    /* didn't send anything, error out */
    throw RGWRestfulIOEngine::Exception(EIO, std::system_category());
  } else if (ret < 0) {
    throw RGWRestfulIOEngine::Exception(-ret, std::system_category());
  }
  return ret;
}

RGWCivetWeb::RGWCivetWeb(mg_connection* const conn, const int port)
  : conn(conn),
    port(port),
    explicit_keepalive(false),
    explicit_conn_close(false)
{
}

std::size_t RGWCivetWeb::read_data(char *buf, std::size_t len)
{
  const int ret = mg_read(conn, buf, len);
  if (ret < 0) {
    throw RGWRestfulIOEngine::Exception(-ret, std::system_category());
  }
  return ret;
}

void RGWCivetWeb::flush()
{
}

int RGWCivetWeb::complete_request()
{
  return 0;
}

void RGWCivetWeb::init_env(CephContext *cct)
{
  env.init(cct);
  struct mg_request_info* const info = mg_get_request_info(conn);

  if (! info) {
    return;
  }

  for (int i = 0; i < info->num_headers; i++) {
    struct mg_request_info::mg_header* const header = &info->http_headers[i];
    const boost::string_ref name(header->name);
    const auto& value = header->value;

    if (boost::algorithm::iequals(name, "content-length")) {
      env.set("CONTENT_LENGTH", value);
      continue;
    }
    if (boost::algorithm::iequals(name, "content-type")) {
      env.set("CONTENT_TYPE", value);
      continue;
    }
    if (boost::algorithm::iequals(name, "connection")) {
      explicit_keepalive = boost::algorithm::iequals(value, "keep-alive");
      explicit_conn_close = boost::algorithm::iequals(value, "close");
    }

    static const boost::string_ref HTTP_{"HTTP_"};

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

    env.set(buf, value);
  }

  env.set("REQUEST_METHOD", info->request_method);
  env.set("REQUEST_URI", info->uri);
  env.set("SCRIPT_URI", info->uri); /* FIXME */
  if (info->query_string) {
    env.set("QUERY_STRING", info->query_string);
  }
  if (info->remote_user) {
    env.set("REMOTE_USER", info->remote_user);
  }

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", port);
  env.set("SERVER_PORT", port_buf);

  if (info->is_ssl) {
    if (port == 0) {
      strcpy(port_buf,"443");
    }
    env.set("SERVER_PORT_SECURE", port_buf);
  }
}

template <class... Args>
static inline std::size_t safe_mg_printf(Args&&... args)
{
  const int ret = mg_printf(std::forward<Args>(args)...);
  if (ret == 0) {
    /* didn't send anything, error out */
    throw RGWRestfulIOEngine::Exception(EIO, std::system_category());
  } else if (ret < 0) {
    throw RGWRestfulIOEngine::Exception(-ret, std::system_category());
  }
  return static_cast<std::size_t>(ret);
}

std::size_t RGWCivetWeb::send_status(int status, const char *status_name)
{
  mg_set_http_status(conn, status);

  return safe_mg_printf(conn, "HTTP/1.1 %d %s\r\n", status,
                        status_name ? status_name : "");
}

std::size_t RGWCivetWeb::send_100_continue()
{
  const char HTTTP_100_CONTINUE[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";
  return write_data(HTTTP_100_CONTINUE, sizeof(HTTTP_100_CONTINUE) - 1);
}

std::size_t RGWCivetWeb::send_header(const boost::string_ref& name,
                                     const boost::string_ref& value)
{
  return safe_mg_printf(conn, "%.*s: %.*s\r\n", name.length(), name.data(),
                        value.length(), value.data());
}

std::size_t RGWCivetWeb::dump_date_header()
{
  char timestr[TIME_BUF_SIZE];

  const time_t gtime = time(nullptr);
  struct tm result;
  struct tm const* const tmp = gmtime_r(&gtime, &result);

  if (nullptr == tmp) {
    return 0;
  }

  if (! strftime(timestr, sizeof(timestr),
                 "Date: %a, %d %b %Y %H:%M:%S %Z\r\n", tmp)) {
    return 0;
  }

  return write_data(timestr, strlen(timestr));
}

std::size_t RGWCivetWeb::complete_header()
{
  std::size_t sent = dump_date_header();

  if (explicit_keepalive) {
    constexpr char CONN_KEEP_ALIVE[] = "Connection: Keep-Alive\r\n";
    sent += write_data(CONN_KEEP_ALIVE, sizeof(CONN_KEEP_ALIVE) - 1);
  } else if (explicit_conn_close) {
    constexpr char CONN_KEEP_CLOSE[] = "Connection: close\r\n";
    sent += write_data(CONN_KEEP_CLOSE, sizeof(CONN_KEEP_CLOSE) - 1);
  }

  constexpr char HEADER_END[] = "\r\n";
  return sent + write_data(HEADER_END, sizeof(HEADER_END) - 1);
}

std::size_t RGWCivetWeb::send_content_length(uint64_t len)
{
  return safe_mg_printf(conn, "Content-Length: %" PRIu64 "\r\n", len);
}
