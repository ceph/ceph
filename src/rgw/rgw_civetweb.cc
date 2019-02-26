// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/utility/string_ref.hpp>

#include "civetweb/civetweb.h"
#include "rgw_civetweb.h"
#include "rgw_perf_counters.h"


#define dout_subsys ceph_subsys_rgw

size_t RGWCivetWeb::write_data(const char *buf, const size_t len)
{
  auto to_sent = len;
  while (to_sent) {
    const int ret = mg_write(conn, buf, len);
    if (ret < 0 || ! ret) {
      /* According to the documentation of mg_write() it always returns -1 on
       * error. The details aren't available, so we will just throw EIO. Same
       * goes to 0 that is associated with writing to a closed connection. */
      throw rgw::io::Exception(EIO, std::system_category());
    } else {
      to_sent -= static_cast<size_t>(ret);
    }
  }
  return len;
}

RGWCivetWeb::RGWCivetWeb(mg_connection* const conn)
  : conn(conn),
    explicit_keepalive(false),
    explicit_conn_close(false),
    got_eof_on_read(false),
    txbuf(*this)
{
    sockaddr *lsa = mg_get_local_addr(conn);
    switch(lsa->sa_family) {
    case AF_INET:
	port = ntohs(((struct sockaddr_in*)lsa)->sin_port);
	break;
    case AF_INET6:
	port = ntohs(((struct sockaddr_in6*)lsa)->sin6_port);
	break;
    default:
	port = -1;
    }
}

size_t RGWCivetWeb::read_data(char *buf, size_t len)
{
  size_t c;
  int ret;
  if (got_eof_on_read) {
    return 0;
  }
  for (c = 0; c < len; c += ret) {
    ret = mg_read(conn, buf+c, len-c);
    if (ret < 0) {
      throw rgw::io::Exception(EIO, std::system_category());
    }
    if (!ret) {
      got_eof_on_read = true;
      break;
    }
  }
  return c;
}

void RGWCivetWeb::flush()
{
  txbuf.pubsync();
}

size_t RGWCivetWeb::complete_request()
{
  perfcounter->inc(l_rgw_qlen, -1);
  perfcounter->inc(l_rgw_qactive, -1);
  return 0;
}

int RGWCivetWeb::init_env(CephContext *cct)
{
  env.init(cct);
  const struct mg_request_info* info = mg_get_request_info(conn);

  if (! info) {
    // request info is NULL; we have no info about the connection
    return -EINVAL;
  }

  for (int i = 0; i < info->num_headers; i++) {
    const auto header = &info->http_headers[i];

    if (header->name == nullptr || header->value==nullptr) {
      lderr(cct) << "client supplied malformatted headers" << dendl;
      return -EINVAL;
    }

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

  perfcounter->inc(l_rgw_qlen);
  perfcounter->inc(l_rgw_qactive);

  env.set("REMOTE_ADDR", info->remote_addr);
  env.set("REQUEST_METHOD", info->request_method);
  env.set("HTTP_VERSION", info->http_version);
  env.set("REQUEST_URI", info->request_uri); // get the full uri, we anyway handle abs uris later
  env.set("SCRIPT_URI", info->local_uri);
  if (info->query_string) {
    env.set("QUERY_STRING", info->query_string);
  }
  if (info->remote_user) {
    env.set("REMOTE_USER", info->remote_user);
  }

  if (port <= 0)
    lderr(cct) << "init_env: bug: invalid port number" << dendl;
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", port);
  env.set("SERVER_PORT", port_buf);
  if (info->is_ssl) {
    env.set("SERVER_PORT_SECURE", port_buf);
  }
  return 0;
}

size_t RGWCivetWeb::send_status(int status, const char *status_name)
{
  mg_set_http_status(conn, status);

  static constexpr size_t STATUS_BUF_SIZE = 128;

  char statusbuf[STATUS_BUF_SIZE];
  const auto statuslen = snprintf(statusbuf, sizeof(statusbuf),
                                  "HTTP/1.1 %d %s\r\n", status, status_name);

  return txbuf.sputn(statusbuf, statuslen);
}

size_t RGWCivetWeb::send_100_continue()
{
  const char HTTTP_100_CONTINUE[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";
  const size_t sent = txbuf.sputn(HTTTP_100_CONTINUE,
                                  sizeof(HTTTP_100_CONTINUE) - 1);
  flush();
  return sent;
}

size_t RGWCivetWeb::send_header(const boost::string_ref& name,
                                const boost::string_ref& value)
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

size_t RGWCivetWeb::dump_date_header()
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

  return txbuf.sputn(timestr, strlen(timestr));
}

size_t RGWCivetWeb::complete_header()
{
  size_t sent = dump_date_header();

  if (explicit_keepalive) {
    constexpr char CONN_KEEP_ALIVE[] = "Connection: Keep-Alive\r\n";
    sent += txbuf.sputn(CONN_KEEP_ALIVE, sizeof(CONN_KEEP_ALIVE) - 1);
  } else if (explicit_conn_close) {
    constexpr char CONN_KEEP_CLOSE[] = "Connection: close\r\n";
    sent += txbuf.sputn(CONN_KEEP_CLOSE, sizeof(CONN_KEEP_CLOSE) - 1);
  }

  static constexpr char HEADER_END[] = "\r\n";
  sent += txbuf.sputn(HEADER_END, sizeof(HEADER_END) - 1);

  flush();
  return sent;
}

size_t RGWCivetWeb::send_content_length(uint64_t len)
{
  static constexpr size_t CONLEN_BUF_SIZE = 128;

  char sizebuf[CONLEN_BUF_SIZE];
  const auto sizelen = snprintf(sizebuf, sizeof(sizebuf),
                                "Content-Length: %" PRIu64 "\r\n", len);
  return txbuf.sputn(sizebuf, sizelen);
}
