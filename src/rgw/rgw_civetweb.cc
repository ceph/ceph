// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "civetweb/civetweb.h"
#include "rgw_civetweb.h"


#define dout_subsys ceph_subsys_rgw

int RGWCivetWeb::write_data(const char *buf, int len)
{
  int r = mg_write(conn, buf, len);
  if (r == 0) {
    /* didn't send anything, error out */
    return -EIO;
  }
  return r;
}

RGWCivetWeb::RGWCivetWeb(mg_connection *_conn, int _port)
  : RGWStreamIOFacade(this),
    conn(_conn), port(_port),
    explicit_keepalive(false),
    explicit_conn_close(false)
{
}

int RGWCivetWeb::read_data(char *buf, int len)
{
  return mg_read(conn, buf, len);
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
  struct mg_request_info *info = mg_get_request_info(conn);

  if (!info)
    return;

  for (int i = 0; i < info->num_headers; i++) {
    struct mg_request_info::mg_header *header = &info->http_headers[i];

    if (strcasecmp(header->name, "content-length") == 0) {
      env.set("CONTENT_LENGTH", header->value);
      continue;
    }

    if (strcasecmp(header->name, "content-type") == 0) {
      env.set("CONTENT_TYPE", header->value);
      continue;
    }

    if (strcasecmp(header->name, "connection") == 0) {
      explicit_keepalive = (strcasecmp(header->value, "keep-alive") == 0);
      explicit_conn_close = (strcasecmp(header->value, "close") == 0);
    }

    int len = strlen(header->name) + 5; /* HTTP_ prepended */
    char buf[len + 1];
    memcpy(buf, "HTTP_", 5);
    const char *src = header->name;
    char *dest = &buf[5];
    for (; *src; src++, dest++) {
      char c = *src;
      switch (c) {
       case '-':
	 c = '_';
	 break;
      default:
	c = toupper(c);
	break;
      }
      *dest = c;
    }
    *dest = '\0';

    env.set(buf, header->value);
  }

  env.set("REQUEST_METHOD", info->request_method);
  env.set("REQUEST_URI", info->uri);
  if (info->query_string) {
    env.set("QUERY_STRING", info->query_string);
  }
  if (info->remote_user) {
    env.set("REMOTE_USER", info->remote_user);
  }
  env.set("SCRIPT_URI", info->uri); /* FIXME */

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

int RGWCivetWeb::send_status(int status, const char *status_name)
{
  mg_set_http_status(conn, status);

  return mg_printf(conn, "HTTP/1.1 %d %s\r\n", status,
                   status_name ? status_name : "");
}

int RGWCivetWeb::send_100_continue()
{
  const char buf[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";

  return mg_write(conn, buf, sizeof(buf) - 1);
}

int RGWCivetWeb::dump_date_header()
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

int RGWCivetWeb::complete_header()
{
  constexpr char CONN_KEEP_ALIVE[] = "Connection: Keep-Alive\r\n";
  constexpr char CONN_KEEP_CLOSE[] = "Connection: close\r\n";
  constexpr char HEADER_END[] = "\r\n";

  size_t sent = 0;

  dump_date_header();

  if (explicit_keepalive) {
    sent += write_data(CONN_KEEP_ALIVE, sizeof(CONN_KEEP_ALIVE) - 1);
  } else if (explicit_conn_close) {
    sent += write_data(CONN_KEEP_CLOSE, sizeof(CONN_KEEP_CLOSE) - 1);
  }

  return sent + write_data(HEADER_END, sizeof(HEADER_END) - 1);
}

int RGWCivetWeb::send_content_length(uint64_t len)
{
  char buf[21];
  snprintf(buf, sizeof(buf), "%" PRIu64, len);

  return mg_printf(conn, "Content-Length: %s\r\n", buf);
}
