// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "civetweb/civetweb.h"
#include "rgw_civetweb.h"


#define dout_subsys ceph_subsys_rgw

int RGWMongoose::write_data(const char *buf, int len)
{
  const int ret = mg_write(conn, buf, len);
  if (ret > 0) {
    return ret;
  }

  /* didn't send anything, error out */
  dout(3) << "mg_write() returned " << ret << dendl;
  return -EIO;
}

RGWMongoose::RGWMongoose(mg_connection *_conn, int _port)
  : conn(_conn),
    port(_port),
    has_content_length(false),
    explicit_keepalive(false),
    explicit_conn_close(false)
{
}

int RGWMongoose::read_data(char *buf, int len)
{
  return mg_read(conn, buf, len);
}

void RGWMongoose::flush()
{
  /* NOP */
}

void RGWMongoose::init_env(CephContext *cct)
{
  env.init(cct);
  struct mg_request_info *info = mg_get_request_info(conn);
  if (!info) {
    return;
  }

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
  env.set("QUERY_STRING", info->query_string);
  env.set("REMOTE_USER", info->remote_user);
  env.set("SCRIPT_URI", info->uri); /* FIXME */

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", port);
  env.set("SERVER_PORT", port_buf);
}

int RGWMongoose::send_status(const char * status,
                             const char * status_name)
{
  int status_num = atoi(status);
  mg_set_http_status(conn, status_num);

  if (!status_name) {
    status_name = "";
  }

  char buf[128];
  snprintf(buf, sizeof(buf), "HTTP/1.1 %s %s\r\n", status, status_name);
  write_data(buf, strlen(buf));

  return 0;
}

int RGWMongoose::send_100_continue()
{
  char buf[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";

  return mg_write(conn, buf, sizeof(buf) - 1);
}

int RGWMongoose::complete_header()
{
  if (!has_content_length) {
    mg_enforce_close(conn);
  }

  string str;
  if (explicit_keepalive) {
    str = "Connection: Keep-Alive\r\n";
  } else if (explicit_conn_close) {
    str = "Connection: close\r\n";
  }

  str.append("\r\n");

  return write_data(str.c_str(), str.length());
}

int RGWMongoose::send_content_length(uint64_t len)
{
  has_content_length = true;

  char buf[21];
  snprintf(buf, sizeof(buf), "%" PRIu64, len);
  return print("Content-Length: %s\r\n", buf);
}
