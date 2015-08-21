// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "civetweb/civetweb.h"
#include "rgw_civetweb.h"


#define dout_subsys ceph_subsys_rgw

int RGWMongoose::write_data(const char * const buf, const int len)
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

int RGWMongoose::read_data(char * const buf, const int len)
{
  return mg_read(conn, buf, len);
}

void RGWMongoose::flush(RGWClientIO& controller)
{
  /* NOP */
}

void RGWMongoose::init_env(CephContext * const cct)
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

int RGWMongoose::send_status(RGWClientIO& controller,
                             const char * const status,
                             const char * const status_name)
{
  const int status_num = atoi(status);
  mg_set_http_status(conn, status_num);

  return controller.print("HTTP/1.1 %s %s\r\n", status,
          status_name ? status_name : "");
}

int RGWMongoose::send_100_continue(RGWClientIO& controller)
{
  char buf[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";

  return controller.write(buf, sizeof(buf) - 1);
}

static int dump_date_header(RGWClientIO& controller)
{
  char timestr[TIME_BUF_SIZE];
  const time_t gtime = time(NULL);
  struct tm result;
  struct tm const * const tmp = gmtime_r(&gtime, &result);

  if (tmp == NULL) {
    return 0;
  }

  if (!strftime(timestr, sizeof(timestr),
               "Date: %a, %d %b %Y %H:%M:%S %Z\r\n", tmp)) {
    return 0;
  }

  return controller.print(timestr);
}

int RGWMongoose::complete_header(RGWClientIO& controller)
{
  const char CONN_KEEP_ALIVE[] = "Connection: Keep-Alive\r\n";
  const char CONN_CLOSE[] = "Connection: close\r\n";
  ssize_t rc;

  if (!has_content_length) {
    mg_enforce_close(conn);
  }

  rc = dump_date_header(controller);
  if (rc < 0) {
    dout(3) << "dump_date_header() returned " << rc << dendl;
    return rc;
  }

  if (explicit_keepalive) {
    rc = controller.write(CONN_KEEP_ALIVE, sizeof(CONN_KEEP_ALIVE) - 1);
  } else if (explicit_conn_close) {
    rc = controller.write(CONN_CLOSE, sizeof(CONN_CLOSE) - 1);
  }
  if (rc < 0) {
    dout(3) << "sending conn state returned " << rc << dendl;
    return rc;
  }

  return controller.print("\r\n");
}

int RGWMongoose::send_content_length(RGWClientIO& controller,
                                     const uint64_t len)
{
  has_content_length = true;

  return controller.print("Content-Length: %" PRIu64 "\r\n", len);
}
