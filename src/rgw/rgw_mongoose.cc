
#include <string.h>

#include "mongoose/mongoose.h"
#include "rgw_mongoose.h"


#define dout_subsys ceph_subsys_rgw

int RGWMongoose::write_data(const char *buf, int len)
{
  if (!sent_header) {
    header_data.append(buf, len);
    return 0;
  }
  dout(0) << buf << dendl;
  return mg_write(event->conn, buf, len);
}

RGWMongoose::RGWMongoose(mg_event *_event) : event(_event), sent_header(false) {
}

int RGWMongoose::read_data(char *buf, int len)
{
  return mg_read(event->conn, buf, len);
}

void RGWMongoose::flush()
{
}

void RGWMongoose::init_env(CephContext *cct)
{
  env.init(cct);
  struct mg_request_info *info = event->request_info;
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
}

int RGWMongoose::send_status(const char *status, const char *status_name)
{
  char buf[128];

  if (!status_name)
    status_name = "";

  snprintf(buf, sizeof(buf), "HTTP/1.1 %s %s\n", status, status_name);

  bufferlist bl;
  bl.append(buf);
  bl.append(header_data);
  header_data = bl;

  return 0;
}

int RGWMongoose::send_100_continue()
{
  char buf[] = "HTTP/1.1 100 CONTINUE\r\n\r\n";

  return mg_write(event->conn, buf, sizeof(buf) - 1);
}

int RGWMongoose::complete_header()
{
  header_data.append("\r\n");

  sent_header = true;

  return write_data(header_data.c_str(), header_data.length());
}
