// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "rgw_loadgen.h"
#include "rgw_auth_s3.h"


#define dout_subsys ceph_subsys_rgw

void RGWLoadGenRequestEnv::set_date(utime_t& tm)
{
  stringstream s;
  tm.asctime(s);
  date_str = s.str();
}

int RGWLoadGenRequestEnv::sign(RGWAccessKey& access_key)
{
  map<string, string> meta_map;
  map<string, string> sub_resources;

  string canonical_header;
  string digest;

  rgw_create_s3_canonical_header(request_method.c_str(),
                                 NULL, /* const char *content_md5 */
                                 content_type.c_str(),
                                 date_str.c_str(),
                                 meta_map,
                                 uri.c_str(),
                                 sub_resources,
                                 canonical_header);

  int ret = rgw_get_s3_header_digest(canonical_header, access_key.key, digest);
  if (ret < 0) {
    return ret;
  }

  headers["HTTP_DATE"] = date_str;
  headers["HTTP_AUTHORIZATION"] = string("AWS ") + access_key.id + ":" + digest;

  return 0;
}

int RGWLoadGenIO::write_data(const char *buf, int len)
{
  return len;
}

int RGWLoadGenIO::read_data(char *buf, int len)
{
  int read_len = MIN(left_to_read, (uint64_t)len);
  left_to_read -= read_len;
  return read_len;
}

void RGWLoadGenIO::flush()
{
}

int RGWLoadGenIO::complete_request()
{
  return 0;
}

void RGWLoadGenIO::init_env(CephContext *cct)
{
  env.init(cct);

  left_to_read = req->content_length;

  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)req->content_length);
  env.set("CONTENT_LENGTH", buf);

  env.set("CONTENT_TYPE", req->content_type.c_str());
  env.set("HTTP_DATE", req->date_str.c_str());

  for (map<string, string>::iterator iter = req->headers.begin(); iter != req->headers.end(); ++iter) {
    env.set(iter->first.c_str(), iter->second.c_str());
  }

  env.set("REQUEST_METHOD", req->request_method.c_str());
  env.set("REQUEST_URI", req->uri.c_str());
  env.set("QUERY_STRING", req->query_string.c_str());
  env.set("SCRIPT_URI", req->uri.c_str());

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", req->port);
  env.set("SERVER_PORT", port_buf);
}

int RGWLoadGenIO::send_status(int status, const char *status_name)
{
  return 0;
}

int RGWLoadGenIO::send_100_continue()
{
  return 0;
}

int RGWLoadGenIO::complete_header()
{
  return 0;
}

int RGWLoadGenIO::send_content_length(uint64_t len)
{
  return 0;
}
