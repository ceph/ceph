// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include <sstream>
#include <string.h>

#include "rgw_loadgen.h"
#include "rgw_auth_s3.h"


#define dout_subsys ceph_subsys_rgw

void RGWLoadGenRequestEnv::set_date(utime_t& tm)
{
  date_str = rgw_to_asctime(tm);
}

int RGWLoadGenRequestEnv::sign(RGWAccessKey& access_key)
{
  meta_map_t meta_map;
  map<string, string> sub_resources;

  string canonical_header;
  string digest;

  rgw_create_s3_canonical_header(request_method.c_str(),
                                 nullptr, /* const char *content_md5 */
                                 content_type.c_str(),
                                 date_str.c_str(),
                                 meta_map,
				                 meta_map_t{},
                                 uri.c_str(),
                                 sub_resources,
                                 canonical_header);

  headers["HTTP_DATE"] = date_str;
  try {
    /* FIXME(rzarzynski): kill the dependency on g_ceph_context. */
    const auto signature = static_cast<std::string>(
      rgw::auth::s3::get_v2_signature(g_ceph_context, canonical_header,
                                      access_key.key));
    headers["HTTP_AUTHORIZATION"] = \
      std::string("AWS ") + access_key.id + ":" + signature;
  } catch (int ret) {
    return ret;
  }

  return 0;
}

size_t RGWLoadGenIO::write_data(const char* const buf,
                                const size_t len)
{
  return len;
}

size_t RGWLoadGenIO::read_data(char* const buf, const size_t len)
{
  const size_t read_len = std::min(left_to_read,
                                   static_cast<uint64_t>(len));
  left_to_read -= read_len;
  return read_len;
}

void RGWLoadGenIO::flush()
{
}

size_t RGWLoadGenIO::complete_request()
{
  return 0;
}

int RGWLoadGenIO::init_env(CephContext *cct)
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
  return 0;
}

size_t RGWLoadGenIO::send_status(const int status,
                                 const char* const status_name)
{
  return 0;
}

size_t RGWLoadGenIO::send_100_continue()
{
  return 0;
}

size_t RGWLoadGenIO::send_header(const std::string_view& name,
                                 const std::string_view& value)
{
  return 0;
}

size_t RGWLoadGenIO::complete_header()
{
  return 0;
}

size_t RGWLoadGenIO::send_content_length(const uint64_t len)
{
  return 0;
}
