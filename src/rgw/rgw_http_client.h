#ifndef CEPH_RGW_HTTP_CLIENT_H
#define CEPH_RGW_HTTP_CLIENT_H

#include "rgw_common.h"

class RGWHTTPClient
{
public:
  virtual ~RGWHTTPClient() {}
  RGWHTTPClient() {}

  virtual int read_header(void *ptr, size_t len) { return 0; }

  int process(const string& url);
};

#endif
