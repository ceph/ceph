#ifndef CEPH_RGW_HTTP_CLIENT_H
#define CEPH_RGW_HTTP_CLIENT_H

#include "rgw_common.h"

class RGWHTTPClient
{
  list<pair<string, string> > headers;
public:
  virtual ~RGWHTTPClient() {}
  RGWHTTPClient() {}

  void append_header(const string& name, const string& val) {
    headers.push_back(pair<string, string>(name, val));
  }

  virtual int read_header(void *ptr, size_t len) { return 0; }
  virtual int read_data(void *ptr, size_t len) { return 0; }

  int process(const string& url);
};

#endif
