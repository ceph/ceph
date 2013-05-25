#ifndef CEPH_RGW_REST_CLIENT_H
#define CEPH_RGW_REST_CLIENT_H

#include <list>

#include "rgw_http_client.h"

class RGWRESTClient : public RGWHTTPClient {
protected:
  CephContext *cct;

  int status;

  string url;

  map<string, string> out_headers;
  list<pair<string, string> > params;

  bufferlist::iterator *send_iter;

  void append_param(string& dest, const string& name, const string& val);
  void get_params_str(map<string, string>& extra_args, string& dest);
public:
  RGWRESTClient(CephContext *_cct, string& _url, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : cct(_cct), status(0), url(_url), send_iter(NULL) {
    if (_headers)
      headers = *_headers;

    if (_params)
      params = *_params;
  }

  int receive_header(void *ptr, size_t len);
  int send_data(void *ptr, size_t len);

  int execute(RGWAccessKey& key, const char *method, const char *resource);
  int forward_request(RGWAccessKey& key, req_info& info, bufferlist *inbl);
};


#endif

