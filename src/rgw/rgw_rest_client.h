#ifndef CEPH_RGW_REST_CLIENT_H
#define CEPH_RGW_REST_CLIENT_H

#include <list>

#include "rgw_http_client.h"

class RGWGetDataCB;

class RGWRESTSimpleRequest : public RGWHTTPClient {
protected:
  CephContext *cct;

  int status;

  string url;

  map<string, string> out_headers;
  list<pair<string, string> > params;

  bufferlist::iterator *send_iter;

  size_t max_response; /* we need this as we don't stream out response */
  bufferlist response;

  void append_param(string& dest, const string& name, const string& val);
  void get_params_str(map<string, string>& extra_args, string& dest);

  int sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info);
public:
  RGWRESTSimpleRequest(CephContext *_cct, string& _url, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : cct(_cct), status(0), url(_url), send_iter(NULL),
                                                        max_response(0) {
    if (_headers)
      headers = *_headers;

    if (_params)
      params = *_params;
  }

  int receive_header(void *ptr, size_t len);
  virtual int receive_data(void *ptr, size_t len);
  virtual int send_data(void *ptr, size_t len);

  bufferlist& get_response() { return response; }

  int execute(RGWAccessKey& key, const char *method, const char *resource);
  int forward_request(RGWAccessKey& key, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl);

  map<string, string>& get_out_headers() { return out_headers; }
};


class RGWRESTStreamRequest : public RGWRESTSimpleRequest {
  Mutex lock;
  list<bufferlist> pending_send;
  void *handle;
  RGWGetDataCB *cb;
public:
  int add_output_data(bufferlist& bl);
  int send_data(void *ptr, size_t len);

  RGWRESTStreamRequest(CephContext *_cct, string& _url, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : RGWRESTSimpleRequest(_cct, _url, _headers, _params),
                lock("RGWRESTStreamRequest"), handle(NULL), cb(NULL) {}
  ~RGWRESTStreamRequest();
  int put_obj_init(RGWAccessKey& key, rgw_obj& obj, uint64_t obj_size, map<string, bufferlist>& attrs);
  int complete();

  RGWGetDataCB *get_out_cb() { return cb; }
};

#endif

