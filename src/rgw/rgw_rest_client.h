// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CLIENT_H
#define CEPH_RGW_REST_CLIENT_H

#include <list>

#include "rgw_http_client.h"

class RGWGetDataCB;

class RGWRESTSimpleRequest : public RGWHTTPClient {
protected:
  int http_status;
  int status;

  string url;

  map<string, string> out_headers;
  list<pair<string, string> > params;

  bufferlist::iterator *send_iter;

  size_t max_response; /* we need this as we don't stream out response */
  bufferlist response;

  virtual int handle_header(const string& name, const string& val) { return 0; }
  void append_param(string& dest, const string& name, const string& val);
  void get_params_str(map<string, string>& extra_args, string& dest);

  int sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info);
public:
  RGWRESTSimpleRequest(CephContext *_cct, string& _url, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : RGWHTTPClient(_cct), http_status(0), status(0),
                url(_url), send_iter(NULL),
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


class RGWRESTStreamWriteRequest : public RGWRESTSimpleRequest {
  Mutex lock;
  list<bufferlist> pending_send;
  void *handle;
  RGWGetDataCB *cb;
public:
  int add_output_data(bufferlist& bl);
  int send_data(void *ptr, size_t len);

  RGWRESTStreamWriteRequest(CephContext *_cct, string& _url, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : RGWRESTSimpleRequest(_cct, _url, _headers, _params),
                lock("RGWRESTStreamWriteRequest"), handle(NULL), cb(NULL) {}
  ~RGWRESTStreamWriteRequest();
  int put_obj_init(RGWAccessKey& key, rgw_obj& obj, uint64_t obj_size, map<string, bufferlist>& attrs);
  int complete(string& etag, time_t *mtime);

  RGWGetDataCB *get_out_cb() { return cb; }
};

class RGWRESTStreamReadRequest : public RGWRESTSimpleRequest {
  Mutex lock;
  RGWGetDataCB *cb;
  bufferlist in_data;
  size_t chunk_ofs;
  size_t ofs;
protected:
  int handle_header(const string& name, const string& val);
public:
  int send_data(void *ptr, size_t len);
  int receive_data(void *ptr, size_t len);

  RGWRESTStreamReadRequest(CephContext *_cct, string& _url, RGWGetDataCB *_cb, list<pair<string, string> > *_headers,
                list<pair<string, string> > *_params) : RGWRESTSimpleRequest(_cct, _url, _headers, _params),
                lock("RGWRESTStreamReadRequest"), cb(_cb),
                chunk_ofs(0), ofs(0) {}
  ~RGWRESTStreamReadRequest() {}
  int get_obj(RGWAccessKey& key, map<string, string>& extra_headers, rgw_obj& obj);
  int complete(string& etag, time_t *mtime, map<string, string>& attrs);

  void set_in_cb(RGWGetDataCB *_cb) { cb = _cb; }
};

#endif

