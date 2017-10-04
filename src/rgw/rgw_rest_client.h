// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CLIENT_H
#define CEPH_RGW_REST_CLIENT_H

#include "rgw_http_client.h"

class RGWGetDataCB;

class RGWHTTPSimpleRequest : public RGWHTTPClient {
protected:
  int http_status;
  int status;

  map<string, string> out_headers;
  param_vec_t params;

  bufferlist::iterator *send_iter;

  size_t max_response; /* we need this as we don't stream out response */
  bufferlist response;

  virtual int handle_header(const string& name, const string& val);
  void append_param(string& dest, const string& name, const string& val);
  void get_params_str(map<string, string>& extra_args, string& dest);

public:
  RGWHTTPSimpleRequest(CephContext *_cct, const string& _method, const string& _url,
                       param_vec_t *_headers, param_vec_t *_params) : RGWHTTPClient(_cct, _method, _url),
                http_status(0), status(0),
                send_iter(NULL),
                max_response(0) {
    set_headers(_headers);
    set_params(_params);
  }

  void set_headers(param_vec_t *_headers) {
    if (_headers)
      headers = *_headers;
  }

  void set_params(param_vec_t *_params) {
    if (_params)
      params = *_params;
  }

  int receive_header(void *ptr, size_t len) override;
  int receive_data(void *ptr, size_t len) override;
  int send_data(void *ptr, size_t len) override;

  bufferlist& get_response() { return response; }

  map<string, string>& get_out_headers() { return out_headers; }

  int get_http_status() { return http_status; }
  int get_status();
};

class RGWRESTSimpleRequest : public RGWHTTPSimpleRequest {
public:
  RGWRESTSimpleRequest(CephContext *_cct, const string& _method, const string& _url,
                       param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params) {}

  int execute(RGWAccessKey& key, const char *method, const char *resource);
  int forward_request(RGWAccessKey& key, req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl);
};


class RGWHTTPStreamRWRequest : public RGWHTTPSimpleRequest {
  Mutex lock;
  Mutex write_lock;
  RGWGetDataCB *cb{nullptr};
  bufferlist outbl;
  bufferlist in_data;
  size_t chunk_ofs{0};
  size_t ofs{0};
  uint64_t write_ofs{0};
  bool send_paused{false};
  bool stream_writes{false};
  bool write_stream_complete{false};
protected:
  int handle_header(const string& name, const string& val) override;
public:
  int send_data(void *ptr, size_t len, bool *pause) override;
  int receive_data(void *ptr, size_t len) override;

  RGWHTTPStreamRWRequest(CephContext *_cct, const string& _method, const string& _url,
                         param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params),
                                                                        lock("RGWHTTPStreamRWRequest"), write_lock("RGWHTTPStreamRWRequest::write_lock") {
  }
  RGWHTTPStreamRWRequest(CephContext *_cct, const string& _method, const string& _url, RGWGetDataCB *_cb,
                         param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params),
                                                                        lock("RGWHTTPStreamRWRequest"), write_lock("RGWHTTPStreamRWRequest::write_lock"), cb(_cb) {
  }
  virtual ~RGWHTTPStreamRWRequest() override {}

  void set_outbl(bufferlist& _outbl) {
    outbl.swap(_outbl);
  }

  void set_in_cb(RGWGetDataCB *_cb) { cb = _cb; }

  void add_send_data(bufferlist& bl);

  void set_stream_write(bool s);

  /* finish streaming writes */
  void finish_write();
};

class RGWRESTStreamRWRequest : public RGWHTTPStreamRWRequest {
public:
  RGWRESTStreamRWRequest(CephContext *_cct, const string& _method, const string& _url, RGWGetDataCB *_cb,
		param_vec_t *_headers, param_vec_t *_params) : RGWHTTPStreamRWRequest(_cct, _method, _url, _cb, _headers, _params) {
  }
  virtual ~RGWRESTStreamRWRequest() override {}
  int send_request(RGWAccessKey& key, map<string, string>& extra_headers, rgw_obj& obj, RGWHTTPManager *mgr);
  int send_request(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource, RGWHTTPManager *mgr, bufferlist *send_data = nullptr /* optional input data */);
  int complete_request(string& etag, real_time *mtime, uint64_t *psize, map<string, string>& attrs);
};

class RGWRESTStreamReadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamReadRequest(CephContext *_cct, const string& _url, RGWGetDataCB *_cb, param_vec_t *_headers,
		param_vec_t *_params) : RGWRESTStreamRWRequest(_cct, "GET", _url, _cb, _headers, _params) {}
};

class RGWRESTStreamHeadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamHeadRequest(CephContext *_cct, const string& _url, RGWGetDataCB *_cb, param_vec_t *_headers,
		param_vec_t *_params) : RGWRESTStreamRWRequest(_cct, "HEAD", _url, _cb, _headers, _params) {}
};

class RGWRESTStreamS3PutObj : public RGWRESTStreamRWRequest {
  RGWGetDataCB *out_cb;
public:
  RGWRESTStreamS3PutObj(CephContext *_cct, const string& _method, const string& _url, param_vec_t *_headers,
		param_vec_t *_params) : RGWRESTStreamRWRequest(_cct, _method, _url, nullptr, _headers, _params),
                out_cb(NULL) {}
  ~RGWRESTStreamS3PutObj() override;
  int put_obj_init(RGWAccessKey& key, rgw_obj& obj, uint64_t obj_size, map<string, bufferlist>& attrs, bool send);

  RGWGetDataCB *get_out_cb() { return out_cb; }
};

#endif

