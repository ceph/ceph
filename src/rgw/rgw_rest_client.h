// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_http_client.h"

class RGWGetDataCB;

class RGWHTTPSimpleRequest : public RGWHTTPClient {
protected:
  int http_status;
  int status;

  using unique_lock = std::unique_lock<std::mutex>;

  std::mutex out_headers_lock;
  map<string, string> out_headers;
  param_vec_t params;

  bufferlist::iterator *send_iter;

  size_t max_response; /* we need this as we don't stream out response */
  bufferlist response;

  virtual int handle_header(const string& name, const string& val);
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
  int receive_data(void *ptr, size_t len, bool *pause) override;
  int send_data(void *ptr, size_t len, bool* pause=nullptr) override;

  bufferlist& get_response() { return response; }

  void get_out_headers(map<string, string> *pheaders); /* modifies out_headers */

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

class RGWWriteDrainCB {
public:
  RGWWriteDrainCB() = default;
  virtual ~RGWWriteDrainCB() = default;
  virtual void notify(uint64_t pending_size) = 0;
};

class RGWRESTGenerateHTTPHeaders {
  CephContext *cct;
  RGWEnv *new_env;
  req_info *new_info;
  string method;
  string url;
  string resource;

public:
  RGWRESTGenerateHTTPHeaders(CephContext *_cct, RGWEnv *_env, req_info *_info) : cct(_cct), new_env(_env), new_info(_info) {}
  void init(const string& method, const string& url, const string& resource, const param_vec_t& params);
  void set_extra_headers(const map<string, string>& extra_headers);
  int set_obj_attrs(map<string, bufferlist>& rgw_attrs);
  void set_http_attrs(const map<string, string>& http_attrs);
  void set_policy(RGWAccessControlPolicy& policy);
  int sign(RGWAccessKey& key);

  const string& get_url() { return url; }
};

class RGWHTTPStreamRWRequest : public RGWHTTPSimpleRequest {
public:
    class ReceiveCB;

private:
  ceph::mutex lock =
    ceph::make_mutex("RGWHTTPStreamRWRequest");
  ceph::mutex write_lock =
    ceph::make_mutex("RGWHTTPStreamRWRequest::write_lock");
  ReceiveCB *cb{nullptr};
  RGWWriteDrainCB *write_drain_cb{nullptr};
  bufferlist outbl;
  bufferlist in_data;
  size_t chunk_ofs{0};
  size_t ofs{0};
  uint64_t write_ofs{0};
  bool read_paused{false};
  bool send_paused{false};
  bool stream_writes{false};
  bool write_stream_complete{false};
protected:
  int handle_header(const string& name, const string& val) override;
public:
  int send_data(void *ptr, size_t len, bool *pause) override;
  int receive_data(void *ptr, size_t len, bool *pause) override;

  class ReceiveCB {
    protected:
      uint64_t extra_data_len{0};
    public:
      ReceiveCB() = default;
      virtual ~ReceiveCB() = default;
      virtual int handle_data(bufferlist& bl, bool *pause = nullptr) = 0;
      virtual void set_extra_data_len(uint64_t len) {
        extra_data_len = len;
      }
  };

  RGWHTTPStreamRWRequest(CephContext *_cct, const string& _method, const string& _url,
                         param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params) {
  }
  RGWHTTPStreamRWRequest(CephContext *_cct, const string& _method, const string& _url, ReceiveCB *_cb,
                         param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params),
									cb(_cb) {
  }
  virtual ~RGWHTTPStreamRWRequest() override {}

  void set_outbl(bufferlist& _outbl) {
    outbl.swap(_outbl);
  }

  void set_in_cb(ReceiveCB *_cb) { cb = _cb; }
  void set_write_drain_cb(RGWWriteDrainCB *_cb) { write_drain_cb = _cb; }

  void unpause_receive();

  void add_send_data(bufferlist& bl);

  void set_stream_write(bool s);

  uint64_t get_pending_send_size();

  /* finish streaming writes */
  void finish_write();
};

class RGWRESTStreamRWRequest : public RGWHTTPStreamRWRequest {
protected:
  HostStyle host_style;
public:
  RGWRESTStreamRWRequest(CephContext *_cct, const string& _method, const string& _url, RGWHTTPStreamRWRequest::ReceiveCB *_cb,
		param_vec_t *_headers, param_vec_t *_params, HostStyle _host_style = PathStyle) : RGWHTTPStreamRWRequest(_cct, _method, _url, _cb, _headers, _params), host_style(_host_style) {
  }
  virtual ~RGWRESTStreamRWRequest() override {}

  int send_prepare(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource, bufferlist *send_data = nullptr /* optional input data */);
  int send_prepare(RGWAccessKey& key, map<string, string>& extra_headers, const rgw_obj& obj);
  int send(RGWHTTPManager *mgr);

  int send_request(RGWAccessKey& key, map<string, string>& extra_headers, const rgw_obj& obj, RGWHTTPManager *mgr);
  int send_request(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource, RGWHTTPManager *mgr, bufferlist *send_data = nullptr /* optional input data */);

  int complete_request(string *etag = nullptr,
                       real_time *mtime = nullptr,
                       uint64_t *psize = nullptr,
                       map<string, string> *pattrs = nullptr,
                       map<string, string> *pheaders = nullptr);

  void add_params(param_vec_t *params);

private:
  int do_send_prepare(RGWAccessKey *key, map<string, string>& extra_headers, const string& resource, bufferlist *send_data = nullptr /* optional input data */);
};

class RGWRESTStreamReadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamReadRequest(CephContext *_cct, const string& _url, ReceiveCB *_cb, param_vec_t *_headers,
		param_vec_t *_params, HostStyle _host_style = PathStyle) : RGWRESTStreamRWRequest(_cct, "GET", _url, _cb, _headers, _params, _host_style) {}
};

class RGWRESTStreamHeadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamHeadRequest(CephContext *_cct, const string& _url, ReceiveCB *_cb, param_vec_t *_headers,
		param_vec_t *_params) : RGWRESTStreamRWRequest(_cct, "HEAD", _url, _cb, _headers, _params) {}
};

class RGWRESTStreamS3PutObj : public RGWRESTStreamRWRequest {
  RGWGetDataCB *out_cb;
  RGWEnv new_env;
  req_info new_info;
  RGWRESTGenerateHTTPHeaders headers_gen;
public:
  RGWRESTStreamS3PutObj(CephContext *_cct, const string& _method, const string& _url, param_vec_t *_headers,
		param_vec_t *_params, HostStyle _host_style) : RGWRESTStreamRWRequest(_cct, _method, _url, nullptr, _headers, _params, _host_style),
                out_cb(NULL), new_info(cct, &new_env), headers_gen(_cct, &new_env, &new_info) {}
  ~RGWRESTStreamS3PutObj() override;

  void send_init(rgw::sal::RGWObject* obj);
  int send_ready(RGWAccessKey& key, map<string, bufferlist>& rgw_attrs, bool send);
  int send_ready(RGWAccessKey& key, const map<string, string>& http_attrs,
                 RGWAccessControlPolicy& policy, bool send);
  int send_ready(RGWAccessKey& key, bool send);

  int put_obj_init(RGWAccessKey& key, rgw::sal::RGWObject* obj, uint64_t obj_size, map<string, bufferlist>& attrs, bool send);

  RGWGetDataCB *get_out_cb() { return out_cb; }
};
