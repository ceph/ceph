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
  std::map<std::string, std::string> out_headers;
  param_vec_t params;

  bufferlist::iterator *send_iter;

  size_t max_response; /* we need this as we don't stream out response */
  bufferlist response;

  virtual int handle_header(const std::string& name, const std::string& val);
  void get_params_str(std::map<std::string, std::string>& extra_args, std::string& dest);

public:
  RGWHTTPSimpleRequest(CephContext *_cct, const std::string& _method, const std::string& _url,
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

  void get_out_headers(std::map<std::string, std::string> *pheaders); /* modifies out_headers */

  int get_http_status() { return http_status; }
  int get_status();
};

class RGWRESTSimpleRequest : public RGWHTTPSimpleRequest {
  std::optional<std::string> api_name;
public:
  RGWRESTSimpleRequest(CephContext *_cct, const std::string& _method, const std::string& _url,
                       param_vec_t *_headers, param_vec_t *_params,
                       std::optional<std::string> _api_name) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params), api_name(_api_name) {}

  int forward_request(const DoutPrefixProvider *dpp, const RGWAccessKey& key, const req_info& info, size_t max_response, bufferlist *inbl, bufferlist *outbl, optional_yield y, std::string service="");
};

class RGWWriteDrainCB {
public:
  RGWWriteDrainCB() = default;
  virtual ~RGWWriteDrainCB() = default;
  virtual void notify(uint64_t pending_size) = 0;
};

class RGWRESTGenerateHTTPHeaders : public DoutPrefix {
  CephContext *cct;
  RGWEnv *new_env;
  req_info *new_info;
  std::string region;
  std::string service;
  std::string method;
  std::string url;
  std::string resource;

public:
  RGWRESTGenerateHTTPHeaders(CephContext *_cct, RGWEnv *_env, req_info *_info);
  void init(const std::string& method, const std::string& host,
            const std::string& resource_prefix, const std::string& url,
            const std::string& resource, const param_vec_t& params,
            std::optional<std::string> api_name);
  void set_extra_headers(const std::map<std::string, std::string>& extra_headers);
  int set_obj_attrs(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>& rgw_attrs);
  void set_http_attrs(const std::map<std::string, std::string>& http_attrs);
  void set_policy(const RGWAccessControlPolicy& policy);
  int sign(const DoutPrefixProvider *dpp, RGWAccessKey& key, const bufferlist *opt_content);

  const std::string& get_url() { return url; }
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
  bufferlist in_data;
  size_t chunk_ofs{0};
  size_t ofs{0};
  uint64_t write_ofs{0};
  bool read_paused{false};
  bool send_paused{false};
  bool stream_writes{false};
  bool write_stream_complete{false};
protected:
  bufferlist outbl;

  int handle_header(const std::string& name, const std::string& val) override;
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

  RGWHTTPStreamRWRequest(CephContext *_cct, const std::string& _method, const std::string& _url,
                         param_vec_t *_headers, param_vec_t *_params) : RGWHTTPSimpleRequest(_cct, _method, _url, _headers, _params) {
  }
  RGWHTTPStreamRWRequest(CephContext *_cct, const std::string& _method, const std::string& _url, ReceiveCB *_cb,
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

  virtual int send(RGWHTTPManager *mgr);

  int complete_request(optional_yield y,
                       std::string *etag = nullptr,
                       real_time *mtime = nullptr,
                       uint64_t *psize = nullptr,
                       std::map<std::string, std::string> *pattrs = nullptr,
                       std::map<std::string, std::string> *pheaders = nullptr);
};

class RGWRESTStreamRWRequest : public RGWHTTPStreamRWRequest {
  std::optional<RGWAccessKey> sign_key;
  std::optional<RGWRESTGenerateHTTPHeaders> headers_gen;
  RGWEnv new_env;
  req_info new_info;

protected:
  std::optional<std::string> api_name;
  HostStyle host_style;
public:
  RGWRESTStreamRWRequest(CephContext *_cct, const std::string& _method, const std::string& _url, RGWHTTPStreamRWRequest::ReceiveCB *_cb,
                         param_vec_t *_headers, param_vec_t *_params,
                         std::optional<std::string> _api_name, HostStyle _host_style = PathStyle) :
                         RGWHTTPStreamRWRequest(_cct, _method, _url, _cb, _headers, _params),
                         new_info(_cct, &new_env),
                         api_name(_api_name), host_style(_host_style) {
  }
  virtual ~RGWRESTStreamRWRequest() override {}

  int send_prepare(const DoutPrefixProvider *dpp, RGWAccessKey *key, std::map<std::string, std::string>& extra_headers, const std::string& resource, bufferlist *send_data = nullptr /* optional input data */);
  int send_prepare(const DoutPrefixProvider *dpp, RGWAccessKey& key, std::map<std::string, std::string>& extra_headers, const rgw_obj& obj);
  int send(RGWHTTPManager *mgr) override;

  int send_request(const DoutPrefixProvider *dpp, RGWAccessKey& key, std::map<std::string, std::string>& extra_headers, const rgw_obj& obj, RGWHTTPManager *mgr);
  int send_request(const DoutPrefixProvider *dpp, RGWAccessKey *key, std::map<std::string, std::string>& extra_headers, const std::string& resource, RGWHTTPManager *mgr, bufferlist *send_data = nullptr /* optional input data */);

  void add_params(param_vec_t *params);

private:
  int do_send_prepare(const DoutPrefixProvider *dpp, RGWAccessKey *key, std::map<std::string, std::string>& extra_headers, const std::string& resource, bufferlist *send_data = nullptr /* optional input data */);
};

class RGWRESTStreamReadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamReadRequest(CephContext *_cct, const std::string& _url, ReceiveCB *_cb, param_vec_t *_headers,
		param_vec_t *_params, std::optional<std::string> _api_name,
                HostStyle _host_style = PathStyle) : RGWRESTStreamRWRequest(_cct, "GET", _url, _cb, _headers, _params, _api_name, _host_style) {}
};

class RGWRESTStreamHeadRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamHeadRequest(CephContext *_cct, const std::string& _url, ReceiveCB *_cb, param_vec_t *_headers,
		param_vec_t *_params, std::optional<std::string> _api_name) : RGWRESTStreamRWRequest(_cct, "HEAD", _url, _cb, _headers, _params, _api_name) {}
};

class RGWRESTStreamSendRequest : public RGWRESTStreamRWRequest {
public:
  RGWRESTStreamSendRequest(CephContext *_cct, const std::string& method,
                           const std::string& _url,
                           ReceiveCB *_cb, param_vec_t *_headers, param_vec_t *_params,
                           std::optional<std::string> _api_name,
                           HostStyle _host_style = PathStyle) : RGWRESTStreamRWRequest(_cct, method, _url, _cb, _headers, _params, _api_name, _host_style) {}
};

class RGWRESTStreamS3PutObj : public RGWHTTPStreamRWRequest {
  std::optional<std::string> api_name;
  HostStyle host_style;
  RGWGetDataCB *out_cb;
  RGWEnv new_env;
  req_info new_info;
  RGWRESTGenerateHTTPHeaders headers_gen;
public:
  RGWRESTStreamS3PutObj(CephContext *_cct, const std::string& _method, const std::string& _url, param_vec_t *_headers,
		param_vec_t *_params, std::optional<std::string> _api_name,
                HostStyle _host_style) : RGWHTTPStreamRWRequest(_cct, _method, _url, nullptr, _headers, _params),
                api_name(_api_name), host_style(_host_style),
                out_cb(NULL), new_info(cct, &new_env), headers_gen(_cct, &new_env, &new_info) {}
  ~RGWRESTStreamS3PutObj() override;

  void send_init(const rgw_obj& obj);
  void send_ready(const DoutPrefixProvider *dpp, RGWAccessKey& key, std::map<std::string, bufferlist>& rgw_attrs);
  void send_ready(const DoutPrefixProvider *dpp, RGWAccessKey& key, const std::map<std::string, std::string>& http_attrs,
                  RGWAccessControlPolicy& policy);
  void send_ready(const DoutPrefixProvider *dpp, RGWAccessKey& key);

  void put_obj_init(const DoutPrefixProvider *dpp, RGWAccessKey& key, const rgw_obj& obj, std::map<std::string, bufferlist>& attrs);

  RGWGetDataCB *get_out_cb() { return out_cb; }
};
