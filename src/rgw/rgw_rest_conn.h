// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest_client.h"
#include "common/ceph_json.h"
#include "common/RefCountedObj.h"
#include "include/common_fwd.h"
#include "rgw_sal_fwd.h"

#include <atomic>

class RGWSI_Zone;

template<class T>
inline int parse_decode_json(T& t, bufferlist& bl)
{
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    return -EINVAL;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    return -EINVAL;
  }
  return 0;
}

struct rgw_http_param_pair {
  const char *key;
  const char *val;
};

// append a null-terminated rgw_http_param_pair list into a list of string pairs
inline void append_param_list(param_vec_t& params, const rgw_http_param_pair* pp)
{
  while (pp && pp->key) {
    std::string k = pp->key;
    std::string v = (pp->val ? pp->val : "");
    params.emplace_back(make_pair(std::move(k), std::move(v)));
    ++pp;
  }
}

// copy a null-terminated rgw_http_param_pair list into a list of std::string pairs
inline param_vec_t make_param_list(const rgw_http_param_pair* pp)
{
  param_vec_t params;
  append_param_list(params, pp);
  return params;
}

inline param_vec_t make_param_list(const std::map<std::string, std::string> *pp)
{
  param_vec_t params;
  if (!pp) {
    return params;
  }
  for (auto iter : *pp) {
    params.emplace_back(make_pair(iter.first, iter.second));
  }
  return params;
}

class RGWRESTConn
{
  CephContext *cct;
  std::vector<std::string> endpoints;
  RGWAccessKey key;
  std::string self_zone_group;
  std::string remote_id;
  std::optional<std::string> api_name;
  HostStyle host_style;
  std::atomic<int64_t> counter = { 0 };

public:

  RGWRESTConn(CephContext *_cct,
              rgw::sal::Driver* driver,
              const std::string& _remote_id,
              const std::list<std::string>& endpoints,
              std::optional<std::string> _api_name,
              HostStyle _host_style = PathStyle);
  RGWRESTConn(CephContext *_cct,
	      const std::string& _remote_id,
	      const std::list<std::string>& endpoints,
	      RGWAccessKey _cred,
	      std::string _zone_group,
	      std::optional<std::string> _api_name,
	      HostStyle _host_style = PathStyle);

  // custom move needed for atomic
  RGWRESTConn(RGWRESTConn&& other);
  RGWRESTConn& operator=(RGWRESTConn&& other);
  virtual ~RGWRESTConn() = default;

  int get_url(std::string& endpoint);
  std::string get_url();
  const std::string& get_self_zonegroup() {
    return self_zone_group;
  }
  const std::string& get_remote_id() {
    return remote_id;
  }
  RGWAccessKey& get_key() {
    return key;
  }

  std::optional<std::string> get_api_name() const {
    return api_name;
  }

  HostStyle get_host_style() {
    return host_style;
  }

  CephContext *get_ctx() {
    return cct;
  }
  size_t get_endpoint_count() const { return endpoints.size(); }

  virtual void populate_params(param_vec_t& params, const rgw_user *uid, const std::string& zonegroup);

  /* sync request */
  int forward(const DoutPrefixProvider *dpp, const rgw_user& uid, const req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl, optional_yield y);

  /* sync request */
  int forward_iam_request(const DoutPrefixProvider *dpp, const req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl, optional_yield y);


  /* async requests */
  int put_obj_send_init(const rgw_obj& obj, const rgw_http_param_pair *extra_params, RGWRESTStreamS3PutObj **req);
  int put_obj_async_init(const DoutPrefixProvider *dpp, const rgw_user& uid, const rgw_obj& obj,
                         std::map<std::string, bufferlist>& attrs, RGWRESTStreamS3PutObj **req);
  int complete_request(RGWRESTStreamS3PutObj *req, std::string& etag,
                       ceph::real_time *mtime, optional_yield y);

  struct get_obj_params {
    rgw_user uid;
    req_info *info{nullptr};
    const ceph::real_time *mod_ptr{nullptr};
    const ceph::real_time *unmod_ptr{nullptr};
    bool high_precision_time{true};

    std::string etag;

    uint32_t mod_zone_id{0};
    uint64_t mod_pg_ver{0};

    bool prepend_metadata{false};
    bool get_op{false};
    bool rgwx_stat{false};
    bool sync_manifest{false};
    bool sync_cloudtiered{false};

    bool skip_decrypt{true};
    RGWHTTPStreamRWRequest::ReceiveCB *cb{nullptr};

    bool range_is_set{false};
    uint64_t range_start{0};
    uint64_t range_end{0};
    rgw_zone_set_entry *dst_zone_trace{nullptr};
  };

  int get_obj(const DoutPrefixProvider *dpp, const rgw_obj& obj, const get_obj_params& params, bool send, RGWRESTStreamRWRequest **req);

  int get_obj(const DoutPrefixProvider *dpp, const rgw_user& uid, req_info *info /* optional */, const rgw_obj& obj,
              const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
              uint32_t mod_zone_id, uint64_t mod_pg_ver,
              bool prepend_metadata, bool get_op, bool rgwx_stat, bool sync_manifest,
              bool skip_decrypt, rgw_zone_set_entry *dst_zone_trace, bool sync_cloudtiered,
              bool send, RGWHTTPStreamRWRequest::ReceiveCB *cb, RGWRESTStreamRWRequest **req);
  int complete_request(RGWRESTStreamRWRequest *req,
                       std::string *etag,
                       ceph::real_time *mtime,
                       uint64_t *psize,
                       std::map<std::string, std::string> *pattrs,
                       std::map<std::string, std::string> *pheaders,
                       optional_yield y);

  int get_resource(const DoutPrefixProvider *dpp,
                   const std::string& resource,
		   param_vec_t *extra_params,
                   std::map<std::string, std::string>* extra_headers,
                   bufferlist& bl,
                   bufferlist *send_data,
                   RGWHTTPManager *mgr,
                   optional_yield y);

  int send_resource(const DoutPrefixProvider *dpp,
                   const std::string& method,
                   const std::string& resource,
		           rgw_http_param_pair *extra_params,
                   std::map<std::string, std::string>* extra_headers,
                   bufferlist& bl,
                   bufferlist *send_data,
                   RGWHTTPManager *mgr,
                   optional_yield y);

  template <class T>
  int get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, param_vec_t *params,
                        bufferlist *in_data, optional_yield y, T& t);
  template <class T>
  int get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, param_vec_t *params,
                        optional_yield y, T& t);
  template <class T>
  int get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, const rgw_http_param_pair *pp,
                        optional_yield y, T& t);

private:
  void populate_zonegroup(param_vec_t& params, const std::string& zonegroup) {
    if (!zonegroup.empty()) {
      params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "zonegroup", zonegroup));
    }
  }
  void populate_uid(param_vec_t& params, const rgw_user *uid) {
    if (uid) {
      std::string uid_str = uid->to_str();
      if (!uid->empty()){
        params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "uid", uid_str));
      }
    }
  }
};

class S3RESTConn : public RGWRESTConn {

public:

  S3RESTConn(CephContext *_cct, rgw::sal::Driver* driver, const std::string& _remote_id, const std::list<std::string>& endpoints, std::optional<std::string> _api_name, HostStyle _host_style = PathStyle) :
    RGWRESTConn(_cct, driver, _remote_id, endpoints, _api_name, _host_style) {}
  S3RESTConn(CephContext *_cct, const std::string& _remote_id, const std::list<std::string>& endpoints, RGWAccessKey _cred, std::string _zone_group, std::optional<std::string> _api_name, HostStyle _host_style = PathStyle):
    RGWRESTConn(_cct, _remote_id, endpoints, _cred, _zone_group, _api_name, _host_style) {}
  ~S3RESTConn() override = default;

  void populate_params(param_vec_t& params, const rgw_user *uid, const std::string& zonegroup) override {
    // do not populate any params in S3 REST Connection.
    return;
  }
};


template<class T>
int RGWRESTConn::get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, param_vec_t *params,
                                   bufferlist *in_data, optional_yield y, T& t)
{
  bufferlist bl;
  int ret = get_resource(dpp, resource, params, nullptr, bl, in_data, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  ret = parse_decode_json(t, bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

template<class T>
int RGWRESTConn::get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, param_vec_t *params,
                                   optional_yield y, T& t)
{
  return get_json_resource(dpp, resource, params, nullptr, y, t);
}

template<class T>
int RGWRESTConn::get_json_resource(const DoutPrefixProvider *dpp, const std::string& resource, const rgw_http_param_pair *pp,
                                   optional_yield y, T& t)
{
  param_vec_t params = make_param_list(pp);
  return get_json_resource(dpp, resource, &params, y, t);
}

class RGWStreamIntoBufferlist : public RGWHTTPStreamRWRequest::ReceiveCB {
  bufferlist& bl;
public:
  explicit RGWStreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, bool *pause) override {
    bl.claim_append(inbl);
    return inbl.length();
  }
};

class RGWRESTReadResource : public RefCountedObject, public RGWIOProvider {
  CephContext *cct;
  RGWRESTConn *conn;
  std::string resource;
  param_vec_t params;
  std::map<std::string, std::string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamReadRequest req;

  void init_common(param_vec_t *extra_headers);

public:
  RGWRESTReadResource(RGWRESTConn *_conn,
		      const std::string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  RGWRESTReadResource(RGWRESTConn *_conn,
		      const std::string& _resource,
		      param_vec_t& _params,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);
  ~RGWRESTReadResource() = default;

  rgw_io_id get_io_id(int io_type) {
    return req.get_io_id(io_type);
  }

  void set_io_user_info(void *user_info) override {
    req.set_io_user_info(user_info);
  }

  void *get_io_user_info() override {
    return req.get_io_user_info();
  }

  template <class T>
  int decode_resource(T *dest);

  int read(const DoutPrefixProvider *dpp, optional_yield y);

  int aio_read(const DoutPrefixProvider *dpp);

  std::string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  int wait(bufferlist *pbl, optional_yield y) {
    int ret = req.wait(y);
    if (ret < 0) {
      return ret;
    }

    if (req.get_status() < 0) {
      return req.get_status();
    }
    *pbl = bl;
    return 0;
  }

  template <class T>
  int wait(T *dest, optional_yield y);

  template <class T>
  int fetch(const DoutPrefixProvider *dpp, T *dest, optional_yield y);
};


template <class T>
int RGWRESTReadResource::decode_resource(T *dest)
{
  int ret = req.get_status();
  if (ret < 0) {
    return ret;
  }
  ret = parse_decode_json(*dest, bl);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T>
int RGWRESTReadResource::fetch(const DoutPrefixProvider *dpp, T *dest, optional_yield y)
{
  int ret = read(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = decode_resource(dest);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T>
int RGWRESTReadResource::wait(T *dest, optional_yield y)
{
  int ret = req.wait(y);
  if (ret < 0) {
    return ret;
  }

  ret = decode_resource(dest);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

class RGWRESTSendResource : public RefCountedObject, public RGWIOProvider {
  CephContext *cct;
  RGWRESTConn *conn;
  std::string method;
  std::string resource;
  param_vec_t params;
  std::map<std::string, std::string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamRWRequest req;

  void init_common(param_vec_t *extra_headers);

public:
  RGWRESTSendResource(RGWRESTConn *_conn,
                      const std::string& _method,
		      const std::string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  RGWRESTSendResource(RGWRESTConn *_conn,
                      const std::string& _method,
		      const std::string& _resource,
		      param_vec_t& params,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  ~RGWRESTSendResource() = default;

  rgw_io_id get_io_id(int io_type) {
    return req.get_io_id(io_type);
  }

  void set_io_user_info(void *user_info) override {
    req.set_io_user_info(user_info);
  }

  void *get_io_user_info() override {
    return req.get_io_user_info();
  }

  int send(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y);

  int aio_send(const DoutPrefixProvider *dpp, bufferlist& bl);

  std::string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  template <class E = int>
  int wait(bufferlist *pbl, optional_yield y, E *err_result = nullptr) {
    int ret = req.wait(y);
    *pbl = bl;

    if (ret < 0 && err_result ) {
      ret = parse_decode_json(*err_result, bl);
    }

    return req.get_status();
  }

  template <class T, class E = int>
  int wait(T *dest, optional_yield y, E *err_result = nullptr);
};

template <class T, class E>
int RGWRESTSendResource::wait(T *dest, optional_yield y, E *err_result)
{
  int ret = req.wait(y);
  if (ret >= 0) {
    ret = req.get_status();
  }

  if (ret < 0 && err_result) {
    ret = parse_decode_json(*err_result, bl);
  }

  if (ret < 0) {
    return ret;
  }

  ret = parse_decode_json(*dest, bl);
  if (ret < 0) {
    return ret;
  }
  return 0;

}

class RGWRESTPostResource : public RGWRESTSendResource {
public:
  RGWRESTPostResource(RGWRESTConn *_conn,
		      const std::string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "POST", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTPostResource(RGWRESTConn *_conn,
		      const std::string& _resource,
		      param_vec_t& params,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "POST", _resource,
                                                                  params, extra_headers, _mgr) {}

};

class RGWRESTPutResource : public RGWRESTSendResource {
public:
  RGWRESTPutResource(RGWRESTConn *_conn,
		     const std::string& _resource,
		     const rgw_http_param_pair *pp,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "PUT", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTPutResource(RGWRESTConn *_conn,
		     const std::string& _resource,
		     param_vec_t& params,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "PUT", _resource,
                                                                  params, extra_headers, _mgr) {}

};

class RGWRESTDeleteResource : public RGWRESTSendResource {
public:
  RGWRESTDeleteResource(RGWRESTConn *_conn,
		     const std::string& _resource,
		     const rgw_http_param_pair *pp,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "DELETE", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTDeleteResource(RGWRESTConn *_conn,
		     const std::string& _resource,
		     param_vec_t& params,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "DELETE", _resource,
                                                                  params, extra_headers, _mgr) {}

};
