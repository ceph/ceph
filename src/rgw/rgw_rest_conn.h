// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rados.h"
#include "rgw_rest_client.h"
#include "common/ceph_json.h"
#include "common/RefCountedObj.h"

#include <atomic>

class CephContext;
class RGWSI_Zone;

template <class T>
static int parse_decode_json(CephContext *cct, T& t, bufferlist& bl)
{
  JSONParser p;
  int ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    return ret;
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
    string k = pp->key;
    string v = (pp->val ? pp->val : "");
    params.emplace_back(make_pair(std::move(k), std::move(v)));
    ++pp;
  }
}

// copy a null-terminated rgw_http_param_pair list into a list of string pairs
inline param_vec_t make_param_list(const rgw_http_param_pair* pp)
{
  param_vec_t params;
  append_param_list(params, pp);
  return params;
}

inline param_vec_t make_param_list(const map<string, string> *pp)
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
  vector<string> endpoints;
  RGWAccessKey key;
  string self_zone_group;
  string remote_id;
  HostStyle host_style;
  std::atomic<int64_t> counter = { 0 };

public:

  RGWRESTConn(CephContext *_cct, RGWSI_Zone *zone_svc, const string& _remote_id, const list<string>& endpoints, HostStyle _host_style = PathStyle);
  RGWRESTConn(CephContext *_cct, RGWSI_Zone *zone_svc, const string& _remote_id, const list<string>& endpoints, RGWAccessKey _cred, HostStyle _host_style = PathStyle);

  // custom move needed for atomic
  RGWRESTConn(RGWRESTConn&& other);
  RGWRESTConn& operator=(RGWRESTConn&& other);
  virtual ~RGWRESTConn() = default;

  int get_url(string& endpoint);
  string get_url();
  const string& get_self_zonegroup() {
    return self_zone_group;
  }
  const string& get_remote_id() {
    return remote_id;
  }
  RGWAccessKey& get_key() {
    return key;
  }

  HostStyle get_host_style() {
    return host_style;
  }

  CephContext *get_ctx() {
    return cct;
  }
  size_t get_endpoint_count() const { return endpoints.size(); }

  virtual void populate_params(param_vec_t& params, const rgw_user *uid, const string& zonegroup);

  /* sync request */
  int forward(const rgw_user& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl);


  /* async requests */
  int put_obj_send_init(rgw_obj& obj, const rgw_http_param_pair *extra_params, RGWRESTStreamS3PutObj **req);
  int put_obj_async(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                    map<string, bufferlist>& attrs, bool send, RGWRESTStreamS3PutObj **req);
  int complete_request(RGWRESTStreamS3PutObj *req, string& etag, ceph::real_time *mtime);

  struct get_obj_params {
    rgw_user uid;
    req_info *info{nullptr};
    const ceph::real_time *mod_ptr{nullptr};
    const ceph::real_time *unmod_ptr{nullptr};
    bool high_precision_time{true};

    string etag;

    uint32_t mod_zone_id{0};
    uint64_t mod_pg_ver{0};

    bool prepend_metadata{false};
    bool get_op{false};
    bool rgwx_stat{false};
    bool sync_manifest{false};

    bool skip_decrypt{true};
    RGWHTTPStreamRWRequest::ReceiveCB *cb{nullptr};

    bool range_is_set{false};
    uint64_t range_start{0};
    uint64_t range_end{0};
  };

  int get_obj(const rgw_obj& obj, const get_obj_params& params, bool send, RGWRESTStreamRWRequest **req);

  int get_obj(const rgw_user& uid, req_info *info /* optional */, const rgw_obj& obj,
              const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
              uint32_t mod_zone_id, uint64_t mod_pg_ver,
              bool prepend_metadata, bool get_op, bool rgwx_stat, bool sync_manifest,
              bool skip_decrypt, bool send, RGWHTTPStreamRWRequest::ReceiveCB *cb, RGWRESTStreamRWRequest **req);
  int complete_request(RGWRESTStreamRWRequest *req,
                       string *etag,
                       ceph::real_time *mtime,
                       uint64_t *psize,
                       map<string, string> *pattrs,
                       map<string, string> *pheaders);

  int get_resource(const string& resource,
		   param_vec_t *extra_params,
                   map<string, string>* extra_headers,
                   bufferlist& bl,
                   bufferlist *send_data = nullptr,
                   RGWHTTPManager *mgr = nullptr);

  template <class T>
  int get_json_resource(const string& resource, param_vec_t *params, bufferlist *in_data, T& t);
  template <class T>
  int get_json_resource(const string& resource, param_vec_t *params, T& t);
  template <class T>
  int get_json_resource(const string& resource, const rgw_http_param_pair *pp, T& t);

private:
  void populate_zonegroup(param_vec_t& params, const string& zonegroup) {
    if (!zonegroup.empty()) {
      params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "zonegroup", zonegroup));
    }
  }
  void populate_uid(param_vec_t& params, const rgw_user *uid) {
    if (uid) {
      string uid_str = uid->to_str();
      if (!uid->empty()){
        params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "uid", uid_str));
      }
    }
  }
};

class S3RESTConn : public RGWRESTConn {

public:

  S3RESTConn(CephContext *_cct, RGWSI_Zone *svc_zone, const string& _remote_id, const list<string>& endpoints, HostStyle _host_style = PathStyle) :
    RGWRESTConn(_cct, svc_zone, _remote_id, endpoints, _host_style) {}

  S3RESTConn(CephContext *_cct, RGWSI_Zone *svc_zone, const string& _remote_id, const list<string>& endpoints, RGWAccessKey _cred, HostStyle _host_style = PathStyle):
    RGWRESTConn(_cct, svc_zone, _remote_id, endpoints, _cred, _host_style) {}
  ~S3RESTConn() override = default;

  void populate_params(param_vec_t& params, const rgw_user *uid, const string& zonegroup) override {
    // do not populate any params in S3 REST Connection.
    return;
  }
};


template<class T>
int RGWRESTConn::get_json_resource(const string& resource, param_vec_t *params, bufferlist *in_data, T& t)
{
  bufferlist bl;
  int ret = get_resource(resource, params, nullptr, bl, in_data);
  if (ret < 0) {
    return ret;
  }

  ret = parse_decode_json(cct, t, bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

template<class T>
int RGWRESTConn::get_json_resource(const string& resource, param_vec_t *params, T& t)
{
  return get_json_resource(resource, params, nullptr, t);
}

template<class T>
int RGWRESTConn::get_json_resource(const string& resource,  const rgw_http_param_pair *pp, T& t)
{
  param_vec_t params = make_param_list(pp);
  return get_json_resource(resource, &params, t);
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
  string resource;
  param_vec_t params;
  map<string, string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamReadRequest req;

  void init_common(param_vec_t *extra_headers);

public:
  RGWRESTReadResource(RGWRESTConn *_conn,
		      const string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  RGWRESTReadResource(RGWRESTConn *_conn,
		      const string& _resource,
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

  int read();

  int aio_read();

  string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  int wait(bufferlist *pbl) {
    int ret = req.wait();
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
  int wait(T *dest);

  template <class T>
  int fetch(T *dest);
};


template <class T>
int RGWRESTReadResource::decode_resource(T *dest)
{
  int ret = req.get_status();
  if (ret < 0) {
    return ret;
  }
  ret = parse_decode_json(cct, *dest, bl);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T>
int RGWRESTReadResource::fetch(T *dest)
{
  int ret = read();
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
int RGWRESTReadResource::wait(T *dest)
{
  int ret = req.wait();
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
  string method;
  string resource;
  param_vec_t params;
  map<string, string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamRWRequest req;

  void init_common(param_vec_t *extra_headers);

public:
  RGWRESTSendResource(RGWRESTConn *_conn,
                      const string& _method,
		      const string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  RGWRESTSendResource(RGWRESTConn *_conn,
                      const string& _method,
		      const string& _resource,
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

  template <class T, class E>
  int decode_resource(T *dest, E *err_result);

  int send(bufferlist& bl);

  int aio_send(bufferlist& bl);

  string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  int wait(bufferlist *pbl) {
    int ret = req.wait();
    *pbl = bl;
    if (ret < 0) {
      return ret;
    }

    if (req.get_status() < 0) {
      return req.get_status();
    }
    return 0;
  }

  template <class T, class E = int>
  int wait(T *dest, E *err_result = nullptr);
};

template <class T, class E>
int RGWRESTSendResource::decode_resource(T *dest, E *err_result)
{
  int ret = req.get_status();
  if (ret < 0) {
    if (err_result) {
      parse_decode_json(cct, *err_result, bl);
    }
    return ret;
  }

  if (!dest) {
    return 0;
  }

  ret = parse_decode_json(cct, *dest, bl);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

template <class T, class E>
int RGWRESTSendResource::wait(T *dest, E *err_result)
{
  int ret = req.wait();
  if (ret < 0) {
    if (err_result) {
      parse_decode_json(cct, *err_result, bl);
    }
    return ret;
  }

  ret = decode_resource(dest, err_result);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

class RGWRESTPostResource : public RGWRESTSendResource {
public:
  RGWRESTPostResource(RGWRESTConn *_conn,
		      const string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "POST", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTPostResource(RGWRESTConn *_conn,
		      const string& _resource,
		      param_vec_t& params,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "POST", _resource,
                                                                  params, extra_headers, _mgr) {}

};

class RGWRESTPutResource : public RGWRESTSendResource {
public:
  RGWRESTPutResource(RGWRESTConn *_conn,
		     const string& _resource,
		     const rgw_http_param_pair *pp,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "PUT", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTPutResource(RGWRESTConn *_conn,
		     const string& _resource,
		     param_vec_t& params,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "PUT", _resource,
                                                                  params, extra_headers, _mgr) {}

};

class RGWRESTDeleteResource : public RGWRESTSendResource {
public:
  RGWRESTDeleteResource(RGWRESTConn *_conn,
		     const string& _resource,
		     const rgw_http_param_pair *pp,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "DELETE", _resource,
                                                                  pp, extra_headers, _mgr) {}

  RGWRESTDeleteResource(RGWRESTConn *_conn,
		     const string& _resource,
		     param_vec_t& params,
		     param_vec_t *extra_headers,
		     RGWHTTPManager *_mgr) : RGWRESTSendResource(_conn, "DELETE", _resource,
                                                                  params, extra_headers, _mgr) {}

};



#endif
