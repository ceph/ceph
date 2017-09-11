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
class RGWRados;

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

// copy a null-terminated rgw_http_param_pair list into a list of string pairs
inline param_vec_t make_param_list(const rgw_http_param_pair* pp)
{
  param_vec_t params;
  while (pp && pp->key) {
    string k = pp->key;
    string v = (pp->val ? pp->val : "");
    params.emplace_back(make_pair(std::move(k), std::move(v)));
    ++pp;
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
  std::atomic<int64_t> counter = { 0 };

public:

  RGWRESTConn(CephContext *_cct, RGWRados *store, const string& _remote_id, const list<string>& endpoints);
  // custom move needed for atomic
  RGWRESTConn(RGWRESTConn&& other);
  RGWRESTConn& operator=(RGWRESTConn&& other);

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

  CephContext *get_ctx() {
    return cct;
  }
  size_t get_endpoint_count() const { return endpoints.size(); }

  /* sync request */
  int forward(const rgw_user& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl);

  /* async request */
  int put_obj_init(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                   map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req);
  int complete_request(RGWRESTStreamWriteRequest *req, string& etag, ceph::real_time *mtime);

  int get_obj(const rgw_user& uid, req_info *info /* optional */, rgw_obj& obj,
              const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
              uint32_t mod_zone_id, uint64_t mod_pg_ver,
              bool prepend_metadata, bool get_op, bool rgwx_stat, bool sync_manifest,
              bool skip_decrypt, RGWGetDataCB *cb, RGWRESTStreamRWRequest **req);
  int complete_request(RGWRESTStreamRWRequest *req, string& etag, ceph::real_time *mtime, uint64_t *psize, map<string, string>& attrs);

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

class RGWStreamIntoBufferlist : public RGWGetDataCB {
  bufferlist& bl;
public:
  RGWStreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, off_t bl_ofs, off_t bl_len) override {
    bl.claim_append(inbl);
    return bl_len;
  }
};

class RGWRESTReadResource : public RefCountedObject {
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

  void set_user_info(void *user_info) {
    req.set_user_info(user_info);
  }
  void *get_user_info() {
    return req.get_user_info();
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

  int wait_bl(bufferlist *pbl) {
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

class RGWRESTSendResource : public RefCountedObject {
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

  void set_user_info(void *user_info) {
    req.set_user_info(user_info);
  }
  void *get_user_info() {
    return req.get_user_info();
  }

  template <class T>
  int decode_resource(T *dest);

  int send(bufferlist& bl);

  int aio_send(bufferlist& bl);

  string to_str() {
    return req.to_str();
  }

  int get_http_status() {
    return req.get_http_status();
  }

  int wait_bl(bufferlist *pbl) {
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
};

template <class T>
int RGWRESTSendResource::decode_resource(T *dest)
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
int RGWRESTSendResource::wait(T *dest)
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
