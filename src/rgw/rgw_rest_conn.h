// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_CONN_H
#define CEPH_RGW_REST_CONN_H

#include "rgw_rados.h"
#include "rgw_rest_client.h"
#include "common/ceph_json.h"
#include "common/RefCountedObj.h"


class CephContext;
class RGWRados;
class RGWGetObjData;

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
  atomic_t counter;

public:

  RGWRESTConn(CephContext *_cct, RGWRados *store, const string& _remote_id, const list<string>& endpoints);
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
              bool prepend_metadata, bool sync_manifest, RGWGetDataCB *cb,
              RGWRESTStreamReadRequest **req);
  int complete_request(RGWRESTStreamReadRequest *req, string& etag, ceph::real_time *mtime, map<string, string>& attrs);

  int get_resource(const string& resource,
		   param_vec_t *extra_params,
                   map<string, string>* extra_headers,
                   bufferlist& bl, RGWHTTPManager *mgr = NULL);

  template <class T>
  int get_json_resource(const string& resource, param_vec_t *params, T& t);
  template <class T>
  int get_json_resource(const string& resource, const rgw_http_param_pair *pp, T& t);
};


template<class T>
int RGWRESTConn::get_json_resource(const string& resource, param_vec_t *params, T& t)
{
  bufferlist bl;
  int ret = get_resource(resource, params, NULL, bl);
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
int RGWRESTConn::get_json_resource(const string& resource,  const rgw_http_param_pair *pp, T& t)
{
  param_vec_t params = make_param_list(pp);
  return get_json_resource(resource, &params, t);
}

class RGWStreamIntoBufferlist : public RGWGetDataCB {
  bufferlist& bl;
public:
  RGWStreamIntoBufferlist(bufferlist& _bl) : bl(_bl) {}
  int handle_data(bufferlist& inbl, off_t bl_ofs, off_t bl_len) {
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

class RGWRESTPostResource : public RefCountedObject {
  CephContext *cct;
  RGWRESTConn *conn;
  string resource;
  param_vec_t params;
  map<string, string> headers;
  bufferlist bl;
  RGWStreamIntoBufferlist cb;

  RGWHTTPManager *mgr;
  RGWRESTStreamRWRequest req;

  void init_common(param_vec_t *extra_headers);

public:
  RGWRESTPostResource(RGWRESTConn *_conn,
		      const string& _resource,
		      const rgw_http_param_pair *pp,
		      param_vec_t *extra_headers,
		      RGWHTTPManager *_mgr);

  RGWRESTPostResource(RGWRESTConn *_conn,
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
int RGWRESTPostResource::decode_resource(T *dest)
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
int RGWRESTPostResource::wait(T *dest)
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

#endif
