// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_rest_conn.h"

#define dout_subsys ceph_subsys_rgw

RGWRESTConn::RGWRESTConn(CephContext *_cct, RGWRados *store,
                         const string& _remote_id,
                         const list<string>& remote_endpoints)
  : cct(_cct),
    endpoints(remote_endpoints.begin(), remote_endpoints.end()),
    remote_id(_remote_id)
{
  key = store->get_zone_params().system_key;
  self_zone_group = store->get_zonegroup().get_id();
}

int RGWRESTConn::get_url(string& endpoint)
{
  if (endpoints.empty()) {
    ldout(cct, 0) << "ERROR: endpoints not configured for upstream zone" << dendl;
    return -EIO;
  }

  int i = counter.inc();
  endpoint = endpoints[i % endpoints.size()];

  return 0;
}

string RGWRESTConn::get_url()
{
  string endpoint;
  if (endpoints.empty()) {
    ldout(cct, 0) << "WARNING: endpoints not configured for upstream zone" << dendl; /* we'll catch this later */
    return endpoint;
  }

  int i = counter.inc();
  endpoint = endpoints[i % endpoints.size()];

  return endpoint;
}

static void populate_params(param_vec_t& params, const rgw_user *uid, const string& zonegroup)
{
  if (uid) {
    string uid_str = uid->to_str();
    if (!uid->empty()) {
      params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "uid", uid_str));
    }
  }
  if (!zonegroup.empty()) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "zonegroup", zonegroup));
  }
}

int RGWRESTConn::forward(const rgw_user& uid, req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;
  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  if (objv) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "tag", objv->tag));
    char buf[16];
    snprintf(buf, sizeof(buf), "%lld", (long long)objv->ver);
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "ver", buf));
  }
  RGWRESTSimpleRequest req(cct, url, NULL, &params);
  return req.forward_request(key, info, max_response, inbl, outbl);
}

class StreamObjData : public RGWGetDataCB {
  rgw_obj obj;
public:
    explicit StreamObjData(rgw_obj& _obj) : obj(_obj) {}
};

int RGWRESTConn::put_obj_init(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                                      map<string, bufferlist>& attrs, RGWRESTStreamWriteRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  *req = new RGWRESTStreamWriteRequest(cct, url, NULL, &params);
  return (*req)->put_obj_init(key, obj, obj_size, attrs);
}

int RGWRESTConn::complete_request(RGWRESTStreamWriteRequest *req, string& etag, real_time *mtime)
{
  int ret = req->complete(etag, mtime);
  delete req;

  return ret;
}

static void set_date_header(const real_time *t, map<string, string>& headers, const string& header_name)
{
  if (!t) {
    return;
  }
  stringstream s;
  utime_t tm = utime_t(*t);
  tm.gmtime_nsec(s);
  headers[header_name] = s.str();
}

template <class T>
static void set_header(T val, map<string, string>& headers, const string& header_name)
{
  stringstream s;
  s << val;
  headers[header_name] = s.str();
}


int RGWRESTConn::get_obj(const rgw_user& uid, req_info *info /* optional */, rgw_obj& obj,
                         const real_time *mod_ptr, const real_time *unmod_ptr,
                         uint32_t mod_zone_id, uint64_t mod_pg_ver,
                         bool prepend_metadata, bool sync_manifest,
                         RGWGetDataCB *cb, RGWRESTStreamReadRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  if (prepend_metadata) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "prepend-metadata", self_zone_group));
  }
  if (sync_manifest) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "sync-manifest", ""));
  }
  if (!obj.get_instance().empty()) {
    const string& instance = obj.get_instance();
    params.push_back(param_pair_t("versionId", instance));
  }
  *req = new RGWRESTStreamReadRequest(cct, url, cb, NULL, &params);
  map<string, string> extra_headers;
  if (info) {
    map<string, string, ltstr_nocase>& orig_map = info->env->get_map();

    /* add original headers that start with HTTP_X_AMZ_ */
#define SEARCH_AMZ_PREFIX "HTTP_X_AMZ_"
    for (map<string, string>::iterator iter = orig_map.lower_bound(SEARCH_AMZ_PREFIX); iter != orig_map.end(); ++iter) {
      const string& name = iter->first;
      if (name == "HTTP_X_AMZ_DATE") /* dont forward date from original request */
        continue;
      if (name.compare(0, sizeof(SEARCH_AMZ_PREFIX) - 1, "HTTP_X_AMZ_") != 0)
        break;
      extra_headers[iter->first] = iter->second;
    }
  }

  set_date_header(mod_ptr, extra_headers, "HTTP_IF_MODIFIED_SINCE");
  set_date_header(unmod_ptr, extra_headers, "HTTP_IF_UNMODIFIED_SINCE");
  if (mod_zone_id != 0) {
    set_header(mod_zone_id, extra_headers, "HTTP_DEST_ZONE_SHORT_ID");
  }
  if (mod_pg_ver != 0) {
    set_header(mod_pg_ver, extra_headers, "HTTP_DEST_PG_VER");
  }

  int r = (*req)->get_obj(key, extra_headers, obj);
  if (r < 0) {
    delete *req;
    *req = nullptr;
  }
  
  return r;
}

int RGWRESTConn::complete_request(RGWRESTStreamReadRequest *req, string& etag, real_time *mtime, map<string, string>& attrs)
{
  int ret = req->complete(etag, mtime, attrs);
  delete req;

  return ret;
}

int RGWRESTConn::get_resource(const string& resource,
		     param_vec_t *extra_params,
		     map<string, string> *extra_headers,
		     bufferlist& bl,
		     RGWHTTPManager *mgr)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;

  if (extra_params) {
    params.insert(params.end(), extra_params->begin(), extra_params->end());
  }

  populate_params(params, nullptr, self_zone_group);

  RGWStreamIntoBufferlist cb(bl);

  RGWRESTStreamReadRequest req(cct, url, &cb, NULL, &params);

  map<string, string> headers;
  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  ret = req.get_resource(key, headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource),
    params(make_param_list(pp)), cb(bl), mgr(_mgr),
    req(cct, conn->get_url(), &cb, NULL, NULL)
{
  init_common(extra_headers);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
					 param_vec_t& _params,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource), params(_params),
    cb(bl), mgr(_mgr), req(cct, conn->get_url(), &cb, NULL, NULL)
{
  init_common(extra_headers);
}

void RGWRESTReadResource::init_common(param_vec_t *extra_headers)
{
  populate_params(params, nullptr, conn->get_self_zonegroup());

  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  req.set_params(&params);
}

int RGWRESTReadResource::read()
{
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

int RGWRESTReadResource::aio_read()
{
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

RGWRESTPostResource::RGWRESTPostResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource),
    params(make_param_list(pp)), cb(bl), mgr(_mgr),
    req(cct, "POST", conn->get_url(), &cb, NULL, NULL)
{
  init_common(extra_headers);
}

RGWRESTPostResource::RGWRESTPostResource(RGWRESTConn *_conn,
                                         const string& _resource,
					 param_vec_t& params,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource), params(params),
    cb(bl), mgr(_mgr), req(cct, "POST", conn->get_url(), &cb, NULL, NULL)
{
  init_common(extra_headers);
}

void RGWRESTPostResource::init_common(param_vec_t *extra_headers)
{
  populate_params(params, nullptr, conn->get_self_zonegroup());

  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  req.set_params(&params);
}

int RGWRESTPostResource::send(bufferlist& outbl)
{
  req.set_outbl(outbl);
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  string etag;
  map<string, string> attrs;
  return req.complete(etag, NULL, attrs);
}

int RGWRESTPostResource::aio_send(bufferlist& outbl)
{
  req.set_outbl(outbl);
  int ret = req.get_resource(conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": get_resource() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

