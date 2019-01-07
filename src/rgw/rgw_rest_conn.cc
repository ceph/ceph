// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_rest_conn.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWRESTConn::RGWRESTConn(CephContext *_cct, RGWSI_Zone *zone_svc,
                         const string& _remote_id,
                         const list<string>& remote_endpoints,
                         HostStyle _host_style)
  : cct(_cct),
    endpoints(remote_endpoints.begin(), remote_endpoints.end()),
    remote_id(_remote_id), host_style(_host_style)
{
  if (zone_svc) {
    key = zone_svc->get_zone_params().system_key;
    self_zone_group = zone_svc->get_zonegroup().get_id();
  }
}

RGWRESTConn::RGWRESTConn(CephContext *_cct, RGWSI_Zone *zone_svc,
                         const string& _remote_id,
                         const list<string>& remote_endpoints,
                         RGWAccessKey _cred,
                         HostStyle _host_style)
  : cct(_cct),
    endpoints(remote_endpoints.begin(), remote_endpoints.end()),
    key(std::move(_cred)),
    remote_id(_remote_id), host_style(_host_style)
{
  if (zone_svc) {
    self_zone_group = zone_svc->get_zonegroup().get_id();
  }
}

RGWRESTConn::RGWRESTConn(RGWRESTConn&& other)
  : cct(other.cct),
    endpoints(std::move(other.endpoints)),
    key(std::move(other.key)),
    self_zone_group(std::move(other.self_zone_group)),
    remote_id(std::move(other.remote_id)),
    counter(other.counter.load())
{
}

RGWRESTConn& RGWRESTConn::operator=(RGWRESTConn&& other)
{
  cct = other.cct;
  endpoints = std::move(other.endpoints);
  key = std::move(other.key);
  self_zone_group = std::move(other.self_zone_group);
  remote_id = std::move(other.remote_id);
  counter = other.counter.load();
  return *this;
}

int RGWRESTConn::get_url(string& endpoint)
{
  if (endpoints.empty()) {
    ldout(cct, 0) << "ERROR: endpoints not configured for upstream zone" << dendl;
    return -EIO;
  }

  int i = ++counter;
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

  int i = ++counter;
  endpoint = endpoints[i % endpoints.size()];

  return endpoint;
}

void RGWRESTConn::populate_params(param_vec_t& params, const rgw_user *uid, const string& zonegroup)
{
  populate_uid(params, uid);
  populate_zonegroup(params, zonegroup);
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
  RGWRESTSimpleRequest req(cct, info.method, url, NULL, &params);
  return req.forward_request(key, info, max_response, inbl, outbl);
}

class StreamObjData : public RGWGetDataCB {
  rgw_obj obj;
public:
    explicit StreamObjData(rgw_obj& _obj) : obj(_obj) {}
};

int RGWRESTConn::put_obj_send_init(rgw_obj& obj, const rgw_http_param_pair *extra_params, RGWRESTStreamS3PutObj **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  rgw_user uid;
  param_vec_t params;
  populate_params(params, &uid, self_zone_group);

  if (extra_params) {
    append_param_list(params, extra_params);
  }

  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, NULL, &params, host_style);
  wr->send_init(obj);
  *req = wr;
  return 0;
}

int RGWRESTConn::put_obj_async(const rgw_user& uid, rgw_obj& obj, uint64_t obj_size,
                               map<string, bufferlist>& attrs, bool send,
                               RGWRESTStreamS3PutObj **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, NULL, &params, host_style);
  ret = wr->put_obj_init(key, obj, obj_size, attrs, send);
  if (ret < 0) {
    delete wr;
    return ret;
  }
  *req = wr;
  return 0;
}

int RGWRESTConn::complete_request(RGWRESTStreamS3PutObj *req, string& etag, real_time *mtime)
{
  int ret = req->complete_request(&etag, mtime);
  delete req;

  return ret;
}

static void set_date_header(const real_time *t, map<string, string>& headers, bool high_precision_time, const string& header_name)
{
  if (!t) {
    return;
  }
  stringstream s;
  utime_t tm = utime_t(*t);
  if (high_precision_time) {
    tm.gmtime_nsec(s);
  } else {
    tm.gmtime(s);
  }
  headers[header_name] = s.str();
}

template <class T>
static void set_header(T val, map<string, string>& headers, const string& header_name)
{
  stringstream s;
  s << val;
  headers[header_name] = s.str();
}


int RGWRESTConn::get_obj(const rgw_user& uid, req_info *info /* optional */, const rgw_obj& obj,
                         const real_time *mod_ptr, const real_time *unmod_ptr,
                         uint32_t mod_zone_id, uint64_t mod_pg_ver,
                         bool prepend_metadata, bool get_op, bool rgwx_stat,
                         bool sync_manifest, bool skip_decrypt,
                         bool send, RGWHTTPStreamRWRequest::ReceiveCB *cb, RGWRESTStreamRWRequest **req)
{
  get_obj_params params;
  params.uid = uid;
  params.info = info;
  params.mod_ptr = mod_ptr;
  params.mod_pg_ver = mod_pg_ver;
  params.prepend_metadata = prepend_metadata;
  params.get_op = get_op;
  params.rgwx_stat = rgwx_stat;
  params.sync_manifest = sync_manifest;
  params.skip_decrypt = skip_decrypt;
  params.cb = cb;
  return get_obj(obj, params, send, req);
}

int RGWRESTConn::get_obj(const rgw_obj& obj, const get_obj_params& in_params, bool send, RGWRESTStreamRWRequest **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &in_params.uid, self_zone_group);
  if (in_params.prepend_metadata) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "prepend-metadata", "true"));
  }
  if (in_params.rgwx_stat) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "stat", "true"));
  }
  if (in_params.sync_manifest) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "sync-manifest", ""));
  }
  if (in_params.skip_decrypt) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "skip-decrypt", ""));
  }
  if (!obj.key.instance.empty()) {
    const string& instance = obj.key.instance;
    params.push_back(param_pair_t("versionId", instance));
  }
  if (in_params.get_op) {
    *req = new RGWRESTStreamReadRequest(cct, url, in_params.cb, NULL, &params, host_style);
  } else {
    *req = new RGWRESTStreamHeadRequest(cct, url, in_params.cb, NULL, &params);
  }
  map<string, string> extra_headers;
  if (in_params.info) {
    const auto& orig_map = in_params.info->env->get_map();

    /* add original headers that start with HTTP_X_AMZ_ */
    static constexpr char SEARCH_AMZ_PREFIX[] = "HTTP_X_AMZ_";
    for (auto iter= orig_map.lower_bound(SEARCH_AMZ_PREFIX); iter != orig_map.end(); ++iter) {
      const string& name = iter->first;
      if (name == "HTTP_X_AMZ_DATE") /* don't forward date from original request */
        continue;
      if (name.compare(0, strlen(SEARCH_AMZ_PREFIX), SEARCH_AMZ_PREFIX) != 0)
        break;
      extra_headers[iter->first] = iter->second;
    }
  }

  set_date_header(in_params.mod_ptr, extra_headers, in_params.high_precision_time, "HTTP_IF_MODIFIED_SINCE");
  set_date_header(in_params.unmod_ptr, extra_headers, in_params.high_precision_time, "HTTP_IF_UNMODIFIED_SINCE");
  if (!in_params.etag.empty()) {
    set_header(in_params.etag, extra_headers, "HTTP_IF_MATCH");
  }
  if (in_params.mod_zone_id != 0) {
    set_header(in_params.mod_zone_id, extra_headers, "HTTP_DEST_ZONE_SHORT_ID");
  }
  if (in_params.mod_pg_ver != 0) {
    set_header(in_params.mod_pg_ver, extra_headers, "HTTP_DEST_PG_VER");
  }
  if (in_params.range_is_set) {
    char buf[64];
    snprintf(buf, sizeof(buf), "bytes=%lld-%lld", (long long)in_params.range_start, (long long)in_params.range_end);
    set_header(buf, extra_headers, "RANGE");
  }

  int r = (*req)->send_prepare(key, extra_headers, obj);
  if (r < 0) {
    goto done_err;
  }
  
  if (!send) {
    return 0;
  }

  r = (*req)->send(nullptr);
  if (r < 0) {
    goto done_err;
  }
  return 0;
done_err:
  delete *req;
  *req = nullptr;
  return r;
}

int RGWRESTConn::complete_request(RGWRESTStreamRWRequest *req,
                                  string *etag,
                                  real_time *mtime,
                                  uint64_t *psize,
                                  map<string, string> *pattrs,
                                  map<string, string> *pheaders)
{
  int ret = req->complete_request(etag, mtime, psize, pattrs, pheaders);
  delete req;

  return ret;
}

int RGWRESTConn::get_resource(const string& resource,
		     param_vec_t *extra_params,
		     map<string, string> *extra_headers,
		     bufferlist& bl,
                     bufferlist *send_data,
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

  RGWRESTStreamReadRequest req(cct, url, &cb, NULL, &params, host_style);

  map<string, string> headers;
  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  ret = req.send_request(&key, headers, resource, mgr, send_data);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return req.complete_request();
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
  conn->populate_params(params, nullptr, conn->get_self_zonegroup());

  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  req.set_params(&params);
}

int RGWRESTReadResource::read()
{
  int ret = req.send_request(&conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return req.complete_request();
}

int RGWRESTReadResource::aio_read()
{
  int ret = req.send_request(&conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

RGWRESTSendResource::RGWRESTSendResource(RGWRESTConn *_conn,
                                         const string& _method,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), method(_method), resource(_resource),
    params(make_param_list(pp)), cb(bl), mgr(_mgr),
    req(cct, method.c_str(), conn->get_url(), &cb, NULL, NULL, _conn->get_host_style())
{
  init_common(extra_headers);
}

RGWRESTSendResource::RGWRESTSendResource(RGWRESTConn *_conn,
                                         const string& _method,
                                         const string& _resource,
					 param_vec_t& params,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), method(_method), resource(_resource), params(params),
    cb(bl), mgr(_mgr), req(cct, method.c_str(), conn->get_url(), &cb, NULL, NULL, _conn->get_host_style())
{
  init_common(extra_headers);
}

void RGWRESTSendResource::init_common(param_vec_t *extra_headers)
{
  conn->populate_params(params, nullptr, conn->get_self_zonegroup());

  if (extra_headers) {
    headers.insert(extra_headers->begin(), extra_headers->end());
  }

  req.set_params(&params);
}

int RGWRESTSendResource::send(bufferlist& outbl)
{
  req.set_send_length(outbl.length());
  req.set_outbl(outbl);

  int ret = req.send_request(&conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return req.complete_request();
}

int RGWRESTSendResource::aio_send(bufferlist& outbl)
{
  req.set_send_length(outbl.length());
  req.set_outbl(outbl);

  int ret = req.send_request(&conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}
