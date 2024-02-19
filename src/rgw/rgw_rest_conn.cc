// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_sal.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

RGWRESTConn::RGWRESTConn(CephContext *_cct, rgw::sal::Driver* driver,
                         const string& _remote_id,
                         const list<string>& remote_endpoints,
                         std::optional<string> _api_name,
                         HostStyle _host_style)
  : cct(_cct),
    endpoints(remote_endpoints.begin(), remote_endpoints.end()),
    remote_id(_remote_id),
    api_name(_api_name),
    host_style(_host_style)
{
  endpoints_status.reserve(remote_endpoints.size());
  std::for_each(remote_endpoints.begin(), remote_endpoints.end(),
                [this](const auto& url) {
                  this->endpoints_status.emplace(url, ceph::real_clock::zero());
                });

  if (driver) {
    key = driver->get_zone()->get_system_key();
    self_zone_group = driver->get_zone()->get_zonegroup().get_id();
  }
}

RGWRESTConn::RGWRESTConn(CephContext *_cct,
                         const string& _remote_id,
                         const list<string>& remote_endpoints,
                         RGWAccessKey _cred,
                         std::string _zone_group,
                         std::optional<string> _api_name,
                         HostStyle _host_style)
  : cct(_cct),
    endpoints(remote_endpoints.begin(), remote_endpoints.end()),
    key(_cred),
    self_zone_group(_zone_group),
    remote_id(_remote_id),
    api_name(_api_name),
    host_style(_host_style)
{
  endpoints_status.reserve(remote_endpoints.size());
  std::for_each(remote_endpoints.begin(), remote_endpoints.end(),
                [this](const auto& url) {
                  this->endpoints_status.emplace(url, ceph::real_clock::zero());
                });
}

RGWRESTConn::RGWRESTConn(RGWRESTConn&& other)
  : cct(other.cct),
    endpoints(std::move(other.endpoints)),
    endpoints_status(std::move(other.endpoints_status)),
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
  endpoints_status = std::move(other.endpoints_status);
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
    return -EINVAL;
  }

  size_t num = 0;
  while (num < endpoints.size()) {
    int i = ++counter;
    endpoint = endpoints[i % endpoints.size()];

    if (endpoints_status.find(endpoint) == endpoints_status.end()) {
      ldout(cct, 1) << "ERROR: missing status for endpoint " << endpoint << dendl;
      num++;
      continue;
    }

    const auto& upd_time = endpoints_status[endpoint].load();

    if (ceph::real_clock::is_zero(upd_time)) {
      break;
    }

    auto diff = ceph::to_seconds<double>(ceph::real_clock::now() - upd_time);

    ldout(cct, 20) << "endpoint url=" << endpoint
                   << " last endpoint status update time="
                   << ceph::real_clock::to_double(upd_time)
                   << " diff=" << diff << dendl;

    static constexpr uint32_t CONN_STATUS_EXPIRE_SECS = 2;
    if (diff >= CONN_STATUS_EXPIRE_SECS) {
      endpoints_status[endpoint].store(ceph::real_clock::zero());
      ldout(cct, 10) << "endpoint " << endpoint << " unconnectable status expired. mark it connectable" << dendl;
      break;
    }
    num++;
  };

  if (num == endpoints.size()) {
    ldout(cct, 5) << "ERROR: no valid endpoint" << dendl;
    return -EINVAL;
  }
  ldout(cct, 20) << "get_url picked endpoint=" << endpoint << dendl;

  return 0;
}

string RGWRESTConn::get_url()
{
  string endpoint;
  get_url(endpoint);
  return endpoint;
}

void RGWRESTConn::set_url_unconnectable(const std::string& endpoint)
{
  if (endpoint.empty() || endpoints_status.find(endpoint) == endpoints_status.end()) {
    ldout(cct, 0) << "ERROR: endpoint is not a valid or doesn't have status. endpoint="
                  << endpoint << dendl;
    return;
  }

  endpoints_status[endpoint].store(ceph::real_clock::now());

  ldout(cct, 10) << "set endpoint unconnectable. url=" << endpoint << dendl;
}

void RGWRESTConn::populate_params(param_vec_t& params, const rgw_owner* uid, const string& zonegroup)
{
  populate_uid(params, uid);
  populate_zonegroup(params, zonegroup);
}

int RGWRESTConn::forward(const DoutPrefixProvider *dpp, const rgw_owner& uid, const req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl, optional_yield y)
{
  int ret = 0;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    string url;
    ret = get_url(url);
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
    RGWRESTSimpleRequest req(cct, info.method, url, NULL, &params, api_name);
    ret = req.forward_request(dpp, key, info, max_response, inbl, outbl, y);
    if (ret == -EIO) {
      set_url_unconnectable(url);
      if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to forward request. retries=" << tries << dendl;
        continue;
      }
    }
    break;
  }
  return ret;
}

int RGWRESTConn::forward_iam_request(const DoutPrefixProvider *dpp, const req_info& info, obj_version *objv, size_t max_response, bufferlist *inbl, bufferlist *outbl, optional_yield y)
{
  int ret = 0;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    string url;
    ret = get_url(url);
    if (ret < 0)
      return ret;
    param_vec_t params;
    if (objv) {
      params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "tag", objv->tag));
      char buf[16];
      snprintf(buf, sizeof(buf), "%lld", (long long)objv->ver);
      params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "ver", buf));
    }
    std::string service = "iam";
    RGWRESTSimpleRequest req(cct, info.method, url, NULL, &params, api_name);
    // coverity[uninit_use_in_call:SUPPRESS]
    ret = req.forward_request(dpp, key, info, max_response, inbl, outbl, y, service);
    if (ret == -EIO) {
      set_url_unconnectable(url);
      if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to forward request. retries=" << tries << dendl;
        continue;
      }
    }
    break;
  }
  return ret;
}

int RGWRESTConn::put_obj_send_init(const rgw_obj& obj, const rgw_http_param_pair *extra_params, RGWRESTStreamS3PutObj **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, nullptr, self_zone_group);

  if (extra_params) {
    append_param_list(params, extra_params);
  }

  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, NULL, &params, api_name, host_style);
  // coverity[uninit_use_in_call:SUPPRESS]
  wr->send_init(obj);
  *req = wr;
  return 0;
}

int RGWRESTConn::put_obj_async_init(const DoutPrefixProvider *dpp, const rgw_owner& uid, const rgw_obj& obj,
                                    map<string, bufferlist>& attrs,
                                    RGWRESTStreamS3PutObj **req)
{
  string url;
  int ret = get_url(url);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, NULL, &params, api_name, host_style);
  // coverity[uninit_use_in_call:SUPPRESS]
  wr->put_obj_init(dpp, key, obj, attrs);
  *req = wr;
  return 0;
}

int RGWRESTConn::complete_request(RGWRESTStreamS3PutObj *req, string& etag,
                                  real_time *mtime, optional_yield y)
{
  int ret = req->complete_request(y, &etag, mtime);
  if (ret == -EIO) {
    ldout(cct, 5) << __func__ << ": complete_request() returned ret=" << ret << dendl;
    set_url_unconnectable(req->get_url_orig());
  }

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


int RGWRESTConn::get_obj(const DoutPrefixProvider *dpp, const rgw_owner& uid, req_info *info /* optional */, const rgw_obj& obj,
                         const real_time *mod_ptr, const real_time *unmod_ptr,
                         uint32_t mod_zone_id, uint64_t mod_pg_ver,
                         bool prepend_metadata, bool get_op, bool rgwx_stat,
                         bool sync_manifest, bool skip_decrypt,
                         rgw_zone_set_entry *dst_zone_trace, bool sync_cloudtiered,
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
  params.sync_cloudtiered = sync_cloudtiered;
  params.dst_zone_trace = dst_zone_trace;
  params.cb = cb;
  return get_obj(dpp, obj, params, send, req);
}

int RGWRESTConn::get_obj(const DoutPrefixProvider *dpp, const rgw_obj& obj, const get_obj_params& in_params, bool send, RGWRESTStreamRWRequest **req)
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
  if (in_params.sync_cloudtiered) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "sync-cloudtiered", ""));
  }
  if (in_params.skip_decrypt) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "skip-decrypt", ""));
  }
  if (in_params.dst_zone_trace) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "if-not-replicated-to", in_params.dst_zone_trace->to_str()));
  }
  if (!obj.key.instance.empty()) {
    params.push_back(param_pair_t("versionId", obj.key.instance));
  }
  if (in_params.get_op) {
    *req = new RGWRESTStreamReadRequest(cct, url, in_params.cb, NULL, &params, api_name, host_style);
  } else {
    *req = new RGWRESTStreamHeadRequest(cct, url, in_params.cb, NULL, &params, api_name);
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

  // coverity[uninit_use_in_call:SUPPRESS]
  int r = (*req)->send_prepare(dpp, key, extra_headers, obj);
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
                                  map<string, string> *pheaders,
                                  optional_yield y)
{
  int ret = req->complete_request(y, etag, mtime, psize, pattrs, pheaders);
  if (ret == -EIO) {
    ldout(cct, 5) << __func__ << ": complete_request() returned ret=" << ret << dendl;
    set_url_unconnectable(req->get_url_orig());
  }
  delete req;

  return ret;
}

int RGWRESTConn::get_resource(const DoutPrefixProvider *dpp,
                     const string& resource,
		     param_vec_t *extra_params,
		     map<string, string> *extra_headers,
		     bufferlist& bl,
                     bufferlist *send_data,
		     RGWHTTPManager *mgr,
		     optional_yield y)
{
  int ret = 0;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    string url;
    ret = get_url(url);
    if (ret < 0)
      return ret;

    param_vec_t params;

    if (extra_params) {
      params.insert(params.end(), extra_params->begin(), extra_params->end());
    }

    populate_params(params, nullptr, self_zone_group);

    RGWStreamIntoBufferlist cb(bl);

    RGWRESTStreamReadRequest req(cct, url, &cb, NULL, &params, api_name, host_style);

    map<string, string> headers;
    if (extra_headers) {
      headers.insert(extra_headers->begin(), extra_headers->end());
    }

    ret = req.send_request(dpp, &key, headers, resource, mgr, send_data);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
      return ret;
    }

    ret = req.complete_request(y);
    if (ret == -EIO) {
      set_url_unconnectable(url);
      if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to get resource. retries=" << tries << dendl;
        continue;
      }
    }
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": complete_request() returned ret=" << ret << dendl;
    }
    break;
  }

  return ret;
}

int RGWRESTConn::send_resource(const DoutPrefixProvider *dpp, const std::string& method,
                        const std::string& resource, rgw_http_param_pair *extra_params,
		                std::map<std::string, std::string> *extra_headers, bufferlist& bl,
                        bufferlist *send_data, RGWHTTPManager *mgr, optional_yield y)
{
  int ret = 0;

  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    std::string url;
    ret = get_url(url);
    if (ret < 0)
      return ret;

    param_vec_t params;

    if (extra_params) {
      params = make_param_list(extra_params);
    }

    populate_params(params, nullptr, self_zone_group);

    RGWStreamIntoBufferlist cb(bl);

    RGWRESTStreamSendRequest req(cct, method, url, &cb, NULL, &params, api_name, host_style);

    std::map<std::string, std::string> headers;
    if (extra_headers) {
      headers.insert(extra_headers->begin(), extra_headers->end());
    }

    ret = req.send_request(dpp, &key, headers, resource, mgr, send_data);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
      return ret;
    }

    ret = req.complete_request(y);
    if (ret == -EIO) {
      set_url_unconnectable(url);
      if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
        ldpp_dout(dpp, 20) << __func__  << "(): failed to send resource. retries=" << tries << dendl;
        continue;
      }
    }
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": complete_request() resource=" << resource << " returned ret=" << ret << dendl;
    }
    break;
  }

  return ret;
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
		                         const rgw_http_param_pair *pp,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource),
    params(make_param_list(pp)), cb(bl), mgr(_mgr),
    req(cct, conn->get_url(), &cb, NULL, NULL, _conn->get_api_name())
{
  init_common(extra_headers);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
					 param_vec_t& _params,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource), params(_params),
    cb(bl), mgr(_mgr), req(cct, conn->get_url(), &cb, NULL, NULL, _conn->get_api_name())
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

int RGWRESTReadResource::read(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = req.send_request(dpp, &conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  ret = req.complete_request(y);
  if (ret == -EIO) {
    conn->set_url_unconnectable(req.get_url_orig());
    ldpp_dout(dpp, 20) << __func__ << ": complete_request() returned ret=" << ret << dendl;
  }

  return ret;
}

int RGWRESTReadResource::aio_read(const DoutPrefixProvider *dpp)
{
  int ret = req.send_request(dpp, &conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
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
    req(cct, method.c_str(), conn->get_url(), &cb, NULL, NULL, _conn->get_api_name(), _conn->get_host_style())
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
    cb(bl), mgr(_mgr), req(cct, method.c_str(), conn->get_url(), &cb, NULL, NULL, _conn->get_api_name(), _conn->get_host_style())
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

int RGWRESTSendResource::send(const DoutPrefixProvider *dpp, bufferlist& outbl, optional_yield y)
{
  req.set_send_length(outbl.length());
  req.set_outbl(outbl);

  int ret = req.send_request(dpp, &conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  ret = req.complete_request(y);
  if (ret == -EIO) {
    conn->set_url_unconnectable(req.get_url_orig());
    ldpp_dout(dpp, 20) << __func__ << ": complete_request() returned ret=" << ret << dendl;
  }

  return ret;
}

int RGWRESTSendResource::aio_send(const DoutPrefixProvider *dpp, bufferlist& outbl)
{
  req.set_send_length(outbl.length());
  req.set_outbl(outbl);

  int ret = req.send_request(dpp, &conn->get_key(), headers, resource, mgr);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}
