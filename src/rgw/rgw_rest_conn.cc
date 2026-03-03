// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_conn.h"
#include "rgw_sal.h"
#include "rgw_resolve.h"

#include <boost/url.hpp>

#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWRESTConn::resolve_endpoints() {
  for (auto& res_ep : resolved_endpoints) {
    const std::string& ep_url = res_ep.url;
    // parse URL
    boost::system::result<boost::urls::url_view> r = boost::urls::parse_uri(ep_url);
    if (r.has_error()) {
      ldout(cct, 0) << "RGWRESTConn: invalid endpoint url=" << ep_url
                    << " err=" << r.error().message() << dendl;
      continue;
    }
    boost::urls::url_view u = r.value();

    // scheme
    std::string scheme = std::string(u.scheme());
    if (scheme.empty()) {
      scheme = "http";
    }
    res_ep.scheme = scheme;

    // host
    res_ep.host = std::string(u.host());
    if (res_ep.host.empty()) {
      ldout(cct, 0) << "RGWRESTConn: endpoint url=" << ep_url
                    << " has empty host" << dendl;
      continue;
    }

    // port
    if (u.has_port()) {
      try {
        res_ep.port = static_cast<uint16_t>(std::stoi(std::string(u.port())));
      } catch (...) {
        ldout(cct, 0) << "RGWRESTConn: invalid port in endpoint url=" << ep_url<< dendl;
        continue;
      }
    } else {
      res_ep.port = (scheme == "https" ? 443 : 80);
    }

    // resolve all IP addresses for the host
    std::vector<entity_addr_t> addrs;
    int rr = rgw_resolver->resolve_all_addrs(res_ep.host, &addrs);
    if (rr >= 0 && !addrs.empty()) {
      ldout(cct, 2) << "endpoint=" << ep_url << " resolved to "
        << addrs.size() << " IP addresses" << dendl;

      // Pre-compute host:port prefix and port string once
      std::string port_str = std::to_string(res_ep.port);
      std::string host_port_prefix = res_ep.host + ":" + port_str + ":";

      // Pre-compute full connect_to strings with per-IP status
      res_ep.resolved_ips.reserve(addrs.size());
      for (const auto& ea : addrs) {
        char ipbuf[INET6_ADDRSTRLEN] = {0};
        const sockaddr* sa = ea.get_sockaddr();
        if (sa->sa_family == AF_INET) {
          auto sin = reinterpret_cast<const sockaddr_in*>(sa);
          inet_ntop(AF_INET, &sin->sin_addr, ipbuf, sizeof(ipbuf));
        } else if (sa->sa_family == AF_INET6) {
          auto sin6 = reinterpret_cast<const sockaddr_in6*>(sa);
          inet_ntop(AF_INET6, &sin6->sin6_addr, ipbuf, sizeof(ipbuf));
        }
        if (ipbuf[0] != '\0') {
          // Pre-compute: "host:port:ip:port" with the initial status 'available'
          res_ep.resolved_ips.emplace_back(host_port_prefix + ipbuf + ":" + port_str);
          ldout(cct, 2) << "endpoint_url=" << ep_url << " resolved to ip=" << ipbuf << dendl;
        }
      }
    } else {
      ldout(cct, 0) << "WARNING: RGWRESTConn no IP addresses found for endpoint=" << ep_url << dendl;
    }
  }
}

RGWRESTConn::RGWRESTConn(CephContext *_cct, rgw::sal::Driver* driver,
                         const string& _remote_id,
                         const list<string>& remote_endpoints,
                         std::optional<string> _api_name,
                         HostStyle _host_style)
  : cct(_cct),
    remote_id(_remote_id),
    api_name(_api_name),
    host_style(_host_style)
{
  resolved_endpoints.reserve(remote_endpoints.size());
  for (const auto& ep_url : remote_endpoints) {
    ResolvedEndpoint res_ep;
    res_ep.url = ep_url;
    resolved_endpoints.push_back(std::move(res_ep));
  }
  resolve_endpoints();

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
    key(_cred),
    self_zone_group(_zone_group),
    remote_id(_remote_id),
    api_name(_api_name),
    host_style(_host_style)
{
  resolved_endpoints.reserve(remote_endpoints.size());
  for (const auto& ep_url : remote_endpoints) {
    ResolvedEndpoint res_ep;
    res_ep.url = ep_url;
    resolved_endpoints.push_back(std::move(res_ep));
  }
  resolve_endpoints();
}

RGWRESTConn::RGWRESTConn(RGWRESTConn&& other)
  : cct(other.cct),
    endpoint_rr_index(other.endpoint_rr_index.load()),
    resolved_endpoints(std::move(other.resolved_endpoints)),
    key(std::move(other.key)),
    self_zone_group(std::move(other.self_zone_group)),
    remote_id(std::move(other.remote_id)),
    api_name(std::move(other.api_name)),
    host_style(other.host_style)
{
}

RGWRESTConn& RGWRESTConn::operator=(RGWRESTConn&& other)
{
  cct = other.cct;
  endpoint_rr_index = other.endpoint_rr_index.load();
  resolved_endpoints = std::move(other.resolved_endpoints);
  key = std::move(other.key);
  self_zone_group = std::move(other.self_zone_group);
  remote_id = std::move(other.remote_id);
  api_name = std::move(other.api_name);
  host_style = other.host_style;
  return *this;
}

ResolvedEndpoint* RGWRESTConn::find_resolved_endpoint(const std::string& url)
{
  for (auto& res_ep : resolved_endpoints) {
    if (res_ep.url == url) {
      return &res_ep;
    }
  }
  return nullptr;
}

void RGWRESTConn::populate_connect_to(RGWEndpoint& endpoint, ResolvedEndpoint& resolved_endpoint)
{
  if (!cct->_conf->rgw_rest_conn_connect_to_resolved_ips) {
    return;
  }

  if (resolved_endpoint.resolved_ips.empty()) {
    return;
  }

  const auto ip_fail_timeout = cct->_conf->rgw_rest_conn_ip_fail_timeout_secs;
  const size_t num_ips = resolved_endpoint.resolved_ips.size();
  auto now = ceph::real_clock::now();

  // Round-robin through IPs, skipping any that are marked down
  for (size_t i = 0; i < num_ips; ++i) {
    size_t idx = resolved_endpoint.ip_rr_index++ % num_ips;
    ResolvedIP& ip_status = resolved_endpoint.resolved_ips[idx];

    const auto& last_fail = ip_status.last_failure.load();
    if (ceph::real_clock::is_zero(last_fail)) {
      endpoint.set_connect_to(ip_status.connect_to);  // IP is up
      return;
    }

    auto diff = ceph::to_seconds<double>(now - last_fail);
    if (diff >= ip_fail_timeout) {
      // Failure expired, mark IP as up and use it
      ip_status.mark_up();
      ldout(cct, 5) << "IP " << ip_status.connect_to << " failure expired, marking up" << dendl;
      endpoint.set_connect_to(ip_status.connect_to);
      return;
    }
  }

  // All IPs are down - do not populate connect_to; i.e.,
  // let libcurl handle it without connect_to hint.
  ldout(cct, 5) << "All IPs down for endpoint=" << resolved_endpoint.url
    << " - skip connect_to hint" << dendl;
}

int RGWRESTConn::get_endpoint(RGWEndpoint& endpoint)
{
  if (resolved_endpoints.empty()) {
    ldout(cct, 0) << "ERROR: endpoints not configured for upstream zone" << dendl;
    return -EINVAL;
  }

  const auto ip_fail_timeout = cct->_conf->rgw_rest_conn_ip_fail_timeout_secs;
  auto now = ceph::real_clock::now();

  // Helper to check if an endpoint has at least one available IP
  auto endpoint_has_available_ip = [&](ResolvedEndpoint& res_ep) -> bool {
    // If no IP resolution, endpoint is available (will use DNS directly)
    if (res_ep.resolved_ips.empty()) {
      return true;
    }

    // Fast path: if no recent failures at endpoint level, all IPs are available
    const auto& ep_last_fail = res_ep.last_failure_time.load();
    if (ceph::real_clock::is_zero(ep_last_fail) ||
        ceph::to_seconds<double>(now - ep_last_fail) >= ip_fail_timeout) {
      return true;
    }

    // Slow path: check individual IPs (only when there's a recent failure)
    for (auto& ip_status : res_ep.resolved_ips) {
      const auto& last_fail = ip_status.last_failure.load();
      if (ceph::real_clock::is_zero(last_fail)) {
        return true;  // This IP is up
      }
      auto diff = ceph::to_seconds<double>(now - last_fail);
      if (diff >= ip_fail_timeout) {
        return true;  // This IP's failure has expired
      }
    }
    return false;  // All IPs are down
  };

  size_t num = 0;
  size_t selected_idx = 0;
  while (num < resolved_endpoints.size()) {
    int i = ++endpoint_rr_index;
    selected_idx = i % resolved_endpoints.size();

    ResolvedEndpoint& res_ep = resolved_endpoints[selected_idx];

    if (endpoint_has_available_ip(res_ep)) {
      endpoint.set_url(res_ep.url);
      break;
    }

    ldout(cct, 5) << "endpoint url=" << res_ep.url << " all IPs down, trying next" << dendl;
    num++;
  }

  if (num == resolved_endpoints.size()) {
    ldout(cct, 1) << "ERROR: no valid endpoint (all IPs down for all endpoints)" << dendl;
    return -EINVAL;
  }

  populate_connect_to(endpoint, resolved_endpoints[selected_idx]);
  ldout(cct, 20) << "get_endpoint picked " << endpoint << dendl;

  return 0;
}

RGWEndpoint RGWRESTConn::get_endpoint()
{
  RGWEndpoint endpoint;
  get_endpoint(endpoint);
  return endpoint;
}

void RGWRESTConn::set_endpoint_unconnectable(const RGWEndpoint& endpoint)
{
  const string& orig_url = endpoint.get_original_url();
  const string& connect_to = endpoint.get_connect_to();

  ResolvedEndpoint* res_ep = find_resolved_endpoint(orig_url);
  if (orig_url.empty() || !res_ep) {
    ldout(cct, 0) << "ERROR: endpoint is not valid or not found: "
      << endpoint << dendl;
    return;
  }

  // Update endpoint-level last_failure_time for fast-path optimization
  auto now = ceph::real_clock::now();
  res_ep->last_failure_time.store(now);

  // If we have a connect_to string, mark that specific IP as down as well
  if (!connect_to.empty()) {
    ResolvedIP* res_ip = res_ep->find_ip_status(connect_to);
    if (res_ip) {
      res_ip->mark_down();
      ldout(cct, 10) << "set IP unconnectable: " << connect_to << dendl;
      return;
    }
  }

  // Fallback: mark all IPs for this endpoint as down
  for (auto& res_ip : res_ep->resolved_ips) {
    res_ip.mark_down();
  }
  ldout(cct, 10) << "set all IPs unconnectable for endpoint url=" << orig_url << dendl;
}

void RGWRESTConn::populate_params(param_vec_t& params, const rgw_owner* uid, const string& zonegroup)
{
  populate_uid(params, uid);
  populate_zonegroup(params, zonegroup);
}

auto RGWRESTConn::forward(const DoutPrefixProvider *dpp, const rgw_owner& uid,
                          const req_info& info, size_t max_response,
                          bufferlist *inbl, bufferlist *outbl, optional_yield y)
  -> tl::expected<int, int>
{
  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    RGWEndpoint endpoint;
    int ret = get_endpoint(endpoint);
    if (ret < 0) {
      return tl::unexpected(ret);
    }

    param_vec_t params;
    populate_params(params, &uid, self_zone_group);
    RGWRESTSimpleRequest req(cct, info.method, endpoint, NULL, &params, api_name);
    auto result = req.forward_request(dpp, key, info, max_response, inbl, outbl, y);
    if (result) {
      return result;
    } else if (result.error() != -EIO) {
      return result;
    }
    set_endpoint_unconnectable(endpoint);
    if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
      ldpp_dout(dpp, 20) << __func__  << "(): failed to forward request. retries=" << tries << dendl;
    }
  }
  return tl::unexpected(-EIO);
}

auto RGWRESTConn::forward_iam(const DoutPrefixProvider *dpp, const req_info& info,
                              size_t max_response, bufferlist *inbl,
                              bufferlist *outbl, optional_yield y)
  -> tl::expected<int, int>
{
  static constexpr int NUM_ENPOINT_IOERROR_RETRIES = 20;
  for (int tries = 0; tries < NUM_ENPOINT_IOERROR_RETRIES; tries++) {
    RGWEndpoint endpoint;
    int ret = get_endpoint(endpoint);
    if (ret < 0) {
      return tl::unexpected(ret);
    }

    param_vec_t params;
    std::string service = "iam";
    RGWRESTSimpleRequest req(cct, info.method, endpoint, NULL, &params, api_name);
    // coverity[uninit_use_in_call:SUPPRESS]
    auto result = req.forward_request(dpp, key, info, max_response, inbl, outbl, y, service);
    if (result) {
      return result;
    } else if (result.error() != -EIO) {
      return result;
    }
    set_endpoint_unconnectable(endpoint);
    if (tries < NUM_ENPOINT_IOERROR_RETRIES - 1) {
      ldpp_dout(dpp, 20) << __func__  << "(): failed to forward request. retries=" << tries << dendl;
    }
  }
  return tl::unexpected(-EIO);
}

int RGWRESTConn::put_obj_send_init(const rgw_obj& obj, const rgw_http_param_pair *extra_params, RGWRESTStreamS3PutObj **req)
{
  RGWEndpoint endpoint;
  int ret = get_endpoint(endpoint);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, nullptr, self_zone_group);

  if (extra_params) {
    append_param_list(params, extra_params);
  }

  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", endpoint, NULL, &params, api_name, host_style);
  // coverity[uninit_use_in_call:SUPPRESS]
  wr->send_init(obj);
  *req = wr;
  return 0;
}

int RGWRESTConn::put_obj_async_init(const DoutPrefixProvider *dpp, const rgw_owner& uid, const rgw_obj& obj,
                                    map<string, bufferlist>& attrs,
                                    RGWRESTStreamS3PutObj **req)
{
  RGWEndpoint endpoint;
  int ret = get_endpoint(endpoint);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, &uid, self_zone_group);
  RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", endpoint, NULL, &params, api_name, host_style);
  // coverity[uninit_use_in_call:SUPPRESS]
  wr->put_obj_init(dpp, key, obj, attrs);
  *req = wr;
  return 0;
}

int RGWRESTConn::complete_request(const DoutPrefixProvider* dpp,
                                  RGWRESTStreamS3PutObj *req, string& etag,
                                  real_time *mtime, optional_yield y)
{
  int ret = req->complete_request(dpp, y, &etag, mtime);
  if (ret == -EIO) {
    ldout(cct, 5) << __func__ << ": complete_request() returned ret=" << ret << dendl;
    set_endpoint_unconnectable(req->get_endpoint());
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


int RGWRESTConn::get_obj(const DoutPrefixProvider *dpp, const rgw_owner *uid,
                         const rgw_user *perm_check_uid,
                         req_info *info /* optional */, const rgw_obj& obj,
                         const real_time *mod_ptr, const real_time *unmod_ptr,
                         uint32_t mod_zone_id, uint64_t mod_pg_ver,
                         bool prepend_metadata, bool get_op, bool rgwx_stat,
                         bool sync_manifest, bool skip_decrypt,
                         rgw_zone_set_entry *dst_zone_trace, bool sync_cloudtiered,
                         bool send, RGWHTTPStreamRWRequest::ReceiveCB *cb, RGWRESTStreamRWRequest **req)
{
  get_obj_params params;
  params.uid = uid;
  params.perm_check_uid = perm_check_uid;
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
  RGWEndpoint endpoint;
  int ret = get_endpoint(endpoint);
  if (ret < 0)
    return ret;

  param_vec_t params;
  populate_params(params, in_params.uid, self_zone_group);
  if (in_params.perm_check_uid) {
    params.push_back(param_pair_t(RGW_SYS_PARAM_PREFIX "perm-check-uid", to_string(*in_params.perm_check_uid)));
  }
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
    *req = new RGWRESTStreamReadRequest(cct, endpoint, in_params.cb, NULL, &params, api_name, host_style);
  } else {
    *req = new RGWRESTStreamHeadRequest(cct, endpoint, in_params.cb, NULL, &params, api_name);
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

int RGWRESTConn::complete_request(const DoutPrefixProvider* dpp,
                                  RGWRESTStreamRWRequest *req,
                                  string *etag,
                                  real_time *mtime,
                                  uint64_t *psize,
                                  map<string, string> *pattrs,
                                  map<string, string> *pheaders,
                                  optional_yield y)
{
  int ret = req->complete_request(dpp, y, etag, mtime, psize, pattrs, pheaders);
  if (ret == -EIO) {
    ldout(cct, 5) << __func__ << ": complete_request() returned ret=" << ret << dendl;
    set_endpoint_unconnectable(req->get_endpoint());
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
    RGWEndpoint endpoint;
    ret = get_endpoint(endpoint);
    if (ret < 0)
      return ret;

    param_vec_t params;

    if (extra_params) {
      params.insert(params.end(), extra_params->begin(), extra_params->end());
    }

    populate_params(params, nullptr, self_zone_group);

    RGWStreamIntoBufferlist cb(bl);

    RGWRESTStreamReadRequest req(cct, endpoint, &cb, NULL, &params, api_name, host_style);

    map<string, string> headers;
    if (extra_headers) {
      headers.insert(extra_headers->begin(), extra_headers->end());
    }

    ret = req.send_request(dpp, &key, headers, resource, mgr, send_data);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
      return ret;
    }

    ret = req.complete_request(dpp, y);
    if (ret == -EIO) {
      set_endpoint_unconnectable(endpoint);
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
    RGWEndpoint endpoint;
    ret = get_endpoint(endpoint);
    if (ret < 0)
      return ret;

    param_vec_t params;

    if (extra_params) {
      params = make_param_list(extra_params);
    }

    populate_params(params, nullptr, self_zone_group);

    RGWStreamIntoBufferlist cb(bl);

    RGWRESTStreamSendRequest req(cct, method, endpoint, &cb, NULL, &params, api_name, host_style);

    std::map<std::string, std::string> headers;
    if (extra_headers) {
      headers.insert(extra_headers->begin(), extra_headers->end());
    }

    ret = req.send_request(dpp, &key, headers, resource, mgr, send_data);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": send_request() resource=" << resource << " returned ret=" << ret << dendl;
      return ret;
    }

    ret = req.complete_request(dpp, y);
    if (ret == -EIO) {
      set_endpoint_unconnectable(endpoint);
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
    req(cct, conn->get_endpoint(), &cb, NULL, NULL, _conn->get_api_name())
{
  init_common(extra_headers);
}

RGWRESTReadResource::RGWRESTReadResource(RGWRESTConn *_conn,
                                         const string& _resource,
					 param_vec_t& _params,
					 param_vec_t *extra_headers,
                                         RGWHTTPManager *_mgr)
  : cct(_conn->get_ctx()), conn(_conn), resource(_resource), params(_params),
    cb(bl), mgr(_mgr), req(cct, conn->get_endpoint(), &cb, NULL, NULL, _conn->get_api_name())
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

  ret = req.complete_request(dpp, y);
  if (ret == -EIO) {
    conn->set_endpoint_unconnectable(req.get_endpoint());
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
    req(cct, method.c_str(), conn->get_endpoint(), &cb, NULL, NULL, _conn->get_api_name(), _conn->get_host_style())
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
    cb(bl), mgr(_mgr), req(cct, method.c_str(), conn->get_endpoint(), &cb, NULL, NULL, _conn->get_api_name(), _conn->get_host_style())
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

  ret = req.complete_request(dpp, y);
  if (ret == -EIO) {
    conn->set_endpoint_unconnectable(req.get_endpoint());
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
