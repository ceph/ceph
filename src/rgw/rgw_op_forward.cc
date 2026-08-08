// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <cerrno>
#include <cstddef>
#include <string>

#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_http_errors.h"
#include "rgw_op.h"
#include "rgw_rest_conn.h"
#include "rgw_xml.h"
#include "rgw_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

// try to parse the xml <Error> response body
static bool parse_aws_s3_error(const std::string& input, rgw_err& err)
{
  RGWXMLParser parser;
  if (!parser.init()) {
    return false;
  }
  if (!parser.parse(input.c_str(), input.length(), 1)) {
    return false;
  }
  auto error = parser.find_first("Error");
  if (!error) {
    return false;
  }
  if (auto code = error->find_first("Code"); code) {
    err.err_code = code->get_data();
  }
  if (auto message = error->find_first("Message"); message) {
    err.message = message->get_data();
  }
  return true;
}

int rgw_forward_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const rgw_owner& effective_owner,
                                  bufferlist* indata, JSONParser* jp,
                                  const req_info& req, rgw_err& err,
                                  optional_yield y)
{
  const auto& period = site.get_period();
  if (!period) {
    return 0; // not multisite
  }
  if (site.is_meta_master()) {
    return 0; // don't need to forward metadata requests
  }
  const auto& pmap = period->period_map;
  const auto zg = pmap.zonegroups.find(pmap.master_zonegroup);
  if (zg == pmap.zonegroups.end()) {
    return -EINVAL;
  }
  const auto z = zg->second.zones.find(zg->second.master_zone);
  if (z == zg->second.zones.end()) {
    return -EINVAL;
  }
  const RGWAccessKey& creds = site.get_zone_params().system_key;

  bufferlist data;
  if (indata == nullptr) {
    // forward() needs an input bufferlist to set the content-length
    indata = &data;
  }

  // use the master zone's endpoints
  auto conn = RGWRESTConn{dpp->get_cct(), z->second.id, z->second.endpoints,
                          creds, site.get_zonegroup().id, zg->second.api_name};
  bufferlist outdata;
  constexpr size_t max_response_size = 128 * 1024; // we expect a very small response
  auto result = conn.forward(dpp, effective_owner, req,
                             max_response_size, indata, &outdata, y);
  if (!result) {
    return result.error();
  }
  err.http_ret = *result;
  if (err.is_err() && outdata.length()) { // 4xx or 5xx
    static_cast<void>(parse_aws_s3_error(rgw_bl_str(outdata), err));
  }
  const int ret = rgw_http_error_to_errno(err.http_ret);
  if (ret < 0) {
    return ret;
  }
  if (jp && !jp->parse(outdata.c_str(), outdata.length())) {
    ldpp_dout(dpp, 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }
  return 0;
}
