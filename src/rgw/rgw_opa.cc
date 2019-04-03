// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_opa.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int rgw_opa_authorize(RGWOp *& op,
                      req_state * const s)
{

  ldpp_dout(op, 2) << "authorizing request using OPA" << dendl;

  /* get OPA url */
  const string& opa_url = s->cct->_conf->rgw_opa_url;
  if (opa_url == "") {
    ldpp_dout(op, 2) << "OPA_URL not provided" << dendl;
    return -ERR_INVALID_REQUEST;
  }
  ldpp_dout(op, 2) << "OPA URL= " << opa_url.c_str() << dendl;

  /* get authentication token for OPA */
  const string& opa_token = s->cct->_conf->rgw_opa_token;

  int ret;
  bufferlist bl;
  RGWHTTPTransceiver req(s->cct, "POST", opa_url.c_str(), &bl);

  /* set required headers for OPA request */
  req.append_header("X-Auth-Token", opa_token);
  req.append_header("Content-Type", "application/json");

  /* check if we want to verify OPA server SSL certificate */
  req.set_verify_ssl(s->cct->_conf->rgw_opa_verify_ssl);

  /* create json request body */
  JSONFormatter jf;
  jf.open_object_section("");
  jf.open_object_section("input");
  jf.dump_string("method", s->info.env->get("REQUEST_METHOD"));
  jf.dump_string("relative_uri", s->relative_uri.c_str());
  jf.dump_string("decoded_uri", s->decoded_uri.c_str());
  jf.dump_string("params", s->info.request_params.c_str());
  jf.dump_string("request_uri_aws4", s->info.request_uri_aws4.c_str());
  jf.dump_string("object_name", s->object.name.c_str());
  jf.dump_object("user_info", *s->user);
  jf.dump_object("bucket_info", s->bucket_info);
  jf.close_section();
  jf.close_section();

  std::stringstream ss;
  jf.flush(ss);
  req.set_post_data(ss.str());
  req.set_send_length(ss.str().length());

  /* send request */
  ret = req.process(null_yield);
  if (ret < 0) {
    ldpp_dout(op, 2) << "OPA process error:" << bl.c_str() << dendl;
    return ret;
  }

  /* check OPA response */
  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    ldpp_dout(op, 2) << "OPA parse error: malformed json" << dendl;
    return -EINVAL;
  }

  bool opa_result;
  JSONDecoder::decode_json("result", opa_result, &parser);

  if (opa_result == false) {
    ldpp_dout(op, 2) << "OPA rejecting request" << dendl;
    return -EPERM;
  }

  ldpp_dout(op, 2) << "OPA accepting request" << dendl;
  return 0;
}
