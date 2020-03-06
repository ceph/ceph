#include "rgw_rest_s3control.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#include "rgw_tools.h"
#include "rgw_zone.h"

static constexpr auto AMZ_ACCOUNT_ID_ATTR = "x-amz-account-id";

RGWOp* RGWHandler_S3AccountPublicAccessBlock::op_put()
{
  return new RGWPutAccountPublicAccessBlock;
}

RGWOp* RGWHandler_S3AccountPublicAccessBlock::op_get()
{
  return new RGWGetAccountPublicAccessBlock;
}

int RGWPutAccountPublicAccessBlock::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("user-policy", RGW_CAP_WRITE);
}

RGWOpType RGWPutAccountPublicAccessBlock::get_type()
{
  return RGW_OP_PUT_ACCOUNT_PUBLIC_ACCESS_BLOCK;
}

int RGWPutAccountPublicAccessBlock::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if(int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }
  // TODO implement x-amz-account-id stuff here
  return 0;
}

int RGWPutAccountPublicAccessBlock::get_params()
{
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  std::tie(op_ret, data) = rgw_rest_read_all_input(s, max_size, false);
  return op_ret;
}

void RGWPutAccountPublicAccessBlock::execute()
{
  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldpp_dout(this, 0) << "ERROR: failed to initialize parser" << dendl;
    op_ret = -EINVAL;
    return;
  }

  op_ret = get_params();
  if (op_ret < 0)
    return;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    ldpp_dout(this, 0) << "ERROR: malformed XML" << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  try {
    RGWXMLDecoder::decode_xml("PublicAccessBlockConfiguration", access_conf, &parser, true);
  } catch (RGWXMLDecoder::err &err) {
    ldpp_dout(this, 5) << "unexpected xml:" << err << dendl;
    op_ret = -ERR_MALFORMED_XML;
    return;
  }

  if (!store->svc()->zone->is_meta_master()) {
    op_ret = forward_request_to_master(s, NULL, store, data, nullptr);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  bufferlist bl;
  access_conf.encode(bl);

  map<string, bufferlist> attrs;
  attrs.emplace(RGW_ATTR_PUBLIC_ACCESS, std::move(bl));
  if (const auto account_name = s->user->get_tenant();
      ! account_name.empty()) {
    auto svc = store->getRados()->pctl->svc;
    auto obj_ctx = svc->sysobj->init_obj_ctx();

    bufferlist empty_bl;
    op_ret = rgw_put_system_obj(obj_ctx, svc->zone->get_zone_params().user_uid_pool,
                                account_name,
                                empty_bl, false,
                                nullptr, real_time(), null_yield,
                                &attrs);
    return;
  } else {
    ldpp_dout(this, 5) << __func__ << "ERROR: Invalid account name" << dendl;
    op_ret = -EINVAL;
    return;
  }
}

void RGWPutAccountPublicAccessBlock::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWGetAccountPublicAccessBlock::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("user-policy", RGW_CAP_READ);
}

RGWOpType RGWGetAccountPublicAccessBlock::get_type()
{
  return RGW_OP_GET_ACCOUNT_PUBLIC_ACCESS_BLOCK;
}

int RGWGetAccountPublicAccessBlock::get_params()
{
  if (auto it = s->info.x_meta_map.find(AMZ_ACCOUNT_ID_ATTR);
      it != s->info.x_meta_map.end()) {
    const auto& account_name = it->second;
    if (account_name.empty()) {
      ldpp_dout(this, 5) << "ERROR: empty account name"
                         << dendl;
      return -EINVAL;
    }

    if (account_name != s->user->get_tenant()) {
      ldpp_dout(this, 5) << __func__ << "ERROR: Account ID doesn't match the tenant" << dendl;
      return -EINVAL;
    }
  }


  return 0;
}

int RGWGetAccountPublicAccessBlock::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if(int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }
  // TODO implement x-amz-account-id stuff here
  return 0;
}

void RGWGetAccountPublicAccessBlock::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }


  auto svc = store->getRados()->pctl->svc;
  auto obj_ctx = svc->sysobj->init_obj_ctx();
  const auto& account_name = s->user->get_tenant();

  map<string, bufferlist> attrs;
  bufferlist bl;
  op_ret = rgw_get_system_obj(obj_ctx, svc->zone->get_zone_params().user_uid_pool,
                              account_name, bl, nullptr,
                              nullptr, null_yield,
                              &attrs);

  if (op_ret < 0) {
    ldpp_dout(this, 20) << __func__ << " getting account sys obj. returned r=" << op_ret << dendl;
    return;
  }

  if (auto aiter = attrs.find(RGW_ATTR_PUBLIC_ACCESS);
      aiter != attrs.end()) {
    bufferlist::const_iterator iter{&aiter->second};
    try {
      access_conf.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "decode access_conf failed" << dendl;
      op_ret = -EIO;
      return;
    }
  } else {
    ldpp_dout(this, 20) << "Can't find public access attr, returning the default" << dendl;
  }
}

void RGWGetAccountPublicAccessBlock::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  access_conf.dump_xml(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWOpType RGWDeleteAccountPublicAccessBlock::get_type()
{
  return RGW_OP_DELETE_ACCOUNT_PUBLIC_ACCESS_BLOCK;
}

int RGWDeleteAccountPublicAccessBlock::get_params()
{
  if (auto it = s->info.x_meta_map.find(AMZ_ACCOUNT_ID_ATTR);
      it != s->info.x_meta_map.end()) {
    const auto& account_name = it->second;
    if (account_name.empty()) {
      ldpp_dout(this, 5) << "ERROR: empty account name"
                         << dendl;
      return -EINVAL;
    }

    if (account_name != s->user->get_tenant()) {
      ldpp_dout(this, 5) << __func__ << "ERROR: Account ID doesn't match the tenant" << dendl;
      return -EINVAL;
    }
  }


  return 0;
}
int RGWDeleteAccountPublicAccessBlock::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("user-policy", RGW_CAP_WRITE);
}

int RGWDeleteAccountPublicAccessBlock::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if(int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }
  // TODO implement x-amz-account-id stuff here
  return 0;
}


void RGWDeleteAccountPublicAccessBlock::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }


  auto svc = store->getRados()->pctl->svc;
  auto obj_ctx = svc->sysobj->init_obj_ctx();
  const auto& account_name = s->user->get_tenant();

  map<string, bufferlist> attrs;
  bufferlist bl;
  op_ret = rgw_get_system_obj(obj_ctx, svc->zone->get_zone_params().user_uid_pool,
                              account_name, bl, nullptr,
                              nullptr, null_yield,
                              &attrs);

  if (op_ret == -ENOENT) {
    // object doesn't exist, so deletion is successful
    op_ret = 0;
    return;
  }

  if (op_ret < 0) {
    ldpp_dout(this, 20) << __func__ << " getting account sys obj. returned r=" << op_ret << dendl;
    return;
  }

  attrs.erase(RGW_ATTR_PUBLIC_ACCESS);
  op_ret = rgw_put_system_obj(obj_ctx, svc->zone->get_zone_params().user_uid_pool,
                              account_name,
                              bl, false,
                              nullptr, real_time(), null_yield,
                              &attrs);


}

void RGWDeleteAccountPublicAccessBlock::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}
