#include "rgw_rest_s3control.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "services/svc_zone.h"

RGWOp* RGWHandler_S3AccountPublicAccessBlock::op_put()
{
  return new RGWPutAccountPublicAccessBlock;
}

int RGWPutAccountPublicAccessBlock::check_caps(RGWUserCaps& caps)
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

  if(int ret = check_caps(s->user->caps); ret == 0) {
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

  // TODO implement write!
}

void RGWPutAccountPublicAccessBlock::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}
