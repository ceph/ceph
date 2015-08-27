// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_user.h"
#include "rgw_rest_lib.h"


RGWHandler* RGWRESTMgr_Lib::get_handler(struct req_state* s)
{
  if (!s->librgw_user_command) {
    return new RGWHandler_ObjStore_Lib;
  }
  return RGWRESTMgr_S3::get_handler(s);
}
