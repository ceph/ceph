// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "objclass/objclass.h"
#include "common/Formatter.h"
#include "common/Clock.h"
#include "common/ceph_json.h"

#include "include/utime.h"

#include "cls/otp/cls_otp_types.h"

using namespace rados::cls::otp;

void otp_info_t::dump(Formatter *f) const
{
  encode_json("type", (int)type, f);
  encode_json("id", id, f);
  encode_json("seed", seed, f);
  encode_json("time_ofs", time_ofs, f);
  encode_json("step_size", step_size, f);
  encode_json("window", window, f);
}

void otp_info_t::decode_json(JSONObj *obj)
{
  int t{-1};
  JSONDecoder::decode_json("type", t, obj);
  type = (OTPType)t;
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("seed", seed, obj);
  JSONDecoder::decode_json("time_ofs", time_ofs, obj);
  JSONDecoder::decode_json("step_size", step_size, obj);
  JSONDecoder::decode_json("window", window, obj);
}
