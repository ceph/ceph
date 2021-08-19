// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_rest_s3_rados.h"
#include "rgw_sal_rados.h"
#include "rgw_rest.h"
#include "rgw_obj_manifest.h"

void RGWGetObjLayout_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/json");

  JSONFormatter f;

  if (op_ret < 0) {
    return;
  }

  f.open_object_section("result");
  ::encode_json("head", head_obj, &f);
  ::encode_json("manifest", *manifest, &f);
  f.open_array_section("data_location");
  for (auto miter = manifest->obj_begin(this); miter != manifest->obj_end(this); ++miter) {
    f.open_object_section("obj");
    rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(static_cast<rgw::sal::RadosStore*>(store));
    uint64_t ofs = miter.get_ofs();
    uint64_t left = manifest->get_obj_size() - ofs;
    ::encode_json("ofs", miter.get_ofs(), &f);
    ::encode_json("loc", raw_loc, &f);
    ::encode_json("loc_ofs", miter.location_ofs(), &f);
    uint64_t loc_size = miter.get_stripe_size();
    if (loc_size > left) {
      loc_size = left;
    }
    ::encode_json("loc_size", loc_size, &f);
    f.close_section();
    rgw_flush_formatter(s, &f);
  }
  f.close_section();
  f.close_section();
  rgw_flush_formatter(s, &f);
}

