// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_op_rados.h"

class RGWGetObjLayout_ObjStore_S3 : public RGWGetObjLayout {
public:
  RGWGetObjLayout_ObjStore_S3() {}
  ~RGWGetObjLayout_ObjStore_S3() {}

  void send_response() override;
};

