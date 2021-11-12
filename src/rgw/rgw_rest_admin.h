// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_rest.h"

class RGWRESTMgr_Admin : public RGWRESTMgr {
public:
  RGWRESTMgr_Admin() {}
  ~RGWRESTMgr_Admin() override {}
};
