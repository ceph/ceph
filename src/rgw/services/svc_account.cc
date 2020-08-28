// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_account.h"
#include "rgw/rgw_account.h"

RGWSI_Account::RGWSI_Account(CephContext *cct): RGWServiceInstance(cct) {
}

RGWSI_Account::~RGWSI_Account() {
}

std::string RGWSI_Account::get_meta_key(const RGWAccountInfo& info) {
  return info.get_id();
}
