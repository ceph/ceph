// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_user.h"

RGWSI_User::RGWSI_User(CephContext *cct, boost::asio::io_context& ioc)
  : RGWServiceInstance(cct, ioc) {
}

RGWSI_User::~RGWSI_User() {
}
