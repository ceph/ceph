// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "rgw_common.h"

#include "civetweb/civetweb.h"
#include "rgw_crypt_sanitize.h"

#define dout_subsys ceph_subsys_civetweb


#define dout_context g_ceph_context
int rgw_civetweb_log_callback(const struct mg_connection *conn, const char *buf) {
  dout(0) << "civetweb: " << (void *)conn << ": " << rgw::crypt_sanitize::log_content(buf) << dendl;
  return 0;
}

int rgw_civetweb_log_access_callback(const struct mg_connection *conn, const char *buf) {
  dout(1) << "civetweb: " << (void *)conn << ": " << rgw::crypt_sanitize::log_content(buf) << dendl;
  return 0;
}


