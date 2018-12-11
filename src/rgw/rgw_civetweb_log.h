// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CIVETWEB_LOG_H
#define CEPH_RGW_CIVETWEB_LOG_H

int rgw_civetweb_log_callback(const struct mg_connection *conn, const char *buf);
int rgw_civetweb_log_access_callback(const struct mg_connection *conn, const char *buf);

#endif
