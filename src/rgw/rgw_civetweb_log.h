#ifndef CEPH_RGW_CIVETWEB_LOG_H
#define CEPH_RGW_CIVETWEB_LOG_H

int rgw_civetweb_log_callback(const struct mg_connection *conn, const char *buf);
int rgw_civetweb_log_access_callback(const struct mg_connection *conn, const char *buf);

#endif
