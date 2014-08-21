#include "common/config.h"
#include "rgw_common.h"

#include "civetweb/civetweb.h"

#define dout_subsys ceph_subsys_civetweb


int rgw_civetweb_log_callback(const struct mg_connection *conn, const char *buf) {
  dout(10) << "civetweb: " << (void *)conn << ": " << buf << dendl;
  return 0;
}


