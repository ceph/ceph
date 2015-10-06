// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_op.h"
#include "rgw_request.h"

#define dout_subsys ceph_subsys_rgw

/* XXX */
void RGWRequest::log_format(struct req_state *s, const char *fmt, ...)
{
#define LARGE_SIZE 1024
  char buf[LARGE_SIZE];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  log(s, buf);
} /* RGWRequest::log_format */

void RGWRequest::log_init() {
  ts = ceph_clock_now(g_ceph_context);
}

void RGWRequest::log(struct req_state *s, const char *msg) {
  if (s->info.method && req_str.size() == 0) {
    req_str = s->info.method;
    req_str.append(" ");
    req_str.append(s->info.request_uri);
  }
  utime_t t = ceph_clock_now(g_ceph_context) - ts;
  dout(2) << "req " << id << ":" << t << ":" << s->dialect << ":"
	  << req_str << ":" << (op ? op->name() : "") << ":" << msg
	  << dendl;
}
