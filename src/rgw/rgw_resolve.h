#ifndef CEPH_RGW_RESOLVE_H
#define CEPH_RGW_RESOLVE_H

#include "rgw_common.h"

class RGWDNSResolver;

class RGWResolver {
  RGWDNSResolver *resolver;

  ~RGWResolver();
public:
  RGWResolver();
  int resolve_cname(const string& hostname, string& cname, bool *found);
};


extern void rgw_init_resolver(void);
extern RGWResolver *rgw_resolver;

#endif
