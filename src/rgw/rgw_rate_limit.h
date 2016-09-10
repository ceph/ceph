// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_RATE_LIMIT_H
#define CEPH_RGW_RATE_LIMIT_H

#include "rgw_op.h"

using namespace std;

int rgw_rate_limit_init();
bool rgw_rate_limit_ok(string& user, RGWOp *op);

#endif
