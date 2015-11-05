// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_HTTP_ERRORS_H_
#define RGW_HTTP_ERRORS_H_

#include "rgw_common.h"

typedef const std::map<int,const std::pair<int, const char*>> rgw_http_errors;

extern rgw_http_errors rgw_http_s3_errors;

extern rgw_http_errors rgw_http_swift_errors;

#endif
