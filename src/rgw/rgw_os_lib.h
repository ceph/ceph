// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_OS_LIB_H
#define RGW_OS_LIB_H

#include <functional>
#include "rgw_common.h"
#include "rgw_lib.h"

/* RGWOps */

class RGWListBuckets_OS_Lib : public RGWListBuckets {
public:

  RGWListBuckets_OS_Lib() {}
  ~RGWListBuckets_OS_Lib() {}

  virtual void send_response_begin(bool has_buckets);
  virtual void send_response_data(RGWUserBuckets& buckets);
  virtual void send_response_end();

  int get_params() {
    limit = -1; /* no limit */
    return 0;
  }
}; /* RGWListBuckets_OS_Lib */

class RGWListBucket_OS_Lib : public RGWListBucket {
public:
  RGWListBucket_OS_Lib() {
    default_max = 1000;
  }

  ~RGWListBucket_OS_Lib() {}

  int get_params();
  virtual void send_response();

  virtual void send_versioned_response() {
    send_response();
  }
}; /* RGWListBucket_OS_Lib */

class RGWStatBucket_OS_Lib : public RGWStatBucket {
public:
  RGWStatBucket_OS_Lib() {}
  ~RGWStatBucket_OS_Lib() {}

  virtual void send_response();

}; /* RGWStatBucket_OS_Lib */

class RGWCreateBucket_OS_Lib : public RGWCreateBucket {
public:
  RGWCreateBucket_OS_Lib() {}
  ~RGWCreateBucket_OS_Lib() {}
  virtual int get_params();
  virtual void send_response();
}; /* RGWCreateBucket_OS_Lib */

class RGWDeleteBucket_OS_Lib : public RGWDeleteBucket {
public:
  RGWDeleteBucket_OS_Lib() {}
  ~RGWDeleteBucket_OS_Lib() {}
}; /* RGWCreateBucket_OS_Lib */

class RGWPutObj_OS_Lib : public RGWPutObj
{
public:
  RGWPutObj_OS_Lib() {}
  ~RGWPutObj_OS_Lib() {}

  virtual int verify_params();
  virtual int get_params();

}; /* RGWPutObj_OS_Lib */

#endif /* RGW_OS_LIB_H */
