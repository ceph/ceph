// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FILE_H
#define RGW_FILE_H

#include "include/rados/rgw_file.h"

/* internal header */

/*
  read directory content
*/

class RGWListBucketsRequest : public RGWLibRequest,
			      public RGWListBuckets_ObjStore_Lib /* RGWOp */
{
public:
  std::string user_id;
  uint64_t* offset;
  void* cb_arg;
  rgw_readdir_cb rcb;

  RGWListBucketsRequest(CephContext* _cct, char *_user_id,
			rgw_readdir_cb _rcb, void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct), user_id(_user_id), offset(_offset), cb_arg(_cb_arg),
      rcb(_rcb) {
    // req->op = op
    op = this;
  }

  virtual bool only_bucket() { return false; }

  int operator()(const std::string& name, const std::string& marker) {
    rcb(name.c_str(), cb_arg, (*offset)++);
    return 0;
  }

}; /* RGWListBucketsRequest */

#endif /* RGW_FILE_H */
