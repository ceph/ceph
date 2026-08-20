// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_vector_bucket.h"

#include "rgw_op.h"
#include "rgw_vector_bucket_admin.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_VectorBucket_Get : public RGWRESTOp {
public: 
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    op_ret = 0;
  }

  const char* name() const override { return "get_vectorbucket"; }
};

class RGWOp_VectorBucket_Delete : public RGWRESTOp {
public: 
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    op_ret = 0;
  }

  const char* name() const override { return "delete_vectorbucket"; }
};

class RGWOp_VectorBucketSession_Invalid : public RGWRESTOp {
  int check_caps(const RGWUserCaps& caps) override {
    return 0;
  }

  void execute(optional_yield) override {
    op_ret = -EINVAL;
  }

  const char* name() const override {return "invalid_vectorbucket_session";} 
}; 


//class for GET /admin/vectorbucket/session?vectorbucket=<name>
class RGWOp_VectorBucketSession_Info : public RGWRESTOp {
public: 
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state;
    std::string uid; 
    bool uid_existed = false; 
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed); 
    if (op_ret < 0 || uid_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL; 
      return;
    }

    std::string tenant; 
    RESTArgs::get_string(s, "tenant", tenant, &tenant); 

    std::string bucket_name; 
    bool bucket_existed = false; 
    op_ret = RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,
                              &bucket_existed);
    if(op_ret < 0 || !bucket_existed || bucket_name.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL; 
      return;
    }

    op_state.uid.tenant = tenant; 
    op_state.bucket_name = bucket_name; 
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::get_session_info(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "get_vectorbucket_session"; }
};


// class for GET /admin/vectorbucket/session?uid=<user>
class RGWOp_VectorBucketSession_List : public RGWRESTOp { 
public: 
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state; 
    
    std::string bucket_name; 
    bool bucket_existed = false; 
    op_ret = RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,
                              &bucket_existed);
    if (op_ret < 0 || bucket_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }

    std::string uid; 
    bool uid_existed = false; 
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed); 
    if (op_ret < 0 || !uid_existed || uid.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL; 
      return;
    }

    RESTArgs::get_uint32(s, "max-entries", op_state.max_entries, &op_state.max_entries); 
    RESTArgs::get_string(s, "marker", op_state.marker, &op_state.marker); 
    op_state.uid = rgw_user(uid); 

    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::list_sessions(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "list_vectorbucket_session";}
};

//class for DELETE /admin/vectorbucket/session?vectorbucket=name

class RGWOp_VectorBucketSession_Remove : public RGWRESTOp {
public: 
  int check_caps(const RGWUserCaps &caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state; 

    std::string uid; 
    bool uid_existed = false; 
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed); 
    if (op_ret < 0 || uid_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL; 
      return;
    }

    std::string tenant; 
    RESTArgs::get_string(s, "tenant", tenant, &tenant); 

    std::string bucket_name; 
    bool bucket_existed = false; 
    op_ret = RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,
                              &bucket_existed);
    if(op_ret < 0 || !bucket_existed || bucket_name.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL; 
      return;
    }

    op_state.uid.tenant = tenant; 
    op_state.bucket_name = bucket_name; 

    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::remove_session(driver, op_state, this, y);  
  }

  const char* name() const override { return "remove_vectorbucket_session"; }  
};

RGWRESTMgr_VectorBucket::RGWRESTMgr_VectorBucket() {
  register_resource("session", new RGWRESTMgr_VectorBucketSession);
}

RGWOp* RGWHandler_VectorBucket::op_get() {
  return new RGWOp_VectorBucket_Get;
}

RGWOp* RGWHandler_VectorBucket::op_delete() {
  return new RGWOp_VectorBucket_Delete;
}

RGWOp* RGWHandler_VectorBucketSession::op_get() {
  
  std::string uid; 
  bool uid_existed = false; 
  if (RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed) < 0) {
    return new RGWOp_VectorBucketSession_Invalid;
  }
  
  std::string bucket_name; 
  bool bucket_existed = false; 
  if (RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,&bucket_existed) < 0) {
    return new RGWOp_VectorBucketSession_Invalid;
  }

  // if both bucket and uid params exist in request ; or don't exist in request -> invalid op 
  if(bucket_existed == uid_existed) {
    return new RGWOp_VectorBucketSession_Invalid;
  }

  return bucket_existed ? static_cast<RGWOp*>(new RGWOp_VectorBucketSession_Info) : static_cast<RGWOp*>(new RGWOp_VectorBucketSession_List);

}

RGWOp* RGWHandler_VectorBucketSession::op_delete() {

 std::string uid; 
  bool uid_existed = false; 
  if (RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed) < 0) {
    return new RGWOp_VectorBucketSession_Invalid;
  }
  
  std::string bucket_name; 
  bool bucket_existed = false; 
  if (RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,&bucket_existed) < 0) {
    return new RGWOp_VectorBucketSession_Invalid;
  }

  if(uid_existed || !bucket_existed || bucket_name.empty()) {
    return new RGWOp_VectorBucketSession_Invalid;
  }

  return new RGWOp_VectorBucketSession_Remove;
}