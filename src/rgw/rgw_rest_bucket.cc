// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_op.h"
#include "rgw_bucket.h"
#include "rgw_rest_bucket.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

static int forward_request_to_master(struct req_state *s, obj_version *objv, RGWRados *store, bufferlist& in_data, JSONParser *jp);

class RGWOp_Bucket_Info : public RGWRESTOp {

public:
  RGWOp_Bucket_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute();

  virtual const string name() { return "get_bucket_info"; }
};

void RGWOp_Bucket_Info::execute()
{
  RGWBucketAdminOpState op_state;

  bool fetch_stats;

  std::string bucket;

  string uid_str;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "stats", false, &fetch_stats);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_fetch_stats(fetch_stats);

  http_ret = RGWBucketAdminOp::info(store, op_state, flusher);
}

class RGWOp_Get_Policy : public RGWRESTOp {

public:
  RGWOp_Get_Policy() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute();

  virtual const string name() { return "get_policy"; }
};

void RGWOp_Get_Policy::execute()
{
  RGWBucketAdminOpState op_state;

  std::string bucket;
  std::string object;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  http_ret = RGWBucketAdminOp::get_policy(store, op_state, flusher);
}

class RGWOp_Check_Bucket_Index : public RGWRESTOp {

public:
  RGWOp_Check_Bucket_Index() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "check_bucket_index"; }
};

void RGWOp_Check_Bucket_Index::execute()
{
  std::string bucket;

  bool fix_index;
  bool check_objects;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "fix", false, &fix_index);
  RESTArgs::get_bool(s, "check-objects", false, &check_objects);

  op_state.set_bucket_name(bucket);
  op_state.set_fix_index(fix_index);
  op_state.set_check_objects(check_objects);

  http_ret = RGWBucketAdminOp::check_index(store, op_state, flusher);
}

static void rgw_bucket_object_pre_exec(struct req_state *s)
{
  if (s->expect_cont)
    dump_continue(s);

  dump_bucket_from_state(s);
}

class RGWOp_Bucket_Syncing_Parser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) {
    return new XMLObj;
  }

public:
  RGWOp_Bucket_Syncing_Parser() {}
  ~RGWOp_Bucket_Syncing_Parser() {}

  int get_syncing_status(bool *status) {
    XMLObj *config = find_first("SyncingConfiguration");
    if (!config)
      return -EINVAL;

    *status = false;

    XMLObj *field = config->find_first("Status");
    if (!field)
      return 0;

    string& s = field->get_data();

    if (stringcasecmp(s, "Enabled") == 0) {
      *status = true;
    } else if (stringcasecmp(s, "Disabled") == 0) {
      *status = false;
    }

    return 0;
  }
};

class RGWOp_Check_Bucket_Syncing : public RGWRESTOp {
protected:
  bool enable_syncing;
public:
  RGWOp_Check_Bucket_Syncing() : enable_syncing(false) {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  //int verify_permission();
  void pre_exec();
  void execute();

  void send_response();
  virtual const string name() { return "check_bucket_syncing"; }
  virtual RGWOpType get_type() { return RGW_OP_GET_BUCKET_SYNCING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }

};

void RGWOp_Check_Bucket_Syncing::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWOp_Check_Bucket_Syncing::execute()
{
  std::string bucket;
  std::string tenant;
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  RGWObjectCtx obj_ctx(store);
  op_ret = store->get_bucket_info(obj_ctx,tenant, bucket, s->bucket_info, NULL, &s->bucket_attrs);
  http_ret = op_ret ;
  if (op_ret < 0){
    return ;
  }
  enable_syncing = s->bucket_info.datasync_flag_enabled();  
}

void RGWOp_Check_Bucket_Syncing::send_response(){

  if (!flusher.did_start()) {
    set_req_state_err(s, http_ret);
    dump_errno(s);
    end_header(s, this);
  }
  
  s->formatter->open_object_section_in_ns("SyncingConfiguration", XMLNS_AWS_S3);
  const char *status = (enable_syncing ? "Enabled" : "Disabled");
  
  s->formatter->dump_string("Status", status);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWOp_Bucket_Syncing : public RGWRESTOp {
protected:
  bool enable_syncing;
public:
  RGWOp_Bucket_Syncing() : enable_syncing(false) {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  //int verify_permission();
  void pre_exec();
  void execute();

  int get_params();

  //void send_response();
  virtual const string name() { return "syncing_bucket"; }
  virtual RGWOpType get_type() { return RGW_OP_SET_BUCKET_SYNCING; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_WRITE; }
};

/*void RGWOp_Bucket_Syncing::send_response(){
    
}*/

static int forward_request_to_master(struct req_state *s, obj_version *objv,
				    RGWRados *store, bufferlist& in_data,
				    JSONParser *jp)
{
  if (!store->rest_master_conn) {
    ldout(s->cct, 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldout(s->cct, 0) << "sending create_bucket request to master zonegroup" << dendl;
  bufferlist response;
  string uid_str = s->user->user_id.to_str();
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = store->rest_master_conn->forward(uid_str, s->info, objv,
					    MAX_REST_RESPONSE, &in_data,
					    &response);
  if (ret < 0)
    return ret;

  ldout(s->cct, 20) << "response: " << response.c_str() << dendl;
  if (jp && !jp->parse(response.c_str(), response.length())) {
    ldout(s->cct, 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWOp_Bucket_Syncing::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWOp_Bucket_Syncing::execute()
{
/*  if (!store->is_meta_master()) {
    ldout(s->cct, 0) << "NOTICE:BucketSyncing not in master " << dendl;
    return;
  }*/
  RGWObjectCtx obj_ctx(store);

  std::string bucket;
  std::string tenant;

  int shards_num ;
  int shard_id ;
  
  if (!store->is_meta_master()) {
    bufferlist in_data;
    JSONParser jp;
    op_ret = forward_request_to_master(s, NULL, store, in_data, &jp);
    
    if (op_ret < 0) {
      ldout(s->cct, 20) << __func__ << "forward_request_to_master returned ret=" << op_ret << dendl;
    }
    goto done;
  }

  op_ret = get_params();

  if (op_ret < 0)
    goto done;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  
  op_ret = store->get_bucket_info(obj_ctx, tenant, bucket, s->bucket_info, NULL, &s->bucket_attrs);  
  if(op_ret < 0){
    goto done;
   }
  
  if (enable_syncing) {
    s->bucket_info.flags &= ~BUCKET_DATASYNC_DISABLED;
  } else {
    s->bucket_info.flags |= BUCKET_DATASYNC_DISABLED;
  }
  
  op_ret = store->put_bucket_instance_info(s->bucket_info, false, real_time(),
					  &s->bucket_attrs);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "NOTICE: put_bucket_info on bucket=" << s->bucket.name
		     << " returned err=" << op_ret << dendl;
    goto done;
  }

  shards_num = s->bucket_info.num_shards? s->bucket_info.num_shards : 1;
  shard_id = s->bucket_info.num_shards? 0 : -1;

  if (!enable_syncing) {
    op_ret = store->stop_bi_log_entries(s->bucket_info.bucket, -1);
    if (op_ret < 0) {
      lderr(store->ctx()) << "ERROR: failed writing bilog" << dendl;
      goto done;
    }
  } else {
    op_ret = store->resync_bi_log_entries(s->bucket_info.bucket, -1);
    if (op_ret < 0) {
      lderr(store->ctx()) << "ERROR: failed writing bilog" << dendl;       
      goto done;
    }
  }


  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    op_ret = store->data_log->add_entry(s->bucket_info.bucket, shard_id);
    if (op_ret < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
      goto done; 
    }
  }
 done:
  http_ret = op_ret ;
  return ;
}


int RGWOp_Bucket_Syncing::get_params()
{ 
  #define GET_BUCKET_SYNCING_BUF_MAX (128 * 1024)

  char *data;
  int len = 0;
  int r =
    rgw_rest_read_all_input(s, &data, &len, GET_BUCKET_SYNCING_BUF_MAX);
  if (r < 0) {
    return r;
  }

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  RGWOp_Bucket_Syncing_Parser parser;
  JSONParser p;

  if(!p.parse(data, len)){
    ldout(s->cct, 0) << "ERROR: failed to parser malformed json" << dendl;
    if (!parser.init()) {
      ldout(s->cct, 0) << "ERROR: failed to initialize xml parser" << dendl;
      r = -EIO;
      goto done;
   }else if (!parser.parse(data, len, 1)) {
      ldout(s->cct, 0) << "ERROR: failed to parser xml parser" << dendl;
      r = -EINVAL;
      goto done; 
   }else{
      r = parser.get_syncing_status(&enable_syncing);
   }
   }else{
      JSONObjIter status_iter = p.find_first("SyncingConfiguration");
      for (; !status_iter.end(); ++status_iter) {
        JSONObj *o = *status_iter;

        if(o->is_array()) {
          vector<string> array;
          array = o->get_array_elements();
          for (auto i : array) {
            JSONParser p;
            if (!p.parse(i.c_str(), i.length())) 
                return -EINVAL;
            JSONObjIter iter = p.find_first("Status");
            
            if (iter.end()) {
              continue ; 
            } 
            JSONObj *obj = *iter;
            if (stringcasecmp(obj->get_data(), "Enabled") == 0) {
                enable_syncing = true;
                break ;
              } else if (stringcasecmp(obj->get_data(), "Disabled") == 0) {
                enable_syncing = false;
                break ; 
              }
            }
          }else{
            continue;
         }     
      }
}
  
  done:
  free(data);
  return r;
  
}

class RGWOp_Bucket_Link : public RGWRESTOp {

public:
  RGWOp_Bucket_Link() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "link_bucket"; }
};

void RGWOp_Bucket_Link::execute()
{
  std::string uid_str;
  std::string bucket;
  std::string bucket_id;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "bucket-id", bucket_id, &bucket_id);

  rgw_user uid(uid_str);
  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_bucket_id(bucket_id);

  http_ret = RGWBucketAdminOp::link(store, op_state);
}

class RGWOp_Bucket_Unlink : public RGWRESTOp {

public:
  RGWOp_Bucket_Unlink() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "unlink_bucket"; }
};

void RGWOp_Bucket_Unlink::execute()
{
  std::string uid_str;
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);

  http_ret = RGWBucketAdminOp::unlink(store, op_state);
}

class RGWOp_Bucket_Remove : public RGWRESTOp {

public:
  RGWOp_Bucket_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_bucket"; }
};

void RGWOp_Bucket_Remove::execute()
{
  std::string bucket;
  bool delete_children;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "purge-objects", false, &delete_children);

  op_state.set_bucket_name(bucket);
  op_state.set_delete_children(delete_children);

  http_ret = RGWBucketAdminOp::remove_bucket(store, op_state);
}

class RGWOp_Object_Remove: public RGWRESTOp {

public:
  RGWOp_Object_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_object"; }
};

void RGWOp_Object_Remove::execute()
{
  std::string bucket;
  std::string object;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  http_ret = RGWBucketAdminOp::remove_object(store, op_state);
}

RGWOp *RGWHandler_Bucket::op_get()
{

  if (s->info.args.sub_resource_exists("policy"))
    return new RGWOp_Get_Policy;

  if (s->info.args.sub_resource_exists("index"))
    return new RGWOp_Check_Bucket_Index;

  if (s->info.args.sub_resource_exists("syncing"))
    return new RGWOp_Check_Bucket_Syncing ; 

  return new RGWOp_Bucket_Info;
}

RGWOp *RGWHandler_Bucket::op_put()
{
  if (s->info.args.sub_resource_exists("syncing"))
    return new RGWOp_Bucket_Syncing;

  return new RGWOp_Bucket_Link;
}

RGWOp *RGWHandler_Bucket::op_post()
{
  return new RGWOp_Bucket_Unlink;
}

RGWOp *RGWHandler_Bucket::op_delete()
{
  if (s->info.args.sub_resource_exists("object"))
    return new RGWOp_Object_Remove;

  return new RGWOp_Bucket_Remove;
}

