// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_s3.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

namespace rgw { namespace sal {

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;
  
  return dynamic_cast<FilterObject*>(t)->get_next();
}  

static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterUser*>(t)->get_next();
}

int RGWGetBucketCB::handle_data(bufferlist& bl, bool *pause){
	//ldout(cct, 20) << " AMIN: " << __func__ << __LINE__ << " bufferlist is: " << in_data << dendl;
	string in_data = bl.c_str();
    //while(true){
      vector<string> all;
      vector<string> all_keys = get_xml_data(in_data, "Key");
      vector<string> name = get_xml_data(in_data, "Name");
      vector<string> all_prefix = get_xml_data(in_data, "Prefix");
      vector<string> sizes = get_xml_data(in_data, "Size");
      vector<string> etags = get_xml_data(in_data, "ETag");
      vector<string> owners = get_xml_data(in_data, "ID");
      vector<string> modified = get_xml_data(in_data, "LastModified");
      vector<string> storageClass = get_xml_data(in_data, "StorageClass");
      //vector<string> marker = get_xml_data(in_data, "Marker");
      if(all_keys.size() == 0)
        return -2;
      else if (all_keys.size() >= 1){
          all = all_keys;
      }
      else{
        all = all_prefix;
      }
      int ind = 0;
	  Attrs attrs;
	  //RGWBucketInfo info = this->bucket->get_info();
      for (vector<string>::iterator t=all.begin(); t!=all.end(); ++t)
      {
        rgw_bucket_dir_entry entry;
        rgw_obj_index_key index_key(*t);
        entry.key = index_key;
        entry.exists =true;
        if(all_keys.size() >= 1){
          string s = "&quot;";
          etags[ind].erase (0,s.length());
          etags[ind].erase(etags[ind].length()-s.length(),etags[ind].length());
          entry.meta.etag =  etags[ind];
          entry.meta.owner = owners[ind];
          //ldout(cct, 20) << __func__ << ind << " " << *t << " entry.meta.etag  " << entry.meta.etag << dendl;
          entry.meta.size = stoull(sizes[ind]);
          entry.meta.accounted_size = stoull(sizes[ind]);
          //entry.meta.mtime = stoull(modified[ind]); AMIN: FIXME
          //ldout(cct, 20) << __func__ <<  " entry.meta.size " <<  entry.meta.size << dendl;;
        }
        remote_bucket->push_back(entry);
        remote_bucket_list->push_back(*t);
		if (ind == 0){ //AMIN: FIXME
		  attrs[RGW_ATTR_ETAG] = bufferlist::static_from_string(etags[0]);
		  attrs[RGW_ATTR_STORAGE_CLASS] = bufferlist::static_from_string(storageClass[0]);
		  this->bucket->set_attrs(attrs);
		  //this->bucket->set_attrs(RGW_ATTR_ETAG, bufferlist::static_from_string(etags[0]));
		  //this->bucket->set_attrs(RGW_ATTR_STORAGE_CLASS, bufferlist::static_from_string(storageClass[0]));
		}
		//info.storage_class = StorageClass[ind];
        ind = ind + 1;
      }
	  //this->bucket->set_info(info);
	  /* AMIN
      vector<string> all_marker = get_xml_data(in_data, "NextMarker");
      if (all_marker.size() == 0)
        break;
      string tmp = all_marker.front();
      unsigned first = tmp.find(prefix);
      unsigned last = tmp.find("/", first + prefix.size() + 1);
      marker =  tmp.substr(first,last-first) +"/";
      ldout(cct, 20) << __func__ <<" remote_bucket_size" <<  remote_bucket_list.size() << dendl;
	  */

    return 0;
}



int S3FilterStore::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterStore::initialize(cct, dpp);
  _cct = cct; 
  return 0;
}

std::unique_ptr<User> S3FilterStore::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<S3FilterUser>(std::move(user), this);
}

std::unique_ptr<Object> S3FilterStore::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<S3FilterObject>(std::move(o), this);
}

//TODO
std::unique_ptr<Object> S3FilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<S3FilterObject>(std::move(o), this, filter);
}

// Returns user's S3 Access and Secret Key
/*
int RGWRados::get_s3_credentials(RGWRados *store, string userid, RGWAccessKey& s3_key){
  RGWObjVersionTracker objv_tracker;
  RGWObjectCtx obj_ctx(this->store);
  RGWUserInfo info;
  rgw_user user_id(userid);
  int ret = rgw_get_user_info_by_uid(this->store->ctl()->user, user_id , info, &objv_tracker, NULL, NULL, NULL);
  if (ret < 0)
    return ret;
  map<string, RGWAccessKey>::iterator kiter;
  for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    s3_key.id=k.id;
    s3_key.key = k.key;
  }
  return 0;
}



int RGWRados::copy_remote(RGWRados *store, cache_obj* c_obj){
   ldout(cct, 0) << __func__ << "key" << c_obj->bucket_name + c_obj->obj_name <<dendl;
  // Get S3 Credentials of the user
  RGWAccessKey accesskey;
  int ret = get_s3_credentials(store, c_obj->owner, accesskey);
  RGWBucketInfo src_bucket_info;
  RGWBucketInfo dest_bucket_info;
  RGWObjectCtx obj_ctx(this->store);
  map<string, bufferlist> dest_attrs;
  map<string, bufferlist> src_attrs;
  const string src_tenant_name = "";
  const string src_bucket_name = c_obj->bucket_name;
  const string src_obj_name = c_obj->obj_name;
  string url ="http://" + cct->_conf->backend_url;
  string etag;

  HostStyle host_style = PathStyle;
  ret = get_bucket_info(&svc, src_tenant_name, src_bucket_name, src_bucket_info, NULL, null_yield, &src_attrs);
  rgw_obj src_obj(src_bucket_info.bucket, src_obj_name);
  dest_bucket_info = src_bucket_info;
  dest_attrs = src_attrs;
  rgw_bucket dest_bucket = dest_bucket_info.bucket;
  rgw_obj dest_obj(dest_bucket, src_obj_name); 
  uint64_t obj_size;

  //Create Bucket
  RGWRESTStreamS3PutObj *bucket_wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, NULL, NULL, host_style);
  bucket_wr->set_send_length(0);
  map<string, bufferlist> bucket_attrs;
  rgw_obj dest_bucket_obj(dest_bucket, "");
  ret = bucket_wr->put_obj_init(accesskey, dest_bucket_obj, 0, bucket_attrs, true);
  if (ret < 0) {
    delete bucket_wr;
	return -1;
  }
  
  ret = bucket_wr->complete_request(&etag, nullptr);
  if (ret < 0){
	return -1;
	delete bucket_wr;
  }
  
  //Create Object
  RGWObjState *astate = NULL;
  ret =  get_obj_state(&obj_ctx, src_bucket_info, src_obj, &astate, false, null_yield);
  
  RGWRados::Object src_op_target(store, src_bucket_info, obj_ctx, src_obj);
  RGWRados::Object::Read read_op(&src_op_target);
  read_op.params.attrs = &src_attrs;
  read_op.params.obj_size = &obj_size;
  ret = read_op.prepare(null_yield);
  if (ret < 0)
	 return ret;
  
  RGWRESTConn *conn = svc.zone->get_master_conn();
  RGWRESTStreamS3PutObj *wr;
  rgw_user user_id(c_obj->owner);

  ret = conn->put_obj_async(user_id, dest_obj, astate->size, src_attrs, true, &wr, url, accesskey);
  if (ret < 0) 
    return ret;
  
  ret = read_op.iterate(0, astate->size - 1, wr->get_out_cb(), null_yield);
  if (ret < 0) {
    delete wr;
    return ret;
  }
  ret = conn->complete_request(wr, etag, nullptr);
  if (ret < 0)
    return ret;

  objDirectory->updateField(c_obj, "home_location", "dataLake");
  objDirectory->updateField(c_obj, "intermediate", "false");
  return 0;
}

*/

int S3FilterUser::create_bucket(const DoutPrefixProvider* dpp,
                              const rgw_bucket& b,
                              const std::string& zonegroup_id,
                              rgw_placement_rule& placement_rule,
                              std::string& swift_ver_location,
                              const RGWQuotaInfo * pquota_info,
                              const RGWAccessControlPolicy& policy,
                              Attrs& attrs,
                              RGWBucketInfo& info,
                              obj_version& ep_objv,
                              bool exclusive,
                              bool obj_lock_enabled,
                              bool* existed,
                              req_info& req_info,
                              std::unique_ptr<Bucket>* bucket_out,
                              optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  //User* nu = nextUser(u);
  int ret;

  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket." << dendl;

  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: user ID is: " << this->get_id() << dendl;
  map<std::string, RGWAccessKey> accessKeys =  this->get_info().access_keys;
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: user Access Key is: " << accessKeys[this->get_id().to_str()].id << dendl;
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: user Secret Key is: " << accessKeys[this->get_id().to_str()].key << dendl;
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: user first element id is: " << accessKeys.begin()->first << dendl;
  RGWAccessKey accesskey;
  //RGWAccessKey& k = accessKeys[this->get_id().to_str()];
  //accesskey.id=k.id; FIXME
  //accesskey.key = k.key;
  accesskey.id="test5";
  accesskey.key = "test5";
  
  //strcpy(b.tenant, this->get_id().tenant.c_str());
  /* If it exists, look it up; otherwise create it */
  //FIXME: AMIN: we should first check if the bucket exist or not.
  /* 
  ret = this->filter->get_bucket(dpp, this, b, &nb, y);
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: get_bucket return is: " << ret << " ENOENT is: " << ENOENT << dendl;
  if (ret < 0 && ret != -ENOENT){
    return ret;
  }
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: ret bucket name is: " << nb->get_info().bucket.name << dendl;
  */ 
  //nb = std::make_unique<S3FilterBucket>(this->filter, b, this);
/* UGUR's remote request old version 
		  if (cct->_conf->rgw_datacache_test){
		  //if (true){
			string cache_id = "10.12.1.15:8082";
			//this->getRemoteCacheWeight();
			ldout(cct, 10) << __func__  <<" last copy, no dw, remote copy : " << del_oid <<dendl;
			RemoteRequest *c =  new RemoteRequest();
			c->req_type = 1;
			c->path = del_oid;
			c->dest =  cache_id;
			c->sizeleft = tmp.size_in_bytes;
			tp->addTask(new RemoteS3Request(c, cct));
*/


  ldpp_dout(dpp, 20) << __func__ << " AMIN bucket name: " << b.name << " tenant: " + b.tenant << dendl;
  string url ="http://" + this->filter->_cct->_conf->backend_url;
  ldpp_dout(dpp, 20) << __func__ << " AMIN URL: " << url << dendl;

  HostStyle host_style = PathStyle;
	 
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  RGWRESTStreamS3PutObj *bucket_wr = new RGWRESTStreamS3PutObj(this->filter->_cct, "PUT", url, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  bucket_wr->set_send_length(0);
  map<string, bufferlist> bucket_attrs;
  //const rgw_obj_key key;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  rgw_bucket bucket(b);
  bucket.tenant = (string) this->get_id().tenant;
  //Bucket* fb = new FilterBucket(std::move(nb), this);
  //std::unique_ptr<rgw::sal::Object> dest_bucket_obj = fb->get_object(key);
  //std::unique_ptr<rgw::sal::Object> bucket_obj;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  bucket_wr->put_bucket_init(dpp, accesskey, &bucket, bucket_attrs);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  /*
  if (ret < 0) {
    delete bucket_wr;
	return -1;
  }
  */

  ret = RGWHTTP::send(bucket_wr);
  if (ret < 0) {
    delete bucket_wr;
    return ret;
  }


  string etag; 
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = bucket_wr->complete_request(null_yield);
  //ret = bucket_wr->complete_request();
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  if (ret < 0){
	delete bucket_wr;
	return -1;
  }
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  
  /*
  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, &nb, y);
  ldpp_dout(dpp, 20) << "AMIN S3 Filter: Creating Bucket: create_bucekt return is: " << ret << " ENOENT is: " << ENOENT << dendl;
  if (ret < 0)
    return ret;
  */

  /*
  Bucket* fb = new S3FilterBucket(std::move(nb), this, filter);
  bucket_out->reset(fb);
  */
  return 0;
}

std::unique_ptr<Object::ReadOp> S3FilterObject::get_read_op()
{
  std::unique_ptr<Object::ReadOp> r = next->get_read_op();
  return std::make_unique<S3FilterObject::S3FilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> S3FilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<S3FilterDeleteOp>(std::move(d), this);
}

int S3FilterObject::S3FilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "ofs is: " << ofs << " end is: " << end << dendl;
  /*
  int ret = next->iterate(dpp, ofs, end, cb, y);
  if (ret < 0)
    return ret;
  */
  //cb->handle_data();
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  /* Copy params out of next */
  //params = next->params;
  return 0;
}



int S3FilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket_out, optional_yield y)
{
 
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  rgw_placement_rule placement_rule;
  placement_rule.name = "";
  placement_rule.storage_class = "";
  std::string swift_ver_location = "";
  RGWAccessControlPolicy policy;
  Attrs attrs;
  RGWBucketInfo info;
  obj_version ep_objv;
  bool exclusive = false;
  bool obj_lock_enabled = false;
  bool existed;
  RGWEnv env;
  req_info req_info(this->_cct, &env);

  //ret = next->get_bucket(dpp, nu, b, &nb, y);
  ret = nu->create_bucket(dpp, b, "", placement_rule, swift_ver_location, nullptr, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, &existed, req_info, &nb, y);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " return is: " << ret << dendl;
  /* AMIN: FIXME: what should we do if we have it local?
  if (ret == 0)
    return ret; //we have it local
  if (!existed)
	do something...
  */

  /*
  Bucket* fb = new S3FilterBucket(std::move(nb), u, this);
  bucket_out->reset(fb);
  return 0;
  */
  
  map<std::string, RGWAccessKey> accessKeys =  u->get_info().access_keys;
  RGWAccessKey accesskey;
  //RGWAccessKey& k = accessKeys[u->get_id().to_str()];
  accesskey.id= "test5"; //FIXME
  accesskey.key = "test5";
  ldpp_dout(dpp, 20) << __func__ << " AMIN bucket name: " << b.name << " tenant: " + b.tenant << dendl;
  string url ="http://" + this->_cct->_conf->backend_url;
  ldpp_dout(dpp, 20) << __func__ << " AMIN URL: " << url << dendl;

  HostStyle host_style = PathStyle;
  vector<rgw_bucket_dir_entry> remote_bucket;
  vector<string> remote_bucket_list;

  //rgw_bucket bucket;
  //S3FilterBucket* fb = new S3FilterBucket(nullptr, bucket, u, this);
  //S3FilterBucket* fb = new S3FilterBucket(std::move(nb), bucket, nu, this);

  S3FilterBucket* fb = new S3FilterBucket(std::move(nb), nu, this);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "get_info bucket name is: " <<  fb->get_info().bucket.name << dendl;
  RGWGetBucketCB cb(fb, &remote_bucket_list, &remote_bucket);

  const string tenant_id="";
  list<string> endpoints;
  endpoints.push_back(url);
  
  RGWRESTStreamRWRequest *bucket_rd = new RGWRESTStreamRWRequest(this->_cct, "GET", url, &cb, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  map<string, bufferlist> bucket_attrs;

  const rgw_obj obj_b(b, "");
  map<string, string> extra_headers;

  ret = bucket_rd->send_request(dpp, &accesskey, extra_headers, b.name, nullptr, nullptr);

  //ret = RGWHTTP::send(bucket_rd);
  //ret = bucket_rd->send();
  if (ret < 0) {
    delete bucket_rd;
    return ret;
  }

  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = bucket_rd->complete_request(null_yield);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  if (ret < 0){
	delete bucket_rd;
	return -1;
  }
  
  int count = 0;
  for (auto em: remote_bucket)
  {
	ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "bucket objects are: "<< em.key.name << dendl;
	//bucket.name = em;
	/*
    ret = next->get_bucket(dpp, nu, b, &nb, y);
	ldpp_dout(dpp, 20) << "AMIN S3 Filter: " << __func__ << " return is: " << ret << dendl;
	if (ret != 0)
	  return ret;
	*/
    //Bucket* fb = new S3FilterBucket(std::move(nb), bucket, u, this);
	//ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "bucket_cb attrs are: " << bucket_cb.attrs << dendl;
	//fb->set_attrs(bucket_cb.attrs);
	ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "fb attrs are: " << fb->get_attrs()[RGW_ATTR_ETAG] << dendl;
	//Bucket* fb = new S3FilterBucket(std::move(nb), u, this);
	info.has_instance_obj = true;
	count ++;
	break;
  }
  if (count == 0)
	info.has_instance_obj = false;
  info.bucket = b;
  info.owner = u->get_id();
   

  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "created info: bucket name: " <<  info.bucket.name << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "created info: owner tenant: " <<  info.owner.tenant << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "created info: owner id: " <<  info.owner.id << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "created info: placement name: " <<  info.placement_rule.name << dendl;
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "created info: has object: " <<  info.has_instance_obj << dendl;

  bucket_out->reset(fb);

  return 0;

}

std::unique_ptr<Writer> S3FilterStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  ldpp_dout(dpp, 20) << "AMIN: " << __func__  << dendl;
  std::unique_ptr<Object> no = nextObject(_head_obj.get())->clone();
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "next object name is : " <<  no->get_name() << dendl;
   
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " : owner is : " << owner.tenant << dendl;
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " : owner id is : " << owner.id << dendl;
  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " : store name is : " << next->get_name() << dendl;
 
   
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);
  //ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " : " << "next write name is : " <<  no->get_name() << dendl;

  ldpp_dout(dpp, 20) << "AMIN " << __func__ << " : " << __LINE__ << dendl;
  return std::make_unique<S3FilterWriter>(std::move(writer), this, std::move(_head_obj), dpp, true);
  //return std::make_unique<S3FilterWriter>(nullptr, this, std::move(_head_obj), dpp, true);
  
}

//TODO
//int S3FilterWriter::prepare(optional_yield y, uint64_t obj_size = 0)
int S3FilterWriter::prepare(optional_yield y)
{
  ldpp_dout(this->save_dpp, 20) << "AMIN " << __func__ << " : owner is : " << this->head_obj->get_bucket()->get_owner() << dendl;
  this->user = (rgw::sal::S3FilterUser*) this->head_obj->get_bucket()->get_owner();

  string url ="http://" + this->filter->_cct->_conf->backend_url;
  HostStyle host_style = PathStyle;
  ldpp_dout(this->save_dpp, 20) << __func__ << " AMIN URL: " << url << dendl;
	 
  this->obj_wr = new RGWRESTStreamS3PutObj(this->filter->_cct, "PUT", url, NULL, NULL, "", host_style);
  ldpp_dout(this->save_dpp, 20) << "AMIN " << __func__ << " : tenant is : " << this->user->get_id() << dendl;

  //map<std::string, RGWAccessKey> accessKeys =  this->user->get_info().access_keys;
  RGWAccessKey accesskey;
  accesskey.id="test5";
  accesskey.key = "test5";
  map<string, bufferlist> obj_attrs;
 

  this->obj_wr->put_obj_init(this->save_dpp, accesskey, this->head_obj.get(), obj_attrs);
  //this->obj_wr->set_send_length(this->head_obj->get_obj_size());
  this->obj_wr->set_send_length(9437184);
  //ldpp_dout(this->save_dpp, 20) << __func__ << " len is: " << this->head_obj->get_obj_size() << dendl;
  
  int ret = RGWHTTP::send(obj_wr);
  if (ret < 0) {
    delete obj_wr;
    return ret;
  }
  

  return 0;
}

int S3FilterWriter::process(bufferlist&& data, uint64_t offset)
{
  int ret = 0; 
  bufferlist objectData = data;

  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << "length is: " << objectData.length() << ", ofs is: " << offset << dendl;

  if (objectData.length() == 0)
	return 0;
  //std::unique_ptr<rgw::sal::Object::ReadOp> read_op(this->head_obj->get_read_op());

  ldpp_dout(this->save_dpp, 20) << __func__ << " AMIN data is: " << objectData << dendl;
  
  //this->obj_wr->set_send_length(head_obj->get_obj_size());
  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  /*  
  ret = RGWHTTP::send(obj_wr);
  if (ret < 0) {
    delete obj_wr;
    return ret;
  }
  */
  

  //ret = this->obj_wr->get_out_cb()->handle_data(objectData, offset, objectData.length());
  ret = this->obj_wr->get_out_cb()->handle_data(objectData, 0, objectData.length());
  //ret = read_op->iterate(this->save_dpp, 0, objectData.length()-1, obj_wr->get_out_cb(), null_yield);
  //ret = read_op->iterate(this->save_dpp, 0, astate->size - 1, out_stream_req->get_out_cb(), null_yield);
  //FIXME: implement iterate. use RGWEADOS::Object::Read::read_op in rgw_rados.cc
  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  //return next->process(std::move(data), offset);
  
  return 0;

}

int S3FilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  int ret = 0;
/*
  ret = RGWHTTP::send(this->obj_wr);
  if (ret < 0) {
    delete obj_wr;
    return ret;
  }
*/
  this->obj_wr->set_send_length(accounted_size);
  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = this->obj_wr->complete_request(null_yield);
  if (ret < 0){
	delete obj_wr;
	return -1;
  }
  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  return 0; //FIXME
}



int S3FilterStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(nu, i, &nb);
  if (ret != 0)
    return ret;

  Bucket* fb = new S3FilterBucket(std::move(nb), u, this);
  bucket->reset(fb);
  return 0;
}

int S3FilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu = nextUser(u);

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new S3FilterBucket(std::move(nb), u, this);
  bucket->reset(fb);
  return 0;
}


//TODO
int S3FilterObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state,
			    optional_yield y, bool follow_olh){
  return 0;
}


/*

int S3FilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) 
{
  if (setattrs != NULL) {
    if (delattrs != NULL) {
      for (const auto& attr : *delattrs) {
        if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
    }

    int updateAttrsReturn = filter->get_d4n_cache()->setObject(this->get_name(), &(this->get_attrs()), setattrs);

    if (updateAttrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache set object attributes operation succeeded." << dendl;
    }
  }

  if (delattrs != NULL) {
    std::vector<std::string> delFields;
    Attrs::iterator attrs;

    // Extract fields from delattrs
    for (attrs = delattrs->begin(); attrs != delattrs->end(); ++attrs) {
      delFields.push_back(attrs->first);
    }

    Attrs currentattrs = this->get_attrs();
    std::vector<std::string> currentFields;
    
    // Extract fields from current attrs 
    for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
      currentFields.push_back(attrs->first);
    }
    
    int delAttrsReturn = filter->get_d4n_cache()->delAttrs(this->get_name(), currentFields, delFields);

    if (delAttrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation failed." << dendl;
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attributes operation succeeded." << dendl;
    }
  }

  return next->set_obj_attrs(dpp, setattrs, delattrs, y);  
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  rgw::sal::Attrs newAttrs;
  int getAttrsReturn = filter->get_d4n_cache()->getObject(this->get_name(), &(this->get_attrs()), &newAttrs);

  if (getAttrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

    return next->get_obj_attrs(y, dpp, target_obj);
  } else {
    int setAttrsReturn = this->set_attrs(newAttrs);
    
    if (setAttrsReturn < 0) {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation failed." << dendl;

      return next->get_obj_attrs(y, dpp, target_obj);
    } else {
      ldpp_dout(dpp, 20) << "D4N Filter: Cache get object attributes operation succeeded." << dendl;
  
      return 0;
    }
  }
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp) 
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  int updateAttrsReturn = filter->get_d4n_cache()->setObject(this->get_name(), &(this->get_attrs()), &update);

  if (updateAttrsReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache modify object attribute operation succeeded." << dendl;
  }

  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);  
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) 
{
  std::vector<std::string> delFields;
  delFields.push_back((std::string)attr_name);
  
  Attrs::iterator attrs;
  Attrs currentattrs = this->get_attrs();
  std::vector<std::string> currentFields;
  
  // Extract fields from current attrs 
  for (attrs = currentattrs.begin(); attrs != currentattrs.end(); ++attrs) {
    currentFields.push_back(attrs->first);
  }
  
  int delAttrReturn = filter->get_d4n_cache()->delAttrs(this->get_name(), currentFields, delFields);

  if (delAttrReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete object attribute operation succeeded." << dendl;
  }
  
  return next->delete_obj_attrs(dpp, attr_name, y);  
}


int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int getDirReturn = source->filter->get_block_dir()->getValue(source->filter->get_cache_block());

  if (getDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory get operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory get operation succeeded." << dendl;
  }

  rgw::sal::Attrs newAttrs;
  source->filter->get_d4n_cache()->getObject(source->get_name(), &(source->get_attrs()), &newAttrs);
  int getObjReturn = source->set_attrs(newAttrs);

  if (getObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache get operation succeeded." << dendl;
  }
  
  return next->prepare(y, dpp);
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  int delDirReturn = source->filter->get_block_dir()->delValue(source->filter->get_cache_block());

  if (delDirReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Directory delete operation succeeded." << dendl;
  }

  int delObjReturn = source->filter->get_d4n_cache()->delObject(source->get_name());

  if (delObjReturn < 0) {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation failed." << dendl;
  } else {
    ldpp_dout(dpp, 20) << "D4N Filter: Cache delete operation succeeded." << dendl;
  }

  return next->delete_obj(dpp, y);
}

*/
} } // namespace rgw::sal

extern "C" {

rgw::sal::Store* newS3Filter(rgw::sal::Store* next)
{
  rgw::sal::S3FilterStore* store = new rgw::sal::S3FilterStore(next);

  return store;
}

}
