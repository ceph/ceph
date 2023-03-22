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
#include <string>

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

void getAccessSecretKeys(RGWAccessKey* accesskey, User* user)
{
  map<std::string, RGWAccessKey> accessKeys =  user->get_info().access_keys;

  accesskey->id = accessKeys.begin()->second.id;
  accesskey->key = accessKeys.begin()->second.key;
}


int RGWGetBucketCB::handle_data(bufferlist& bl, bool *pause){
      string in_data = bl.c_str();
      vector<string> name = get_xml_data(in_data, "Name");
      Attrs attrs;
     
      vector<string> all_keys = get_xml_data(in_data, "Key");
      vector<string> etags = get_xml_data(in_data, "ETag");
      vector<string> storageClass = get_xml_data(in_data, "StorageClass");
      
      if(name.size() == 0)
        return -ENOENT;
      
      else if (all_keys.size() >= 1){
          string s = "&quot;";
          etags[0].erase (0,s.length());
          etags[0].erase(etags[0].length()-s.length(),etags[0].length());
    	  attrs[RGW_ATTR_ETAG] = bufferlist::static_from_string(etags[0]);
	  attrs[RGW_ATTR_STORAGE_CLASS] = bufferlist::static_from_string(storageClass[0]);
	  this->attrs = attrs;
      }

    return 0;
}

int RGWListBucketCB::handle_data(bufferlist& bl, bool *pause){
      string in_data = bl.c_str();
      vector<string> name = get_xml_data(in_data, "Name");
      vector<string> truncated = get_xml_data(in_data, "IsTruncated");
      vector<string> keys = get_xml_data(in_data, "Key");
      vector<string> prefixs = get_xml_data(in_data, "Prefix");
      vector<string> sizes = get_xml_data(in_data, "Size");
      vector<string> etags = get_xml_data(in_data, "ETag");
      vector<string> owners = get_xml_data(in_data, "ID");
      vector<string> modified = get_xml_data(in_data, "LastModified");
      vector<string> storageClass = get_xml_data(in_data, "StorageClass");
      if(name.size() == 0)
        return -ENOENT;

      if (truncated[0] == "true")
      	this->is_truncated = true;
      else
      	this->is_truncated = false;
      int ind = 0;
      //Attrs attrs;
      for (vector<string>::iterator t=keys.begin(); t!=keys.end(); ++t)
      {
        rgw_bucket_dir_entry entry;
        rgw_obj_index_key index_key(*t);
        entry.key = index_key;
        entry.exists =true;

          string s = "&quot;";
          etags[ind].erase (0,s.length());
          etags[ind].erase(etags[ind].length()-s.length(),etags[ind].length());
          entry.meta.etag =  etags[ind];
          entry.meta.owner = owners[ind];
          entry.meta.size = stoull(sizes[ind]);
          entry.meta.accounted_size = stoull(sizes[ind]);
          //entry.meta.mtime = stoull(modified[ind]);

        this->remote_bucket_obj_details->push_back(entry);
        this->remote_bucket_obj_list->push_back(*t);
		//info.storage_class = StorageClass[ind];
        ind = ind + 1;
      }
    return 0;
}



int RGWGetObjectCB::handle_data(bufferlist& bl, bool *pause)
{
      this->rc_bl->append(bl);
      string in_data = bl.c_str();
      vector<string> name = get_xml_data(in_data, "Name");
      vector<string> keys = get_xml_data(in_data, "Key");
      vector<string> prefixs = get_xml_data(in_data, "Prefix");
      vector<string> sizes = get_xml_data(in_data, "Size");
      vector<string> etags = get_xml_data(in_data, "ETag");
      vector<string> owners = get_xml_data(in_data, "ID");
      vector<string> modified = get_xml_data(in_data, "LastModified");
      vector<string> storageClass = get_xml_data(in_data, "StorageClass");
      if(name.size() == 0)
        return -ENOENT;

      int ind = 0;
      for (vector<string>::iterator t=keys.begin(); t!=keys.end(); ++t)
      {
	if (this->object->get_name() == *t){
          string s = "&quot;";
          etags[ind].erase (0,s.length());
          etags[ind].erase(etags[ind].length()-s.length(),etags[ind].length());
    	  attrs[RGW_ATTR_ETAG] = bufferlist::static_from_string(etags[0]);
	  break;
	}
        ind = ind + 1;
      }
    return 0;
}

int RGWDelObjectCB::handle_data(bufferlist& bl, bool *pause){

    string in_data = bl.c_str();
    return 0;
}

int S3FilterStore::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterStore::initialize(cct, dpp);
  _cct = cct;
  _dpp = dpp; 
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

std::unique_ptr<Object> S3FilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<S3FilterObject>(std::move(o), this, filter);
}

int S3FilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		       ListResults& results, optional_yield y)
{
	
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  int ret;
  //std::unique_ptr<Bucket> bucket_out;
  vector<string> remote_bucket_obj_list;
  vector<rgw_bucket_dir_entry> remote_bucket_obj_details;
  bool is_truncated = false;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  RGWListBucketCB cb(this, &remote_bucket_obj_list, &remote_bucket_obj_details, is_truncated);
  
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " owner is: " << this->get_owner()->get_id() << dendl;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " bucket is: " << get_key() << dendl;
  ret = this->filter->send_get_bucket(dpp, this->get_owner(), get_key(), y, &cb);
  if (ret < 0)
	  return ret;

  results.objs = remote_bucket_obj_details;
  results.is_truncated = is_truncated;
  return 0;
}




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
  int ret;

  ldpp_dout(dpp, 20) << __func__ << " AMIN bucket name: " << b.name << " tenant: " + b.tenant << dendl;
  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, this);
  
  string url ="https://" + this->filter->_cct->_conf->backend_url;
  ldpp_dout(dpp, 20) << __func__ << " AMIN URL: " << url << dendl;

  HostStyle host_style = PathStyle;
	 
  RGWRESTStreamS3PutObj *bucket_wr = new RGWRESTStreamS3PutObj(this->filter->_cct, "PUT", url, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  bucket_wr->set_send_length(0);
  map<string, bufferlist> bucket_attrs;
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  rgw_bucket bucket(b);
  bucket.tenant = (string) this->get_id().tenant;
  bucket_wr->put_bucket_init(dpp, accesskey, &bucket, bucket_attrs);
  
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  ret = RGWHTTP::send(bucket_wr);
  if (ret < 0) {
    delete bucket_wr;
    return ret;
  }


  string etag; 
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = bucket_wr->complete_request(null_yield);
  if (ret < 0){
	delete bucket_wr;
	return ret;
  }
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  
  return 0;
}

int S3FilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket_out, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  
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
  

  ret = nu->create_bucket(dpp, b, "", placement_rule, swift_ver_location, nullptr, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, &existed, req_info, &nb, y);
  ldpp_dout(dpp, 20) << "AMIN: " << __func__ << " return is: " << ret << dendl;
 

  S3FilterBucket* fb = new S3FilterBucket(std::move(nb), nu, this);

  RGWGetBucketCB cb(attrs);
  ret = send_get_bucket(dpp, u, b, y, &cb);
  if (ret < 0)
	  return ret;
  info.bucket = b;
  info.owner = u->get_id();
  
  bucket_out->reset(fb);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  return 0;
}

int S3FilterStore::send_get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, optional_yield y, RGWHTTPStreamRWRequest::ReceiveCB *cb)
{
  int ret = 0; 
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
 
  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, u);

  string url ="https://" + this->_cct->_conf->backend_url;

  HostStyle host_style = PathStyle;
  
  list<string> endpoints;
  endpoints.push_back(url);
  
  RGWRESTStreamRWRequest *bucket_rd = new RGWRESTStreamRWRequest(this->_cct, "GET", url, cb, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  map<string, bufferlist> bucket_attrs;

  map<string, string> extra_headers;

  ret = bucket_rd->send_request(dpp, &accesskey, extra_headers, b.name, nullptr, nullptr);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " ret is: " << ret << dendl;
  if (ret < 0) {
    delete bucket_rd;
    return ret;
  }

  ret = bucket_rd->complete_request(null_yield);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " ret is: " << ret << dendl;
  if (ret < 0){
	delete bucket_rd;
	return ret;
  }
  
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  return 0;
}

//This is used to send data to remote over HTTP client
std::unique_ptr<Writer> S3FilterStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Object> no = nextObject(_head_obj.get())->clone();
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<S3FilterWriter>(std::move(writer), this, std::move(_head_obj), dpp, true);
  
}



int S3FilterWriter::prepare(optional_yield y)
{
  ldpp_dout(this->save_dpp, 20) << "AMIN " << __func__ << " : owner is : " << this->head_obj->get_bucket()->get_owner() << dendl;
  this->user = (rgw::sal::S3FilterUser*) this->head_obj->get_bucket()->get_owner();

  string url ="https://" + this->filter->_cct->_conf->backend_url;
  HostStyle host_style = PathStyle;
	 
  this->obj_wr = new RGWRESTStreamS3PutObj(this->filter->_cct, "PUT", url, NULL, NULL, "", host_style);

  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, this->head_obj->get_bucket()->get_owner());

  map<string, bufferlist> obj_attrs;
 

  this->obj_wr->put_obj_init(this->save_dpp, accesskey, this->head_obj.get(), obj_attrs);

  return 0;
}

int S3FilterWriter::process(bufferlist&& data, uint64_t offset)
{
  int ret = 0; 
  bufferlist objectData = data;
  if (objectData.length() != 0){
    send_data.claim_append(objectData);
	ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << "length is: " << send_data.length() << ", ofs is: " << offset << dendl;
  }
  else{
	this->obj_wr->set_send_length(send_data.length());
	ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << "length is: " << send_data.length() << dendl;

    ret = RGWHTTP::send(obj_wr);
	if (ret < 0) {
	  delete obj_wr;
      return ret;
	}
  
	ret = this->obj_wr->get_out_cb()->handle_data(send_data, 0, send_data.length());
    ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  }
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
  ldpp_dout(this->save_dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  int ret = 0;
  this->obj_wr->set_send_length(accounted_size);
  ret = this->obj_wr->complete_request(null_yield);
  if (ret < 0){
	delete obj_wr;
	return ret;
  }

  return 0;
}

std::unique_ptr<Object::ReadOp> S3FilterObject::get_read_op()
{
  std::unique_ptr<Object::ReadOp> r = next->get_read_op();
  return std::make_unique<S3FilterObject::S3FilterReadOp>(std::move(r), this);
}

int S3FilterObject::S3FilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{ 
  map<string, bufferlist>::iterator attr_iter;
  Attrs b_attr = source->get_bucket()->get_attrs();

  attr_iter = b_attr.find(name);
  if (attr_iter != b_attr.end()) {
	ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " attr is: " << name << " its value is: " << attr_iter->second << dendl;
	dest.append(attr_iter->second);
    return 0;
  }
  else{
	ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " NO DATA! "<< dendl;
	return -ENODATA;
  }

  return 0;
}

const Attrs& S3FilterObject::get_attrs() const
{ 
  return this->get_bucket()->get_attrs();
}

Attrs& S3FilterObject::get_attrs()
{ 
  return this->get_bucket()->get_attrs();
}

int S3FilterObject::S3FilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  User* u = source->get_bucket()->get_owner();

  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, u);

  string url ="https://" + source->filter->_cct->_conf->backend_url;

  HostStyle host_style = PathStyle;

  Attrs object_attrs;
  RGWGetObjectCB cb(this->source, object_attrs, &(this->received_data));


  list<string> endpoints;
  endpoints.push_back(url);
  
  ord = new RGWRESTStreamRWRequest(source->filter->_cct, "GET", url, &cb, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  map<string, string> extra_headers;
  int ret = 0;
  ret = ord->send_request(dpp, accesskey, extra_headers, source->get_obj(), nullptr);
  if (ret < 0) {
    delete ord;
    return ret;
  }

  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = ord->complete_request(null_yield);
  if (ret < 0){
	delete ord;
	return ret;
  }
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << " received data size is: "<< received_data.length() << dendl;
  source->set_obj_size(received_data.length());

  return 0;
}

int S3FilterObject::S3FilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << "ofs is: " << ofs << " end is: " << end << dendl;
  /*
  int ret;
  uint64_t len;
  uint64_t chunk_size = source->get_filter()->_cct->_conf->rgw_max_chunk_size;
  
  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  while (ofs <= end) {
	uint64_t read_len = std::min(len, chunk_size);

	ret = cb->handle_data(received_data, ofs, read_len);
    if (ret < 0) {
	  return ret;
    }

    len -= read_len;
    ofs += read_len;
  }
  */
  cb->handle_data(received_data, ofs, end - ofs + 1);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  return 0;
}

std::unique_ptr<Object::DeleteOp> S3FilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<S3FilterDeleteOp>(std::move(d), this);
}

int S3FilterObject::S3FilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  User* u = source->get_bucket()->get_owner();
  RGWAccessKey accesskey;
  getAccessSecretKeys(&accesskey, u);

  string url ="https://" + source->filter->_cct->_conf->backend_url; //FIXME: AMIN : We need to have the flexbility of choosing backend

  HostStyle host_style = PathStyle;

  Attrs object_attrs;
  RGWDelObjectCB cb(source);


  list<string> endpoints;
  endpoints.push_back(url);
  
  RGWRESTStreamRWRequest *odl = new RGWRESTStreamRWRequest(source->filter->_cct, "DELETE", url, &cb, NULL, NULL, "", host_style);
  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;

  map<string, string> extra_headers;
  int ret = 0;
  ret = odl->send_request(dpp, accesskey, extra_headers, source->get_obj(), nullptr);
  if (ret < 0) {
    delete odl;
    return ret;
  }

  ldpp_dout(dpp, 20) << " AMIN: " << __func__ << " : " << __LINE__ << dendl;
  ret = odl->complete_request(null_yield);
  if (ret < 0){
	delete odl;
	return ret;
  }

  return 0;
}


int S3FilterStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  return this->get_bucket(this->_dpp, u, i.bucket,
				bucket, null_yield);
}

int S3FilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  return this->get_bucket(dpp, u, rgw_bucket(tenant,
					name, ""),
				bucket, y);
}


int S3FilterObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state,
			    optional_yield y, bool follow_olh){
  RGWObjState *astate = new RGWObjState;
  *state = astate;
  return 0;
}

/*
int S3FilterUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &buckets, optional_yield y)
{
  BucketList bl;
  int ret;

  buckets.clear();
  ret = this->filter->get_bucket(dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket_out, optional_yield y, GWHTTPStreamRWRequest::ReceiveCB cb)
  if (ret < 0)
    return ret;

  buckets.set_truncated(bl.is_truncated());
  for (auto& ent : bl.get_buckets()) {
    buckets.add(std::make_unique<S3FilterBucket>(std::move(ent.second), this));
  }

  return 0;

}
*/


/*
//This is the basic writer to send data to the next
std::unique_ptr<S3InternalFilterWriter> S3FilterStore::get_s3_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  S3FilterObject * _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Object> no = nextObject(_head_obj)->clone();
  std::unique_ptr<Object> co = _head_obj->clone();
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<S3InternalFilterWriter>(std::move(writer), this, std::move(co), dpp);
  
}
*/

/*
int S3InternalFilterWriter::process(bufferlist&& data, uint64_t offset)
{
  return next->process(std::move(data), offset);
}

int S3InternalFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, y);
}
*/

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


*/
} } // namespace rgw::sal

extern "C" {

rgw::sal::Store* newS3Filter(rgw::sal::Store* next)
{
  rgw::sal::S3FilterStore* store = new rgw::sal::S3FilterStore(next);

  return store;
}

}
