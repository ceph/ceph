// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "include/page.h"
#include <sstream>
#include <iostream>

#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_remoted4n.h"
#include "rgw_client_io.h"
#include "rgw_mdlog_types.h"
#include "driver/d4n/rgw_sal_d4n.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "rgw/rgw_b64.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static inline std::vector<std::string> get_remoted4n_objectInfo(req_state *s, char delim) {
  std::vector<std::string> result;
  std::string item;

  string key;
  if (!rgw::sal::Object::empty(s->object.get())){
    key = s->object->get_name();
    ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": key is: " << key << dendl;
    std::stringstream ss(key);
    while (getline(ss, item, delim)) {
        result.push_back(item);
    }
  }
  return result;
}

bool objectIsDirty(string key) {
  if (key.rfind("D", 0) == 0)
    return true;
  else
    return false;
}

int RGWOp_RemoteD4N_Get::send_response_data_error(optional_yield y)
{
  bufferlist bl;
  return send_response_data(bl, 0 , 0);
}

int RGWOp_RemoteD4N_Get::send_response_data(bufferlist& bl, off_t bl_ofs,
					      off_t bl_len)
{
  int r = dump_body(s, bl.c_str() + bl_ofs, bl_len);
  if (r < 0)
    return r;
  return 0;
}



void RGWOp_RemoteD4N_Get::execute(optional_yield y) {
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": " << dendl;
  string bucketName;
  string objectName;
  uint64_t offset;
  uint64_t len;
  std::string version;
  string oid_in_cache;
  string key; 
  bufferlist bl;
 
  //all the info  including offset, ... should come from remote cache as the key: bucket_version_object_ofs_length 
  std::vector<std::string> v = get_remoted4n_objectInfo(s, '_');
  if (v.empty()){
    ldpp_dout(s, 5) << __func__ << ": " << __LINE__ <<  ": Remote Object name is not in right format!" << dendl;
    op_ret = -1;
    return;
  }

  int j = 0;
  for (auto i : v){ 
    ldpp_dout(s, 20) << __func__ << ": " << __LINE__ <<  "v["  << j << "]: " << i << dendl;
    j++;
  }

  bool dirty = false;
  if (v[0] == string("D")){
    dirty = true;
  }

  char* end;
  if (dirty == true){
    if (v.size() == 5){
      bucketName = v[1];
      objectName = v[2];     
      offset = strtoull(v[3].c_str(), &end,10);
      len = strtoull(v[4].c_str(), &end,10);
      oid_in_cache = bucketName + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);
    }
    else if (v.size() == 6){
      bucketName = v[1];
      version = v[2];
      objectName = v[3];     
      offset = strtoull(v[4].c_str(), &end,10);
      len = strtoull(v[5].c_str(), &end,10);
      oid_in_cache = bucketName + "_" + version + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);
    }
    else{
      ldpp_dout(s, 5) << __func__ << ": " << __LINE__ <<  ": Remote Object name is not in right format!" << dendl;
      op_ret = -1;
      return;
    }
  }
  else{ // dirty == flase
    if (v.size() == 4){
      bucketName = v[0];
      objectName = v[1];     
      offset = strtoull(v[2].c_str(), &end,10);
      len = strtoull(v[3].c_str(), &end,10);
      oid_in_cache = bucketName + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);
    }
    else if (v.size() == 5){
      bucketName = v[0];     
      version = v[1];
      objectName = v[2];     
      offset = strtoull(v[3].c_str(), &end,10);
      len = strtoull(v[4].c_str(), &end,10);
      oid_in_cache = bucketName + "_" + version + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);
    }
    else{
      ldpp_dout(s, 5) << __func__ << ": " << __LINE__ <<  ": Remote Object name is not in right format!" << dendl;
      op_ret = -1;
      return;
    }
  }

  if (dirty)
    //RD_bucket_version_key_...
    key = string("RD_") + oid_in_cache;
  else 
    key = oid_in_cache;


  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": key is: " << key << dendl;

  if (!rgw::sal::Object::empty(s->object.get()))
      attrs = s->object->get_attrs();

  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->get(s, key, offset, len , bl, attrs, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't get key: " << cpp_strerror(op_ret) << dendl;
    return;
  }

  op_ret = send_response_data(bl, offset, len);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't send seponse data: " << cpp_strerror(op_ret) << dendl;
    return;
  }
  op_ret = 0;
}

int RGWOp_RemoteD4N_Put::get_data(bufferlist& bl) {
  
  size_t cl = 0;
  int read_len;
  char *data;
  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       return -ENOMEM;
    }
    read_len = recv_body(s, data, cl);
    if (cl != (size_t)read_len) {
      ldpp_dout(this, 10) << "recv_body incomplete" << dendl;
    }
    if (read_len < 0) {
      free(data);
      return read_len;
    }
    bl.append(data, read_len);
  } else {
    int chunk_size = CEPH_PAGE_SIZE;
    const char *enc = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!enc || strcmp(enc, "chunked")) {
      return -ERR_LENGTH_REQUIRED;
    }
    data = (char *)malloc(chunk_size);
    if (!data) {
      return -ENOMEM;
    }
    do {
      read_len = recv_body(s, data, chunk_size);
      if (read_len < 0) {
	free(data);
	return read_len;
      }
      bl.append(data, read_len);
    } while (read_len == chunk_size);
  }
  free(data);
  return 0;
}

void RGWOp_RemoteD4N_Put::execute(optional_yield y) {
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << dendl;
  bufferlist bl;
  rgw::sal::Attrs attrs;
  string bucketName;
  string objectName;
  uint64_t offset;
  uint64_t len;
  std::string version;
  string oid_in_cache;
  string key;
 
  //all the info  including offset, ... should come from remote cache as the key: bucket_version_object_ofs_length 
  std::vector<std::string> v = get_remoted4n_objectInfo(s, '_');
  if (v.empty()){
    ldpp_dout(s, 5) << __func__ << ": Remote Object name is not in right format!" << dendl;
    op_ret = -1;
    return;
  }

  bool dirty = false;
  if (v[0] == string("D")){
    dirty = true;
  }

  char* end;
  if (dirty == true){
    if (v.size() == 5){
      bucketName = v[1];
      objectName = v[2];     
      offset = strtoull(v[3].c_str(), &end,10);
      len = strtoull(v[4].c_str(), &end,10);
    }
    else if (v.size() == 6){
      bucketName = v[1];
      version = v[2];
      objectName = v[3];     
      offset = strtoull(v[4].c_str(), &end,10);
      len = strtoull(v[5].c_str(), &end,10);
    }
    else{
      ldpp_dout(s, 5) << __func__ << ": Remote Object name is not in right format!" << dendl;
      op_ret = -1;
      return;
    }
  }
  else{ // dirty == flase
    if (v.size() == 4){
      bucketName = v[0];
      objectName = v[1];     
      offset = strtoull(v[2].c_str(), &end,10);
      len = strtoull(v[3].c_str(), &end,10);
    }
    else if (v.size() == 5){
      bucketName = v[0];     
      version = v[1];
      objectName = v[2];     
      offset = strtoull(v[3].c_str(), &end,10);
      len = strtoull(v[4].c_str(), &end,10);
    }
    else{
      ldpp_dout(s, 5) << __func__ << ": Remote Object name is not in right format!" << dendl;
      op_ret = -1;
      return;
    }
  }
  if (version.length() > 0)
    oid_in_cache = bucketName + "_" + version + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);
  else 
    oid_in_cache = bucketName + "_" + objectName + "_" + to_string(offset) + "_" + to_string(len);

  if (dirty)
    //RD_bucket_version_key_...
    key = string("RD_") + oid_in_cache;
  else
    key = oid_in_cache;

  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": key is: " << key << dendl;

  if (!rgw::sal::Object::empty(s->object.get()))
      attrs = s->object->get_attrs();

  op_ret = get_data(bl);
  if (op_ret < 0) {
    return;
  }

  ldpp_dout(s, 20) << "AMIN: " << __func__ << __LINE__ << ": data is: " << bl.to_str() << dendl;  

  if (bl.length() != len){
    ldpp_dout(s, 5) << __func__ << ": Data length is not right!" << dendl;
    op_ret = -1;
    return;
  }

  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->put(s, key, bl, bl.length(), attrs, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't write object into the cache: " << cpp_strerror(op_ret) << dendl;
    return;
  }

  time_t creationTime = time(NULL);

  //FIXME: AMIN: this is only for test, remove it
  rgw_user user;
  user.tenant = "AMIN_TEST";
  static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_policy_driver()->get_cache_policy()->update(s, oid_in_cache, offset, len, version, dirty, creationTime, user, y);
  //op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_policy_driver()->get_cache_policy()->update(s, oid_in_cache, offset, len, version, dirty, creationTime,  s->object->get_bucket()->get_owner(), y);

  rgw::d4n::CacheBlock block;
  block.cacheObj.objName = objectName;
  block.cacheObj.bucketName = bucketName;
  block.blockID = offset;
  block.size = len;

  ldpp_dout(s, 5) << __func__ << "(): Updating directory entry for " << block.cacheObj.objName << dendl;
  ldpp_dout(s, 5) << __func__ << "(): Adding " << s->get_cct()->_conf->rgw_local_cache_address << " to the hosts list" << dendl;
  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_block_dir()->update_field(&block, "blockHosts", s->get_cct()->_conf->rgw_local_cache_address, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't update directory entry: " << cpp_strerror(op_ret) << dendl;
    return;
  }


  // translate internal codes into return header
  if (op_ret == 0)
    update_status = "created";
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << dendl;
}

void RGWOp_RemoteD4N_Put::send_response() {
  int op_return_code = op_ret;
  if ((op_ret == STATUS_NO_APPLY) || (op_ret == STATUS_APPLIED))
    op_return_code = STATUS_NO_CONTENT;
  set_req_state_err(s, op_return_code);
  dump_errno(s);
  stringstream ver_stream;
  ver_stream << "ver:" << ondisk_version.ver
	     <<",tag:" << ondisk_version.tag;
  dump_header_if_nonempty(s, "RGWX_UPDATE_STATUS", update_status);
  dump_header_if_nonempty(s, "RGWX_UPDATE_VERSION", ver_stream.str());
  end_header(s);
}

void RGWOp_RemoteD4N_Delete::execute(optional_yield y) {
  string key;
  std::vector<std::string> v = get_remoted4n_objectInfo(s, '_');
  if (v.empty()){
    ldpp_dout(s, 5) << __func__ << ": Remote Object name is not in right format!" << dendl;
    op_ret = -1;
    return;
  }

  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->del(s, key, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't remove key: " << cpp_strerror(op_ret) << dendl;
    return;
  }
}

RGWOp *RGWHandler_RemoteD4N::op_get() {
  return new RGWOp_RemoteD4N_Get;
}

RGWOp *RGWHandler_RemoteD4N::op_put() {
  return new RGWOp_RemoteD4N_Put;
}

RGWOp *RGWHandler_RemoteD4N::op_delete() {
  return new RGWOp_RemoteD4N_Delete;
}

RGWHandler_REST* RGWRESTMgr_RemoteD4N::get_handler(rgw::sal::Driver* driver,
			     req_state* const s,
			     const rgw::auth::StrategyRegistry& auth_registry,
			     const std::string& frontend_prefix) {
  return new RGWHandler_RemoteD4N(auth_registry);
}
