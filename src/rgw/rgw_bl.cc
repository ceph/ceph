// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string.h>
#include <iostream>
#include <map>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#include <common/errno.h>
#include "auth/Crypto.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_bl.h"
#include "rgw_rados.h"
#include "common/Clock.h"
#include "rgw_rest_client.h"
#include "rgw_user.h"
#include "rgw_acl_s3.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

const char* BL_STATUS[] = {
  "UNINITIAL",
  "PROCESSING",
  "FAILED",
  "PERM_ERROR",
  "ACL_ERROR",
  "COMPLETE"
};

std::map<const string, const uint32_t> rgw_perm_map = boost::assign::map_list_of
      ("READ", RGW_PERM_READ)
      ("WRITE", RGW_PERM_WRITE)
      ("READ_ACP", RGW_PERM_READ_ACP)
      ("WRITE_ACP", RGW_PERM_WRITE_ACP)
      ("FULL_CONTROL", RGW_PERM_FULL_CONTROL);

void *RGWBL::BLWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    if (should_work(start)) {
      dout(5) << "bucket logging deliver: start" << dendl;
      int r = bl->process();
      if (r < 0) {
        dout(0) << "ERROR: bucket logging process() err=" << r << dendl;
      }
      dout(5) << "bucket logging deliver: stop" << dendl;
    }
    if (bl->going_down())
      break;

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(end);
    time_t next_time = end + secs;
    char buf[30];
    char *nt = ctime_r(&next_time, buf);
    dout(5) << "schedule bucket logging deliver next start time: "
            << nt <<dendl;

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!bl->going_down());

  return nullptr;
}

void RGWBL::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;
  max_objs = cct->_conf->rgw_bl_max_objs;
  if (max_objs > BL_HASH_PRIME)
    max_objs = BL_HASH_PRIME;

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = bl_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf); // bl.X
  }

#define BL_COOKIE_LEN 16
  char cookie_buf[BL_COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

bool RGWBL::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now();
  localtime_r(&start_date, &bdt);

  bdt.tm_hour = 0;
  bdt.tm_min = 0;
  bdt.tm_sec = 0;
  begin_of_day = mktime(&bdt);

  if (cct->_conf->rgw_bl_debug_interval > 0) {
    if ((now - begin_of_day) < cct->_conf->rgw_bl_debug_interval)
      return true;
    else
      return false;
  }

  if (now - begin_of_day < 24*60*60)
    return true;
  else
    return false;
}

void RGWBL::finalize()
{
  delete[] obj_names;
}

int RGWBL::bucket_bl_prepare(int index)
{
  map<string, int > entries;

  string marker;

#define MAX_BL_LIST_ENTRIES 100
  do {
    int ret = cls_rgw_bl_list(store->bl_pool_ctx,
                              obj_names[index], marker,
                              MAX_BL_LIST_ENTRIES, entries);
    if (ret < 0)
      return ret;
    for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
      pair<string, int> entry(iter->first, bl_uninitial);
      ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index], entry);
      if (ret < 0) {
        dout(0) << "RGWBL::bucket_bl_prepare() failed to set entry "
                << obj_names[index] << dendl;
        break;
      }
      marker = iter->first;
    }
  } while (!entries.empty());

  return 0;
}

static string render_target_key(CephContext *cct, const string prefix)
{
  string target_key;

  char unique_string_buf[BL_UNIQUE_STRING_LEN + 1];
  int ret = gen_rand_alphanumeric_plain(g_ceph_context, unique_string_buf,
					sizeof(unique_string_buf));
  if (ret < 0) {
      return target_key;
  } else {
    string unique_str = string(unique_string_buf);
    utime_t now = ceph_clock_now();
    struct tm current_time;
    time_t tt = now.sec();
    localtime_r(&tt, &current_time);
    char buffer[20];
    strftime(buffer, 20, "%Y-%m-%d-%H-%M-%S", &current_time);
    std::string time(buffer);

    target_key += prefix;
    target_key += time;
    target_key += "-";
    target_key += unique_str;

    ldout(cct, 20) << "RGWBL::render_target_key "<< "prefix=" << prefix
                   << " unique_str=" << unique_str
                   << " target_key=" << target_key << dendl;

    return target_key;
  }

}

int RGWBL::bucket_bl_fetch(const string opslog_obj, bufferlist *buffer)
{
  RGWAccessHandle sh;
  int r = store->log_show_init(opslog_obj, &sh);
  if (r < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_fetch"
                  << " log_show_init() failed, obj=" << opslog_obj
                  << " ret=" << cpp_strerror(-r) << dendl;
    return r;
  }

  struct rgw_log_entry entry;
  do {
    r = store->log_show_next(sh, &entry);
    if (r < 0) {
      ldout(cct, 20) << "RGWBL::bucket_bl_fetch log_show_next obj=" << opslog_obj
                     << " failed ret=" << cpp_strerror(-r) << dendl;
     return r;
    }

    if (r == 0) {
      ldout(cct, 20) << "RGWBL::bucket_bl_fetch log_show_next reached end." << dendl;
      break;
    }

    if (!entry.http_status.empty())
      format_opslog_entry(entry, buffer);

  } while (r > 0);

  return 0;
} 

void RGWBL::format_opslog_entry(struct rgw_log_entry& entry, bufferlist *buffer)
{
  std::string row_separator = " ";
  std::string newliner = "\n";
  std::stringstream pending_column;
  std::string oname;
  std::string oversion_id;

  oname = entry.obj.name.empty() ? "-" : entry.obj.name;
  oversion_id = entry.obj.instance.empty() ? "-" : entry.obj.instance;

  struct tm entry_time;
  time_t tt = entry.time.sec();
  localtime_r(&tt, &entry_time);
  char time_buffer[29];
  strftime(time_buffer, 29, "[%d/%b/%Y:%H:%M:%S %z]", &entry_time);
  std::string time(time_buffer);

                                                                               // S3 BL field
  pending_column << entry.bucket_owner.id << row_separator                     // Bucket Owner
                 << entry.bucket << row_separator                              // Bucket
                 << time << row_separator                                      // Time
                 << entry.remote_addr << row_separator                         // Remote IP
                 << entry.user << row_separator                                // Requester
                 << "-" << row_separator                                       // Request ID
                 << entry.op << row_separator                                  // Operation
                 << oname << row_separator                                     // Key
                 << entry.uri << row_separator                                 // Request-URI
                 << entry.http_status << row_separator                         // HTTP status
                 << entry.error_code << row_separator                          // Error Code
                 << entry.bytes_sent << row_separator                          // Bytes Sent
                 << entry.obj_size << row_separator                            // Object Size
                 << entry.total_time << row_separator                          // Total Time
                 << "-" << row_separator                                       // Turn-Around Time
                 << entry.referrer << row_separator                            // Referrer
                 << entry.user_agent << row_separator                          // User-Agent
                 << oversion_id << row_separator                               // Version Id
                 << newliner;

  buffer->append(pending_column.str());
}

int RGWBL::bucket_bl_upload(bufferlist* opslog_buffer, rgw_obj obj,
			    map<string, bufferlist> tobject_attrs)
{
  string url = cct->_conf->rgw_bl_url;
  if (url.empty()) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << " rgw_bl_url should not be empty." << dendl;
    return -EINVAL;
  }
  RGWRESTStreamWriteRequest req(cct, url, NULL, NULL);

  RGWAccessKey& key = store->get_zone_params().bl_deliver_key;
  if (key.id.empty()) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << " bl_deliver access key should not be empty." << dendl;
    return -EPERM;
  }

  if (key.key.empty()) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << "bl_deliver secret key should not be empty." << dendl;
    return -EPERM;
  }

  int ret = req.put_obj_init(key, obj, opslog_buffer->length(), tobject_attrs);
  if(ret < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << " req.put_obj_init failed ret="
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }
    
  // load buffer
  ret = req.get_out_cb()->handle_data(*opslog_buffer, 0,
                                      ((uint64_t)opslog_buffer->length()));
  if(ret < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << " req.get_out_cb()->handle_data failed ret="
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }

  string etag; 
  ret = req.complete(etag, nullptr);
  if(ret < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_upload"
                  << "req.complete failed ret="
		  << cpp_strerror(-ret) << dendl;

    return ret;
  }
  return ret;
}

int RGWBL::bucket_bl_remove(const string obj_name)
{
  int r = store->log_remove(obj_name);
  if (r < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_remove" 
                  << " log_remove() failed uploaded ret="
		  << cpp_strerror(-r) << dendl;

  }
  return r;
} 

int RGWBL::bucket_bl_deliver(string opslog_obj, const rgw_bucket target_bucket,
			     const string target_prefix,
			     map<string, bufferlist> tobject_attrs)
{
  ldout(cct, 20) << __func__ << " fetch phrase:" << dendl;

  RGWAccessHandle sh;
  int ret = store->log_show_init(opslog_obj, &sh);
  if (ret < 0) {
    ldout(cct, 0) << "RGWBL::bucket_bl_deliver"
                  << " log_show_init() failed, obj=" << opslog_obj
                  << " ret=" << cpp_strerror(-ret) << dendl;
    return ret;
  }
  
  bufferlist opslog_buffer;
  struct rgw_log_entry entry;
  int entry_nums = 0;
  std::vector<std::string> uploaded_obj_keys;
 
  do {
    ret = store->log_show_next(sh, &entry);
    if (ret < 0) {
      ldout(cct, 20) << "RGWBL::bucket_bl_deliver log_show_next obj=" << opslog_obj
                     << " failed ret=" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    
#define MAX_OPSLOG_UPLOAD_ENTRIES 10000
    if (ret > 0) {
      format_opslog_entry(entry, &opslog_buffer);
      entry_nums += 1;
    }

    if (entry_nums == MAX_OPSLOG_UPLOAD_ENTRIES || ret == 0) { 
      entry_nums = 0;

      if (opslog_buffer.length() == 0) {
        ldout(cct, 0) << __func__ << " opslog obj has no entries, obj = " 
	              << opslog_obj << dendl;
        break;
      }

      ldout(cct, 20) << __func__ << " render key phrase:" << dendl;
      string target_key = render_target_key(cct, target_prefix);
      if (target_key.empty()) {
        ldout(cct, 0) << __func__ << " render target object failed" << dendl;
        return -1;
      }

      rgw_obj tobject(target_bucket, target_key);
      
      ldout(cct, 20) << __func__ << " upload phrase:" << dendl;
      int upload_ret = -1;
#define BL_UPLOAD_RETRY_NUMS 2
      int retry_nums = BL_UPLOAD_RETRY_NUMS;
      do {
        upload_ret = bucket_bl_upload(&opslog_buffer, tobject, tobject_attrs);
      } while (upload_ret < 0 && upload_ret != -EPERM && (retry_nums--) != 0);
 
      opslog_buffer.clear();
      if (upload_ret < 0) {
        ldout(cct, 0) << __func__ << " bucket_bl_upload() failed ret="
		      << cpp_strerror(-upload_ret) << dendl;

        RGWObjectCtx obj_ctx(store);
        obj_ctx.obj.set_atomic(tobject);
        RGWBucketInfo bucket_info;
        int bucket_ret = store->get_bucket_info(obj_ctx, target_bucket.tenant, target_bucket.name, 
                                                bucket_info, NULL, NULL); 
        if (bucket_ret < 0) {
          if (bucket_ret == -ENOENT) {
            bucket_ret = -ERR_NO_SUCH_BUCKET;
          } 
          ldout(cct, 0) << __func__ << " get bucket_info failed! ret = " 
                        << bucket_ret << dendl;
          return bucket_ret;
        } 
        
        for (auto key_iter = uploaded_obj_keys.begin(); key_iter != uploaded_obj_keys.end(); key_iter++) {
          rgw_obj del_obj(target_bucket, *key_iter);
          int del_ret = store->delete_obj(obj_ctx, bucket_info, del_obj, bucket_info.versioning_status());
          if (del_ret < 0 && del_ret != -ENOENT) {
            ldout(cct, 0) << __func__ << " ERROR: delete log obj failed ret = "
                          << del_ret << " obj_key = " << target_key << dendl;
          }
        }
        
        return upload_ret;
      }
      uploaded_obj_keys.push_back(target_key);  
    }
  } while (ret > 0);

  ldout(cct, 20) << __func__ << " cleanup phrase:" << dendl;
  int remove_ret = bucket_bl_remove(opslog_obj);
  if (remove_ret < 0){
    return remove_ret;
  } else {
    return 0;
  }
}

int RGWBL::bucket_bl_process(string& shard_id)
{
  RGWBucketLoggingStatus status(cct);
  RGWBucketInfo sbucket_info;
  map<string, bufferlist> sbucket_attrs;
  RGWObjectCtx obj_ctx(store);

  vector<std::string> result;
  boost::split(result, shard_id, boost::is_any_of(":"));
  assert(result.size() == 3);
  string sbucket_tenant = result[0]; // sbucket stands for source bucket
  string sbucket_name = result[1];
  string sbucket_id = result[2];


  ldout(cct, 20) << "RGWBL:bucket_bl_process shard_id=" << shard_id
                 << " source bucket tenant=" << sbucket_tenant
                 << " source bucket name=" << sbucket_name
                 << " source bucket id=" << sbucket_id << dendl;
  int ret = store->get_bucket_info(obj_ctx, sbucket_tenant, sbucket_name,
                                   sbucket_info, NULL, &sbucket_attrs);
  if (ret < 0) {
    ldout(cct, 0) << "RGWBL:get_bucket_info failed, source_bucket_name="
                  << sbucket_name << dendl;
    return ret;
  }

  ret = sbucket_info.bucket.bucket_id.compare(sbucket_id) ;
  if (ret != 0) {
    ldout(cct, 0) << "RGWBL:old bucket id found, source_bucket_name="
		  << sbucket_name << " should be deleted." << dendl;
    return -ENOENT;
  }

  map<string, bufferlist>::iterator aiter = sbucket_attrs.find(RGW_ATTR_BL);
  if (aiter == sbucket_attrs.end())
    return 0;

  bufferlist::iterator iter(&aiter->second);
  try {
    status.decode(iter);
  } catch (const buffer::error& e) {
    ldout(cct, 0) << __func__ << " decode bucket logging status failed" << dendl;
    return -1;
  }

  if (!status.is_enabled()) {
    ldout(cct, 0) << __func__ << " bucket logging is diabled in the config, "
                  << "need to rm entry in following bucket_bl_post" << dendl;
    return -ENOENT;
  }

  int final_ret;
  map<string, bufferlist> tobject_attrs;

  string filter("");
  filter += sbucket_id;
  filter += "-";
  filter += sbucket_name;
  RGWAccessHandle lh;
  ret = store->log_list_init(filter, &lh);
  if (ret == -ENOENT) {
    // no opslog object
    return 0;
  } else {
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " list_log_init() failed ret="
		    << cpp_strerror(-ret) << dendl;
      return ret;
    }
 
    string tbucket_name = status.get_target_bucket();
    RGWBucketInfo tbucket_info;
    map<string, bufferlist> tbucket_attrs;
    RGWObjectCtx tobj_ctx(store);

    // source/target bucket owned by same user
    int ret = store->get_bucket_info(tobj_ctx, sbucket_tenant, tbucket_name,
                                     tbucket_info, NULL, &tbucket_attrs);
    if (ret < 0) {
      ldout(cct, 0) << "RGWBL:get_bucket_info failed, target_bucket_name="
                    << tbucket_name << dendl;
      return ret;
    } else {
      if (ret == 0) { 
         map<string, bufferlist>::iterator piter = tbucket_attrs.find(RGW_ATTR_ACL);
         if (piter == tbucket_attrs.end()) {
           ldout(cct, 0) << __func__ << " can't find tbucket ACL attr"
	                 << " tbucket_name=" << tbucket_name << dendl;
           return -EACCES;
         }

         // check LDG(bl_deliver) permission in target bucket acl
         bufferlist::iterator bpiter(&piter->second);
         RGWAccessControlPolicy tbucket_policy;
         try {
           tbucket_policy.decode(bpiter);
         } catch (const buffer::error& e) {
           ldout(cct, 0) << __func__
                         << "ERROR: caught buffer::error, could not decode tbucket policy"
                         << dendl;
           return -EIO;
         }

         std::string bl_deliver_accesskey = store->get_zone_params().bl_deliver_key.id;
         RGWUserInfo bl_deliver;
         if (bl_deliver.user_id.empty() && 
           rgw_get_user_info_by_access_key(store, bl_deliver_accesskey, bl_deliver) < 0) {
           ldout(cct, 0) << __func__ 
                         << "get bl deliver user info failed, bl_deliver_accesskey = " 
                         << bl_deliver_accesskey << dendl;
           return -EPERM;
         }
 
         std::multimap<string, ACLGrant> tbucket_grant_map = tbucket_policy.get_acl().get_grant_map(); 
         auto giter = tbucket_grant_map.find(bl_deliver.user_id.id);
         if (giter == tbucket_grant_map.end()) {
           ldout(cct, 0) << __func__ 
                         << "bl_deliver has no op permission to the target bucket, tbucket name = " 
                         << tbucket_name << dendl;
           return -EACCES; 
         }

         bool is_write = false;
         bool is_read_acp = false;

         std::pair<std::multimap<string, ACLGrant>::iterator, 
                   std::multimap<string, ACLGrant>::iterator> grant_ret;
         grant_ret = tbucket_grant_map.equal_range(bl_deliver.user_id.id);
         for (std::multimap<string, ACLGrant>::iterator iter = grant_ret.first;
              iter != grant_ret.second; iter++) {
             if (iter->second.get_permission().get_permissions() == RGW_PERM_WRITE) {
               is_write = true;
             } else if (iter->second.get_permission().get_permissions() == RGW_PERM_READ_ACP) {
               is_read_acp = true;
             }
         }

         if (is_write == false || is_read_acp == false){
           ldout(cct, 0) << __func__ 
                         << "bl_deliver has no write or read_acp permission to the target bucket, tbucket name = " 
                         << tbucket_name << dendl;
           return -EACCES;
         } 

         // Log obj acl granting 
         // The owner of log obj is LDG(bl_deliver), so LDG has full control permission in default
         // According to S3, target bucket has full control permission in default 
         // The target grants in request xml should be set into log obj's acl
         // http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerLogs.html
         RGWAccessControlPolicy tobject_policy(cct);
         bufferlist bl;
         RGWUserInfo tbucket_owner;
         RGWAccessControlList& tobject_acl = tobject_policy.get_acl();
         ACLOwner& tobject_owner = tobject_policy.get_owner();

         if (tbucket_owner.user_id.empty() && 
           rgw_get_user_info_by_uid(store, tbucket_info.owner, tbucket_owner) < 0) {
           ldout(cct, 0) << __func__ 
                         << "get target bucket owner failed, target bucket owner id = " 
                         << tbucket_info.owner.id << dendl;
           return -ENOENT;
         }

         tobject_owner.set_id(bl_deliver.user_id);
         tobject_owner.set_name(bl_deliver.display_name);
         ldout(cct, 20) << __func__ << " policy owner id = " << tobject_owner.get_id() << dendl;
          
         int acl_nums = 0;
         // add LDG(bl_deliver) full control permission in default
         ACLGrant ldg_new_grant;
         ldg_new_grant.set_canon(bl_deliver.user_id, bl_deliver.display_name, RGW_PERM_FULL_CONTROL);
         tobject_acl.add_grant(&ldg_new_grant);
         acl_nums++;
         // add owner of target bucket full control permission in default
         ACLGrant tbucket_owner_new_grant;
         tbucket_owner_new_grant.set_canon(tbucket_owner.user_id, tbucket_owner.display_name, RGW_PERM_FULL_CONTROL);
         tobject_acl.add_grant(&tbucket_owner_new_grant);
         acl_nums++;
         // add operating permission introduced from target grants in request xml
         ACLGrant target_grant;
         const std::vector<BLGrant> & bl_grant = status.get_target_grants();
         uint32_t rgw_permission = RGW_PERM_NONE;
         ACLGranteeType grantee_type;
         for (auto grant_iter = bl_grant.begin(); (grant_iter != bl_grant.end()) && (acl_nums < RGW_ACL_MAX_NUMS); grant_iter++) {
           rgw_permission = rgw_perm_map[grant_iter->get_permission()];
           ACLGranteeType_S3::set(grant_iter->get_type().c_str(), grantee_type);
           ACLGranteeTypeEnum acl_grantee_type = grantee_type.get_type();
           if (acl_grantee_type == ACL_TYPE_CANON_USER) {
             rgw_user canonical_user(grant_iter->get_id());
             target_grant.set_canon(canonical_user, grant_iter->get_display_name(), 
		                    rgw_permission);
           } else if (acl_grantee_type == ACL_TYPE_GROUP) {
#define RGW_URI_ALL_USERS	"http://acs.amazonaws.com/groups/global/AllUsers"
#define RGW_URI_AUTH_USERS	"http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
#define RGW_URI_LOG_DELIVERY    "http://acs.amazonaws.com/groups/s3/LogDelivery"
	     if (grant_iter->get_uri() != RGW_URI_LOG_DELIVERY) {
               std::string uri = grant_iter->get_uri();
               target_grant.set_group(target_grant.uri_to_group(uri), 
		                      rgw_permission);
             } else {
               std::string bl_deliver_accesskey = store->get_zone_params().bl_deliver_key.id;
               RGWUserInfo bl_deliver;

               if (bl_deliver.user_id.empty() &&
                   rgw_get_user_info_by_access_key(store, bl_deliver_accesskey, bl_deliver) < 0) {
                 ldout(cct, 10) << __func__ << "bl_deliver --> grant user does not exist:"
                                << bl_deliver_accesskey << dendl;
                 return -EINVAL;
               } else {
                 target_grant.set_canon(bl_deliver.user_id, bl_deliver.display_name,
                                        rgw_permission);
               }
             }
           } else if (acl_grantee_type == ACL_TYPE_EMAIL_USER) {
             RGWUserInfo email_user;
             std::string email_addr = grant_iter->get_email_address();
             rgw_get_user_info_by_email(store, email_addr, email_user);
             target_grant.set_canon(email_user.user_id, email_user.display_name,
		                    rgw_permission);
           }
           tobject_acl.add_grant(&target_grant);
           acl_nums++;
         }
         ldout(cct, 15) << __func__ << " source bucket " << sbucket_name
                        << " log obj ACL grants nums is " << acl_nums << dendl;
 
         tobject_policy.encode(bl);
         tobject_attrs[RGW_ATTR_ACL] = bl;
      }
    }

    string tprefix = status.get_target_prefix(); // prefix is optional
    rgw_bucket tbucket;
    tbucket = tbucket_info.bucket;
    string opslog_obj;
    while (true) {
      opslog_obj.clear();
      int r = store->log_list_next(lh, &opslog_obj);
      if (r == -ENOENT) {
	final_ret = 0; // no opslog object
	break;
      }
      if (r < 0) {
	ldout(cct, 0) << __func__ << " log_list_next() failed ret="
		      << cpp_strerror(-r) << dendl;
	final_ret = r;
	break;
      } else {
	int r = bucket_bl_deliver(opslog_obj, tbucket, tprefix, tobject_attrs);
	if (r < 0 ){
	  final_ret = r;
	  break;
	}
      }
    }
  }

  return final_ret;
}

int RGWBL::bucket_bl_post(int index, int max_lock_sec,
                          pair<string, int>& entry, int& result)
{
  utime_t lock_duration(cct->_conf->rgw_bl_lock_max_time, 0);

  rados::cls::lock::Lock l(bl_index_lock_name);
  l.set_cookie(cookie);
  l.set_duration(lock_duration);

  do {
    int ret = l.lock_exclusive(&store->bl_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another bl processor */
      dout(0) << "RGWBL::bucket_bl_post() failed to acquire lock on, sleep 5, try again. "
              << "obj " << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    dout(20) << "RGWBL::bucket_bl_post() get lock " << obj_names[index] << dendl;
    if (result == -ENOENT) {
      ret = cls_rgw_bl_rm_entry(store->bl_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        dout(0) << "RGWBL::bucket_bl_post() failed to remove entry "
                << obj_names[index] << dendl;
      }
      goto clean;
    } else if (result < 0) {
      if (result == -EPERM)
        entry.second = bl_perm_error;
      else if (result == -EACCES)
        entry.second = bl_acl_error;
      else  
        entry.second = bl_failed;
    } else {
      entry.second = bl_complete;
    }

    ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to set entry "
              << obj_names[index] << dendl;
    }
 clean:
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    dout(20) << "RGWBL::bucket_bl_post() unlock " << obj_names[index] << dendl;
    return 0;
  } while (true);
}

int RGWBL::list_bl_progress(const string& marker, uint32_t max_entries,
                            map<string, int> *progress_map)
{
  int index = 0;
  progress_map->clear();
  for(; index < max_objs; index++) {
    map<string, int > entries;
    int ret = cls_rgw_bl_list(store->bl_pool_ctx, obj_names[index],
                              marker, max_entries, entries);
    if (ret < 0) {
      if (ret == -ENOENT) {
        dout(0) << __func__ << " ignoring unfound bl object=" << obj_names[index] << dendl;
        continue;
      } else {
        return ret;
      }
    }
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      progress_map->insert(*iter);
    }
  }
  return 0;
}

int RGWBL::process()
{
  int max_secs = cct->_conf->rgw_bl_lock_max_time;

  unsigned start;
  int ret = get_random_bytes((char *)&start, sizeof(start));
  if (ret < 0)
    return ret;

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    ret = process(index, max_secs);
    if (ret < 0)
      dout(0) << __func__ << " processed  bl object=" << obj_names[index]
                          << " ret=" << cpp_strerror(-ret) << dendl;
  }

  return 0;
}

int RGWBL::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(bl_index_lock_name);
  do {
    utime_t now = ceph_clock_now();
    pair<string, int> entry; // string = bucket_name:bucket_id ,int = BL_BUCKET_STATUS
    if (max_lock_secs <= 0)
      return -EAGAIN;

    utime_t time(max_lock_secs, 0);
    l.set_duration(time);

    int ret = l.lock_exclusive(&store->bl_pool_ctx, obj_names[index]);
    if (ret == -EBUSY) { /* already locked by another bl processor */
      dout(0) << "RGWBL::process() failed to acquire lock on,"
              << " sleep 5, try again"
              << "obj " << obj_names[index] << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    string marker;
    cls_rgw_bl_obj_head head;
    ret = cls_rgw_bl_get_head(store->bl_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to get obj head "
              << obj_names[index] << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_bl_prepare(index);
      if (ret < 0) {
        dout(0) << "RGWBL::process() failed to update bl object "
                << obj_names[index] << ret << dendl;
        goto exit;
      }
    }

    ret = cls_rgw_bl_get_next_entry(store->bl_pool_ctx, obj_names[index],
                                    head.marker, entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to get obj entry "
              <<  obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    entry.second = bl_processing;
    ret = cls_rgw_bl_set_entry(store->bl_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to set obj entry "
              << obj_names[index] << entry.first << entry.second << dendl;
      goto exit;
    }

    head.marker = entry.first;
    ret = cls_rgw_bl_put_head(store->bl_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      dout(0) << "RGWBL::process() failed to put head "
              << obj_names[index] << dendl;
      goto exit;
    }

    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    ret = bucket_bl_process(entry.first);
    bucket_bl_post(index, max_lock_secs, entry, ret);
    continue;

 exit:
    l.unlock(&store->bl_pool_ctx, obj_names[index]);
    return 0;

  } while (1);

}

void RGWBL::start_processor()
{
  worker = new BLWorker(cct, this);
  worker->create("bl");
}

void RGWBL::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = nullptr;
}

void RGWBL::BLWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

bool RGWBL::going_down()
{
  return down_flag;
}

bool RGWBL::BLWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",
         &start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);
  if (cct->_conf->rgw_bl_debug_interval > 0) {
    /* we're debugging, so say we can run */
    return true;
  } else {
    if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
        (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
      return true;
    } else {
      return false;
    }
  }

}

int RGWBL::BLWorker::schedule_next_start_time(utime_t& now)
{
  if (cct->_conf->rgw_bl_debug_interval > 0) {
       int secs = cct->_conf->rgw_bl_debug_interval;
       return (secs);
  }

  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_bl_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  time_t nt;
  localtime_r(&tt, &bdt);
  bdt.tm_hour = start_hour;
  bdt.tm_min = start_minute;
  bdt.tm_sec = 0;
  nt = mktime(&bdt);

  return (nt+24*60*60 - tt);
}
