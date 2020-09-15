// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020-2021 IBM Research Europe <koutsovasilis.panagiotis1@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_S3_MIRROR_H
#define RGW_S3_MIRROR_H

#include "rgw/rgw_sal.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/MultipartUpload.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "rgw_s3_mirror_local.h"
#include "rgw_s3_mirror_sync_tasks.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

// this return code means that S3Mirror 
// has dumped the appropriate headers and
// body to the request and radosgw shouldn't
// do any further actions
#define S3MIRRORINTERCEPTED -1989

// class that holds the remote S3 endpoint, access_key, access_secret
class S3MirrorRemoteCredentials
{
public:
    string endpoint;
    string access_key;
    string access_secret;

    S3MirrorRemoteCredentials() : endpoint(""), access_key(""), access_secret("") {}
};

// singleton class, *should be initialized only once from rgw_main.cc*
class S3Mirror
{
private:
  S3Mirror() = default;
  S3Mirror(S3Mirror const &) = delete;
  S3Mirror &operator=(S3Mirror const &) = delete;

  rgw::sal::RGWRadosStore *store;
  CephContext *cxt;

  static Aws::S3::S3Client get_s3_client(string _endpoint, string _access_key, string _access_secret)
  {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::String("");
    config.verifySSL = false;
    config.connectTimeoutMs = 60000;
    config.requestTimeoutMs = 10000;
    config.maxConnections = 1024;
    config.endpointOverride = Aws::String(_endpoint);

    if (_endpoint.find("https://") != std::string::npos)
    {
      config.scheme = Aws::Http::Scheme::HTTPS;
    }
    else
    {
      config.scheme = Aws::Http::Scheme::HTTP;
    }

    if (_access_secret.compare("") != 0)
    {
      Aws::Auth::AWSCredentials credentials;
      credentials.SetAWSAccessKeyId(_access_key);
      credentials.SetAWSSecretKey(_access_secret);
      return Aws::S3::S3Client(credentials, config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
    }

    return Aws::S3::S3Client(config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
  }

public:
  Aws::S3::S3Client client;
  string bucket_to_listen;
  S3MirrorSyncMap sync_map;
  S3MirrorRemoteCredentials credentials;
  unsigned int consecutive_zero_refs_window;
  unsigned int interval_zero_refs_check;
  static std::shared_ptr<S3Mirror> instance;

  static void init(CephContext *cxt, rgw::sal::RGWRadosStore *store, string _endpoint,
                    string _access_key, string _access_secret, string _bucket_to_listen)
  {
    if (!instance)
    {
      if (_bucket_to_listen.empty())
        return;

      Aws::SDKOptions options;
      Aws::InitAPI(options);
      instance.reset(new S3Mirror());
      instance->store = store;
      instance->cxt = cxt;
      instance->credentials.endpoint = _endpoint;
      instance->credentials.access_key = _access_key;
      instance->credentials.access_secret = _access_secret;
      instance->bucket_to_listen = _bucket_to_listen;
      instance->client = get_s3_client(_endpoint, _access_key, _access_secret);
      instance->consecutive_zero_refs_window = 3;
      instance->interval_zero_refs_check = 10;

      // Let's check if the bucket on the remote exists
      if (instance->remote_head_bucket(_bucket_to_listen) < 0)
      {
        instance = nullptr;
        dout(20) << "S3Mirror remote bucket does not exist" << dendl;
        return;        
      }

      dout(20) << "S3Mirror initialized" << dendl;
    }
  }

  static std::shared_ptr<S3Mirror> getInstance()
  {
    return instance;
  }

  static std::shared_ptr<S3Mirror> getInstanceForRequest(struct req_state *_req_state)
  {
    if (instance != nullptr && instance->should_intercept_req_state(_req_state)) {
      return instance;
    }
    return nullptr;
  }

  rgw::sal::RGWRadosStore *getStore()
  {
    return this->store;
  }

  CephContext *getContext()
  {
    return this->cxt;
  }
  
  bool should_intercept_req_state(struct req_state *_req_state);

  // intercepting functions of rgw ops and rgw_process
  int intercept_list_buckets_rgwop(struct req_state *_req_state, rgw::sal::RGWBucketList &_bucket_list);
  int intercept_list_objects_rgwop(struct req_state *_req_state, int max, vector<rgw_bucket_dir_entry> &_result, 
                                  map<string, bool> &common_prefixes, rgw::sal::RGWBucket::ListParams &params, 
                                  bool &is_truncated, rgw_obj_key &next_marker);
  int intercept_list_objects_rgwop_V2(struct req_state *_req_state, int max, vector<rgw_bucket_dir_entry> &_result, 
                                      map<string, bool> &common_prefixes, rgw::sal::RGWBucket::ListParams &params, 
                                      bool &is_truncated, rgw_obj_key &next_marker, bool continuation_toke_exists);
  int intercept_rgw_process_error(int rgw_error, struct req_state *_req_state);
  int intercept_pre_process_req_state(struct req_state *_req_state);
  int intercept_copy_object_rgwop(struct req_state *_req_state, string copysource);
  int intercept_put_object_rgwop(struct req_state *_req_state, string multipart_upload_id, string multipart_part_str, 
                                int multipart_part_num, long long contentLength, shared_ptr<Aws::IOStream> awsbody);
  int intercept_init_multipart_upload_rgwop(struct req_state *_req_state, string& upload_id);
  int intercept_complete_multipart_rgwop(struct req_state *_req_state, string upload_id, map<uint32_t, RGWUploadPartInfo> &obj_parts);
  int intercept_abort_multipart_rgwop(struct req_state *_req_state, string upload_id);

  // remote S3 endpoint ops
  int remote_head_object(struct req_state *_req_state, bool dump_response=false);    
  int remote_create_bucket(struct req_state* _req_state, bool dump_response=false);
  int remote_delete_object(struct req_state *_req_state, bool dump_response = false);
  int remote_download_object(struct req_state *_req_state, bool dump_response = false);
  int remote_copy_object(struct req_state *_req_state, string copysource, bool dump_response = false);
  int remote_head_bucket(string bucket_name);

  // local Ceph ops
  int local_create_bucket(string _bucket_name, rgw_user user_id);
  int local_put_object(string _bucket_name, string _object_name, char* object_buffer, long long object_length, rgw_user user_id);
  int local_upload_to_remote(string _object_name, string bucket_name, rgw_user user_id);

  
  // async completion handlers for S3 async ops
  static void async_complete_multipart_finished(const Aws::S3::S3Client* client, 
                                                const Aws::S3::Model::CompleteMultipartUploadRequest& request, 
                                                const Aws::S3::Model::CompleteMultipartUploadOutcome& outcome, 
                                                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);

  static void async_put_part_finished(const Aws::S3::S3Client* client, const Aws::S3::Model::UploadPartRequest& request, 
                                      const Aws::S3::Model::UploadPartOutcome& outcome, 
                                      const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);

  static void async_put_object_finished(const Aws::S3::S3Client* client, const Aws::S3::Model::PutObjectRequest& request, 
                                        const Aws::S3::Model::PutObjectOutcome& outcome, 
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);

  static void async_abort_multipart_finished(const Aws::S3::S3Client* client, const Aws::S3::Model::AbortMultipartUploadRequest& request, 
                                            const Aws::S3::Model::AbortMultipartUploadOutcome& outcome, 
                                            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);

  static void GetObjectAsyncFinished(const Aws::S3::S3Client* client, const Aws::S3::Model::GetObjectRequest& request, 
                                      const Aws::S3::Model::GetObjectOutcome& outcome, 
                                      const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);
};

#endif
