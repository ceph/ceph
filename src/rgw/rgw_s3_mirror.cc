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

#include "rgw_common.h"
#include "global/global_context.h"
#include <boost/algorithm/string/replace.hpp>
#include "rgw_s3_mirror.h"
#include "rgw_op.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_user.h"
#include "rgw_sal.h"

using namespace std;

shared_ptr<S3Mirror> S3Mirror::instance = nullptr;

int S3Mirror::remote_head_bucket(string bucket_name)
{
  Aws::S3::Model::HeadBucketRequest head_bucket_request;
  head_bucket_request.SetBucket(bucket_name);

  auto head_bucket_outcome = this->client.HeadBucket(head_bucket_request);

  if (head_bucket_outcome.IsSuccess())
    return 0;
  
  return -1;
}

// Creates the request bucket on the remote S3 endpoint
int S3Mirror::remote_create_bucket(struct req_state* _req_state, bool dump_response)
{
  string bucket_name = _req_state->bucket_name;
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(bucket_name);

  auto create_bucket_outcome = this->client.CreateBucket(create_bucket_request);

  if (!create_bucket_outcome.IsSuccess())
  {
    auto err = create_bucket_outcome.GetError();
    dout(20) << "S3Mirror remote_create_bucket: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  if (!dump_response)
  {
    return 0;
  }
  
  auto create_bucket_result = create_bucket_outcome.GetResult();

  set_req_state_err(_req_state, 0);
  dump_errno(_req_state);
  dump_header_if_nonempty(_req_state, "Location", create_bucket_result.GetLocation());
  dump_content_length(_req_state, 0);
  end_header(_req_state);

  return S3MIRRORINTERCEPTED;
}

// Deletes the request object on the remote S3 endpoint
int S3Mirror::remote_delete_object(struct req_state* _req_state, bool dump_response)
{
  string bucket_name = _req_state->bucket_name;
  string object_name = _req_state->object->get_key().name;

  Aws::S3::Model::DeleteObjectRequest delete_object_request;
  delete_object_request.SetBucket(bucket_name);
  delete_object_request.SetKey(object_name);

  auto delete_object_outcome = this->client.DeleteObject(delete_object_request);

  if (!delete_object_outcome.IsSuccess())
  {
    auto err = delete_object_outcome.GetError();
    string exceptionName = err.GetExceptionName();
    if (exceptionName.compare("NoSuchBucket") != 0) 
    {
      dout(20) << "S3Mirror remote_delete_object: AWS Error " << 
                exceptionName << " " << err.GetMessage() << dendl;
      return -1;
    }
  }

  if (!dump_response)
  {
    return 0;
  }

  set_req_state_err(_req_state, STATUS_NO_CONTENT);
  dump_errno(_req_state);
  end_header(_req_state);

  return S3MIRRORINTERCEPTED;
}

// Performs a head request of object on the remote S3 endpoint
int S3Mirror::remote_head_object(struct req_state* _req_state, bool dump_response)
{
  string bucket_name = _req_state->bucket_name;
  string object_name = _req_state->object->get_key().name;

  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(bucket_name);
  head_object_request.SetKey(object_name);

  auto head_object_outcome = this->client.HeadObject(head_object_request);

  if (!head_object_outcome.IsSuccess())
  {
    auto err = head_object_outcome.GetError();
    dout(20) << "S3Mirror remote_head_object: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  if (!dump_response)
  {
    return 0;
  }

  auto head_object_result = head_object_outcome.GetResult();

  set_req_state_err(_req_state, 0);
  dump_errno(_req_state);
  dump_header_if_nonempty(_req_state, "Cache-Control", head_object_result.GetCacheControl());
  dump_header_if_nonempty(_req_state, "Content-Disposition", head_object_result.GetContentDisposition());
  dump_header_if_nonempty(_req_state, "Content-Encoding", head_object_result.GetContentEncoding());
  dump_header_if_nonempty(_req_state, "Content-Language", head_object_result.GetContentLanguage());
  dump_content_length(_req_state, head_object_result.GetContentLength());
  dump_header_if_nonempty(_req_state, "Content-Type", head_object_result.GetContentType());
  dump_header_if_nonempty(_req_state, "ETag", head_object_result.GetETag());
  auto medata = head_object_result.GetMetadata();
  for (Aws::Map<Aws::String, Aws::String>::iterator it = medata.begin(); it != medata.end(); ++it)
  {
      dump_header(_req_state, it->first , it->second );
  }

  auto ceph_timepoint = ceph::real_clock::from_double(
    chrono::system_clock::to_time_t(head_object_result.GetExpires().UnderlyingTimestamp())
  );
  dump_time_header(_req_state, "Expires", ceph_timepoint);
  auto ceph_timepoint_last_modified = ceph::real_clock::from_double(
    static_cast<double>(chrono::system_clock::to_time_t(head_object_result.GetLastModified().UnderlyingTimestamp()))
  );
  dump_time_header(_req_state, "Last-Modified", ceph_timepoint_last_modified);
  end_header(_req_state);

  return S3MIRRORINTERCEPTED;
}

// Copies the request object on the remote
int S3Mirror::remote_copy_object(struct req_state *_req_state, string copysource, bool dump_response)
{
  Aws::S3::Model::CopyObjectRequest copy_object_request;
  copy_object_request.SetCopySource(copysource.c_str());
  copy_object_request.SetBucket(_req_state->bucket_name);
  copy_object_request.SetKey(_req_state->object->get_key().name);
  copy_object_request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::COPY);
  
  auto copy_object_outcome = this->client.CopyObject(copy_object_request);
  if (!copy_object_outcome.IsSuccess()) 
  {
    auto err = copy_object_outcome.GetError();
    dout(20) << "S3Mirror intercept_copy_object_rgwop: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  if (!dump_response)
  {
    return 0;
  }

  auto copy_object_result = copy_object_outcome.GetResult().GetCopyObjectResultDetails();

  set_req_state_err(_req_state, 0);
  dump_errno(_req_state);
  end_header(_req_state, nullptr, "application/xml", CHUNKED_TRANSFER_ENCODING);
  dump_start(_req_state);
  _req_state->formatter->open_object_section_in_ns("CopyObjectResult", XMLNS_AWS_S3);
  auto ceph_timepoint_last_modified = ceph::real_clock::from_double(
    static_cast<double>(chrono::system_clock::to_time_t(copy_object_result.GetLastModified().UnderlyingTimestamp()))
  );
  dump_time(_req_state, "LastModified", &ceph_timepoint_last_modified);
  string etag = copy_object_result.GetETag();
  _req_state->formatter->dump_string("ETag", std::move(etag));
  _req_state->formatter->close_section();
  rgw_flush_formatter_and_reset(_req_state, _req_state->formatter);

  return S3MIRRORINTERCEPTED;
}


// helper function to parse the range of a blocking request. See below the remote_download_object for more info
// on the "blocking request"
static bool parse_http_range(struct req_state* _req_state, off_t &start, off_t &end, long long total)
{
  string defaultVal = "";
  string rs(_req_state->info.env->get("HTTP_RANGE", defaultVal.c_str()));

  if (rs == defaultVal)
    return false;

  string ofs_str;
  string end_str;

  size_t pos = rs.find("bytes=");
  if (pos == string::npos) {
    pos = 0;
    while (isspace(rs[pos]))
      pos++;
    int end = pos;
    while (isalpha(rs[end]))
      end++;
    if (strncasecmp(rs.c_str(), "bytes", end - pos) != 0)
      return 0;
    while (isspace(rs[end]))
      end++;
    if (rs[end] != '=')
      return 0;
    rs = rs.substr(end + 1);
  } else {
    rs = rs.substr(pos + 6); /* size of("bytes=")  */
  }
  pos = rs.find('-');
  if (pos == string::npos)
    goto done;

  ofs_str = rs.substr(0, pos);
  end_str = rs.substr(pos + 1);
  if (end_str.length()) {
    end = atoll(end_str.c_str());
    if (end < 0)
      goto done;
  }
  else {
    end = total - 1;
  }

  if (ofs_str.length()) {
    start = atoll(ofs_str.c_str());
  } else { // RFC2616 suffix-byte-range-spec
    start = -end;
    end = -1;
  }

  if (end >= 0 && end < start)
    goto done;

  return true;

done:
  return false;
}


// helper function to dump the headers of a blocking request. See below the remote_download_object for more info
// on the "blocking request"
static void dump_request_headers(struct req_state *_req_state, std::shared_ptr<S3MirrorDownloadTask> download_task, 
                                off_t &start, off_t &end, long long &content_size)
{
  string bucket_name = _req_state->bucket_name;
  string object_name = _req_state->object->get_key().name;
  long long content_length = download_task->content_length;
  bool partial_content = parse_http_range(_req_state, start, end, content_length);

  if (end > content_length - 1 || end == 0) 
  {
    end = content_length - 1;
  }

  if (partial_content) 
  {
    content_size = (end+1)-start;
  }
  else 
  {
    content_size = content_length;
    start = 0;
    end = content_length - 1;
  }
  
  if (partial_content)
  {
    set_req_state_err(_req_state, STATUS_PARTIAL_CONTENT);
  }
  else 
  {
    set_req_state_err(_req_state, 0);
  }
  dump_errno(_req_state);
  dump_time_header(_req_state, "Last-Modified", download_task->last_modified);
  dump_content_length(_req_state, content_size);
  dump_header_if_nonempty(_req_state, "ETag", download_task->etag);
  dump_header_if_nonempty(_req_state, "Cache-Control", download_task->cache_control);
  dump_header_if_nonempty(_req_state, "Content-Disposition", download_task->content_disposition);
  dump_header_if_nonempty(_req_state, "Content-Encoding", download_task->content_encoding);
  dump_header_if_nonempty(_req_state, "Content-Language", download_task->content_language);
  dump_header_if_nonempty(_req_state, "Content-Type", download_task->content_type);
  dump_range(_req_state, start, end, content_length);
  dump_time_header(_req_state, "Expires", download_task->expires);
  end_header(_req_state);
}

// Downloads an object from the remote S3 endpoint. Also in parallel serves all requests
// and writes the object locally to the ceph cluster
int S3Mirror::remote_download_object(struct req_state* _req_state, bool dump_response)
{
  bool existed = false;
  string bucket_name = _req_state->bucket_name;
  string object_name = _req_state->object->get_key().name;
  string download_task_id = "download_" + bucket_name + "_" + object_name;

  std::shared_ptr<S3MirrorDownloadTask> download_task = this->sync_map.find_or_create_task<S3MirrorDownloadTask>(download_task_id, existed);
  download_task->refs++;
  if (!existed) 
  {
    download_task->user_id = _req_state->user->get_id();

    // Since we need to download the object we create 
    // this thread which is responsible *only* for writing the 
    // object variables through a head request, downloading the object
    // and afterwards, when the object is "cold" write it locally
    // and remove the task from the syncmap
    std::thread([this, download_task, bucket_name, object_name, download_task_id]() {

      // Do a headobjectrequest so we can in parallel supply the blocking requests
      // with the necessary variables to dump the corresponding headers
      Aws::S3::Model::HeadObjectRequest head_object_request;
      head_object_request.SetBucket(bucket_name);
      head_object_request.SetKey(object_name);

      auto head_object_outcome = this->client.HeadObject(head_object_request);

      if (!head_object_outcome.IsSuccess())
      {
        download_task->ret = -1;
        download_task->completed = true;
        auto err = head_object_outcome.GetError();
        dout(20) << "S3Mirror remote_download_object AWS Error " << 
                    err.GetExceptionName() << " " << err.GetMessage() << dendl;
        this->sync_map.remove_task(download_task_id);
        return;
      }

      // write the mandatory object variables so the blocking
      // requests waiting practically the object of this task 
      // can begin dumping the headers
      auto head_object_result = head_object_outcome.GetResult();

      const long long total_length = head_object_result.GetContentLength();

      download_task->expires = ceph::real_clock::from_double(
        static_cast<double>(chrono::system_clock::to_time_t(head_object_result.GetExpires().UnderlyingTimestamp()))
      );
      download_task->last_modified = ceph::real_clock::from_double(
        static_cast<double>(chrono::system_clock::to_time_t(head_object_result.GetLastModified().UnderlyingTimestamp()))
      );
      download_task->etag = head_object_result.GetETag();
      download_task->cache_control = head_object_result.GetCacheControl();
      download_task->content_disposition = head_object_result.GetContentDisposition();
      download_task->content_encoding = head_object_result.GetContentEncoding();
      download_task->content_language = head_object_result.GetContentLanguage();
      download_task->content_type = head_object_result.GetContentType();

      download_task->customStreamBuf = make_shared<S3MirrorDownloadTask::CustomStreamBuffer>(total_length);
      auto stream_buf = download_task->customStreamBuf;
      if (stream_buf->get_buffer() == nullptr)
      {
        //OOM?!
        dout(20) << "S3Mirror remote_download_object: Error OOM?! " << dendl;
        download_task->ret = -1;
        download_task->completed = true;
        this->sync_map.remove_task(download_task_id);
        return;
      }
      // allocate the iostream and get the actual object
      // this "new" allocation is internally deallocated 
      // by the aws sdk when the result is getting out of scope
      // In our case, when the shared_ptr will destruct
      download_task->ioStream = new std::basic_iostream<char, std::char_traits<char>>(stream_buf.get());

      // *ALWAYS* write last the downloadTask->content_length
      // the blocking requests rely on this to start dumping the headers
      // and the body
      download_task->content_length = total_length;
      
      Aws::S3::Model::GetObjectRequest get_object_request;
      get_object_request.SetBucket(bucket_name);
      get_object_request.SetKey(object_name);
      auto _custom_iostream_fn = [download_task](){ return download_task->ioStream;};
      get_object_request.SetResponseStreamFactory(_custom_iostream_fn);

      auto get_object_outcome = this->client.GetObject(get_object_request);

      if (!get_object_outcome.IsSuccess()) {
        auto err = get_object_outcome.GetError();
        dout(1) << "S3Mirror remote_download_object: AWS Error " << 
                    err.GetExceptionName() << " " << err.GetMessage() << dendl;
        download_task->ret = -1;
        download_task->completed = true;
        this->sync_map.remove_task(download_task_id);
        return;
      }
      
      // We need to get a hold of the result so that AWS SDK 
      // won't clear our iostream at the end of this function. 
      // This is the reason we dont explicitly delete the download_task-ioStream
      // PS: these "ugly" const_casts are mandatory to get the result with ownership
      download_task->result.push_back(
              const_cast<Aws::S3::Model::GetObjectResult&&>(
                const_cast<Aws::S3::Model::GetObjectOutcome&>(get_object_outcome).GetResultWithOwnership()
              ));
      download_task->completed = true;

      // Consecutive time windows that task refs are 0 
      // mean that the object of the task is cold and we will 
      // write locally and the smart pointers will
      // take care releasing the allocated memory
      unsigned int zero_refs_window_counter = 0;
      while(zero_refs_window_counter != this->consecutive_zero_refs_window) {
        std::this_thread::sleep_for(std::chrono::seconds(this->interval_zero_refs_check));
        if (download_task->refs.load(std::memory_order_relaxed) != 0)
          zero_refs_window_counter = 0;
        else
          zero_refs_window_counter++;
      }
      
      // Write the object locally to the ceph cluster 
      this->local_put_object(bucket_name, object_name, stream_buf->get_buffer(), total_length, download_task->user_id);
      this->sync_map.remove_task(download_task_id);

    }).detach();
  }
  
  // This request will block until the above thread supplies
  // the object headers. Afterwards in a polling fashion it 
  // the body of the request
  {
    
    bool has_dumped_headers = false;
    off_t start, end;
    long long total_bytes_to_write = 0;

    while (!download_task->completed)
    {
      // If this is true it means that headers are not ready yet
      if (download_task->content_length == 0)
        continue;
      
      // Dump the headers to the client
      if (!has_dumped_headers)
      {
        has_dumped_headers = true;
        dump_request_headers(_req_state, download_task, start, end, total_bytes_to_write);
      }
      
      // stream_buf shouldn't be nullptr but lets check
      auto stream_buf = download_task->customStreamBuf;
      long long bytes_written;
      if (stream_buf == nullptr || !(bytes_written = stream_buf->get_unsafe_length())) {
        continue;
      }
      
      // Dump the avalaible body bytes to the client
      if (total_bytes_to_write > 0 && bytes_written >= start + total_bytes_to_write) {
        int availableBytesToWrite = (bytes_written >= start + total_bytes_to_write) ? total_bytes_to_write : bytes_written - start;    
        dump_body(_req_state, &(stream_buf->get_buffer()[start]), availableBytesToWrite);
        start += availableBytesToWrite;
        total_bytes_to_write -= availableBytesToWrite;
      }

      // If we are all done here break the loop
      if (has_dumped_headers && total_bytes_to_write == 0) {
        break;
      }
    }

    // If something was unsuccessful return negative
    // and let radosgw handle this
    if (download_task->ret < 0)
    {
      download_task->refs--;
      return -1;
    }

    // Need to check for headers and dump the body
    // as there is a change the request arrived
    // when the actual download task is completed
    // and is still available because it is "hot"
    if (download_task->content_length > 0 && !has_dumped_headers)
    {
      has_dumped_headers = true;
      dump_request_headers(_req_state, download_task, start, end, total_bytes_to_write);
    }

    auto stream_buf = download_task->customStreamBuf;
    long long bytes_written;
    if (stream_buf == nullptr || !(bytes_written = stream_buf->get_unsafe_length())) {
      dout(20) << "S3Mirror remote_download_object: Error download task " << 
                  "completed but stream_buf null or empty" << dendl;
      download_task->refs--;
      return -1;
    }

    if (has_dumped_headers && total_bytes_to_write > 0 && bytes_written >= start + total_bytes_to_write) {
      int availableBytesToWrite = (bytes_written >= start + total_bytes_to_write) ? total_bytes_to_write : bytes_written - start;    
      dump_body(_req_state, &(stream_buf->get_buffer()[start]), availableBytesToWrite);
      start += availableBytesToWrite;
      total_bytes_to_write -= availableBytesToWrite;
    }
    
    // If our start does not match end
    // it means that we missed some bytes of the 
    // client request so report back and let 
    // radosgw handle this
    if (start != end + 1) {
      dout(20) << "S3Mirror remote_download_object Error missed bytes" << dendl;
      download_task->refs--;
      return -1;
    }
    
    // At this point we are all set flush and complete
    RESTFUL_IO(_req_state)->flush();
    RESTFUL_IO(_req_state)->complete_request();
  }

  download_task->refs--;
  return S3MIRRORINTERCEPTED;
}

/* Region
 * 
 * S3Mirror intercept functions on existing rgw ops
 * 
 */

// Check if this request should be mirrored by the remote S3 endpoint
bool S3Mirror::should_intercept_req_state(struct req_state *_req_state)
{
  // if S3Mirror: bypass header exists it means that we wont mirror any rgw op
  map<string, string, ltstr_nocase> http_env_map = _req_state->info.env->get_map();
  map<string, string, ltstr_nocase>::iterator mirror_http_header = http_env_map.find("HTTP_S3MIRROR");
  if (mirror_http_header != http_env_map.end() && mirror_http_header->second.compare("bypass") == 0)
  {
    dout(20) << "S3Mirror: not intercepting req_state bypass http header found" << dendl;
    return false;
  }

  if (_req_state->op_type == RGW_OP_LIST_BUCKETS) {
    return true;
  }

  if (!_req_state->bucket_name.empty() && !this->bucket_to_listen.empty() && this->bucket_to_listen.compare(_req_state->bucket_name) != 0)
  {
    dout(20) << "S3Mirror: not intercepting req_state bucket_to_listen doesn't match bucket_name" << dendl;
    return false;
  }

  dout(20) << "S3Mirror: will intercept req_state for op type " << _req_state->op_type << dendl;

  return true;
}

// Intercept the errors of rgw_process_authenticated 
// The errors of Interest are -ERR_NO_SUCH_BUCKET and -ENOENT
// These mean that the bucket or object does not exist locally and S3Mirror
// needs to intercept either by downloading the remote object or by 
// serving the request on its own
int S3Mirror::intercept_rgw_process_error(int rgw_error, struct req_state *_req_state)
{
  auto req_state_type = _req_state->op_type;
  int ret;
  bool create_bucket = false, fetch_object = false;

  if (rgw_error != -ERR_NO_SUCH_BUCKET && rgw_error != -ENOENT)
  {
    return -1;
  }

  switch (req_state_type)
  {
  case RGW_OP_PUT_OBJ:
  case RGW_OP_LIST_BUCKET:
  case RGW_OP_STAT_BUCKET:
    // For these ops we need to create the bucket locally
    create_bucket = rgw_error == -ERR_NO_SUCH_BUCKET;
    break;
  case RGW_OP_GET_OBJ:
    // For these ops we need to download the object from the remote
    create_bucket = rgw_error == -ERR_NO_SUCH_BUCKET;
    fetch_object = create_bucket || rgw_error == -ENOENT;
    break;
  case RGW_OP_DELETE_OBJ:
    // No need to download anything delegate this to remote S3
    // and dump the response
    return this->remote_delete_object(_req_state, true);
  case RGW_OP_COPY_OBJ:
    {
      // No need to download anything delegate this to remote S3
      // and dump the response
      string copy_source = _req_state->src_bucket_name + "/" + _req_state->src_object->get_key().name;
      return this->remote_copy_object(_req_state, copy_source, true);
    }
  default:
    return -1;
  }

  if (create_bucket)
  {
    // if the op requires the bucket present create it
    ret = this->local_create_bucket(_req_state->bucket_name, _req_state->user->get_id());

    if (ret < 0 && ret != -ERR_BUCKET_EXISTS)
    {
      dout(20) << "S3Mirror: couldn't create local bucket although remote exists " << ret << dendl;
      return ret;
    }
  }

  if (fetch_object)
  {
    if (req_state_type == RGW_OP_GET_OBJ && _req_state->op == OP_HEAD)
    {
      // No need to download anything delegate this to remote S3
      // and dump the response
      return this->remote_head_object(_req_state, true);
    }
    
    // download the object from the remote S3 endpoint
    ret = this->remote_download_object(_req_state);
    
    if (ret < 0)
    {
      dout(20) << "S3Mirror: couldn't fetch the remote file to ceph" << dendl;
      return ret;
    }
  }

  return ret;
}

// Intercept the RGW_OP_COPY_OBJ request
// should be called only by rgw_op.cc
int S3Mirror::intercept_copy_object_rgwop(struct req_state *_req_state, string copysource) 
{
  return this->remote_copy_object(_req_state, copysource);
}

// Intercept the RGW_OP_INIT_MULTIPART request
// *should be called only by rgw_op.cc*
// We supply the upload_id from the remote S3 so 
// the following request of the client will supply
// us a valid, in regard with the remote S3, upload_id 
int S3Mirror::intercept_init_multipart_upload_rgwop(struct req_state *_req_state, string& upload_id)
{
  string default_content_type = "";
  string content_type = string(_req_state->info.env->get("HTTP_CONTENT_TYPE", default_content_type.c_str()));

  Aws::S3::Model::CreateMultipartUploadRequest multipart_upload_request;
  multipart_upload_request.SetBucket(_req_state->bucket_name);
  multipart_upload_request.SetKey(_req_state->object->get_key().name);
  if (content_type != "")
    multipart_upload_request.SetContentType(content_type);
  
  auto multipart_upload_outcome = this->client.CreateMultipartUpload(multipart_upload_request);

  if (!multipart_upload_outcome.IsSuccess())
  {
    auto err = multipart_upload_outcome.GetError();
    dout(20) << "S3Mirror intercept_init_multipart_upload_rgwop: AWS Error " << 
                  err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  upload_id = multipart_upload_outcome.GetResult().GetUploadId();
  return 0;
}

// Intercept the RGW_OP_ABORT_MULTIPART request
// *should be called only by rgw_op.cc*
// Propagate the abort multipart to the remote
int S3Mirror::intercept_abort_multipart_rgwop(struct req_state *_req_state, string upload_id)
{
  std::shared_ptr<Aws::Client::AsyncCallerContext> context = 
        Aws::MakeShared<Aws::Client::AsyncCallerContext>("cache:context");
  context->SetUUID(_req_state->object->get_key().name);
  
  Aws::S3::Model::AbortMultipartUploadRequest abort_multi_upload_request;
  abort_multi_upload_request.SetBucket(_req_state->bucket_name);
  abort_multi_upload_request.SetKey(_req_state->object->get_key().name);
  abort_multi_upload_request.SetUploadId(upload_id);
  
  dout(20) << "S3Mirror abort multipart upload " << _req_state->object->get_key().name << " object to remote" << dendl;
  this->client.AbortMultipartUploadAsync(abort_multi_upload_request, S3Mirror::async_abort_multipart_finished, context);
  return 0;
}

// Intercept the RGW_OP_COMPLETE_MULTIPART request
// *should be called only by rgw_op.cc*
// This will utilize the async s3 client as we dont 
// need to wait for all the other async part uploads
// to finish
int S3Mirror::intercept_complete_multipart_rgwop(struct req_state *_req_state, string upload_id, map<uint32_t, RGWUploadPartInfo> &obj_parts)
{
  string bucket_name = _req_state->bucket_name;
  string object_name = _req_state->object->get_key().name;
  
  auto multipart_upload_ptr = std::make_shared<Aws::S3::Model::CompleteMultipartUploadRequest>();
  multipart_upload_ptr->SetBucket(bucket_name);
  multipart_upload_ptr->SetKey(object_name);
  multipart_upload_ptr->SetUploadId(upload_id);

  auto completed_multipart_upload = std::make_shared<Aws::S3::Model::CompletedMultipartUpload>();
      
  for (auto obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
    std::shared_ptr<Aws::S3::Model::CompletedPart> completedPart = std::make_shared<Aws::S3::Model::CompletedPart>();
    completedPart->SetPartNumber(obj_iter->second.num);
    completedPart->SetETag(obj_iter->second.etag);
    completed_multipart_upload->AddParts(*completedPart);
  }

  multipart_upload_ptr->SetMultipartUpload(*completed_multipart_upload); 

  // Need to introduce this in thread so we can wait for all the object parts to upload
  // and then issue the multipart complete
  std::thread([this, bucket_name, object_name, multipart_upload_ptr, completed_multipart_upload]() {
      string complete_multipart_task_id = "partialupload_" + bucket_name + object_name + "#";
      std::shared_ptr<S3MirrorTask> task = this->sync_map.findPrefix<S3MirrorTask>(complete_multipart_task_id);
      while (task != nullptr) {
        task->wait();
        task = this->sync_map.findPrefix<S3MirrorTask>(complete_multipart_task_id);
      }

      std::shared_ptr<Aws::Client::AsyncCallerContext> context = Aws::MakeShared<Aws::Client::AsyncCallerContext>("PutObjectAllocationTag");
      context->SetUUID(bucket_name + object_name);

      this->client.CompleteMultipartUploadAsync(*multipart_upload_ptr, S3Mirror::async_complete_multipart_finished, context);

  }).detach();

  return 0;
}

// Intercept the RGW_OP_PUT_OBJ request
// *should be called only by rgw_op.cc*
// This will utilize the async s3 client as we dont 
// want the client to blocking waiting for the object
// to upload on the remote S3 endpoint
int S3Mirror::intercept_put_object_rgwop(struct req_state *_req_state, string multipart_upload_id, 
              string multipart_part_str, int multipart_part_num, long long content_length, 
              shared_ptr<Aws::IOStream> aws_body) 
{
  auto context = Aws::MakeShared<Aws::Client::AsyncCallerContext>("cache:context");
  
  if (!multipart_part_str.empty()) 
  {
    //NOTE: I would love to use `supplied_md5_b64` but s3fs does something weird here
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*aws_body));
    string part_md5_b64 = Aws::Utils::HashingUtils::Base64Encode(part_md5);

    Aws::S3::Model::UploadPartRequest upload_part_request;
    upload_part_request.SetBucket(_req_state->bucket_name);
    upload_part_request.SetKey(_req_state->object->get_key().name);
    upload_part_request.SetPartNumber(multipart_part_num);
    upload_part_request.SetUploadId(multipart_upload_id.c_str());
    upload_part_request.SetContentLength(content_length);
    upload_part_request.SetContentMD5(part_md5_b64);
    upload_part_request.SetBody(aws_body);
    
    string task_id = "partialupload_" + _req_state->bucket_name + _req_state->object->get_key().name + "#" + multipart_part_str;
    bool existed = false;
    std::shared_ptr<S3MirrorTask> part_upload_task = this->sync_map.find_or_create_task<S3MirrorTask>(task_id, existed);
    context->SetUUID(task_id);
    dout(20) << "S3Mirror uploading part " << multipart_part_str << " object to remote" << dendl;
    this->client.UploadPartAsync(upload_part_request, S3Mirror::async_put_part_finished, context);
  } 
  else
  {
    Aws::S3::Model::PutObjectRequest put_object_request;
    put_object_request.SetBucket(_req_state->bucket_name);
    put_object_request.SetKey(_req_state->object->get_key().name);
    put_object_request.SetContentLength(content_length);
    put_object_request.SetBody(aws_body);

    dout(20) << "S3Mirror uploading single object to remote" << dendl;
    this->client.PutObjectAsync(put_object_request, S3Mirror::async_put_object_finished, context);
  }
  return 0;
}

// Intercept the RGW_OP_LIST_BUCKETS request
// *should be called only by rgw_op.cc*
int S3Mirror::intercept_list_buckets_rgwop(struct req_state *_req_state, rgw::sal::RGWBucketList &_bucket_list)
{
  auto list_buckets_outcome = this->client.ListBuckets();

  if (!list_buckets_outcome.IsSuccess())
  {
    auto err = list_buckets_outcome.GetError();
    dout(20) << "S3Mirror intercept_list_buckets_rgwop: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  for (Aws::S3::Model::Bucket const &_aws_bucket : list_buckets_outcome.GetResult().GetBuckets())
  {
      string aws_bucket_name = _aws_bucket.GetName();
      
      if (!this->bucket_to_listen.empty() && this->bucket_to_listen.compare(aws_bucket_name) != 0) {
          continue;
      }

      struct RGWBucketEnt rgw_bucket_ent;
      rgw_bucket_ent.bucket.name = aws_bucket_name;
      rgw_bucket_ent.creation_time = ceph::real_clock::from_time_t(
                                        chrono::system_clock::to_time_t(
                                          _aws_bucket.GetCreationDate().UnderlyingTimestamp()
                                        ));

      _bucket_list.add(std::unique_ptr<rgw::sal::RGWBucket>(new rgw::sal::RGWRadosBucket(this->store, rgw_bucket_ent, _req_state->user.get())));
  }

  return 0;
}

// Intercept the RGW_OP_LIST_BUCKET request
// *should be called only by rgw_op.cc*
int S3Mirror::intercept_list_objects_rgwop(struct req_state *_req_state, int max, vector<rgw_bucket_dir_entry> &_result, map<string, bool> &common_prefixes, rgw::sal::RGWBucket::ListParams &params, bool &is_truncated, rgw_obj_key &next_marker)
{
  _result.clear();

  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(_req_state->bucket_name);
  list_objects_request.SetMaxKeys(max);

  if (!params.prefix.empty()){
    list_objects_request.SetPrefix(params.prefix);
  }

  if (!params.delim.empty()) {
    list_objects_request.SetDelimiter(params.delim);
  }

  if (!params.marker.empty()) {
    list_objects_request.SetMarker(params.marker.name);
  }

  auto list_objects_outcome = this->client.ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess())
  {
    auto err = list_objects_outcome.GetError();
    dout(20) << "S3Mirror remote_list_bucket_objects: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  auto list_objects_result = list_objects_outcome.GetResult();

  for (Aws::S3::Model::Object const &_aws_object : list_objects_result.GetContents())
  {
    rgw_bucket_dir_entry rgw_dir_entry = rgw_bucket_dir_entry();
    rgw_dir_entry.exists = true;

    auto key = cls_rgw_obj_key();
    key.name = _aws_object.GetKey();
    rgw_dir_entry.key = key;

    rgw_dir_entry.meta = rgw_bucket_dir_entry_meta();
    auto time = static_cast<double>(chrono::system_clock::to_time_t(_aws_object.GetLastModified().UnderlyingTimestamp()));
    rgw_dir_entry.meta.mtime = ceph::real_clock::from_double(time);
    rgw_dir_entry.meta.etag = _aws_object.GetETag();
    boost::replace_all(rgw_dir_entry.meta.etag,"\"","");
    rgw_dir_entry.meta.size = rgw_dir_entry.meta.accounted_size = _aws_object.GetSize();
    _result.push_back(rgw_dir_entry);
  }

  for (Aws::S3::Model::CommonPrefix const &_aws_common_prefix : list_objects_result.GetCommonPrefixes())
  {
      common_prefixes[_aws_common_prefix.GetPrefix()] = true;
  }

  is_truncated = list_objects_result.GetIsTruncated();

  if (is_truncated)
      next_marker = list_objects_result.GetNextMarker();

  return 0;
}

// Intercept the RGW_OP_LIST_BUCKET (V2) request
// *should be called only by rgw_op.cc*
int S3Mirror::intercept_list_objects_rgwop_V2(struct req_state *_req_state, int max, vector<rgw_bucket_dir_entry> &_result, map<string, bool> &common_prefixes, rgw::sal::RGWBucket::ListParams &params, bool &is_truncated, rgw_obj_key &next_marker, bool continuation_token_exists)
{
  _result.clear();

  Aws::S3::Model::ListObjectsV2Request list_objects_request;
  list_objects_request.SetBucket(_req_state->bucket_name);
  list_objects_request.SetMaxKeys(max);

  if (!params.prefix.empty()){
    list_objects_request.SetPrefix(params.prefix);
  }

  if (!params.delim.empty()) {
    list_objects_request.SetDelimiter(params.delim);
  }

  if (!params.marker.empty()) {
    if (!continuation_token_exists) {
      list_objects_request.SetStartAfter(params.marker.name);
    }
    else {
      list_objects_request.SetContinuationToken(params.marker.name);
    }
  }
  
  auto list_objects_outcome = this->client.ListObjectsV2(list_objects_request);

  if (!list_objects_outcome.IsSuccess())
  {
    auto err = list_objects_outcome.GetError();
    dout(20) << "S3Mirror intercept_list_objects_rgwop_V2: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  auto list_objects_result = list_objects_outcome.GetResult();

  for (Aws::S3::Model::Object const &_aws_object : list_objects_result.GetContents())
  {
    string _aws_object_key = _aws_object.GetKey();
    
    rgw_bucket_dir_entry rgw_dir_entry = rgw_bucket_dir_entry();
    rgw_dir_entry.exists = true;

    auto key = cls_rgw_obj_key();
    key.name = _aws_object_key;
    rgw_dir_entry.key = key;

    rgw_dir_entry.meta = rgw_bucket_dir_entry_meta();
    auto time = static_cast<double>(chrono::system_clock::to_time_t(_aws_object.GetLastModified().UnderlyingTimestamp()));
    rgw_dir_entry.meta.mtime = ceph::real_clock::from_double(time);
    rgw_dir_entry.meta.etag = _aws_object.GetETag();
    boost::replace_all(rgw_dir_entry.meta.etag,"\"","");
    rgw_dir_entry.meta.size = rgw_dir_entry.meta.accounted_size = _aws_object.GetSize();

    _result.push_back(rgw_dir_entry);
  }

  for (Aws::S3::Model::CommonPrefix const &commonPrefix : list_objects_result.GetCommonPrefixes())
  {
    common_prefixes[commonPrefix.GetPrefix()] = true;
  }

  is_truncated = list_objects_result.GetIsTruncated();

  if (is_truncated)
    next_marker.name = list_objects_result.GetNextContinuationToken();
  
  return 0;
}

/* Region
 * 
 * S3Mirror local ops
 * 
 */
// Reads an object that is locally stored and uploads it to the remote S3 endpoint
int S3Mirror::local_upload_to_remote(string _object_name, string _bucket_name, rgw_user user_id)
{
  int ret = 0;
  
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.SetBucket(_bucket_name);
  put_object_request.SetKey(_object_name);

  const shared_ptr<Aws::IOStream> data = Aws::MakeShared<Aws::StringStream>("putobj", stringstream::in | stringstream::out | stringstream::binary);

  auto _user = this->store->get_user(user_id);
  S3MirrorGetObjRequest req(this->cxt, this->store, std::move(_user), _bucket_name, _object_name, &data);
  ret = do_process_request(this->store, &req);
  if (ret < 0)
  {
    dout(20) << "S3Mirror local_upload_to_remote: " << ret << dendl;
    return ret;
  }

  put_object_request.SetBody(data);

  auto put_object_outcome = this->client.PutObject(put_object_request);

  if (!put_object_outcome.IsSuccess())
  {
    auto err = put_object_outcome.GetError();
    dout(20) << "S3Mirror local_upload_to_remote: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    return -1;
  }

  return 0;
}

// Creates a bucket locally
int S3Mirror::local_create_bucket(string _bucket_name, rgw_user user_id)
{
    auto _user = this->store->get_user(user_id);
    S3MirrorCreateBucketRequest req(this->cxt, this->store, std::move(_user), _bucket_name);
    int ret = do_process_request(this->store, &req);
    if (ret < 0)
    {
      dout(20) << "S3Mirror local_create_bucket: " << ret << dendl;
    }
    return req.get_ret();
}

// Downloads an object from the remote S3 endpoint and writes it locally
int S3Mirror::local_put_object(string _bucket_name, string _object_name, char * object_buffer, 
                              long long object_length, rgw_user user_id) 
{
  auto _user = this->store->get_user(user_id);
  S3MirrorPutObjRequest req(this->cxt, this->store, std::move(_user), _bucket_name, _object_name, object_buffer, object_length);
  int ret = do_process_request(this->store, &req);
  if (ret < 0)
    dout(20) << "S3Mirror local_put_object: " << ret << dendl;
  return ret;
}

/* Region
 * 
 * Async void function finishers for
 * Aws S3 Client
 * 
 */
void S3Mirror::async_put_object_finished(const Aws::S3::S3Client* s3Client, 
              const Aws::S3::Model::PutObjectRequest& request, 
              const Aws::S3::Model::PutObjectOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
  if (!outcome.IsSuccess()) {
    auto err = outcome.GetError();
    dout(20) << "S3Mirror async_put_object_finished: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
  }
}

void S3Mirror::async_complete_multipart_finished(const Aws::S3::S3Client* s3Client, 
              const Aws::S3::Model::CompleteMultipartUploadRequest& request, 
              const Aws::S3::Model::CompleteMultipartUploadOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
  if (!outcome.IsSuccess()) { 
    auto err = outcome.GetError();
    dout(20) << "S3Mirror async_complete_multipart_finished: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
  }
}

void S3Mirror::async_put_part_finished(const Aws::S3::S3Client* s3Client, 
              const Aws::S3::Model::UploadPartRequest& request, 
              const Aws::S3::Model::UploadPartOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
  int ret = 0;
  if (!outcome.IsSuccess()) {
    auto err = outcome.GetError();
    dout(20) << "S3Mirror async_complete_multipart_finished: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
    ret = -1;
  }

  auto mirror = S3Mirror::getInstance();
  if (mirror == nullptr)
    return;

  auto part_upload_task = mirror->sync_map.find<S3MirrorTask>(context->GetUUID());
  part_upload_task->notify(ret);
  mirror->sync_map.remove_task(context->GetUUID());
}

void S3Mirror::async_abort_multipart_finished(const Aws::S3::S3Client* s3Client, 
              const Aws::S3::Model::AbortMultipartUploadRequest& request, 
              const Aws::S3::Model::AbortMultipartUploadOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
  if (!outcome.IsSuccess()) { 
    auto err = outcome.GetError();
    dout(20) << "S3Mirror async_abort_multipart_finished: AWS Error " << 
                err.GetExceptionName() << " " << err.GetMessage() << dendl;
  }
}
