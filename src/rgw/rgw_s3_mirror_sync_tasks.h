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

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <map>

#include "rgw_sal.h"
#include "common/ceph_time.h"

#include <aws/core/Aws.h>

// Base S3MirrorTask that is used only for synchronization between 
// other tasks
class S3MirrorTask
{
private:
  std::mutex task_mutex;
  std::condition_variable condVar;

public:
    bool completed = false;
    int ret = 0;
    
    S3MirrorTask() {};
    ~S3MirrorTask() {};

    int wait() {
        std::unique_lock<std::mutex> lck(task_mutex);
        condVar.wait(lck, [this]{ return this->completed; });
        return ret;
    }

    int waitfor(std::chrono::milliseconds dur, bool& cv_status) {
        std::unique_lock<std::mutex> lck(task_mutex);
        cv_status = condVar.wait_for(lck, dur, [this]{ return this->completed; });
        return ret;
    }

    void notify(int ret) {
        {
            std::lock_guard<std::mutex> lck(task_mutex);
            completed = true;
            this->ret = ret;
        }
        condVar.notify_all();
    }
};

// S3MirrorDownloadTask inherits S3MirrorTask and carries further variables 
// that regard downloading an object through AWS S3 client
class S3MirrorDownloadTask : public S3MirrorTask, public std::enable_shared_from_this<S3MirrorTask>
{
public:
  class CustomStreamBuffer : public std::streambuf
  {
    private:
      std::unique_ptr<char[]> bodybuffer;
      long long length =0;
    public:
      CustomStreamBuffer(long long size) 
      {
        
        bodybuffer.reset(new char[size]);

        this->pubsetbuf(&(bodybuffer[0]), size);
        this->setg(0, 0, 0);
        this->setp(&(bodybuffer[0]), &(bodybuffer[size-1]));
      }
      
      streamsize xsputn (const char* s, streamsize n) override {
        memcpy(&(bodybuffer[length]), s, n);
        length += n;
        return n;
      }

      char *get_buffer() {
        return &(bodybuffer[0]);
      }

      long long get_unsafe_length()
      {
        return length;
      }
  };
  // necessary to write the object locally
  rgw_user user_id;
  // object hotness ref counter
  std::atomic<int> refs = 0;
  // vector with result to keep the iostream alive
  std::vector<Aws::S3::Model::GetObjectResult> result;
  std::shared_ptr<CustomStreamBuffer> customStreamBuf = nullptr;
  std::basic_iostream<char, std::char_traits<char>>  *ioStream;
  // S3 object properties to dump the headers
  ceph::time_detail::real_clock::time_point expires;
  ceph::time_detail::real_clock::time_point last_modified;
  string etag = "";
  string cache_control = "";
  string content_disposition = "";
  string content_encoding = "";
  string content_language = "";
  string content_type = "";
  long long content_length = 0;
};

// S3MirrorSyncMap is used for searching existing S3MirrorTasks so 
// future S3MirrorTasks can wait for them to finish 
class S3MirrorSyncMap 
{
private:
    std::shared_mutex download_lock;
    std::map<string, std::shared_ptr<S3MirrorTask>> download_map;

public:
    template<typename T>
    std::shared_ptr<T> find_or_create_task(string taskName, bool &existed)
    {
        std::unique_lock wl(download_lock);
        auto iter = download_map.find(taskName);
        if (iter != download_map.end())
        { 
            existed = true;
            return std::static_pointer_cast<T>(iter->second);
        }
        existed = false;
        auto tmpTask = std::make_shared<T>();
        auto pair = download_map.insert(std::make_pair(taskName, std::move(tmpTask)));
        existed = false;
        return std::static_pointer_cast<T>(pair.first->second);
    }

    template<typename T>
    std::shared_ptr<T> find(string taskName)
    {
        std::shared_lock rl(download_lock);
        auto iter = download_map.find(taskName);
        if (iter == download_map.end())
        { 
            return nullptr;
        }

        return std::static_pointer_cast<T>(iter->second);
    }

    template<typename T>
    std::shared_ptr<T> findPrefix(string prefix)
    {
        std::shared_lock rl(download_lock);
        auto iter = download_map.lower_bound(prefix);
        if (iter == download_map.end())
        { 
            return nullptr;
        }

        return std::static_pointer_cast<T>(iter->second);
    }

    void remove_task(string taskName) 
    {
        std::unique_lock wl(download_lock);
        std::map<string, std::shared_ptr<S3MirrorTask>>::iterator iter = download_map.find(taskName);
        if (iter != download_map.end())
        { 
            download_map.erase(iter);
        }
    }

};