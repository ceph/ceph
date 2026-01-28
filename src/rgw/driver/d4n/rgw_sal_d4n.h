// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#pragma once

#include "rgw_sal_filter.h"
#include "rgw_sal.h"
#include "rgw_role.h"
#include "common/dout.h" 
#include "rgw_aio_throttle.h"
#include "rgw_ssd_driver.h"
#include "rgw_redis_driver.h"
#include "rgw_rest_conn.h"

#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_policy.h"
#include "driver/d4n/d4n_remote_cache_manager.h"

#include <boost/intrusive/list.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <boost/asio/thread_pool.hpp>

#include <fmt/core.h>

namespace rgw::d4n {
  class PolicyDriver;
  class RemoteCachePut;
  class RemoteCachePutBatch;
}

namespace rgw { namespace sal {

inline std::string get_cache_block_prefix(rgw::sal::Object* object, const std::string& version)
{
  return fmt::format("{}{}{}{}{}", url_encode(object->get_bucket()->get_bucket_id(), true), CACHE_DELIM, url_encode(version, true), CACHE_DELIM, url_encode(object->get_name(), true));
}

inline std::string get_key_in_cache(const std::string& prefix, const std::string& offset, const std::string& len)
{
  return fmt::format("{}{}{}{}{}", prefix, CACHE_DELIM, offset, CACHE_DELIM, len);
}

using boost::redis::connection;

class RGWRemoteD4NGetCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:
  bufferlist *in_bl;                                  
  RGWRemoteD4NGetCB(bufferlist* _bl): in_bl(_bl) {}
  int handle_data(bufferlist& bl, bool *pause) override {
    this->in_bl->append(bl);
    return 0;
  }
};

class CoroutinePool {
public:
  using Task = std::function<void(boost::asio::yield_context)>;

  CoroutinePool(boost::asio::any_io_executor executor, size_t pool_size)
      : executor(executor),
        pool_size(pool_size),
        strand(boost::asio::make_strand(executor)),
        running(false)
  {}

  void start(const DoutPrefixProvider *dpp) {
    running = true;
    for (size_t i = 0; i < pool_size; ++i) {
      spawn_worker(dpp, i);
    }
  }

  void stop() {
    running = false;
    cv.notify_all();
  }

  void submit(Task task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        task_queue.push(std::move(task));
        queued_tasks++;
    }
    cv.notify_one();
  }

  struct Stats {
    size_t pool_size;
    size_t queue_size;
    int active_workers;
    int idle_workers;
    int queued_tasks;
  };

  Stats get_stats() const {
    std::lock_guard<std::mutex> lock(queue_mutex);
    return Stats{
      pool_size,
      task_queue.size(),
      active_workers,
      static_cast<int>(pool_size) - active_workers,
      queued_tasks
    };
  }

private:
  void spawn_worker(const DoutPrefixProvider *dpp, size_t worker_id) {
    boost::asio::spawn(
      executor,
      [this, dpp, worker_id](boost::asio::yield_context yield) {
        worker_loop(dpp, worker_id, yield);
      },
      boost::asio::detached);
  }

  void worker_loop(const DoutPrefixProvider *dpp, size_t worker_id, boost::asio::yield_context yield) {
    while (running) {
      Task task;
      {
        std::unique_lock<std::mutex> lock(queue_mutex);

        cv.wait(lock, [&] {
          return !running || !task_queue.empty();
        });

        if (!running && task_queue.empty()) {
          return;
        }

        task = std::move(task_queue.front());
        task_queue.pop();
        queued_tasks--;
      }

      auto task_start = std::chrono::steady_clock::now();
      active_workers++;
      try {
        task(yield);
      } catch (const std::exception& e) {
        ldpp_dout(dpp, 0) << "ERROR: CoroutinePool Worker: " << worker_id
                           << " caught exception: " << e.what() << dendl;
      }
      auto task_duration = std::chrono::steady_clock::now() - task_start;
      active_workers--;
      ldpp_dout(dpp, 20) << "Worker " << worker_id << " task took "
          << std::chrono::duration_cast<std::chrono::milliseconds>(task_duration).count()
          << "ms" << dendl;
    }
  }

  boost::asio::any_io_executor executor;
  size_t pool_size;
  boost::asio::strand<boost::asio::any_io_executor> strand;
  std::queue<Task> task_queue;
  mutable std::mutex queue_mutex;
  std::condition_variable cv;
  std::atomic<bool> running;
  std::atomic<int> active_workers{0};
  std::atomic<int> queued_tasks{0};
};

class D4NFilterDriver : public FilterDriver {
  private:
    std::shared_ptr<connection> conn;
    std::unique_ptr<rgw::cache::CacheDriver> cacheDriver;
    std::unique_ptr<rgw::d4n::ObjectDirectory> objDir;
    std::unique_ptr<rgw::d4n::BlockDirectory> blockDir;
    std::unique_ptr<rgw::d4n::BucketDirectory> bucketDir;
    std::unique_ptr<rgw::d4n::PolicyDriver> policyDriver;
    boost::asio::io_context& io_context;
    optional_yield y;
    std::unique_ptr<boost::asio::thread_pool> d4n_thread_pool;
    std::unique_ptr<CoroutinePool> d4n_coroutine_pool;

    // Redis connection pool
    std::shared_ptr<rgw::d4n::RedisPool> redis_pool;

  public:
    void initialize_pool(const DoutPrefixProvider *dpp,
                        boost::asio::any_io_executor executor,
                               size_t pool_size = 32) {
      if (!d4n_coroutine_pool) {
        d4n_coroutine_pool = std::make_unique<CoroutinePool>(executor, pool_size);
        d4n_coroutine_pool->start(dpp);
      }
    }
    void shutdown_pool() {
      if (d4n_coroutine_pool) {
        d4n_coroutine_pool->stop();
        d4n_coroutine_pool.reset();
      }
    }
    CoroutinePool::Stats get_pool_stats() {
      if (d4n_coroutine_pool) {
        return d4n_coroutine_pool->get_stats();
      }
      return CoroutinePool::Stats{0, 0, 0, 0, 0};
    }

    CoroutinePool* get_pool() {
      return d4n_coroutine_pool.get();
    }

    D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context, bool admin);
    virtual ~D4NFilterDriver();

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<User> get_user(const rgw_user& u) override;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override;
    int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                  std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;
    rgw::cache::CacheDriver* get_cache_driver() { return cacheDriver.get(); }
    rgw::d4n::ObjectDirectory* get_obj_dir() { return objDir.get(); }
    rgw::d4n::BlockDirectory* get_block_dir() { return blockDir.get(); }
    rgw::d4n::BucketDirectory* get_bucket_dir() { return bucketDir.get(); }
    rgw::d4n::PolicyDriver* get_policy_driver() { return policyDriver.get(); }
    void save_y(optional_yield y) { this->y = y; }
    std::shared_ptr<connection> get_conn() { return conn; }
    std::shared_ptr<rgw::d4n::RedisPool> get_redis_pool() { return redis_pool; }
    boost::asio::io_context& get_io_context() { return io_context; }
    void shutdown() override;
    auto get_d4n_executor() {
      return d4n_thread_pool->get_executor();
    }
};

class D4NFilterUser : public FilterUser {
  private:
    D4NFilterDriver* driver;

  public:
    D4NFilterUser(std::unique_ptr<User> _next, D4NFilterDriver* _driver) : 
      FilterUser(std::move(_next)),
      driver(_driver) {}
    virtual ~D4NFilterUser() = default;
};

class D4NFilterBucket : public FilterBucket {
  private:
    struct rgw_bucket_list_entries{
      rgw_obj_key key;
      uint16_t flags;
    };
    D4NFilterDriver* filter;
    bool cache_request{false};

  public:
    D4NFilterBucket(std::unique_ptr<Bucket> _next, D4NFilterDriver* _filter) :
      FilterBucket(std::move(_next)),
      filter(_filter) {}
    virtual ~D4NFilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
    virtual int list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		   ListResults& results, optional_yield y) override;
    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) override;
    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
    void set_cache_request() { cache_request = true; }
};

class D4NFilterObject : public FilterObject {
  private:
    D4NFilterDriver* driver;
    std::string version;
    std::string prefix;
    Attrs attrs_d4n;
    rgw_obj obj;
    rgw::sal::Object* dest_object{nullptr}; //for copy-object
    rgw::sal::Bucket* dest_bucket{nullptr}; //for copy-object
    bool multipart{false};
    bool delete_marker{false};
    bool exists_in_cache{false};
    bool load_from_store{false};
    bool attrs_read_from_cache{false};
    bool cache_request{false};
    bool remote_cache_request{false}; //sent by another rgw
    uint64_t blk_offset;
    uint64_t blk_len;
    uint64_t obj_size; //sent by remote rgw

  public:
    struct D4NFilterReadOp : FilterReadOp {
      public:
	class D4NFilterGetCB: public RGWGetDataCB {
	  private:
	    D4NFilterDriver* filter;
	    D4NFilterObject* source;
	    RGWGetDataCB* client_cb;
	    int64_t start_ofs = 0, len = 0, end_ofs = 0;
      int64_t adjusted_start_ofs{0};
      int64_t adjusted_end_ofs{0};
	    bufferlist bl_rem;
	    bool last_part{false};
	    bool write_to_cache{true};
	    const DoutPrefixProvider* dpp;
	    optional_yield* y;
      int part_num{0}, num_parts{0};
      int len_sent = 0;
      std::vector<rgw::d4n::CacheBlock> blocks, dest_blocks;

	  public:
	    D4NFilterGetCB(D4NFilterDriver* _filter, D4NFilterObject* _source) : filter(_filter),
												        source(_source) {}

	    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
	    void set_client_cb(RGWGetDataCB* client_cb, const DoutPrefixProvider* dpp, optional_yield* y) { 
              this->client_cb = client_cb; 
              this->dpp = dpp;
              this->y = y;
            }
	    void set_start_ofs(uint64_t ofs) { this->start_ofs = ofs; }
      void set_len(uint64_t len) { this->len = len; }
      void set_adjusted_start_ofs(uint64_t adjusted_start_ofs) { this->adjusted_start_ofs = adjusted_start_ofs; }
      void set_part_num(uint64_t part_num) { this->part_num = part_num; }
      void set_num_parts(uint64_t num_parts) { this->num_parts = num_parts; }
	    int flush_last_part();
	    void bypass_cache_write() { this->write_to_cache = false; }
	};

	D4NFilterObject* source;

	D4NFilterReadOp(std::unique_ptr<ReadOp> _next, D4NFilterObject* _source) : FilterReadOp(std::move(_next)),
										   source(_source) 
        {
          cb = std::make_unique<D4NFilterGetCB>(source->driver, source);
	}
	virtual ~D4NFilterReadOp() = default;

  int getRemote(const DoutPrefixProvider* dpp, long long start, long long end, std::string key, std::string remoteCacheAddress, bufferlist *bl, optional_yield y);
  int remoteFlush(const DoutPrefixProvider* dpp, bufferlist bl, uint64_t ofs, uint64_t len, uint64_t read_ofs, std::string creationTime, optional_yield y);

	virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
	virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			     RGWGetDataCB* cb, optional_yield y) override;
	virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			      bufferlist& dest, optional_yield y) override;

      private:
	RGWGetDataCB* client_cb;
	std::unique_ptr<D4NFilterGetCB> cb;
        std::unique_ptr<rgw::Aio> aio;
	uint64_t offset = 0; // next offset to write to client
        rgw::AioResultList completed; // completed read results, sorted by offset
	std::unordered_map<uint64_t, std::pair<uint64_t,uint64_t>> blocks_info;
  std::map<off_t, std::tuple<off_t, bool, bufferlist>> read_data;
  bool last_part_done = false;
  uint64_t last_adjusted_ofs = -1;
  bool first_block = true; //is it first_block

  void set_first_block(bool val) {first_block = val;}
	int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y);
	void cancel();
	int drain(const DoutPrefixProvider* dpp, optional_yield y);
    };

    struct D4NFilterDeleteOp : FilterDeleteOp {
      D4NFilterObject* source;

      D4NFilterDeleteOp(std::unique_ptr<DeleteOp> _next, D4NFilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~D4NFilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
    };

    D4NFilterObject(std::unique_ptr<Object> _next, D4NFilterDriver* _driver) : FilterObject(std::move(_next)),
									      driver(_driver) {}
    D4NFilterObject(std::unique_ptr<Object> _next, Bucket* _bucket, D4NFilterDriver* _driver) : FilterObject(std::move(_next), _bucket),
											       driver(_driver) {}
    D4NFilterObject(D4NFilterObject& _o, D4NFilterDriver* _driver) : FilterObject(_o),
								    driver(_driver) {}
    virtual ~D4NFilterObject() = default;

    virtual int copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              rgw::sal::DataProcessorFactory* dp_factory,
                              const DoutPrefixProvider* dpp,
                              optional_yield y) override;

    virtual const std::string &get_name() const override { return next->get_name(); }
    virtual int load_obj_state(const DoutPrefixProvider *dpp, optional_yield y,
                             bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,
			       uint32_t flags = rgw::sal::FLAG_LOG_OP) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y) override;
    virtual ceph::real_time get_mtime(void) const override { return next->get_mtime(); };

    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;

    void set_object_version(const std::string& version) { this->version = version; }
    const std::string get_object_version() { return this->version; }

    void set_prefix(const std::string& prefix) { this->prefix = prefix; }
    const std::string get_prefix() { return this->prefix; }
    void set_object_attrs(Attrs attrs) { this->attrs_d4n = attrs; }
    Attrs get_object_attrs() { return this->attrs_d4n; }
    int get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y);
    void set_attrs_from_obj_state(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool dirty = false);
    int calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, rgw::sal::Attrs& attrs);
    int set_head_obj_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, bool is_latest_version = true, bool dirty = false);
    int update_head_block_hostslist(const DoutPrefixProvider* dpp, optional_yield y);
    int set_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    int delete_data_block_cache_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    bool check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, rgw::d4n::CacheBlock& blk, optional_yield y);
    rgw::sal::Bucket* get_destination_bucket(const DoutPrefixProvider* dpp) { return dest_bucket;}
    rgw::sal::Object* get_destination_object(const DoutPrefixProvider* dpp) { return dest_object; }
    bool is_multipart() { return multipart; }
    int set_attr_crypt_parts(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs);
    int create_delete_marker(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_delete_marker() { return delete_marker; }
    bool exists(void) override { if (exists_in_cache) { return true;} return next->exists(); };
    bool load_obj_from_store() { return load_from_store; }
    void set_load_obj_from_store(bool load_from_store) { this->load_from_store = load_from_store; }
    void set_cache_request() { cache_request = true; }
    bool is_cache_request() { return cache_request; }
    //the following are helper methods to get/set from/to cache
    bool is_remote_cache_request() { return remote_cache_request; }
    void set_remote_cache_request() { remote_cache_request = true; }
    void set_block_offset(uint64_t offset) { blk_offset = offset; }
    void set_block_len(uint64_t len) { blk_len = len; }
    uint64_t get_remote_block_offset() { return blk_offset; }
    uint64_t get_remote_block_len() { return blk_len; }
    void set_remote_obj_size(uint64_t size) { obj_size = size; }
    uint64_t get_remote_obj_size() { return obj_size; }
    bool is_remote_head_block_request() { return (remote_cache_request && (blk_offset == 0) && (blk_len == 0)); }
};

class D4NFilterDPP : public DoutPrefixProvider {
  CephContext* cct;
public:
  explicit D4NFilterDPP(CephContext* c) : cct(c) {}
  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const override {
    out << "write_to_remote_cache: ";
    return out;
  }
};

class D4NFilterWriter : public FilterWriter {
  private:
    D4NFilterDriver* driver; 
    D4NFilterObject* object;
    const DoutPrefixProvider* dpp;
    bool atomic;
    optional_yield y;
    bool d4n_writecache;
    std::string version;
    std::string prev_oid_in_cache;
    std::vector<std::unique_ptr<rgw::d4n::RemoteCachePut>> requests;

    static void write_to_remote_cache(const DoutPrefixProvider* dpp_o, const std::string& prefix, uint64_t size, const rgw_user& user, const std::string& remote_addr, const std::string& bucket_name, const std::string& oid, const std::string& version, D4NFilterDriver* driver, optional_yield y);

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, optional_yield _y) : FilterWriter(std::move(_next), _obj),
							     driver(_driver),
							     dpp(_dpp), atomic(false), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, bool _atomic, optional_yield _y) : FilterWriter(std::move(_next), _obj),
									   driver(_driver),
									   dpp(_dpp), atomic(_atomic), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    virtual ~D4NFilterWriter() = default;

    virtual int prepare(optional_yield y);
    virtual int process(bufferlist&& data, uint64_t offset) override;
    virtual int complete(size_t accounted_size, const std::string& etag,
			 ceph::real_time *mtime, ceph::real_time set_mtime,
			 std::map<std::string, bufferlist>& attrs,
			 const std::optional<rgw::cksum::Cksum>& cksum,
			 ceph::real_time delete_at,
			 const char *if_match, const char *if_nomatch,
			 const std::string *user_data,
			 rgw_zone_set *zones_trace, bool *canceled,
			 const req_context& rctx,
			 uint32_t flags) override;
   bool is_atomic() { return atomic; };
   const DoutPrefixProvider* get_dpp() { return this->dpp; } 
   void set_cache_request() { object->set_cache_request(); }
   void set_remote_cache_request() { object->set_remote_cache_request(); }
};

class D4NGetObjectCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:                                                     
  bufferlist *in_bl;
  D4NGetObjectCB(bufferlist* _bl): in_bl(_bl) {}
  int handle_data(bufferlist& bl, bool *pause) override {
    this->in_bl->append(bl);
    return 0;
  }
};

class D4NFilterMultipartUpload : public FilterMultipartUpload {
private:
  D4NFilterDriver* driver;
public:
  D4NFilterMultipartUpload(std::unique_ptr<MultipartUpload> _next, Bucket* _b, D4NFilterDriver* driver) :
    FilterMultipartUpload(std::move(_next), _b),
    driver(driver) {}
  virtual ~D4NFilterMultipartUpload() = default;

  virtual int complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
            prefix_map_t& processed_prefixes,
            const char *if_match = nullptr,
            const char *if_nomatch = nullptr) override;
};

} } // namespace rgw::sal
