// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
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
#include "common/split.h"
#include "rgw_aio_throttle.h"
#include "rgw_ssd_driver.h"
#include "rgw_redis_driver.h"
#include "rgw_rest_conn.h"

#include "driver/d4n/d4n_backend.h"
#include "driver/d4n/d4n_policy.h"
#include "driver/d4n/d4n_remote_cache_manager.h"

#include <boost/intrusive/list.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/compat/function_ref.hpp>

#include <array>
#include <charconv>
#include <cstdint>
#include <expected>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <source_location>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

#include <fmt/core.h>

namespace rgw::d4n {
  class CachePolicy;
  class PolicyDriver;
  class RemoteCacheOp;
  class RemoteCacheDeleteOp;
  class RemoteCachePutOp;
  class RemoteCachePutBatch;
}

namespace rgw::sal {

inline std::string get_cache_block_prefix(const std::string& bucket_id, const std::string& object_name, const std::string& version)
{
  return fmt::format("{}{}{}{}{}", url_encode(bucket_id, true), CACHE_DELIM, url_encode(version, true), CACHE_DELIM, url_encode(object_name, true));
}

inline std::string get_cache_block_prefix(rgw::sal::Object* object, const std::string& version)
{
  return get_cache_block_prefix(object->get_bucket()->get_bucket_id(), object->get_name(), version);
}

inline std::string get_key_in_cache(const std::string& prefix, const std::string& offset, const std::string& len)
{
  return fmt::format("{}{}{}{}{}", prefix, CACHE_DELIM, offset, CACHE_DELIM, len);
}

inline std::string get_latest_head_block_name(const std::string& obj_name)
{
  return obj_name.starts_with('_') ? "_" + obj_name : obj_name;
}

// Builds the objName for a version-specific head block directory entry used for non-versioned
// bucket objects. Format: "_:<d4n_version>_<obj_name>". Allows remote GETs to look up attrs
// for a specific version without relying on the overwriteable "latest" head block.
inline std::string get_versioned_head_block_name(const std::string& version, const std::string& obj_name)
{
  return "_:" + version + "_" + obj_name;
}

// Lease TTL for GET operations to prevent concurrent deletion (in nanoseconds)
// 30 seconds = 30,000,000,000 nanoseconds
constexpr uint64_t D4N_LEASE_TTL_NANOSECONDS = 30'000'000'000ULL;

// Builds a unique lease resource key for preventing concurrent GET/PUT conflicts.
// Format: "<bucket_id>:<object_name>:<version>:<operation>:<uuid>"
inline std::string get_lease_resource_key(const std::string& bucket_id,
                                          const std::string& object_name,
                                          const std::string& version,
                                          const std::string& operation,
                                          const std::string& uuid)
{
  // Hierarchical format using '/' separator for prefix matching
  // Each component is URL-encoded (including '/') to handle '/' in S3 object names
  // e.g., "bucket123/photos%2F2024%2Fimg.jpg/v1/GET/uuid"
  return fmt::format("{}/{}/{}/{}/{}",
                     url_encode(bucket_id, true),
                     url_encode(object_name, true),
                     url_encode(version, true),
                     url_encode(operation, true),
                     url_encode(uuid, true));
}

// Builds a lease resource prefix for checking any active leases on an object version
// for a specific operation type.
// Hierarchical format using '/' separator for prefix matching
// e.g., "bucket/object/version/GET" matches all GETs on that version (wildcard on uuid)
inline std::string get_lease_resource_prefix(const std::string& bucket_id,
                                             const std::string& object_name,
                                             const std::string& version,
                                             const std::string& operation)
{
  return fmt::format("{}/{}/{}/{}",
                     url_encode(bucket_id, true),
                     url_encode(object_name, true),
                     url_encode(version, true),
                     url_encode(operation, true));
}

inline std::optional<rgw::d4n::CacheBlock> parse_block_from_cache(const std::string& key)
{
  std::array<std::string_view, 5> fields;
  auto position = fields.begin();
  for (auto part : std::string_view{key} | std::views::split('#')) {
    if (fields.end() == position) {
      return std::nullopt;
    }
    *position++ = std::string_view{part};
  }
  if (fields.end() != position) {
    return std::nullopt;
  }

  const auto read_number = [](const std::string_view field, std::uint64_t& value) {
    const auto end = field.data() + field.size();
    const auto parsed = std::from_chars(field.data(), end, value);

    return std::errc{} == parsed.ec && end == parsed.ptr;
  };

  rgw::d4n::CacheBlock block{};
  if (!read_number(fields[3], block.blockID) || !read_number(fields[4], block.size)) {
    return std::nullopt;
  }

  block.cacheObj.bucketName = url_decode(fields[0]);
  block.version = url_decode(fields[1]);
  block.cacheObj.objName = url_decode(fields[2]);

  return block;
}

namespace lfdb = ceph::libfdb;

using boost::redis::connection;
using fdbase= lfdb::database;

class RGWRemoteD4NGetCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:
  bufferlist *in_bl;
  RGWRemoteD4NGetCB(bufferlist* _bl): in_bl(_bl) {}
  int handle_data(bufferlist& bl, bool *pause) override {
    this->in_bl->append(bl);
    return 0;
  }
};

struct TaskGroup {
  std::atomic<int> in_flight{0};
  std::atomic<bool> timer_set{false};
  boost::asio::steady_timer signal_timer;

  TaskGroup(boost::asio::any_io_executor ex) : signal_timer(ex) {
    signal_timer.expires_at(std::chrono::steady_clock::time_point::max());
  }

  void wait(const DoutPrefixProvider *dpp, boost::asio::yield_context yield) {
    ldpp_dout(dpp, 0) << "DEBUG: wait() starting, in_flight=" << in_flight << dendl;
    timer_set.store(true, std::memory_order_release);
    while (in_flight.load(std::memory_order_acquire) > 0) {
      signal_timer.expires_at(std::chrono::steady_clock::time_point::max());
      boost::system::error_code ec;
      signal_timer.async_wait(yield[ec]);

      if (ec == boost::asio::error::operation_aborted) {
        continue;
      }

      if (ec) {
        throw boost::system::system_error(ec);
      }
    }
    ldpp_dout(dpp, 0) << "DEBUG: wait() exiting, in_flight=" << in_flight << dendl;
    timer_set.store(false, std::memory_order_release);
  }
};

class CoroutinePool {
public:
  using Task = std::move_only_function<void(boost::asio::yield_context)>;

  CoroutinePool(boost::asio::any_io_executor executor, size_t pool_size, bool on_strand=false)
      : executor(executor),
        pool_size(pool_size),
        strand(boost::asio::make_strand(executor)),
        running(false),
        on_strand(on_strand),
        // A concurrent_channel is internally synchronized, so producers (submit) and
        // consumers (workers) can use it from different threads with no strand. Delivery
        // is buffered/level-triggered: a task sent while no worker is waiting is retained
        // and picked up by the next async_receive. This removes both the data race and the
        // lost-wakeup that the shared steady_timer (used as a condition variable) had.
        // An effectively-unbounded capacity preserves the previous non-blocking submit().
        channel(executor, std::numeric_limits<std::size_t>::max())
  {}

  void start(const DoutPrefixProvider *dpp) {
    running = true;
    for (size_t i = 0; i < pool_size; ++i) {
      spawn_worker(dpp, i);
    }
  }

  void stop() {
    running = false;
    // Closing the channel completes every pending async_receive with channel_closed,
    // so parked workers wake and exit cleanly.
    channel.close();
  }

  void submit(Task task) {
    // try_send never blocks; with the max capacity above it only fails once the channel
    // has been closed by stop(), in which case the task is dropped.
    if (channel.try_send(boost::system::error_code{}, std::move(task))) {
      queued_tasks++;
    }
  }

  struct Stats {
    size_t pool_size   = 0;
    size_t queue_size  = 0;
    int active_workers = 0;
    int idle_workers   = 0;
    int queued_tasks   = 0;

    Stats() = default;

    explicit Stats(const CoroutinePool& pool) {
      pool_size      = pool.pool_size;
      active_workers = pool.active_workers.load();
      idle_workers   = static_cast<int>(pool.pool_size) - active_workers;
      queued_tasks   = pool.queued_tasks.load();
      queue_size     = queued_tasks < 0 ? 0 : static_cast<size_t>(queued_tasks);
    }
  };

  Stats get_stats() const { return Stats{*this}; }

private:
  void spawn_worker(const DoutPrefixProvider *dpp, size_t worker_id) {
    auto context = on_strand ? strand : executor;
    boost::asio::spawn(
      context,
      [this, dpp, worker_id](boost::asio::yield_context yield) {
        worker_loop(dpp, worker_id, yield);
      },
      boost::asio::detached);
  }

  void worker_loop(const DoutPrefixProvider *dpp, size_t worker_id, boost::asio::yield_context yield) {
    while (running) {
      boost::system::error_code ec;
      ldpp_dout(dpp, 20) << "Worker " << worker_id << " waiting for task" << dendl;
      // Blocks (suspends) the coroutine until a task arrives or the channel is closed.
      // No mutex is held across this suspension; the channel handles synchronization.
      Task task = channel.async_receive(yield[ec]);
      if (ec) {
        // channel_closed (stop() called) or operation aborted: exit the worker.
        return;
      }
      queued_tasks--;

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
  std::atomic<bool> running;
  std::atomic<int> active_workers{0};
  std::atomic<int> queued_tasks{0};
  bool on_strand{false};
  boost::asio::experimental::concurrent_channel<void(boost::system::error_code, Task)> channel;
};

class D4NFilterDriver : public FilterDriver {
  private:
    std::shared_ptr<rgw::d4n::Backend> backend;

    std::unique_ptr<rgw::cache::CacheDriver> cacheDriver;
    std::unique_ptr<rgw::d4n::PolicyDriver> policyDriver;
    boost::asio::io_context& io_context;
    optional_yield y;

    std::unique_ptr<boost::asio::thread_pool> d4n_thread_pool;
    std::unique_ptr<CoroutinePool> d4n_coroutine_pool;
    std::unique_ptr<CoroutinePool> d4n_coroutine_get_pool;


  public:
    void initialize_pool(const DoutPrefixProvider *dpp,
                        boost::asio::any_io_executor executor,
                               size_t pool_size = 32) {
      if (!d4n_coroutine_pool) {
        d4n_coroutine_pool = std::make_unique<CoroutinePool>(executor, pool_size);
        d4n_coroutine_pool->start(dpp);
      }
    }

    void initialize_get_pool(const DoutPrefixProvider *dpp,
                        boost::asio::any_io_executor executor,
                               size_t pool_size = 32) {
      if (!d4n_coroutine_get_pool) {
        d4n_coroutine_get_pool = std::make_unique<CoroutinePool>(executor, pool_size);
        d4n_coroutine_get_pool->start(dpp);
      }
    }
    void shutdown_pool() {
      if (d4n_coroutine_pool) {
        d4n_coroutine_pool->stop();
        d4n_coroutine_pool.reset();
      }
      if (d4n_coroutine_get_pool) {
        d4n_coroutine_get_pool->stop();
        d4n_coroutine_get_pool.reset();
      }
    }
    CoroutinePool::Stats get_pool_stats() {
      if (d4n_coroutine_pool) {
        return d4n_coroutine_pool->get_stats();
      }
      return CoroutinePool::Stats{};
    }

    CoroutinePool* get_pool() {
      return d4n_coroutine_pool.get();
    }

    CoroutinePool* get_d4n_get_pool() {
      return d4n_coroutine_get_pool.get();
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
    rgw::d4n::Directory* get_dir();
    rgw::d4n::ObjectDirectory* get_obj_dir();
    rgw::d4n::BlockDirectory* get_block_dir();
    rgw::d4n::BucketDirectory* get_bucket_dir();
    rgw::d4n::PolicyDriver* get_policy_driver() { return policyDriver.get(); }
    rgw::d4n::Lease* get_lease();
    void save_y(optional_yield y) { this->y = y; }
    rgw::d4n::TransactionResult retry_transaction(const DoutPrefixProvider *dpp, optional_yield y,
        boost::compat::function_ref<int(rgw::d4n::Transaction&)> fn, unsigned max_attempts = 5);
    int execute_transaction(const DoutPrefixProvider *dpp, optional_yield y,
        boost::compat::function_ref<int(rgw::d4n::Transaction&)> body, unsigned max_attempts = 5,
        std::source_location whence = std::source_location::current());

    int populate_listing_metadata(const DoutPrefixProvider *dpp, const std::string& bucket_id,
                                  std::vector<rgw_bucket_dir_entry>& entries, optional_yield y);

    rgw::d4n::Pipeline *make_pipeline(std::optional<rgw::d4n::Pipeline>& pipeline);

    boost::asio::io_context& get_io_context() { return io_context; }

    bool uses_fdb() const noexcept;
    bool uses_redis() const noexcept;

    void shutdown() override;
    void finalize() override;
    auto get_d4n_executor() {
      return d4n_thread_pool->get_executor();
    }
    Driver* get_next() { return next; }
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
    bool return_blocks{false}; // indicates whether dir_blocks should be populated
    std::unordered_map<std::string, rgw::d4n::CacheBlock> dir_blocks; // for use in bucket removal

    struct FetchContext {
      std::vector<rgw::d4n::CacheObject> objects;
      std::string continuation_token;
    };

    struct ListingBatch
    {
     ListResults results;
     std::string continuation_token;
    };

    [[nodiscard]] std::expected<FetchContext, int> fetch_objects_batch(const DoutPrefixProvider* dpp, const ListParams& params, int batch_size,
                            const std::string& cursor_or_start, bool is_first_batch,
                            std::optional<rgw::d4n::Transaction> txn, optional_yield y);

    int build_versioned_entries(const DoutPrefixProvider* dpp, const rgw::d4n::CacheObject& obj,
                              const ListParams& params, std::vector<rgw_bucket_dir_entry>& entries,
                              std::string& last_version, int& num_objs, bool& object_exhausted, int max,
                              std::optional<rgw::d4n::Transaction> txn, optional_yield y);

    [[nodiscard]] std::expected<ListResults, int> process_objects_batch(const DoutPrefixProvider* dpp, const std::vector<rgw::d4n::CacheObject>& input_objects,
                              const ListParams& params, const std::map<std::string, bool>& known_prefixes, int max, bool is_truncated,
                              std::optional<rgw::d4n::Transaction> txn, optional_yield y);

    [[nodiscard]] std::expected<ListingBatch, int> read_listing_batch(
        const DoutPrefixProvider *dpp, const ListParams& params, int max,
        const std::string& cursor_or_start, bool is_first_batch,
        const ListResults& committed, optional_yield y);

    void merge_results(const DoutPrefixProvider* dpp, const ListParams& params,
                        ListResults& cache_results, ListResults& store_results,
                        int max, ListResults& results);

  public:
    D4NFilterBucket(std::unique_ptr<Bucket> _next, D4NFilterDriver* _filter) :
      FilterBucket(std::move(_next)),
      filter(_filter) {}
    virtual ~D4NFilterBucket() = default;
   
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
    
    virtual int list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		   ListResults& results, optional_yield y) override;
    virtual int remove(const DoutPrefixProvider* dpp, bool delete_children,
		       optional_yield y) override;
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
    
    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) override;
    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
    virtual void set_cache_request() override { cache_request = true; }
};

struct BlockMeta {
  uint64_t id;
  uint64_t offset;
  uint64_t len;
  uint64_t read_offset;
  uint64_t read_len;
  bool is_remote{false};
};

struct RemoteTaskResult {
  std::vector<BlockMeta> blocks;
  rgw::AioResultList result_list;
};

class D4NFilterObject : public FilterObject {
  private:
    struct held_lease final
    {
     std::string resource;
     std::string holder;
     std::string token;
    };

    // Publishing confirmed acquisition must not allocate or throw.
    static_assert(std::is_nothrow_move_constructible_v<held_lease>);

    D4NFilterDriver* driver;
    std::string version;
    std::string old_version;  // Previous version to invalidate (for PUT overwrites in non-versioned buckets)
    bool remote_dirty;
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
    bool head_dirty{false}; //the HEAD read from the directory is dirty: its version may not be in the backend yet
    bool cache_request{false};
    bool remote_cache_request{false}; //sent by another rgw
    std::string delete_marker_version; //version of a delete marker to replicate on the remote rgw
    std::optional<ceph::real_time> delete_marker_mtime; //mtime of that delete marker, as set by the local rgw
    std::optional<ceph::real_time> remote_mtime; //mtime of the object in a remote PUT, as set by the local rgw
    std::optional<held_lease> read_lease; // Present after successful GET lease acquisition.
    uint64_t blk_offset = 0; //sent by remote rgw
    uint64_t blk_len = 0;    //sent by remote rgw; 0 for a head put
    uint64_t obj_size = 0; //sent by remote rgw
    bool block_only = false; // sent by remote rgw; used in PUT op for an evicted block

  private:
    // Query without changing SAL state or acquiring a read lease; borrowed results are working data.
    [[nodiscard]] std::expected<std::optional<rgw::d4n::CacheBlock>, int>
    lookup_cached_head(const DoutPrefixProvider *dpp, optional_yield y,
                       std::optional<rgw::d4n::Transaction> txn = std::nullopt);

    // Install readable metadata while preserving the requested instance.
    int load_cached_head(const DoutPrefixProvider *dpp, rgw::d4n::CacheBlock&& head);

    void resolve_cached_instance();

    // Keep lookup state from a failed attempt out of later attribute operations.
    rgw::d4n::TransactionResult retry_attribute_transaction(const DoutPrefixProvider *dpp,
        optional_yield y, boost::compat::function_ref<int(rgw::d4n::Transaction&)> fn);

  public:
    struct D4NFilterReadOp : FilterReadOp {
      public:
	class D4NFilterGetCB: public RGWGetDataCB {
	  private:
	    D4NFilterDriver* filter;
	    D4NFilterObject* source;
            RGWGetDataCB* client_cb{nullptr};
            std::uint64_t start_ofs{0}, len{0}, len_sent{0}, adjusted_start_ofs{0};
	    bufferlist bl_rem;
	    bool write_to_cache{true};
            const DoutPrefixProvider* dpp{nullptr};
            optional_yield* y{nullptr};
      std::vector<rgw::d4n::CacheBlock> blocks; // source blocks cached by this read, for the directory

      /* What the cache writes need, the same for every chunk of a read: set up on the first chunk. */
      struct CacheWriteState {
        rgw::d4n::CachePolicy* policy{nullptr};
        std::string version;            // source version (data block keys and directory entries)
        std::string prefix;             // source cache block prefix
        rgw_owner owner;
        std::string bucket_name;
        rgw::d4n::CacheBlock block;     // template for the source data block entries
        bool copy_dest{false};          // write-back copy: also write the destination blocks
        std::string dest_version;
        std::string dest_prefix;
        rgw_owner dest_owner;
        std::string dest_bucket_name;
        Attrs dest_attrs;
      };
      std::optional<CacheWriteState> cache_state;

      void init_cache_write_state();
      int cache_chunk(const bufferlist& data);

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
      void set_len(uint64_t len) { this->len = len; len_sent = 0; }
      void set_adjusted_start_ofs(uint64_t adjusted_start_ofs) { this->adjusted_start_ofs = adjusted_start_ofs; }
	    int flush_last_part();
	    void bypass_cache_write() { this->write_to_cache = false; }
	};

	D4NFilterObject* source;

	D4NFilterReadOp(std::unique_ptr<ReadOp> _next, D4NFilterObject* _source) : FilterReadOp(std::move(_next)),
										   source(_source) 
        {
          cb = std::make_unique<D4NFilterGetCB>(source->driver, source);
	}
	virtual ~D4NFilterReadOp();

	virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
	virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			     RGWGetDataCB* cb, optional_yield y) override;
	virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			      bufferlist& dest, optional_yield y) override;

      private:
	RGWGetDataCB* client_cb;
	std::unique_ptr<D4NFilterGetCB> cb;
        std::shared_ptr<rgw::Aio> aio;
	uint64_t offset = 0; // next offset to write to client
        rgw::AioResultList completed; // completed read results, sorted by offset
        std::map<uint64_t, std::unique_ptr<rgw::AioResultEntry>> completed_map;
	std::unordered_map<uint64_t, BlockMeta> blocks_info;
  std::mutex data_mutex;
  std::vector<std::shared_ptr<RemoteTaskResult>> remote_task_results;

	int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y);
	void cancel();
  int process_remote_results(const DoutPrefixProvider* dpp, std::shared_ptr<TaskGroup> group, optional_yield y);
	int drain(const DoutPrefixProvider* dpp, optional_yield y);
    };

    struct D4NFilterDeleteOp : FilterDeleteOp {
      D4NFilterObject* source;

      D4NFilterDeleteOp(std::unique_ptr<DeleteOp> _next, D4NFilterObject* _source) : FilterDeleteOp(std::move(_next)),
										     source(_source) {}
      virtual ~D4NFilterDeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;

    private:
      int update_directory_entries(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    rgw::d4n::Transaction& txn,
                                    bool objDirty,
                                    const std::string& version,
                                    const std::string& objName,
                                    rgw::d4n::CacheBlock& block);

      // Suspended-versioning simple delete of a clean object: remove the cached null version's
      // HEADs ("_:null_<name>" and its "_:<version>_<name>" alias), which the backend replaces
      // with a null delete marker.
      int drop_clean_null_head(const DoutPrefixProvider* dpp, optional_yield y, rgw::d4n::Transaction& txn);

      int send_remote_delete(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            const std::string& version,
                            bool objDirty,
                            const std::string& dm_version,
                            ceph::real_time dm_mtime = {});
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
    void set_old_version(const std::string& old_version) { this->old_version = old_version; }
    const std::string get_old_version() { return this->old_version; }

    void set_prefix(const std::string& prefix) { this->prefix = prefix; }
    const std::string get_prefix() { return this->prefix; }
    void set_object_attrs(Attrs attrs) { this->attrs_d4n = attrs; }
    Attrs get_object_attrs() { return this->attrs_d4n; }
    void set_attrs_from_obj_state(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool dirty = false);
    int calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, rgw::sal::Attrs& attrs);
    int set_head_block_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool is_latest_version = true, bool dirty = false, std::optional<rgw::d4n::Transaction> txn = std::nullopt);
    int set_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    /* Adds the local RGW to the hostsList of the given data block entries and sets their version, in one
       transaction (one replay on an uncertain commit). dirty == nullopt leaves the dirty flag of existing
       entries as is; new entries are created clean. */
    int update_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y,
                                      const std::vector<rgw::d4n::CacheBlock>& blocks,
                                      const std::string& version, std::optional<bool> dirty,
                                      const char* component = "D4NFilterObject");
    /* The commit point of a write of this object's version (PUT, copy): the HEAD transaction. With
       read_current, the current state is read in the transaction (latest version rule of versioned
       buckets, the precondition of a conditional PUT, and the "null" version this write replaces,
       tombstoned if dirty). Returns 0 once committed, with old_version set to the replaced version
       (empty if none); a precondition error; or another error (-EIO for an uncertain commit that
       could not be confirmed). */
    int commit_head_block_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, bool dirty,
                                    bool read_current, const char* if_match, const char* if_nomatch,
                                    const char* component, std::string& old_version);
    /* Replication of a write-back copy (called on the source, for its dest_object), in sync mode
       (rgw_d4n_remote_put without rgw_d4n_async_remote_put), as D4NFilterWriter does for a PUT: each
       destination block before the HEAD commit, the head put after it. No-ops otherwise. */
    int replicate_copy_block(const DoutPrefixProvider* dpp, optional_yield y, const bufferlist& data, uint64_t ofs);
    int replicate_copy_head(const DoutPrefixProvider* dpp, optional_yield y, uint64_t obj_size, const std::string& old_version);
    int delete_data_block_cache_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty = false);
    // Positive: live head; zero: cache miss or delete marker (returned in blk); negative: error/local tombstone.
    int check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::d4n::CacheBlock& blk, optional_yield y, std::optional<rgw::d4n::Transaction> txn = std::nullopt);
    rgw::sal::Bucket* get_destination_bucket(const DoutPrefixProvider* dpp) { return dest_bucket;}
    rgw::sal::Object* get_destination_object(const DoutPrefixProvider* dpp) { return dest_object; }
    bool is_multipart() { return multipart; }
    int set_attr_crypt_parts(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs);
    /* Delete marker creation is split so that the directory part can be replayed by
       retry_transaction() without side effects:
         make_delete_marker_spec()   - decide version/instance/mtime/attrs once (no I/O, leaves `this` unchanged)
         put_delete_marker_head()    - write the 0-byte head to the local cache (no policy update)
         stage_delete_marker_dir()   - directory writes only; safe to run on every attempt
         track_delete_marker()       - register with the cache policy; call once, after commit
         remove_delete_marker_head() - roll back put_delete_marker_head() */
    struct DeleteMarkerSpec {
      std::string version;
      std::string instance;   // "null" if versioning is suspended, else version
      ceph::real_time mtime;
      std::string cache_key;
      rgw::sal::Attrs attrs;
    };
    /* Fields of the object that delete marker creation changes. delete_obj() restores these
       at the start of every transaction attempt so that a replay runs as the original request. */
    struct ObjectSnapshot {
      std::string instance;
      std::string version;
      std::string delete_marker_version;
      bool delete_marker{false};
      ceph::real_time mtime;
      uint64_t size{0};
      uint64_t accounted_size{0};
    };
    ObjectSnapshot snapshot_object();
    void restore_object(const ObjectSnapshot& s);
    // forced_version/forced_mtime: reuse the local RGW's delete marker version and mtime (remote RGW).
    DeleteMarkerSpec make_delete_marker_spec(const DoutPrefixProvider* dpp, optional_yield y,
                                             const std::string& forced_version,
                                             std::optional<ceph::real_time> forced_mtime = std::nullopt);
    void apply_delete_marker_state(const DeleteMarkerSpec& spec);
    int put_delete_marker_head(const DoutPrefixProvider* dpp, optional_yield y, const DeleteMarkerSpec& spec);
    void remove_delete_marker_head(const DoutPrefixProvider* dpp, optional_yield y, const DeleteMarkerSpec& spec);
    int stage_delete_marker_dir(const DoutPrefixProvider* dpp, optional_yield y, const DeleteMarkerSpec& spec,
                                rgw::d4n::Transaction& txn);
    void track_delete_marker(const DoutPrefixProvider* dpp, optional_yield y, const DeleteMarkerSpec& spec);
    // Returns 1 if the delete marker's version-specific head is in the directory, 0 if not, < 0 on error.
    int delete_marker_in_directory(const DoutPrefixProvider* dpp, optional_yield y, const DeleteMarkerSpec& spec);
    // Whether the version-specific head "_:<version>_<name>" is absent from the directory: returns 1
    // if it is missing or tombstoned, 0 if it is live, < 0 on error. Used to resolve an uncertain
    // commit: a delete was applied if it is absent, a PUT/copy HEAD commit if it is live.
    int version_head_absent_in_directory(const DoutPrefixProvider* dpp, optional_yield y, const std::string& version);
    // The cache and backend handling shared by set_obj_attrs(), modify_obj_attrs() and delete_obj_attrs().
    // resolve_current_version: the backend change applies to the current version (modify_obj_attrs()),
    // not to the object as addressed (set_obj_attrs()).
    int update_obj_attrs(const DoutPrefixProvider* dpp, optional_yield y, const char* component,
                         bool resolve_current_version,
                         const std::function<bool(rgw::sal::Attrs&)>& apply_to_cache,
                         const std::function<int()>& apply_to_backend);
    bool is_delete_marker() { return delete_marker; }
    bool exists(void) override { if (exists_in_cache) { return true;} return next->exists(); };
    bool load_obj_from_store() { return load_from_store; }
    void set_load_obj_from_store(bool load_from_store) { this->load_from_store = load_from_store; }
    int delete_cache_entry(const DoutPrefixProvider* dpp, const std::string key, optional_yield y);
    virtual void set_cache_request() override { cache_request = true; }
    bool is_cache_request() { return cache_request; }
    //the following are helper methods to get/set from/to cache
    bool is_remote_cache_request() const { return remote_cache_request; }
    void set_remote_cache_request() { remote_cache_request = true; }
    void set_block_offset(uint64_t offset) { blk_offset = offset; }
    void set_block_len(uint64_t len) { blk_len = len; }
    uint64_t get_remote_block_offset() { return blk_offset; }
    uint64_t get_remote_block_len() { return blk_len; }
	const bool get_remote_dirty_flag() {return remote_dirty;}
	void set_remote_dirty_flag(bool flag) {remote_dirty = flag;}
    // Version of the delete marker to be replicated on the remote RGW. Empty when
    // the delete op does not create a delete marker.
    const std::string get_delete_marker_version() { return delete_marker_version; }
    void set_delete_marker_version(const std::string& v) { delete_marker_version = v; }
    // mtime of the delete marker to be replicated, in nanoseconds since the epoch (as sent by the
    // local RGW). An unparsable value is ignored, and the remote RGW falls back to its own clock.
    void set_delete_marker_mtime(const DoutPrefixProvider* dpp, const std::string& ns);
    std::optional<ceph::real_time> get_delete_marker_mtime() const { return delete_marker_mtime; }
    // mtime of the object in a remote PUT, in nanoseconds since the epoch (as sent by the local
    // RGW). An unparsable value is ignored, and the remote RGW falls back to its own clock.
    void set_remote_mtime(const DoutPrefixProvider* dpp, const std::string& ns);
    std::optional<ceph::real_time> get_remote_mtime() const { return remote_mtime; }
    bool get_remote_block_only() const { return block_only; }
    void set_remote_obj_size(uint64_t size) { obj_size = size; }
    void set_remote_block_only(bool flag) { block_only = flag; }
    uint64_t get_remote_obj_size() const { return obj_size; }
	int write_if_space_available(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const Attrs& attrs,
								  uint64_t offset, const std::string& version, const bool& dirty, const rgw_owner user, const std::string& bucketName,
								  uint8_t op, optional_yield y, rgw::d4n::CacheBlock* block, bool update_policy = true);
};

/* Prefix provider to be used when spawning a new coroutine for
 * async remote ops (PUT, GET)
 */
class D4NFilterDPP : public DoutPrefixProvider {
  CephContext* cct;
  std::string prefix;
public:
  explicit D4NFilterDPP(CephContext* c, std::string& prefix) : cct(c), prefix(prefix) {}
  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const override {
    out << prefix << ": ";
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
    std::vector<std::unique_ptr<rgw::d4n::RemoteCachePutOp>> requests;
    // Dirty data written to the local cache by process(), from offset 0 (local requests only).
    uint64_t dirty_bytes_written = 0;
    // Set once complete() has committed the HEAD; until then the PUT can still fail or be abandoned.
    bool head_committed = false;

    static int write_to_remote_cache(const DoutPrefixProvider* dpp_o, const std::string& prefix, uint64_t size, const std::string& bucket_tenant, const std::string& remote_addr, const std::string& bucket_name, const std::string& obj_name, const std::string& version, const std::string& old_version, const std::string& mtime, bool dirty, D4NFilterDriver* driver, optional_yield y);

  public:
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, optional_yield _y) : FilterWriter(std::move(_next), _obj),
							     driver(_driver),
							     dpp(_dpp), atomic(false), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    D4NFilterWriter(std::unique_ptr<Writer> _next, D4NFilterDriver* _driver, Object* _obj, 
	const DoutPrefixProvider* _dpp, bool _atomic, optional_yield _y) : FilterWriter(std::move(_next), _obj),
									   driver(_driver),
									   dpp(_dpp), atomic(_atomic), y(_y) { object = static_cast<D4NFilterObject*>(obj); }
    virtual ~D4NFilterWriter() override;

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

} // namespace rgw::sal

extern "C" rgw::sal::Driver *newD4NFilter(rgw::sal::Driver *next,
    boost::asio::io_context& io_context, bool admin);
