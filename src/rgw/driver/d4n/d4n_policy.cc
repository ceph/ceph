#include "d4n_policy.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "rgw_perf_counters.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = asio::consign(std::move(handler), conn);
    return asio::dispatch(get_executor(),
        [c=conn, &req, &resp, h=std::move(h)] () mutable {
          c->async_exec(req, resp, std::move(h));
        });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename... Types>
static inline void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

LFUDAPolicy::~LFUDAPolicy()
{
  rthread_stop();
  quit = true;
  {
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    cond->notify(l);
  }
  if (watermark_timer.has_value()) {
    watermark_timer->cancel();
  }
  if (cleaning_coroutine_pool) {
    cleaning_coroutine_pool->stop();
    cleaning_coroutine_pool.reset();
  }
  if (eviction_timer.has_value()) {
    eviction_timer->cancel();
  }
  eviction_done_future.wait();
  lw_quit = true;
  lw_cond.notify_all();
  if (lwthread.joinable()) { lwthread.join(); }
  for (auto& it : entries_map) {
    delete it.second;
  }
  for (auto& it : o_entries_map) {
    delete it.second.first;
  }
}

int LFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) {
  cache_capacity = cacheDriver->get_current_partition_info(dpp).size;
  eviction_watermark_bytes = cache_capacity * EVICTION_WATERMARK;
  target_bytes = cache_capacity * TARGET_WATERMARK;

  response<int, int, int, int> resp;
  static auto obj_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, const std::string& instance, optional_yield y, std::string& restore_val) {
    std::string dirty_obj_key = rgw::sal::get_cache_block_prefix(bucket_id, obj_key.name, version);
    //Since there could be multiple data blocks of an object, we check if o_entries_map has already been populated for an object
    if (!find_obj_entry(dirty_obj_key)) {
      rgw::d4n::CacheBlock block;
      if (instance == "null") {
        block.cacheObj.objName = "_:null_" + obj_key.name;
      } else {
        block.cacheObj.objName = obj_key.get_oid();
      }
      block.cacheObj.bucketName = bucket_id;
      block.blockID = 0;
      block.size = 0;
      auto ret = blockDir->get(dpp, &block, y);
      if (ret < 0) {
        //this can happen for invalid dirty objects (have been deleted)
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() blockDir->get() failed: " << ret << dendl;
        return;
      }
      rgw::sal::Attrs attrs = std::move(block.cacheObj.attrs);
      std::string etag;
      if (auto i = attrs.find(RGW_ATTR_ETAG); i != attrs.end()) {
        etag = i->second.to_str();
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): etag: " << etag << dendl;
      }
      uint64_t size = 0;
      if (auto i = attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE); i != attrs.end()) {
        size = std::stoull(i->second.to_str());
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): size: " << size << dendl;
      }
      double creationTime = 0;
      if (auto i = attrs.find(RGW_CACHE_ATTR_MTIME); i != attrs.end()) {
        creationTime = std::stod(i->second.to_str());
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): creationTime: " << creationTime << dendl;
      }
      rgw_user user;
      if (auto i = attrs.find(RGW_ATTR_ACL); i != attrs.end()) {
        bufferlist bl_acl = i->second;
        RGWAccessControlPolicy policy;
        auto iter = bl_acl.cbegin();
        try {
          policy.decode(iter);
        } catch (buffer::error& err) {
          ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
        }
        user = std::get<rgw_user>(policy.get_owner().id);
        ldpp_dout(dpp, 20) << "LFUDAPolicy: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
      }
      std::string bucket_name;
      if (auto i = attrs.find(RGW_CACHE_ATTR_BUCKET_NAME); i != attrs.end()) {
        bucket_name = i->second.to_str();
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_name: " << bucket_name << dendl;
      }
      State state{State::INIT};
      if (!restore_val.empty() && restore_val == "1") { // No need to set the xattr because this case only occurs when the state has
        state = State::INVALID;                         // been retrieved from the xattr itself.
        ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): State restored to INVALID." << dendl;
      } else {
        state = State::INIT;
      }
      //create new LFUDAObjEntry and populate o_entries_map and per_obj_versions map
      std::lock_guard<std::mutex> l(lfuda_cleaning_lock);
      create_obj_entry(dpp, dirty_obj_key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, state);
    }
  };

  static auto block_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, const rgw_user user, const std::string& bucketName, optional_yield y, std::string& restore_val) {
    update(dpp, key, offset, len, version, dirty, user, bucketName, RefCount::NOOP, y, restore_val);
  };

  cacheDriver->restore_blocks_objects(dpp, obj_callback, block_callback);
  {
    //Now loop through the version map and populate heap with the first entry as they are sorted by creation time
    const std::lock_guard l(lfuda_cleaning_lock);
    for (auto& [obj_name, version_map] : per_obj_versions) {
      if (version_map.empty()) {
        continue;
      }
      // version_map is std::map<uint64_t, LFUDAObjEntry*> sorted ascending by
      // creationTime, so begin() is the oldest version — push that to the heap
      LFUDAObjEntry* e = version_map.begin()->second;
      handle_type handle = object_heap.push(e);
      e->set_handle(handle);
      ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                        << "(): pushed to heap obj_name=" << obj_name
                        << " creationTime=" << version_map.begin()->first << dendl;
    }
  }
  driver = _driver;
  if (dpp->get_cct()->_conf->d4n_writecache_enabled) {
    int num_cleaning_threads = dpp->get_cct()->_conf->rgw_d4n_cleaning_threads;
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ 
                       << "(): initializing cleaning thread pool with " 
                       << num_cleaning_threads << " threads" << dendl;
    cleaning_thread_pool = std::make_unique<boost::asio::thread_pool>(num_cleaning_threads);
    quit = false;
    cond.emplace(io_context.get_executor());
    watermark_timer.emplace(io_context);
    if (!cleaning_coroutine_pool) {
      cleaning_coroutine_pool = std::make_unique<rgw::sal::CoroutinePool>(cleaning_thread_pool->get_executor(), num_cleaning_threads);
      cleaning_coroutine_pool->start(dpp);
    }
  }

  lwthread = std::thread(&LFUDAPolicy::localweight_writer, this, dpp);
  lw_quit = false;

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>>
    > resp;
    request req;
    req.push("MULTI");
    req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), /* New cache node will always have the minimum average weight */
              "minLocalWeights_size", std::to_string(entries_map.size()), 
              "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
    req.push("HSETNX", "lfuda", "age", age); /* Only set maximum age if it doesn't exist */
    req.push("EXEC");
  
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  asio::co_spawn(io_context.get_executor(),
		   redis_sync(dpp, y), asio::detached);

  if (cleaning_coroutine_pool) {
    int num_cleaning_threads = dpp->get_cct()->_conf->rgw_d4n_cleaning_threads;
    for (int i = 0; i < num_cleaning_threads; ++i) {
      cleaning_coroutine_pool->submit([this, dpp](boost::asio::yield_context yield) {
      optional_yield y(yield);
      cleaning(dpp, y);
      });
    }
  }
  eviction_timer.emplace(io_context);
  boost::asio::spawn(
        io_context,
        [this, dpp](boost::asio::yield_context yield) {
          optional_yield y{yield};
          background_eviction_worker(dpp, y);
        },
        [this, dpp](std::exception_ptr e) {
          if (e) {
            eviction_done_promise.set_exception(e);
          } else {
            eviction_done_promise.set_value();
          }
          ldpp_dout(dpp, 10) << "Background eviction co-routine stopped" << dendl;
      }
    );
  return 0;
}

int LFUDAPolicy::getMinAvgWeight(const DoutPrefixProvider* dpp, int *minAvgWeight, std::string *cache_address, optional_yield y) {
  response<std::string, std::string, std::string> resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "minLocalWeights_sum");
    req.push("HGET", "lfuda", "minLocalWeights_size");
    req.push("HGET", "lfuda", "minLocalWeights_address");
      
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 10) << __func__ << "(): Error: " << ec.value() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << __func__ << "(): Error: " << "EINVAL" << dendl;
    return -EINVAL;
  }

  *minAvgWeight = 0; 
  if (std::get<0>(resp).has_value() && std::get<1>(resp).has_value()) {
	try {
	  int lw_size = std::stoi(std::get<1>(resp).value());
      if (lw_size > 0) {
		*minAvgWeight =  std::stoi(std::get<0>(resp).value()) / lw_size;
      }
	} catch (std::exception &e) {
	  return -EINVAL;
	}
  }

  *cache_address =  std::get<2>(resp).value();
  ldpp_dout(dpp, 20) << __func__ << "(): Cache address with minimum local weight is " << *cache_address << dendl;
  return 0;
}

int LFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  response< std::optional<std::string> > resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (std::get<0>(resp).value().value().empty() || age > std::stoi(std::get<0>(resp).value().value())) { /* Set new maximum age */
    try { 
      boost::system::error_code ec;
      response<ignore_t> ret;
      request req;
      req.push("HSET", "lfuda", "age", age);

      redis_exec(conn, ec, req, ret, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  } else {
    age = std::stoi(std::get<0>(resp).value().value());
  }

  return 0;
}

int LFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
    response<std::vector<std::string>> resp;

    try { 
      boost::system::error_code ec;
      request req;
      req.push("HMGET", "lfuda", "minLocalWeights_sum", "minLocalWeights_size");
	
      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  
    float minAvgWeight = std::stof(std::get<0>(resp).value()[0]) / std::stof(std::get<0>(resp).value()[1]);
    float localAvgWeight = 0;
    if (entries_map.size())
      localAvgWeight = static_cast<float>(weightSum) / static_cast<float>(entries_map.size());

    if (localAvgWeight < minAvgWeight) { /* Set new minimum weight */
      try { 
	boost::system::error_code ec;
	response<ignore_t> resp;
	request req;
	req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), 
                  "minLocalWeights_size", std::to_string(entries_map.size()), 
                  "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      } catch (std::exception &e) {
	return -EINVAL;
      }
    } else {
      weightSum = std::stoi(std::get<0>(resp).value()[0]);
      postedSum = std::stoi(std::get<0>(resp).value()[0]);
    }
  }

  try { /* Post update for local cache */
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("HSET", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, "avgLocalWeight_sum", std::to_string(weightSum), 
              "avgLocalWeight_size", std::to_string(entries_map.size()));

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    return 0;
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

asio::awaitable<void> LFUDAPolicy::redis_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  rthread_timer.emplace(co_await asio::this_coro::executor);
  co_await asio::this_coro::throw_if_cancelled(true);
  co_await asio::this_coro::reset_cancellation_state(
    asio::enable_terminal_cancellation());

  for (;;) try {
    /* Update age */
    if (int ret = age_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ret << dendl;
    }
    
    /* Update minimum local weight sum */
    if (int ret = local_weight_sync(dpp, y) < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << ret << dendl;
    }

    int interval = dpp->get_cct()->_conf->rgw_lfuda_sync_frequency;
    rthread_timer->expires_after(std::chrono::seconds(interval));
    co_await rthread_timer->async_wait(asio::use_awaitable);
  } catch (sys::system_error& e) {
    if (e.code() == asio::error::operation_aborted) {
      break;
    } else {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
      continue;
    }
  }
}

/* Changes state to INVALID for dirty objects. An INVALID state indicates that a delete request has been
 issued on an object and it must be deleted rather than written to the backend. This lazy deletion occurs
 in the Cleaning method and prevents data races during concurrent requests. The method below returns "false"
 if the state has not been set to INVALID, and "true" if it has. The state is not set to INVALID when
 cleaning is in progress, a process which writes the object to the backend store. */
bool LFUDAPolicy::invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) {
  std::unique_lock<std::mutex> l(lfuda_cleaning_lock);

  if (o_entries_map.empty())
    return false;

  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): key=" << key << " not found" << dendl;
    return false;
  }

  if (p->second.second == State::INIT) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Setting State::INVALID for key=" << key << dendl;
    p->second.second = State::INVALID;
    //head block is only for delete marker
    if(p->second.first->delete_marker) {
      if(int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_INVALID, "1", y); ret < 0) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Failed to set xattr, ret=" << ret << dendl;
        return false;
      }
    }
    return true;
  } else if (p->second.second == State::IN_PROGRESS) {
    state_cond.wait(l, [this, &key]{ return (o_entries_map.find(key) == o_entries_map.end()); });
  }

  return false;
}

int LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, CacheBlock* victim, optional_yield y) {
  if (entries_heap.empty())
    return -ENOENT;

  /* Get victim cache block */
  auto entry = entries_heap.top();
  std::string key = entry->key;

  if (rgw::sal::parse_block_from_cache(key).has_value()) {
    *victim = rgw::sal::parse_block_from_cache(key).value(); 
  } else {
    return -ENOENT;
  }

  /* check dirty flag of entry to be evicted, if the flag is dirty, all entries on the local node are dirty
    check refcount also, if refcount > 0 then no entries are available for eviction */
  if (entry->dirty || entry->refcount > 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Top entry in min heap is dirty or with positive refcount, no entry is available for eviction!" << dendl;
    return -ENOENT;
  }

  return 0;
}

int LFUDAPolicy::exist_key(const std::string& key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int LFUDAPolicy::perform_background_eviction(const DoutPrefixProvider* dpp, uint64_t bytes_to_free, optional_yield y)
{
  uint64_t total_freed = 0;
  int ret = 0;

  // Evict in batches to avoid holding locks too long
  while (total_freed < bytes_to_free && !quit) {
    uint64_t before = cacheDriver->get_free_space(dpp, y);
    uint64_t batch_size = std::min(bytes_to_free - total_freed, EVICTION_BATCH_SIZE);

    // call eviction
    ret = eviction(dpp, batch_size, y);
    if (ret < 0) {
      //may fail due to all objects being dirty
      ldpp_dout(dpp, 5) << "Background eviction failed: " << ret << dendl;
      break;
    }

    uint64_t after = cacheDriver->get_free_space(dpp, y);
    uint64_t actually_freed = 0;
    if (after > before) {
      actually_freed = after - before;
      total_freed += actually_freed;
    }

    ldpp_dout(dpp, 20) << "Batch freed " << actually_freed << " bytes (requested " << batch_size << ")" << dendl;
    uint64_t used_space = cache_capacity - after;
    if (used_space < eviction_watermark_bytes) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << " used_space=" << used_space
                         << " dropped below watermark=" << eviction_watermark_bytes
                         << ", clearing flag" << dendl;
      above_watermark = false;
      break;
    }
  }
  uint64_t final_used = cache_capacity - cacheDriver->get_free_space(dpp, y);
  if (final_used < eviction_watermark_bytes) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                         << " final_used=" << final_used
                         << " dropped below watermark=" << eviction_watermark_bytes
                         << ", clearing flag" << dendl;
  }

  ldpp_dout(dpp, 10) << "Background eviction completed: freed " 
                 << (total_freed / 1024 / 1024) << "MB" << dendl;
  return ret;
}

void LFUDAPolicy::background_eviction_worker(const DoutPrefixProvider* dpp, optional_yield y)
{
  ldpp_dout(dpp, 10) << "Background eviction co-routine started" << dendl;
  int consecutive_failures = 0;
  const int MAX_BACKOFF = 5;  // Max 2^5 = 32x EVICTION_CHECK_INTERVAL
  while (!quit) {
    auto wait_duration = EVICTION_CHECK_INTERVAL;
    if (consecutive_failures > 0) {
      int multiplier = 1 << std::min(consecutive_failures, MAX_BACKOFF);
      wait_duration = EVICTION_CHECK_INTERVAL * multiplier;
      ldpp_dout(dpp, 10) << "Backing off: waiting " << wait_duration.count() << dendl;
    }
    eviction_timer->expires_after(wait_duration);
    boost::system::error_code ec;
    eviction_timer->async_wait(y.get_yield_context()[ec]);

    if (ec == boost::asio::error::operation_aborted || quit.load()) {
      break;
    }

    // Check current cache usage
    uint64_t free_space = cacheDriver->get_free_space(dpp, y);
    uint64_t used_space = (free_space < cache_capacity) ? (cache_capacity - free_space) : 0;

    ldpp_dout(dpp, 20) << "LFUDAPolicy:: " << __func__ << " cache_capacity: " << cache_capacity << dendl;
    ldpp_dout(dpp, 20) << "LFUDAPolicy:: " << __func__ << " free_space: " << free_space << dendl;
    ldpp_dout(dpp, 10) << "LFUDAPolicy:: " << __func__ << " used_space: " << used_space << dendl;

    // Only evict if above watermark
    if (used_space < eviction_watermark_bytes) {
      consecutive_failures = 0;
      continue;
    }

    above_watermark = true;

    // Calculate bytes to free (evict to TARGET_WATERMARK)
    uint64_t bytes_to_free = (used_space > target_bytes) ? (used_space - target_bytes) : 0;
    if (bytes_to_free == 0) {
      consecutive_failures = 0;
      continue;
    }

    double usage_pct = (static_cast<double>(used_space) / cache_capacity) * 100;
    ldpp_dout(dpp, 5) << "Cache at " << usage_pct 
                   << "% - evicting " << (bytes_to_free / 1024 / 1024) 
                   << "MB to reach " << TARGET_WATERMARK << dendl;

    // Perform eviction
    auto ret = perform_background_eviction(dpp, bytes_to_free, y);
    if (ret < 0) {
      consecutive_failures++;
    } else {
      consecutive_failures = 0;
    }

    //if still above watermark, trigger cleaning
    if (above_watermark) {
      watermark_timer->cancel();
    }
  }
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  int ret = -1;
  std::vector<LFUDAEntry> to_delete;

  // Under lfuda_lock, select victims until enough space is freed, delete them from the heap and map so that they do not get 
  // selected again by another thread when the lock is released
  {
    std::unique_lock<std::mutex> lfuda_l(lfuda_lock);

    uint64_t freed = 0;
    while (freed < size) {
	  CacheBlock victim;
	  ret = get_victim_block(dpp, &victim, y);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
        return -ENOSPC;
      }

	  std::string victim_key = entries_heap.top()->key;
	  auto it = entries_map.find(victim_key);
	  if (it == entries_map.end()) {
        ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not locate victim block entry in entries_map." << dendl;
		return -ENOENT;
	  }

      uint64_t victim_size = victim.size;
      to_delete.push_back(*it->second);
      _erase(dpp, victim_key, y);
      freed += victim_size;
    }
  } // lfuda_lock released

  // Outside both locks, do expensive I/O
  for (auto& entry : to_delete) {
    rgw::d4n::CacheBlock block = rgw::sal::parse_block_from_cache(entry.key).value();
    std::string globalWeight;
    bool update_global_weight = true;

    //FIXME: remoteCacheAddress is getting overriden by a new cache. it should be updates instead.
    int avgWeight;
    std::string remoteCacheAddress;
    if (getMinAvgWeight(dpp, &avgWeight, &remoteCacheAddress, y) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve min average weight." << dendl;
      return -ENOENT;
    } else if (remoteCacheAddress == dpp->get_cct()->_conf->rgw_d4n_local_rgw_address) {
      remoteCacheAddress.clear(); // evict normally without remote put 
    }

	if ((ret = blockDir->get(dpp, &block, y)) < 0) {
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Unable to retrieve victim block's hostsList." << dendl;
	  return ret;
	}

    ldpp_dout(dpp, 20) << __func__ << "(): " << __LINE__ << " Victim host list size is " << block.cacheObj.hostsList.size() << dendl;

    //the following part takes care of updating the weight (globalWeight) of the block if this is the last copy in a remote setup
    //and is pushed out to a remote cache where space is available
    if (block.cacheObj.hostsList.size() == 1 && *(block.cacheObj.hostsList.begin()) == dpp->get_cct()->_conf->rgw_d4n_local_rgw_address) { // Last copy 
	  update_global_weight = false;
      if (block.globalWeight) {
		entry.localWeight += block.globalWeight;
		block.globalWeight = 0;
      }

	  if (!remoteCacheAddress.empty()) {
		if (entry.localWeight > avgWeight) {
		  // Write the victim block to the remote cache
		  bufferlist out_bl;
		  rgw::sal::Attrs obj_attrs;

		  int ret = cacheDriver->get(dpp, entry.key, block.blockID, block.size, out_bl, obj_attrs, y);
		  if (ret < 0) {
			ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " Failed to retrieve victim data block from cache." << dendl;
			return ret;
		  } else {
			rgw::d4n::RemoteCachePutOp::RemoteCachePutOpData op {
			  entry.bucketName,
			  block.cacheObj.objName,
			  block.blockID,
			  block.size,
			  block.version,
			  false,
			  entry.user,
			  remoteCacheAddress,
			  block.cacheObj.size
			};
			std::unique_ptr<rgw::d4n::RemoteCachePutOp> remote_put = std::make_unique<rgw::d4n::RemoteCachePutOp>(driver, op, true);
			if ((ret = remote_put->send_and_complete_request(dpp, y, &out_bl)) < 0){
			  ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " Sending to remote has failed: " << remoteCacheAddress << dendl;
			  return ret;
			}
			ldpp_dout(dpp, 20) << __func__ << "(): " << __LINE__ << " Sending to remote is done." << dendl;
			update_global_weight = true;
		  } 
	    }
	  }
    }

	// Only update victim block's global weight if the block wasn't completely evicted 
    if (update_global_weight) {
	  block.globalWeight += entry.localWeight;
	  globalWeight = std::to_string(block.globalWeight);
	  if (int ret = blockDir->update_field(dpp, &block, "globalWeight", globalWeight, y) < 0) {
		return ret;
	  }

	  //Need to get and then update the host atomically in a remote setup
	  if ((ret = blockDir->remove_host(dpp, &block, dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, y)) < 0) {
		ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " Failed to remove local host from victim block." << dendl;
		return ret;
	  }
    }


    if ((ret = cacheDriver->delete_data(dpp, entry.key, y)) < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): " << __LINE__ << " Failed to delete victim block from cache." << dendl;
      return ret;
    }

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << block.cacheObj.objName << " has been evicted." << dendl;

    if (perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_evictions);
    }
  }

  return 0;
}

bool LFUDAPolicy::update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y)
{
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updating refcount for entry: " << key << dendl;
  const std::lock_guard l(lfuda_lock);
  auto entry = find_entry(key);
  uint64_t refcount = 0;
  if (entry == nullptr) {
    return false;
  }
  refcount = entry->refcount;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): old refcount is: " << refcount << dendl;
  if (op == RefCount::INCR) {
    refcount += 1;
  } else if (op == RefCount::DECR) {
    if (refcount > 0) {
      refcount -= 1;
    }
  }
  (*entry->handle)->refcount = refcount;
  entries_heap.update(entry->handle);
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << (*entry->handle)->refcount << dendl;

  return true;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, const rgw_user user, const std::string& bucketName, uint8_t op, optional_yield y, std::string& restore_val)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updating entry: " << key << dendl;
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  bool updateLocalWeight = true, updateBucketName = true, should_notify = false;
  {
    const std::lock_guard l(lfuda_lock);
    int localWeight = age;
    auto entry = find_entry(key);
    uint64_t refcount = 0;
    if (!restore_val.empty()) {
      updateLocalWeight = false;
      updateBucketName = false;
      localWeight = std::stoull(restore_val);
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): restored localWeight is: " << localWeight << dendl;
    }

    /* check the dirty flag in the existing entry for the key and the incoming dirty flag. If the
      incoming dirty flag is false, that means update() is invoked as part of cleaning process,
      so we must not update its localWeight. */
    if (entry) {
      refcount = entry->refcount;
      if (entry->dirty && dirty.has_value()) {
        bool is_dirty = dirty.value();
        if (!is_dirty) {
          localWeight = entry->localWeight;
          updateLocalWeight = false;
        }
      }
      if (updateLocalWeight) {
        localWeight = entry->localWeight + age;
      }
      if (op == RefCount::INCR) {
        refcount += 1;
      }
      if (op == RefCount::DECR) {
        if (refcount > 0) {
          refcount -= 1;
        }
      }
    }
    //pick the existing value of dirty, if no value has been passed in
    bool is_dirty = false;
    if (dirty.has_value()) {
      is_dirty = dirty.value();
    } else if (entry) {
      is_dirty = entry->dirty;
    }
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << refcount << dendl;

    if (entry) {
      entry->key = key;
      entry->offset = offset;
      entry->len = len;
      entry->version = version;
      entry->dirty = is_dirty;
      entry->refcount = refcount;
      entry->localWeight = localWeight;
      entries_heap.update(entry->handle, entry);
      updateBucketName = false;
    } else {
      LFUDAEntry* e = new LFUDAEntry(key, offset, len, version, is_dirty, refcount, user, localWeight, bucketName);
      handle_type handle = entries_heap.push(e);
      e->set_handle(handle);
      entries_map.emplace(key, e);
    }

    if (updateLocalWeight) {
      updated_blocks.emplace(key, localWeight);
      if (updated_blocks.size() >= LOCALWEIGHT_BATCH_SIZE) {
        should_notify = true;
      }
    }

    weightSum += ((localWeight < 0) ? 0 : localWeight);
  } //lock will be released here
  /*adding bucket name as attribute as it is needed during eviction
  * for pushing the block to a remote rgw. needs to be added when the
  * entry is created initially only
  */
  if(updateBucketName) {
    if (auto ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_BUCKET_NAME, bucketName, y); ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
    }
  }
  if (should_notify) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): notify_one: "<< dendl;
    lw_cond.notify_one();
  }
}

LFUDAPolicy::LFUDAObjEntry* LFUDAPolicy::create_obj_entry(const DoutPrefixProvider* dpp, const std::string& dirty_obj_key, const std::string& version,
                                              bool deleteMarker, uint64_t size, double creationTime,
                                              const rgw_user& user, const std::string& etag,
                                              const std::string& bucket_name, const std::string& bucket_id,
                                              const rgw_obj_key& obj_key, State state)
{
  LFUDAObjEntry* e = new LFUDAObjEntry{dirty_obj_key, version, deleteMarker, size, creationTime,
                                        user, etag, bucket_name, bucket_id, obj_key};
  o_entries_map.emplace(dirty_obj_key, std::make_pair(e, state));

  std::string obj_name = e->obj_key.name;
  auto& versions = per_obj_versions[obj_name];
  versions[e->creationTime] = e;

  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                     << "(): created entry key=" << dirty_obj_key
                     << " obj_name=" << obj_name
                     << " creationTime=" << creationTime << dendl;
  return e;
}

void LFUDAPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  State state{State::INIT};
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Before acquiring lock, adding entry: " << key << dendl;

  std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
  LFUDAObjEntry* e = create_obj_entry(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, state);
  auto& versions = per_obj_versions[e->obj_key.name];
  if (versions.size() == 1) {
    handle_type handle = object_heap.push(e);
    e->set_handle(handle);
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                           << "(): added obj=" << e->obj_key.name
                           << " key=" << e->key
                           << " creationTime=" << e->creationTime
                           << dendl;
  } else {
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                           << "(): queued version for existing obj="
                           << e->obj_key.name
                           << " key=" << e->key
                           << " creationTime=" << e->creationTime
                           << " versions_count=" << versions.size()
                           << dendl;
  }
  cond->notify(l);
}

bool LFUDAPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  weightSum -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);

  entries_heap.erase(p->second->handle);
  delete p->second;
  p->second = nullptr;
  entries_map.erase(p);
  
  return true;
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  return _erase(dpp, key, y);
}

bool LFUDAPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_cleaning_lock);
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ 
                       << "(): erasing key=" << key << dendl;
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  LFUDAObjEntry* e = p->second.first;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ 
                       << "(): obj_name=" << e->obj_key.name << dendl;
  std::string obj_name = e->obj_key.name;
  auto v_it = per_obj_versions.find(obj_name);
  if (v_it != per_obj_versions.end()) {
    v_it->second.erase(e->creationTime);
    if (v_it->second.empty()) {
      // No more versions for this object — full cleanup
      per_obj_versions.erase(v_it);
    }
  }
  delete p->second.first;
  p->second.first = nullptr;
  o_entries_map.erase(p);
  state_cond.notify_one();
  return true;
}

int LFUDAPolicy::delete_data_blocks(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y) {
  off_t lst = e->size, fst = 0;

  do {
    if (fst >= lst) {
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;
    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));

    int ret = -1;
    std::unique_lock<std::mutex> ll(lfuda_lock);
    auto it = entries_map.find(oid_in_cache);
    if (it != entries_map.end()) {
      if (it->second->refcount > 0) {
        return -EBUSY;//better error code?
      }
    }
    ll.unlock();
    if ((ret = cacheDriver->delete_data(dpp, oid_in_cache, y)) == 0) {
      if (!(ret = erase(dpp, oid_in_cache, y))) {
	ldpp_dout(dpp, 0) << "Failed to delete policy entry for: " << oid_in_cache << ", ret=" << ret << dendl;
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to delete data block " << oid_in_cache << ", ret=" << ret << dendl;
      return -EINVAL;
    }

    fst += cur_len;
  } while (fst < lst);

  return 0;
}

/* This method deletes INVALID cache entries during cleaning.
   Invalid entries are dirty entries that have been marked invalid due to a delete request from the client.
   It defers the deletion time of the object, in case it is still being read (using its refcount) */
int LFUDAPolicy::do_delete(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, int interval, optional_yield y)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                      << "(): State is INVALID; deleting key=" << e->key << dendl;

  int ret;
	//check if key exists and get the refcount of block, if greater than zero then modify the creationTime of dirty object to attempt to delete later
  if (e->delete_marker) {
    std::unique_lock<std::mutex> ll(lfuda_lock);
    auto it = entries_map.find(e->key);
    if (it != entries_map.end()) {
      ll.unlock();
      //head block exists only for delete markers (and no data blocks)
      if ((ret = cacheDriver->delete_data(dpp, e->key, y)) == 0) {
        if ((ret = erase(dpp, e->key, y)) < 0) {
          ldpp_dout(dpp, 0) << "Failed to delete head policy entry for: " << e->key << ", ret=" << ret << dendl;
          return ret;
        }
      } else {
        ldpp_dout(dpp, 0) << "Failed to delete head block for: " << e->key << ", ret=" << ret << dendl;
        return ret;
      }
    } else {
      ll.unlock();
      ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << " head block not found in entries_map for key="
                       << e->key << ", nothing to delete" << dendl;
    }
  } else { //non delete marker path
    //only data blocks exist for non-delete markers
    ret = delete_data_blocks(dpp, e, y);
    if (ret == -EBUSY) {
      std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
      //deferring the deletion of the invalid object
      auto v_it = per_obj_versions.find(e->obj_key.name);
      if (v_it != per_obj_versions.end()) {
        v_it->second.erase(e->creationTime);
        e->creationTime += (interval / 2);
        v_it->second[e->creationTime] = e;
      }
      ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated creation time is: " << e->creationTime << dendl;
      l.unlock();
    } else if (ret < 0) {
      ldpp_dout(dpp, 0) << "Failed to delete blocks for: " << e->key << ", ret=" << ret << dendl;
      return ret;
    }
  }
	return 0;
}

/* As part of the cleaning process, this method reads an object from the cache
 * and writes it to the backend store. It marks the object clean in the directory
 * It is also responsible for correctly updating the version in the directory.
*/
int LFUDAPolicy::do_writeback(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__
                       << "(): writing back key=" << e->key << dendl;
  rgw::sal::Attrs obj_attrs;
  uint64_t len = 0;
  rgw_user c_rgw_user = e->user; 
  //writing data to the backend
  //we need to create an atomic_writer
  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(c_rgw_user);

  std::unique_ptr<rgw::sal::Bucket> c_bucket;
  rgw_bucket c_rgw_bucket = rgw_bucket(c_rgw_user.tenant, e->bucket_name, e->bucket_id);

  RGWBucketInfo c_bucketinfo;
  c_bucketinfo.bucket = c_rgw_bucket;
  c_bucketinfo.owner = c_rgw_user;
  int ret = driver->load_bucket(dpp, c_rgw_bucket, &c_bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << __func__ << "(): load_bucket() returned ret=" << ret << dendl;
    return ret;
  }

  std::unique_ptr<rgw::sal::Object> c_obj = c_bucket->get_object(e->obj_key);
  bool null_instance = (c_obj->get_instance() == "null");
  if (null_instance) {
    //clear the instance for backend store
    c_obj->clear_instance();
  }
  ldpp_dout(dpp, 20) << __func__ << "(): c_obj oid =" << c_obj->get_oid() << dendl;

  ACLOwner owner{c_user->get_id(), c_user->get_display_name()};

  ldpp_dout(dpp, 10) << __func__ << "(): e->key=" << e->key << dendl;
  int op_ret;
  if (e->delete_marker) {
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = c_obj->get_delete_op();
    del_op->params.obj_owner = owner;
    del_op->params.bucket_owner = c_bucket->get_owner();
    del_op->params.versioning_status = c_bucket->get_info().versioning_status();
    //populate marker_version_id only when delete marker is not null
    if (!null_instance) {
      del_op->params.marker_version_id = e->version;
    }
    op_ret = del_op->delete_obj(dpp, y, rgw::sal::FLAG_LOG_OP);
    if (op_ret >= 0) {
      bool delete_marker = del_op->result.delete_marker;
      std::string version_id = del_op->result.version_id;
      ldpp_dout(dpp, 20) << __func__ << "delete_obj delete_marker=" << delete_marker << dendl;
      ldpp_dout(dpp, 20) << __func__ << "delete_obj version_id=" << version_id << dendl;
    } else {
      ldpp_dout(dpp, 20) << __func__ << "delete_obj returned ret=" << op_ret << dendl;
      return op_ret;
    }
  } else { //end-if delete_marker
    std::unique_ptr<rgw::sal::Writer> processor =  driver->get_atomic_writer(dpp,
      y,
      c_obj.get(),
      owner,
      NULL,
      0,
      "");

    op_ret = processor->prepare(y);
    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "processor->prepare() returned ret=" << op_ret << dendl;
      return op_ret;
    }

    off_t lst = e->size;
    off_t fst = 0;
    off_t ofs = 0;

    rgw::sal::DataProcessor* filter = processor.get();
    rgw::d4n::CacheBlock block;
    block.cacheObj.objName = e->obj_key.get_oid();
    block.cacheObj.bucketName = e->bucket_id;
    block.blockID = 0;
    block.size = 0;
    auto ret = blockDir->get(dpp, &block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "() blockDir->get() failed: " << ret << dendl;
      return ret;
    }
    obj_attrs = std::move(block.cacheObj.attrs);
    obj_attrs.erase(RGW_CACHE_ATTR_MTIME);
    obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    obj_attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    obj_attrs.erase(RGW_CACHE_ATTR_EPOCH);
    obj_attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    obj_attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    obj_attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    obj_attrs.erase(RGW_CACHE_ATTR_LOCAL_WEIGHT);

    do {
      ceph::bufferlist data;
      if (fst >= lst){
        break;
      }
      off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
      off_t cur_len = cur_size - fst;
      std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
      ldpp_dout(dpp, 10) << __func__ << "(): oid_in_cache=" << oid_in_cache << dendl;
      rgw::sal::Attrs attrs;
      cacheDriver->get(dpp, oid_in_cache, 0, cur_len, data, attrs, y);
      if (op_ret < 0) {
        ldpp_dout(dpp, 20) << __func__ << "cacheDriver->get returned ret=" << op_ret << dendl;
        return op_ret;
      }
      len = data.length();
      fst += len;

      if (len == 0) {
        // TODO: if len of any block is 0 for some reason, we must return from here?
        break;
      }

      op_ret = filter->process(std::move(data), ofs);
      if (op_ret < 0) {
        ldpp_dout(dpp, 20) << __func__ << "processor->process() returned ret=" << op_ret << dendl;
        return op_ret;
      }
      ofs += len;
    } while (len > 0);

    op_ret = filter->process({}, ofs);

    const req_context rctx{dpp, y, nullptr};
    ceph::real_time mtime = ceph::real_clock::from_time_t(e->creationTime);
    op_ret = processor->complete(lst, e->etag, &mtime, ceph::real_clock::from_time_t(e->creationTime), obj_attrs,
          std::nullopt, ceph::real_time(), nullptr, nullptr,
          nullptr, nullptr, nullptr,
          rctx, rgw::sal::FLAG_LOG_OP);

    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "processor->complete() returned ret=" << op_ret << dendl;
      return op_ret;
    }
    /* invoke update() with dirty flag set to false, to update in-memory metadata for each block
        reset values */
    lst = e->size;
    fst = 0;
    do {
      if (fst >= lst) {
        break;
      }
      off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
      off_t cur_len = cur_size - fst;

      std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));
      ldpp_dout(dpp, 20) << __func__ << "(): oid_in_cache =" << oid_in_cache << dendl;
      //Update in-memory data structure for each block
      this->update(dpp, oid_in_cache, 0, 0, e->version, false, e->user, e->bucket_name, 0, y);

      rgw::d4n::CacheBlock block;
      block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
      block.cacheObj.objName = c_obj->get_key().get_oid();
      block.size = cur_len;
      block.blockID = fst;
      if ((op_ret = cacheDriver->set_attr(dpp, oid_in_cache, RGW_CACHE_ATTR_DIRTY, "0", y)) == 0) {
        std::string dirty = "false";
        op_ret = blockDir->update_field(dpp, &block, "dirty", dirty, y);
        if (op_ret < 0) {
          ldpp_dout(dpp, 0) << __func__ << "updating dirty flag in block directory failed, ret=" << op_ret << dendl;
        }
      } else {
        ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty xattr in cache, ret=" << op_ret << dendl;
      }

      fst += cur_len;
    } while(fst < lst);
  } //end-else if delete_marker

  //head block exists only for delete marker
  if (e->delete_marker) {
    //invoke update() with dirty flag set to false, to update in-memory metadata for head
    this->update(dpp, e->key, 0, 0, e->version, false, e->user, e->bucket_name, 0, y);
    if ((ret = cacheDriver->set_attr(dpp, e->key, RGW_CACHE_ATTR_DIRTY, "0", y)) < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to update dirty attr in cache, ret=" << op_ret << dendl;
    }
  }
  if (null_instance) {
    //restore instance for directory data processing in later steps
    c_obj->set_instance("null");
  }
  rgw::d4n::CacheBlock block;
  block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
  ldpp_dout(dpp, 20) << __func__ << "(): bucket name: " << block.cacheObj.bucketName << dendl;
  block.cacheObj.objName = c_obj->get_name();
  block.size = 0;
  block.blockID = 0;
  //non-versioned case
  if (!c_obj->have_instance()) {
    // hash entry for latest version
    op_ret = blockDir->get(dpp, &block, y);
    if (op_ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
      return op_ret;
    } else {
      // if this entry is not the latest, it could have been overwritten by a newer one
      if (block.version == e->version) {
        rgw::d4n::CacheBlock null_block;
        null_block = block;
        null_block.cacheObj.objName = "_:null_" + c_obj->get_name();
        //hash entry for null block
        op_ret = blockDir->get(dpp, &null_block, y);
        if (op_ret < 0) {
          ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << null_block.cacheObj.objName << ", ret=" << ret << dendl;
        } else {
          if (null_block.version == e->version) {
            block.cacheObj.dirty = false;
            null_block.cacheObj.dirty = false;
            auto blk_op_ret = blockDir->set(dpp, &block, y);
            auto null_op_ret = blockDir->set(dpp, &null_block, y);
            if (blk_op_ret < 0 || null_op_ret < 0) {
              ldpp_dout(dpp, 0) << __func__ << "(): Failed to Queue update dirty flag for latest entry/null entry in block directory" << dendl;
            }
          }
        }
      } //end-if (block.version == entry->version)
    } //end - else if op_ret == 0
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Removing object name: "<< c_obj->get_name() << " score: " << std::setprecision(std::numeric_limits<double>::max_digits10) << e->creationTime << " from ordered set" << dendl;
    rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
      .objName = c_obj->get_name(),
      .bucketName = c_obj->get_bucket()->get_bucket_id(),
    };
    /* remove the entry from the ordered set using its score, as the object is already cleaned
        need not be part of a transaction as it is being removed based on its score which is its creation time. */
    ret = objDir->zremrangebyscore(dpp, &dir_obj, e->creationTime, e->creationTime, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to remove object from ordered set with error: " << ret << dendl;
      return ret;
    }
  }
  if (c_obj->have_instance()) { //versioned case
    std::string objName = c_obj->get_oid();
    if (c_obj->get_instance() == "null") {
      objName = "_:null_" + c_obj->get_name();
    }
    rgw::d4n::CacheBlock instance_block;
    instance_block.cacheObj.bucketName = c_obj->get_bucket()->get_bucket_id();
    instance_block.cacheObj.objName = objName;
    instance_block.size = 0;
    instance_block.blockID = 0;
    std::string dirty = "false";
    op_ret = blockDir->update_field(dpp, &instance_block, "dirty", dirty, y);
    if (op_ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << "updating dirty flag in block directory for instance block failed!" << dendl;
    }
    //the next steps remove the entry from the ordered set and if needed the latest hash entry also in case of versioned buckets
    rgw::d4n::CacheBlock latest_block = block;
    latest_block.cacheObj.objName = c_obj->get_name();
    int retry = 3;
    while(retry) {
      retry--;
      //get latest entry
      ret = blockDir->get(dpp, &latest_block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << __func__ << "(): Failed to get latest entry in block directory for: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
      }
      if (latest_block.version == e->version) {
        //remove object entry from ordered set of versions
        if (c_obj->have_instance()) {
          blockDir->del(dpp, &latest_block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << __func__ << "(): Failed to queue del for latest hash entry: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
            return ret;
          }
        }
        //delete entry from ordered set of objects, as older versions would have been written to the backend store
        ret = bucketDir->zrem(dpp, e->bucket_id, c_obj->get_name(), y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << __func__ << "(): Failed to queue zrem for object entry: " << c_obj->get_name() << ", ret=" << ret << dendl;
          return ret;
        }
      }
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Removing object name: "<< c_obj->get_name() << " score: " << std::setprecision(std::numeric_limits<double>::max_digits10) << e->creationTime << " from ordered set" << dendl;
      rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
        .objName = c_obj->get_name(),
        .bucketName = c_obj->get_bucket()->get_bucket_id(),
      };
      ret = objDir->zremrangebyscore(dpp, &dir_obj, e->creationTime, e->creationTime, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << __func__ << "(): Failed to remove object from ordered set with error: " << ret << dendl;
        return ret;
      }
      break;
    }//end-while (retry)
  }
  return 0;
}

static bool is_transient_error(int ret)
{
  switch (-ret) {
    case ETIMEDOUT:
    case ECONNREFUSED:
    case EIO:
    case EAGAIN:
      return true;
    case ENOENT:
    case EACCES:
    case EINVAL:
    default:
      return false;
  }
}

/* This method "cleans" and writes back dirty data from the cache to the backend store.
   It also deletes invalid dirty data from cache backend.
*/
void LFUDAPolicy::cleaning(const DoutPrefixProvider* dpp, optional_yield y)
{
  const int interval = dpp->get_cct()->_conf->rgw_d4n_cache_cleaning_interval;
  std::optional<LFUDAObjEntry*> e;
  while(!quit) {
    ldpp_dout(dpp, 20) << __func__ << " : " << " Cache cleaning!" << dendl;
    bool invalid = false;
  
    ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "" << __LINE__ << "(): Before acquiring cleaning-lock" << dendl;
    std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
    //saving e till the entry expires and is processed so that the same entry is not picked by another thread
    if (!e.has_value()) {
      while (!quit && object_heap.empty()) {
        boost::system::error_code ec;
        cond->async_wait(l, y.get_yield_context()[ec]);
      }
      if (quit) break;
      e = object_heap.top();
      object_heap.pop();
    }
    State s = State::INIT;
    std::string obj_name = (*e)->obj_key.name;
    ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): e->key=" << (*e)->key << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->delete_marker=" << (*e)->delete_marker << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->version=" << (*e)->version << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_name=" << (*e)->bucket_name << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->bucket_id=" << (*e)->bucket_id << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->user=" << (*e)->user << dendl;
    ldpp_dout(dpp, 10) << __LINE__ << " " << __func__ << "(): e->obj_key=" << (*e)->obj_key << dendl;

    int diff = std::difftime(time(NULL), (*e)->creationTime);
    bool entry_expired = (diff >= interval);
    if (!entry_expired && !above_watermark) {
      ldpp_dout(dpp, 10) << __LINE__ << " " << __func__
                         << "(): entry not expired and below watermark, waiting on=" << (*e)->key << dendl;
      l.unlock();
      while (!quit && !above_watermark) {
        diff = (int)std::difftime(time(NULL), (*e)->creationTime);
        if (diff >= interval) break;     // entry expired while we slept

        boost::system::error_code ec;
        watermark_timer->expires_after(std::chrono::seconds(interval - diff));
        watermark_timer->async_wait(y.get_yield_context()[ec]);
      }
      continue;
    }
    //start cleaning, either when entry is expired or when cache filled space is above EVICTION_WATERMARK
    if (!(*e)->key.empty()) { // if block is dirty and written more than interval seconds ago
      ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): entry has expired= " << (*e)->key << dendl;
      auto p = o_entries_map.find((*e)->key);
      if (p == o_entries_map.end()) {
        e.reset();
	l.unlock();
	continue;
      }
      if (p->second.second == State::INVALID) {
	invalid = true;
      } else if (p->second.second == State::IN_PROGRESS) { //already being processed by another thread
        e.reset();
        l.unlock();
        continue;
      }
      //set to IN_PROGRESS irrespective of INVALID as this indicates that processing is in progress
      p->second.second = State::IN_PROGRESS;
      l.unlock();

      int ret = 0;
      // If the state is invalid, the blocks must be deleted from the cache rather than written to the backend.
      if (invalid) {
        s = State::INVALID;
        ret = do_delete(dpp, *e, interval, y);
      } else {
        s = State::INIT;
        ret = do_writeback(dpp, *e, y);
      }
      //retry logic upon failure
      if (ret < 0) {
        if (is_transient_error(ret) && (*e)->retry_count < MAX_CLEANING_RETRY) {
          std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
          //reset state
          auto p = o_entries_map.find((*e)->key);
          if (p != o_entries_map.end()) {
            p->second.second = s;
          }
          //put it back in heap
          (*e)->retry_count++;
          auto handle = object_heap.push(*e);
          (*e)->set_handle(handle);
          cond->notify(l);
          e.reset();
          continue;
        } else {
          //queue in failed entries queue
          std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
          failed_entries.push_back(*(*e));
        }
      }
      //remove from map and release memory for *e
      erase_dirty_object(dpp, (*e)->key, y);
      e.reset();
      {
        ldpp_dout(dpp, 10) <<__LINE__ << " " << __func__ << "(): promoting next version" << dendl;
        std::unique_lock<std::mutex> l(lfuda_cleaning_lock);
        auto v_it = per_obj_versions.find(obj_name);
        
        if (v_it != per_obj_versions.end() && !v_it->second.empty()) {
          auto next_it = v_it->second.begin();
          LFUDAObjEntry* next = next_it->second;
          auto handle = object_heap.push(next);
          next->set_handle(handle);

          ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                              << "(): promoted next version key="
                              << next->key
                              << " creationTime="
                              << next->creationTime
                              << dendl;
          v_it->second.erase(next_it);
          cond->notify(l);
        } else if (v_it != per_obj_versions.end()) {
          // No more versions — cleanup
          per_obj_versions.erase(v_it);
          ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__
                              << "(): all versions processed for obj="
                              << obj_name << dendl;
        }
      }
    } // end-if !e->key.empty()
  } //end-while true
}

void LFUDAPolicy::localweight_writer(const DoutPrefixProvider* dpp)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Starting thread " << dendl;
  auto TIMEOUT_DURATION = std::chrono::seconds(dpp->get_cct()->_conf->rgw_d4n_localweight_processing_interval);
  while (!lw_quit.load()) {
    std::unordered_map<std::string, uint64_t> temp;
    bool woke_up = false;
    //sleep for some duration or till size crosses 10K before processing
    {
      std::unique_lock<std::mutex> wait_lock(lfuda_lock);
      woke_up = lw_cond.wait_for(wait_lock, TIMEOUT_DURATION, [this] {
                return updated_blocks.size() >= LOCALWEIGHT_BATCH_SIZE || lw_quit.load();
      });
      if (lw_quit.load()) {
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Quit signal received, exiting" << dendl;
          break;
      }
      if (!updated_blocks.empty()) {
          updated_blocks.swap(temp);
          if (woke_up) {
              ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Woke up due to size threshold, processing " << temp.size() << " items" << dendl;
          } else {
              ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Woke up due to timeout, processing " << temp.size() << " items" << dendl;
          }
      }
    } //lock released here
    if (!temp.empty()) {
      ldpp_dout(dpp, 5) << "LFUDAPolicy::" << __func__ << "(): Processing batch of " << temp.size() << " items" << dendl;
      for (auto& it : temp) {
        if (lw_quit.load()) {
          ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Quit signal received, exiting" << lw_quit << dendl;
          break;
        }
        auto& key = it.first;
        auto localWeight = it.second;
        ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method called for key: " << key << dendl;
        int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(localWeight), y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
        }
      } //end-for
      ldpp_dout(dpp, 5) << "LFUDAPolicy::" << __func__ << "(): Finished processing batch" << dendl;
    } //end-if
  }//end-while
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Thread exiting" << dendl;
}

int LRUPolicy::exist_key(const std::string& key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  uint64_t freeSpace = cacheDriver->get_free_space(dpp, y);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to delete data from the cache backend, ret=" << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp, y);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, const rgw_user user, const std::string& bucketName, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  bool is_dirty = false;
  if (dirty.has_value()) {
    is_dirty = dirty.value();
  }
  Entry* e = new Entry(key, offset, len, version, is_dirty, 0, user, bucketName);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  ObjEntry* e = new ObjEntry(key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool LRUPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }
  o_entries_map.erase(p);
  return true;
}

bool LRUPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}

} } // namespace rgw::d4n
