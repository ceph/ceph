#include "d4n_policy.h"

#include <boost/lexical_cast.hpp>
#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;
  boost::redis::request req;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, Response& resp)
  {
    conn->async_exec(req, resp, boost::asio::consign(std::move(handler), conn));
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn), req}, token, resp);
}

template <typename T>
void redis_exec(std::shared_ptr<connection> conn, boost::system::error_code& ec, boost::redis::request& req, boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int LFUDAPolicy::set_age(int age, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "age", std::to_string(age));

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::get_age(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "age");

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return -1;
  } catch(std::exception &e) {
    return -1;
  }

  if (!std::get<0>(resp).value()) {
    if (set_age(0, y)) /* Initialize age */
      return 0;
    else
      return -1;
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn, ec, req, value, y);

    if (ec)
      return -1;

    return std::stoi(std::get<0>(value).value());
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::set_min_avg_weight(size_t weight, std::string cacheLocation, optional_yield y) {
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "minAvgWeight:cache", cacheLocation);

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};
  } catch(std::exception &e) {
    return -1;
  }
  
  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("HSET", "lfuda", "minAvgWeight:weight", boost::lexical_cast<int>(weight));

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return {};

    return std::get<0>(resp).value(); /* Returns number of fields set */
  } catch(std::exception &e) {
    return -1;
  }
}

int LFUDAPolicy::get_min_avg_weight(optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", "lfuda", "minAvgWeight:cache");

    redis_exec(conn, ec, req, resp, y);

    if (ec)
      return -1;
  } catch(std::exception &e) {
    return -1;
  }

  if (!std::get<0>(resp).value()) {
    if (set_min_avg_weight(0, dir->cct->_conf->rgw_local_cache_address, y)) { /* Initialize minimum average weight */
      return 0;
    } else {
      return -1;
    }
  }

  try { 
    boost::system::error_code ec;
    response<std::string> value;
    request req;
    req.push("HGET", "lfuda", "minAvgWeight:weight");
      
    redis_exec(conn, ec, req, value, y);

    if (ec)
      return -1;

    return std::stoi(std::get<0>(value).value());
  } catch(std::exception &e) {
    return -1;
  }
}

CacheBlock LFUDAPolicy::find_victim(const DoutPrefixProvider* dpp, optional_yield y) {
  if (entries_heap.empty())
    return {};

  /* Get victim cache block */
  std::string key = entries_heap.top()->key;
  CacheBlock victim;

  victim.cacheObj.bucketName = key.substr(0, key.find('_')); 
  key.erase(0, key.find('_') + 1);
  victim.cacheObj.objName = key.substr(0, key.find('_'));
  victim.blockID = entries_heap.top()->offset;
  victim.size = entries_heap.top()->len;

  if (dir->get(&victim, y) < 0) {
    return {};
  }

  return victim;
}

void LFUDAPolicy::shutdown() {
  dir->shutdown();
  
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}

int LFUDAPolicy::exist_key(std::string key) {
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

#if 0
int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheDriver, optional_yield y) {
  response<std::string> resp;
  int age = get_age(y);

  if (exist_key(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size))) { /* Local copy */
    auto it = entries_map.find(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size));
    it->second->localWeight += age;
    return cacheDriver->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
  } else {
    if (eviction(dpp, block->size, cacheDriver, y) < 0)
      return -1; // what if eviction turns into infinite loop? -Sam

    int exists = dir->exist_key(block, y);
    if (exists > 0) { /* Remote copy */
      if (dir->get(block, y) < 0) {
	return -1;
      } else {
	if (!block->hostsList.empty()) { 
	  block->globalWeight += age;
	  
	  if (dir->update_field(block, "globalWeight", std::to_string(block->globalWeight), y) < 0) {
	    return -1;
	  } else {
	    return 0;
	  }
	} else {
          return -1;
        }
      }
    } else if (!exists) { /* No remote copy */
      // localWeight += age;
      //return cacheDriver->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
      return 0;
    } else {
      return -1;
    }
  }
}
#endif

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) {
    CacheBlock victim = find_victim(dpp, y);

    if (victim.cacheObj.objName.empty()) {
      ldpp_dout(dpp, 10) << "RGW D4N Policy: Could not retrieve victim block" << dendl;
      return -1;
    }

    std::string key = victim.cacheObj.bucketName + "_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(victim.size);
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      return -1;
    }

    int avgWeight = get_min_avg_weight(y);
    if (avgWeight < 0) {
      return -1;
    }

    if (victim.hostsList.size() == 1 && victim.hostsList[0] == dir->cct->_conf->rgw_local_cache_address) { /* Last copy */
      if (victim.globalWeight) {
	it->second->localWeight += victim.globalWeight;

	for (auto& entry : entries_heap) {
	  if (entry->key == key) {
	    (*(entry->handle))->localWeight = it->second->localWeight;
	    entries_heap.increase(entry->handle);
	  }
	}

	if (cacheDriver->set_attr(dpp, key, "localWeight", std::to_string(it->second->localWeight), y) < 0) {
	  return -1;
	}

	victim.globalWeight = 0;
	if (dir->update_field(&victim, "globalWeight", std::to_string(victim.globalWeight), y) < 0) {
	  return -1;
	}
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
      }
    }

    victim.globalWeight += it->second->localWeight;
    if (dir->update_field(&victim, "globalWeight", std::to_string(victim.globalWeight), y) < 0) {
      return -1;
    }

    if (dir->remove_host(&victim, dir->cct->_conf->rgw_local_cache_address, y) < 0) {
      return -1;
    } else {
      if (cacheDriver->del(dpp, key, y) < 0) {
        return -1;
      } else {
	ldpp_dout(dpp, 10) << "RGW D4N Policy: Block " << victim.cacheObj.objName << " has been evicted." << dendl;

	uint64_t num_entries = entries_map.size();

	if (!avgWeight) {
	  if (set_min_avg_weight(0, dir->cct->_conf->rgw_local_cache_address, y) < 0) // Where else must this be set? -Sam 
	    return -1;
	} else {
	  if (set_min_avg_weight(avgWeight - (it->second->localWeight/num_entries), dir->cct->_conf->rgw_local_cache_address, y) < 0) // Where else must this be set? -Sam 
	    return -1;
	} 

	int age = get_age(y);
	age = std::max(it->second->localWeight, age);
	if (set_age(age, y) < 0)
	  return -1;
      }
    }

    freeSpace = cacheDriver->get_free_space(dpp);
  }
  
  return 0;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, optional_yield y)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;

  int age = get_age(y); 
  int localWeight = age;
  auto entry = find_entry(key);
  if (entry != nullptr) { 
    entry->localWeight += age;
    localWeight = entry->localWeight;
  }  

  erase(dpp, key);
  
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, localWeight);
  handle_type handle = entries_heap.push(e);
  e->set_handle(handle);
  entries_map.emplace(key, e);

  if (cacheDriver->set_attr(dpp, key, "localWeight", std::to_string(localWeight), y) < 0) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::update:: " << __func__ << "(): Cache driver set_attr method failed." << dendl;
  }
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  for (auto const& it : entries_heap) {
    if (it->key == key) {
      entries_heap.erase(it->handle);
      break;
    }
  }

  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  entries_map.erase(p);

  return false;
}

int LRUPolicy::exist_key(std::string key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
  uint64_t freeSpace = cacheDriver->get_free_space(dpp);

  while (freeSpace < size) {
    const std::lock_guard l(lru_lock);
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    cacheDriver->delete_data(dpp, p.key, null_yield);

    freeSpace = cacheDriver->get_free_space(dpp);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, optional_yield y)
{
  erase(dpp, key);

  Entry *e = new Entry(key, offset, len, version);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  const std::lock_guard l(lru_lock);
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}

} } // namespace rgw::d4n
