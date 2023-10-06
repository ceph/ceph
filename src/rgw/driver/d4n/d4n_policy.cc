#include "d4n_policy.h"

#include <boost/lexical_cast.hpp>
#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

std::string build_index(std::string bucketName, std::string oid, uint64_t offset, uint64_t size) {
  return bucketName + "_" + oid + "_" + std::to_string(offset) + "_" + std::to_string(size); 
}

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
  if (entries_map.empty())
    return {};

  auto it = std::min_element(std::begin(entries_map), std::end(entries_map),
			      [](const auto& l, const auto& r) { return l.second->localWeight < r.second->localWeight; });

  /* Get victim cache block */
  std::string key = it->second->key;
  CacheBlock victim;

  victim.cacheObj.bucketName = key.substr(0, key.find('_')); 
  key.erase(0, key.find('_') + 1);
  victim.cacheObj.objName = key.substr(0, key.find('_'));
  victim.blockID = it->second->offset;
  victim.size = it->second->len;

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

int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y) {
  response<std::string> resp;
  int age = get_age(y);

  if (exist_key(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size))) { /* Local copy */
    auto it = entries_map.find(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size));
    it->second->localWeight += age;
    return cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
  } else {
    uint64_t freeSpace = cacheNode->get_free_space(dpp);

    while (freeSpace < block->size) { /* Not enough space in local cache */
      if (int ret = eviction(dpp, cacheNode, y) > 0)
        freeSpace += ret;
      else 
        return -1;
    }

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
      // how to get bufferlist data? -Sam
      // do I need to add the block to the local cache here? -Sam
      // update hosts list for block as well?
      // insert entry here? -Sam
      // localWeight += age;
      //return cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y);
      return 0;
    } else {
      return -1;
    }
  }
}

uint64_t LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y) {
  CacheBlock victim = find_victim(dpp, y);

  if (victim.cacheObj.objName.empty()) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: Could not find victim block" << dendl;
    return 0; /* Return zero for failure */
  }

  std::string key = build_index(victim.cacheObj.bucketName, victim.cacheObj.objName, victim.blockID, victim.size);
  auto it = entries_map.find(key);
  if (it == entries_map.end()) {
    return 0;
  }

  int avgWeight = get_min_avg_weight(y);
  if (avgWeight < 0) {
    return 0;
  }

  if (victim.hostsList.size() == 1 && victim.hostsList[0] == dir->cct->_conf->rgw_local_cache_address) { /* Last copy */
    if (victim.globalWeight) {
      it->second->localWeight += victim.globalWeight;
      if (cacheNode->set_attr(dpp, victim.cacheObj.objName, "localWeight", std::to_string(it->second->localWeight), y) < 0) {
	return 0;
      }

      victim.globalWeight = 0;
      if (dir->update_field(&victim, "globalWeight", std::to_string(victim.globalWeight), y) < 0) {
	return 0;
      }
    }

    if (it->second->localWeight > avgWeight) {
      // TODO: push victim block to remote cache
    }
  }

  victim.globalWeight += it->second->localWeight;
  if (dir->update_field(&victim, "globalWeight", std::to_string(victim.globalWeight), y) < 0) { // just have one update? -Sam
    return 0;
  }

  ldpp_dout(dpp, 10) << "RGW D4N Policy: Block " << victim.cacheObj.objName << " has been evicted." << dendl;

  if (cacheNode->del(dpp, key, y) < 0 && dir->remove_host(&victim, dir->cct->_conf->rgw_local_cache_address, y) < 0) {
    return 0;
  } else {
    uint64_t num_entries = entries_map.size();

    if (!avgWeight) {
      if (set_min_avg_weight(0, dir->cct->_conf->rgw_local_cache_address, y) < 0) // Where else must this be set? -Sam 
	return 0;
    } else {
      if (set_min_avg_weight(avgWeight - (it->second->localWeight/num_entries), dir->cct->_conf->rgw_local_cache_address, y) < 0) { // Where else must this be set? -Sam 
	return 0;
    } 
      int age = get_age(y);
      age = std::max(it->second->localWeight, age);
      if (set_age(age, y) < 0)
	return 0;
    }
  }

  return victim.size; // this doesn't account for the additional attributes that were removed and need to be set with the new block -Sam
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  erase(dpp, key);

  int age = get_age(y);
  assert(age > -1);
  
  LFUDAEntry *e = new LFUDAEntry(key, offset, len, version, age);
  entries_lfuda_list.push_back(*e);
  entries_map.emplace(key, e);
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  entries_map.erase(p);
  entries_lfuda_list.erase_and_dispose(entries_lfuda_list.iterator_to(*(p->second)), LFUDA_Entry_delete_disposer());
  return true;
}

int LRUPolicy::exist_key(std::string key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  uint64_t freeSpace = cacheNode->get_free_space(dpp);
  while(freeSpace < block->size) {
    freeSpace = eviction(dpp, cacheNode, y);
  }
  return 0;
}

uint64_t LRUPolicy::eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = entries_lru_list.front();
  entries_map.erase(entries_map.find(p.key));
  entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
  cacheNode->delete_data(dpp, p.key, null_yield);
  return cacheNode->get_free_space(dpp);
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, rgw::cache::CacheDriver* cacheNode, optional_yield y)
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

int PolicyDriver::init() {
  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy(io);
    return 0;
  } else if (policyName == "lru") {
    cachePolicy = new LRUPolicy();
    return 0;
  }

  return -1;
}

} } // namespace rgw::d4n
