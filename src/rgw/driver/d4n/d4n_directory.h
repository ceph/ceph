#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <concepts>

namespace rgw { namespace d4n {

template<typename T>
  concept SeqContainer = requires(T& t, typename T::value_type v) {
      t.push_back(v);
  };

using boost::redis::connection;
class RedisPool {
public:
    RedisPool(boost::asio::io_context* ioc, const boost::redis::config& cfg, std::size_t size)
        :  m_ioc(ioc),m_cfg(cfg) {
        for (std::size_t i = 0; i < size; ++i) {
            // Each connection gets its own strand
            auto strand = boost::asio::make_strand(*m_ioc);
            auto conn = std::make_shared<connection>(strand);
            m_pool.push_back(conn);
        }
    }

    ~RedisPool() {
      cancel_all();
    }

    std::shared_ptr<connection> acquire() {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);

	if (!m_is_pool_connected) {
		for(auto& it:m_pool) {
	    		auto conn = it;
	    		conn->async_run(m_cfg, {}, boost::asio::consign(boost::asio::detached, conn));
		}
	    m_is_pool_connected = true;
	}

        if (m_pool.empty()) {
		maybe_warn_about_blocking(nullptr);
		//wait until m_pool is not empty
		m_cond_var.wait(lock, [this] { return !m_pool.empty(); });
        } 
        auto conn = m_pool.front();
        m_pool.pop_front();
        return conn;
    }

    void release(std::shared_ptr<connection> conn) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        m_pool.push_back(conn);
	// Notify one waiting thread that a connection is available
	m_cond_var.notify_one();
    }

    int current_pool_size() const {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        return m_pool.size();
    }

    void cancel_all() {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        if(m_is_pool_connected) {
	for(auto& conn : m_pool) {
		conn->cancel();
        }
      }
    }

private:
    boost::asio::io_context* m_ioc;
    boost::redis::config m_cfg;
    std::deque<std::shared_ptr<connection>> m_pool;
    mutable std::mutex m_aquire_release_mtx;
    std::condition_variable m_cond_var;
    bool m_is_pool_connected{false};
};

class MultiRedisPoolManager {
public:
    MultiRedisPoolManager(boost::asio::io_context* ioc, 
                         const std::vector<std::string>& hosts,
                         const std::vector<std::string>& ports,
                         std::size_t pool_size_per_host)
        : m_ioc(ioc), m_pool_size_per_host(pool_size_per_host) {

        for (size_t i = 0; i < hosts.size(); ++i) {
            boost::redis::config cfg;
            cfg.addr.host = hosts[i];
            cfg.addr.port = ports[i];
            
            auto pool = std::make_shared<RedisPool>(m_ioc, cfg, pool_size_per_host);
            m_pools.push_back(pool);
        }
    }

    ~MultiRedisPoolManager() {
      cancel_all();
    }

    std::shared_ptr<connection> acquire_connection(size_t pool_index) {
      if (is_valid_index(pool_index)) {
        return m_pools[pool_index]->acquire();
      }
      return nullptr;
    }

    void release_connection(size_t pool_index, std::shared_ptr<connection> conn) {
      if (is_valid_index(pool_index)) {
        m_pools[pool_index]->release(conn);
      }
    }

    // Get total number of pools (hosts)
    size_t pool_count() const {
        return m_pools.size();
    }

    // Get current pool size for a specific host by index
    int pool_size(size_t pool_index) const {
        if (pool_index >= m_pools.size()) {
            return -EINVAL;
        }
        return m_pools[pool_index]->current_pool_size();
    }

    bool is_valid_index(size_t pool_index) const {
        return pool_index < m_pools.size();
    }

    // Cancel all connections across all pools
    void cancel_all() {
        for (auto& pool : m_pools) {
            if (pool) {
                pool->cancel_all();
            }
        }
    }
private:
    boost::asio::io_context* m_ioc;
    std::size_t m_pool_size_per_host;
    std::vector<std::shared_ptr<RedisPool>> m_pools;
};

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

enum class ObjectFields { // Fields stored in object directory 
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName
};

enum class BlockFields { // Fields stored in block directory 
  BlockID,
  Version, 
  DeleteMarker,
  Size,
  GlobalWeight,
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty{false};
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string etag; //etag needed for list objects
  uint64_t size; //total object size (and not block size), needed for list objects
  std::string user_id; // id of user, needed for list object versions
  std::string display_name; // display name of owner, needed for list object versions
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  bool deleteMarker{false};
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  /* Blocks use the cacheObj's dirty and hostsList metadata to store their dirty flag values and locations in the block directory. */
};

class Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool
    	void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      	redis_pool = pool;
    }
    void set_redis_pool_manager(MultiRedisPoolManager* pool_manager) {
      	redis_pool_manager = pool_manager;
    }
    Directory() {}
    std::optional<MultiRedisPoolManager> connectClient(const DoutPrefixProvider* dpp,
                                                          boost::asio::io_context& io_context,
                                                          const std::vector<std::string>& hosts,
                                                          const std::vector<std::string>& ports,
                                                          size_t connection_pool_size);
    int findClient(const DoutPrefixProvider* dpp, std::string key);
  protected:
    MultiRedisPoolManager* redis_pool_manager{nullptr};
  private:
    std::pair<std::vector<std::string>, std::vector<int>> parseHostPorts(const DoutPrefixProvider* dpp, std::string_view address);
};

class Pipeline {
  public:
    Pipeline(std::vector<std::shared_ptr<connection>>& connections, MultiRedisPoolManager* redis_pool_manager) : connections(connections), redis_pool_manager(redis_pool_manager) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request(int index) { return requests[index]; }
    bool has_requests_for_index(int index) const {
      return requests.contains(index);
    }

  private:
    std::vector<std::shared_ptr<connection>>& connections;
    MultiRedisPoolManager* redis_pool_manager{nullptr};
    std::unordered_map<int, request> requests;
    bool pipeline_mode{false};
};

class BucketDirectory: public Directory {
  public:
    BucketDirectory(std::vector<std::shared_ptr<connection>>& connections) : connections(connections) {}
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y);
    int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y);

  private:
    std::vector<std::shared_ptr<connection>>& connections;
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(std::vector<std::shared_ptr<connection>>& connections) : connections(connections) {}

    int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y); /* If nx is true, set only if key doesn't exist */
    int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y);
    int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& idx, optional_yield y);
    //Return value is the incremented value, else return error
    int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

  private:
    std::vector<std::shared_ptr<connection>> connections;

    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(std::vector<std::shared_ptr<connection>>& connections) : connections(connections) {}
    
    int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);

    //Pipelined version of set
    int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr);
    int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    //Pipelined version of get using boost::redis::response for list bucket
    template <size_t N = 100>
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    //Pipelined version of get using boost::redis::generic_response
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y);
    int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y);

  private:
    std::vector<std::shared_ptr<connection>> connections;
    std::string build_index(CacheBlock* block);

    template<SeqContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y);
};

} } // namespace rgw::d4n
