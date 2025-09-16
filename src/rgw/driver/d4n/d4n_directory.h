#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"

#include "driver/shared/d4n_data.h"

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

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

class Directory {
  public:
	std::shared_ptr<RedisPool> redis_pool{nullptr}; // Redis connection pool
    	void set_redis_pool(std::shared_ptr<RedisPool> pool) {
      	redis_pool = pool;
    }
    Directory() {}
};

class Pipeline {
  public:
    Pipeline(std::shared_ptr<connection>& conn, std::shared_ptr<RedisPool> redis_pool) : conn(conn), redis_pool(redis_pool) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request() { return req; }

  private:
    std::shared_ptr<connection> conn;
    std::shared_ptr<RedisPool> redis_pool{nullptr};
    request req;
    bool pipeline_mode{false};
};

class BucketDirectory: public Directory {
  public:
    BucketDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y);
    int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y);

  private:
    std::shared_ptr<connection> conn;
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}

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
    int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y);
    //Return value is the incremented value, else return error
    int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}
    
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
    std::shared_ptr<connection> conn;
    std::string build_index(CacheBlock* block);

    template<SeqContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y);
};

} } // namespace rgw::d4n
