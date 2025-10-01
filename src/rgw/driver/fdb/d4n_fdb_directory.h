#pragma once

#include "rgw_common.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

// I had split this out into another header-- and we should
// probably still do that... #include "drivers/shared/d4n_data.h"
// ...unfortunately, I hadn't realized there was active work on this also, so
// things are changing out from under me already!

#include "driver/d4n/rgw_sal_d4n.h"


/* JFW: I've mostly just copied this wholesale from D4N-- I suspect it will need significant re-imagining as the Redis-isms infuse quite
a way into the fabric of the library. */

#include "fdb/fdb.h"

namespace rgw::d4n {

namespace net = boost::asio;
namespace redis = boost::redis;
namespace lfdb = ceph::libfdb;

lfdb::database_handle global_fdb_dbh;

class FDB_BucketDirectory: public Directory 
{
  lfdb::database_handle dbh; 

  public:
    FDB_BucketDirectory(lfdb::database_handle dbh) 
     : dbh(dbh) 
    {}

  public:
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, bool multi=false);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y, bool multi=false);
    int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y);
    int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y);
    int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y);
};

class FDB_ObjectDirectory: public Directory 
{
    lfdb::database_handle dbh;
  
  public:
    FDB_ObjectDirectory(lfdb::database_handle dbh) 
     : dbh(dbh) 
    {}

    int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y); /* If nx is true, set only if key doesn't exist */
    int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, bool multi=false);
    int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y, bool multi=false);
    int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y, bool multi=false);
    int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y);
    //Return value is the incremented value, else return error
    int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

  private:
    std::string build_index(CacheObj* object);
};

class FDB_BlockDirectory : public Directory 
{
   lfdb::database_handle dbh;

  public:
    FDB_BlockDirectory(lfdb::database_handle dbh) 
     : dbh(dbh) 
    {}
    
    int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    //Pipelined version of get for list bucket
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, bool multi=false);
    int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y);
    int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y);
    int zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y, bool multi=false);
    int zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y);
    int watch(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    //Move MULTI, EXEC and DISCARD to directory? As they do not operate on a key
    int exec(const DoutPrefixProvider* dpp, std::vector<std::string>& responses, optional_yield y);
    int multi(const DoutPrefixProvider* dpp, optional_yield y);
    int discard(const DoutPrefixProvider* dpp, optional_yield y);
    int unwatch(const DoutPrefixProvider* dpp, optional_yield y);

  private:
    std::string build_index(CacheBlock* block);
};

} // namespace rgw::d4n
