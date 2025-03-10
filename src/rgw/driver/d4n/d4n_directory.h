#pragma once

#include "rgw_common.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <set>

namespace rgw { namespace d4n {

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
    enum class redis_operation_type {
	NONE,READ_OP,WRITE_OP
    };
    enum class TrxState {
			NONE,
			STARTED,
			ENDED
		} trxState{TrxState::NONE};

    std::string m_trx_id;
    int start_trx(const DoutPrefixProvider* dpp,std::shared_ptr<connection> , optional_yield y);
    int end_trx(const DoutPrefixProvider* dpp,std::shared_ptr<connection> , optional_yield y);
    bool is_trx_started(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn,std::string &key,redis_operation_type op, optional_yield y);
    std::string get_end_trx_script(const DoutPrefixProvider* dpp, std::shared_ptr<connection> conn, optional_yield y);
    int get_trx_id(const DoutPrefixProvider* dpp,std::shared_ptr<connection> conn, optional_yield y);

    int clone_key_for_transaction(std::string key_source, std::string key_destination, std::shared_ptr<connection> conn, optional_yield y);
    std::string m_evalsha_clone_key;
    std::string m_evalsha_end_trx;

    std::set<std::string> m_temp_read_keys;//temporary key for use in transactions
    std::set<std::string> m_temp_write_keys;//temporary key for use in transactions
    std::set<std::string> m_temp_test_write_keys;//temporary key for use in transactions
    std::string m_original_key;//original key, used to restore the key from Redis
    std::string m_temp_key_read;//temporary key for use in transactions
    std::string m_temp_key_write;//temporary key for use in transactions
    std::string m_temp_key_test_write;//temporary key for use in transactions
};

class BucketDirectory: public Directory {
  public:
    BucketDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}
    int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, bool multi=false);
    int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y, bool multi=false);
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
    int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, bool multi=false);
    int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y);
    int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y);
    int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y, bool multi=false);
    int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y, bool multi=false);
    int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y);
    //Return value is the incremented value, else return error
    int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheObj* object);
    std::string create_temp_key(std::string key);//return unique key for temporary use
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}
    
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

    std::shared_ptr<connection> get_connection() {return conn;}	
  private:
    std::shared_ptr<connection> conn;
    std::string build_index(CacheBlock* block);
    std::string create_temp_key(std::string key);//return unique key for temporary use
};

} } // namespace rgw::d4n
