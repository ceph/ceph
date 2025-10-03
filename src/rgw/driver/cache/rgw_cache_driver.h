#pragma once

#include "rgw_common.h"
#include "rgw_aio.h"

constexpr char RGW_CACHE_ATTR_MTIME[] = "user.rgw.mtime";
constexpr char RGW_CACHE_ATTR_EPOCH[] = "user.rgw.epoch";
constexpr char RGW_CACHE_ATTR_OBJECT_SIZE[] = "user.rgw.object_size";
constexpr char RGW_CACHE_ATTR_ACCOUNTED_SIZE[] = "user.rgw.accounted_size";
constexpr char RGW_CACHE_ATTR_MULTIPART[] = "user.rgw.multipart";
constexpr char RGW_CACHE_ATTR_OBJECT_NS[] = "user.rgw.object_ns";
constexpr char RGW_CACHE_ATTR_BUCKET_NAME[] = "user.rgw.bucket_name";
constexpr char RGW_CACHE_ATTR_VERSION_ID[] = "user.rgw.version_id";
constexpr char RGW_CACHE_ATTR_SOURC_ZONE[] = "user.rgw.source_zone";
constexpr char RGW_CACHE_ATTR_LOCAL_WEIGHT[] = "user.rgw.localWeight";
constexpr char RGW_CACHE_ATTR_DELETE_MARKER[] = "user.rgw.deleteMarker";
constexpr char RGW_CACHE_ATTR_INVALID[] = "user.rgw.invalid";
constexpr char RGW_CACHE_ATTR_DIRTY[] = "user.rgw.dirty";

constexpr char CACHE_DELIM = '#';

namespace rgw { namespace cache {

typedef std::function<void(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    time_t creationTime, const rgw_user user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, optional_yield y, std::string& restore_val)> ObjectDataCallback;

typedef std::function<void(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version,
        bool dirty, optional_yield y, std::string& restore_val)> BlockDataCallback;

struct Partition {
  std::string name;
  std::string type;
  std::string location;
  uint64_t size;
};

class CacheDriver {
  public:
    CacheDriver() {}
    virtual ~CacheDriver() = default;

    virtual int initialize(const DoutPrefixProvider* dpp) = 0;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) = 0;
    virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) = 0;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) = 0;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) = 0;
    virtual int rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y) = 0;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) = 0;
    virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) = 0;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) = 0;

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) = 0;
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) = 0;

    /* Data Recovery from Cache */
    virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) = 0;
};

} } // namespace rgw::cache

