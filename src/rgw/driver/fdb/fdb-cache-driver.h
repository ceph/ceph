#ifndef RGW_D4N_FDB_CACHE_DRIVER
 #define RGW_D4N_FDB_CACHE_DRIVER

#include "fdb/fdb.h"
#include "driver/cache/rgw_cache_driver.h"

namespace rgw::cache {

struct FDB_CacheDriver : CacheDriver
{
  public:
    FDB_CacheDriver();

    virtual ~FDB_CacheDriver();

    int initialize(const DoutPrefixProvider* dpp) override;

    int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override;
    int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;

    rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
    rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override;
    int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) override;
    int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;

    int rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y) override;

    int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
    int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;

    int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
    int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
  
    int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
    int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;

    Partition get_current_partition_info(const DoutPrefixProvider* dpp) override;
    uint64_t get_free_space(const DoutPrefixProvider* dpp) override;

    void shutdown() override;
  
    int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;
};

} // namespace rgw::cache

#endif
