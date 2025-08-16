
#include "fdb/fdb.h"

#include "rgw/driver/fdb/fdb-cache-driver.h"

namespace rgw::cache {

FDB_CacheDriver::FDB_CacheDriver()
{
}

FDB_CacheDriver::~FDB_CacheDriver()
{
}

int FDB_CacheDriver::initialize(const DoutPrefixProvider* dpp)
{
 return -1;
}

int FDB_CacheDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
 return -1;
}

rgw::AioResultList FDB_CacheDriver::get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
 return {};
}

rgw::AioResultList FDB_CacheDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id)
{
 return {};
}

int FDB_CacheDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
 return -1;
}

int FDB_CacheDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
 return -1;
}

Partition FDB_CacheDriver::get_current_partition_info(const DoutPrefixProvider* dpp)
{
 return {};
}

uint64_t FDB_CacheDriver::get_free_space(const DoutPrefixProvider* dpp)
{
 return -1;
}

void FDB_CacheDriver::shutdown()
{
}

int FDB_CacheDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
 return -1;
}

} // namespace rgw::cache

