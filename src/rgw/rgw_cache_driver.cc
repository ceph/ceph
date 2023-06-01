#include "rgw_cache_driver.h"

namespace rgw { 

int CacheDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  return 0;
}

bool CacheDriver::key_exists(const DoutPrefixProvider* dpp, std::string& key) {
  return false;
}

int CacheDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) {
  return 0;
}

int CacheDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) {
  return 0;
}

/*rgw::AioResultList CacheDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) {
  return nullptr;
}

rgw::AioResultList CacheDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, bufferlist& bl, uint64_t len, uint64_t cost, uint64_t id) {
  return nullptr;
}*/

int CacheDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) {
  return 0;
}

int CacheDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string&	key) {
  return 0;
}

int CacheDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  return 0;
}

int CacheDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  return 0;
}

int CacheDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) {
  return 0;
}

std::string CacheDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) {
  return {};
}

int CacheDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val) {
  return 0;
}

} // namespace rgw::sal
