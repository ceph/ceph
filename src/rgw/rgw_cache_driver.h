#pragma once

#include "rgw_common.h"

namespace rgw { namespace cache {

struct Partition {
    std::string name;
    std::string type;
    std::string location;
    uint64_t size;
};

struct Entry {
  std::string key;
  off_t offset;
  uint64_t len;
  int localWeight;
};

class CacheDriver {
  public:
    CacheDriver() {}
    virtual ~CacheDriver() = default;

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) = 0;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) = 0;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) = 0;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) = 0;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key) = 0;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) = 0;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) = 0;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) = 0;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) = 0;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) = 0;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val) = 0;

    /* Entry */
    virtual bool key_exists(const DoutPrefixProvider* dpp, const std::string& key) = 0;
    virtual std::vector<Entry> list_entries(const DoutPrefixProvider* dpp) = 0;
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) = 0;

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) = 0;
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) = 0;
};

} } // namespace rgw::cache

