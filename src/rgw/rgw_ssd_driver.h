#pragma once

#include "rgw_common.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace cal { //cal stands for Cache Abstraction Layer

class SSDDriver : public CacheDriver {
public:
    SSDDriver(Partition& _partition_info);
    virtual ~SSDDriver();

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) = 0;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) override;
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
    virtual bool key_exists(const DoutPrefixProvider* dpp, const std::string& key) override { return entries.count(key) != 0; }
    virtual size_t get_num_entries(const DoutPrefixProvider* dpp) override { return entries.size(); }

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override { return free_space; }
    static std::optional<Partition> get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type);
    static std::vector<Partition> list_partitions(const DoutPrefixProvider* dpp);

protected:
    static std::unordered_map<std::string, Partition> partitions;
    std::unordered_map<std::string, Entry> entries;
    Partition partition_info;
    uint64_t free_space;

    int add_partition_info(Partition& info);
    int remove_partition_info(Partition& info);
    int insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len);
    int remove_entry(const DoutPrefixProvider* dpp, std::string key);
    std::optional<Entry> get_entry(const DoutPrefixProvider* dpp, std::string key);
    std::unordered_map<std::string, Entry>& list_entries(const DoutPrefixProvider* dpp);
};

} } // namespace rgw::cal

