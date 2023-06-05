#include "rgw_ssd_driver.h"

namespace rgw { namespace cal {

std::optional<Partition> SSDDriver::get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type)
{
    std::string key = name + type;
    auto iter = partitions.find(key);
    if (iter != partitions.end()) {
        return iter->second;
    }

    return std::nullopt;
}

std::vector<Partition> SSDDriver::list_partitions(const DoutPrefixProvider* dpp)
{
    std::vector<Partition> partitions_v;
    for (auto& it : SSDDriver::partitions) {
        partitions_v.emplace_back(it.second);
    }
    return partitions_v;
}

int SSDDriver::add_partition_info(Partition& info)
{
    std::string key = info.name + info.type;
    auto ret = partitions.emplace(key, info);
    return ret.second;
}

int SSDDriver::remove_partition_info(Partition& info)
{
    std::string key = info.name + info.type;
    return partitions.erase(key);
}

int SSDDriver::insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len)
{
    auto ret = entries.emplace(key, Entry(key, offset, len));
    return ret.second;
}

int SSDDriver::remove_entry(const DoutPrefixProvider* dpp, std::string key)
{
    return entries.erase(key);
}

std::optional<Entry> SSDDriver::get_entry(const DoutPrefixProvider* dpp, std::string key)
{
    auto iter = entries.find(key);
    if (iter != entries.end()) {
        return iter->second;
    }

    return std::nullopt;
}

std::unordered_map<std::string, Entry>& SSDDriver::list_entries(const DoutPrefixProvider* dpp)
{
    return entries;
}

SSDDriver::SSDDriver(Partition& _partition_info) : CacheDriver(_partition_info), partition_info(_partition_info)
{
    add_partition_info(partition_info);
}

SSDDriver::~SSDDriver()
{
    remove_partition_info(partition_info);
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs)
{
    //TODO - write to file

    return insert_entry(dpp, key, 0, len);
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key)
{
    //TODO - delete file

    return remove_entry(dpp, key);
}

} } // namespace rgw::cal
