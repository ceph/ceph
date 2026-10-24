// RGW CLS: Usage wire profiles independent of public owning op/res records.
#ifndef CEPH_CLS_RGW_USAGE_DATUM_H
 #define CEPH_CLS_RGW_USAGE_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>

#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::usage_datum {

struct log_info final
{
    std::vector<::rgw_usage_log_entry> entries;
};

struct add_request final
{
    log_info info;
};

struct read_request final
{
    std::uint64_t start_epoch = 0;
    std::uint64_t end_epoch = 0;

    std::string owner;
    std::string bucket;

    std::string iter;
    std::uint32_t max_entries = 0;
};

struct trim_request final
{
    std::uint64_t start_epoch = 0;
    std::uint64_t end_epoch = 0;

    std::string user;
    std::string bucket;
};

template <::ceph::concepts::has_try_emplace_key EntriesT>
struct read_reply final
{
    EntriesT usage;
    std::string next_iter;

    bool truncated = false;
};

using native_entries = std::map<::rgw_user_bucket, ::rgw_usage_log_entry>;
using reply = read_reply<native_entries>;

::ceph::buffer::list encode_add(const std::vector<::rgw_usage_log_entry>& entries);
::ceph::buffer::list encode_read(std::string_view owner, std::string_view bucket,
                                 std::uint64_t start, std::uint64_t finish,
                                 std::uint32_t maximum, std::string_view marker);
::ceph::buffer::list encode_trim(std::string_view user, std::string_view bucket,
                                 std::uint64_t start, std::uint64_t finish);
void decode(add_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(read_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

// Unique-key associative bindings retain first-wins semantics for both map layouts.
template <typename EntriesT>
requires ::ceph::concepts::same_as_any<
    EntriesT, native_entries,
    ::boost::container::flat_map<::rgw_user_bucket, ::rgw_usage_log_entry>>
read_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::usage_datum

#endif // CEPH_CLS_RGW_USAGE_DATUM_H
