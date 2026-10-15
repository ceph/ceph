// RGW CLS: Bind OLH wire fields to native maps or independent vector records.
#include "cls_rgw_olh_datum.h"
#include "cls_rgw_olh_wire.h"

#include <algorithm>
#include <ranges>
#include <utility>

namespace cls::rgw::olh_datum
{
namespace
{
namespace datum = ::ceph::salad::datum;
using native_entry = ::rgw_bucket_olh_log_entry;
using native_key = ::cls_rgw_obj_key;
using client_entry = ::cls::rgw::olh::event;
using client_key = ::cls::rgw::olh::object_key;

constexpr auto native_event_fields = datum::binary_fields<&native_entry::epoch, &native_entry::op,
    &native_entry::op_tag, &native_entry::key, &native_entry::delete_marker> {};
constexpr auto native_key_fields = datum::binary_fields<&native_key::name, &native_key::instance> {};
constexpr auto client_event_fields = datum::binary_fields<&client_entry::epoch, &client_entry::operation,
    &client_entry::tag, &client_entry::object, &client_entry::delete_marker> {};
constexpr auto client_key_fields = datum::binary_fields<&client_key::name, &client_key::instance> {};

} // anonymous namespace

::ceph::buffer::list encode_request(const ::cls_rgw_obj_key& key, const std::uint64_t marker,
                                    const std::string_view tag, const bool stales)
{
    return ::cls::rgw::olh_wire::encode_request(key.name, key.instance, marker, tag, stales);
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto candidate = ::cls::rgw::olh_wire::decode_request(cursor,
        [](std::string&& name, std::string&& instance, const std::uint64_t marker,
           std::string&& tag, const bool stales) {
            request result;
            result.olh.name = std::move(name);
            result.olh.instance = std::move(instance);
            result.olh_tag = std::move(tag);
            result.ver_marker = marker;
            result.get_stales = stales;

            return result;
        });
    value = std::move(candidate);
    input = cursor;
}

void encode_reply(const log_page groups, const bool stales, const bool truncated, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::olh_wire::encode_reply(groups, truncated, native_event_fields, native_key_fields,
        [](const native_entry& value) { return ::CLS_RGW_OLH_OP_STALE != value.op; }, stales);
    output.claim_append(candidate);
}

void decode_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value)
{
    using groups_type = decltype(::rgw_cls_read_olh_log_ret::log);

    auto cursor = input.cbegin();
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, groups_type::mapped_type>(
        cursor, native_event_fields, native_key_fields,
        [](auto& groups, const std::uint64_t epoch, auto&& entries) {
            groups.try_emplace(epoch, std::move(entries));
        });
    value.log = std::move(groups);
    value.is_truncated = truncated;
}

void decode_reply(const ::ceph::buffer::list& input, ::cls::rgw::olh::reply& value)
{
    using groups_type = decltype(::cls::rgw::olh::reply::epochs);
    using entries_type = ::cls::rgw::olh::epoch_group::second_type;

    auto cursor = input.cbegin();
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, entries_type>(
        cursor, client_event_fields, client_key_fields,
        [](auto& groups, const std::uint64_t epoch, auto&& entries) {
            groups.emplace_back(epoch, std::move(entries));
        });
    // Native map decoding orders epochs and keeps the first value for a
    // duplicate key. Normal ascending responses avoid the sorting pass.
    const auto epoch = &::cls::rgw::olh::epoch_group::first;
    if (!std::ranges::is_sorted(groups, {}, epoch)) {
        std::ranges::stable_sort(groups, {}, epoch);
    }
    const auto duplicates = std::ranges::unique(groups, {}, epoch);
    groups.erase(duplicates.begin(), duplicates.end());
    value = {std::move(groups), truncated};
}

} // cls::rgw::olh_datum
