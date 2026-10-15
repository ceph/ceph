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
constexpr auto native_event = [](const std::uint64_t epoch, const std::uint8_t operation,
                                 std::string&& tag, std::string&& name,
                                 std::string&& instance, const bool deleted)
{
    ::rgw_bucket_olh_log_entry value;
    value.epoch = epoch;
    value.op = static_cast<::OLHLogOp>(operation);
    value.op_tag = std::move(tag);
    value.key.name = std::move(name);
    value.key.instance = std::move(instance);
    value.delete_marker = deleted;

    return value;
};

constexpr auto client_event = [](const std::uint64_t epoch, const std::uint8_t operation,
                                 std::string&& tag, std::string&& name,
                                 std::string&& instance, const bool deleted)
{
    return ::cls::rgw::olh::event {{std::move(name), std::move(instance)}, std::move(tag),
                                  epoch, static_cast<::cls::rgw::olh::action>(operation), deleted};
};

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

void encode_reply(const stored_log::const_iterator lower, const stored_log::const_iterator upper, const bool stales,
                  const bool truncated, ::ceph::buffer::list& output)
{
    namespace datum = ::ceph::salad::datum;
    using entry = ::rgw_bucket_olh_log_entry;
    using key = ::cls_rgw_obj_key;

    auto candidate = ::cls::rgw::olh_wire::encode_reply(std::ranges::subrange {lower, upper}, truncated,
        datum::binary_fields<&entry::epoch, &entry::op, &entry::op_tag, &entry::key, &entry::delete_marker> {},
        datum::binary_fields<&key::name, &key::instance> {},
        [stales](const entry& value) { return stales || ::CLS_RGW_OLH_OP_STALE != value.op; }, stales);
    output.claim_append(candidate);
}

void decode_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value)
{
    using groups_type = decltype(::rgw_cls_read_olh_log_ret::log);

    auto cursor = input.cbegin();
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, groups_type::mapped_type>(cursor, native_event,
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
    auto [groups, truncated] = ::cls::rgw::olh_wire::decode_reply<groups_type, entries_type>(cursor, client_event,
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
