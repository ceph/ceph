// RGW CLS: Bind native client/module records to the same BI wire profile.
// Opaque payloads retain native backing storage; only metadata uses zpp spans.
// Native types and completion ownership never enter Datum's pure mechanisms.
#include "cls_rgw_bi_datum.h"
#include "cls_rgw_bi_wire.h"

#include <utility>

namespace cls::rgw::bi_datum
{
namespace datum = ::ceph::salad::datum;

namespace
{
using native_entry = ::rgw_cls_bi_entry;
constexpr auto native_entry_fields = datum::binary_fields<&native_entry::type, &native_entry::idx, &native_entry::data> {};
constexpr auto module_entry_fields = datum::binary_fields<&entry::type, &entry::idx, &entry::data> {};

} // anonymous namespace

::ceph::buffer::list encode_request(const std::uint32_t maximum, const std::string_view filter,
                                    const std::string_view marker, const bool reshard)
{
    return ::cls::rgw::bi_wire::encode_request(maximum, filter, marker, reshard);
}

::ceph::buffer::list encode_get_request(const ::cls_rgw_obj_key& key, const ::BIIndexType type)
{
    return ::cls::rgw::bi_wire::encode_get_request(key.name, key.instance, std::to_underlying(type));
}

void decode(get_request& value, ::ceph::buffer::list::const_iterator& input)
{
    using key = ::cls_rgw_obj_key;

    auto cursor = input;
    get_request candidate;
    candidate.type = static_cast<::BIIndexType>(::cls::rgw::bi_wire::decode_get_request(
        cursor, candidate.key, datum::binary_fields<&key::name, &key::instance> {}));
    value = std::move(candidate);
    input = cursor;
}

void encode_get_reply(const entry& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entry_message(value, module_entry_fields);
    output.claim_append(candidate);
}

::rgw_cls_bi_entry decode_get_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    ::rgw_cls_bi_entry candidate;
    ::cls::rgw::bi_wire::decode_entry_message(cursor, candidate, native_entry_fields);

    return candidate;
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto candidate = ::cls::rgw::bi_wire::decode_request(cursor,
        [](const std::uint32_t maximum, std::string&& filter, std::string&& marker, const bool reshard) {
            return request {maximum, std::move(filter), std::move(marker), reshard};
        });
    value = std::move(candidate);
    input = cursor;
}

void decode(entry& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    entry candidate;
    ::cls::rgw::bi_wire::decode_entry(cursor, candidate, module_entry_fields);
    value = std::move(candidate);
    input = cursor;
}

void decode(put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto [entries, check_existing] = ::cls::rgw::bi_wire::decode_entries<std::vector<entry>>(cursor, module_entry_fields);
    value = {std::move(entries), check_existing};
    input = cursor;
}

void decode(entry_put_request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    entry_put_request candidate;
    ::cls::rgw::bi_wire::decode_entry_message(cursor, candidate.item, module_entry_fields);
    value = std::move(candidate);
    input = cursor;
}

::ceph::buffer::list encode_put_entry(const ::rgw_cls_bi_entry& value)
{
    return ::cls::rgw::bi_wire::encode_entry_message(value, native_entry_fields);
}

::ceph::buffer::list encode_put_request(const std::vector<::rgw_cls_bi_entry>& entries,
                                        const bool check_existing)
{
    return ::cls::rgw::bi_wire::encode_entries(entries, check_existing, native_entry_fields);
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = ::cls::rgw::bi_wire::encode_entries(value.entries, value.is_truncated, module_entry_fields);
    output.claim_append(candidate);
}

template <typename EntriesT>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();

    return ::cls::rgw::bi_wire::decode_entries<EntriesT>(cursor, native_entry_fields);
}

template std::pair<std::vector<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum
