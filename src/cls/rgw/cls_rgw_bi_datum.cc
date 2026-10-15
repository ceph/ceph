// RGW CLS: Bind native client/module records to the same BI wire profile.
// Opaque payloads retain native backing storage; only metadata uses zpp spans.
// Native types and completion ownership never enter Datum's pure mechanisms.
#include "cls_rgw_bi_datum.h"
#include "cls_rgw_bi_wire.h"

#include <utility>

namespace cls::rgw::bi_datum
{
namespace
{
template <typename EntryT>
auto make_entry(const std::uint8_t kind, std::string key, ::ceph::buffer::list data)
{
    return EntryT {static_cast<::BIIndexType>(kind), std::move(key), std::move(data)};
}

} // anonymous namespace

::ceph::buffer::list encode_request(const std::uint32_t maximum, const std::string_view filter,
                                    const std::string_view marker, const bool reshard)
{
    return ::cls::rgw::bi_wire::encode_request(maximum, filter, marker, reshard);
}

void decode(request& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto candidate = ::cls::rgw::bi_wire::decode_request(cursor,
        [](const std::uint32_t maximum, std::string filter, std::string marker, const bool reshard) {
            return request {maximum, std::move(filter), std::move(marker), reshard};
        });
    value = std::move(candidate);
    input = cursor;
}

void decode(entry& value, ::ceph::buffer::list::const_iterator& input)
{
    auto cursor = input;
    auto candidate = ::cls::rgw::bi_wire::decode_entry(cursor, make_entry<entry>);
    value = std::move(candidate);
    input = cursor;
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    namespace datum = ::ceph::salad::datum;

    auto candidate = ::cls::rgw::bi_wire::encode_reply(value.entries, value.is_truncated,
        datum::binary_fields<&entry::type, &entry::idx, &entry::data> {});
    output.claim_append(candidate);
}

template <typename EntriesT>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();

    return ::cls::rgw::bi_wire::decode_reply<EntriesT>(cursor, make_entry<typename EntriesT::value_type>);
}

template std::pair<std::vector<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);
template std::pair<std::list<::rgw_cls_bi_entry>, bool>
decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum
