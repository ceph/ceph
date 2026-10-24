// RGW CLS: Native declarations for the Datum BI wire bindings.
// The module owns its records; clients select independent reply containers.
// Keep C++26 reflection and wire implementation out of ordinary native callers.
#ifndef CEPH_CLS_RGW_BI_DATUM_H
 #define CEPH_CLS_RGW_BI_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>
#include <utility>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::bi_datum {

enum struct reply_format : std::uint8_t {
    framed = 1,
    compact = 2
};

struct request final
{
    std::uint32_t max = 0;

    std::string name_filter;
    std::string marker;

    bool reshardlog = false;

    reply_format format = reply_format::framed;
};

struct entry final
{
    ::BIIndexType type = ::BIIndexType::Invalid;
    std::string idx;

    ::ceph::buffer::list data;
};

struct reply final
{
    std::vector<entry> entries;

    bool is_truncated = false;
};

struct get_request final
{
    ::BIIndexType type = ::BIIndexType::Plain;
    ::cls_rgw_obj_key key;
};

struct entry_put_request final
{
    entry item;
};

struct put_request final
{
    std::vector<entry> entries;

    bool check_existing = false;
};

// Borrow records only during encoding; the returned message retains its bytes.
::ceph::buffer::list encode_request(std::uint32_t maximum, std::string_view filter,
                                    std::string_view marker, bool reshard);
::ceph::buffer::list encode_request(reply_format format, std::uint32_t maximum, std::string_view filter,
                                    std::string_view marker, bool reshard);
::ceph::buffer::list encode_get_request(const ::cls_rgw_obj_key& key, ::BIIndexType type);
::ceph::buffer::list encode_put_request(const std::vector<::rgw_cls_bi_entry>& entries,
                                        bool check_existing);
::ceph::buffer::list encode_put_entry(const ::rgw_cls_bi_entry& value);

void encode(const entry& value, ::ceph::buffer::list& output);
void encode(const reply& value, ::ceph::buffer::list& output);
void encode(reply_format format, const reply& value, ::ceph::buffer::list& output);

void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void decode(get_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(put_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(entry_put_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(entry& value, ::ceph::buffer::list::const_iterator& input);
::rgw_cls_bi_entry decode_get_reply(const ::ceph::buffer::list& input);

// Explicit vector/list instantiations return owned candidates. The client
// publishes caller outputs only after transport and decoding have succeeded.
template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::rgw_cls_bi_entry>, std::list<::rgw_cls_bi_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum

#endif // CEPH_CLS_RGW_BI_DATUM_H
