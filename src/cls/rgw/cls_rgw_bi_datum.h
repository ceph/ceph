// RGW CLS: Native declarations for the Datum BI listing implementation.
// The module owns its records; clients select independent reply containers.
// Keep C++26 reflection and wire implementation out of ordinary native callers.
#ifndef CEPH_CLS_RGW_BI_DATUM_H
 #define CEPH_CLS_RGW_BI_DATUM_H

#include "cls/rgw/cls_rgw_types.h"

#include <cstdint>
#include <utility>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::bi_datum
{
struct request final
{
    std::uint32_t max = 0;

    std::string name_filter;
    std::string marker;

    bool reshardlog = false;
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

::ceph::buffer::list encode_request(std::uint32_t maximum, std::string_view filter,
                                    std::string_view marker, bool reshard);
void decode(request& value, ::ceph::buffer::list::const_iterator& input);
void decode(entry& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

// Explicit vector/list instantiations return owned candidates. The client
// publishes caller outputs only after transport and decoding have succeeded.
template <typename EntriesT>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::bi_datum

#endif // CEPH_CLS_RGW_BI_DATUM_H
