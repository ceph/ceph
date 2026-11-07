// RGW CLS: Legacy GC message profiles with independent sequence ownership.
#ifndef CEPH_CLS_RGW_GC_DATUM_H
 #define CEPH_CLS_RGW_GC_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::gc_datum {

struct set_request final
{
    ::cls_rgw_gc_obj_info info;
    std::uint32_t expiration_secs = 0;
};

struct defer_request final
{
    std::string tag;
    std::uint32_t expiration_secs = 0;
};

struct list_request final
{
    std::string marker;

    std::uint32_t max = 0;
    bool expired_only = true;
};

struct remove_request final
{
    std::vector<std::string> tags;
};

template <typename EntriesT>
struct list_reply final
{
    EntriesT entries;
    std::string next_marker;
    bool truncated = false;
};

using reply = list_reply<std::vector<::cls_rgw_gc_obj_info>>;

::ceph::buffer::list encode_set(std::uint32_t expiration, const ::cls_rgw_gc_obj_info& info);
::ceph::buffer::list encode_defer(std::uint32_t expiration, std::string_view tag);
::ceph::buffer::list encode_list(std::string_view marker, std::uint32_t maximum, bool expired);
::ceph::buffer::list encode_remove(const std::vector<std::string>& tags);
void decode(set_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(defer_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(list_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const reply& value, ::ceph::buffer::list& output);

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::cls_rgw_gc_obj_info>, std::list<::cls_rgw_gc_obj_info>>
list_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::gc_datum

#endif // CEPH_CLS_RGW_GC_DATUM_H
