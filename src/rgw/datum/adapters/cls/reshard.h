// RGW CLS: Reshard queue/control profiles with independent owning messages.
#ifndef CEPH_CLS_RGW_RESHARD_DATUM_H
 #define CEPH_CLS_RGW_RESHARD_DATUM_H

#include "cls/rgw/cls_rgw_types.h"
#include "common/container_concepts.h"

#include <cstdint>
#include <utility>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::reshard_datum {

struct add_request final
{
    ::cls_rgw_reshard_entry entry;
    bool create_only = false;
};

struct entry_message final
{
    ::cls_rgw_reshard_entry entry;
};

struct list_request final
{
    std::string marker;
    std::uint32_t max = 0;
};

template <typename EntriesT>
struct list_reply final
{
    EntriesT entries;
    bool is_truncated = false;
};

using reply = list_reply<std::vector<::cls_rgw_reshard_entry>>;

struct remove_request final
{
    std::string tenant;
    std::string bucket_name;
    std::string bucket_id;
};

struct instance_message final
{
    ::cls_rgw_bucket_instance_entry entry;
};

struct empty_request final {};

struct guard_request final
{
    std::int32_t ret_err = 0;
};

::ceph::buffer::list encode_add(const ::cls_rgw_reshard_entry& entry, bool create_only);
::ceph::buffer::list encode_entry(const ::cls_rgw_reshard_entry& entry);
::ceph::buffer::list encode_list(std::string_view marker, std::uint32_t maximum);
::ceph::buffer::list encode_remove(const ::cls_rgw_reshard_entry& entry);
::ceph::buffer::list encode_instance(const ::cls_rgw_bucket_instance_entry& entry);
::ceph::buffer::list encode_empty();
::ceph::buffer::list encode_guard(int error);
void decode(add_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(entry_message& value, ::ceph::buffer::list::const_iterator& input);
void decode(list_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(instance_message& value, ::ceph::buffer::list::const_iterator& input);
void decode(empty_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(guard_request& value, ::ceph::buffer::list::const_iterator& input);
void encode(const entry_message& value, ::ceph::buffer::list& output);
void encode(const instance_message& value, ::ceph::buffer::list& output);
void encode(const reply& value, ::ceph::buffer::list& output);
::cls_rgw_reshard_entry decode_entry(const ::ceph::buffer::list& input);
::cls_rgw_bucket_instance_entry decode_instance(const ::ceph::buffer::list& input);

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::cls_rgw_reshard_entry>, std::list<::cls_rgw_reshard_entry>>
std::pair<EntriesT, bool> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::reshard_datum

#endif // CEPH_CLS_RGW_RESHARD_DATUM_H
