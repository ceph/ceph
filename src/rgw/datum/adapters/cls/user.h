// RGW user CLS: independent outer records with local native leaf codecs.
#ifndef CEPH_CLS_USER_DATUM_H
 #define CEPH_CLS_USER_DATUM_H

#include "cls/user/cls_user_types.h"

#include <cstdint>
#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::user_datum {

struct bulk_request final
{
    std::vector<::cls_user_bucket_entry> entries;

    bool add = false;
    ::ceph::real_time time;
};

struct bucket_request final
{
    ::cls_user_bucket bucket;
};

struct bucket_list_request final
{
    std::string marker;
    std::string end_marker;

    int max_entries = 0;
};

struct empty_request final
{};

struct time_request final
{
    ::ceph::real_time time;
};

struct reset_request final
{
    ::ceph::real_time time;
    std::string marker;
    ::cls_user_stats acc_stats;
};

struct resource_add_request final
{
    ::cls_user_account_resource entry;

    bool exclusive = false;
    std::uint32_t limit = 0;
};

struct name_request final
{
    std::string name;
};

struct resource_list_request final
{
    std::string marker;
    std::string path_prefix;

    std::uint32_t max_entries = 0;
};

struct bucket_list_reply final
{
    std::vector<::cls_user_bucket_entry> entries;

    std::string marker;
    bool truncated = false;
};

struct header_reply final
{
    ::cls_user_header header;
};

struct reset_reply final
{
    std::string marker;
    ::cls_user_stats acc_stats;
    bool truncated = false;
};

struct resource_reply final
{
    ::cls_user_account_resource entry;
};

struct resource_list_reply final
{
    std::vector<::cls_user_account_resource> entries;

    bool truncated = false;
    std::string marker;
};

::ceph::buffer::list encode_bulk(const std::list<::cls_user_bucket_entry>& entries,
                                 bool add, ::ceph::real_time time);
::ceph::buffer::list encode_bucket(const ::cls_user_bucket& bucket);
::ceph::buffer::list encode_bucket_list(std::string_view marker, std::string_view end_marker,
                                        int maximum);
::ceph::buffer::list encode_get_header();
::ceph::buffer::list encode_time(::ceph::real_time time);
::ceph::buffer::list encode_resource_add(const ::cls_user_account_resource& entry,
                                         bool exclusive, std::uint32_t limit);
::ceph::buffer::list encode_name(std::string_view name);
::ceph::buffer::list encode_resource_list(std::string_view marker, std::string_view path_prefix,
                                          std::uint32_t maximum);

void decode(bulk_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(bucket_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(bucket_list_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(empty_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(time_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(reset_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(resource_add_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(name_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(resource_list_request& value, ::ceph::buffer::list::const_iterator& input);

void encode(const bucket_list_reply& value, ::ceph::buffer::list& output);
void encode(const header_reply& value, ::ceph::buffer::list& output);
void encode(const reset_reply& value, ::ceph::buffer::list& output);
void encode(const resource_reply& value, ::ceph::buffer::list& output);
void encode(const resource_list_reply& value, ::ceph::buffer::list& output);

} // cls::rgw::user_datum

#endif // CEPH_CLS_USER_DATUM_H
