// RGW CLS: Object requests independent of the public owning operation types.
#ifndef CEPH_CLS_RGW_OBJECT_DATUM_H
 #define CEPH_CLS_RGW_OBJECT_DATUM_H

#include "common/ceph_time.h"
#include "include/buffer.h"

#include <cstdint>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::object_datum {

struct remove_request final
{
    std::vector<std::string> keep_attr_prefixes;
};

struct version_request final
{
    std::string attr;
};

struct prefix_request final
{
    std::string check_prefix;

    bool fail_if_exist = false;
};

struct mtime_request final
{
    ::ceph::real_time mtime;

    std::uint8_t type = 0;
    bool high_precision_time = false;
};

::ceph::buffer::list encode_remove(const std::list<std::string>& prefixes);
::ceph::buffer::list encode_version(std::string_view attribute);
::ceph::buffer::list encode_prefix(std::string_view prefix, bool fail_if_exist);
::ceph::buffer::list encode_mtime(::ceph::real_time mtime, bool precise, std::uint8_t type);
void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(version_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(prefix_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(mtime_request& value, ::ceph::buffer::list::const_iterator& input);

} // cls::rgw::object_datum

#endif // CEPH_CLS_RGW_OBJECT_DATUM_H
