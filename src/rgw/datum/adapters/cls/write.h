// RGW CLS: Independent module requests and borrowed native client arguments.
#ifndef CEPH_CLS_RGW_WRITE_DATUM_H
 #define CEPH_CLS_RGW_WRITE_DATUM_H

#include "cls/rgw/cls_rgw_types.h"

#include <cstdint>

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace cls::rgw::write_datum {

struct prepare_request final
{
    ::cls_rgw_obj_key key;
    std::string tag;
    std::string locator;

    ::RGWModifyOp op = ::CLS_RGW_OP_UNKNOWN;
    bool log_op = false;
    std::uint16_t bilog_flags = 0;
    ::rgw_zone_set zones_trace;
};

struct complete_request final
{
    ::cls_rgw_obj_key key;
    ::rgw_bucket_entry_ver ver;
    ::rgw_bucket_dir_entry_meta meta;
    std::vector<::cls_rgw_obj_key> remove_objs;

    std::string op_tag;
    std::string locator;
    ::rgw_zone_set zones_trace;

    ::RGWModifyOp op = ::CLS_RGW_OP_ADD;
    bool log_op = false;
    std::uint16_t bilog_flags = 0;
};

struct link_request final
{
    ::cls_rgw_obj_key key;
    ::rgw_bucket_dir_entry_meta meta;
    std::string olh_tag;
    std::string op_tag;
    ::rgw_zone_set zones_trace;

    std::uint64_t olh_epoch = 0;
    ::ceph::real_time unmod_since {};

    bool delete_marker = false;
    bool log_op = false;
    bool high_precision_time = false;
    std::uint16_t bilog_flags = 0;
};

struct trim_request final
{
    ::cls_rgw_obj_key olh;
    std::string olh_tag;

    std::uint64_t ver = 0;
};

struct unlink_request final
{
    ::cls_rgw_obj_key key;
    std::string op_tag;
    std::string olh_tag;
    ::rgw_zone_set zones_trace;

    std::uint64_t olh_epoch = 0;
    bool log_op = false;
    std::uint16_t bilog_flags = 0;
};

struct clear_request final
{
    ::cls_rgw_obj_key key;
    std::string olh_tag;
};

struct refresh_request final
{
    ::cls_rgw_obj_key key;
};

// Encoding borrows these inputs only for the call; the returned message owns its bytes.
struct complete_view final
{
    ::RGWModifyOp operation;
    std::string_view operation_tag;
    std::string_view locator;

    const ::cls_rgw_obj_key& key;
    const ::rgw_bucket_entry_ver& version;
    const ::rgw_bucket_dir_entry_meta& metadata;
    const std::list<::cls_rgw_obj_key> *removed_keys = nullptr;
    const ::rgw_zone_set *zones_trace = nullptr;

    bool log = false;
    std::uint16_t bilog_flags = 0;
};

struct link_view final
{
    const ::cls_rgw_obj_key& key;
    const ::ceph::buffer::list& olh_tag;
    std::string_view operation_tag;
    const ::rgw_bucket_dir_entry_meta *metadata = nullptr;
    const ::rgw_zone_set& zones_trace;

    std::uint64_t olh_epoch = 0;
    ::ceph::real_time unmodified_since {};

    bool delete_marker = false;
    bool high_precision_time = false;
    bool log = false;
};

// Publish the complete candidate and input cursor together after validation.
void decode(prepare_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(complete_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(link_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(trim_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(unlink_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(clear_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(refresh_request& value, ::ceph::buffer::list::const_iterator& input);

::ceph::buffer::list encode_prepare(::RGWModifyOp operation, std::string_view tag,
                                    const ::cls_rgw_obj_key& key, std::string_view locator);
::ceph::buffer::list encode(const complete_view& value);
::ceph::buffer::list encode(const link_view& value);
::ceph::buffer::list encode_trim(const ::cls_rgw_obj_key& key, std::uint64_t version,
                                 std::string_view tag);
::ceph::buffer::list encode_unlink(const ::cls_rgw_obj_key& key, std::string_view operation_tag,
                                   std::string_view tag, std::uint64_t epoch, bool log,
                                   std::uint16_t flags, const ::rgw_zone_set& zones);
::ceph::buffer::list encode_clear(const ::cls_rgw_obj_key& key, std::string_view tag);
::ceph::buffer::list encode_refresh(const ::cls_rgw_obj_key& key);

} // cls::rgw::write_datum

#endif // CEPH_CLS_RGW_WRITE_DATUM_H
