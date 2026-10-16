// RGW CLS: Borrow write arguments while preserving native request encodings.
#ifndef CEPH_CLS_RGW_WRITE_DATUM_H
 #define CEPH_CLS_RGW_WRITE_DATUM_H

#include "cls_rgw_types.h"

#include <cstdint>

#include <list>
#include <string_view>

namespace cls::rgw::write_datum {

::ceph::buffer::list encode_prepare(::RGWModifyOp operation, std::string_view tag,
                                    const ::cls_rgw_obj_key& key, std::string_view locator);
::ceph::buffer::list encode_complete(::RGWModifyOp operation, std::string_view tag,
                                     const ::rgw_bucket_entry_ver& version,
                                     const ::cls_rgw_obj_key& key,
                                     const ::rgw_bucket_dir_entry_meta& metadata,
                                     const std::list<::cls_rgw_obj_key> *removed,
                                     bool log, std::uint16_t flags,
                                     const ::rgw_zone_set *zones, std::string_view locator);
::ceph::buffer::list encode_link(const ::cls_rgw_obj_key& key,
                                 const ::ceph::buffer::list& tag, bool deleted,
                                 std::string_view operation_tag,
                                 const ::rgw_bucket_dir_entry_meta *metadata,
                                 std::uint64_t epoch, ::ceph::real_time unmodified,
                                 bool precise, bool log, const ::rgw_zone_set& zones);
::ceph::buffer::list encode_trim(const ::cls_rgw_obj_key& key, std::uint64_t version,
                                 std::string_view tag);

} // cls::rgw::write_datum

#endif // CEPH_CLS_RGW_WRITE_DATUM_H
