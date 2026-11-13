// RGW CLS: Owning bucket pages, independent of their wire bindings.
#ifndef CEPH_CLS_RGW_BUCKET_TYPES_H
 #define CEPH_CLS_RGW_BUCKET_TYPES_H

#include "cls/rgw/cls_rgw_types.h"

#include <string>

namespace cls::rgw::bucket_datum {

template <typename EntriesT>
struct directory final
{
    rgw_bucket_dir_header header;

    EntriesT m;
};

template <typename EntriesT>
struct list_reply final
{
    directory<EntriesT> dir;
    cls_rgw_obj_key marker;

    bool is_truncated = false;
    bool cls_filtered = true;
};

using flat_entries = boost::container::flat_map<std::string, rgw_bucket_dir_entry>;
using reply = list_reply<flat_entries>;

} // cls::rgw::bucket_datum

#endif // CEPH_CLS_RGW_BUCKET_TYPES_H
