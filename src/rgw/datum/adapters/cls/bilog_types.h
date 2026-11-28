// RGW CLS: Owning bilog pages, independent of their wire bindings.
#ifndef CEPH_CLS_RGW_BILOG_TYPES_H
 #define CEPH_CLS_RGW_BILOG_TYPES_H

#include "cls/rgw/cls_rgw_types.h"

#include <list>

namespace cls::rgw::bilog {

// Clients keep lists; modules encode vectors through the same counted wire profile.
template <typename EntriesT = std::list<rgw_bi_log_entry>>
struct reply final
{
    EntriesT entries;

    bool truncated = false;
};

} // cls::rgw::bilog

#endif // CEPH_CLS_RGW_BILOG_TYPES_H
