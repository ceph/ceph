// RGW-owned queue wrapper profiles; shared queue records stay native.
#ifndef CEPH_CLS_RGW_QUEUED_GC_DATUM_H
 #define CEPH_CLS_RGW_QUEUED_GC_DATUM_H

#include "gc.h"

namespace cls::rgw::queued_gc_datum {

struct init_request final
{
    std::uint64_t size = 0;
    std::uint64_t num_deferred_entries = 0;
};

struct remove_request final
{
    std::uint64_t num_entries = 0;
};

::ceph::buffer::list encode_init(std::uint64_t size, std::uint64_t deferred);
::ceph::buffer::list encode_remove(std::uint64_t count);
void decode(init_request& value, ::ceph::buffer::list::const_iterator& input);
void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input);

} // cls::rgw::queued_gc_datum

#endif // CEPH_CLS_RGW_QUEUED_GC_DATUM_H
