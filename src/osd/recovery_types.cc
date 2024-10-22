// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "recovery_types.h"

BackfillInterval::BackfillInterval(hobject_t _begin) :
  begin(_begin) {}

BackfillInterval::BackfillInterval(hobject_t _begin,
                                   hobject_t _end) :
  begin(_begin),
  end(_end) {}

void BackfillInterval::update(const pg_log_entry_t &e) {
  const hobject_t &soid = e.soid;
  if (soid >= begin &&
      soid < end) {
    if (e.is_update()) {
      objects.erase(e.soid);
      objects.emplace(e.soid,e.version);
    } else if (e.is_delete()) {
      objects.erase(e.soid);
    }
  }
  // When scanning log entries for updates, we call update
  // with each entry up until the latest one we have
  ceph_assert(e.version > version);
  version = e.version;
}

std::ostream& operator<<(std::ostream& out, const BackfillInterval& bi)
{
  out << "BackfillInfo(" << "populated: " << bi.populated
      << " " << bi.begin << "-" << bi.end
      << " " << bi.objects.size() << " objects";
  if (!bi.objects.empty())
    out << " " << bi.objects;
  out << ")";
  return out;
}


