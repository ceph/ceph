// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "recovery_types.h"

BackfillInterval::BackfillInterval(hobject_t _begin) :
  begin(_begin) {}

BackfillInterval::BackfillInterval(hobject_t _begin,
                                   hobject_t _end) :
  begin(_begin),
  end(_end) {}

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


