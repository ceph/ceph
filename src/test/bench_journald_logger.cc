// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Journald.h"
#include "log/Entry.h"
#include "log/SubsystemMap.h"

using namespace ceph::logging;

int main()
{
  SubsystemMap subs;
  JournaldLogger journald(&subs);

  for (int i = 0; i < 100000; i++) {
    MutableEntry entry(0, 0);
    entry.get_ostream() << "This is log message " << i << ", which is a little bit looooooooo********ooooooooog and may contains multiple\nlines.";
    journald.log_entry(entry);
  }
}
