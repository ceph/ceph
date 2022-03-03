// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cleaner.h"
#include "cleaner/segment_cleaner.h"

namespace crimson::os::seastore::cleaner {

CleanerRef make_segmented(
  seg_cleaner_config_t config,
  ExtentReaderRef&& scanner,
  bool detailed)
{
  return std::make_unique<SegmentCleaner>(config, move(scanner), detailed);
}
}
