// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal.h"
#include "journal/segmented_journal.h"
#include "journal/circular_bounded_journal.h"

namespace crimson::os::seastore::journal {

JournalRef make_segmented(
  SegmentProvider &provider,
  JournalTrimmer &trimmer)
{
  return std::make_unique<SegmentedJournal>(provider, trimmer);
}

JournalRef make_circularbounded(
  JournalTrimmer &trimmer,
  crimson::os::seastore::random_block_device::RBMDevice* device,
  std::string path)
{
  return std::make_unique<CircularBoundedJournal>(trimmer, device, path);
}

}
