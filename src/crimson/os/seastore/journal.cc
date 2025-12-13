// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "journal.h"
#include "journal/segmented_journal.h"
#include "journal/circular_bounded_journal.h"

namespace crimson::os::seastore::journal {

JournalRef make_segmented(
  unsigned int store_index,
  SegmentProvider &provider,
  JournalTrimmer &trimmer)
{
  return std::make_unique<SegmentedJournal>(store_index, provider, trimmer);
}

JournalRef make_circularbounded(
  unsigned int store_index,
  JournalTrimmer &trimmer,
  crimson::os::seastore::random_block_device::RBMDevice* device,
  std::string path)
{
  return std::make_unique<CircularBoundedJournal>(store_index, trimmer, device, path);
}

}
