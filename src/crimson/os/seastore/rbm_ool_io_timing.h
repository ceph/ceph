// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

// Detailed OOL device-I/O (DMA) timing helpers.
//
// The RBM OOL write path (RandomBlockOolWriter::do_write in
// extent_placement_manager.cc) needs to attribute the wall time spent inside
// the actual device DMA (RBMDevice::write -> dma_write, deep in
// nvme_block_device.cc / rbm_device.cc) to the owning transaction. Those
// device methods have no handle to the Transaction, so we bridge them with a
// thread-local tracker: the OOL writer installs a tracker around the batch of
// writes, and each device write stamps first/last DMA times into it.
//
// All of this is compiled out unless CRIMSON_DETAILED_SAMPLING is defined;
// when disabled the note helpers are empty inlines that evaluate to nothing
// (no clock reads, no thread-local access), so the device write path pays
// zero overhead in production builds.

#ifdef CRIMSON_DETAILED_SAMPLING

#include <optional>

#include <seastar/core/lowres_clock.hh>

namespace crimson::os::seastore {

struct rbm_ool_io_dma_tracker_t {
  std::optional<seastar::lowres_clock::time_point> first_dma_start;
  std::optional<seastar::lowres_clock::time_point> last_dma_end;
};

inline thread_local rbm_ool_io_dma_tracker_t* rbm_ool_io_dma_tracker = nullptr;

inline void rbm_ool_io_dma_tracker_note_dma_start()
{
  if (!rbm_ool_io_dma_tracker) {
    return;
  }
  auto t = seastar::lowres_clock::now();
  auto& tracker = *rbm_ool_io_dma_tracker;
  if (!tracker.first_dma_start || t < *tracker.first_dma_start) {
    tracker.first_dma_start = t;
  }
}

inline void rbm_ool_io_dma_tracker_note_dma_end()
{
  if (!rbm_ool_io_dma_tracker) {
    return;
  }
  auto t = seastar::lowres_clock::now();
  auto& tracker = *rbm_ool_io_dma_tracker;
  if (!tracker.last_dma_end || t > *tracker.last_dma_end) {
    tracker.last_dma_end = t;
  }
}

} // namespace crimson::os::seastore

#else // !CRIMSON_DETAILED_SAMPLING

namespace crimson::os::seastore {

inline void rbm_ool_io_dma_tracker_note_dma_start() {}
inline void rbm_ool_io_dma_tracker_note_dma_end() {}

} // namespace crimson::os::seastore

#endif // CRIMSON_DETAILED_SAMPLING
