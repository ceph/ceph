// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_READAHEAD_H
#define CEPH_READAHEAD_H

#include "Mutex.h"
#include "Cond.h"
#include <list>

/**
   This class provides common state and logic for code that needs to perform readahead
   on linear things such as RBD images or files.
   Unless otherwise specified, all methods are thread-safe.

   Minimum and maximum readahead sizes may be violated by up to 50\% if alignment is enabled.
   Minimum readahead size may be violated if the end of the readahead target is reached.
 */
class Readahead {
public:
  typedef std::pair<uint64_t, uint64_t> extent_t;

  // equal to UINT64_MAX
  static const uint64_t NO_LIMIT = 18446744073709551615ULL;

  Readahead();

  ~Readahead();

  /**
     Update state with new reads and return readahead to be performed.
     If the length of the returned extent is 0, no readahead should be performed.
     The readahead extent is guaranteed not to pass \c limit.

     Note that passing in NO_LIMIT as the limit and truncating the returned extent
     is not the same as passing in the correct limit, because the internal state
     will differ in the two cases.

     @param extents read operations since last call to update
     @param limit size of the thing readahead is being applied to
   */
  extent_t update(const vector<extent_t>& extents, uint64_t limit);

  /**
     Update state with a new read and return readahead to be performed.
     If the length of the returned extent is 0, no readahead should be performed.
     The readahead extent is guaranteed not to pass \c limit.

     Note that passing in NO_LIMIT as the limit and truncating the returned extent
     is not the same as passing in the correct limit, because the internal state
     will differ in the two cases.

     @param offset offset of the read operation
     @param length length of the read operation
     @param limit size of the thing readahead is being applied to
   */
  extent_t update(uint64_t offset, uint64_t length, uint64_t limit);

  /**
     Increment the pending counter.
   */
  void inc_pending(int count = 1);

  /**
     Decrement the pending counter.
     The counter must not be decremented below 0.
   */
  void dec_pending(int count = 1);

  /**
     Waits until the pending count reaches 0.
   */
  void wait_for_pending();
  void wait_for_pending(Context *ctx);

  /**
     Sets the number of sequential requests necessary to trigger readahead.
   */
  void set_trigger_requests(int trigger_requests);

  /**
     Sets the minimum size of a readahead request, in bytes.
   */
  void set_min_readahead_size(uint64_t min_readahead_size);

  /**
     Sets the maximum size of a readahead request, in bytes.
   */
  void set_max_readahead_size(uint64_t max_readahead_size);

  /**
     Sets the alignment units.
     If the end point of a readahead request can be aligned to an alignment unit
     by increasing or decreasing the size of the request by 50\% or less, it will.
     Alignments are tested in order, so larger numbers should almost always come first.
   */
  void set_alignments(const std::vector<uint64_t> &alignments);

private:
  /**
     Records that a read request has been received.
     m_lock must be held while calling.
   */
  void _observe_read(uint64_t offset, uint64_t length);

  /**
     Computes the next readahead request.
     m_lock must be held while calling.
  */
  extent_t _compute_readahead(uint64_t limit);

  /// Number of sequential requests necessary to trigger readahead
  int m_trigger_requests;

  /// Minimum size of a readahead request, in bytes
  uint64_t m_readahead_min_bytes;

  /// Maximum size of a readahead request, in bytes
  uint64_t m_readahead_max_bytes;

  /// Alignment units, in bytes
  std::vector<uint64_t> m_alignments;

  /// Held while reading/modifying any state except m_pending
  Mutex m_lock;

  /// Number of consecutive read requests in the current sequential stream
  int m_nr_consec_read;

  /// Number of bytes read in the current sequenial stream
  uint64_t m_consec_read_bytes;

  /// Position of the read stream
  uint64_t m_last_pos;

  /// Position of the readahead stream
  uint64_t m_readahead_pos;

  /// When readahead is already triggered and the read stream crosses this point, readahead is continued
  uint64_t m_readahead_trigger_pos;

  /// Size of the next readahead request (barring changes due to alignment, etc.)
  uint64_t m_readahead_size;

  /// Number of pending readahead requests, as determined by inc_pending() and dec_pending()
  int m_pending;

  /// Lock for m_pending
  Mutex m_pending_lock;

  /// Waiters for pending readahead
  std::list<Context *> m_pending_waiting;
};

#endif
