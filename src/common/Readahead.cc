// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Readahead.h"

using namespace std;

Readahead::Readahead()
  : m_trigger_requests(10),
    m_readahead_min_bytes(0),
    m_readahead_max_bytes(NO_LIMIT),
    m_alignments(),
    m_lock("Readahead::m_lock"),
    m_nr_consec_read(0),
    m_consec_read_bytes(0),
    m_last_pos(0),
    m_readahead_pos(0),
    m_readahead_trigger_pos(0),
    m_readahead_size(0),
    m_pending(0),
    m_pending_lock("Readahead::m_pending_lock") {
}

Readahead::~Readahead() {
}

Readahead::extent_t Readahead::update(const vector<extent_t>& extents, uint64_t limit) {
  m_lock.Lock();
  for (vector<extent_t>::const_iterator p = extents.begin(); p != extents.end(); ++p) {
    _observe_read(p->first, p->second);
  }
  if (m_readahead_pos >= limit) {
    m_lock.Unlock();
    return extent_t(0, 0);
  }
  pair<uint64_t, uint64_t> extent = _compute_readahead(limit);
  m_lock.Unlock();
  return extent;
}

Readahead::extent_t Readahead::update(uint64_t offset, uint64_t length, uint64_t limit) {
  m_lock.Lock();
  _observe_read(offset, length);
  if (m_readahead_pos >= limit) {
    m_lock.Unlock();
    return extent_t(0, 0);
  }
  extent_t extent = _compute_readahead(limit);
  m_lock.Unlock();
  return extent;
}

void Readahead::_observe_read(uint64_t offset, uint64_t length) {
  if (offset == m_last_pos) {
    m_nr_consec_read++;
    m_consec_read_bytes += length;
  } else {
    m_nr_consec_read = 0;
    m_consec_read_bytes = 0;
    m_readahead_trigger_pos = 0;
    m_readahead_size = 0;
    m_readahead_pos = 0;
  }
  m_last_pos = offset + length;
}

Readahead::extent_t Readahead::_compute_readahead(uint64_t limit) {
  uint64_t readahead_offset = 0;
  uint64_t readahead_length = 0;
  if (m_nr_consec_read >= m_trigger_requests) {
    // currently reading sequentially
    if (m_last_pos >= m_readahead_trigger_pos) {
      // need to read ahead
      if (m_readahead_size == 0) {
	// initial readahead trigger
	m_readahead_size = m_consec_read_bytes;
	m_readahead_pos = m_last_pos;
      } else {
	// continuing readahead trigger
	m_readahead_size *= 2;
	if (m_last_pos > m_readahead_pos) {
	  m_readahead_pos = m_last_pos;
	}
      }
      m_readahead_size = MAX(m_readahead_size, m_readahead_min_bytes);
      m_readahead_size = MIN(m_readahead_size, m_readahead_max_bytes);
      readahead_offset = m_readahead_pos;
      readahead_length = m_readahead_size;

      // Snap to the first alignment possible
      uint64_t readahead_end = readahead_offset + readahead_length;
      for (vector<uint64_t>::iterator p = m_alignments.begin(); p != m_alignments.end(); ++p) {
	// Align the readahead, if possible.
	uint64_t alignment = *p;
	uint64_t align_prev = readahead_end / alignment * alignment;
	uint64_t align_next = align_prev + alignment;
	uint64_t dist_prev = readahead_end - align_prev;
	uint64_t dist_next = align_next - readahead_end;
	if (dist_prev < readahead_length / 2 && dist_prev < dist_next) {
	  // we can snap to the previous alignment point by a less than 50% reduction in size
	  assert(align_prev > readahead_offset);
	  readahead_length = align_prev - readahead_offset;
	  break;
	} else if(dist_next < readahead_length / 2) {
	  // we can snap to the next alignment point by a less than 50% increase in size
	  assert(align_next > readahead_offset);
	  readahead_length = align_next - readahead_offset;
	  break;
	}
	// Note that m_readahead_size should remain unadjusted.
      }

      if (m_readahead_pos + readahead_length > limit) {
	readahead_length = limit - m_readahead_pos;
      }

      m_readahead_trigger_pos = m_readahead_pos + readahead_length / 2;
      m_readahead_pos += readahead_length;
    }
  }
  return extent_t(readahead_offset, readahead_length);
}

void Readahead::inc_pending(int count) {
  assert(count > 0);
  m_pending_lock.Lock();
  m_pending += count;
  m_pending_lock.Unlock();
}

void Readahead::dec_pending(int count) {
  assert(count > 0);
  m_pending_lock.Lock();
  assert(m_pending >= count);
  m_pending -= count;
  if (m_pending == 0) {
    std::list<Context *> pending_waiting(std::move(m_pending_waiting));
    m_pending_lock.Unlock();

    for (auto ctx : pending_waiting) {
      ctx->complete(0);
    }
  } else {
    m_pending_lock.Unlock();
  }
}

void Readahead::wait_for_pending() {
  C_SaferCond ctx;
  wait_for_pending(&ctx);
  ctx.wait();
}

void Readahead::wait_for_pending(Context *ctx) {
  m_pending_lock.Lock();
  if (m_pending > 0) {
    m_pending_lock.Unlock();
    m_pending_waiting.push_back(ctx);
    return;
  }
  m_pending_lock.Unlock();

  ctx->complete(0);
}
void Readahead::set_trigger_requests(int trigger_requests) {
  m_lock.Lock();
  m_trigger_requests = trigger_requests;
  m_lock.Unlock();
}

void Readahead::set_min_readahead_size(uint64_t min_readahead_size) {
  m_lock.Lock();
  m_readahead_min_bytes = min_readahead_size;
  m_lock.Unlock();
}

void Readahead::set_max_readahead_size(uint64_t max_readahead_size) {
  m_lock.Lock();
  m_readahead_max_bytes = max_readahead_size;
  m_lock.Unlock();
}

void Readahead::set_alignments(const vector<uint64_t> &alignments) {
  m_lock.Lock();
  m_alignments = alignments;
  m_lock.Unlock();
}
