// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/ceph_time.h"
#include "include/buffer.h"
#include "include/common_fwd.h"

namespace ceph { class Formatter; }

class cuObjClient;

/**
 * Per-OSD RDMA target for erasure-coded sub-reads: the memory that a
 * primary's peers write their shard chunks into instead of returning
 * them over the messenger.
 *
 * This is the client half of cuObject (libcuobjclient), the same role
 * elbencho or a GPU data loader plays towards RGW: one registered
 * arena and one descriptor token for it. A secondary that receives the
 * token in an ECSubRead pushes its chunk with the very code path it
 * uses to deliver to a client (OSDCuObj::execute_plan), at the arena
 * offset the primary assigned.
 *
 * The arena is carved into fixed-size slots. A slot is claimed per
 * sub-read extent, wrapped into the read result as a zero-copy buffer
 * once the peer reports the write landed, and returned when the last
 * reference to that buffer drops. A slot whose write may still be in
 * flight (the peer never replied, or replied without confirming the
 * push) is quarantined for longer than a peer may take to start and
 * finish a write, so a late RDMA write can never land in memory that
 * has been handed to another read.
 *
 * Thread safety: acquire()/release() may be called from any op worker
 * thread.
 */
class OSDCuObjGather {
public:
  /// a contiguous registered region of the arena
  struct slot {
    uint64_t ofs = 0;   ///< offset into the arena == offset in the token window
    size_t len = 0;     ///< usable bytes
    char* ptr = nullptr;
  };

  /// rdma_addr: this OSD's RDMA address, named to the client library
  /// when the deployment leaves that to rdma_network
  OSDCuObjGather(CephContext *cct, const std::string& rdma_addr);
  ~OSDCuObjGather();

  OSDCuObjGather(const OSDCuObjGather&) = delete;
  OSDCuObjGather& operator=(const OSDCuObjGather&) = delete;

  /// true once the arena is registered and a token was minted
  bool is_available() const { return m_available; }

  /// descriptor of the whole arena, as sent to peers in ECSubRead
  const std::string& token() const { return m_token; }
  /// the same window as a source peers RDMA-read from
  const std::string& put_token() const { return m_put_token; }
  /// true if [ptr, ptr+len) lies inside the arena
  bool contains(const void* ptr, size_t len) const {
    const char* p = static_cast<const char*>(ptr);
    return p >= m_arena && p + len <= m_arena + m_arena_size;
  }

  size_t slot_size() const { return m_slot_size; }
  /// the whole registered window, for registering it with the server
  /// library too so deliveries out of the slots need no registration
  void* arena() const { return m_arena; }
  size_t arena_size() const { return m_arena_size; }

  /**
   * Claim a slot able to hold len bytes. Returns nullopt when len
   * exceeds the slot size or the pool is exhausted; the caller then
   * reads over the messenger as before.
   */
  std::optional<slot> acquire(size_t len);

  /**
   * Return a slot. With quarantine set it is held out of circulation
   * until any RDMA write a peer could still issue against it has
   * necessarily completed or failed.
   */
  void release(const slot& s, bool quarantine);

  /**
   * Memory of this OSD that peers RDMA-read (the write payload a peer
   * pulls): registered with the client library and described by a PUT
   * token. keep pins the buffers the registration covers.
   */
  struct source {
    void* ptr = nullptr;
    size_t len = 0;
    std::string token;
    char* token_raw = nullptr;
    ceph::buffer::list keep;
  };
  /**
   * Register [ptr, ptr+len) - page-aligned - as a pull source. Returns
   * null when the registration or the token mint fails.
   */
  std::unique_ptr<source> register_source(void* ptr, size_t len,
                                          ceph::buffer::list keep);
  /// a source for memory already inside the arena: no registration,
  /// the arena's own PUT token; keep pins the slots under it
  std::unique_ptr<source> arena_source(ceph::buffer::list keep);
  /**
   * Drop a source. With quarantine set the registration and the memory
   * stay alive for the quarantine period first, for a source a peer
   * might still be reading (its reply never came).
   */
  void release_source(std::unique_ptr<source> s, bool quarantine);

  /**
   * A slot filled by RDMA-reading a client's memory (the primary pulling
   * a write payload) remembers where the bytes came from. A peer that
   * needs the same bytes can then read them from the client itself
   * instead of through this arena, taking the primary off the data
   * path for the data shards (osd_cuobj_pull_writes_direct). The
   * record dies with the slot's release.
   */
  struct client_backing {
    std::string token;
    uint64_t remote_ofs = 0;  ///< client offset of the queried address
  };
  void set_client_backing(const slot& s, const std::string& token,
                          uint64_t remote_ofs);
  /// the client-side location of an address inside a backed slot
  std::optional<client_backing> client_backing_of(const void* ptr) const;
  void note_direct_pull(uint64_t bytes) {
    m_direct_pieces++;
    m_direct_bytes += bytes;
  }

  /// asok/debug counters
  void dump_stats(ceph::Formatter* f) const;

private:
  int do_init();
  void do_shutdown();
  void reap_quarantine(ceph::coarse_mono_clock::time_point now);
  void reap_sources(ceph::coarse_mono_clock::time_point now);
  void drop_source(source& s);

  CephContext* m_cct;
  const std::string m_rdma_addr;
  std::unique_ptr<cuObjClient> m_client;
  bool m_available = false;

  char* m_arena = nullptr;
  size_t m_arena_size = 0;
  size_t m_slot_size = 0;
  size_t m_slot_count = 0;
  std::string m_token;
  char* m_token_raw = nullptr;   ///< owned by the library
  std::string m_put_token;
  char* m_put_token_raw = nullptr;
  std::chrono::milliseconds m_quarantine{0};

  std::mutex m_lock;
  std::vector<uint32_t> m_free;  ///< slot indexes
  struct held {
    uint32_t idx;
    ceph::coarse_mono_clock::time_point until;
  };
  std::deque<held> m_quarantined; ///< ascending by until
  std::vector<std::optional<client_backing>> m_backing; ///< per slot

  std::mutex m_client_lock;       ///< the library serializes badly on its own
  struct held_source {
    std::unique_ptr<source> s;
    ceph::coarse_mono_clock::time_point until;
  };
  std::deque<held_source> m_quarantined_sources; ///< ascending by until

  std::atomic<uint64_t> m_sources{0};
  std::atomic<uint64_t> m_source_failures{0};
  std::atomic<uint64_t> m_source_quarantines{0};
  std::atomic<uint64_t> m_acquired{0};
  std::atomic<uint64_t> m_exhausted{0};
  std::atomic<uint64_t> m_oversized{0};
  std::atomic<uint64_t> m_quarantines{0};
  std::atomic<uint64_t> m_direct_pieces{0};
  std::atomic<uint64_t> m_direct_bytes{0};
  std::atomic<uint32_t> m_in_use{0};
};
