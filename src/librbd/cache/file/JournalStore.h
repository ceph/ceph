// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE
#define CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE

#include "librbd/cache/Types.h"
#include "librbd/cache/file/AioFile.h"
#include "common/Mutex.h"
#include <boost/intrusive/slist.hpp>
#include <boost/intrusive/set.hpp>
#include <unordered_map>
#include <vector>

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {

struct BlockGuard;

namespace file {

template <typename> class MetaStore;

template <typename ImageCtxT>
class JournalStore {
public:
  JournalStore(ImageCtxT &image_ctx, BlockGuard &block_guard,
               MetaStore<ImageCtxT> &metastore);

  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void reset(Context *on_finish);

  /**
   * Expected Writeback Flow (assuming not cache miss):
   *
   * ALLOCATE_TID
   *    |
   *    v
   * DEMOTE_BLOCK (skip if not replace result from policy)
   *    |
   *    v
   * APPEND_EVENT (block should already be promoted to image store)
   *    |
   *    v
   * GET_WRITEBACK_EVENT
   *    |
   *    v
   * GET_WRITEBACK_BLOCK (skip if block not demoted to journal)
   *    |
   *    v
   * COMMIT_EVENT
   *
   */

  /// reserve the next sequential entry in journal, fails if no slots
  /// currently available
  int allocate_tid(uint64_t *tid);

  /// record journal event to specified slot
  void append_event(uint64_t tid, uint64_t block,
                    IOType io_type, Context *on_finish);

  /// true if most recent block op is write
  bool is_demote_required(uint64_t block);

  /// migrate provided block to journal block ring-buffer
  void demote_block(uint64_t block, bufferlist &&bl, Context *on_finish);

  bool is_writeback_pending() const;
  int get_writeback_event(uint64_t *tid, uint64_t *block, IOType *io_type,
                          bool *demoted);
  void get_writeback_block(uint64_t tid, bufferlist *bl, Context *on_finish);
  void commit_event(uint64_t tid, Context *on_finish);

private:
  static const uint64_t INVALID_IMAGE_BLOCK = static_cast<uint64_t>(-1);

  enum EventState {
    EVENT_STATE_INVALID   = 0,
    EVENT_STATE_ALLOCATED = 1,
    EVENT_STATE_VALID     = 2,
    EVENT_STATE_WRITEBACK = 3,
    EVENT_STATE_COMMITTED = 4
  };

  // TODO reduce EventRef size / only keep a subset in-memory
  // TODO combine w/ journal_store::Event if no memory savings
  struct EventRef : public boost::intrusive::slist_base_hook<>,
                    public boost::intrusive::set_base_hook<> {
    EventRef() : event_state(EVENT_STATE_INVALID) {
    }

    uint64_t tid;     ///< monotonically increasing event sequence
    uint64_t block;   ///< image block associated with event
    IOType io_type : 2;
    EventState event_state : 3;
    bool demoted : 1;
  };

  struct EventRefTidKey {
    bool operator()(const EventRef &lhs, const EventRef &rhs) const {
      return lhs.tid < rhs.tid;
    }
  };

  typedef std::vector<EventRef> EventRefPool;
  typedef boost::intrusive::slist<EventRef> EventRefs;

  typedef boost::intrusive::set<
    EventRef, boost::intrusive::compare<EventRefTidKey> > TidToEventRefs;

  typedef std::unordered_map<uint64_t, uint64_t> BlockToTids;

  ImageCtxT &m_image_ctx;
  BlockGuard &m_block_guard;
  MetaStore<ImageCtxT> &m_metastore;
  AioFile<ImageCtx> m_event_file;
  AioFile<ImageCtx> m_block_file;

  mutable Mutex m_lock;
  uint64_t m_tid = 0;
  uint64_t m_committed_tid = 0;
  uint32_t m_ring_buf_cnt = 0;
  uint32_t m_event_ref_cnt = 0;
  uint32_t m_block_size = 0;


  EventRefPool m_event_ref_pool;
  EventRefs m_event_refs;   ///< ring-buffer of events
  typename EventRefs::iterator m_event_ref_alloc_iter;
  typename EventRefs::iterator m_event_ref_writeback_iter;
  typename EventRefs::iterator m_event_ref_commit_iter;

  TidToEventRefs m_tid_to_event_refs;

  /// most recent uncommitted event associated with a block
  BlockToTids m_block_to_tids;
};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::JournalStore<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE
