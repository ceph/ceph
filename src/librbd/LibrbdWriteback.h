// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_LIBRBDWRITEBACKHANDLER_H
#define CEPH_LIBRBD_LIBRBDWRITEBACKHANDLER_H

#include <queue>

#include "common/snap_types.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

class Mutex;
class Context;

namespace librbd {

  struct ImageCtx;

  class LibrbdWriteback : public WritebackHandler {
  public:
    LibrbdWriteback(ImageCtx *ictx, Mutex& lock);

    // Note that oloc, trunc_size, and trunc_seq are ignored
    void read(const object_t& oid, uint64_t object_no,
              const object_locator_t& oloc, uint64_t off, uint64_t len,
              snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
              __u32 trunc_seq, int op_flags,
	      const ZTracer::Trace &parent_trace, Context *onfinish) override;

    // Determine whether a read to this extent could be affected by a
    // write-triggered copy-on-write
    bool may_copy_on_write(const object_t& oid, uint64_t read_off,
                           uint64_t read_len, snapid_t snapid) override;

    // Note that oloc, trunc_size, and trunc_seq are ignored
    ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
                     uint64_t off, uint64_t len,
                     const SnapContext& snapc, const bufferlist &bl,
                     ceph::real_time mtime, uint64_t trunc_size,
                     __u32 trunc_seq, ceph_tid_t journal_tid,
                     const ZTracer::Trace &parent_trace,
                     Context *oncommit) override;
    using WritebackHandler::write;

    void overwrite_extent(const object_t& oid, uint64_t off,
                          uint64_t len, ceph_tid_t original_journal_tid,
                          ceph_tid_t new_journal_tid) override;

    struct write_result_d {
      bool done;
      int ret;
      std::string oid;
      Context *oncommit;
      write_result_d(const std::string& oid, Context *oncommit) :
        done(false), ret(0), oid(oid), oncommit(oncommit) {}
    private:
      write_result_d(const write_result_d& rhs);
      const write_result_d& operator=(const write_result_d& rhs);
    };

    struct write_item {
      object_t oid;
      uint64_t off;
      uint64_t len;
      SnapContext snapc;
      bufferlist bl;
      ceph_tid_t journal_tid;
      const ZTracer::Trace &parent_trace;
      write_result_d *result;
      write_item(const object_t& oid, uint64_t off, uint64_t len,
      const SnapContext& snapc, const bufferlist &bl, ceph_tid_t journal_tid,
      const ZTracer::Trace &parent_trace, write_result_d *result):
      oid(oid),off(off), len(len), snapc(snapc), bl(bl), journal_tid(journal_tid),
      parent_trace(parent_trace), result(result) {}
   };

   class WritebackQueue: public ThreadPool::PointerWQ<write_item> {
     public:
        WritebackQueue(LibrbdWriteback *wb, librbd::ImageCtx *m_ictx, const string &name, time_t ti,
                       ThreadPool *tp);
     protected:
       void process(write_item *req) override;
     private:
       LibrbdWriteback *m_wb_handler;
       librbd::ImageCtx *m_ictx;
   };
  private:
    void complete_writes(const std::string& oid);

    ceph_tid_t m_tid;
    Mutex& m_lock;
    librbd::ImageCtx *m_ictx;
    ceph::unordered_map<std::string, std::queue<write_result_d*> > m_writes;
    WritebackQueue *writeback_queue;
    friend class C_OrderedWrite;
  };
}

#endif
