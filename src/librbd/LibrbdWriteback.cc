// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/ObjectMap.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/ReadResult.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdwriteback: "

namespace librbd {

  /**
   * callback to finish a rados completion as a Context
   *
   * @param c completion
   * @param arg Context* recast as void*
   */
  void context_cb(rados_completion_t c, void *arg)
  {
    Context *con = reinterpret_cast<Context *>(arg);
    con->complete(rados_aio_get_return_value(c));
  }

  /**
   * context to wrap another context in a Mutex
   *
   * @param cct cct
   * @param c context to finish
   * @param l mutex to lock
   */
  class C_ReadRequest : public Context {
  public:
    C_ReadRequest(CephContext *cct, Context *c, Mutex *cache_lock)
      : m_cct(cct), m_ctx(c), m_cache_lock(cache_lock) {
    }
    void finish(int r) override {
      ldout(m_cct, 20) << "aio_cb completing " << dendl;
      {
        Mutex::Locker cache_locker(*m_cache_lock);
	m_ctx->complete(r);
      }
      ldout(m_cct, 20) << "aio_cb finished" << dendl;
    }
  private:
    CephContext *m_cct;
    Context *m_ctx;
    Mutex *m_cache_lock;
  };

  class C_OrderedWrite : public Context {
  public:
    C_OrderedWrite(CephContext *cct, LibrbdWriteback::write_result_d *result,
		   const ZTracer::Trace &trace, LibrbdWriteback *wb)
      : m_cct(cct), m_result(result), m_trace(trace), m_wb_handler(wb) {}
    ~C_OrderedWrite() override {}
    void finish(int r) override {
      ldout(m_cct, 20) << "C_OrderedWrite completing " << m_result << dendl;
      {
	Mutex::Locker l(m_wb_handler->m_lock);
	assert(!m_result->done);
	m_result->done = true;
	m_result->ret = r;
	m_wb_handler->complete_writes(m_result->oid);
      }
      ldout(m_cct, 20) << "C_OrderedWrite finished " << m_result << dendl;
      m_trace.event("finish");
    }
  private:
    CephContext *m_cct;
    LibrbdWriteback::write_result_d *m_result;
    ZTracer::Trace m_trace;
    LibrbdWriteback *m_wb_handler;
  };

  struct C_CommitIOEventExtent : public Context {
    ImageCtx *image_ctx;
    uint64_t journal_tid;
    uint64_t offset;
    uint64_t length;

    C_CommitIOEventExtent(ImageCtx *image_ctx, uint64_t journal_tid,
                          uint64_t offset, uint64_t length)
      : image_ctx(image_ctx), journal_tid(journal_tid), offset(offset),
        length(length) {
    }

    void finish(int r) override {
      // all IO operations are flushed prior to closing the journal
      assert(image_ctx->journal != nullptr);

      image_ctx->journal->commit_io_event_extent(journal_tid, offset, length,
                                                 r);
    }
  };

  LibrbdWriteback::LibrbdWriteback(ImageCtx *ictx, Mutex& lock)
    : m_tid(0), m_lock(lock), m_ictx(ictx) {
  }

  void LibrbdWriteback::read(const object_t& oid, uint64_t object_no,
			     const object_locator_t& oloc,
			     uint64_t off, uint64_t len, snapid_t snapid,
			     bufferlist *pbl, uint64_t trunc_size,
			     __u32 trunc_seq, int op_flags,
                             const ZTracer::Trace &parent_trace,
                             Context *onfinish)
  {
    ZTracer::Trace trace;
    if (parent_trace.valid()) {
      trace.init("", &m_ictx->trace_endpoint, &parent_trace);
      trace.copy_name("cache read " + oid.name);
      trace.event("start");
    }

    // on completion, take the mutex and then call onfinish.
    onfinish = new C_ReadRequest(m_ictx->cct, onfinish, &m_lock);

    // re-use standard object read state machine
    auto aio_comp = io::AioCompletion::create_and_start(onfinish, m_ictx,
                                                        io::AIO_TYPE_READ);
    aio_comp->read_result = io::ReadResult{pbl};
    aio_comp->set_request_count(1);

    auto req_comp = new io::ReadResult::C_ObjectReadRequest(
      aio_comp, off, len, {{0, len}}, false);

    auto req = io::ObjectDispatchSpec::create_read(
      m_ictx, io::OBJECT_DISPATCH_LAYER_CACHE, oid.name, object_no, off, len,
      snapid, op_flags, trace, &req_comp->bl, &req_comp->extent_map, req_comp);
    req->send();
  }

  bool LibrbdWriteback::may_copy_on_write(const object_t& oid, uint64_t read_off, uint64_t read_len, snapid_t snapid)
  {
    m_ictx->snap_lock.get_read();
    librados::snap_t snap_id = m_ictx->snap_id;
    m_ictx->parent_lock.get_read();
    uint64_t overlap = 0;
    m_ictx->get_parent_overlap(snap_id, &overlap);
    m_ictx->parent_lock.put_read();
    m_ictx->snap_lock.put_read();

    uint64_t object_no = oid_to_object_no(oid.name, m_ictx->object_prefix);

    // reverse map this object extent onto the parent
    vector<pair<uint64_t,uint64_t> > objectx;
    Striper::extent_to_file(m_ictx->cct, &m_ictx->layout,
			  object_no, 0, m_ictx->layout.object_size,
			  objectx);
    uint64_t object_overlap = m_ictx->prune_parent_extents(objectx, overlap);
    bool may = object_overlap > 0;
    ldout(m_ictx->cct, 10) << "may_copy_on_write " << oid << " " << read_off
			   << "~" << read_len << " = " << may << dendl;
    return may;
  }

  ceph_tid_t LibrbdWriteback::write(const object_t& oid,
				    const object_locator_t& oloc,
				    uint64_t off, uint64_t len,
				    const SnapContext& snapc,
				    const bufferlist &bl,
				    ceph::real_time mtime, uint64_t trunc_size,
				    __u32 trunc_seq, ceph_tid_t journal_tid,
                                    const ZTracer::Trace &parent_trace,
				    Context *oncommit)
  {
    ZTracer::Trace trace;
    if (parent_trace.valid()) {
      trace.init("", &m_ictx->trace_endpoint, &parent_trace);
      trace.copy_name("writeback " + oid.name);
      trace.event("start");
    }

    uint64_t object_no = oid_to_object_no(oid.name, m_ictx->object_prefix);

    write_result_d *result = new write_result_d(oid.name, oncommit);
    m_writes[oid.name].push(result);
    ldout(m_ictx->cct, 20) << "write will wait for result " << result << dendl;

    bufferlist bl_copy(bl);

    Context *ctx = new C_OrderedWrite(m_ictx->cct, result, trace, this);
    ctx = util::create_async_context_callback(*m_ictx, ctx);

    auto req = io::ObjectDispatchSpec::create_write(
      m_ictx, io::OBJECT_DISPATCH_LAYER_CACHE, oid.name, object_no, off,
      std::move(bl_copy), snapc, 0, journal_tid, trace, ctx);
    req->object_dispatch_flags = (
      io::OBJECT_DISPATCH_FLAG_FLUSH |
      io::OBJECT_DISPATCH_FLAG_WILL_RETRY_ON_ERROR);
    req->send();

    return ++m_tid;
  }


  void LibrbdWriteback::overwrite_extent(const object_t& oid, uint64_t off,
					 uint64_t len,
					 ceph_tid_t original_journal_tid,
                                         ceph_tid_t new_journal_tid) {
    typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

    ldout(m_ictx->cct, 20) << __func__ << ": " << oid << " "
                           << off << "~" << len << " "
                           << "journal_tid=" << original_journal_tid << ", "
                           << "new_journal_tid=" << new_journal_tid << dendl;

    uint64_t object_no = oid_to_object_no(oid.name, m_ictx->object_prefix);

    // all IO operations are flushed prior to closing the journal
    assert(original_journal_tid != 0 && m_ictx->journal != NULL);

    Extents file_extents;
    Striper::extent_to_file(m_ictx->cct, &m_ictx->layout, object_no, off,
			    len, file_extents);
    for (Extents::iterator it = file_extents.begin();
	 it != file_extents.end(); ++it) {
      if (new_journal_tid != 0) {
        // ensure new journal event is safely committed to disk before
        // committing old event
        m_ictx->journal->flush_event(
          new_journal_tid, new C_CommitIOEventExtent(m_ictx,
                                                     original_journal_tid,
                                                     it->first, it->second));
      } else {
        m_ictx->journal->commit_io_event_extent(original_journal_tid, it->first,
					        it->second, 0);
      }
    }
  }

  void LibrbdWriteback::complete_writes(const std::string& oid)
  {
    assert(m_lock.is_locked());
    std::queue<write_result_d*>& results = m_writes[oid];
    ldout(m_ictx->cct, 20) << "complete_writes() oid " << oid << dendl;
    std::list<write_result_d*> finished;

    while (!results.empty()) {
      write_result_d *result = results.front();
      if (!result->done)
	break;
      finished.push_back(result);
      results.pop();
    }

    if (results.empty())
      m_writes.erase(oid);

    for (std::list<write_result_d*>::iterator it = finished.begin();
	 it != finished.end(); ++it) {
      write_result_d *result = *it;
      ldout(m_ictx->cct, 20) << "complete_writes() completing " << result
			     << dendl;
      result->oncommit->complete(result->ret);
      delete result;
    }
  }
}
