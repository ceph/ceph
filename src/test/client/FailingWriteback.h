// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <atomic>
#include <memory>
#include <utility>

#include "test/client/TestClient.h"
#include "osdc/WritebackHandler.h"

// Keep the real Objecter I/O and its completion thread/locking. Only replace
// one successful completion, so ObjectCacher exercises its normal error path.
class FailingWriteback : public WritebackHandler {
  std::unique_ptr<WritebackHandler> backing;
  object_t target;
  int64_t target_pool = -1;
  int next_write_error = 0;
  unsigned failures = 0;

  Context* wrap(const object_t& oid, const object_locator_t& oloc,
                Context* onfinish) {
    if (!next_write_error || oid != target || oloc.pool != target_pool) {
      return onfinish;
    }
    const int error = std::exchange(next_write_error, 0);
    return make_lambda_context([this, error, onfinish](int r) {
      if (r >= 0) {
        r = error;
        ++failures;
      }
      onfinish->complete(r);
    });
  }

public:
  explicit FailingWriteback(std::unique_ptr<WritebackHandler> backing)
    : backing(std::move(backing)) {}

  // These methods, like WritebackHandler callbacks, run under client_lock.
  // Fail the next write of the file's first object.
  void arm_write(Inode* in, int error) {
    target = file_object_t(in->ino, 0);
    target_pool = in->layout.pool_id;
    next_write_error = error;
  }
  void disarm() { next_write_error = 0; }
  unsigned failure_count() const { return failures; }

  void read(const object_t& oid, uint64_t object_no,
            const object_locator_t& oloc, uint64_t off, uint64_t len,
            snapid_t snapid, bufferlist* bl, uint64_t trunc_size,
            __u32 trunc_seq, int op_flags, const ZTracer::Trace& trace,
            Context* onfinish) override {
    backing->read(oid, object_no, oloc, off, len, snapid, bl, trunc_size,
                  trunc_seq, op_flags, trace, onfinish);
  }

  bool may_copy_on_write(const object_t& oid, uint64_t off, uint64_t len,
                         snapid_t snapid) override {
    return backing->may_copy_on_write(oid, off, len, snapid);
  }

  void overwrite_extent(const object_t& oid, uint64_t off, uint64_t len,
                        ceph_tid_t original_tid, ceph_tid_t new_tid) override {
    backing->overwrite_extent(oid, off, len, original_tid, new_tid);
  }

  ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
                   uint64_t off, uint64_t len, const SnapContext& snapc,
                   const bufferlist& bl, ceph::real_time mtime,
                   uint64_t trunc_size, __u32 trunc_seq,
                   ceph_tid_t journal_tid, const ZTracer::Trace& trace,
                   Context* oncommit) override {
    return backing->write(oid, oloc, off, len, snapc, bl, mtime, trunc_size,
                          trunc_seq, journal_tid, trace,
                          wrap(oid, oloc, oncommit));
  }

  bool can_scattered_write() override {
    return backing->can_scattered_write();
  }

  ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
                   std::vector<std::pair<uint64_t, bufferlist>>& io_vec,
                   const SnapContext& snapc, ceph::real_time mtime,
                   uint64_t trunc_size, __u32 trunc_seq,
                   Context* oncommit) override {
    return backing->write(oid, oloc, io_vec, snapc, mtime, trunc_size,
                          trunc_seq, wrap(oid, oloc, oncommit));
  }
};

class FailingWritebackClient : public ClientScaffold {
  FailingWriteback* failing_writeback;

public:
  FailingWritebackClient(Messenger* messenger, MonClient* mc,
                         Objecter* objecter, fscrypt_env* fse)
    : ClientScaffold(messenger, mc, objecter, fse) {
    // This runs before init(): the original cache is empty and its flusher
    // has not started. Preserve the original handler and its private finisher.
    objectcacher.reset();
    auto handler = std::make_unique<FailingWriteback>(
        std::move(writeback_handler));
    failing_writeback = handler.get();
    writeback_handler = std::move(handler);
    objectcacher = std::make_unique<ObjectCacher>(
        cct, "libcephfs", *writeback_handler, client_lock,
        [](void* p, ObjectCacher::ObjectSet* oset) {
          static_cast<FailingWritebackClient*>(p)->flush_set_callback(oset);
        }, this,
        // Keep this test's small dirty writes out of background writeback.
        64 << 20, 1024, 32 << 20, 16 << 20, 3600, true);
  }

  void arm_write(Inode* in, int error) {
    std::scoped_lock lock(client_lock);
    failing_writeback->arm_write(in, error);
  }

  void disarm() {
    std::scoped_lock lock(client_lock);
    failing_writeback->disarm();
  }

  unsigned failure_count() {
    std::scoped_lock lock(client_lock);
    return failing_writeback->failure_count();
  }

  bool has_dirty_data(Inode* in) {
    std::scoped_lock lock(client_lock);
    return in->oset.dirty_or_tx > 0;
  }
};

class CountingCompletion : public C_SaferCond {
public:
  std::atomic<unsigned> calls = 0;

  void complete(int r) override {
    ++calls;
    C_SaferCond::complete(r);
  }
};
