// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "LogOperation.h"
#include "librbd/cache/rwl/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogOperation: " << this << " " \
			   <<  __func__ << ": "

namespace librbd {

namespace cache {

namespace rwl {

template <typename T>
GenericLogOperation<T>::GenericLogOperation(T &rwl, const utime_t dispatch_time)
  : rwl(rwl), dispatch_time(dispatch_time) {
}

template <typename T>
std::ostream& GenericLogOperation<T>::format(std::ostream &os) const {
  os << "dispatch_time=[" << dispatch_time << "], "
     << "buf_persist_time=[" << buf_persist_time << "], "
     << "buf_persist_comp_time=[" << buf_persist_comp_time << "], "
     << "log_append_time=[" << log_append_time << "], "
     << "log_append_comp_time=[" << log_append_comp_time << "], ";
  return os;
};

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const GenericLogOperation<T> &op) {
  return op.format(os);
}

template <typename T>
SyncPointLogOperation<T>::SyncPointLogOperation(T &rwl,
                                                std::shared_ptr<SyncPoint<T>> sync_point,
                                                const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time), sync_point(sync_point) {
}

template <typename T>
SyncPointLogOperation<T>::~SyncPointLogOperation() { }

template <typename T>
std::ostream &SyncPointLogOperation<T>::format(std::ostream &os) const {
  os << "(Sync Point) ";
  GenericLogOperation<T>::format(os);
  os << ", "
     << "sync_point=[" << *sync_point << "]";
  return os;
};

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const SyncPointLogOperation<T> &op) {
  return op.format(os);
}

template <typename T>
void SyncPointLogOperation<T>::appending() {
  std::vector<Context*> appending_contexts;

  assert(sync_point);
  {
    std::lock_guard locker(rwl.m_lock);
    if (!sync_point->m_appending) {
      ldout(rwl.m_image_ctx.cct, 20) << "Sync point op=[" << *this
                                     << "] appending" << dendl;
      sync_point->m_appending = true;
    }
    appending_contexts.swap(sync_point->m_on_sync_point_appending);
  }
  for (auto &ctx : appending_contexts) {
    ctx->complete(0);
  }
}

template <typename T>
void SyncPointLogOperation<T>::complete(int result) {
  std::vector<Context*> persisted_contexts;

  assert(sync_point);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "Sync point op =[" << *this
                                   << "] completed" << dendl;
  }
  {
    std::lock_guard locker(rwl.m_lock);
    /* Remove link from next sync point */
    assert(sync_point->later_sync_point);
    assert(sync_point->later_sync_point->earlier_sync_point ==
           sync_point);
    sync_point->later_sync_point->earlier_sync_point = nullptr;
  }

  /* Do append now in case completion occurred before the
   * normal append callback executed, and to handle
   * on_append work that was queued after the sync point
   * entered the appending state. */
  appending();

  {
    std::lock_guard locker(rwl.m_lock);
    /* The flush request that scheduled this op will be one of these
     * contexts */
    persisted_contexts.swap(sync_point->m_on_sync_point_persisted);
    // TODO handle flushed sync point in later PRs
  }
  for (auto &ctx : persisted_contexts) {
    ctx->complete(result);
  }
}

template <typename T>
GeneralWriteLogOperation<T>::GeneralWriteLogOperation(T &rwl,
                                                      std::shared_ptr<SyncPoint<T>> sync_point,
                                                      const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time),
  m_lock("librbd::cache::rwl::GeneralWriteLogOperation::m_lock"), sync_point(sync_point) {
}

template <typename T>
GeneralWriteLogOperation<T>::~GeneralWriteLogOperation() { }

template <typename T>
std::ostream &GeneralWriteLogOperation<T>::format(std::ostream &os) const {
  GenericLogOperation<T>::format(os);
  return os;
};

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const GeneralWriteLogOperation<T> &op) {
  return op.format(os);
}

/* Called when the write log operation is appending and its log position is guaranteed */
template <typename T>
void GeneralWriteLogOperation<T>::appending() {
  Context *on_append = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    std::lock_guard locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }
  if (on_append) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_append=" << on_append << dendl;
    }
    on_append->complete(0);
  }
}

/* Called when the write log operation is completed in all log replicas */
template <typename T>
void GeneralWriteLogOperation<T>::complete(int result) {
  appending();
  Context *on_persist = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    std::lock_guard locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }
  if (on_persist) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_persist=" << on_persist << dendl;
    }
    on_persist->complete(result);
  }
}

template <typename T>
WriteLogOperation<T>::WriteLogOperation(WriteLogOperationSet<T> &set,
                                        uint64_t image_offset_bytes, uint64_t write_bytes)
  : GeneralWriteLogOperation<T>(set.rwl, set.sync_point, set.dispatch_time),
    log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = set.extent_ops_appending->new_sub();
  on_write_persist = set.extent_ops_persist->new_sub();
  log_entry->sync_point_entry->writes++;
  log_entry->sync_point_entry->bytes += write_bytes;
}

template <typename T>
WriteLogOperation<T>::~WriteLogOperation() { }

template <typename T>
std::ostream &WriteLogOperation<T>::format(std::ostream &os) const {
  os << "(Write) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  os << "bl=[" << bl << "],"
     << "buffer_alloc=" << buffer_alloc;
  return os;
};

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const WriteLogOperation<T> &op) {
  return op.format(os);
}


template <typename T>
WriteLogOperationSet<T>::WriteLogOperationSet(T &rwl, utime_t dispatched, std::shared_ptr<SyncPoint<T>> sync_point,
                                              bool persist_on_flush, Context *on_finish)
  : rwl(rwl), m_on_finish(on_finish),
    persist_on_flush(persist_on_flush), dispatch_time(dispatched), sync_point(sync_point) {
  on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
  on_ops_persist = nullptr;
  extent_ops_persist =
    new C_Gather(rwl.m_image_ctx.cct,
                 new LambdaContext( [this](int r) {
                     if (RWL_VERBOSE_LOGGING) {
                       ldout(this->rwl.m_image_ctx.cct,20) << __func__ << " " << this << " m_extent_ops_persist completed" << dendl;
                     }
                     if (on_ops_persist) {
                       on_ops_persist->complete(r);
                     }
                     m_on_finish->complete(r);
                   }));
  auto appending_persist_sub = extent_ops_persist->new_sub();
  extent_ops_appending =
    new C_Gather(rwl.m_image_ctx.cct,
                 new LambdaContext( [this, appending_persist_sub](int r) {
                     if (RWL_VERBOSE_LOGGING) {
                       ldout(this->rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " m_extent_ops_appending completed" << dendl;
                     }
                     on_ops_appending->complete(r);
                     appending_persist_sub->complete(r);
                   }));
}

template <typename T>
WriteLogOperationSet<T>::~WriteLogOperationSet() { }

template <typename T>
std::ostream &operator<<(std::ostream &os,
                                const WriteLogOperationSet<T> &s) {
  os << "cell=" << (void*)s.cell << ", "
     << "extent_ops_appending=[" << s.extent_ops_appending << ", "
     << "extent_ops_persist=[" << s.extent_ops_persist << "]";
  return os;
};

} // namespace rwl
} // namespace cache
} // namespace librbd
