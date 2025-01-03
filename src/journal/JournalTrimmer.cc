// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalTrimmer.h"
#include "journal/Utils.h"
#include "common/Cond.h"
#include "common/errno.h"
#include <limits>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalTrimmer: " << this << " "

namespace journal {

struct JournalTrimmer::C_RemoveSet : public Context {
  JournalTrimmer *journal_trimmer;
  uint64_t object_set;
  ceph::mutex lock = ceph::make_mutex("JournalTrimmer::m_lock");
  uint32_t refs;
  int return_value;

  C_RemoveSet(JournalTrimmer *_journal_trimmer, uint64_t _object_set,
              uint8_t _splay_width);
  void complete(int r) override;
  void finish(int r) override {
    journal_trimmer->handle_set_removed(r, object_set);
    journal_trimmer->m_async_op_tracker.finish_op();
  }
};

JournalTrimmer::JournalTrimmer(librados::IoCtx &ioctx,
                               const std::string &object_oid_prefix,
                               const ceph::ref_t<JournalMetadata>& journal_metadata)
    : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
      m_journal_metadata(journal_metadata), m_metadata_listener(this),
      m_remove_set_pending(false),
      m_remove_set(0), m_remove_set_ctx(NULL) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  m_journal_metadata->add_listener(&m_metadata_listener);
}

JournalTrimmer::~JournalTrimmer() {
  ceph_assert(m_shutdown);
}

void JournalTrimmer::shut_down(Context *on_finish) {
  ldout(m_cct, 20) << __func__ << dendl;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(!m_shutdown);
    m_shutdown = true;
  }

  m_journal_metadata->remove_listener(&m_metadata_listener);

  // chain the shut down sequence (reverse order)
  on_finish = new LambdaContext([this, on_finish](int r) {
      m_async_op_tracker.wait_for_ops(on_finish);
    });
  m_journal_metadata->flush_commit_position(on_finish);
}

void JournalTrimmer::remove_objects(bool force, Context *on_finish) {
  ldout(m_cct, 20) << __func__ << dendl;

  on_finish = new LambdaContext([this, force, on_finish](int r) {
				    std::lock_guard locker{m_lock};

      if (m_remove_set_pending) {
        on_finish->complete(-EBUSY);
      }

      if (!force) {
        JournalMetadata::RegisteredClients registered_clients;
        m_journal_metadata->get_registered_clients(&registered_clients);

        if (registered_clients.size() == 0) {
          on_finish->complete(-EINVAL);
          return;
        } else if (registered_clients.size() > 1) {
          on_finish->complete(-EBUSY);
          return;
        }
      }

      m_remove_set = std::numeric_limits<uint64_t>::max();
      m_remove_set_pending = true;
      m_remove_set_ctx = on_finish;

      remove_set(m_journal_metadata->get_minimum_set());
    });

  m_async_op_tracker.wait_for_ops(on_finish);
}

void JournalTrimmer::committed(uint64_t commit_tid) {
  ldout(m_cct, 20) << __func__ << ": commit_tid=" << commit_tid << dendl;
  m_journal_metadata->committed(commit_tid,
                                m_create_commit_position_safe_context);
}

void JournalTrimmer::trim_objects(uint64_t minimum_set) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  ldout(m_cct, 20) << __func__ << ": min_set=" << minimum_set << dendl;
  if (minimum_set <= m_journal_metadata->get_minimum_set()) {
    return;
  }

  if (m_remove_set_pending) {
    m_remove_set = std::max(m_remove_set, minimum_set);
    return;
  }

  m_remove_set = minimum_set;
  m_remove_set_pending = true;
  remove_set(m_journal_metadata->get_minimum_set());
}

void JournalTrimmer::remove_set(uint64_t object_set) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  m_async_op_tracker.start_op();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  C_RemoveSet *ctx = new C_RemoveSet(this, object_set, splay_width);

  ldout(m_cct, 20) << __func__ << ": removing object set " << object_set
                   << dendl;
  for (uint64_t object_number = object_set * splay_width;
       object_number < (object_set + 1) * splay_width;
       ++object_number) {
    std::string oid = utils::get_object_name(m_object_oid_prefix,
                                             object_number);

    ldout(m_cct, 20) << "removing journal object " << oid << dendl;
    auto comp =
      librados::Rados::aio_create_completion(ctx, utils::rados_ctx_callback);
    int r = m_ioctx.aio_remove(oid, comp,
                               CEPH_OSD_FLAG_FULL_FORCE | CEPH_OSD_FLAG_FULL_TRY);
    ceph_assert(r == 0);
    comp->release();
  }
}

void JournalTrimmer::handle_metadata_updated() {
  ldout(m_cct, 20) << __func__ << dendl;

  std::lock_guard locker{m_lock};

  JournalMetadata::RegisteredClients registered_clients;
  m_journal_metadata->get_registered_clients(&registered_clients);

  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint64_t minimum_set = m_journal_metadata->get_minimum_set();
  uint64_t active_set = m_journal_metadata->get_active_set();
  uint64_t minimum_commit_set = active_set;
  std::string minimum_client_id;

  for (auto &client : registered_clients) {
    if (client.state == cls::journal::CLIENT_STATE_DISCONNECTED) {
      continue;
    }

    if (client.commit_position.object_positions.empty()) {
      // client hasn't recorded any commits
      minimum_commit_set = minimum_set;
      minimum_client_id = client.id;
      break;
    }

    for (auto &position : client.commit_position.object_positions) {
      uint64_t object_set = position.object_number / splay_width;
      if (object_set < minimum_commit_set) {
        minimum_client_id = client.id;
        minimum_commit_set = object_set;
      }
    }
  }

  if (minimum_commit_set > minimum_set) {
    trim_objects(minimum_commit_set);
  } else {
    ldout(m_cct, 20) << "object set " << minimum_commit_set << " still "
                     << "in-use by client " << minimum_client_id << dendl;
  }
}

void JournalTrimmer::handle_set_removed(int r, uint64_t object_set) {
  ldout(m_cct, 20) << __func__ << ": r=" << r << ", set=" << object_set << ", "
                   << "trim=" << m_remove_set << dendl;

  std::lock_guard locker{m_lock};
  m_remove_set_pending = false;

  if (r == -ENOENT) {
    // no objects within the set existed
    r = 0;
  }
  if (r == 0) {
    // advance the minimum set to the next set
    m_journal_metadata->set_minimum_set(object_set + 1);
    uint64_t active_set = m_journal_metadata->get_active_set();
    uint64_t minimum_set = m_journal_metadata->get_minimum_set();

    if (m_remove_set > minimum_set && minimum_set <= active_set) {
      m_remove_set_pending = true;
      remove_set(minimum_set);
    }
  }

  if (m_remove_set_ctx != nullptr && !m_remove_set_pending) {
    ldout(m_cct, 20) << "completing remove set context" << dendl;
    m_remove_set_ctx->complete(r);
    m_remove_set_ctx = nullptr;
  }
}

JournalTrimmer::C_RemoveSet::C_RemoveSet(JournalTrimmer *_journal_trimmer,
                                         uint64_t _object_set,
                                         uint8_t _splay_width)
  : journal_trimmer(_journal_trimmer), object_set(_object_set),
    lock(ceph::make_mutex(utils::unique_lock_name("C_RemoveSet::lock", this))),
    refs(_splay_width), return_value(-ENOENT) {
}

void JournalTrimmer::C_RemoveSet::complete(int r) {
  lock.lock();
  if (r < 0 && r != -ENOENT &&
      (return_value == -ENOENT || return_value == 0)) {
    return_value = r;
  } else if (r == 0 && return_value == -ENOENT) {
    return_value = 0;
  }

  if (--refs == 0) {
    finish(return_value);
    lock.unlock();
    delete this;
  } else {
    lock.unlock();
  }
}

} // namespace journal
