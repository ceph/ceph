// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalTrimmer.h"
#include "journal/Utils.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Finisher.h"
#include <limits>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalTrimmer: "

namespace journal {

JournalTrimmer::JournalTrimmer(librados::IoCtx &ioctx,
                               const std::string &object_oid_prefix,
                               const JournalMetadataPtr &journal_metadata)
    : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
      m_journal_metadata(journal_metadata), m_lock("JournalTrimmer::m_lock"),
      m_remove_set_pending(false), m_remove_set(0), m_remove_set_ctx(NULL) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

JournalTrimmer::~JournalTrimmer() {
  m_async_op_tracker.wait_for_ops();
}

int JournalTrimmer::remove_objects() {
  ldout(m_cct, 20) << __func__ << dendl;
  m_async_op_tracker.wait_for_ops();

  C_SaferCond ctx;
  {
    Mutex::Locker locker(m_lock);
    JournalMetadata::RegisteredClients registered_clients;
    m_journal_metadata->get_registered_clients(&registered_clients);

    if (registered_clients.size() == 0) {
      return -EINVAL;
    } else if (registered_clients.size() > 1 || m_remove_set_pending) {
      return -EBUSY;
    }

    m_remove_set = std::numeric_limits<uint64_t>::max();
    m_remove_set_pending = true;
    m_remove_set_ctx = &ctx;

    remove_set(m_journal_metadata->get_minimum_set());
  }
  return ctx.wait();
}

void JournalTrimmer::update_commit_position(
    const ObjectSetPosition &object_set_position) {
  ldout(m_cct, 20) << __func__ << ": pos=" << object_set_position
                   << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.start_op();
  }

  Context *ctx = new C_CommitPositionSafe(this, object_set_position);
  m_journal_metadata->set_commit_position(object_set_position, ctx);
}

void JournalTrimmer::trim_objects(uint64_t minimum_set) {
  assert(m_lock.is_locked());

  ldout(m_cct, 20) << __func__ << ": min_set=" << minimum_set << dendl;
  if (minimum_set <= m_journal_metadata->get_minimum_set()) {
    return;
  }

  if (m_remove_set_pending) {
    m_remove_set = MAX(m_remove_set, minimum_set);
    return;
  }

  m_remove_set = minimum_set;
  m_remove_set_pending = true;
  remove_set(m_journal_metadata->get_minimum_set());
}

void JournalTrimmer::remove_set(uint64_t object_set) {
  assert(m_lock.is_locked());

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
    librados::AioCompletion *comp =
      librados::Rados::aio_create_completion(ctx, NULL,
                                             utils::rados_ctx_callback);
    int r = m_ioctx.aio_remove(oid, comp);
    assert(r == 0);
    comp->release();
  }
}

void JournalTrimmer::handle_commit_position_safe(
    int r, const ObjectSetPosition &object_set_position) {
  ldout(m_cct, 20) << __func__ << ": r=" << r << ", pos="
                   << object_set_position << dendl;

  Mutex::Locker locker(m_lock);
  if (r == 0) {
    uint8_t splay_width = m_journal_metadata->get_splay_width();
    uint64_t object_set = object_set_position.object_number / splay_width;

    JournalMetadata::RegisteredClients registered_clients;
    m_journal_metadata->get_registered_clients(&registered_clients);

    bool trim_permitted = true;
    for (JournalMetadata::RegisteredClients::iterator it =
           registered_clients.begin();
         it != registered_clients.end(); ++it) {
      const JournalMetadata::Client &client = *it;
      uint64_t client_object_set = client.commit_position.object_number /
                                   splay_width;
      if (client.id != m_journal_metadata->get_client_id() &&
          client_object_set < object_set) {
        ldout(m_cct, 20) << "object set " << client_object_set << " still "
                         << "in-use by client " << client.id << dendl;
        trim_permitted = false;
        break;
      }
    }

    if (trim_permitted) {
      trim_objects(object_set_position.object_number / splay_width);
    }
  }
  m_async_op_tracker.finish_op();
}

void JournalTrimmer::handle_set_removed(int r, uint64_t object_set) {
  ldout(m_cct, 20) << __func__ << ": r=" << r << ", set=" << object_set << ", "
                   << "trim=" << m_remove_set << dendl;

  Mutex::Locker locker(m_lock);
  m_remove_set_pending = false;

  if (r == 0 || (r == -ENOENT && m_remove_set_ctx == NULL)) {
    // advance the minimum set to the next set
    m_journal_metadata->set_minimum_set(object_set + 1);
    uint64_t minimum_set = m_journal_metadata->get_minimum_set();

    if (m_remove_set > minimum_set) {
      m_remove_set_pending = true;
      remove_set(minimum_set);
    }
  } else if (r == -ENOENT) {
    // no objects within the set existed
    r = 0;
  }

  if (m_remove_set_ctx != NULL && !m_remove_set_pending) {
    ldout(m_cct, 20) << "completing remove set context" << dendl;
    m_remove_set_ctx->complete(r);
  }
  m_async_op_tracker.finish_op();
}

JournalTrimmer::C_RemoveSet::C_RemoveSet(JournalTrimmer *_journal_trimmer,
                                         uint64_t _object_set,
                                         uint8_t _splay_width)
  : journal_trimmer(_journal_trimmer), object_set(_object_set),
    lock(utils::unique_lock_name("C_RemoveSet::lock", this)),
    refs(_splay_width), return_value(-ENOENT) {
}

void JournalTrimmer::C_RemoveSet::complete(int r) {
  lock.Lock();
  if (r < 0 && r != -ENOENT && return_value == -ENOENT) {
    return_value = r;
  } else if (r == 0 && return_value == -ENOENT) {
    return_value = 0;
  }

  if (--refs == 0) {
    finish(return_value);
    lock.Unlock();
    delete this;
  } else {
    lock.Unlock();
  }
}

} // namespace journal
