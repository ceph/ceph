#include "mds/QuiesceAgent.h"
#include "common/debug.h"
#include "include/ceph_assert.h"
#include <future>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_quiesce
#undef dout_prefix
#define dout_prefix *_dout << "quiesce.agt <" << __func__ << "> "

#undef dout
#define dout(lvl)                                                        \
  do {                                                                   \
    auto subsys = ceph_subsys_mds;                                       \
    if ((dout_context)->_conf->subsys.should_gather(dout_subsys, lvl)) { \
      subsys = dout_subsys;                                              \
    }                                                                    \
  dout_impl(dout_context, ceph::dout::need_dynamic(subsys), lvl) dout_prefix

#undef dendl
#define dendl \
  dendl_impl; \
  }           \
  while (0)

template <class CharT, class Traits>
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const QuiesceAgent::TrackedRootsVersion& tr)
{
  return os << "tracked_roots[armed: " << tr.armed << ", v: " << tr.db_version << " r: " << tr.roots.size() << "]";
}

bool QuiesceAgent::db_update(QuiesceMap& map)
{
  // copy of the current roots
  TrackedRoots current_roots = tracked_roots();
  TrackedRoots new_roots;

  dout(20) << "got a db update version " << map.db_version << " with " << map.roots.size() << " roots" << dendl;

  for (auto their_it = map.roots.begin(); their_it != map.roots.end();) {
    auto &[root, info] = *their_it;
    TrackedRootRef tracked_root_ref;

    ceph_assert(info.state > QS__INVALID);

    if (info.state >= QS__FAILURE) {
      // we don't care about roots in failed states
      dout(5) << "ignoring a root in a failed state: '" << root << "', " << info.state << dendl;
      their_it = map.roots.erase(their_it);
      continue;
    }

    if (const auto& my_it = current_roots.find(root); my_it != current_roots.end()) {
      // keep the existing root
      new_roots.insert(*my_it);
      tracked_root_ref = my_it->second;
    } else {
      // introduce a new root
      tracked_root_ref = std::make_shared<TrackedRoot>(info.state, info.ttl);
      new_roots[root] = tracked_root_ref;
    }

    std::lock_guard l(*tracked_root_ref);
    tracked_root_ref->update_committed(info);

    auto actual_state = tracked_root_ref->get_actual_state();
    
    if (actual_state != info.state) {
      // we have an update for the state
      info.state = actual_state;
      info.ttl = tracked_root_ref->get_ttl();
    } else {
      // our tracked root has the same state as the db
      // we can just drop it from the response
      their_it = map.roots.erase(their_it);
      continue;
    }
    ++their_it;
  }

  // ack with the known state stored in `map`
  set_pending_roots(map.db_version, std::move(new_roots));

  // to avoid ack races with the agent_thread,
  // never send a synchronous ack
  return false;
}

void* QuiesceAgent::agent_thread_main() {
  std::unique_lock agent_lock(agent_mutex);

  while (!stop_agent_thread) {
    TrackedRootsVersion old;

    if (pending.armed) {
      std::swap(old, current);
      current.roots.swap(pending.roots);
      current.db_version = pending.db_version;
    }

    dout(20)
        << "old = " << old.db_version
        << ", current = " << current.db_version
        << dendl;

    // it's safe to clear the pending roots under agent_lock because it shouldn't
    // ever hold a last shared ptr to quiesced tracked roots, causing their destructors to run cancel.
    pending.clear();
    current.armed = true;
    upkeep_needed = false;

    // for somebody waiting for the internal state to progress
    agent_cond.notify_all();
    agent_lock.unlock();

    _agent_thread_will_work();

    QuiesceMap ack(current.db_version);
  
    // upkeep what we believe is the current state.
    for (auto& [root, info] : current.roots) {

      info->lock();
      bool should_quiesce = info->should_quiesce();
      bool issue_quiesce = should_quiesce && !info->quiesce_request && !info->quiesce_result;
      std::optional<QuiesceInterface::RequestHandle> cancel_handle;
      if (!should_quiesce && !info->cancel_result) {
        cancel_handle = info->quiesce_request;
      }
      auto actual_state = info->get_actual_state();
      if (info->committed_state != actual_state) {
        ack.roots[root] = { actual_state, info->get_ttl() };
      }
      info->unlock();

      if (issue_quiesce) {
        std::weak_ptr<TrackedRoot> weak_info = info;
        auto request_handle = quiesce_control.submit_request(root, new LambdaContext([weak_info, submitted_root = root, this](int rc) {
          if (auto info = weak_info.lock()) {
            dout(20) << "completing request (rc=" << rc << ") for '" << submitted_root << "'" << dendl;
            info->lock();
            info->quiesce_result = rc;
            info->unlock();

            // TODO: capturing QuiesceAgent& `this` is potentially dangerous
            //       the assumption is that since the tracked root pointer is weak
            //       it will have been deleted by the QuiesceAgent shutdown sequence
            set_upkeep_needed();
          }
          dout(20) << "done with submit callback for '" << submitted_root << "'" << dendl;
        }));

        dout(10) << "got request handle <" << request_handle << "> for '" << root << "'" << dendl;
        info->lock();
        info->quiesce_request = request_handle;
        info->cancel = quiesce_control.cancel_request;
        info->unlock();
      } else if (cancel_handle) {
        dout(10) << "Calling `cancel` on `" << root << "` with handle <" << *cancel_handle << ">" << dendl;
        int rc = quiesce_control.cancel_request(*cancel_handle);
        if (rc != 0) {
          dout(1) << "ERROR (" << rc << ") when trying to cancel quiesce request id: " << *cancel_handle << dendl;
        }
        info->lock();
        info->cancel_result = rc;
        info->unlock();
      }
    }

    _agent_thread_did_work();

    // send the ack and clear the old roots outside of the lock
    bool new_version = current.db_version != old.db_version;
    if (new_version || !ack.roots.empty()) {
      dout(20) << "asyncrhonous ack for " << (new_version ? "a new" : "the current") << " version: " << ack << dendl;
      int rc = quiesce_control.agent_ack(std::move(ack));
      if (rc != 0) {
        dout(3) << "got error: " << rc << " trying to send " << ack << dendl;
      }
    }
    old.clear();
    ack.clear();

    agent_lock.lock();

    current.armed = false;
    // a new pending version could be set while we weren't locked
    // if that's the case just go for another pass
    // otherwise, wait for updates
    while (!pending.armed && !current.armed && !upkeep_needed && !stop_agent_thread) {
      // for somebody waiting for the thread to idle
      agent_cond.notify_all();
      agent_cond.wait(agent_lock);
    }
  }
  agent_cond.notify_all();
  return nullptr;
}

void QuiesceAgent::set_pending_roots(QuiesceDbVersion version, TrackedRoots&& new_roots)
{
  std::unique_lock l(agent_mutex);

  bool rollback = current.db_version > version;

  if (rollback) {
    dout(5) << "version rollback to " << version 
      << ". current = " << current.db_version
      << ", pending = " << pending.db_version << dendl;
  }

  // set the pending version unconditionally
  pending.db_version = version;
  pending.roots = std::move(new_roots);
  pending.armed = true;

  agent_cond.notify_all();
}

void QuiesceAgent::set_upkeep_needed()
{
  std::unique_lock l(agent_mutex);

  dout(20)
      << "current = " << current.db_version
      << ", pending = " << pending.db_version << dendl;

  upkeep_needed = true;
  agent_cond.notify_all();
}

QuiesceAgent::TrackedRoot::~TrackedRoot()
{
  std::optional<QuiesceInterface::RequestHandle> request_handle;

  lock();
  request_handle.swap(quiesce_request);
  bool should_cancel = !cancel_result.has_value();
  unlock();

  if (should_cancel && request_handle && cancel) {
    dout(10) << "Calling `cancel` on an abandoned root with handle <" << request_handle << ">" << dendl;
    cancel(*request_handle);
  }

  dout(20) << "done with request handle <" << request_handle << ">" << dendl;
}
