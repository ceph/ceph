#include "MDSRank.h"
#include "MDCache.h"

#include "QuiesceDbManager.h"
#include "QuiesceAgent.h"

#include "messages/MMDSQuiesceDbListing.h"
#include "messages/MMDSQuiesceDbAck.h"

#include <boost/url.hpp>
#include <chrono>
#include <ranges>
#include <algorithm>
#include <queue>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_quiesce
#undef dout_prefix
#define dout_prefix *_dout << "quiesce.mds." << whoami << " <" << __func__ << "> "

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

void MDSRank::command_quiesce_db(const cmdmap_t& cmdmap, std::function<void(int, const std::string&, bufferlist&)> on_finish)
{
  // validate the command:
  using ceph::common::cmd_getval;
  using ceph::common::cmd_getval_or;
  using std::chrono::duration_cast;
  using dd = std::chrono::duration<double>;

  bool op_include = cmd_getval_or<bool>(cmdmap, "include", false);
  bool op_query = cmd_getval_or<bool>(cmdmap, "query", false);
  bool op_exclude = cmd_getval_or<bool>(cmdmap, "exclude", false);
  bool op_reset = cmd_getval_or<bool>(cmdmap, "reset", false);
  bool op_release = cmd_getval_or<bool>(cmdmap, "release", false);
  bool op_cancel = cmd_getval_or<bool>(cmdmap, "cancel", false);
  bool all = cmd_getval_or<bool>(cmdmap, "all", false);
  std::optional<std::string> set_id = cmd_getval<std::string>(cmdmap, "set_id");

  auto roots = cmd_getval_or<std::vector<std::string>>(cmdmap, "roots", std::vector<std::string> {});

  int all_ops = op_include + op_exclude + op_reset + op_release + op_cancel + op_query;

  if (all_ops > 1) {
    bufferlist bl;
    on_finish(-EINVAL, "Operations [include, exclude, reset, release, cancel, query] are mutually exclusive", bl);
    return;
  } else if (all_ops == 0) {
    op_include = true;
  }

  if ((op_release || op_cancel) && roots.size() > 0) {
    bufferlist bl;
    on_finish(-EINVAL, "Operations [release, cancel] don't take roots", bl);
    return;
  }

  if (op_cancel && !set_id && !all) {
    bufferlist bl;
    on_finish(-EINVAL, "Operation `cancel` requires a `--set-id` or `--all` to cancel all active sets", bl);
    return;
  }

  if (op_reset && !set_id) {
    bufferlist bl;
    on_finish(-EINVAL, "Operation `reset` requires a `--set-id`", bl);
    return;
  }

  if (op_reset && roots.empty()) {
    bufferlist bl;
    on_finish(-EINVAL, "Operation `reset` expects at least one root", bl);
    return;
  }

  if (op_query && roots.size() > 0) {
    bufferlist bl;
    on_finish(-EINVAL, "Operation `query` doesn't take any roots", bl);
    return;
  }

  if (!quiesce_db_manager) {
    bufferlist bl;
    on_finish(-EFAULT, "No quiesce db manager instance", bl);
    return;
  }

  struct Ctx : public QuiesceDbManager::RequestContext {
    std::function<void(int, const std::string&, bufferlist&)> on_finish;
    bool all = false;

    double sec(QuiesceTimeInterval duration) {
      return duration_cast<dd>(duration).count();
    }

    double age(QuiesceTimeInterval of, QuiesceTimeInterval ref) {
      return sec(ref - of);
    }

    double age(QuiesceTimeInterval of = QuiesceTimeInterval::zero()) {
      return age(of, response.db_age);
    }

    void finish(int rc)
    {
      auto f = Formatter::create_unique("json-pretty");
      CachedStackStringStream css;
      bufferlist outbl;

      auto dump_seconds = [&f](const std::string_view& name, double seconds) {
        f->dump_format_unquoted(name, "%0.1f", seconds);
      };

      f->open_object_section("response"); {
        f->dump_int("epoch", response.db_version.epoch);
        f->dump_int("set_version", response.db_version.set_version);
        f->open_object_section("sets"); {
          for (auto&& [set_id, set] : response.sets) {
            if (!all && !set.is_active() && set_id != request.set_id) {
              continue;
            }
            f->open_object_section(set_id); {
              f->dump_int("version", set.version);
              QuiesceTimeInterval ref = response.db_age;
              if (!set.is_active()) {
                ref = set.rstate.at_age;
              }
              dump_seconds("age_ref", age(ref));
              f->open_object_section("state"); {
                f->dump_string("name", quiesce_state_name(set.rstate.state));
                dump_seconds("age", age(set.rstate.at_age, ref));
              } f->close_section();
              dump_seconds("timeout", sec(set.timeout));
              dump_seconds("expiration", sec(set.expiration));
              f->open_object_section("members"); {
                for (auto&& [root, info] : set.members) {
                  f->open_object_section(root); {
                    f->dump_bool("excluded", info.excluded);
                    f->open_object_section("state"); {
                      f->dump_string("name", quiesce_state_name(info.rstate.state));
                      dump_seconds("age", age(info.rstate.at_age, ref));
                    } f->close_section();
                  } f->close_section();
                }
              } f->close_section();
            } f->close_section();
          }
        } f->close_section();
      } f->close_section();

      f->flush(outbl);
      on_finish(rc, css->str(), outbl);
    }
  };

  auto* ctx = new Ctx();

  ctx->on_finish = std::move(on_finish);
  ctx->all = all;

  ctx->request.reset([&](auto& r) {
    r.set_id = set_id;

    if (op_include) {
      r.include_roots(roots);
    } else if (op_exclude) {
      r.exclude_roots(roots);
    } else if (op_reset) {
      r.reset_roots(roots);
    } else if (op_release) {
      r.release_roots();
    } else if (op_cancel) {
      r.cancel_roots();
    }

    double timeout;

    if (cmd_getval(cmdmap, "await_for", timeout)) {
      r.await = duration_cast<QuiesceTimeInterval>(dd(timeout));
    } else if (cmd_getval_or<bool>(cmdmap, "await", false)) {
      r.await = QuiesceTimeInterval::max();
    }

    if (cmd_getval(cmdmap, "expiration", timeout)) {
      r.expiration = duration_cast<QuiesceTimeInterval>(dd(timeout));
    }

    if (cmd_getval(cmdmap, "timeout", timeout)) {
      r.timeout = duration_cast<QuiesceTimeInterval>(dd(timeout));
    }

    int64_t ifv;
    if (cmd_getval(cmdmap, "if_version", ifv)) {
      r.if_version = QuiesceSetVersion(ifv);
    }
  });

  dout(20) << "Submitting " << ctx->request << dendl;
  int rc = quiesce_db_manager->submit_request(ctx);
  if (rc != 0) {
    bufferlist bl;
    // on_finish was moved there, so should only call via the ctx.
    ctx->on_finish(rc, "Error submitting the command to the local db manager", bl);
    delete ctx;
  }
}

static void rebind_agent_callback(std::shared_ptr<QuiesceAgent> agt, std::shared_ptr<QuiesceDbManager> mgr) {
  if (!agt || !mgr) {
    return;
  }
  std::weak_ptr<QuiesceAgent> weak_agent = agt;
  mgr->reset_agent_callback([weak_agent](QuiesceMap& update) {
    if (auto agent = weak_agent.lock()) {
      return agent->db_update(update);
    } else {
      return false;
    }
  });
}

void MDSRank::quiesce_cluster_update() {
  // the quiesce leader is the lowest rank with the highest state up to ACTIVE
  auto less_leader = [](MDSMap::mds_info_t const* l, MDSMap::mds_info_t const* r) {
    ceph_assert(l->rank != MDS_RANK_NONE);
    ceph_assert(r->rank != MDS_RANK_NONE);
    ceph_assert(l->state <= MDSMap::STATE_ACTIVE);
    ceph_assert(r->state <= MDSMap::STATE_ACTIVE);
    if (l->rank == r->rank) {
      return l->state < r->state;
    } else {
      return l->rank > r->rank;
    }
  };

  std::priority_queue<MDSMap::mds_info_t const*, std::vector<MDSMap::mds_info_t const*>, decltype(less_leader)> member_info(less_leader);
  QuiesceClusterMembership membership;

  QuiesceInterface::PeerId me = mds_gid_t(monc->get_global_id());

  for (auto&& [gid, info] : mdsmap->get_mds_info()) {
    // if it has a rank and state <= ACTIVE, it's good enough
    // if (info.rank != MDS_RANK_NONE && info.state <= MDSMap::STATE_ACTIVE) {
    if (info.rank != MDS_RANK_NONE && info.state == MDSMap::STATE_ACTIVE) {
      member_info.push(&info);
      membership.members.insert(info.global_id);
    }
  }

  QuiesceInterface::PeerId leader = 
    member_info.empty() 
    ? QuiesceClusterMembership::INVALID_MEMBER 
    : member_info.top()->global_id;

  membership.epoch = mdsmap->get_epoch();
  membership.leader = leader;
  membership.me = me;
  membership.fs_name = mdsmap->get_fs_name();

  dout(5) << "epoch:" << membership.epoch << " me:" << me << " leader:" << leader << " members:" << membership.members 
    << (mdsmap->is_degraded() ? " (degraded)" : "") << dendl;

  if (leader != QuiesceClusterMembership::INVALID_MEMBER) {
    membership.send_ack = [=, this](QuiesceMap&& ack) {
      if (me == leader) {
        // loopback
        quiesce_db_manager->submit_ack_from(me, std::move(ack));
        return 0;
      } else {
        std::lock_guard guard(mds_lock);

        if (mdsmap->get_state_gid(leader) == MDSMap::STATE_NULL) {
          dout(5) << "couldn't find the leader " << leader << " in the map" << dendl;
          return -ENOENT;
        }
        auto addrs = mdsmap->get_info_gid(leader).addrs;

        auto ack_msg = make_message<MMDSQuiesceDbAck>();
        dout(10) << "sending ack " << ack << " to the leader " << leader << dendl;
        ack_msg->encode_payload_from(me, ack);
        return send_message_mds(ack_msg, addrs);
      }
    };

    membership.send_listing_to = [=, this](QuiesceInterface::PeerId to, QuiesceDbListing&& db) {
      std::lock_guard guard(mds_lock);
      if (mdsmap->get_state_gid(to) == MDSMap::STATE_NULL) {
        dout(5) << "couldn't find the peer " << to << " in the map" << dendl;
        return -ENOENT;
      }
      auto addrs = mdsmap->get_info_gid(to).addrs;
      auto listing_msg = make_message<MMDSQuiesceDbListing>();
      dout(10) << "sending listing " << db << " to the peer " << to << dendl;
      listing_msg->encode_payload_from(me, db);
      return send_message_mds(listing_msg, addrs);
    };
  }

  QuiesceDbManager::RequestContext* inject_request = nullptr;

  bool degraded = mdsmap->is_degraded();

  if (degraded && membership.is_leader()) {
    dout(5) << "WARNING: injecting a cancel all request"
      << " members: " << membership.members
      << " in: " << mdsmap->get_num_in_mds() 
      << " up: " << mdsmap->get_num_up_mds() 
      << " sr: " << mdsmap->get_num_standby_replay_mds()
      << dendl;
    
    struct CancelAll: public QuiesceDbManager::RequestContext {
      mds_rank_t whoami;
      CancelAll(mds_rank_t whoami) : whoami(whoami) {
        request.cancel_roots();
      }
      void finish(int rc) override {
        dout(rc == 0 ? 15 : 3) << "injected cancel all completed with rc: " << rc << dendl;
      }
    };

    inject_request = new CancelAll(whoami);
  }

  if (!is_active()) {
    quiesce_db_manager->reset_agent_callback([whoami = whoami, degraded, is_sr = is_standby_replay()](QuiesceMap& quiesce_map) {
      for (auto it = quiesce_map.roots.begin(); it != quiesce_map.roots.end();) {
        switch (it->second.state) {
        case QS_QUIESCING:
          if (degraded) {
            it->second.state = QS_FAILED;
            dout(3) << "DEGRADED RESPONDER: reporting '" << it->first << "' as " << it->second.state << dendl;
            ++it;
          } else if (is_sr) {
            it->second.state = QS_QUIESCED;
            dout(15) << "STANDBY REPLAY RESPONDER: reporting '" << it->first << "' as " << it->second.state << dendl;
            ++it;
          } else {
            // just ack.
            dout(20) << "INACTIVE RESPONDER: reporting '" << it->first << "' as " << it->second.state << dendl;
            it = quiesce_map.roots.erase(it);
          }
          break;
        default:
          it = quiesce_map.roots.erase(it);
          break;
        }
      }
      return true;
    });

    if (quiesce_agent) {
      // reset the agent if it's present
      // because it won't receive any more callbacks
      quiesce_agent->reset_async();
    }
  } else {
    rebind_agent_callback(quiesce_agent, quiesce_db_manager);
  }

  quiesce_db_manager->update_membership(membership, inject_request);
}

bool MDSRank::quiesce_dispatch(const cref_t<Message> &m) {
  try {
    switch(m->get_type()) {
      case MSG_MDS_QUIESCE_DB_LISTING:
      {
        const auto& req = ref_cast<MMDSQuiesceDbListing>(m);
        mds_gid_t gid;
        QuiesceDbListing db_listing;
        req->decode_payload_into(gid, db_listing);
        if (quiesce_db_manager) {
          dout(10) << "got " << db_listing << " from peer " << gid << dendl;
          int result = quiesce_db_manager->submit_listing_from(gid, std::move(db_listing));
          if (result != 0) {
            dout(3) << "error (" << result << ") submitting " << db_listing << " from peer " << gid << dendl;
          }
        } else {
          dout(5) << "no db manager to process " << db_listing << dendl;
        }
        return true;
      }
      case MSG_MDS_QUIESCE_DB_ACK:
      {
        const auto& req = ref_cast<MMDSQuiesceDbAck>(m);
        mds_gid_t gid;
        QuiesceMap diff_map;
        req->decode_payload_into(gid, diff_map);
        if (quiesce_db_manager) {
          dout(10) << "got ack " << diff_map << " from peer " << gid << dendl;
          int result = quiesce_db_manager->submit_ack_from(gid, std::move(diff_map));
          if (result != 0) {
            dout(3) << "error (" << result << ") submitting an ack from peer " << gid << dendl;
          }
        } else {
          dout(5) << "no db manager to process an ack: " << diff_map << dendl;
        }
        return true;
      }
      default: break;
    }
  }
  catch (const ceph::buffer::error &e) {
    if (cct) {
      dout(-1) << "failed to decode message of type " << m->get_type()
                 << " v" << m->get_header().version
                 << ": " << e.what() << dendl;
      dout(10) << "dump: \n";
      m->get_payload().hexdump(*_dout);
      *_dout << dendl;
      if (cct->_conf->ms_die_on_bad_msg) {
        ceph_abort();
      }
    }
  }
  return false;
}

void MDSRank::quiesce_agent_setup() {
  // TODO: replace this with a non-debug implementation
  //       Potentially, allow the debug interface under some runtime configuration

  ceph_assert(quiesce_db_manager);

  using RequestHandle = QuiesceInterface::RequestHandle;
  using QuiescingRoot = std::pair<RequestHandle, Context*>;
  auto dummy_requests = std::make_shared<std::unordered_map<QuiesceRoot, QuiescingRoot>>();

  QuiesceAgent::ControlInterface ci;

  ci.submit_request = [this, dummy_requests](QuiesceRoot root, Context* c)
      -> std::optional<RequestHandle> {
    auto uri = boost::urls::parse_uri_reference(root);
    if (!uri) {
      dout(5) << "error parsing the quiesce root as an URI: " << uri.error() << dendl;
      c->complete(uri.error());
      return std::nullopt;
    }

    dout(10) << "submit_request: " << uri << dendl;

    std::chrono::milliseconds quiesce_delay_ms = 0ms;
    if (auto pit = uri->params().find("delayms"); pit != uri->params().end()) {
      try {
        quiesce_delay_ms = std::chrono::milliseconds((*pit).has_value ? std::stoul((*pit).value) : 1000);
      } catch (...) {
        dout(5) << "error parsing the time to quiesce for query: " << uri->query() << dendl;
        c->complete(-EINVAL);
        return std::nullopt;
      }
    }
    std::optional<double> debug_quiesce_after;
    if (auto pit = uri->params().find("q"); pit != uri->params().end()) {
      try {
        debug_quiesce_after = (*pit).has_value ? std::stod((*pit).value) : 1 /*second*/;
      } catch (...) {
        dout(5) << "error parsing the time for debug quiesce for query: " << uri->query() << dendl;
        c->complete(-EINVAL);
        return std::nullopt;
      }
    }
    std::optional<double> debug_fail_after;
    if (auto pit = uri->params().find("f"); pit != uri->params().end()) {
      try {
        debug_fail_after = (*pit).has_value ? std::stod((*pit).value) : 1 /*second*/;
      } catch (...) {
        dout(5) << "error parsing the time for debug fail for query: " << uri->query() << dendl;
        c->complete(-EINVAL);
        return std::nullopt;
      }
    }
    std::optional<mds_rank_t> debug_rank;
    if (auto pit = uri->params().find("r"); pit != uri->params().end()) {
      try {
        if ((*pit).has_value) {
          debug_rank = (mds_rank_t)std::stoul((*pit).value);
        }
      } catch (...) {
        dout(5) << "error parsing the rank for debug pin for query: " << uri->query() << dendl;
        c->complete(-EINVAL);
        return std::nullopt;
      }
    }

    if (debug_rank && (debug_rank >= mdsmap->get_max_mds())) {
        dout(5) << "invalid rank: " << uri->query() << dendl;
        c->complete(-EINVAL);
        return std::nullopt;
    }

    auto path = uri->path();

    std::lock_guard l(mds_lock);

    if (!debug_quiesce_after && !debug_fail_after && !debug_rank) {
      // the real deal!
      if (mdsmap->is_degraded()) {
        dout(3) << "DEGRADED: refusing to quiesce" << dendl;
        c->complete(EPERM);
        return std::nullopt;
      }
      auto qc = new MDCache::C_MDS_QuiescePath(mdcache, c);
      auto mdr = mdcache->quiesce_path(filepath(path), qc, nullptr, quiesce_delay_ms);
      return mdr ? mdr->reqid : std::optional<RequestHandle>();
    } else {
      /* we use this branch to allow for quiesce emulation for testing purposes */
      // always create a new request id
      auto req_id = metareqid_t(entity_name_t::MDS(whoami), issue_tid());
      auto [it, inserted] = dummy_requests->try_emplace(path, req_id, c);

      if (!inserted) {
        dout(3) << "duplicate quiesce request for root '" << it->first << "'" << dendl;
        // we must update the request id so that old one can't cancel this request.
        it->second.first = req_id;
        if (it->second.second) {
          it->second.second->complete(-EINTR);
          it->second.second = c;
        } else {
          // if we have no context, it means we've completed it
          // since we weren't inserted, we must have successfully quiesced
          c->complete(0);
        }
      } else if (debug_rank && (debug_rank != whoami)) {
        // the root was pinned to a different rank
        // we should acknowledge the quiesce regardless of the other flags
        it->second.second->complete(0);
        it->second.second = nullptr;
      } else {
        // do quiesce or fail

        bool do_fail = false;
        double delay;
        if (debug_quiesce_after.has_value() && debug_fail_after.has_value()) {
          do_fail = debug_fail_after < debug_quiesce_after;
        } else {
          do_fail = debug_fail_after.has_value();
        }

        if (do_fail) {
          delay = debug_fail_after.value();
        } else {
          delay = debug_quiesce_after.value();
        }

        auto quiesce_task = new LambdaContext([dummy_requests, req_id, do_fail, this](int) {
          // the mds lock should be held by the timer
          ceph_assert(ceph_mutex_is_locked_by_me(mds_lock));
          dout(20) << "quiesce_task: callback by the timer" << dendl;
          auto it = std::ranges::find(*dummy_requests, req_id, [](auto x) { return x.second.first; });
          if (it != dummy_requests->end() && it->second.second != nullptr) {
            dout(20) << "quiesce_task: completing the root '" << it->first << "' as failed: " << do_fail << dendl;
            it->second.second->complete(do_fail ? -EBADF : 0);
            it->second.second = nullptr;
          }
          dout(20) << "quiesce_task: done" << dendl;
        });

        dout(20) << "scheduling a quiesce_task (" << quiesce_task
                 << ") to fire after " << delay
                 << " seconds on timer " << &timer << dendl;
        timer.add_event_after(delay, quiesce_task);
      }
      return it->second.first;
    }
  };

  ci.cancel_request = [this, dummy_requests](RequestHandle h) {
    std::lock_guard l(mds_lock);

    if (mdcache->have_request(h)) {
      auto qimdr = mdcache->request_get(h);
      mdcache->request_kill(qimdr);
      // no reason to waste time checking for dummy requests
      return 0;
    }

    // if we get here then it could be a test (dummy) quiesce
    auto it = std::ranges::find(*dummy_requests, h, [](auto x) { return x.second.first; });
    if (it != dummy_requests->end()) {
      if (auto ctx = it->second.second; ctx) {
        dout(20) << "canceling request with id '" << h << "' for root '" << it->first << "'" << dendl;
        ctx->complete(-ECANCELED);
      }
      dummy_requests->erase(it);
      return 0;
    }

    // we must indicate that the handle wasn't found
    // so that the agent can properly report a missing
    // outstanding quiesce, preventing a RELEASED transition 
    return ENOENT;
  };

  std::weak_ptr<QuiesceDbManager> weak_db_manager = quiesce_db_manager;
  ci.agent_ack = [weak_db_manager](QuiesceMap && update) {
    if (auto manager = weak_db_manager.lock()) {
      return manager->submit_agent_ack( std::move(update));
    } else {
      return ENOENT;
    }
  };

  quiesce_agent.reset(new QuiesceAgent(ci));
  rebind_agent_callback(quiesce_agent, quiesce_db_manager);
};
