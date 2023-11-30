#include "MDSRank.h"

#include "QuiesceDbManager.h"
#include "QuiesceAgent.h"
#include <boost/url.hpp>
#include <ranges>
#include <algorithm>

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
        f->dump_int("epoch", response.epoch);
        f->dump_int("db_version", response.db_version);
        f->open_object_section("sets"); {
          for (auto&& [set_id, set] : response.sets) {
            if (!all && !set.is_active() && set_id != request.set_id) {
              continue;
            }
            f->open_object_section(set_id); {
              f->dump_int("db_version", set.db_version);
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
      r.if_version = QuiesceDbVersion(ifv);
    }
  });

  dout(20) << "Submitting a quiesce db request for setid: " << set_id << ", operation: " << ctx->request.op_string() << dendl;
  int rc = quiesce_db_manager->submit_request(ctx);
  if (rc != 0) {
    bufferlist bl;
    delete ctx;
    on_finish(rc, "Error submitting the command to the local db manager", bl);
  }
}

void MDSRank::quiesce_cluster_update() {
  QuiesceClusterMembership membership;

  mds_rank_t leader = 0; // MAYBE LATER: initialize this from the map

  membership.epoch = mdsmap->get_epoch();
  membership.leader = leader;
  membership.me = whoami;
  membership.fs_id = mdsmap->get_info(whoami).join_fscid;
  membership.fs_name = mdsmap->get_fs_name();
  mdsmap->get_up_mds_set(membership.members);

  membership.send_ack = [=,this](const QuiesceMap& ack) {
    if (whoami == leader) {
      // loopback
      quiesce_db_manager->submit_ack_from(whoami, ack);
      return 0;
    } else {
      // TODO: implement messaging
      return -ENOTSUP;
    }
  };

  membership.send_listing_to = [=](mds_rank_t to, const QuiesceDbListing& db) {
    // TODO: implement messaging
    return -ENOTSUP;
  };

  quiesce_db_manager->update_membership(membership);
}

void MDSRank::quiesce_agent_setup() {
  // TODO: replace this with a non-debug implementation
  //       Potentially, allow the debug interface under some runtime configuration

  ceph_assert(quiesce_db_manager);

  using RequestHandle = QuiesceInterface::RequestHandle;
  using QuiescingRoot = std::pair<RequestHandle, Context*>;
  auto quiesce_requests = std::make_shared<std::unordered_map<QuiesceRoot, QuiescingRoot>>();

  QuiesceAgent::ControlInterface ci;

  ci.submit_request = [this, quiesce_requests](QuiesceRoot root, Context* c)
      -> std::optional<RequestHandle> {
    auto uri = boost::urls::parse_uri_reference(root);
    if (!uri) {
      dout(5) << "error parsing the quiesce root as an URI: " << uri.error() << dendl;
      c->complete(uri.error());
      return std::nullopt;
    } else {
      dout(20) << "parsed root '" << root <<"' as : " << uri->path() << " " << uri->query() << dendl;
    }

    std::lock_guard l(mds_lock);

    // always create a new request id
    auto req_id = metareqid_t(entity_name_t::MDS(whoami), issue_tid());

    auto [it, inserted] = quiesce_requests->try_emplace(uri->path(), req_id, c);

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
    } else {
      dout(20) << "got request to quiesce '" << it->first << "'" << dendl;
      // do quiesce if needed
      if (auto pit = uri->params().find("q"); pit != uri->params().end()) {
        double quiesce_after = 0;
        try {
          quiesce_after = (*pit).has_value ? std::stod((*pit).value) : 1 /*second*/;
        } catch (...) {
          dout(5) << "error parsing the time to quiesce for query: " << uri->query() << dendl;
          quiesce_requests->erase(it);
          c->complete(-EINVAL);
          return std::nullopt;
        }

        auto quiesce_task = new LambdaContext([quiesce_requests, req_id, this](int) {
          // the mds lock should be held by the timer
          dout(20) << "quiesce_task: callback by the timer" << dendl;
          auto it = std::ranges::find(*quiesce_requests, req_id, [](auto x) { return x.second.first; });
          if (it != quiesce_requests->end() && it->second.second != nullptr) {
            dout(20) << "quiesce_task: completing the root '" << it->first << "'" << dendl;
            it->second.second->complete(0);
            it->second.second = nullptr;
          }
          dout(20) << "quiesce_task: done" << dendl;
        });

        dout(20) << "scheduling a quiesce_task (" << quiesce_task 
          << ") to fire after " << quiesce_after 
          << " seconds on timer " << &timer << dendl;
        timer.add_event_after(quiesce_after, quiesce_task);
      }
    }

    return it->second.first;
  };

  ci.cancel_request = [this, quiesce_requests](RequestHandle h) {
    std::lock_guard l(mds_lock);

    auto it = std::ranges::find(*quiesce_requests, h, [](auto x) { return x.second.first; });
    if (it != quiesce_requests->end()) {
      if (auto ctx = it->second.second; ctx) {
        dout(20) << "canceling request with id '" << h << "' for root '" << it->first << "'" << dendl;
        ctx->complete(-ECANCELED);
      }
      quiesce_requests->erase(it);
      return 0;
    }

    return ENOENT;
  };

  std::weak_ptr<QuiesceDbManager> weak_db_manager = quiesce_db_manager;
  ci.agent_ack = [weak_db_manager, whoami = this->whoami](QuiesceMap const& update) {
    if (auto manager = weak_db_manager.lock()) {
      return manager->submit_ack_from(whoami, update);
    } else {
      return ENOENT;
    }
  };

  quiesce_agent.reset(new QuiesceAgent(ci));

  std::weak_ptr<QuiesceAgent> weak_agent = quiesce_agent;
  quiesce_db_manager->reset_agent_callback([weak_agent](QuiesceMap& update) {
    if (auto agent = weak_agent.lock()) {
      return agent->db_update(update);
    } else {
      return false;
    }
  });
};
