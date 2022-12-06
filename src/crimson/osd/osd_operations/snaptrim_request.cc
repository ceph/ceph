// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ranges>

#include "crimson/osd/osd_operations/snaptrim_request.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::SnapTrimRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };

  template <>
  struct EventBackendRegistry<osd::SnapTrimObjSubRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

namespace crimson::osd {


void SnapTrimRequest::SubOpBlocker::dump_detail(Formatter *f) const
{
  f->open_array_section("dependent_operations");
  {
    for (auto &i : subops | std::views::keys) {
      f->dump_unsigned("op_id", i);
    }
  }
  f->close_section();
}

template <class... Args>
void SnapTrimRequest::SubOpBlocker::emplace_back(Args&&... args)
{
  subops.emplace_back(std::forward<Args>(args)...);
};

seastar::future<> SnapTrimRequest::SubOpBlocker::wait_completion()
{
  auto rng = subops | std::views::values;
  return seastar::when_all_succeed(
    std::begin(rng), std::end(rng)
  ).then([] (auto&&...) {
    return seastar::now();
  });
}

void SnapTrimRequest::print(std::ostream &lhs) const
{
  lhs << "SnapTrimRequest("
      << "pgid=" << pg->get_pgid()
      << " snapid=" << snapid
      << ")";
}

void SnapTrimRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimRequest");
  f->dump_stream("pgid") << pg->get_pgid();
  f->close_section();
}

seastar::future<seastar::stop_iteration> SnapTrimRequest::start()
{
  logger().debug("{}: start", *this, __func__);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (auto delay = 0; delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref), this] {
    logger().debug("{}: complete", *ref);
    return handle.complete();
  });
}

CommonPGPipeline& SnapTrimRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<seastar::stop_iteration> SnapTrimRequest::with_pg(
  ShardServices &shard_services, Ref<PG> _pg)
{
  return interruptor::with_interruption([&shard_services, this] {
    return enter_stage<interruptor>(
      pp().wait_for_active
    ).then_interruptible([this] {
      return with_blocking_event<PGActivationBlocker::BlockingEvent,
                                 interruptor>([this] (auto&& trigger) {
        return pg->wait_for_active_blocker.wait(std::move(trigger));
      });
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().recover_missing);
    }).then_interruptible([this] {
      //return do_recover_missing(pg, get_target_oid());
      return seastar::now();
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().get_obc);
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().process);
    }).then_interruptible([&shard_services, this] {
      return interruptor::async([this] {
        std::vector<hobject_t> to_trim;
        using crimson::common::local_conf;
        const auto max =
          local_conf().get_val<uint64_t>("osd_pg_max_concurrent_snap_trims");
        // we need to look for at least 1 snaptrim, otherwise we'll misinterpret
        // the ENOENT below and erase snapid.
        int r = snap_mapper.get_next_objects_to_trim(
          snapid,
          max,
          &to_trim);
        if (r == -ENOENT) {
          return std::optional<decltype(to_trim)>{};
        } else if (r != 0) {
          logger().error("{}: get_next_objects_to_trim returned {}",
                         *this, cpp_strerror(r));
          ceph_abort_msg("get_next_objects_to_trim returned an invalid code");
        }
        return std::make_optional(std::move(to_trim));
      }).then([&shard_services, this] (const auto& to_trim) {
        if (!to_trim) {
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        assert(!to_trim->empty());
        for (const auto& object : *to_trim) {
          logger().debug("{}: trimming {}", object);
          auto [op, fut] = shard_services.start_operation<SnapTrimObjSubRequest>(
            pg,
            object,
            snapid);
          subop_blocker.emplace_back(op->get_id(), std::move(fut));
        }
        return subop_blocker.wait_completion().then([] {
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
        });
      });
    });
  }, [this](std::exception_ptr eptr) {
    // TODO: better debug output
    logger().debug("{}: interrupted {}", *this, eptr);
    return seastar::make_ready_future<seastar::stop_iteration>(
      seastar::stop_iteration::no);
  }, pg);
}


CommonPGPipeline& SnapTrimObjSubRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<> SnapTrimObjSubRequest::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (auto delay = 0; delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref), this] {
    logger().debug("{}: complete", *ref);
    return handle.complete();
  });
}

std::pair<ceph::os::Transaction,
          std::vector<pg_log_entry_t>>
SnapTrimObjSubRequest::remove_or_update(ObjectContextRef obc, ObjectContextRef head_obc)
{
  // FIXME above
  ceph::os::Transaction txn{};
  std::vector<pg_log_entry_t> log_entries{};

  SnapSet& snapset = obc->ssc->snapset;
  auto citer = snapset.clone_snaps.find(coid.snap);
  if (citer == snapset.clone_snaps.end()) {
    logger().error("{}: No clone_snaps in snapset {} for object {}",
                   *this, snapset, coid);
    //return -ENOENT;
  }
  std::set<snapid_t> old_snaps(citer->second.begin(), citer->second.end());

  object_info_t &coi = obc->obs.oi;

  std::set<snapid_t> new_snaps;
  const OSDMapRef& osdmap = pg->get_osdmap();
  for (std::set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!osdmap->in_removed_snaps_queue(pg->get_info().pgid.pgid.pool(), *i) &&
	*i != snap_to_trim) {
      new_snaps.insert(*i);
    }
  }
  std::vector<snapid_t>::iterator p = snapset.clones.end();

  if (new_snaps.empty()) {
    p = std::find(snapset.clones.begin(), snapset.clones.end(), coid.snap);
    if (p == snapset.clones.end()) {
      logger().error("{}: Snap {} not in clones",
                     *this, coid.snap);
      //return -ENOENT;
    }
  }
  int64_t num_objects_before_trim = delta_stats.num_objects;
  osd_op_p.at_version = pg->next_version();
  if (new_snaps.empty()) {
    // remove clone
    logger().info("{}: {} snaps {} -> {} ... deleting",
                  *this, coid, old_snaps, new_snaps);

    // ...from snapset
    assert(p != snapset.clones.end());

    snapid_t last = coid.snap;
    delta_stats.num_bytes -= snapset.get_clone_bytes(last);

    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      std::vector<snapid_t>::iterator n = p - 1;
      hobject_t prev_coid = coid;
      prev_coid.snap = *n;

      // does the classical OSD really need is_present_clone(prev_coid)?
      delta_stats.num_bytes -= snapset.get_clone_bytes(*n);
      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);
      delta_stats.num_bytes += snapset.get_clone_bytes(*n);
    }
    delta_stats.num_objects--;
    if (coi.is_dirty()) {
      delta_stats.num_objects_dirty--;
    }
    if (coi.is_omap()) {
      delta_stats.num_objects_omap--;
    }
    if (coi.is_whiteout()) {
      logger().debug("{}: trimming whiteout on {}",
                     *this, coid);
      delta_stats.num_whiteouts--;
    }
    delta_stats.num_object_clones--;

    obc->obs.exists = false;

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
    snapset.clone_snaps.erase(last);

    log_entries.emplace_back(
      pg_log_entry_t{
	pg_log_entry_t::DELETE,
	coid,
        osd_op_p.at_version,
	coi.version,
	0,
	osd_reqid_t(),
	coi.mtime, // will be replaced in `apply_to()`
	0}
      );
    txn.remove(
      pg->get_collection_ref()->get_cid(),
      ghobject_t{coid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD});

    coi = object_info_t(coid);

  } else {
    // save adjusted snaps for this object
    logger().info("{}: {} snaps {} -> {}",
                  *this, coid, old_snaps, new_snaps);
    snapset.clone_snaps[coid.snap] =
      std::vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());
    // we still do a 'modify' event on this object just to trigger a
    // snapmapper.update ... :(

    coi.prior_version = coi.version;
    coi.version = osd_op_p.at_version;
    ceph::bufferlist bl;
    encode(coi, bl, pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    txn.setattr(
      pg->get_collection_ref()->get_cid(),
      ghobject_t{coid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD},
      OI_ATTR,
      bl);
    log_entries.emplace_back(
      pg_log_entry_t{
	pg_log_entry_t::MODIFY,
	coid,
	coi.version,
	coi.prior_version,
	0,
	osd_reqid_t(),
	coi.mtime,
	0}
      );
    logger().info("{}: {} =====",
                  *this, __LINE__);
    logger().info("{}: {} =====",
                  *this, __LINE__);
  }

  //
  // TODO: ensure the log entry has the correct snaps
  //txn.update_snaps(
  //  coid,
  //  old_snaps,
  //  new_snaps);

  osd_op_p.at_version = pg->next_version();

  // save head snapset
  logger().debug("{}: {} new snapset {} on {}",
                 *this, coid, snapset, head_obc->obs.oi);
  const auto head_oid = coid.get_head();
  if (snapset.clones.empty() && head_obc->obs.oi.is_whiteout()) {
    // NOTE: this arguably constitutes minor interference with the
    // tiering agent if this is a cache tier since a snap trim event
    // is effectively evicting a whiteout we might otherwise want to
    // keep around.
    logger().info("{}: {} removing {}",
                  *this, coid, head_oid);
    log_entries.emplace_back(
      pg_log_entry_t{
	pg_log_entry_t::DELETE,
	head_oid,
	osd_op_p.at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	coi.mtime, // will be replaced in `apply_to()`
	0}
      );
    logger().info("{}: remove snap head", *this);
    object_info_t& oi = head_obc->obs.oi;
    delta_stats.num_objects--;
    if (oi.is_dirty()) {
      delta_stats.num_objects_dirty--;
    }
    if (oi.is_omap()) {
      delta_stats.num_objects_omap--;
    }
    if (oi.is_whiteout()) {
      logger().debug("{}: trimming whiteout on {}",
                     *this, oi.soid);
      delta_stats.num_whiteouts--;
    }
    head_obc->obs.exists = false;
    head_obc->obs.oi = object_info_t(head_oid);
    txn.remove(pg->get_collection_ref()->get_cid(),
	       ghobject_t{head_oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD});
  } else {
    snapset.snaps.clear();
    logger().info("{}: writing updated snapset on {}, snapset is {}",
                  *this, head_oid, snapset);
    log_entries.emplace_back(
      pg_log_entry_t{
	pg_log_entry_t::MODIFY,
	head_oid,
	osd_op_p.at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	coi.mtime,
	0}
      );

    head_obc->obs.oi.prior_version = head_obc->obs.oi.version;
    head_obc->obs.oi.version = osd_op_p.at_version;

    std::map<std::string, ceph::bufferlist, std::less<>> attrs;
    ceph::bufferlist bl;
    encode(snapset, bl);
    attrs[SS_ATTR] = std::move(bl);

    bl.clear();
    encode(head_obc->obs.oi, bl,
           pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR] = std::move(bl);
    txn.setattrs(
      pg->get_collection_ref()->get_cid(),
      ghobject_t{head_oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD},
      attrs);
  }

  // Stats reporting - Set number of objects trimmed
  if (num_objects_before_trim > delta_stats.num_objects) {
    int64_t num_objects_trimmed =
      num_objects_before_trim - delta_stats.num_objects;
    //add_objects_trimmed_count(num_objects_trimmed);
  }
  return std::make_pair(std::move(txn), std::move(log_entries));
}

seastar::future<> SnapTrimObjSubRequest::with_pg(
  ShardServices &shard_services, Ref<PG> _pg)
{
  return interruptor::with_interruption([this] {
    return enter_stage<interruptor>(
      pp().wait_for_active
    ).then_interruptible([this] {
      return with_blocking_event<PGActivationBlocker::BlockingEvent,
                                 interruptor>([this] (auto&& trigger) {
        return pg->wait_for_active_blocker.wait(std::move(trigger));
      });
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().recover_missing);
    }).then_interruptible([this] {
      //return do_recover_missing(pg, get_target_oid());
      return seastar::now();
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().get_obc);
    }).then_interruptible([this] {
      logger().debug("{}: getting obc for {}", *this, coid);
      // end of commonality
      // with_cone_obc lock both clone's and head's obcs
      return pg->obc_loader.with_clone_obc<RWState::RWWRITE>(coid, [this](auto clone_obc) {
        logger().debug("{}: got clone_obc={}", *this, clone_obc);
        return enter_stage<interruptor>(
          pp().process
        ).then_interruptible([this, clone_obc=std::move(clone_obc)]() mutable {
          logger().debug("{}: processing clone_obc={}", *this, clone_obc);
          auto head_obc = clone_obc->head;
          return interruptor::async([=, this]() mutable {
            auto [txn, log_entries] = remove_or_update(clone_obc, head_obc);
            auto [submitted, all_completed] = pg->submit_transaction(
              std::move(clone_obc),
              std::move(txn),
              std::move(osd_op_p),
              std::move(log_entries));
            submitted.get();
            all_completed.get();
          }).then_interruptible([this] {
            return enter_stage<interruptor>(
              wait_repop
            );
          }).then_interruptible([this, clone_obc=std::move(clone_obc)] {
            return PG::load_obc_iertr::now();
          });
        });
      }).handle_error_interruptible(PG::load_obc_ertr::all_same_way([] {
        return seastar::now();
      }));
    }).then_interruptible([] {
      // end of commonality
      return seastar::now();
    });
  }, [this](std::exception_ptr eptr) {
    // TODO: better debug output
    logger().debug("{}: interrupted {}", *this, eptr);
  }, pg);
}

void SnapTrimObjSubRequest::print(std::ostream &lhs) const
{
  lhs << "SnapTrimObjSubRequest("
      << "coid=" << coid
      << " snapid=" << snap_to_trim
      << ")";
}

void SnapTrimObjSubRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimObjSubRequest");
  f->dump_stream("coid") << coid;
  f->close_section();
}

} // namespace crimson::osd
