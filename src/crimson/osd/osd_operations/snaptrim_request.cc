// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

seastar::future<> SnapTrimRequest::start()
{
  logger().debug("{}", __func__);
  return seastar::now();

  logger().debug("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (auto delay = 0; delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref)] {
    logger().debug("{}: complete", *ref);
  });
}

CommonPGPipeline& SnapTrimRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<> SnapTrimRequest::with_pg(
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
      std::vector<hobject_t> to_trim;
      assert(!to_trim.empty());
      for (const auto& object : to_trim) {
        logger().debug("{}: trimming {}", object);
        // TODO: start subop and add to subop blcoker
      }
      return seastar::now();
    });
  }, [this](std::exception_ptr eptr) {
    // TODO: better debug output
    logger().debug("{}: interrupted {}", *this, eptr);
  }, pg);
}


CommonPGPipeline& SnapTrimObjSubRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<> SnapTrimObjSubRequest::start()
{
  logger().debug("{}", __func__);
  return seastar::now();

  logger().debug("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (auto delay = 0; delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref)] {
    logger().debug("{}: complete", *ref);
  });
}

std::pair<ceph::os::Transaction,
          std::vector<pg_log_entry_t>>
SnapTrimObjSubRequest::remove_or_update(ObjectContextRef obc, ObjectContextRef head_obc)
{
  snapid_t snap_to_trim;
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
  auto at_version = pg->next_version();
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
        at_version,
	coi.version,
	0,
	osd_reqid_t(),
	coi.mtime, // will be replaced in `apply_to()`
	0}
      );
    txn.remove(pg->get_collection_ref()->get_cid(),
	       ghobject_t{coid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD});

    // TODO: ensure the log entry has the correct snaps
    //txn.update_snaps(
    //  coid,
    //  old_snaps,
    //  new_snaps);

    coi = object_info_t(coid);

    at_version.version++;
  } else {
    // TODO
  }

  // save head snapset
  logger().debug("{}: {} new snapset {} on {}",
                 *this, coid, snapset, head_obc->obs.oi);
  if (snapset.clones.empty() && head_obc->obs.oi.is_whiteout()) {
    const auto head_oid = coid.get_head();
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
	at_version,
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
# if 0
    if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
      // filter SnapSet::snaps for the benefit of pre-octopus
      // peers. This is perhaps overly conservative in that I'm not
      // certain they need this, but let's be conservative here.
      dout(10) << coid << " filtering snapset on " << head_oid << dendl;
      snapset.filter(pool.info);
    } else {
      snapset.snaps.clear();
    }
    dout(10) << coid << " writing updated snapset on " << head_oid
	     << ", snapset is " << snapset << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	head_oid,
	ctx->at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );

    head_obc->obs.oi.prior_version = head_obc->obs.oi.version;
    head_obc->obs.oi.version = ctx->at_version;

    map <string, bufferlist, less<>> attrs;
    bl.clear();
    encode(snapset, bl);
    attrs[SS_ATTR] = std::move(bl);

    bl.clear();
    encode(head_obc->obs.oi, bl,
	     get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR] = std::move(bl);
    t->setattrs(head_oid, attrs);
#endif
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
      // end of commonality
      // with_cone_obc lock both clone's and head's obcs
      return pg->with_clone_obc<RWState::RWWRITE>(coid, [this](auto clone_obc) {
        return enter_stage<interruptor>(
          pp().process
        ).then_interruptible([this, clone_obc=std::move(clone_obc)]() mutable {
          auto head_obc = clone_obc->head;
          osd_op_params_t osd_op_p{};
          auto [txn, log_entries] = remove_or_update(clone_obc, head_obc);
          auto [submitted, all_completed] = pg->submit_transaction(
            std::move(clone_obc),
            std::move(txn),
            std::move(osd_op_p),
            std::move(log_entries));
          return submitted.then_interruptible([this] {
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
