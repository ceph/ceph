// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/future.hh>
#include <seastar/core/do_with.hh>

#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "replicated_recovery_backend.h"

#include "msg/Message.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

seastar::future<> ReplicatedRecoveryBackend::recover_object(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  auto& recovery_waiter = recovering[soid];
  return seastar::do_with(std::map<pg_shard_t, PushOp>(), get_shards_to_push(soid),
    [this, soid, need, &recovery_waiter](auto& pops, auto& shards) {
    return [this, soid, need, &recovery_waiter] {
      pg_missing_tracker_t local_missing = pg.get_local_missing();
      if (local_missing.is_missing(soid)) {
	PullOp po;
	auto& pi = recovery_waiter.pi;
	prepare_pull(po, pi, soid, need);
	auto msg = make_message<MOSDPGPull>();
	msg->from = pg.get_pg_whoami();
	msg->set_priority(pg.get_recovery_op_priority());
	msg->pgid = pg.get_pgid();
	msg->map_epoch = pg.get_osdmap_epoch();
	msg->min_epoch = pg.get_last_peering_reset();
	std::vector<PullOp> pulls;
	pulls.push_back(po);
	msg->set_pulls(&pulls);
	return shard_services.send_to_osd(pi.from.osd,
					   std::move(msg),
					   pg.get_osdmap_epoch()).then(
	  [&recovery_waiter] {
	  return recovery_waiter.wait_for_pull().then([] {
	    return seastar::make_ready_future<bool>(true);
	  });
	});
      } else {
	return seastar::make_ready_future<bool>(false);
      }
    }().then([this, &pops, &shards, soid, need, &recovery_waiter](bool pulled) mutable {
      return [this, &recovery_waiter, soid, pulled] {
	if (!recovery_waiter.obc) {
	  return pg.get_or_load_head_obc(soid).safe_then(
	    [&recovery_waiter, pulled](auto p) {
	    auto& [obc, existed] = p;
	    logger().debug("recover_object: loaded obc: {}", obc->obs.oi.soid);
	    recovery_waiter.obc = obc;
	    if (!existed) {
	      // obc is loaded with excl lock
	      recovery_waiter.obc->put_lock_type(RWState::RWEXCL);
	    }
	    bool got = recovery_waiter.obc->get_recovery_read().get0();
	    assert(pulled ? got : 1);
	    if (!got) {
	      return recovery_waiter.obc->get_recovery_read(true)
	      .then([](bool) { return seastar::now(); });
	    }
	    return seastar::make_ready_future<>();
	  }, crimson::osd::PG::load_obc_ertr::all_same_way(
	      [this, &recovery_waiter, soid](const std::error_code& e) {
	      auto [obc, existed] =
		  shard_services.obc_registry.get_cached_obc(soid);
	      logger().debug("recover_object: load failure of obc: {}",
		  obc->obs.oi.soid);
	      recovery_waiter.obc = obc;
	      // obc is loaded with excl lock
	      recovery_waiter.obc->put_lock_type(RWState::RWEXCL);
	      assert(recovery_waiter.obc->get_recovery_read().get0());
	      return seastar::make_ready_future<>();
	    })
	  );
	}
	return seastar::now();
      }().then([this, soid, need, &pops, &shards] {
	return prep_push(soid, need, &pops, shards);
      });
    }).handle_exception([this, soid](auto e) {
      auto& recovery_waiter = recovering[soid];
      if (recovery_waiter.obc)
	recovery_waiter.obc->drop_recovery_read();
      recovering.erase(soid);
      return seastar::make_exception_future<>(e);
    }).then([this, &pops, &shards, soid] {
      return seastar::parallel_for_each(shards,
	[this, &pops, soid](auto shard) {
	auto msg = make_message<MOSDPGPush>();
	msg->from = pg.get_pg_whoami();
	msg->pgid = pg.get_pgid();
	msg->map_epoch = pg.get_osdmap_epoch();
	msg->min_epoch = pg.get_last_peering_reset();
	msg->set_priority(pg.get_recovery_op_priority());
	msg->pushes.push_back(pops[shard->first]);
	return shard_services.send_to_osd(shard->first.osd, std::move(msg),
					  pg.get_osdmap_epoch()).then(
	  [this, soid, shard] {
	  return recovering[soid].wait_for_pushes(shard->first);
	});
      });
    }).then([this, soid] {
      bool error = recovering[soid].pi.recovery_progress.error;
      if (!error) {
	auto push_info = recovering[soid].pushing.begin();
	object_stat_sum_t stat = {};
	if (push_info != recovering[soid].pushing.end()) {
	  stat = push_info->second.stat;
	} else {
	  // no push happened, take pull_info's stat
	  stat = recovering[soid].pi.stat;
	}
	pg.get_recovery_handler()->on_global_recover(soid, stat, false);
	return seastar::make_ready_future<>();
      } else {
	auto& recovery_waiter = recovering[soid];
	if (recovery_waiter.obc)
	  recovery_waiter.obc->drop_recovery_read();
	recovering.erase(soid);
	return seastar::make_exception_future<>(
	    std::runtime_error(fmt::format("Errors during pushing for {}", soid)));
      }
    });
  });
}

seastar::future<> ReplicatedRecoveryBackend::push_delete(
  const hobject_t& soid,
  eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  recovering[soid];
  epoch_t min_epoch = pg.get_last_peering_reset();

  assert(pg.get_acting_recovery_backfill().size() > 0);
  return seastar::parallel_for_each(pg.get_acting_recovery_backfill(),
    [this, soid, need, min_epoch](pg_shard_t shard) {
    if (shard == pg.get_pg_whoami())
      return seastar::make_ready_future<>();
    auto iter = pg.get_shard_missing().find(shard);
    if (iter == pg.get_shard_missing().end())
      return seastar::make_ready_future<>();
    if (iter->second.is_missing(soid)) {
      logger().debug("{} will remove {} from {}", __func__, soid, shard);
      pg.begin_peer_recover(shard, soid);
      spg_t target_pg = spg_t(pg.get_info().pgid.pgid, shard.shard);
      auto msg = make_message<MOSDPGRecoveryDelete>(
	  pg.get_pg_whoami(), target_pg, pg.get_osdmap_epoch(), min_epoch);

      msg->set_priority(pg.get_recovery_op_priority());
      msg->objects.push_back(std::make_pair(soid, need));
      return shard_services.send_to_osd(shard.osd, std::move(msg),
					pg.get_osdmap_epoch()).then(
	[this, soid, shard] {
	return recovering[soid].wait_for_pushes(shard);
      });
    }
    return seastar::make_ready_future<>();
  });
}

seastar::future<> ReplicatedRecoveryBackend::handle_recovery_delete(
  Ref<MOSDPGRecoveryDelete> m)
{
  logger().debug("{}: {}", __func__, *m);

  auto& p = m->objects.front(); //TODO: only one delete per message for now.
  return local_recover_delete(p.first, p.second, pg.get_osdmap_epoch()).then(
    [this, m] {
    auto reply = make_message<MOSDPGRecoveryDeleteReply>();
    reply->from = pg.get_pg_whoami();
    reply->set_priority(m->get_priority());
    reply->pgid = spg_t(pg.get_info().pgid.pgid, m->from.shard);
    reply->map_epoch = m->map_epoch;
    reply->min_epoch = m->min_epoch;
    reply->objects = m->objects;
    return shard_services.send_to_osd(m->from.osd, std::move(reply), pg.get_osdmap_epoch());
  });
}

seastar::future<> ReplicatedRecoveryBackend::on_local_recover_persist(
  const hobject_t& soid,
  const ObjectRecoveryInfo& _recovery_info,
  bool is_delete,
  epoch_t epoch_frozen)
{
  logger().debug("{}", __func__);
  ceph::os::Transaction t;
  pg.get_recovery_handler()->on_local_recover(soid, _recovery_info, is_delete, t);
  return shard_services.get_store().do_transaction(coll, std::move(t)).then(
    [this, epoch_frozen, last_complete = pg.get_info().last_complete] {
    pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
    return seastar::make_ready_future<>();
  });
}

seastar::future<> ReplicatedRecoveryBackend::local_recover_delete(
  const hobject_t& soid,
  eversion_t need,
  epoch_t epoch_to_freeze)
{
  logger().debug("{}: {}, {}", __func__, soid, need);
  return backend->load_metadata(soid).safe_then([this]
    (auto lomt) {
    if (lomt->os.exists) {
      return seastar::do_with(ceph::os::Transaction(),
	[this, lomt = std::move(lomt)](auto& txn) {
	return backend->remove(lomt->os, txn).then([this, &txn]() mutable {
	  return shard_services.get_store().do_transaction(coll,
							   std::move(txn));
	});
      });
    }
    return seastar::make_ready_future<>();
  }).safe_then([this, soid, epoch_to_freeze, need] {
    auto& recovery_waiter = recovering[soid];
    auto& pi = recovery_waiter.pi;
    pi.recovery_info.soid = soid;
    pi.recovery_info.version = need;
    return on_local_recover_persist(soid, pi.recovery_info,
	                            true, epoch_to_freeze);
  }, PGBackend::load_metadata_ertr::all_same_way(
      [this, soid, epoch_to_freeze, need] (auto e) {
      auto& recovery_waiter = recovering[soid];
      auto& pi = recovery_waiter.pi;
      pi.recovery_info.soid = soid;
      pi.recovery_info.version = need;
      return on_local_recover_persist(soid, pi.recovery_info,
				      true, epoch_to_freeze);
    })
  );
}

seastar::future<> ReplicatedRecoveryBackend::recover_delete(
  const hobject_t &soid, eversion_t need)
{
  logger().debug("{}: {}, {}", __func__, soid, need);

  epoch_t cur_epoch = pg.get_osdmap_epoch();
  return seastar::do_with(object_stat_sum_t(),
    [this, soid, need, cur_epoch](auto& stat_diff) {
    return local_recover_delete(soid, need, cur_epoch).then(
      [this, &stat_diff, cur_epoch, soid, need] {
      if (!pg.has_reset_since(cur_epoch)) {
	bool object_missing = false;
	for (const auto& shard : pg.get_acting_recovery_backfill()) {
	  if (shard == pg.get_pg_whoami())
	    continue;
	  if (pg.get_shard_missing(shard)->is_missing(soid)) {
	    logger().debug("{}: soid {} needs to deleted from replca {}",
			   __func__,
			   soid,
			   shard);
	    object_missing = true;
	    break;
	  }
	}

	if (!object_missing) {
	  stat_diff.num_objects_recovered = 1;
	  return seastar::make_ready_future<>();
	} else {
	  return push_delete(soid, need);
	}
      }
      return seastar::make_ready_future<>();
    }).then([this, soid, &stat_diff] {
      pg.get_recovery_handler()->on_global_recover(soid, stat_diff, true);
      return seastar::make_ready_future<>();
    });
  });
}

seastar::future<> ReplicatedRecoveryBackend::prep_push(
  const hobject_t& soid,
  eversion_t need,
  std::map<pg_shard_t, PushOp>* pops,
  const std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>& shards)
{
  logger().debug("{}: {}, {}", __func__, soid, need);

  return seastar::do_with(std::map<pg_shard_t, interval_set<uint64_t>>(),
    [this, soid, pops, &shards](auto& data_subsets) {
    return seastar::parallel_for_each(shards,
      [this, soid, pops, &data_subsets](auto pg_shard) mutable {
      pops->emplace(pg_shard->first, PushOp());
      auto& recovery_waiter = recovering[soid];
      auto& obc = recovery_waiter.obc;
      auto& data_subset = data_subsets[pg_shard->first];

      if (obc->obs.oi.size) {
	data_subset.insert(0, obc->obs.oi.size);
      }
      const auto& missing = pg.get_shard_missing().find(pg_shard->first)->second;
      if (HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS)) {
	const auto it = missing.get_items().find(soid);
	assert(it != missing.get_items().end());
	data_subset.intersection_of(it->second.clean_regions.get_dirty_regions());
	logger().debug("calc_head_subsets {} data_subset {}", soid, data_subset);
      }

      logger().debug("prep_push: {} to {}", soid, pg_shard->first);
      auto& pi = recovery_waiter.pushing[pg_shard->first];
      pg.begin_peer_recover(pg_shard->first, soid);
      const auto pmissing_iter = pg.get_shard_missing().find(pg_shard->first);
      const auto missing_iter = pmissing_iter->second.get_items().find(soid);
      assert(missing_iter != pmissing_iter->second.get_items().end());

      pi.obc = obc;
      pi.recovery_info.size = obc->obs.oi.size;
      pi.recovery_info.copy_subset = data_subset;
      pi.recovery_info.soid = soid;
      pi.recovery_info.oi = obc->obs.oi;
      pi.recovery_info.version = obc->obs.oi.version;
      pi.recovery_info.object_exist =
	missing_iter->second.clean_regions.object_is_exist();
      pi.recovery_progress.omap_complete =
	!missing_iter->second.clean_regions.omap_is_dirty() &&
	HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS);

      return build_push_op(pi.recovery_info, pi.recovery_progress,
			   &pi.stat, &(*pops)[pg_shard->first]).then(
	[this, soid, pg_shard](auto new_progress) {
	auto& recovery_waiter = recovering[soid];
	auto& pi = recovery_waiter.pushing[pg_shard->first];
	pi.recovery_progress = new_progress;
	return seastar::make_ready_future<>();
      });
    });
  });
}

void ReplicatedRecoveryBackend::prepare_pull(PullOp& po, PullInfo& pi,
  const hobject_t& soid,
  eversion_t need) {
  logger().debug("{}: {}, {}", __func__, soid, need);

  pg_missing_tracker_t local_missing = pg.get_local_missing();
  const auto missing_iter = local_missing.get_items().find(soid);
  auto m = pg.get_missing_loc_shards();
  pg_shard_t fromshard = *(m[soid].begin());

  //TODO: skipped snap objects case for now
  po.recovery_info.copy_subset.insert(0, (uint64_t) -1);
  if (HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS))
    po.recovery_info.copy_subset.intersection_of(
	missing_iter->second.clean_regions.get_dirty_regions());
  po.recovery_info.size = ((uint64_t) -1);
  po.recovery_info.object_exist =
    missing_iter->second.clean_regions.object_is_exist();
  po.recovery_info.soid = soid;
  po.soid = soid;
  po.recovery_progress.data_complete = false;
  po.recovery_progress.omap_complete =
    !missing_iter->second.clean_regions.omap_is_dirty() &&
    HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS);
  po.recovery_progress.data_recovered_to = 0;
  po.recovery_progress.first = true;

  pi.from = fromshard;
  pi.soid = soid;
  pi.recovery_info = po.recovery_info;
  pi.recovery_progress = po.recovery_progress;
}

seastar::future<ObjectRecoveryProgress> ReplicatedRecoveryBackend::build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat,
    PushOp* pop
  ) {
  logger().debug("{}", __func__);
  return seastar::do_with(ObjectRecoveryProgress(progress),
			  object_info_t(),
			  uint64_t(crimson::common::local_conf()
			    ->osd_recovery_max_chunk),
			  eversion_t(),
    [this, &recovery_info, &progress, stat, pop]
    (auto& new_progress, auto& oi, auto& available, auto& v) {
    return [this, &recovery_info, &progress, &new_progress, &oi, pop, &v] {
      if (progress.first) {
	v = recovery_info.version;
	return backend->omap_get_header(coll, ghobject_t(recovery_info.soid))
	  .then([this, &recovery_info, pop](auto bl) {
	  pop->omap_header.claim_append(bl);
	  return store->get_attrs(coll, ghobject_t(recovery_info.soid));
	}).safe_then([&oi, pop, &new_progress, &v](auto attrs) mutable {
	  //pop->attrset = attrs;
	  for (auto p : attrs) {
	    pop->attrset[p.first].push_back(p.second);
	  }
	  logger().debug("build_push_op: {}", pop->attrset[OI_ATTR]);
	  oi.decode(pop->attrset[OI_ATTR]);
	  new_progress.first = false;
	  if (v == eversion_t()) {
	    v = oi.version;
	  }
	  return seastar::make_ready_future<>();
	}, crimson::os::FuturizedStore::get_attrs_ertr::all_same_way(
	    [] (const std::error_code& e) {
	    return seastar::make_exception_future<>(e);
	  })
	);
      }
      return seastar::make_ready_future<>();
    }().then([this, &recovery_info] {
      return shard_services.get_store().get_omap_iterator(coll,
		ghobject_t(recovery_info.soid));
    }).then([&progress, &available, &new_progress, pop](auto iter) {
      if (!progress.omap_complete) {
	return iter->lower_bound(progress.omap_recovered_to).then(
	  [iter, &new_progress, pop, &available](int ret) {
	  return seastar::repeat([iter, &new_progress, pop, &available] {
	    if (!iter->valid()) {
	      new_progress.omap_complete = true;
	      return seastar::make_ready_future<seastar::stop_iteration>(
			seastar::stop_iteration::yes);
	    }
	    if (!pop->omap_entries.empty()
		&& ((crimson::common::local_conf()->osd_recovery_max_omap_entries_per_chunk > 0
		    && pop->omap_entries.size()
		    >= crimson::common::local_conf()->osd_recovery_max_omap_entries_per_chunk)
		  || available <= iter->key().size() + iter->value().length())) {
	      new_progress.omap_recovered_to = iter->key();
	      return seastar::make_ready_future<seastar::stop_iteration>(
			seastar::stop_iteration::yes);
	    }
	    pop->omap_entries.insert(make_pair(iter->key(), iter->value()));
	    if ((iter->key().size() + iter->value().length()) <= available)
	      available -= (iter->key().size() + iter->value().length());
	    else
	      available = 0;
	    return iter->next().then([](int r) {
	      return seastar::stop_iteration::no;
	    });
	  });
	});
      }
      return seastar::make_ready_future<>();
    }).then([this, &recovery_info, &progress, &available, &new_progress, pop] {
      logger().debug("build_push_op: available: {}, copy_subset: {}",
		     available, recovery_info.copy_subset);
      if (available > 0) {
	if (!recovery_info.copy_subset.empty()) {
	  return seastar::do_with(interval_set<uint64_t>(recovery_info.copy_subset),
	    [this, &recovery_info, &progress, &available, pop, &new_progress]
	    (auto& copy_subset) {
	    return backend->fiemap(coll, ghobject_t(recovery_info.soid),
			  0, copy_subset.range_end()).then(
	      [&copy_subset](auto m) {
	      interval_set<uint64_t> fiemap_included(std::move(m));
	      copy_subset.intersection_of(fiemap_included);
	      return seastar::make_ready_future<>();
	    }).then([&recovery_info, &progress,
	      &copy_subset, &available, pop, &new_progress] {
	      pop->data_included.span_of(copy_subset, progress.data_recovered_to,
					 available);
	      if (pop->data_included.empty()) // zero filled section, skip to end!
		new_progress.data_recovered_to =
		  recovery_info.copy_subset.range_end();
	      else
		new_progress.data_recovered_to = pop->data_included.range_end();
	      return seastar::make_ready_future<>();
	    }).handle_exception([&copy_subset](auto e) {
	      copy_subset.clear();
	      return seastar::make_ready_future<>();
	    });
	  });
	} else {
	  return seastar::now();
	}
      } else {
	pop->data_included.clear();
	return seastar::make_ready_future<>();
      }
    }).then([this, &oi, pop] {
      //TODO: there's no readv in cyan_store yet, use read temporarily.
      return store->readv(coll, ghobject_t{oi.soid}, pop->data_included, 0);
    }).safe_then([&recovery_info, &progress,
      &new_progress, stat, pop, &v]
      (auto bl) {
      pop->data.claim_append(bl);
      if (new_progress.is_complete(recovery_info)) {
	new_progress.data_complete = true;
	if (stat)
	  stat->num_objects_recovered++;
      } else if (progress.first && progress.omap_complete) {
      // If omap is not changed, we need recovery omap
      // when recovery cannot be completed once
	new_progress.omap_complete = false;
      }
      if (stat) {
	stat->num_keys_recovered += pop->omap_entries.size();
	stat->num_bytes_recovered += pop->data.length();
      }
      pop->version = v;
      pop->soid = recovery_info.soid;
      pop->recovery_info = recovery_info;
      pop->after_progress = new_progress;
      pop->before_progress = progress;
      logger().debug("build_push_op: pop version: {}, pop data length: {}",
		     pop->version, pop->data.length());
      return seastar::make_ready_future<ObjectRecoveryProgress>
		(std::move(new_progress));
    }, PGBackend::read_errorator::all_same_way([](auto e) {
	logger().debug("build_push_op: read exception");
	return seastar::make_exception_future<ObjectRecoveryProgress>(e);
      })
    );
  });
}

std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>
ReplicatedRecoveryBackend::get_shards_to_push(const hobject_t& soid)
{
  std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator> shards;
  assert(pg.get_acting_recovery_backfill().size() > 0);
  for (set<pg_shard_t>::iterator i =
      pg.get_acting_recovery_backfill().begin();
      i != pg.get_acting_recovery_backfill().end();
      ++i) {
    if (*i == pg.get_pg_whoami())
      continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      pg.get_shard_missing().find(peer);
    assert(j != pg.get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      shards.push_back(j);
    }
  }
  return shards;
}

seastar::future<> ReplicatedRecoveryBackend::handle_pull(Ref<MOSDPGPull> m)
{
  logger().debug("{}: {}", __func__, *m);
  vector<PullOp> pulls;
  m->take_pulls(&pulls);
  return seastar::do_with(std::move(pulls),
    [this, m, from = m->from](auto& pulls) {
    return seastar::parallel_for_each(pulls, [this, m, from](auto& pull_op) {
      const hobject_t& soid = pull_op.soid;
      return seastar::do_with(PushOp(),
	[this, &soid, &pull_op, from](auto& pop) {
	logger().debug("handle_pull: {}", soid);
	return backend->stat(coll, ghobject_t(soid)).then(
	  [this, &pull_op, &pop](auto st) {
	  ObjectRecoveryInfo &recovery_info = pull_op.recovery_info;
	  ObjectRecoveryProgress &progress = pull_op.recovery_progress;
	  if (progress.first && recovery_info.size == ((uint64_t) -1)) {
	    // Adjust size and copy_subset
	    recovery_info.size = st.st_size;
	    if (st.st_size) {
	      interval_set<uint64_t> object_range;
	      object_range.insert(0, st.st_size);
	      recovery_info.copy_subset.intersection_of(object_range);
	    } else {
	      recovery_info.copy_subset.clear();
	    }
	    assert(recovery_info.clone_subset.empty());
	  }
	  return build_push_op(recovery_info, progress, 0, &pop);
	}).handle_exception([soid, &pop](auto e) {
	  pop.recovery_info.version = eversion_t();
	  pop.version = eversion_t();
	  pop.soid = soid;
	  return seastar::make_ready_future<ObjectRecoveryProgress>();
	}).then([this, &pop, from](auto new_progress) {
	  auto msg = make_message<MOSDPGPush>();
	  msg->from = pg.get_pg_whoami();
	  msg->pgid = pg.get_pgid();
	  msg->map_epoch = pg.get_osdmap_epoch();
	  msg->min_epoch = pg.get_last_peering_reset();
	  msg->set_priority(pg.get_recovery_op_priority());
	  msg->pushes.push_back(pop);
	  return shard_services.send_to_osd(from.osd, std::move(msg),
					    pg.get_osdmap_epoch());
	});
      });
    });
  });
}

seastar::future<bool> ReplicatedRecoveryBackend::_handle_pull_response(
  pg_shard_t from,
  PushOp& pop,
  PullOp* response,
  ceph::os::Transaction* t)
{
  logger().debug("handle_pull_response {} {} data.size() is {} data_included: {}",
      pop.recovery_info, pop.after_progress, pop.data.length(), pop.data_included);

  const hobject_t &hoid = pop.soid;
  auto& recovery_waiter = recovering[hoid];
  auto& pi = recovery_waiter.pi;
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
	pop.recovery_info.copy_subset);
  }

  // If primary doesn't have object info and didn't know version
  if (pi.recovery_info.version == eversion_t())
    pi.recovery_info.version = pop.version;

  bool first = pi.recovery_progress.first;

  return [this, &pi, first, &recovery_waiter, &pop] {
    if (first) {
      return pg.get_or_load_head_obc(pi.recovery_info.soid).safe_then(
	[&pi, &recovery_waiter, &pop](auto p) {
	auto& [obc, existed] = p;
	pi.obc = obc;
	recovery_waiter.obc = obc;
	obc->obs.oi.decode(pop.attrset[OI_ATTR]);
	pi.recovery_info.oi = obc->obs.oi;
	return seastar::make_ready_future<>();
      }, crimson::osd::PG::load_obc_ertr::all_same_way(
	  [this, &pi](const std::error_code& e) {
	  auto [obc, existed] = shard_services.obc_registry.get_cached_obc(
				    pi.recovery_info.soid);
	  pi.obc = obc;
	  return seastar::make_ready_future<>();
	})
      );
    }
    return seastar::make_ready_future<>();
  }().then([this, first, &pi, &pop, t, response]() mutable {
    return seastar::do_with(interval_set<uint64_t>(),
			    bufferlist(),
			    interval_set<uint64_t>(),
			    [this, &pop, &pi, first, t, response]
			    (auto& data_zeros, auto& data,
			     auto& usable_intervals) {
      {
        ceph::bufferlist usable_data;
        trim_pushed_data(pi.recovery_info.copy_subset, pop.data_included, pop.data,
	    &usable_intervals, &usable_data);
        data = std::move(usable_data);
      }
      pi.recovery_progress = pop.after_progress;
      logger().debug("new recovery_info {}, new progress {}",
	  pi.recovery_info, pi.recovery_progress);
      uint64_t z_offset = pop.before_progress.data_recovered_to;
      uint64_t z_length = pop.after_progress.data_recovered_to
	  - pop.before_progress.data_recovered_to;
      if (z_length)
	data_zeros.insert(z_offset, z_length);
      bool complete = pi.is_complete();
      bool clear_omap = !pop.before_progress.omap_complete;
      return submit_push_data(pi.recovery_info, first, complete, clear_omap,
	  data_zeros, usable_intervals, data, pop.omap_header,
	  pop.attrset, pop.omap_entries, t).then(
	[this, response, &pi, &pop, &data, complete, t] {
	pi.stat.num_keys_recovered += pop.omap_entries.size();
	pi.stat.num_bytes_recovered += data.length();

	if (complete) {
	  pi.stat.num_objects_recovered++;
	  pg.get_recovery_handler()->on_local_recover(pop.soid, recovering[pop.soid].pi.recovery_info,
			      false, *t);
	  return seastar::make_ready_future<bool>(true);
	} else {
	  response->soid = pop.soid;
	  response->recovery_info = pi.recovery_info;
	  response->recovery_progress = pi.recovery_progress;
	  return seastar::make_ready_future<bool>(false);
	}
      });
    });
  });
}

seastar::future<> ReplicatedRecoveryBackend::handle_pull_response(
  Ref<MOSDPGPush> m)
{
  const PushOp& pop = m->pushes[0]; //TODO: only one push per message for now.
  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    pg.get_recovery_handler()->on_failed_recover({ m->from }, pop.soid,
	get_recovering(pop.soid).pi.recovery_info.version);
    return seastar::make_exception_future<>(
	std::runtime_error(fmt::format(
	    "Error on pushing side {} when pulling obj {}",
	    m->from, pop.soid)));
  }

  logger().debug("{}: {}", __func__, *m);
  return seastar::do_with(PullOp(), [this, m](auto& response) {
    return seastar::do_with(ceph::os::Transaction(), m.get(),
      [this, &response](auto& t, auto& m) {
      pg_shard_t from = m->from;
      PushOp& pop = m->pushes[0]; // only one push per message for now
      return _handle_pull_response(from, pop, &response, &t).then(
	[this, &t](bool complete) {
	epoch_t epoch_frozen = pg.get_osdmap_epoch();
	return shard_services.get_store().do_transaction(coll, std::move(t))
	  .then([this, epoch_frozen, complete,
	  last_complete = pg.get_info().last_complete] {
	  pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
	  return seastar::make_ready_future<bool>(complete);
	});
      });
    }).then([this, m, &response](bool complete) {
      if (complete) {
	auto& pop = m->pushes[0];
	recovering[pop.soid].set_pulled();
	return seastar::make_ready_future<>();
      } else {
	auto reply = make_message<MOSDPGPull>();
	reply->from = pg.get_pg_whoami();
	reply->set_priority(m->get_priority());
	reply->pgid = pg.get_info().pgid;
	reply->map_epoch = m->map_epoch;
	reply->min_epoch = m->min_epoch;
	vector<PullOp> vec = { std::move(response) };
	reply->set_pulls(&vec);
	return shard_services.send_to_osd(m->from.osd, std::move(reply), pg.get_osdmap_epoch());
      }
    });
  });
}

seastar::future<> ReplicatedRecoveryBackend::_handle_push(
  pg_shard_t from,
  const PushOp &pop,
  PushReplyOp *response,
  ceph::os::Transaction *t)
{
  logger().debug("{}", __func__);

  return seastar::do_with(interval_set<uint64_t>(),
			  bufferlist(),
    [this, &pop, t, response](auto& data_zeros, auto& data) {
    data = pop.data;
    bool first = pop.before_progress.first;
    bool complete = pop.after_progress.data_complete
      && pop.after_progress.omap_complete;
    bool clear_omap = !pop.before_progress.omap_complete;
    uint64_t z_offset = pop.before_progress.data_recovered_to;
    uint64_t z_length = pop.after_progress.data_recovered_to
      - pop.before_progress.data_recovered_to;
    if (z_length)
      data_zeros.insert(z_offset, z_length);
    response->soid = pop.recovery_info.soid;

    return submit_push_data(pop.recovery_info, first, complete, clear_omap,
	data_zeros, pop.data_included, data, pop.omap_header, pop.attrset,
	pop.omap_entries, t).then([this, complete, &pop, t] {
      if (complete) {
	pg.get_recovery_handler()->on_local_recover(pop.recovery_info.soid,
			    pop.recovery_info, false, *t);
      }
    });
  });
}

seastar::future<> ReplicatedRecoveryBackend::handle_push(
  Ref<MOSDPGPush> m)
{
  if (pg.is_primary()) {
    return handle_pull_response(m);
  }

  logger().debug("{}: {}", __func__, *m);
  return seastar::do_with(PushReplyOp(), [this, m](auto& response) {
    const PushOp& pop = m->pushes[0]; //TODO: only one push per message for now
    return seastar::do_with(ceph::os::Transaction(),
      [this, m, &pop, &response](auto& t) {
      return _handle_push(m->from, pop, &response, &t).then(
	[this, &t] {
	epoch_t epoch_frozen = pg.get_osdmap_epoch();
	return shard_services.get_store().do_transaction(coll, std::move(t)).then(
	  [this, epoch_frozen, last_complete = pg.get_info().last_complete] {
	  //TODO: this should be grouped with pg.on_local_recover somehow.
	  pg.get_recovery_handler()->_committed_pushed_object(epoch_frozen, last_complete);
	});
      });
    }).then([this, m, &response]() mutable {
      auto reply = make_message<MOSDPGPushReply>();
      reply->from = pg.get_pg_whoami();
      reply->set_priority(m->get_priority());
      reply->pgid = pg.get_info().pgid;
      reply->map_epoch = m->map_epoch;
      reply->min_epoch = m->min_epoch;
      std::vector<PushReplyOp> replies = { std::move(response) };
      reply->replies.swap(replies);
      return shard_services.send_to_osd(m->from.osd,
	  std::move(reply), pg.get_osdmap_epoch());
    });
  });
}

seastar::future<bool> ReplicatedRecoveryBackend::_handle_push_reply(
  pg_shard_t peer,
  const PushReplyOp &op,
  PushOp *reply)
{
  const hobject_t& soid = op.soid;
  logger().debug("{}, soid {}, from {}", __func__, soid, peer);
  auto recovering_iter = recovering.find(soid);
  if (recovering_iter == recovering.end()
      || !recovering_iter->second.pushing.count(peer)) {
    logger().debug("huh, i wasn't pushing {} to osd.{}", soid, peer);
    return seastar::make_ready_future<bool>(true);
  } else {
    auto& pi = recovering_iter->second.pushing[peer];
    return [this, &pi, &soid, reply, peer, recovering_iter] {
      bool error = pi.recovery_progress.error;
      if (!pi.recovery_progress.data_complete && !error) {
	return build_push_op(pi.recovery_info, pi.recovery_progress,
	    &pi.stat, reply).then([&pi] (auto new_progress) {
	  pi.recovery_progress = new_progress;
	  return seastar::make_ready_future<bool>(false);
	});
      }
      if (!error)
	pg.get_recovery_handler()->on_peer_recover(peer, soid, pi.recovery_info);
      recovering_iter->second.set_pushed(peer);
      return seastar::make_ready_future<bool>(true);
    }().handle_exception([recovering_iter, &pi, peer] (auto e) {
      pi.recovery_progress.error = true;
      recovering_iter->second.set_pushed(peer);
      return seastar::make_ready_future<bool>(true);
    });
  }
}

seastar::future<> ReplicatedRecoveryBackend::handle_push_reply(
  Ref<MOSDPGPushReply> m)
{
  logger().debug("{}: {}", __func__, *m);
  auto from = m->from;
  auto& push_reply = m->replies[0]; //TODO: only one reply per message

  return seastar::do_with(PushOp(), [this, &push_reply, from](auto& pop) {
    return _handle_push_reply(from, push_reply, &pop).then(
      [this, &pop, from](bool finished) {
      if (!finished) {
	auto msg = make_message<MOSDPGPush>();
	msg->from = pg.get_pg_whoami();
	msg->pgid = pg.get_pgid();
	msg->map_epoch = pg.get_osdmap_epoch();
	msg->min_epoch = pg.get_last_peering_reset();
	msg->set_priority(pg.get_recovery_op_priority());
	msg->pushes.push_back(pop);
	return shard_services.send_to_osd(from.osd, std::move(msg), pg.get_osdmap_epoch());
      }
      return seastar::make_ready_future<>();
    });
  });
}

void ReplicatedRecoveryBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  ceph::bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  logger().debug("{}", __func__);
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset, intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
      p != intervals_received.end(); ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin(); q != x.end();
	++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

seastar::future<> ReplicatedRecoveryBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  interval_set<uint64_t> &data_zeros,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  const map<string, bufferlist> &attrs,
  const map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  logger().debug("{}", __func__);
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_temp_recovery_object(recovery_info.soid,
					  recovery_info.version);
    if (first) {
      logger().debug("{}: Adding oid {} in the temp collection",
	  __func__, target_oid);
      add_temp_obj(target_oid);
    }
  }

  return [this, &recovery_info, first, complete, t,
    &omap_header, &attrs, target_oid, clear_omap] {
    if (first) {
      if (!complete) {
	t->remove(coll->get_cid(), ghobject_t(target_oid));
	t->touch(coll->get_cid(), ghobject_t(target_oid));
	bufferlist bv = attrs.at(OI_ATTR);
	object_info_t oi(bv);
	t->set_alloc_hint(coll->get_cid(), ghobject_t(target_oid),
			  oi.expected_object_size,
			  oi.expected_write_size,
			  oi.alloc_hint_flags);
      } else {
        if (!recovery_info.object_exist) {
	  t->remove(coll->get_cid(), ghobject_t(target_oid));
          t->touch(coll->get_cid(), ghobject_t(target_oid));
          bufferlist bv = attrs.at(OI_ATTR);
          object_info_t oi(bv);
          t->set_alloc_hint(coll->get_cid(), ghobject_t(target_oid),
                            oi.expected_object_size,
                            oi.expected_write_size,
                            oi.alloc_hint_flags);
        }
        //remove xattr and update later if overwrite on original object
        t->rmattrs(coll->get_cid(), ghobject_t(target_oid));
        //if need update omap, clear the previous content first
        if (clear_omap)
          t->omap_clear(coll->get_cid(), ghobject_t(target_oid));
      }

      t->truncate(coll->get_cid(), ghobject_t(target_oid), recovery_info.size);
      if (omap_header.length())
	t->omap_setheader(coll->get_cid(), ghobject_t(target_oid), omap_header);

      return store->stat(coll, ghobject_t(recovery_info.soid)).then(
	[this, &recovery_info, complete, t, target_oid,
	omap_header = std::move(omap_header)] (auto st) {
	//TODO: pg num bytes counting
	if (!complete) {
	  //clone overlap content in local object
	  if (recovery_info.object_exist) {
	    uint64_t local_size = std::min(recovery_info.size, (uint64_t)st.st_size);
	    interval_set<uint64_t> local_intervals_included, local_intervals_excluded;
	    if (local_size) {
	      local_intervals_included.insert(0, local_size);
	      local_intervals_excluded.intersection_of(local_intervals_included, recovery_info.copy_subset);
	      local_intervals_included.subtract(local_intervals_excluded);
	    }
	    for (interval_set<uint64_t>::const_iterator q = local_intervals_included.begin();
		q != local_intervals_included.end();
		++q) {
	      logger().debug(" clone_range {} {}~{}",
		  recovery_info.soid, q.get_start(), q.get_len());
	      t->clone_range(coll->get_cid(), ghobject_t(recovery_info.soid), ghobject_t(target_oid),
		  q.get_start(), q.get_len(), q.get_start());
	    }
	  }
	}
	return seastar::make_ready_future<>();
      });
    }
    return seastar::make_ready_future<>();
  }().then([this, &data_zeros, &recovery_info, &intervals_included, t, target_oid,
    &omap_entries, &attrs, data_included, complete, first] {
    uint64_t off = 0;
    uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
    // Punch zeros for data, if fiemap indicates nothing but it is marked dirty
    if (data_zeros.size() > 0) {
      data_zeros.intersection_of(recovery_info.copy_subset);
      assert(intervals_included.subset_of(data_zeros));
      data_zeros.subtract(intervals_included);

      logger().debug("submit_push_data recovering object {} copy_subset: {} "
	  "intervals_included: {} data_zeros: {}",
	  recovery_info.soid, recovery_info.copy_subset,
	  intervals_included, data_zeros);

      for (auto p = data_zeros.begin(); p != data_zeros.end(); ++p)
	t->zero(coll->get_cid(), ghobject_t(target_oid), p.get_start(), p.get_len());
    }
    logger().debug("submit_push_data: test");
    for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
	p != intervals_included.end();
	++p) {
      bufferlist bit;
      bit.substr_of(data_included, off, p.get_len());
      logger().debug("submit_push_data: test1");
      t->write(coll->get_cid(), ghobject_t(target_oid),
	  p.get_start(), p.get_len(), bit, fadvise_flags);
      off += p.get_len();
    }

    if (!omap_entries.empty())
      t->omap_setkeys(coll->get_cid(), ghobject_t(target_oid), omap_entries);
    if (!attrs.empty())
      t->setattrs(coll->get_cid(), ghobject_t(target_oid), attrs);

    if (complete) {
      if (!first) {
	logger().debug("{}: Removing oid {} from the temp collection",
	    __func__, target_oid);
	clear_temp_obj(target_oid);
	t->remove(coll->get_cid(), ghobject_t(recovery_info.soid));
	t->collection_move_rename(coll->get_cid(), ghobject_t(target_oid),
				  coll->get_cid(), ghobject_t(recovery_info.soid));
      }
      submit_push_complete(recovery_info, t);
    }
    logger().debug("submit_push_data: done");
    return seastar::make_ready_future<>();
  });
}

void ReplicatedRecoveryBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t>>::const_iterator p =
      recovery_info.clone_subset.begin();
      p != recovery_info.clone_subset.end(); ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	q != p->second.end(); ++q) {
      logger().debug(" clone_range {} {}~{}", p->first, q.get_start(), q.get_len());
      t->clone_range(coll->get_cid(), ghobject_t(p->first), ghobject_t(recovery_info.soid),
	  q.get_start(), q.get_len(), q.get_start());
    }
  }
}

seastar::future<> ReplicatedRecoveryBackend::handle_recovery_delete_reply(
  Ref<MOSDPGRecoveryDeleteReply> m)
{
  auto& p = m->objects.front();
  hobject_t soid = p.first;
  ObjectRecoveryInfo recovery_info;
  recovery_info.version = p.second;
  pg.get_recovery_handler()->on_peer_recover(m->from, soid, recovery_info);
  get_recovering(soid).set_pushed(m->from);
  return seastar::now();
}

seastar::future<> ReplicatedRecoveryBackend::handle_recovery_op(Ref<MOSDFastDispatchOp> m)
{
  switch (m->get_header().type) {
  case MSG_OSD_PG_PULL:
    return handle_pull(boost::static_pointer_cast<MOSDPGPull>(m));
  case MSG_OSD_PG_PUSH:
    return handle_push(boost::static_pointer_cast<MOSDPGPush>(m));
  case MSG_OSD_PG_PUSH_REPLY:
    return handle_push_reply(
	boost::static_pointer_cast<MOSDPGPushReply>(m));
  case MSG_OSD_PG_RECOVERY_DELETE:
    return handle_recovery_delete(
	boost::static_pointer_cast<MOSDPGRecoveryDelete>(m));
  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    return handle_recovery_delete_reply(
	boost::static_pointer_cast<MOSDPGRecoveryDeleteReply>(m));
  default:
    return seastar::make_exception_future<>(
	std::invalid_argument(fmt::format("invalid request type: {}",
					  m->get_header().type)));
  }
}

