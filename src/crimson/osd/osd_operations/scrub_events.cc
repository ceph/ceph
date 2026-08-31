// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/common/log.h"
#include "crimson/common/coroutine.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_connection_priv.h"
#include "messages/MOSDRepScrubMap.h"
#include "scrub_events.h"
#include "crimson/os/futurized_store.h"
#include "osd/osd_types.h"
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

SET_SUBSYS(osd);

namespace crimson::osd {

template <class T>
PGPeeringPipeline &RemoteScrubEventBaseT<T>::get_peering_pipeline(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

template <class T>
ConnectionPipeline &RemoteScrubEventBaseT<T>::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).peering_request_conn_pipeline;
}

template <class T>
PerShardPipeline &RemoteScrubEventBaseT<T>::get_pershard_pipeline(
  ShardServices &shard_services)
{
  return shard_services.get_peering_request_pipeline();
}

template <class T>
seastar::future<> RemoteScrubEventBaseT<T>::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(RemoteEventBaseT::with_pg);
  return interruptor::with_interruption([FNAME, this, pg] {
    DEBUGDPP("{} pg present", *pg, *that());
    return this->template enter_stage<interruptor>(
      get_peering_pipeline(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
	PG_OSDMapGate::OSDMapBlocker::BlockingEvent
	>([this, pg](auto &&trigger) {
	  return pg->osdmap_gate.wait_for_map(
	    std::move(trigger), get_epoch());
	});
    }).then_interruptible([this, pg](auto) {
      return this->template enter_stage<interruptor>(
	get_peering_pipeline(*pg).process);
    }).then_interruptible([this, pg] {
      return handle_event(*pg);
    });
  }, [FNAME, pg, this](std::exception_ptr ep) {
    DEBUGDPP("{} interrupted with {}", *pg, *that(), ep);
  }, pg, epoch);
}

ScrubRequested::ifut<> ScrubRequested::handle_event(PG &pg)
{
  // Operator-requested scrubs (via MOSDScrub2 message) should be enqueued
  // for the scheduler to pick up, not started immediately.
  // The scheduler will call start_scrub() which sets m_active_target before
  // calling handle_scrub_requested().
  pg.scrubber.enqueue_scrub_requested(deep);
  return seastar::now();
}

ScrubMessage::ifut<> ScrubMessage::handle_event(PG &pg)
{
  pg.scrubber.handle_scrub_message(*m);
  return seastar::now();
}

template class RemoteScrubEventBaseT<ScrubRequested>;
template class RemoteScrubEventBaseT<ScrubMessage>;

template <typename T>
ScrubAsyncOpT<T>::ScrubAsyncOpT(Ref<PG> pg) : pg(pg) {}

template <typename T>
typename ScrubAsyncOpT<T>::template ifut<> ScrubAsyncOpT<T>::start()
{
  LOG_PREFIX(ScrubAsyncOpT::start);
  DEBUGDPP("{} starting", *pg, *this);
  return run(*pg);
}

ScrubFindRange::ifut<> ScrubFindRange::run(PG &pg)
{
  LOG_PREFIX(ScrubFindRange::run);
  using crimson::common::local_conf;
  auto [_, next] = co_await pg.backend->list_objects(
    begin,
    local_conf().get_val<int64_t>("osd_scrub_chunk_max"));

  // We rely on seeing an entire set of snapshots in a single chunk
  auto end = next.get_max_object_boundary();

  DEBUGDPP("got next: {}, returning begin, end: {}, {}",
	   pg, next, begin, end);
  pg.scrubber.machine.process_event(
    scrub::ScrubContext::request_range_complete_t{begin, end});
}

template class ScrubAsyncOpT<ScrubFindRange>;

ScrubReserveRange::ifut<> ScrubReserveRange::run(PG &pg)
{
  LOG_PREFIX(ScrubReserveRange::run);
  DEBUGDPP("", pg);
  DEBUGDPP("waiting for pg background_process_lock", pg);
  return pg.background_process_lock.lock(
  ).then_interruptible([FNAME, this, &pg] {
    DEBUGDPP("pg_background_io_mutex locked", pg);
    auto &scrubber = pg.scrubber;
    ceph_assert(!scrubber.blocked);
    scrubber.blocked = scrub::blocked_range_t{begin, end};
    blocked_set = true;
    auto& log = pg.peering_state.get_pg_log().get_log().log;
    auto p = find_if(
      log.crbegin(), log.crend(),
      [this](const auto& e) -> bool {
	return e.soid >= begin && e.soid < end;
      });

    if (p == log.crend()) {
      return scrubber.machine.process_event(
	scrub::ScrubContext::reserve_range_complete_t{eversion_t{}});
    } else {
      return scrubber.machine.process_event(
	scrub::ScrubContext::reserve_range_complete_t{p->version});
    }
  }).finally([FNAME, &pg, this] {
    if (!blocked_set) {
      DEBUGDPP("releasing pg background_process_lock (reserve not set)", pg);
      pg.background_process_lock.unlock();
    }
  });
}

template class ScrubAsyncOpT<ScrubReserveRange>;

ScrubScan::ifut<> ScrubScan::run(PG &pg)
{
  LOG_PREFIX(ScrubScan::start);
  // legacy value, unused
  ret.valid_through = pg.get_info().last_update;

  DEBUGDPP("begin: {}, end: {}", pg, begin, end);
  using crimson::common::local_conf;
  auto throttle = co_await interruptor::make_interruptible(
    pg.shard_services.get_throttle(
      scheduler::params_t{
        static_cast<int>(local_conf()->osd_scrub_event_cost),
        static_cast<unsigned>(local_conf()->osd_scrub_priority),
        0,
        SchedulerClass::background_best_effort}));
  auto [objects, _] = co_await pg.backend->list_objects(begin, end);

  DEBUGDPP("listed {} objects", pg, objects);
  for (const auto &object: objects) {
    co_await scan_object(
      pg,
      ghobject_t(object, ghobject_t::NO_GEN, pg.get_pgid().shard));
  }

  if (local) {
    DEBUGDPP("complete, submitting local event", pg);
    pg.scrubber.handle_event(
      scrub::ScrubContext::scan_range_complete_t(
	pg.get_pg_whoami(),
	std::move(ret)));
  } else {
    DEBUGDPP("complete, sending response to primary", pg);
    auto m = crimson::make_message<MOSDRepScrubMap>(
      spg_t(pg.get_pgid().pgid, pg.get_primary().shard),
      pg.get_osdmap_epoch(),
      pg.get_pg_whoami());
    encode(ret, m->get_data());
    pg.scrubber.handle_event(
      scrub::ScrubContext::generate_and_submit_chunk_result_complete_t{});
    co_await interruptor::make_interruptible(
      pg.shard_services.send_to_osd(
	pg.get_primary().osd,
	std::move(m),
	pg.get_osdmap_epoch()));
  }
}

ScrubScan::ifut<> ScrubScan::scan_object(
  PG &pg,
  const ghobject_t &obj)
{
  LOG_PREFIX(ScrubScan::scan_object);
  DEBUGDPP("obj: {}", pg, obj);
  auto &entry = ret.objects[obj.hobj];
  return interruptor::make_interruptible(
    crimson::os::with_store<&crimson::os::FuturizedStore::Shard::stat>(
      pg.shard_services.get_store(pg.get_store_index()),
      pg.get_collection_ref(),
      obj,
      0)
  ).then_interruptible([FNAME, &pg, &obj, &entry](struct stat obj_stat) {
    DEBUGDPP("obj: {}, stat complete, size {}", pg, obj, obj_stat.st_size);
    entry.size = obj_stat.st_size;
    return crimson::os::with_store<&crimson::os::FuturizedStore::Shard::get_attrs>(
      pg.shard_services.get_store(pg.get_store_index()),
      pg.get_collection_ref(),
      obj,
      0);
  }).safe_then_interruptible([FNAME, &pg, &obj, &entry](auto &&attrs) {
    DEBUGDPP("obj: {}, got {} attrs", pg, obj, attrs.size());
    for (auto &i : attrs) {
      i.second.rebuild();
      if (i.second.length() == 0) {
	entry.attrs[i.first];
      } else {
	entry.attrs.emplace(i.first, i.second);
      }
    }
  }).handle_error_interruptible(
    ct_error::all_same_way([FNAME, &pg, &obj, &entry](auto e) {
      DEBUGDPP("obj: {} stat error", pg, obj);
      entry.stat_error = true;
      return seastar::now();
    })
  ).then_interruptible([FNAME, this, &pg, &obj] {
    if (deep) {
      DEBUGDPP("obj: {} doing deep scan", pg, obj);
      return deep_scan_object(pg, obj);
    } else {
      return interruptor::now();
    }
  });

}

ScrubScan::ifut<> ScrubScan::deep_scan_object(
  PG &pg,
  const ghobject_t &obj)
{
  LOG_PREFIX(ScrubScan::deep_scan_object);
  DEBUGDPP("obj: {}", pg, obj);
  using crimson::common::local_conf;
  auto &entry = ret.objects[obj.hobj];
  auto progress_ref = std::make_unique<obj_scrub_progress_t>();
  auto &progress = *progress_ref;

  co_await interruptor::repeat(
    [FNAME, this, &progress, &obj, &entry, &pg]()
    -> interruptible_future<seastar::stop_iteration>
  {
    auto store_read = [FNAME, this, &progress, &obj, &entry, &pg]()
      -> interruptible_future<seastar::stop_iteration>
    {
      DEBUGDPP("op: {}, obj: {}, progress: {} scanning data",
                pg, *this, obj, progress);
      const auto stride = local_conf().get_val<Option::size_t>(
        "osd_deep_scrub_stride");
      return crimson::os::with_store<&crimson::os::FuturizedStore::Shard::read>(
        pg.shard_services.get_store(pg.get_store_index()),
        pg.get_collection_ref(),
        obj,
        *(progress.offset),
        stride,
        0
      ).safe_then([this, FNAME, stride, &obj, &progress, &entry, &pg](auto bl) {
        size_t offset = *progress.offset;
        DEBUGDPP("op: {}, obj: {}, progress: {} got offset {}",
                  pg, *this, obj, progress, offset);
        progress.data_hash << bl;
        if (bl.length() < stride) {
          progress.offset = std::nullopt;
          entry.digest = progress.data_hash.digest();
          entry.digest_present = true;
        } else {
          ceph_assert(stride == bl.length());
          *(progress.offset) += stride;
        }
      }).handle_error(
        ct_error::all_same_way([&progress, &entry](auto e) {
          entry.read_error = true;
          progress.offset = std::nullopt;
          return seastar::now();
        })
      ).then([] {
        return seastar::make_ready_future<seastar::stop_iteration>(
               seastar::stop_iteration::no);
      });
    };

    auto get_header = [FNAME, this, &progress, &obj, &entry, &pg]()
      -> interruptible_future<seastar::stop_iteration>
   {
      DEBUGDPP("op: {}, obj: {}, progress: {} scanning omap header",
                pg, *this, obj, progress);
      return crimson::os::with_store<&crimson::os::FuturizedStore::Shard::omap_get_header>(
        pg.shard_services.get_store(pg.get_store_index()),
        pg.get_collection_ref(),
        obj,
        0
      ).safe_then([&progress](auto bl) {
        progress.omap_hash << bl;
      }).handle_error(
        ct_error::enodata::handle([] { return seastar::now(); }),
        ct_error::all_same_way([&entry](auto e) {
          entry.read_error = true;
          return seastar::now();
        })
      ).then([&progress] {
        progress.header_done = true;
        return seastar::make_ready_future<seastar::stop_iteration>(
               seastar::stop_iteration::no);
      });
    };

    ObjectStore::omap_iter_seek_t start_from;
    start_from.seek_position =  progress.next_key.has_value() ?
                                progress.next_key.value() : std::string{};
    start_from.seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND;

    std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)> callback =
      [&progress, &entry] (std::string_view key, std::string_view value)
    {
      bufferlist bl;
      encode(key, bl);
      encode(value, bl);
      progress.omap_hash << bl;
      entry.object_omap_keys++;
      entry.object_omap_bytes += value.length();
      return ObjectStore::omap_iter_ret_t::NEXT;
    };

    auto get_keys = [FNAME, this, &progress, &obj, &entry, &pg, start_from, callback]()
      -> interruptible_future<seastar::stop_iteration>
    {
      DEBUGDPP("op: {}, obj: {}, progress: {} scanning omap keys",
                pg, *this, obj, progress);
      return crimson::os::with_store<&crimson::os::FuturizedStore::Shard::omap_iterate>(
        pg.shard_services.get_store(pg.get_store_index()),
        pg.get_collection_ref(),
        obj,
        start_from,
        callback,
        0,
	nullptr
      ).safe_then([FNAME, this, &obj, &progress, &entry, &pg](auto result) {
        assert(result == ObjectStore::omap_iter_ret_t::NEXT);
        DEBUGDPP("op: {}, obj: {}, progress: {} omap done",
                  pg, *this, obj, progress);
        progress.keys_done = true;
        entry.omap_digest = progress.omap_hash.digest();
        entry.omap_digest_present = true;

        if ((entry.object_omap_keys >
             local_conf().get_val<uint64_t>(
             "osd_deep_scrub_large_omap_object_key_threshold")) ||
            (entry.object_omap_bytes >
             local_conf().get_val<Option::size_t>(
             "osd_deep_scrub_large_omap_object_value_sum_threshold"))) {
          entry.large_omap_object_found = true;
          entry.large_omap_object_key_count = entry.object_omap_keys;
          ret.has_large_omap_object_errors = true;
        }
      }).handle_error(
        ct_error::all_same_way([FNAME, this, &obj, &progress, &entry, &pg]
          (auto e)
        {
          DEBUGDPP("op: {}, obj: {}, progress: {} error reading omap {}",
                    pg, *this, obj, progress, e);
          progress.keys_done = true;
          entry.read_error = true;
          return seastar::now();
        })
      ).then([] {
        return seastar::make_ready_future<seastar::stop_iteration>(
               seastar::stop_iteration::no);
      });
    };

    if (progress.offset) {
      co_return co_await store_read();
    } else if (!progress.header_done) {
      co_return co_await get_header();
    } else if (!progress.keys_done) {
      co_return co_await get_keys();
    } else {
      DEBUGDPP("op: {}, obj: {}, progress: {} done",
                pg, *this, obj, progress);
      co_return seastar::stop_iteration::yes;
    }
  }).finally([progress_ref=std::move(progress_ref)] {});
}

template class ScrubAsyncOpT<ScrubScan>;

ScrubSleep::ifut<> ScrubSleep::run(PG &pg)
{
  LOG_PREFIX(ScrubSleep::run);
  auto sleep_time = pg.scrubber.get_scrub_sleep_time();
  DEBUGDPP("sleeping for {} ms", pg, sleep_time.count());
  
  return interruptor::make_interruptible(seastar::sleep(sleep_time)
  ).then_interruptible([FNAME, &pg] {
    DEBUGDPP("sleep complete, posting event to continue scrub", pg);
    pg.scrubber.machine.process_event(
      scrub::events::internal_sched_scrub_t{});
  });
}

template class ScrubAsyncOpT<ScrubSleep>;

ScrubDigestUpdate::ifut<> ScrubDigestUpdate::run(PG &pg)
{
  LOG_PREFIX(ScrubDigestUpdate::run);
  DEBUGDPP("oid: {}", pg, oid);

  auto notify_complete = seastar::defer([&pg, generation = generation] {
    pg.scrubber.on_digest_update_complete(generation);
  });

  // Use a fresh orderer scoped to this operation
  auto obc_orderer = pg.obc_loader.get_obc_orderer(oid);
  auto obc_manager = pg.obc_loader.get_obc_manager(
    obc_orderer, oid, false /* resolve_clone */);

  bool load_failed = false;
  co_await pg.obc_loader.load_and_lock(
    obc_manager, RWState::RWWRITE
  ).handle_error_interruptible(
    crimson::ct_error::enoent::handle([&load_failed] {
      load_failed = true;
      return seastar::now();
    }),
    crimson::ct_error::object_corrupted::handle([&load_failed] {
      load_failed = true;
      return seastar::now();
    })
  );

  if (load_failed) {
    ERRORDPP("failed to load obc for {}, skipping digest update", pg, oid);
    co_return;
  }

  auto obc = obc_manager.get_obc();
  if (!obc->obs.exists) {
    DEBUGDPP("object {} no longer exists, skipping digest update", pg, oid);
    co_return;
  }
  const auto& soid = obc->obs.oi.soid;
  if (soid != oid) {
    DEBUGDPP("digest update oid remapped from {} to {}", pg, oid, soid);
  }
  const std::vector<snapid_t>* clone_snaps = nullptr;
  if (soid.snap < CEPH_MAXSNAP) {
    auto it = obc->ssc->snapset.clone_snaps.find(soid.snap);
    if (it == obc->ssc->snapset.clone_snaps.end() || it->second.empty()) {
      ERRORDPP("missing clone snap mapping for {}, skipping digest update", pg, soid);
      co_return;
    }
    clone_snaps = &it->second;
  }

  // Hold submit_lock around version assignment + transaction submission
  co_await interruptor::make_interruptible(pg.submit_lock.lock());
  auto unlock_submit = seastar::defer([&pg] {
    pg.submit_lock.unlock();
  });

  // Apply the digest updates to oi
  auto &oi = obc->obs.oi;
  if (data_digest) {
    oi.set_data_digest(*data_digest);
    DEBUGDPP("set data_digest=0x{:x} on {}", pg, *data_digest, oid);
  }
  if (omap_digest) {
    oi.set_omap_digest(*omap_digest);
    DEBUGDPP("set omap_digest=0x{:x} on {}", pg, *omap_digest, oid);
  }

  // Build the transaction: write updated OI_ATTR
  ceph::os::Transaction txn;
  {
    ceph::bufferlist bl;
    oi.encode(bl, pg.get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    txn.setattr(
      pg.get_collection_ref()->get_cid(),
      ghobject_t{soid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD},
      OI_ATTR,
      bl);
  }

  // Build MODIFY log entry (mirrors PrimaryLogScrub::submit_digest_fixes)
  osd_op_params_t osd_op_p;
  osd_op_p.at_version = pg.get_next_version();
  osd_op_p.mtime = ceph_clock_now();

  eversion_t prior_version = oi.version;
  oi.prior_version = prior_version;
  oi.version = osd_op_p.at_version;

  std::vector<pg_log_entry_t> log_entries;
  log_entries.emplace_back(
    pg_log_entry_t::MODIFY,
    soid,
    oi.version,
    prior_version,
    oi.user_version,
    osd_reqid_t(),
    osd_op_p.mtime,
    0);

  if (clone_snaps) {
    encode(*clone_snaps, log_entries.back().snaps);
  }

  auto [submitted, all_completed] = co_await pg.submit_transaction(
    ObjectContextRef(obc),
    nullptr,
    std::move(txn),
    std::move(osd_op_p),
    std::move(log_entries));
  co_await std::move(submitted);

  DEBUGDPP("digest update submitted for {}", pg, oid);

  // Release submit_lock before waiting for all_completed
  unlock_submit.cancel();
  pg.submit_lock.unlock();

  co_await std::move(all_completed);
  DEBUGDPP("digest update complete for {}", pg, oid);
}

template class ScrubAsyncOpT<ScrubDigestUpdate>;

}

