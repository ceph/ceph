// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"
#include "crimson/common/coroutine.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_connection_priv.h"
#include "messages/MOSDRepScrubMap.h"
#include "scrub_events.h"

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
  pg.scrubber.handle_scrub_requested(deep);
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
  }).finally([&pg, this] {
    if (!blocked_set) {
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
    pg.shard_services.get_store().stat(
      pg.get_collection_ref(),
      obj)
  ).then_interruptible([FNAME, &pg, &obj, &entry](struct stat obj_stat) {
    DEBUGDPP("obj: {}, stat complete, size {}", pg, obj, obj_stat.st_size);
    entry.size = obj_stat.st_size;
    return pg.shard_services.get_store().get_attrs(
      pg.get_collection_ref(),
      obj);
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
  return interruptor::repeat(
    [FNAME, this, &progress, &obj, &entry, &pg]()
    -> interruptible_future<seastar::stop_iteration> {
      if (progress.offset) {
	DEBUGDPP("op: {}, obj: {}, progress: {} scanning data",
		 pg, *this, obj, progress);
	const auto stride = local_conf().get_val<Option::size_t>(
	  "osd_deep_scrub_stride");
	return pg.shard_services.get_store().read(
	  pg.get_collection_ref(),
	  obj,
	  *(progress.offset),
	  stride
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
	  return interruptor::make_interruptible(
	    seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no));
	});
      } else if (!progress.header_done) {
	DEBUGDPP("op: {}, obj: {}, progress: {} scanning omap header",
		 pg, *this, obj, progress);
	return pg.shard_services.get_store().omap_get_header(
	  pg.get_collection_ref(),
	  obj
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
	  return interruptor::make_interruptible(
	    seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no));
	});
      } else if (!progress.keys_done) {
	DEBUGDPP("op: {}, obj: {}, progress: {} scanning omap keys",
		 pg, *this, obj, progress);
	return pg.shard_services.get_store().omap_get_values(
	  pg.get_collection_ref(),
	  obj,
	  progress.next_key
	).safe_then([FNAME, this, &obj, &progress, &entry, &pg](auto result) {
	  const auto &[done, omap] = result;
	  DEBUGDPP("op: {}, obj: {}, progress: {} got {} keys",
		   pg, *this, obj, progress, omap.size());
	  for (const auto &p : omap) {
	    bufferlist bl;
	    encode(p.first, bl);
	    encode(p.second, bl);
	    progress.omap_hash << bl;
	    entry.object_omap_keys++;
	    entry.object_omap_bytes += p.second.length();
	  }
	  if (done) {
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
	  } else {
	    ceph_assert(!omap.empty()); // omap_get_values invariant
	    DEBUGDPP("op: {}, obj: {}, progress: {} omap not done, next {}",
		     pg, *this, obj, progress, omap.crbegin()->first);
	    progress.next_key = omap.crbegin()->first;
	  }
	}).handle_error(
	  ct_error::all_same_way([FNAME, this, &obj, &progress, &entry, &pg]
				 (auto e) {
	    DEBUGDPP("op: {}, obj: {}, progress: {} error reading omap {}",
		     pg, *this, obj, progress, e);
	    progress.keys_done = true;
	    entry.read_error = true;
	    return seastar::now();
	  })
	).then([] {
	  return interruptor::make_interruptible(
	    seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no));
	});
      } else {
	DEBUGDPP("op: {}, obj: {}, progress: {} done",
		 pg, *this, obj, progress);
	return interruptor::make_interruptible(
	  seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes));
      }
    }).finally([progress_ref=std::move(progress_ref)] {});
}

template class ScrubAsyncOpT<ScrubScan>;

}

