// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_operations/pg_advance_map.h"

#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGAdvanceMap::PGAdvanceMap(
  OSD &osd, Ref<PG> pg, epoch_t from, epoch_t to,
  PeeringCtx &&rctx, bool do_init)
  : osd(osd), pg(pg), from(from), to(to),
    rctx(std::move(rctx)), do_init(do_init) {}

PGAdvanceMap::~PGAdvanceMap() {}

void PGAdvanceMap::print(std::ostream &lhs) const
{
  lhs << "PGAdvanceMap("
      << "pg=" << pg->get_pgid()
      << " from=" << from
      << " to=" << to;
  if (do_init) {
    lhs << " do_init";
  }
  lhs << ")";
}

void PGAdvanceMap::dump_detail(Formatter *f) const
{
  f->open_object_section("PGAdvanceMap");
  f->dump_stream("pgid") << pg->get_pgid();
  f->dump_int("from", from);
  f->dump_int("to", to);
  f->dump_bool("do_init", do_init);
  f->close_section();
}

seastar::future<> PGAdvanceMap::start()
{
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;

  logger().debug("{}: start", *this);

  IRef ref = this;
  return with_blocking_future(
    handle.enter(pg->peering_request_pg_pipeline.process))
    .then([this] {
      if (do_init) {
	pg->handle_initialize(rctx);
	pg->handle_activate_map(rctx);
      }
      return seastar::do_for_each(
	boost::make_counting_iterator(from + 1),
	boost::make_counting_iterator(to + 1),
	[this](epoch_t next_epoch) {
	  return osd.get_map(next_epoch).then(
	    [this] (cached_map_t&& next_map) {
	      pg->handle_advance_map(next_map, rctx);
	    });
	}).then([this] {
	  pg->handle_activate_map(rctx);
	  handle.exit();
	  if (do_init) {
	    osd.pg_map.pg_created(pg->get_pgid(), pg);
	    osd.shard_services.inc_pg_num();
	    logger().info("PGAdvanceMap::start new pg {}", *pg);
	  }
	  return seastar::when_all_succeed(
	    pg->get_need_up_thru() \
              ? osd.shard_services.send_alive(pg->get_same_interval_since())
              : seastar::now(),
	    osd.shard_services.dispatch_context(
	      pg->get_collection_ref(),
	      std::move(rctx)));
	});
    }).then([this, ref=std::move(ref)] {
      logger().debug("{}: complete", *this);
    });
}

}
