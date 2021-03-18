// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <seastar/core/future.hh>

#include "crimson/osd/osd_operations/internal_client_request.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

InternalClientRequest::InternalClientRequest(Ref<PG> pg)
  : pg(std::move(pg))
{
}

InternalClientRequest::~InternalClientRequest()
{
  logger().debug("{}: destroying", *this);
}

void InternalClientRequest::print(std::ostream &) const
{
}

void InternalClientRequest::dump_detail(Formatter *f) const
{
}

CommonPGPipeline& InternalClientRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<> InternalClientRequest::start()
{
  return crimson::common::handle_system_shutdown([this] {
    return seastar::repeat([this] {
      logger().debug("{}: in repeat", *this);
      return interruptor::with_interruption([this]() mutable {
        return with_blocking_future_interruptible<IOInterruptCondition>(
          handle.enter(pp().wait_for_active)
        ).then_interruptible([this] {
          return with_blocking_future_interruptible<IOInterruptCondition>(
            pg->wait_for_active_blocker.wait());
        }).then_interruptible([this] {
          return with_blocking_future_interruptible<IOInterruptCondition>(
            handle.enter(pp().recover_missing)
          ).then_interruptible([this] {
            return do_recover_missing(pg, {});
          }).then_interruptible([this] {
            return with_blocking_future_interruptible<IOInterruptCondition>(
              handle.enter(pp().get_obc)
            ).then_interruptible([this] () -> PG::load_obc_iertr::future<> {
              logger().debug("{}: getting obc lock", *this);
              return fabricate_osd_ops().then([this] (auto&& osd_ops) {
                logger().debug("InternalClientRequest: got {} OSDOps to execute",
                               std::size(osd_ops));
                [[maybe_unused]] const int ret = op_info.set_from_op(
                  std::as_const(osd_ops), pg->get_pgid().pgid, *pg->get_osdmap());
                assert(ret == 0);
                return pg->with_locked_obc(get_target_oid(), op_info,
                  [osd_ops=std::move(osd_ops), this](auto obc) {
                  return with_blocking_future_interruptible<IOInterruptCondition>(
                    handle.enter(pp().process)
                  ).then_interruptible(
                    [obc=std::move(obc), osd_ops=std::move(osd_ops), this] {
                    return pg->do_osd_ops(
                      std::move(obc),
                      std::move(osd_ops),
                      std::as_const(op_info),
                      get_do_osd_ops_params(),
                      [this] {
                        return PG::do_osd_ops_iertr::now();
                      },
                      [this] (const std::error_code& e) {
                        return PG::do_osd_ops_iertr::now();
                      }
                    ).safe_then_interruptible(
                      [this]() -> interruptible_future<> {
                        return interruptor::now();
                      }, crimson::ct_error::eagain::handle([this]() mutable {
                        return interruptor::now();
                      })
                    );
                  });
                });
              });
            }).handle_error_interruptible(PG::load_obc_ertr::all_same_way([] {
              return seastar::now();
            })).then_interruptible([] {
              return seastar::stop_iteration::yes;
            });
          });
        });
      }, [this](std::exception_ptr eptr) mutable {
        if (*eptr.__cxa_exception_type() ==
            typeid(::crimson::common::actingset_changed)) {
          try {
            std::rethrow_exception(eptr);
          } catch(::crimson::common::actingset_changed& e) {
            if (e.is_primary()) {
              logger().debug("{} operation restart, acting set changed", *this);
              return seastar::stop_iteration::no;
            } else {
              logger().debug("{} operation abort, up primary changed", *this);
              return seastar::stop_iteration::yes;
            }
          }
        }
        assert(*eptr.__cxa_exception_type() ==
          typeid(crimson::common::system_shutdown_exception));
        crimson::get_logger(ceph_subsys_osd).debug(
            "{} operation skipped, system shutdown", *this);
        return seastar::stop_iteration::yes;
      }, pg);
    });
  });
}

InternalClientRequest::interruptible_future<>
InternalClientRequest::do_recover_missing(
  Ref<PG>& pgref, const hobject_t& soid)
{
  // TODO: share this with ClientRequest
  return seastar::now();
}

} // namespace crimson::osd

