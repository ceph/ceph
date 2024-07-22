// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <seastar/core/future.hh>

#include "crimson/osd/osd_operations/internal_client_request.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::InternalClientRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

SET_SUBSYS(osd);

namespace crimson::osd {

InternalClientRequest::InternalClientRequest(Ref<PG> pg)
  : pg(std::move(pg))
{
  assert(bool(this->pg));
  assert(this->pg->is_primary());
}

InternalClientRequest::~InternalClientRequest()
{
  LOG_PREFIX(InternalClientRequest::~InternalClientRequest);
  DEBUGI("{}: destroying", *this);
}

void InternalClientRequest::print(std::ostream &) const
{
}

void InternalClientRequest::dump_detail(Formatter *f) const
{
}

CommonPGPipeline& InternalClientRequest::client_pp()
{
  return pg->request_pg_pipeline;
}

seastar::future<> InternalClientRequest::start()
{
  track_event<StartEvent>();
  return crimson::common::handle_system_shutdown([this] {
    return seastar::repeat([this] {
      LOG_PREFIX(InternalClientRequest::start);
      DEBUGI("{}: in repeat", *this);
      return interruptor::with_interruption([this]() mutable {
        return enter_stage<interruptor>(
	  client_pp().wait_for_active
        ).then_interruptible([this] {
          return with_blocking_event<PGActivationBlocker::BlockingEvent,
	  			     interruptor>([this] (auto&& trigger) {
            return pg->wait_for_active_blocker.wait(std::move(trigger));
          });
        }).then_interruptible([this] {
          return enter_stage<interruptor>(
            client_pp().recover_missing);
        }).then_interruptible([this] {
          return do_recover_missing(pg, get_target_oid(), osd_reqid_t());
        }).then_interruptible([this] {
          return enter_stage<interruptor>(
            client_pp().get_obc);
        }).then_interruptible([this] () -> PG::load_obc_iertr::future<> {
          LOG_PREFIX(InternalClientRequest::start);
          DEBUGI("{}: getting obc lock", *this);
          return seastar::do_with(create_osd_ops(),
            [this](auto& osd_ops) mutable {
            LOG_PREFIX(InternalClientRequest::start);
            DEBUGI("InternalClientRequest: got {} OSDOps to execute",
                           std::size(osd_ops));
            [[maybe_unused]] const int ret = op_info.set_from_op(
              std::as_const(osd_ops), pg->get_pgid().pgid, *pg->get_osdmap());
            assert(ret == 0);
            // call with_locked_obc() in order, but wait concurrently for loading.
            enter_stage_sync(client_pp().lock_obc);
            return pg->with_locked_obc(get_target_oid(), op_info,
              [&osd_ops, this](auto, auto obc) {
              return enter_stage<interruptor>(client_pp().process
              ).then_interruptible(
                [obc=std::move(obc), &osd_ops, this] {
                return pg->do_osd_ops(
                  std::move(obc),
                  osd_ops,
                  std::as_const(op_info),
                  get_do_osd_ops_params()
                ).safe_then_unpack_interruptible(
                  [](auto submitted, auto all_completed) {
                    return all_completed.handle_error_interruptible(
                      crimson::ct_error::eagain::handle([] {
                        return seastar::now();
                      }));
                  }, crimson::ct_error::eagain::handle([] {
                    return interruptor::now();
                  })
                );
              });
            });
          });
        }).si_then([this] {
          logger().debug("{}: complete", *this);
          return handle.complete();
        }).handle_error_interruptible(
          PG::load_obc_ertr::all_same_way([] {
            return seastar::now();
          })
        ).then_interruptible([] {
          return seastar::stop_iteration::yes;
        });
      }, [this](std::exception_ptr eptr) {
        if (should_abort_request(*this, std::move(eptr))) {
          return seastar::stop_iteration::yes;
        } else {
          return seastar::stop_iteration::no;
        }
      }, pg);
    }).then([this] {
      track_event<CompletionEvent>();
    }).finally([this] {
      logger().debug("{}: exit", *this);
      handle.exit();
    });
  });
}

} // namespace crimson::osd

