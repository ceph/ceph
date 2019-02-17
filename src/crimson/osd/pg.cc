#include "pg.h"

PG::PG(pg_pool_t&& pool, std::string&& name, ec_profile_t&& ec_profile)
{
  // TODO
}

bool PG::can_discard_request(OpRef op)
{ // implement later
  return false;
}

seastar::future<bool>PG::handle_backoff_feature(OpRef op)
{
  return seastar::make_ready_future<bool>(true);
}

seastar::future<bool>PG::handle_not_peered(OpRef op)
{
  return seastar::make_ready_future<bool>(true);
}

seastar::future<> PG::do_request(OpRef op)
{
  if (can_discard_request(op)) {
    return seastar::now();
  }

  return handle_backoff_feature(op).then([&,this](bool go_on){
    if (!go_on)
      return seastar::now();
    else
      return handle_not_peered(op). then([&,this](bool go_on){
        if (!go_on)
          return seastar::now();
        else
          return pgbackend->handle_message(op);
      });
  });

}

